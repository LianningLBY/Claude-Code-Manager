"""Email verification must use deployment-owned SMTP credentials."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from backend.config import settings
from backend.services import email_service


@pytest.fixture(autouse=True)
def _isolated_verification_state():
    email_service._reset_state_for_tests()
    yield
    email_service._reset_state_for_tests()


def test_send_code_fails_closed_when_smtp_credentials_missing(monkeypatch):
    monkeypatch.setattr(settings, "smtp_user", "")
    monkeypatch.setattr(settings, "smtp_password", "")

    assert email_service.send_verification_code("user@example.com") is False
    assert "user@example.com" not in email_service._codes


def test_send_code_uses_runtime_smtp_configuration(monkeypatch):
    monkeypatch.setattr(settings, "smtp_host", "smtp.example.test")
    monkeypatch.setattr(settings, "smtp_port", 2465)
    monkeypatch.setattr(settings, "smtp_user", "mailer@example.test")
    monkeypatch.setattr(settings, "smtp_password", "deployment-secret")
    monkeypatch.setattr(settings, "smtp_from", "ccm@example.test")

    observed: dict[str, object] = {}

    class _Smtp:
        def __init__(self, host, port, timeout):
            observed.update(host=host, port=port, timeout=timeout)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def login(self, user, password):
            observed.update(user=user, password=password)

        def sendmail(self, sender, recipients, message):
            observed.update(
                sender=sender,
                recipients=recipients,
                message=message,
            )

    monkeypatch.setattr(email_service.smtplib, "SMTP_SSL", _Smtp)

    assert email_service.send_verification_code(
        "User@Example.com",
        "192.0.2.10",
    ) is True
    assert observed["host"] == "smtp.example.test"
    assert observed["port"] == 2465
    assert observed["user"] == "mailer@example.test"
    assert observed["password"] == "deployment-secret"
    assert observed["sender"] == "ccm@example.test"
    assert "user@example.com" in email_service._codes


def test_verification_code_is_consumed_exactly_once_across_threads():
    email_service._codes["user@example.com"] = (
        "123456",
        email_service.time.time() + 60,
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda _: email_service.verify_code(
                "USER@example.com",
                "123456",
            ),
            range(8),
        ))

    assert results.count(True) == 1
    assert results.count(False) == 7


def test_send_code_enforces_email_and_ip_limits(monkeypatch):
    _configure_smtp(monkeypatch)
    monkeypatch.setattr(settings, "verification_code_resend_cooldown_seconds", 0)
    monkeypatch.setattr(settings, "verification_code_email_limit", 1)
    monkeypatch.setattr(settings, "verification_code_ip_limit", 10)

    assert email_service.send_verification_code(
        "user@example.com",
        "192.0.2.1",
    )
    with pytest.raises(email_service.VerificationCodeRateLimitError):
        email_service.send_verification_code(
            "USER@example.com",
            "192.0.2.2",
        )

    email_service._reset_state_for_tests()
    monkeypatch.setattr(settings, "verification_code_email_limit", 10)
    monkeypatch.setattr(settings, "verification_code_ip_limit", 1)
    assert email_service.send_verification_code(
        "first@example.com",
        "192.0.2.1",
    )
    with pytest.raises(email_service.VerificationCodeRateLimitError):
        email_service.send_verification_code(
            "second@example.com",
            "192.0.2.1",
        )


def test_expired_codes_are_cleaned_before_capacity_check(monkeypatch):
    _configure_smtp(monkeypatch)
    monkeypatch.setattr(settings, "verification_code_capacity", 1)
    email_service._codes["expired@example.com"] = (
        "123456",
        email_service.time.time() - 1,
    )

    assert email_service.send_verification_code(
        "new@example.com",
        "192.0.2.1",
    )
    assert "expired@example.com" not in email_service._codes
    assert "new@example.com" in email_service._codes


def test_code_and_rate_state_have_a_hard_capacity(monkeypatch):
    _configure_smtp(monkeypatch)
    monkeypatch.setattr(settings, "verification_code_capacity", 1)
    assert email_service.send_verification_code(
        "first@example.com",
        "192.0.2.1",
    )

    with pytest.raises(email_service.VerificationCodeCapacityError):
        email_service.send_verification_code(
            "second@example.com",
            "192.0.2.2",
        )


def test_smtp_delivery_concurrency_is_bounded(monkeypatch):
    _configure_smtp(monkeypatch)
    monkeypatch.setattr(settings, "verification_code_smtp_concurrency", 1)
    entered = threading.Event()
    release = threading.Event()

    class _BlockingSmtp:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def login(self, *_args):
            pass

        def sendmail(self, *_args):
            entered.set()
            assert release.wait(timeout=2)

    monkeypatch.setattr(
        email_service.smtplib,
        "SMTP_SSL",
        _BlockingSmtp,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(
            email_service.send_verification_code,
            "first@example.com",
            "192.0.2.1",
        )
        assert entered.wait(timeout=2)
        with pytest.raises(email_service.VerificationCodeCapacityError):
            email_service.send_verification_code(
                "second@example.com",
                "192.0.2.2",
            )
        release.set()
        assert first.result(timeout=2) is True


def _configure_smtp(monkeypatch):
    monkeypatch.setattr(settings, "smtp_host", "smtp.example.test")
    monkeypatch.setattr(settings, "smtp_port", 2465)
    monkeypatch.setattr(settings, "smtp_user", "mailer@example.test")
    monkeypatch.setattr(settings, "smtp_password", "deployment-secret")
    monkeypatch.setattr(settings, "smtp_from", "")

    class _Smtp:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def login(self, *_args):
            pass

        def sendmail(self, *_args):
            pass

    monkeypatch.setattr(email_service.smtplib, "SMTP_SSL", _Smtp)
