"""Email verification code service via SMTP."""

from collections import defaultdict, deque
from math import ceil
import logging
import secrets
import smtplib
import threading
import time
from email.mime.text import MIMEText

from backend.config import settings

logger = logging.getLogger(__name__)

# In-memory code store: {email: (code, expire_timestamp)}
_codes: dict[str, tuple[str, float]] = {}
_attempts: deque[tuple[float, str, str]] = deque()
_email_attempts: dict[str, deque[float]] = defaultdict(deque)
_ip_attempts: dict[str, deque[float]] = defaultdict(deque)
_pending_emails: set[str] = set()
_smtp_in_flight = 0
_state_lock = threading.Lock()
CODE_EXPIRE_SECONDS = 300  # 5 minutes


class VerificationCodeRateLimitError(Exception):
    """A verification-code request exceeded a server-side quota."""

    def __init__(self, retry_after: int):
        self.retry_after = max(1, retry_after)
        super().__init__("Too many verification code requests")


class VerificationCodeCapacityError(Exception):
    """The bounded in-memory verification state cannot accept more entries."""


def _normalise_email(email: str) -> str:
    return email.strip().casefold()


def _cleanup_locked(now: float) -> None:
    """Drop expired codes and rate-limit events while holding ``_state_lock``."""
    expired_emails = [
        email for email, (_, expires_at) in _codes.items()
        if expires_at <= now
    ]
    for email in expired_emails:
        _codes.pop(email, None)

    cutoff = now - max(1, settings.verification_code_rate_window_seconds)
    while _attempts and _attempts[0][0] <= cutoff:
        _, email, client_ip = _attempts.popleft()
        email_bucket = _email_attempts[email]
        email_bucket.popleft()
        if not email_bucket:
            _email_attempts.pop(email, None)
        ip_bucket = _ip_attempts[client_ip]
        ip_bucket.popleft()
        if not ip_bucket:
            _ip_attempts.pop(client_ip, None)


def _reserve_send(email: str, client_ip: str, now: float) -> None:
    """Atomically enforce quotas and reserve one bounded code-store slot."""
    global _smtp_in_flight

    capacity = max(1, settings.verification_code_capacity)
    window = max(1, settings.verification_code_rate_window_seconds)
    email_limit = max(1, settings.verification_code_email_limit)
    ip_limit = max(1, settings.verification_code_ip_limit)
    cooldown = max(0, settings.verification_code_resend_cooldown_seconds)
    smtp_concurrency = max(1, settings.verification_code_smtp_concurrency)

    with _state_lock:
        _cleanup_locked(now)

        email_bucket = _email_attempts.get(email)
        ip_bucket = _ip_attempts.get(client_ip)
        if email_bucket and cooldown and now - email_bucket[-1] < cooldown:
            raise VerificationCodeRateLimitError(
                ceil(email_bucket[-1] + cooldown - now)
            )
        if email_bucket and len(email_bucket) >= email_limit:
            raise VerificationCodeRateLimitError(
                ceil(email_bucket[0] + window - now)
            )
        if ip_bucket and len(ip_bucket) >= ip_limit:
            raise VerificationCodeRateLimitError(
                ceil(ip_bucket[0] + window - now)
            )

        # The same bound covers both outstanding codes and rate-limit state,
        # so rotating random email/IP values cannot grow either store forever.
        if len(_attempts) >= capacity:
            raise VerificationCodeCapacityError(
                "Verification code service is at capacity"
            )
        if (
            email not in _codes
            and email not in _pending_emails
            and len(_codes) + len(_pending_emails) >= capacity
        ):
            raise VerificationCodeCapacityError(
                "Verification code service is at capacity"
            )
        if _smtp_in_flight >= smtp_concurrency:
            raise VerificationCodeCapacityError(
                "Verification code delivery is busy"
            )

        _attempts.append((now, email, client_ip))
        _email_attempts[email].append(now)
        _ip_attempts[client_ip].append(now)
        _pending_emails.add(email)
        _smtp_in_flight += 1


def send_verification_code(email: str, client_ip: str = "") -> bool:
    global _smtp_in_flight

    if not (
        settings.smtp_host
        and settings.smtp_port
        and settings.smtp_user
        and settings.smtp_password
    ):
        logger.error(
            "SMTP is not configured; set SMTP_USER and SMTP_PASSWORD "
            "before enabling email registration"
        )
        return False

    normalised_email = _normalise_email(email)
    normalised_ip = client_ip.strip() or "<unknown>"
    _reserve_send(normalised_email, normalised_ip, time.time())

    try:
        code = f"{secrets.randbelow(1_000_000):06d}"

        subject = "CCM 注册验证码"
        body = f"您的验证码是：{code}\n\n有效期 5 分钟。\n\n— Claude Code Manager"

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        sender = settings.smtp_from or settings.smtp_user
        msg["From"] = sender
        msg["To"] = email

        with smtplib.SMTP_SSL(
            settings.smtp_host,
            settings.smtp_port,
            timeout=10,
        ) as server:
            server.login(settings.smtp_user, settings.smtp_password)
            server.sendmail(sender, [email], msg.as_string())
        with _state_lock:
            _cleanup_locked(time.time())
            _codes[normalised_email] = (
                code,
                time.time() + CODE_EXPIRE_SECONDS,
            )
        logger.info("Verification code sent to %s", email)
        return True
    except Exception:
        logger.exception("Failed to send verification code to %s", email)
        return False
    finally:
        with _state_lock:
            _pending_emails.discard(normalised_email)
            _smtp_in_flight = max(0, _smtp_in_flight - 1)


def verify_code(email: str, code: str) -> bool:
    now = time.time()
    normalised_email = _normalise_email(email)
    with _state_lock:
        _cleanup_locked(now)
        entry = _codes.get(normalised_email)
        if not entry:
            return False
        stored_code, _ = entry
        if not secrets.compare_digest(stored_code, code):
            return False
        _codes.pop(normalised_email, None)
        return True


def _reset_state_for_tests() -> None:
    """Clear process-local state; test-only helper kept private."""
    global _smtp_in_flight

    with _state_lock:
        _codes.clear()
        _attempts.clear()
        _email_attempts.clear()
        _ip_attempts.clear()
        _pending_emails.clear()
        _smtp_in_flight = 0
