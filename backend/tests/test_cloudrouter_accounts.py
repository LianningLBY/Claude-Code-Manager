import asyncio
import json
import os
import stat
import sys
import tomllib
import types
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi import HTTPException
from pydantic import SecretStr
from starlette.requests import Request

import backend.services.cloudrouter_accounts as cloudrouter_module
import backend.api.cloudrouter_accounts as cloudrouter_api
from backend.services.cloudrouter_accounts import (
    APEX_CODEX_BASE_URL,
    APEX_MODELS_URL,
    APEX_USAGE_URL,
    CLAUDE_BASE_URL,
    CODEX_BASE_URL,
    MAX_API_RESPONSE_BYTES,
    CloudRouterAccountNotFound,
    CloudRouterAccountStore,
    CloudRouterUnsafePathError,
    CloudRouterUpstreamError,
)


MODELS = {
    "claude": ["claude-opus-4-8", "claude-sonnet-5"],
    "codex": ["gpt-5.4", "gpt-5.5"],
}


def test_api_auth_kind_is_limited_to_registered_gateways():
    assert cloudrouter_module.is_api_auth_kind("cloudrouter_api")
    assert cloudrouter_module.is_api_auth_kind("apex_api")
    assert not cloudrouter_module.is_api_auth_kind("legacy_api")
    assert not cloudrouter_module.is_api_auth_kind("oauth")


async def _add(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    models: dict[str, list[str]] | None = None,
) -> tuple[CloudRouterAccountStore, object]:
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store, "probe_models", AsyncMock(return_value=models or MODELS),
    )
    return store, await store.add_account("Primary API", "cr-secret-value")


def _permissions(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _admin_request() -> Request:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.user_role = "admin"
    return request


@pytest.mark.asyncio
async def test_add_builds_private_dual_cli_home_without_leaking_key(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    root = account.root

    assert account.id == "cloudrouter-1"
    assert account.providers == ["claude", "codex"]
    assert _permissions(store.root) == 0o700
    assert _permissions(root) == 0o700
    assert _permissions(root / "claude") == 0o700
    assert _permissions(root / "codex") == 0o700
    assert _permissions(root / "account.json") == 0o600
    assert _permissions(root / "api.key") == 0o600
    assert _permissions(root / "key-helper") == 0o700

    metadata = json.loads((root / "account.json").read_text())
    assert metadata["models"] == MODELS
    assert metadata["endpoints"]["claude_base_url"] == CLAUDE_BASE_URL
    assert metadata["endpoints"]["codex_base_url"] == CODEX_BASE_URL
    assert "cr-secret-value" not in json.dumps(metadata)

    settings_text = (root / "claude" / "settings.json").read_text()
    settings = json.loads(settings_text)
    assert settings["env"] == {"ANTHROPIC_BASE_URL": CLAUDE_BASE_URL}
    assert "/home/sandbox/.ccm-api-account/key-helper" in settings["apiKeyHelper"]
    assert str(root / "key-helper") in settings["apiKeyHelper"]
    assert settings["skipDangerousModePermissionPrompt"] is True
    assert "model" not in settings
    assert json.loads((root / "claude" / ".claude.json").read_text()) == {
        "hasCompletedOnboarding": True,
    }

    codex_config = (root / "codex" / "config.toml").read_text()
    assert 'model_provider = "cloudrouter"' in codex_config
    assert f'base_url = "{CODEX_BASE_URL}"' in codex_config
    assert 'wire_api = "responses"' in codex_config
    assert "supports_websockets = false" in codex_config
    assert "[model_providers.cloudrouter.auth]" in codex_config
    assert str(root / "key-helper") in codex_config
    assert "\nmodel =" not in codex_config
    assert "cr-secret-value" not in settings_text + codex_config

    helper_output = os.popen(str(root / "key-helper")).read()
    assert helper_output == "cr-secret-value"


@pytest.mark.asyncio
async def test_add_apex_builds_private_codex_only_home_without_leaking_key(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": ["claude-opus-4-8"],
            "codex": ["gpt-5.4"],
        }),
    )

    account = await store.add_account(
        "Apex primary",
        "lck-test-secret",
        api_provider="apex",
    )
    root = account.root

    assert account.id == "apex-1"
    assert account.api_provider == "apex"
    assert account.auth_kind == "apex_api"
    assert account.providers == ["codex"]
    assert account.models == {"claude": [], "codex": ["gpt-5.4"]}
    assert not (root / "claude" / "settings.json").exists()
    assert not (root / "claude" / ".claude.json").exists()

    metadata = json.loads((root / "account.json").read_text())
    assert metadata["api_provider"] == "apex"
    assert metadata["endpoints"]["codex_base_url"] == APEX_CODEX_BASE_URL
    assert metadata["endpoints"]["usage_url"] == APEX_USAGE_URL
    assert "lck-test-secret" not in json.dumps(metadata)

    codex_config = (root / "codex" / "config.toml").read_text()
    assert 'model_provider = "apexrouter"' in codex_config
    assert "[model_providers.apexrouter]" in codex_config
    assert 'name = "ApexRouter"' in codex_config
    assert f'base_url = "{APEX_CODEX_BASE_URL}"' in codex_config
    assert "[model_providers.apexrouter.auth]" in codex_config
    assert "[model_providers.apex_gateway]" in codex_config
    assert "[model_providers.apex_gateway.auth]" in codex_config
    assert str(root / "key-helper") in codex_config
    assert "lck-test-secret" not in codex_config
    assert os.popen(str(root / "key-helper")).read() == "lck-test-secret"


@pytest.mark.asyncio
async def test_legacy_apex_gateway_config_migrates_with_resume_alias(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    helper = account.root / "key-helper"
    config = account.root / "codex" / "config.toml"
    config.write_text(
        'model_provider = "apex_gateway"\n'
        'personality = "pragmatic"\n\n'
        "[model_providers.apex_gateway]\n"
        'name = "Apex Gateway"\n'
        f'base_url = "{APEX_CODEX_BASE_URL}"\n'
        'wire_api = "responses"\n'
        "supports_websockets = false\n\n"
        "[model_providers.apex_gateway.auth]\n"
        f'command = "{helper}"\n'
        "timeout_ms = 5000\n"
        "refresh_interval_ms = 0\n\n"
        '[projects."/tmp/project"]\n'
        'trust_level = "trusted"\n',
    )
    os.chmod(config, 0o600)

    assert [item.id for item in store.reload()] == [account.id]
    migrated = tomllib.loads(config.read_text())
    assert migrated["model_provider"] == "apexrouter"
    assert migrated["personality"] == "pragmatic"
    assert "projects" not in migrated
    assert set(migrated["model_providers"]) == {
        "apexrouter",
        "apex_gateway",
    }
    assert (
        migrated["model_providers"]["apexrouter"]
        == migrated["model_providers"]["apex_gateway"]
    )
    assert migrated["model_providers"]["apexrouter"]["name"] == "ApexRouter"


@pytest.mark.asyncio
async def test_apex_resume_alias_tampering_fails_closed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            "[model_providers.apex_gateway]\n"
            'name = "ApexRouter"\n'
            f'base_url = "{APEX_CODEX_BASE_URL}"',
            "[model_providers.apex_gateway]\n"
            'name = "ApexRouter"\n'
            'base_url = "https://attacker.invalid/v1"',
        ),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Codex API routing",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_apex_usage_separates_key_usage_from_shared_group_quota(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "key_name": "test-key",
            "group_name": "apex-research",
            "used": {
                "requests_5h": 0,
                "requests_day": 0,
                "tokens_day": 0,
                "tokens_month": 0,
            },
            "remaining": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
            },
            "limits": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
                "concurrency": 20,
            },
        }),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex"
    )
    request = AsyncMock(return_value={
        "key_name": "test-key",
        "group_name": "apex-research",
        "used": {
            "requests_5h": 3,
            "requests_day": 7,
            "tokens_day": 1_000,
            "tokens_month": 2_000,
        },
        "remaining": {
            "requests_5h": 24_000,
            "requests_day": 49_000,
            "tokens_day": 9_000_000,
            "tokens_month": 90_000_000,
        },
        "limits": {
            "requests_5h": 25_000,
            "requests_day": 50_000,
            "tokens_day": 10_000_000,
            "tokens_month": 100_000_000,
            "concurrency": 20,
        },
    })
    monkeypatch.setattr(store, "_request_json", request)

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["known"] is True
    assert snapshot["available"] is True
    assert snapshot["mode"] == "shared_group"
    assert snapshot["key_name"] == "test-key"
    assert snapshot["group_name"] == "apex-research"
    assert snapshot["concurrency"] == 20
    assert snapshot["key_usage"]["requests_5h"] == 3
    assert snapshot["windows"][0]["used"] == 1_000
    assert snapshot["windows"][0]["remaining"] == 24_000
    assert snapshot["windows"][0]["limit"] == 25_000
    assert snapshot["windows"][0]["key_used"] == 3
    assert snapshot["windows"][0]["scope"] == "group"
    assert store.cached_quota_decision(account.id) == {
        "available": True,
        "known": True,
        "reason": "active",
    }
    request.assert_awaited_once_with(
        APEX_USAGE_URL,
        "lck-test-secret",
    )


@pytest.mark.asyncio
async def test_partial_apex_group_usage_cannot_replace_known_exhaustion(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex"
    )
    store._quota_cache[account.id] = {
        "account_id": account.id,
        "known": True,
        "available": False,
        "state": "exhausted",
        "reason": "exhausted",
    }
    store._quota_cached_at[account.id] = 1
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "used": {
                "requests_5h": 3,
                "requests_day": 7,
                "tokens_day": 1_000,
                "tokens_month": 2_000,
            },
            # A partial response cannot prove that the shared group is usable.
            "remaining": {},
            "limits": {"concurrency": 20},
        }),
    )

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["known"] is False
    assert snapshot["last_known_available"] is False
    assert snapshot["reason"] == "invalid_usage_response"
    assert store.cached_quota_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "exhausted",
    }


@pytest.mark.asyncio
async def test_models_gate_each_provider_independently(tmp_path, monkeypatch):
    store, account = await _add(
        tmp_path, monkeypatch,
        models={"claude": ["claude-opus-4-8"], "codex": []},
    )

    assert account.providers == ["claude"]
    assert account.supports_model("claude", None)
    assert account.supports_model("claude", "default")
    assert account.supports_model("claude", "claude-opus-4-8[1m]")
    assert not account.supports_model("claude", "claude-sonnet-5")
    assert not account.supports_model("codex", None)
    assert store.account_for_claude_config_dir(account.claude_config_dir) == account
    assert store.account_for_codex_home(account.codex_home) == account
    assert store.account_for_runtime_home(account.codex_home) == account
    assert store.account_for_runtime_home(account.root) is None


@pytest.mark.asyncio
async def test_claude_short_alias_matches_only_exact_dated_model(
    tmp_path, monkeypatch,
):
    _store, account = await _add(
        tmp_path,
        monkeypatch,
        models={
            "claude": ["claude-haiku-4-5-20251001"],
            "codex": [],
        },
    )

    assert account.supports_model("claude", "claude-haiku-4-5")
    assert account.supports_model("claude", "claude-haiku-4-5[1m]")
    assert not account.supports_model("claude", "claude-haiku-4")
    assert not account.supports_model("claude", "claude-haiku-4-5-fast")


@pytest.mark.asyncio
async def test_account_numbers_do_not_reuse_retired_folders(tmp_path, monkeypatch):
    store, first = await _add(tmp_path, monkeypatch)
    await store.retire_account(first.id)
    monkeypatch.setattr(store, "probe_models", AsyncMock(return_value=MODELS))
    second = await store.add_account("Second", "cr-second")

    assert second.id == "cloudrouter-2"
    assert [item.id for item in store.all_accounts()] == ["cloudrouter-2"]
    assert [item.id for item in store.all_accounts(include_retired=True)] == [
        "cloudrouter-1", "cloudrouter-2",
    ]


@pytest.mark.asyncio
async def test_retire_clears_credentials_and_config_but_preserves_sessions(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    claude_project = account.root / "claude" / "projects" / "p" / "history.jsonl"
    claude_project.parent.mkdir(parents=True)
    claude_project.write_text("history")
    (account.root / "claude" / "plugins").mkdir()
    (account.root / "claude" / "plugins" / "state").write_text("state")
    codex_session = account.root / "codex" / "sessions" / "rollout.jsonl"
    codex_session.parent.mkdir()
    codex_session.write_text("session")
    (account.root / "codex" / "history.jsonl").write_text("history")

    retired = await store.retire_account(account.id)

    assert retired.retired
    assert not retired.enabled
    assert not (account.root / "api.key").exists()
    assert not (account.root / "key-helper").exists()
    assert claude_project.read_text() == "history"
    assert codex_session.read_text() == "session"
    assert not (account.root / "claude" / "settings.json").exists()
    assert not (account.root / "claude" / "plugins").exists()
    assert not (account.root / "codex" / "config.toml").exists()
    assert not (account.root / "codex" / "history.jsonl").exists()
    metadata = json.loads((account.root / "account.json").read_text())
    assert metadata["retired"] is True
    assert metadata["enabled"] is False
    assert metadata["cleanup_pending"] is False
    assert "cr-secret-value" not in json.dumps(metadata)
    assert await store.retire_account(account.id) == retired


@pytest.mark.asyncio
async def test_failed_retirement_is_disabled_and_idempotently_resumable(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    original = store._remove_except
    monkeypatch.setattr(
        store,
        "_remove_except",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("interrupted")),
    )

    with pytest.raises(RuntimeError, match="interrupted"):
        await store.retire_account(account.id)

    pending = store.account(account.id)
    assert pending is not None
    assert pending.retired is True
    assert pending.cleanup_pending is True
    assert store.cached_quota_decision(account.id) == {
        "available": False, "known": True, "reason": "disabled",
    }

    monkeypatch.setattr(store, "_remove_except", original)
    completed = await store.retire_account(account.id)
    assert completed.retired is True
    assert completed.cleanup_pending is False
    assert not (account.root / "api.key").exists()


@pytest.mark.asyncio
async def test_usage_quota_exhaustion_is_known_unavailable_and_cached(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    request = AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "isValid": True,
        "quota": {"limit": 100, "used": 100, "remaining": 0},
        "rate_limits": [{
            "window": "7d", "used": 100, "limit": 100,
            "reset_at": "2026-08-01T00:00:00Z",
        }],
        "usage": {
            "today": {"requests": 4, "input_tokens": 10, "actual_cost": 1.25},
        },
    })
    monkeypatch.setattr(store, "_request_json", request)

    snapshot = await store.fetch_usage(account.id)
    again = await store.fetch_usage(account.id)

    assert snapshot["available"] is False
    assert snapshot["known"] is True
    assert snapshot["reason"] == "quota_exhausted"
    assert snapshot["state"] == "exhausted"
    assert snapshot["currency"] == "USD"
    assert snapshot["quota"]["remaining"] == 0
    assert snapshot["windows"][0]["reset_at"] == "2026-08-01T00:00:00Z"
    assert snapshot["usage"]["today"]["actual_cost"] == 1.25
    assert store.cached_quota_decision(account.id) == {
        "available": False, "known": True, "reason": "quota_exhausted",
    }
    assert again == snapshot
    request.assert_awaited_once()


@pytest.mark.asyncio
async def test_subscription_usage_preserves_credit_units(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "subscription",
        "status": "active",
        "subscription": {
            "daily_usage_credits": 3,
            "daily_limit_credits": 10,
            "weekly_usage_credits": 8,
            "weekly_limit_credits": 50,
        },
        "balance": 25,
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["available"] is True
    assert snapshot["known"] is True
    assert snapshot["unit"] == "credits"
    assert [window["currency"] for window in snapshot["windows"]] == [
        "credits", "credits",
    ]
    assert snapshot["windows"][0]["remaining"] == 7
    assert snapshot["windows"][0]["utilization"] == 30.0


@pytest.mark.asyncio
async def test_subscription_usd_window_and_expiry_are_normalised(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "subscription",
        "status": "active",
        "subscription": {
            "planName": "API Pro",
            "daily_usage_usd": "1.25",
            "daily_limit_usd": "5.00",
            "weekly_usage_usd": "20.00",
            "weekly_limit_usd": "20.00",
            "expiry": "2026-09-01T00:00:00Z",
            "daysUntilExpiry": 39,
        },
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["state"] == "exhausted"
    assert snapshot["currency"] == "USD"
    assert snapshot["plan_name"] == "API Pro"
    assert snapshot["expires_at"] == "2026-09-01T00:00:00Z"
    assert snapshot["days_until_expiry"] == 39
    assert snapshot["windows"][0]["remaining"] == 3.75
    assert snapshot["windows"][1]["remaining"] == 0


@pytest.mark.asyncio
async def test_wallet_negative_one_remaining_means_unlimited(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "remaining": -1,
        "balance": 5,
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["state"] == "active"
    assert snapshot["remaining"] == -1
    assert snapshot["available"] is True


@pytest.mark.asyncio
async def test_wallet_negative_balance_other_than_unlimited_is_exhausted(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": -0.5,
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["balance"] == -0.5
    assert snapshot["state"] == "exhausted"
    assert snapshot["available"] is False


@pytest.mark.asyncio
async def test_nested_negative_one_remaining_is_unlimited_not_exhausted(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "active",
        "quota": {"limit": 100, "used": 100, "remaining": -1},
        "rate_limits": [{
            "window": "7d",
            "used": 100,
            "limit": 100,
            "remaining": -1,
        }],
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["state"] == "active"
    assert snapshot["available"] is True
    assert snapshot["quota"]["unlimited"] is True
    assert snapshot["windows"][0]["unlimited"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code,reason", [(401, "invalid_api_key"), (403, "forbidden")])
async def test_usage_auth_failure_is_known_unavailable(
    tmp_path, monkeypatch, status_code, reason,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(side_effect=
        CloudRouterUpstreamError(reason, status_code=status_code)
    ))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["status"] == "unavailable"
    assert snapshot["available"] is False
    assert snapshot["known"] is True
    assert snapshot["reason"] == reason


@pytest.mark.asyncio
async def test_timeout_or_5xx_returns_unknown_stale_without_disabling(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    success = AsyncMock(return_value={"mode": "wallet", "status": "active", "balance": 5})
    monkeypatch.setattr(store, "_request_json", success)
    assert (await store.fetch_usage(account.id, force=True))["known"] is True
    monkeypatch.setattr(store, "_request_json", AsyncMock(side_effect=
        CloudRouterUpstreamError("upstream_unavailable", status_code=503)
    ))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["status"] == "unknown"
    assert snapshot["available"] is True
    assert snapshot["known"] is False
    assert snapshot["stale"] is True
    assert snapshot["last_known_available"] is True
    assert store.cached_quota_decision(account.id)["available"] is True


@pytest.mark.asyncio
async def test_unknown_refresh_cannot_resurrect_last_known_dead_key(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "quota": {"limit": 10, "used": 10, "remaining": 0},
    }))
    assert (await store.fetch_usage(account.id, force=True))["available"] is False

    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(side_effect=CloudRouterUpstreamError(
            "upstream_unavailable", status_code=503,
        )),
    )
    first_unknown = await store.fetch_usage(account.id, force=True)
    second_unknown = await store.fetch_usage(account.id, force=True)

    assert first_unknown["known"] is False
    assert first_unknown["available"] is True
    assert first_unknown["last_known_available"] is False
    assert second_unknown["last_known_available"] is False
    assert store.cached_quota_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "quota_exhausted",
    }


@pytest.mark.asyncio
async def test_probe_models_uses_bounded_non_redirecting_request(
    tmp_path, monkeypatch,
):
    captured = {}

    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield json.dumps({
                "data": [{"id": "claude-opus-4-8"}, {"id": "gpt-5.5"}],
            }).encode()

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, method, url, *, headers):
            captured.update({"method": method, "url": url, "headers": headers})
            return Stream()

    monkeypatch.setattr(
        "backend.services.cloudrouter_accounts.httpx.AsyncClient", Client,
    )
    store = CloudRouterAccountStore(tmp_path / "accounts")

    models = await store.probe_models("cr-private")

    assert models == {"claude": ["claude-opus-4-8"], "codex": ["gpt-5.5"]}
    assert captured["follow_redirects"] is False
    assert captured["method"] == "GET"
    assert captured["headers"]["Authorization"] == "Bearer cr-private"


@pytest.mark.asyncio
async def test_apex_model_probe_uses_apex_endpoint_and_never_projects_claude(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    request = AsyncMock(return_value={
        "models": [
            {"slug": "claude-opus-4-8", "supported_in_api": True},
            {"slug": "gpt-5.4", "supported_in_api": True, "visibility": "list"},
            {"slug": "gpt-hidden", "supported_in_api": True, "visibility": "hide"},
            {"slug": "gpt-disabled", "supported_in_api": False},
        ],
    })
    monkeypatch.setattr(store, "_request_json", request)

    models = await store.probe_models(
        "lck-test-secret",
        api_provider="apex",
    )

    assert models == {"claude": [], "codex": ["gpt-5.4"]}
    request.assert_awaited_once_with(
        (
            f"{APEX_MODELS_URL}?client_version="
            f"{cloudrouter_module.APEX_CODEX_CLIENT_VERSION}"
        ),
        "lck-test-secret",
    )


@pytest.mark.asyncio
async def test_model_probe_rejects_unbounded_model_lists(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "models": [
                {"slug": f"gpt-test-{index}"}
                for index in range(cloudrouter_module.MAX_DISCOVERED_MODELS + 1)
            ],
        }),
    )

    with pytest.raises(CloudRouterUpstreamError, match="too_many_models"):
        await store.probe_models("lck-test-secret", api_provider="apex")


@pytest.mark.asyncio
async def test_oversized_model_metadata_never_leaves_a_poisoned_account(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": [
                f"gpt-{index}-{'x' * 400}"
                for index in range(cloudrouter_module.MAX_DISCOVERED_MODELS)
            ],
        }),
    )

    with pytest.raises(CloudRouterUpstreamError, match="metadata_too_large"):
        await store.add_account(
            "Apex", "lck-test-secret", api_provider="apex"
        )

    assert store.all_accounts() == []
    assert not (store.root / "apex-1").exists()
    assert not any(
        child.name.startswith(".apex-1.")
        for child in store.root.iterdir()
    )


@pytest.mark.asyncio
async def test_upstream_response_size_is_bounded(tmp_path, monkeypatch):
    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield b"x" * (MAX_API_RESPONSE_BYTES + 1)

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *_args, **_kwargs):
            return Stream()

    monkeypatch.setattr(
        "backend.services.cloudrouter_accounts.httpx.AsyncClient", Client,
    )
    store = CloudRouterAccountStore(tmp_path / "accounts")

    with pytest.raises(CloudRouterUpstreamError, match="response_too_large"):
        await store.probe_models("cr-private")


@pytest.mark.asyncio
async def test_path_traversal_and_symlink_metadata_fail_closed(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    with pytest.raises(CloudRouterAccountNotFound):
        store.account("../cloudrouter-1")

    metadata = account.root / "account.json"
    outside = tmp_path / "outside.json"
    outside.write_text(metadata.read_text())
    metadata.unlink()
    metadata.symlink_to(outside)

    with pytest.raises(CloudRouterUnsafePathError):
        store.reload()


@pytest.mark.asyncio
async def test_claude_settings_allow_hooks_but_reject_routing_tampering(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    settings_path = account.root / "claude" / "settings.json"
    settings = json.loads(settings_path.read_text())
    settings["hooks"] = {"PreToolUse": []}
    settings_path.write_text(json.dumps(settings))
    assert store.reload()[0].id == account.id

    settings["env"]["ANTHROPIC_BASE_URL"] = "https://attacker.invalid"
    settings_path.write_text(json.dumps(settings))
    with pytest.raises(CloudRouterUnsafePathError, match="Claude API routing"):
        store.reload()


@pytest.mark.asyncio
async def test_reload_migrates_unattended_claude_ack_and_preserves_hooks(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    settings_path = account.root / "claude" / "settings.json"
    settings = json.loads(settings_path.read_text())
    settings.pop("skipDangerousModePermissionPrompt")
    settings["hooks"] = {"PreToolUse": [{"matcher": "AskUserQuestion"}]}
    settings_path.write_text(json.dumps(settings))

    assert store.reload()[0].id == account.id
    migrated = json.loads(settings_path.read_text())
    assert migrated["skipDangerousModePermissionPrompt"] is True
    assert migrated["hooks"] == settings["hooks"]
    assert _permissions(settings_path) == 0o600


@pytest.mark.asyncio
async def test_reload_converges_cli_mutated_claude_json_mode_without_data_loss(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    state_path = account.root / "claude" / ".claude.json"
    state = {
        "hasCompletedOnboarding": True,
        "theme": "dark",
        "cliOwnedState": {"kept": True},
    }
    state_path.write_text(json.dumps(state))
    os.chmod(state_path, 0o664)

    assert store.reload()[0].id == account.id
    assert json.loads(state_path.read_text()) == state
    assert _permissions(state_path) == 0o600


@pytest.mark.asyncio
async def test_runtime_admission_converts_storage_oserror_to_safe_failure(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(
        store,
        "reload",
        Mock(side_effect=OSError("/private/account became read-only")),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="account storage is unavailable",
    ) as captured:
        async with store.runtime_admission(
            "claude",
            account.claude_config_dir,
            "claude-opus-4-8",
        ):
            pass

    assert "/private/" not in str(captured.value)


@pytest.mark.asyncio
async def test_configuration_admission_validates_route_without_quota_gate(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    store._quota_cache[account.id] = {
        "known": True,
        "available": False,
        "reason": "exhausted",
    }

    async with store.configuration_admission(
        "codex", account.codex_home,
    ) as admitted:
        assert admitted.id == account.id

    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            CODEX_BASE_URL,
            "https://attacker.invalid/v1",
        ),
    )
    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        async with store.configuration_admission(
            "codex", account.codex_home,
        ):
            pass


@pytest.mark.asyncio
async def test_codex_provider_and_key_helper_tampering_fail_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(CODEX_BASE_URL, "https://attacker.invalid/v1"),
    )
    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        store.reload()

    config.write_text(
        config.read_text().replace(
            "https://attacker.invalid/v1", CODEX_BASE_URL,
        ),
    )
    helper = account.root / "key-helper"
    helper.write_text(helper.read_text() + "\n# modified\n")
    os.chmod(helper, 0o700)
    with pytest.raises(CloudRouterUnsafePathError, match="credential helper"):
        store.reload()


@pytest.mark.asyncio
async def test_codex_cli_personality_migration_is_allowed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            'model_provider = "apexrouter"\n',
            'model_provider = "apexrouter"\npersonality = "pragmatic"\n',
        )
    )
    os.chmod(config, 0o600)

    assert [item.id for item in store.reload()] == [account.id]
    async with store.runtime_admission(
        "codex", account.codex_home, "gpt-5.5",
    ) as admitted:
        assert admitted.id == account.id


@pytest.mark.asyncio
@pytest.mark.parametrize("trust_level", ["trusted", "untrusted"])
async def test_codex_cli_project_trust_state_is_allowed(
    tmp_path, monkeypatch, trust_level,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    project_root = (tmp_path / "project").absolute()
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            f'\n[projects.{json.dumps(str(project_root))}]\n'
            f'trust_level = "{trust_level}"\n'
        )

    assert [item.id for item in store.reload()] == [account.id]
    migrated = tomllib.loads(config.read_text())
    assert "projects" not in migrated
    assert migrated["model_provider"] == "apexrouter"
    async with store.runtime_admission(
        "codex", account.codex_home, "gpt-5.5",
    ) as admitted:
        assert admitted.id == account.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "project_path, project_config",
    [
        ("relative/project", 'trust_level = "trusted"'),
        ("/tmp/project", 'trust_level = "unknown"'),
        (
            "/tmp/project",
            'trust_level = "trusted"\ncommand = "/tmp/untrusted-command"',
        ),
    ],
)
async def test_modified_codex_project_trust_state_fails_closed(
    tmp_path, monkeypatch, project_path, project_config,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            f'\n[projects.{json.dumps(project_path)}]\n{project_config}\n'
        )

    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        store.reload()


@pytest.mark.asyncio
async def test_codex_project_trust_rewrite_failure_fails_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            '\n[projects."/tmp/project"]\ntrust_level = "trusted"\n'
        )
    monkeypatch.setattr(
        cloudrouter_module,
        "_atomic_private_write",
        Mock(side_effect=OSError("read-only filesystem")),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Could not secure Codex project state",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_unknown_codex_personality_still_fails_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            'model_provider = "cloudrouter"\n',
            'model_provider = "cloudrouter"\npersonality = "injected"\n',
        )
    )
    os.chmod(config, 0o600)

    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        store.reload()


@pytest.mark.asyncio
async def test_apex_codex_provider_tampering_fails_closed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex"
    )
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            APEX_CODEX_BASE_URL,
            "https://attacker.invalid/v1",
        )
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Codex API routing",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_api_codex_extra_persistent_command_config_fails_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            '\n[mcp_servers.injected]\ncommand = "/tmp/untrusted-command"\n'
        )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Codex API routing",
    ):
        store.reload()


def test_store_rejects_symlink_root(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    root = tmp_path / "accounts"
    root.symlink_to(target, target_is_directory=True)

    with pytest.raises(CloudRouterUnsafePathError):
        CloudRouterAccountStore(root)


@pytest.mark.asyncio
async def test_managed_metadata_owner_mismatch_fails_closed(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    real_uid = os.getuid()
    monkeypatch.setattr(cloudrouter_module.os, "getuid", lambda: real_uid + 1)

    with pytest.raises(CloudRouterUnsafePathError, match="another owner"):
        cloudrouter_module._open_regular_nofollow(
            account.root / "account.json", maximum=1024 * 1024,
        )


@pytest.mark.asyncio
async def test_key_helper_rejects_non_private_key_file(tmp_path, monkeypatch):
    _store, account = await _add(tmp_path, monkeypatch)
    os.chmod(account.root / "api.key", 0o640)

    process = await asyncio.create_subprocess_exec(
        str(account.root / "key-helper"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _stderr = await process.communicate()

    assert process.returncode != 0
    assert stdout == b""


def test_runtime_pool_reload_is_deduplicated(monkeypatch):
    class Pool:
        def __init__(self):
            self.calls = 0

        def reload(self):
            self.calls += 1

    claude = Pool()
    codex = Pool()
    monkeypatch.setattr(
        cloudrouter_api, "_runtime_pools", lambda: (claude, codex),
    )
    cloudrouter_api._reload_runtime_pools()
    assert claude.calls == 1
    assert codex.calls == 1

    monkeypatch.setattr(
        cloudrouter_api, "_runtime_pools", lambda: (claude, claude),
    )
    cloudrouter_api._reload_runtime_pools()
    assert claude.calls == 2


@pytest.mark.asyncio
async def test_first_api_account_lazily_creates_both_runtime_pools(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    store, account = await _add(tmp_path, monkeypatch)
    dispatcher = types.SimpleNamespace(pool=None, codex_pool=None)
    manager = types.SimpleNamespace(
        read_codex_rate_limits=AsyncMock(),
    )
    fake_main = types.SimpleNamespace(
        cloudrouter_store=store,
        dispatcher=dispatcher,
        instance_manager=manager,
        codex_pool=None,
    )
    monkeypatch.setitem(sys.modules, "backend.main", fake_main)
    # `import backend.main as runtime` may resolve the package attribute when
    # another full-suite test imported the real module first. Patch both import
    # caches so this test remains order-independent.
    import backend
    monkeypatch.setattr(backend, "main", fake_main, raising=False)
    monkeypatch.setattr(
        "backend.config.settings.pool_config_path",
        str(tmp_path / "missing-claude-pool.json"),
    )
    monkeypatch.setattr(
        "backend.config.settings.codex_pool_config_path",
        str(tmp_path / "missing-codex-pool.json"),
    )
    monkeypatch.setattr("backend.config.settings.pool_enabled", False)
    monkeypatch.setattr("backend.config.settings.codex_pool_enabled", False)

    cloudrouter_api._reload_runtime_pools()

    assert dispatcher.pool.select(
        model="claude-opus-4-8"
    ) == account.claude_config_dir
    assert fake_main.codex_pool.select(
        model="gpt-5.5"
    ) == str(Path(account.codex_home).resolve())
    assert dispatcher.codex_pool is fake_main.codex_pool


@pytest.mark.asyncio
async def test_create_endpoint_returns_public_account_quota_and_reloads_pools(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(store, "probe_models", AsyncMock(return_value=MODELS))
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "wallet", "status": "active", "balance": 10,
    }))
    reload_pools = Mock()
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    monkeypatch.setattr(cloudrouter_api, "_reload_runtime_pools", reload_pools)

    result = await cloudrouter_api.create_account(
        _admin_request(),
        cloudrouter_api.CloudRouterAccountCreate(
            name="API account", api_key=SecretStr("cr-private-value"),
        ),
    )

    assert result["id"] == "cloudrouter-1"
    assert result["supported_models"] == sorted(MODELS["claude"] + MODELS["codex"])
    assert result["api_quota"]["state"] == "active"
    assert "cr-private-value" not in json.dumps(result)
    reload_pools.assert_called_once_with()


@pytest.mark.asyncio
async def test_create_endpoint_accepts_apex_provider_without_exposing_key(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "key_name": "test-key",
            "group_name": "apex-research",
            "used": {
                "requests_5h": 0,
                "requests_day": 0,
                "tokens_day": 0,
                "tokens_month": 0,
            },
            "remaining": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
            },
            "limits": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
                "concurrency": 20,
            },
        }),
    )
    reload_pools = Mock()
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    monkeypatch.setattr(
        cloudrouter_api,
        "_reload_runtime_pools",
        reload_pools,
    )

    result = await cloudrouter_api.create_account(
        _admin_request(),
        cloudrouter_api.CloudRouterAccountCreate(
            name="Apex API",
            api_key=SecretStr("lck-test-secret"),
            api_provider="apex",
        ),
    )

    assert result["id"] == "apex-1"
    assert result["api_provider"] == "apex"
    assert result["auth_kind"] == "apex_api"
    assert result["providers"] == ["codex"]
    assert result["api_quota"]["known"] is True
    assert result["api_quota"]["group_name"] == "apex-research"
    assert "lck-test-secret" not in json.dumps(result)
    reload_pools.assert_called_once_with()


@pytest.mark.asyncio
async def test_delete_endpoint_is_fail_closed_until_all_runtime_users_are_fenced(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    reload_pools = Mock()
    monkeypatch.setattr(cloudrouter_api, "_reload_runtime_pools", reload_pools)

    with pytest.raises(HTTPException) as blocked:
        await cloudrouter_api.retire_account(_admin_request(), account.id)
    assert blocked.value.status_code == 409
    assert "temporarily disabled" in blocked.value.detail
    assert store.account(account.id).retired is False
    reload_pools.assert_not_called()
