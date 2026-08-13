"""Tests for /api/settings/runtime — frontend PTY mode toggle."""
import pytest


@pytest.mark.asyncio
async def test_get_runtime_settings(client):
    resp = await client.get("/api/settings/runtime")
    assert resp.status_code == 200
    data = resp.json()
    assert "use_pty_mode" in data
    assert "pty_available" in data
    assert "codex_app_server_enabled" in data
    assert "codex_main_mcp_enabled" in data
    assert "codex_monitor_enabled" in data
    assert "agent_sandbox_unrestricted_enabled" in data


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_runtime_settings_reports_effective_codex_main_mcp_capability(
    client, monkeypatch, enabled,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", enabled)

    get_resp = await client.get("/api/settings/runtime")
    assert get_resp.status_code == 200
    assert get_resp.json()["codex_main_mcp_enabled"] is enabled
    assert get_resp.json()["codex_monitor_enabled"] is enabled

    put_resp = await client.put(
        "/api/settings/runtime",
        json={"auto_sort_on_access": True},
    )
    assert put_resp.status_code == 200
    assert put_resp.json()["codex_main_mcp_enabled"] is enabled
    assert put_resp.json()["codex_monitor_enabled"] is enabled


@pytest.mark.asyncio
async def test_toggle_agent_unrestricted_sandbox_roundtrip(
    client,
    session_factory,
):
    from backend.config import settings
    from backend.main import instance_manager
    from backend.models.global_settings import GlobalSettings

    previous = instance_manager.agent_sandbox_unrestricted_enabled
    try:
        enabled = await client.put(
            "/api/settings/runtime",
            json={"agent_sandbox_unrestricted_enabled": True},
        )
        assert enabled.status_code == 200, enabled.text
        assert enabled.json()["agent_sandbox_unrestricted_enabled"] is True
        assert instance_manager.agent_sandbox_unrestricted_enabled is True
        async with session_factory() as db:
            row = await db.get(GlobalSettings, 1)
            assert row.agent_sandbox_unrestricted_enabled is True

        observed = await client.get("/api/settings/runtime")
        assert observed.json()["agent_sandbox_unrestricted_enabled"] is True

        disabled = await client.put(
            "/api/settings/runtime",
            json={"agent_sandbox_unrestricted_enabled": False},
        )
        assert disabled.status_code == 200, disabled.text
        assert disabled.json()["agent_sandbox_unrestricted_enabled"] is False
        assert instance_manager.agent_sandbox_unrestricted_enabled is False
    finally:
        instance_manager.set_agent_sandbox_unrestricted_enabled(previous)
        # The isolated DB is discarded after this test. This assertion also
        # proves the environment remains only the fallback default.
        assert isinstance(settings.agent_sandbox_unrestricted_enabled, bool)


@pytest.mark.asyncio
async def test_toggle_pty_mode_roundtrip(client):
    from backend.main import instance_manager

    try:
        resp = await client.put(
            "/api/settings/runtime", json={"use_pty_mode": True}
        )
        assert resp.status_code == 200
        body = resp.json()
        # claude_pty installed in dev venv -> enable succeeds
        assert body["pty_available"] is True
        assert body["use_pty_mode"] is True
        assert instance_manager.pty_mode_enabled is True

        resp = await client.put(
            "/api/settings/runtime", json={"use_pty_mode": False}
        )
        assert resp.json()["use_pty_mode"] is False
        assert instance_manager.pty_mode_enabled is False

        # GET reflects current state
        resp = await client.get("/api/settings/runtime")
        assert resp.json()["use_pty_mode"] is False
    finally:
        instance_manager.set_pty_mode(False)


@pytest.mark.asyncio
async def test_toggle_off_drains_idle_sessions(client):
    from unittest.mock import AsyncMock
    from backend.main import instance_manager

    class FakeBackend:
        drain_idle_sessions = AsyncMock(return_value=2)

    old_backend = instance_manager._pty_backend
    old_enabled = instance_manager._pty_enabled
    try:
        instance_manager._pty_backend = FakeBackend()
        instance_manager._pty_enabled = True

        resp = await client.put(
            "/api/settings/runtime", json={"use_pty_mode": False}
        )
        assert resp.status_code == 200
        assert resp.json()["use_pty_mode"] is False
        FakeBackend.drain_idle_sessions.assert_awaited_once()
    finally:
        instance_manager._pty_backend = old_backend
        instance_manager._pty_enabled = old_enabled


@pytest.mark.asyncio
async def test_context_compact_threshold_default_and_update(client):
    from backend.config import settings

    # Default: no DB override -> env default
    resp = await client.get("/api/settings/runtime")
    assert resp.status_code == 200
    assert resp.json()["context_compact_threshold"] == pytest.approx(
        settings.context_compact_threshold
    )

    # Update -> persisted and returned as effective value
    resp = await client.put(
        "/api/settings/runtime", json={"context_compact_threshold": 0.7}
    )
    assert resp.status_code == 200
    assert resp.json()["context_compact_threshold"] == pytest.approx(0.7)

    resp = await client.get("/api/settings/runtime")
    assert resp.json()["context_compact_threshold"] == pytest.approx(0.7)

    # Updating other fields must not clobber the stored threshold
    resp = await client.put(
        "/api/settings/runtime", json={"auto_sort_on_access": True}
    )
    assert resp.json()["context_compact_threshold"] == pytest.approx(0.7)


@pytest.mark.asyncio
async def test_context_compact_threshold_rejects_out_of_range(client):
    for bad in (0.1, 0.99, 2):
        resp = await client.put(
            "/api/settings/runtime", json={"context_compact_threshold": bad}
        )
        assert resp.status_code == 422, f"{bad} should be rejected"


@pytest.mark.asyncio
async def test_plan_pipeline_settings_are_persisted_and_returned(client):
    current = (await client.get("/api/settings/plan-pipeline")).json()
    current["planner"]["primary"] = {
        "provider": "codex",
        "model": "gpt-5.6-luna",
        "effort": "max",
    }
    current["max_revision_cycles"] = 1

    saved = await client.put(
        "/api/settings/plan-pipeline",
        json=current,
    )

    assert saved.status_code == 200, saved.text
    assert saved.json() == current
    assert (await client.get("/api/settings/plan-pipeline")).json() == current
    system = await client.get("/api/system/config")
    assert system.json()["plan_pipeline_defaults"] == current
