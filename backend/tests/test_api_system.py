"""Tests for System API endpoints."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.models.instance import Instance


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/api/system/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "commit" in body  # Manager/Worker 版本锁定校验用


@pytest.mark.asyncio
async def test_stats_empty(client):
    resp = await client.get("/api/system/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["tasks"]["pending"] == 0
    assert data["tasks"]["completed"] == 0
    assert data["running_instances"] == 0


@pytest.mark.asyncio
async def test_stats_with_tasks(client, session_factory):
    # Create tasks in various statuses
    await client.post("/api/tasks", json={"title": "A", "description": "d", "target_repo": "/tmp"})
    await client.post("/api/tasks", json={"title": "B", "description": "d", "target_repo": "/tmp"})
    create3 = await client.post("/api/tasks", json={"title": "C", "description": "d", "target_repo": "/tmp"})
    # Cancel one to change its status
    await client.post(f"/api/tasks/{create3.json()['id']}/cancel")

    resp = await client.get("/api/system/stats")
    data = resp.json()
    assert data["tasks"]["pending"] == 2


@pytest.mark.asyncio
async def test_stats_running_instances(client, session_factory):
    # Create an instance with status="running"
    async with session_factory() as db:
        inst = Instance(name="worker-test", status="running")
        db.add(inst)
        await db.commit()

    resp = await client.get("/api/system/stats")
    data = resp.json()
    assert data["running_instances"] >= 1


# === /api/system/update tests ===


@pytest.mark.asyncio
async def test_update_dry_run_forwards_force_and_branch(client, monkeypatch):
    service = MagicMock()
    service.dry_run = AsyncMock(return_value={"has_updates": False})
    monkeypatch.setattr("backend.main.update_service", service)

    resp = await client.post(
        "/api/system/update",
        json={"dry_run": True, "force": True, "branch": "release/test"},
    )

    assert resp.status_code == 200
    assert resp.json() == {"has_updates": False}
    service.dry_run.assert_awaited_once_with(branch="release/test", force=True)


@pytest.mark.asyncio
async def test_update_returns_conflict_when_active_tasks_block_start(client, monkeypatch):
    service = MagicMock()
    service.start_update = AsyncMock(return_value={
        "error": "当前有 1 个任务正在运行，请等待任务完成后再更新",
        "update_blocked": True,
    })
    monkeypatch.setattr("backend.main.update_service", service)

    resp = await client.post(
        "/api/system/update",
        json={"force": True, "branch": "main"},
    )

    assert resp.status_code == 409
    assert "当前有 1 个任务正在运行" in resp.json()["detail"]
    service.start_update.assert_awaited_once_with(
        skip_frontend_build=False,
        force=True,
        branch="main",
    )


@pytest.mark.asyncio
async def test_restart_and_repair_endpoints_delegate(client, monkeypatch):
    service = MagicMock()
    service.restart = AsyncMock(return_value={"status": "started"})
    service.start_repair = AsyncMock(return_value={"status": "started"})
    service.reconcile_blockers = AsyncMock(
        return_value={
            "reconciled": True,
            "update_blocked": False,
            "active_task_count": 0,
            "active_tasks": [],
        }
    )
    monkeypatch.setattr("backend.main.update_service", service)

    restart = await client.post("/api/system/restart")
    repair = await client.post("/api/system/update/repair", json={})
    reconcile = await client.post("/api/system/update/reconcile")

    assert restart.status_code == 200
    assert repair.status_code == 200
    assert reconcile.status_code == 200
    assert reconcile.json()["reconciled"] is True
    service.restart.assert_awaited_once()
    service.start_repair.assert_awaited_once_with(skip_frontend_build=False)
    service.reconcile_blockers.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconcile_endpoint_returns_structured_conflict(
    client, monkeypatch,
):
    service = MagicMock()
    service.reconcile_blockers = AsyncMock(
        return_value={
            "error": "无法安全核对",
            "reconciled": False,
            "update_blocked": True,
            "active_task_count": 0,
            "active_tasks": [],
        }
    )
    monkeypatch.setattr("backend.main.update_service", service)

    response = await client.post("/api/system/update/reconcile")

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "error": "无法安全核对",
        "reconciled": False,
        "update_blocked": True,
        "active_task_count": 0,
        "active_tasks": [],
    }


@pytest.mark.asyncio
async def test_rollback_confirmation_is_structured_conflict(client, monkeypatch):
    service = MagicMock()
    service.rollback = AsyncMock(
        return_value={
            "error": "database restore confirmation required",
            "confirmation_required": True,
            "database_restore_required": True,
        }
    )
    monkeypatch.setattr("backend.main.update_service", service)

    response = await client.post("/api/system/update/rollback", json={})

    assert response.status_code == 409
    assert response.json()["detail"]["confirmation_required"] is True
    service.rollback.assert_awaited_once_with(
        confirm_database_restore=False
    )


# === /api/system/config tests ===


@pytest.mark.asyncio
async def test_config_returns_default_model(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "default_model" in data
    assert isinstance(data["default_model"], str)
    assert len(data["default_model"]) > 0


@pytest.mark.asyncio
async def test_config_ships_codex_sol_as_default(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert data["default_provider"] == "codex"
    assert data["default_codex_model"] == "gpt-5.6-sol"
    assert "gpt-5.6-sol" in data["codex_model_options"]


@pytest.mark.asyncio
async def test_config_returns_two_stage_plan_pipeline_defaults(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    pipeline = resp.json()["plan_pipeline_defaults"]

    assert pipeline == {
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 2,
        "max_interactions": 3,
    }


@pytest.mark.asyncio
async def test_config_returns_codex_service_tier_capabilities(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()

    assert data["default_codex_service_tier"] == "default"
    assert data["codex_service_tier_options"] == ["default", "priority"]
    tiers = data["codex_model_service_tiers"]
    assert tiers["gpt-5.6-sol"] == ["default", "priority"]
    assert tiers["gpt-5.4"] == ["default", "priority"]
    assert tiers["gpt-5.4-mini"] == ["default"]
    assert tiers["gpt-5.3-codex-spark"] == ["default"]


@pytest.mark.asyncio
async def test_config_returns_model_options_list(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "model_options" in data
    assert isinstance(data["model_options"], list)
    assert len(data["model_options"]) > 0


@pytest.mark.asyncio
async def test_config_model_options_no_empty_strings(client):
    """model_options should not contain empty strings."""
    resp = await client.get("/api/system/config")
    for opt in resp.json()["model_options"]:
        assert opt.strip() != ""


@pytest.mark.asyncio
async def test_config_default_model_options_include_1m_variants(client):
    """The shipped default model_options should include 1m variants."""
    resp = await client.get("/api/system/config")
    options = resp.json()["model_options"]
    assert "claude-opus-4-6[1m]" in options
    assert "claude-sonnet-4-6[1m]" in options


@pytest.mark.asyncio
async def test_config_includes_opus5_capabilities(client):
    resp = await client.get("/api/system/config")
    data = resp.json()

    assert "claude-opus-5" in data["model_options"]
    assert "claude-opus-5[1m]" not in data["model_options"]
    assert data["claude_model_context_windows"]["claude-opus-5"] == 1_000_000
    assert data["claude_model_efforts"]["claude-opus-5"] == [
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]


@pytest.mark.asyncio
async def test_config_reflects_settings(client):
    from unittest.mock import patch
    from backend.config import settings

    with patch.object(settings, "default_model", "haiku"), \
         patch.object(settings, "model_options", "haiku,sonnet"):
        resp = await client.get("/api/system/config")
    data = resp.json()
    assert data["default_model"] == "haiku"
    assert data["model_options"] == ["haiku", "sonnet"]


# === Effort config tests ===


@pytest.mark.asyncio
async def test_config_returns_effort_fields(client):
    """Config endpoint returns default_effort and effort_options."""
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "default_effort" in data
    assert data["default_effort"] == "medium"
    assert "effort_options" in data
    assert isinstance(data["effort_options"], list)
    assert "low" in data["effort_options"]
    assert "high" in data["effort_options"]
    assert "max" in data["effort_options"]


@pytest.mark.asyncio
async def test_config_effort_reflects_settings(client):
    """Effort config reflects overridden settings."""
    from unittest.mock import patch
    from backend.config import settings

    with patch.object(settings, "default_effort", "high"), \
         patch.object(settings, "effort_options", "low,high"):
        resp = await client.get("/api/system/config")
    data = resp.json()
    assert data["default_effort"] == "high"
    assert data["effort_options"] == ["low", "high"]
