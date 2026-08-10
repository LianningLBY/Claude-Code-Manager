"""Deployment-safety regressions for CCM's self-update service."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from backend.api import system as system_api
from backend.config import settings
from backend.services.update_service import (
    STEP_NAMES,
    StepInfo,
    UpdateService,
    UpdateState,
)


def _result(
    stdout: str = "", stderr: str = "", returncode: int = 0
) -> dict:
    return {
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
    }


def _service(tmp_path: Path, port: int = 18999) -> UpdateService:
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    shutil.copyfile(
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "update_migrate.sh",
        scripts_dir / "update_migrate.sh",
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    service = UpdateService(
        broadcaster, port=port, project_dir=str(tmp_path)
    )
    service._status_file = tmp_path / f"status-{port}.json"
    service._journal_file = tmp_path / f"journal-{port}.json"
    service._automatic_rollback_supported = True
    service._lease_file.parent.mkdir(parents=True, exist_ok=True)
    return service


def _state(operation: str = "update") -> UpdateState:
    return UpdateState(
        update_id="test",
        status="running",
        steps=[StepInfo(name=name) for name in STEP_NAMES],
        operation=operation,
    )


@pytest.mark.asyncio
async def test_status_compares_exact_running_and_disk_commit_and_caches(
    tmp_path,
):
    service = _service(tmp_path)
    service.running_commit = "a" * 40

    async def fake_run(cmd, **_kwargs):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result("b" * 40)
        if cmd[-1] == "current":
            return _result("rev1\n")
        if cmd[-1] == "heads":
            return _result("rev1 (head)\n")
        raise AssertionError(cmd)

    with patch.object(service, "_run_cmd", side_effect=fake_run) as run:
        first = await service.get_status()
        second = await service.get_status()

    assert first["running_commit"] == "a" * 40
    assert first["disk_commit"] == "b" * 40
    assert first["needs_restart"] is True
    assert first["db_up_to_date"] is True
    assert second == first
    assert run.call_count == 3, "status polling must reuse the short inspection cache"


def test_alembic_revision_parser_ignores_log_lines():
    output = """
INFO  [alembic.runtime.migration] Context impl SQLiteImpl.
abc123 (head)
warning: something unrelated
def456
"""
    assert UpdateService._parse_alembic_revisions(output) == [
        "abc123",
        "def456",
    ]


@pytest.mark.asyncio
async def test_same_head_database_behind_routes_to_repair(tmp_path):
    service = _service(tmp_path)
    state = _state()
    commit = "a" * 40

    async def fake_run(cmd, **_kwargs):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result(commit)
        if cmd[:2] == ["git", "status"]:
            return _result()
        if cmd[-3:] == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return _result("main")
        if cmd[:2] == ["git", "pull"]:
            return _result()
        raise AssertionError(cmd)

    deployment = {
        "repair_required": True,
        "repair_reasons": ["数据库 revision 落后于代码，需要补跑迁移"],
        "repair_reason_codes": ["database_migration_pending"],
        "needs_restart": False,
    }
    with (
        patch.object(service, "_run_cmd", side_effect=fake_run),
        patch.object(
            service,
            "_deployment_status",
            new=AsyncMock(return_value=deployment),
        ),
        patch.object(service, "_repair_inner", new=AsyncMock()) as repair,
    ):
        await service._pipeline_inner(
            state, skip_frontend_build=False, force=False
        )

    assert state.operation == "repair"
    repair.assert_awaited_once_with(
        state,
        skip_frontend_build=False,
        first_step_completed=True,
    )


@pytest.mark.asyncio
async def test_db_check_runs_after_uv_sync_and_forces_migration_without_diff(
    tmp_path,
):
    service = _service(tmp_path)
    state = _state()
    heads = iter(["a" * 40, "b" * 40])
    order: list[str] = []

    async def fake_run(cmd, **_kwargs):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result(next(heads))
        if cmd[:2] == ["git", "status"]:
            return _result()
        if cmd[-3:] == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return _result("main")
        if cmd[:2] == ["git", "pull"]:
            return _result()
        if cmd[:2] == ["git", "diff"]:
            return _result("backend/example.py\n")
        if cmd == ["uv", "sync"]:
            order.append("uv_sync")
            return _result()
        raise AssertionError(cmd)

    async def database_status():
        order.append("database_check")
        return {
            "current_revisions": ["oldrev"],
            "head_revisions": ["newrev"],
            "current_revision": "oldrev",
            "head_revision": "newrev",
            "up_to_date": False,
            "error": "",
        }

    with (
        patch.object(service, "_run_cmd", side_effect=fake_run),
        patch.object(
            service, "_backup_database", new=AsyncMock(return_value="/tmp/db")
        ),
        patch.object(
            service,
            "_database_revision_status",
            side_effect=database_status,
        ),
        patch.object(service, "_migration_path", new=AsyncMock()) as migrate,
        patch.object(service, "_fast_restart_path", new=AsyncMock()) as fast,
    ):
        await service._pipeline_inner(
            state, skip_frontend_build=False, force=False
        )

    assert order == ["uv_sync", "database_check"]
    assert state.database_migration_required is True
    migrate.assert_awaited_once_with(state)
    fast.assert_not_awaited()


@pytest.mark.asyncio
async def test_migration_file_diff_does_not_migrate_database_already_at_head(
    tmp_path,
):
    service = _service(tmp_path)
    state = _state()
    heads = iter(["a" * 40, "b" * 40])

    async def fake_run(cmd, **_kwargs):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result(next(heads))
        if cmd[:2] == ["git", "status"]:
            return _result()
        if cmd[-3:] == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return _result("main")
        if cmd[:2] == ["git", "pull"]:
            return _result()
        if cmd[:2] == ["git", "diff"]:
            return _result("alembic/versions/new_revision.py\n")
        if cmd == ["uv", "sync"]:
            return _result()
        raise AssertionError(cmd)

    current = {
        "current_revisions": ["rev"],
        "head_revisions": ["rev"],
        "current_revision": "rev",
        "head_revision": "rev",
        "up_to_date": True,
        "error": "",
    }
    with (
        patch.object(service, "_run_cmd", side_effect=fake_run),
        patch.object(
            service, "_backup_database", new=AsyncMock(return_value="/tmp/db")
        ),
        patch.object(
            service,
            "_database_revision_status",
            new=AsyncMock(return_value=current),
        ),
        patch.object(service, "_migration_path", new=AsyncMock()) as migrate,
        patch.object(service, "_fast_restart_path", new=AsyncMock()) as fast,
    ):
        await service._pipeline_inner(
            state, skip_frontend_build=False, force=False
        )

    assert state.database_migration_required is False
    migrate.assert_not_awaited()
    fast.assert_awaited_once_with(state)


@pytest.mark.asyncio
async def test_uv_failure_after_pull_schedules_code_only_rollback(tmp_path):
    service = _service(tmp_path)
    state = _state()
    heads = iter(["a" * 40, "b" * 40])

    async def fake_run(cmd, **_kwargs):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result(next(heads))
        if cmd[:2] == ["git", "status"]:
            return _result()
        if cmd[-3:] == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return _result("main")
        if cmd[:2] == ["git", "pull"]:
            return _result()
        if cmd[:2] == ["git", "diff"]:
            return _result("backend/example.py\n")
        if cmd == ["uv", "sync"]:
            return _result(stderr="broken dependency", returncode=1)
        raise AssertionError(cmd)

    spawn = MagicMock()
    with (
        patch.object(service, "_run_cmd", side_effect=fake_run),
        patch.object(
            service, "_backup_database", new=AsyncMock(return_value="/tmp/db")
        ),
        patch.object(service, "_spawn_update_script", spawn),
        patch(
            "backend.services.update_service.asyncio.sleep", new=AsyncMock()
        ),
    ):
        await service._pipeline_inner(
            state, skip_frontend_build=False, force=False
        )

    assert state.status == "rolling_back"
    assert state.deployment_incomplete is True
    assert spawn.call_args.args[0] == "rollback_code"


@pytest.mark.asyncio
async def test_tracked_dirty_tree_is_rejected_without_pull_or_stash(tmp_path):
    service = _service(tmp_path)
    state = _state()
    commands: list[list[str]] = []

    async def fake_run(cmd, **_kwargs):
        commands.append(cmd)
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result("a" * 40)
        if cmd[:2] == ["git", "status"]:
            return _result(" M backend/main.py\n")
        raise AssertionError(cmd)

    with patch.object(service, "_run_cmd", side_effect=fake_run):
        await service._pipeline_inner(
            state, skip_frontend_build=False, force=False
        )

    assert state.status == "failed"
    assert state.deployment_incomplete is False
    assert "本地改动" in state.error
    assert not any(command[:2] == ["git", "pull"] for command in commands)
    assert not any("stash" in command for command in commands)


@pytest.mark.asyncio
async def test_non_sqlite_update_fails_before_any_git_mutation(tmp_path):
    with patch.object(
        settings, "database_url", "postgresql+asyncpg://db/ccm"
    ):
        service = _service(tmp_path)
        service._automatic_rollback_supported = False

    run = AsyncMock()
    with patch.object(service, "_run_cmd", run):
        result = await service.start_update()

    assert "error" in result
    assert "SQLite" in result["error"]
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_repair_syncs_dependencies_before_revision_check_and_rebuilds(
    tmp_path,
):
    service = _service(tmp_path)
    state = _state("repair")
    order: list[str] = []

    async def fake_run(cmd, **_kwargs):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _result("a" * 40)
        if cmd[:2] == ["git", "status"]:
            return _result()
        if cmd == ["uv", "sync"]:
            order.append("uv_sync")
            return _result()
        if cmd == ["npm", "install"]:
            order.append("npm_install")
            return _result()
        if cmd == ["npm", "run", "build"]:
            order.append("frontend_build")
            return _result()
        raise AssertionError(cmd)

    async def database_status():
        order.append("database_check")
        return {
            "current_revisions": ["rev"],
            "head_revisions": ["rev"],
            "current_revision": "rev",
            "head_revision": "rev",
            "up_to_date": True,
            "error": "",
        }

    with (
        patch.object(service, "_run_cmd", side_effect=fake_run),
        patch.object(
            service, "_backup_database", new=AsyncMock(return_value="/tmp/db")
        ),
        patch.object(
            service,
            "_database_revision_status",
            side_effect=database_status,
        ),
        patch.object(service, "_fast_restart_path", new=AsyncMock()) as restart,
    ):
        await service._repair_inner(
            state, skip_frontend_build=False
        )

    assert order == [
        "uv_sync",
        "database_check",
        "npm_install",
        "frontend_build",
    ]
    restart.assert_awaited_once_with(state)


@pytest.mark.asyncio
async def test_restart_rechecks_admission_after_slow_inspection(tmp_path):
    service = _service(tmp_path)
    inspection_started = asyncio.Event()
    inspection_release = asyncio.Event()

    async def slow_status(**_kwargs):
        inspection_started.set()
        await inspection_release.wait()
        return {
            "repair_required": False,
            "repair_reasons": [],
            "disk_commit": "a" * 40,
        }

    with patch.object(service, "_deployment_status", side_effect=slow_status):
        restart_task = asyncio.create_task(service.restart())
        await inspection_started.wait()
        service._current = UpdateState(status="restarting")
        inspection_release.set()
        result = await restart_task

    assert "error" in result


@pytest.mark.asyncio
async def test_restart_refuses_unknown_or_behind_database(tmp_path):
    service = _service(tmp_path)
    deployment = {
        "repair_required": True,
        "repair_reasons": ["数据库 revision 落后于代码，需要补跑迁移"],
        "disk_commit": "a" * 40,
    }
    with patch.object(
        service, "_deployment_status", new=AsyncMock(return_value=deployment)
    ):
        result = await service.restart()
    assert result["repair_required"] is True
    assert "不能只重启" in result["error"]


@pytest.mark.asyncio
async def test_repo_scoped_lease_allows_only_one_service_to_start_update(
    tmp_path,
):
    first = _service(tmp_path, port=19001)
    second = _service(tmp_path, port=19002)
    release = asyncio.Event()

    deployment = {
        "repair_reason_codes": [],
        "repair_reasons": [],
    }

    async def hold_pipeline(*_args, **_kwargs):
        await release.wait()

    with (
        patch.object(
            first,
            "_deployment_status",
            new=AsyncMock(return_value=deployment),
        ),
        patch.object(
            second,
            "_deployment_status",
            new=AsyncMock(return_value=deployment),
        ),
        patch.object(first, "_run_pipeline", side_effect=hold_pipeline),
        patch.object(second, "_run_pipeline", side_effect=hold_pipeline),
    ):
        first_result, second_result = await asyncio.gather(
            first.start_update(), second.start_update()
        )
        started = [
            result
            for result in (first_result, second_result)
            if result.get("status") == "started"
        ]
        blocked = [
            result
            for result in (first_result, second_result)
            if result.get("deployment_busy")
        ]
        assert len(started) == 1
        assert len(blocked) == 1
        release.set()
        owner = first if first_result.get("status") == "started" else second
        await owner._operation_task


@pytest.mark.asyncio
async def test_dead_non_handoff_lease_requires_repair_before_update(tmp_path):
    service = _service(tmp_path)
    service._lease_file.parent.mkdir(parents=True, exist_ok=True)
    service._lease_file.write_text(
        json.dumps(
            {
                "owner_token": "dead",
                "owner_pid": 99999999,
                "owner_pid_start": "missing",
                "port": 1,
                "operation": "update",
                "status": "running",
                "handoff": False,
                "started_at": "2026-07-24T00:00:00+00:00",
                "updated_at": "2026-07-24T00:00:00+00:00",
            }
        )
    )

    result = await service.start_update()

    assert result["repair_required"] is True
    lease = json.loads(service._lease_file.read_text())
    assert lease["status"] == "failed"
    assert lease["deployment_incomplete"] is True


@pytest.mark.asyncio
async def test_failed_incomplete_deployment_cannot_be_hidden_by_new_update(
    tmp_path,
):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="failed",
        operation="update",
        old_commit="a" * 40,
        new_commit="b" * 40,
        deployment_incomplete=True,
        error="frontend build failed",
    )
    preflight = {
        "repair_reason_codes": ["previous_deployment_failed"],
        "repair_reasons": ["frontend build failed"],
    }
    with patch.object(
        service, "_deployment_status", new=AsyncMock(return_value=preflight)
    ):
        result = await service.start_update()

    assert result["repair_required"] is True
    assert service._current.status == "failed"


@pytest.mark.asyncio
async def test_manual_rollback_requires_confirmation_only_for_database_restore(
    tmp_path,
):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="completed",
        old_commit="a" * 40,
        new_commit="b" * 40,
        backup_file="/tmp/db",
        database_migration_required=True,
        database_migration_applied=True,
    )
    refused = await service.rollback()
    assert refused["confirmation_required"] is True

    spawn = MagicMock()
    with (
        patch.object(service, "_spawn_update_script", spawn),
        patch(
            "backend.services.update_service.asyncio.sleep", new=AsyncMock()
        ),
    ):
        accepted = await service.rollback(confirm_database_restore=True)
    assert accepted["status"] == "rolling_back"
    assert spawn.call_args.args[0] == "rollback"


@pytest.mark.asyncio
async def test_unknown_migration_result_requires_confirmed_database_restore(
    tmp_path,
):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="failed",
        operation="update",
        old_commit="a" * 40,
        new_commit="b" * 40,
        backup_file="/tmp/db",
        database_migration_required=True,
        database_migration_applied=None,
    )
    result = await service.rollback()
    assert result["confirmation_required"] is True


@pytest.mark.asyncio
async def test_manual_rollback_rejects_same_commit_operations(tmp_path):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="completed",
        operation="restart",
        old_commit="a" * 40,
        new_commit="a" * 40,
    )
    result = await service.rollback()
    assert "没有切换代码版本" in result["error"]


@pytest.mark.asyncio
async def test_manual_rollback_rejects_missing_target_commit(tmp_path):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="failed",
        operation="update",
        old_commit="a" * 40,
        new_commit="",
    )
    result = await service.rollback()
    assert "没有切换代码版本" in result["error"]


@pytest.mark.asyncio
async def test_manual_rollback_cannot_restore_snapshot_twice(tmp_path):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="rolled_back",
        old_commit="a" * 40,
        new_commit="b" * 40,
        backup_file="/tmp/db",
        database_migration_applied=True,
    )
    result = await service.rollback(confirm_database_restore=True)
    assert "不能重复" in result["error"]


def test_recovery_rejects_mismatched_running_commit(tmp_path):
    service = _service(tmp_path)
    service.running_commit = "b" * 40
    service._status_file.write_text(
        json.dumps(
            {
                "status": "restarting",
                "operation": "update",
                "old_commit": "a" * 40,
                "new_commit": "c" * 40,
                "timestamp": "2026-07-24T00:00:00+00:00",
            }
        )
    )

    service.recover_from_status_file()

    assert service._current is not None
    assert service._current.status == "failed"
    assert service._current.deployment_incomplete is True
    durable = json.loads(service._journal_file.read_text())
    assert durable["status"] == "failed"


@pytest.mark.asyncio
async def test_recovered_migration_success_requires_confirmed_full_rollback(
    tmp_path,
):
    service = _service(tmp_path)
    old_commit = "a" * 40
    new_commit = "b" * 40
    service.running_commit = new_commit
    token = "owner-token"
    payload = {
        "status": "completed",
        "operation": "update",
        "owner_token": token,
        "port": service.port,
        "old_commit": old_commit,
        "new_commit": new_commit,
        "expected_commit": new_commit,
        "backup_file": "/tmp/db",
        "database_migration_required": True,
        "database_migration_applied": True,
        "timestamp": "2026-07-24T00:00:00+00:00",
    }
    service._status_file.write_text(json.dumps(payload))
    service._lease_file.write_text(json.dumps(payload))
    service.recover_from_status_file()

    assert service._current.database_migration_applied is True
    refused = await service.rollback()
    assert refused["confirmation_required"] is True

    with (
        patch.object(service, "_spawn_update_script") as spawn,
        patch(
            "backend.services.update_service.asyncio.sleep", new=AsyncMock()
        ),
    ):
        accepted = await service.rollback(confirm_database_restore=True)
    assert accepted["status"] == "rolling_back"
    assert spawn.call_args.args[0] == "rollback"


@pytest.mark.asyncio
async def test_startup_preserves_active_handoff_until_terminal_status(
    tmp_path,
):
    service = _service(tmp_path)
    commit = "b" * 40
    token = "handoff-token"
    service.running_commit = commit
    active = {
        "status": "starting",
        "step": "start_service",
        "operation": "update",
        "owner_token": token,
        "port": service.port,
        "old_commit": "a" * 40,
        "new_commit": commit,
        "expected_commit": commit,
        "handoff": True,
        "handoff_pid": os.getpid(),
        "handoff_pid_start": service._pid_start_identity(os.getpid()),
        "terminal_intent": "completed",
        "database_migration_required": False,
        "database_migration_applied": False,
        "timestamp": "2026-07-24T00:00:00+00:00",
        "updated_at": "2026-07-24T00:00:00+00:00",
    }
    service._status_file.write_text(json.dumps(active))
    service._lease_file.write_text(json.dumps(active))

    service.recover_from_status_file()
    assert service._current.status == "restarting"
    assert json.loads(service._lease_file.read_text())["handoff"] is True

    terminal = {
        **active,
        "status": "completed",
        "handoff": False,
        "timestamp": "2026-07-24T00:00:01+00:00",
    }
    service._status_file.write_text(json.dumps(terminal))
    status = await service.get_status()
    assert status["status"] == "completed"
    assert json.loads(service._lease_file.read_text())["handoff"] is False


def test_dead_startup_handoff_without_terminal_intent_becomes_failed(tmp_path):
    service = _service(tmp_path)
    commit = "b" * 40
    token = "dead-handoff"
    service.running_commit = commit
    active = {
        "status": "starting",
        "operation": "update",
        "owner_token": token,
        "port": service.port,
        "old_commit": "a" * 40,
        "new_commit": commit,
        "expected_commit": commit,
        "handoff": True,
        "handoff_pid": 99999999,
        "handoff_pid_start": "dead",
        "timestamp": "2026-07-24T00:00:00+00:00",
        "updated_at": "2026-07-24T00:00:00+00:00",
    }
    service._status_file.write_text(json.dumps(active))
    service._lease_file.write_text(json.dumps(active))

    service.recover_from_status_file()

    assert service._current.status == "failed"
    assert service._current.deployment_incomplete is True


@pytest.mark.asyncio
async def test_live_backend_reconciles_external_failure_before_stop(tmp_path):
    service = _service(tmp_path)
    commit = "a" * 40
    token = "live-token"
    service.running_commit = commit
    service._lease_token = token
    service._current = UpdateState(
        status="restarting",
        operation="restart",
        old_commit=commit,
        new_commit=commit,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    active = {
        "owner_token": token,
        "port": service.port,
        "operation": "restart",
        "status": "restarting",
        "handoff": True,
        "expected_commit": commit,
        "updated_at": "2026-07-24T00:00:00+00:00",
    }
    service._lease_file.write_text(json.dumps(active))
    failed = {
        **active,
        "status": "failed",
        "handoff": False,
        "message": "stop service failed",
        "deployment_incomplete": False,
        "timestamp": "2026-07-24T00:00:01+00:00",
    }
    service._status_file.write_text(json.dumps(failed))

    status = await service.get_status()

    assert status["status"] == "failed"
    assert "stop service failed" in status["error"]
    assert service._operation_active() is False


@pytest.mark.asyncio
async def test_stale_empty_token_status_cannot_finalize_new_operation(tmp_path):
    service = _service(tmp_path)
    token = "new-operation-token"
    commit = "a" * 40
    service.running_commit = commit
    service._lease_token = token
    service._current = UpdateState(
        status="restarting",
        operation="restart",
        old_commit=commit,
        new_commit=commit,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    lease = {
        "owner_token": token,
        "port": service.port,
        "status": "restarting",
        "handoff": True,
        "handoff_pid": os.getpid(),
        "handoff_pid_start": service._pid_start_identity(os.getpid()),
        "updated_at": "2026-07-24T00:00:01+00:00",
    }
    service._lease_file.write_text(json.dumps(lease))
    service._status_file.write_text(
        json.dumps(
            {
                "status": "completed",
                "port": service.port,
                "new_commit": commit,
                "timestamp": "2026-07-24T00:00:02+00:00",
            }
        )
    )

    await service.get_status()

    assert service._current.status == "restarting"
    assert service._lease_token == token


@pytest.mark.asyncio
async def test_direct_retry_reconciles_terminal_handoff_without_status_poll(
    tmp_path,
):
    service = _service(tmp_path)
    token = "failed-handoff"
    service._lease_token = token
    service._current = UpdateState(
        status="restarting",
        operation="update",
        old_commit="a" * 40,
        new_commit="b" * 40,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    lease = {
        "owner_token": token,
        "owner_pid": 99999999,
        "owner_pid_start": "dead",
        "port": service.port,
        "status": "failed",
        "handoff": False,
        "deployment_incomplete": True,
        "message": "script failed",
        "timestamp": "2026-07-24T00:00:01+00:00",
    }
    service._lease_file.write_text(json.dumps(lease))
    service._status_file.write_text(json.dumps(lease))

    with patch.object(service, "_run_repair", new=AsyncMock()):
        result = await service.start_repair()
        await service._operation_task

    assert result["status"] == "started"
    assert service._current.operation == "repair"


def test_finish_claim_keeps_token_when_lease_update_fails(tmp_path):
    service = _service(tmp_path)
    service._lease_token = "owner"
    with patch.object(
        service, "_update_deployment_lease", return_value=False
    ):
        service._finish_deployment_claim(
            status="failed", message="x"
        )
    assert service._lease_token == "owner"


def test_live_dead_handoff_is_reconciled_before_admission(tmp_path):
    service = _service(tmp_path)
    token = "dead-live"
    service._lease_token = token
    service._current = UpdateState(
        status="restarting",
        operation="update",
        old_commit="a" * 40,
        new_commit="b" * 40,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    service._lease_file.write_text(
        json.dumps(
            {
                "owner_token": token,
                "port": service.port,
                "status": "restarting",
                "handoff": True,
                "handoff_pid": 99999999,
                "handoff_pid_start": "dead",
                "updated_at": "2026-07-24T00:00:00+00:00",
            }
        )
    )

    assert service._operation_active() is False
    assert service._current.status == "failed"
    assert service._current.deployment_incomplete is True


@pytest.mark.asyncio
async def test_restart_modes_pass_distinct_failure_policy_and_operation(
    tmp_path,
):
    service = _service(tmp_path)
    commit = "a" * 40
    service._lease_token = "token"
    manual = UpdateState(
        status="running",
        operation="restart",
        old_commit=commit,
        new_commit=commit,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    manual.steps[-1].status = "running"
    service._current = manual
    with (
        patch.object(service, "_spawn_update_script") as spawn,
        patch(
            "backend.services.update_service.asyncio.sleep", new=AsyncMock()
        ),
    ):
        await service._run_restart(manual)
    assert spawn.call_args.kwargs["restart_failure_policy"] == "retry"

    repair = UpdateState(
        status="running",
        operation="repair",
        old_commit=commit,
        new_commit=commit,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    service._current = repair
    with (
        patch.object(service, "_spawn_update_script") as spawn,
        patch(
            "backend.services.update_service.asyncio.sleep", new=AsyncMock()
        ),
    ):
        await service._fast_restart_path(repair)
    assert spawn.call_args.kwargs["restart_failure_policy"] == "rollback"


def test_handoff_mark_is_live_before_shell_writes_first_status(tmp_path):
    service = _service(tmp_path)
    assert service._claim_deployment_lease("update") is None
    service._current = UpdateState(
        status="restarting",
        operation="update",
        old_commit="a" * 40,
        new_commit="b" * 40,
    )

    service._mark_deployment_handoff("restart")

    lease = service._read_deployment_lease()
    assert lease["handoff"] is True
    assert service._lease_handoff_alive(lease) is True


def test_provisional_handoff_expires_if_shell_never_acknowledges(tmp_path):
    service = _service(tmp_path)
    assert service._claim_deployment_lease("update") is None
    service._current = UpdateState(
        status="restarting",
        operation="update",
        old_commit="a" * 40,
        new_commit="b" * 40,
        steps=[StepInfo(name=name) for name in STEP_NAMES],
    )
    service._mark_deployment_handoff("restart")
    lease = service._read_deployment_lease()
    lease["handoff_ack_deadline"] = "2026-07-24T00:00:00+00:00"
    service._atomic_write_json(service._lease_file, lease)

    assert service._operation_active() is False
    assert service._current.status == "failed"
    assert service._current.deployment_incomplete is True


def test_spawn_passes_restart_policy_and_deployment_operation(tmp_path):
    service = _service(tmp_path)
    service._current = UpdateState(
        status="restarting",
        operation="repair",
        old_commit="a" * 40,
        new_commit="a" * 40,
    )
    with (
        patch.object(service, "_systemd_scope", return_value=None),
        patch("backend.services.update_service.subprocess.Popen") as popen,
    ):
        service._spawn_update_script(
            "restart",
            service._current.old_commit,
            "",
            restart_failure_policy="rollback",
        )

    argv = popen.call_args.args[0]
    copied_script = Path(argv[1])
    source_script = tmp_path / "scripts" / "update_migrate.sh"
    assert copied_script != source_script
    assert copied_script.read_bytes() == source_script.read_bytes()
    assert copied_script.stat().st_mode & 0o777 == 0o700
    assert argv[-3] == "rollback"
    assert argv[-2] == "repair"
    assert Path(argv[-1]) == copied_script.parent


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups required")
@pytest.mark.asyncio
async def test_run_cmd_timeout_kills_grandchild_process_group(tmp_path):
    service = _service(tmp_path)
    pid_file = tmp_path / "child.pid"
    code = (
        "import os,time\n"
        "pid=os.fork()\n"
        "if pid == 0:\n"
        " open(%r,'w').write(str(os.getpid()))\n"
        " time.sleep(60)\n"
        "else:\n"
        " time.sleep(60)\n"
    ) % str(pid_file)

    result = await service._run_cmd(
        [sys.executable, "-c", code], timeout=1
    )

    assert result["returncode"] == -1
    child_pid = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


@pytest.mark.asyncio
async def test_update_routes_are_admin_only_without_hiding_health():
    app = FastAPI()

    @app.middleware("http")
    async def member_identity(request: Request, call_next):
        request.state.user_role = "member"
        request.state.user_id = 42
        return await call_next(request)

    app.include_router(system_api.router)
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        health = await client.get("/api/system/health")
        responses = [
            await client.post("/api/system/update", json={}),
            await client.get("/api/system/update/status"),
            await client.post("/api/system/update/rollback", json={}),
            await client.post("/api/system/restart"),
            await client.post("/api/system/update/repair", json={}),
        ]

    assert health.status_code == 200
    assert [response.status_code for response in responses] == [403] * 5


@pytest.mark.asyncio
async def test_rollback_confirmation_is_structured_http_409():
    app = FastAPI()

    @app.middleware("http")
    async def admin_identity(request: Request, call_next):
        request.state.user_role = "super_admin"
        request.state.user_id = 1
        return await call_next(request)

    fake_service = MagicMock()
    fake_service.rollback = AsyncMock(
        return_value={
            "error": "database restore confirmation required",
            "confirmation_required": True,
        }
    )
    app.include_router(system_api.router)
    transport = ASGITransport(app=app)
    with patch.object(
        system_api, "_get_update_service", return_value=fake_service
    ):
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/system/update/rollback", json={}
            )

    assert response.status_code == 409
    assert response.json()["detail"]["confirmation_required"] is True
