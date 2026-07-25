"""Safety tests for the durable CCM deployment state machine."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from backend.database import Base
from backend.models.instance import Instance
from backend.models.task import Task
from backend.services.deployment_start_guard import (
    deployment_task_start_fence,
)
from backend.services.dispatcher import GlobalDispatcher
from backend.services.update_service import (
    STEP_NAMES,
    StepInfo,
    UpdateService,
    UpdateState,
)


def _service(
    tmp_path: Path,
    *,
    running_commit: str = "a" * 40,
    db_factory=None,
    dispatcher=None,
) -> UpdateService:
    script_dir = tmp_path / "scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    script = script_dir / "update_migrate.sh"
    script.write_text(
        "#!/bin/bash\nCCM_UPDATE_PROTOCOL_VERSION=2\nexit 0\n"
    )
    script.chmod(0o700)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    service = UpdateService(
        broadcaster,
        port=18765,
        project_dir=str(tmp_path),
        db_factory=db_factory,
        dispatcher=dispatcher,
        running_commit=running_commit,
        update_runtime_root=tmp_path / ".update-runtime",
        legacy_update_runtime_root=tmp_path / ".legacy-update-runtime",
    )
    service._status_file = tmp_path / "status.json"
    service._journal_file = tmp_path / "backups" / "status.json"
    service._lease_file = tmp_path / "backups" / "deployment-lease.json"
    service._lease_lock_file = (
        tmp_path / "backups" / "deployment-lease.lock"
    )
    service._dirty_worktree_files = AsyncMock(return_value=[])
    return service


def _state(**overrides) -> UpdateState:
    values = {
        "update_id": "test",
        "status": "completed",
        "steps": [StepInfo(name=name) for name in STEP_NAMES],
        "old_commit": "a" * 40,
        "new_commit": "b" * 40,
        "operation": "update",
    }
    values.update(overrides)
    return UpdateState(**values)


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def test_parse_alembic_revision_accepts_multiple_status_markers():
    output = (
        "INFO  [alembic.runtime.migration] Context impl SQLiteImpl.\n"
        "c7e9b1d42f60 (head) (mergepoint)\n"
    )

    assert UpdateService._parse_alembic_revisions(output) == [
        "c7e9b1d42f60"
    ]


@pytest.mark.asyncio
async def test_status_reports_exact_code_and_database_revisions(tmp_path):
    service = _service(tmp_path)
    service._disk_commit = AsyncMock(return_value="b" * 40)
    service._database_revision_status = AsyncMock(
        return_value={
            "database_current_revisions": ["rev1"],
            "database_head_revisions": ["rev2"],
            "database_up_to_date": False,
            "db_current_revision": "rev1",
            "db_head_revision": "rev2",
            "db_up_to_date": False,
            "database_revision_error": "",
        }
    )

    status = await service.get_status()

    assert status["running_commit"] == "a" * 40
    assert status["disk_commit"] == "b" * 40
    assert status["db_current_revision"] == "rev1"
    assert status["db_head_revision"] == "rev2"
    assert status["db_up_to_date"] is False
    assert "database_migration_pending" in status["repair_reason_codes"]
    assert "runtime_code_stale" in status["repair_reason_codes"]
    assert status["restart_only_safe"] is False


@pytest.mark.asyncio
async def test_blockers_include_quarantined_process_evidence(tmp_path):
    database_path = tmp_path / "evidence.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as database:
        instance = Instance(
            name="uncertain-orphan",
            status="error",
            pid=os.getpid(),
            current_task_id=987,
        )
        database.add(instance)
        await database.commit()
        instance_id = instance.id

    service = _service(tmp_path, db_factory=factory)
    try:
        blockers = await service._get_blocking_tasks(
            pending_task_ids=set()
        )
    finally:
        await engine.dispose()

    assert blockers == [
        {
            "id": instance_id,
            "instance_id": instance_id,
            "title": (
                "实例 uncertain-orphan（任务 #987 仍有未解除运行证据）"
            ),
            "status": "quarantined_process_evidence",
            "kind": "instance",
        }
    ]


@pytest.mark.asyncio
async def test_reconcile_blockers_pauses_runtime_and_rechecks(tmp_path):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.reconcile_stale_state_for_maintenance = AsyncMock()
    service = _service(tmp_path, dispatcher=dispatcher)
    service._get_blocking_tasks = AsyncMock(return_value=[])

    result = await service.reconcile_blockers()

    assert result == {
        "reconciled": True,
        "update_blocked": False,
        "active_task_count": 0,
        "active_tasks": [],
    }
    dispatcher.pause_dispatching.assert_awaited_once()
    dispatcher.reconcile_stale_state_for_maintenance.assert_awaited_once()
    service._get_blocking_tasks.assert_awaited_once()
    dispatcher.resume_dispatching.assert_called_once()


@pytest.mark.asyncio
async def test_reconcile_blockers_failure_resumes_runtime(tmp_path):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.reconcile_stale_state_for_maintenance = AsyncMock(
        side_effect=RuntimeError("reconciliation failed")
    )
    service = _service(tmp_path, dispatcher=dispatcher)

    result = await service.reconcile_blockers()

    assert result["reconciled"] is False
    assert result["update_blocked"] is True
    assert "reconciliation failed" in result["error"]
    dispatcher.resume_dispatching.assert_called_once()


@pytest.mark.asyncio
async def test_reconcile_blockers_cancellation_resumes_runtime(tmp_path):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    entered = asyncio.Event()
    blocked = asyncio.Event()

    async def reconcile():
        entered.set()
        await blocked.wait()

    dispatcher.reconcile_stale_state_for_maintenance = reconcile
    service = _service(tmp_path, dispatcher=dispatcher)
    request = asyncio.create_task(service.reconcile_blockers())
    await asyncio.wait_for(entered.wait(), timeout=1)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    dispatcher.resume_dispatching.assert_called_once()


@pytest.mark.asyncio
async def test_reconcile_blockers_clears_multi_dead_owner_ghost(tmp_path):
    database_path = tmp_path / "ghost.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    manager._consumer_records = {}
    manager._process_groups = {}
    manager._container_exec_processes = {}
    manager.is_running = MagicMock(return_value=False)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    dispatcher = GlobalDispatcher(
        db_factory=factory,
        instance_manager=manager,
        broadcaster=broadcaster,
    )
    service = _service(
        tmp_path,
        db_factory=factory,
        dispatcher=dispatcher,
    )

    async with factory() as database:
        task = Task(title="", description="test ghost", status="executing")
        database.add(task)
        await database.flush()
        owners = [
            Instance(
                name=f"ghost-{index}",
                status="running",
                pid=994000 + index,
                current_task_id=task.id,
            )
            for index in range(5)
        ]
        database.add_all(owners)
        await database.flush()
        task.instance_id = owners[-1].id
        await database.commit()
        task_id = task.id
        owner_ids = [owner.id for owner in owners]

    try:
        before = await service._get_blocking_tasks(
            pending_task_ids=set()
        )
        assert before == [{
            "id": task_id,
            "title": "test ghost",
            "status": "executing",
            "kind": "task",
            "instance_claim_count": 5,
        }]

        with patch(
            "backend.services.dispatcher.os.kill",
            side_effect=ProcessLookupError,
        ):
            result = await service.reconcile_blockers()

        assert result == {
            "reconciled": True,
            "update_blocked": False,
            "active_task_count": 0,
            "active_tasks": [],
        }
        async with factory() as database:
            task = await database.get(Task, task_id)
            assert task.status == "failed"
            assert task.instance_id is None
            for owner_id in owner_ids:
                owner = await database.get(Instance, owner_id)
                assert owner.status == "error"
                assert owner.pid is None
                assert owner.current_task_id is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_reconcile_keeps_unknown_live_process_as_blocker(tmp_path):
    database_path = tmp_path / "unknown-live.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    manager._consumer_records = {}
    manager._process_groups = {}
    manager._container_exec_processes = {}
    manager.is_running = MagicMock(return_value=False)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    dispatcher = GlobalDispatcher(
        db_factory=factory,
        instance_manager=manager,
        broadcaster=broadcaster,
    )
    service = _service(
        tmp_path,
        db_factory=factory,
        dispatcher=dispatcher,
    )

    async with factory() as database:
        task = Task(
            title="unknown live",
            description="d",
            status="executing",
        )
        database.add(task)
        await database.flush()
        owner = Instance(
            name="unmanaged-live",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        database.add(owner)
        await database.flush()
        task.instance_id = owner.id
        await database.commit()
        task_id, owner_id = task.id, owner.id

    try:
        result = await service.reconcile_blockers()

        assert result["reconciled"] is True
        assert result["update_blocked"] is True
        assert result["active_task_count"] == 1
        assert result["active_tasks"] == [{
            "id": owner_id,
            "instance_id": owner_id,
            "title": (
                f"实例 unmanaged-live（任务 #{task_id} 仍有未解除运行证据）"
            ),
            "status": "quarantined_process_evidence",
            "kind": "instance",
        }]
        async with factory() as database:
            task = await database.get(Task, task_id)
            owner = await database.get(Instance, owner_id)
            assert task.status == "failed"
            assert owner.status == "error"
            assert owner.pid == os.getpid()
            assert owner.current_task_id == task_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_reconcile_preserves_manager_owned_active_generation(tmp_path):
    database_path = tmp_path / "manager-live.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    manager._consumer_records = {}
    manager._process_groups = {}
    manager._container_exec_processes = {}
    manager.is_running = MagicMock(return_value=False)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    dispatcher = GlobalDispatcher(
        db_factory=factory,
        instance_manager=manager,
        broadcaster=broadcaster,
    )
    service = _service(
        tmp_path,
        db_factory=factory,
        dispatcher=dispatcher,
    )

    async with factory() as database:
        task = Task(
            title="manager live",
            description="d",
            status="executing",
        )
        database.add(task)
        await database.flush()
        owner = Instance(
            name="manager-live-owner",
            status="running",
            pid=43210,
            current_task_id=task.id,
        )
        database.add(owner)
        await database.flush()
        task.instance_id = owner.id
        await database.commit()
        task_id, owner_id = task.id, owner.id
    manager.processes[owner_id] = MagicMock(returncode=None)

    try:
        result = await service.reconcile_blockers()

        assert result["reconciled"] is True
        assert result["update_blocked"] is True
        assert result["active_tasks"] == [{
            "id": task_id,
            "title": "manager live",
            "status": "executing",
            "kind": "task",
            "instance_claim_count": 1,
        }]
        async with factory() as database:
            task = await database.get(Task, task_id)
            owner = await database.get(Instance, owner_id)
            assert task.status == "executing"
            assert task.instance_id == owner_id
            assert owner.status == "running"
            assert owner.pid == 43210
            assert owner.current_task_id == task_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_reconcile_rejects_maintenance_or_active_deployment_without_pause(
    tmp_path,
):
    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    service = _service(tmp_path, dispatcher=dispatcher)

    service.maintenance_only = True
    maintenance = await service.reconcile_blockers()
    assert maintenance["repair_required"] is True
    dispatcher.pause_dispatching.assert_not_awaited()

    service.maintenance_only = False
    service._current = _state(status="running")
    active = await service.reconcile_blockers()
    assert active == {"error": "有部署操作正在进行中"}
    dispatcher.pause_dispatching.assert_not_awaited()


@pytest.mark.asyncio
async def test_target_protocol_is_validated_at_pinned_fetched_sha(tmp_path):
    service = _service(tmp_path)
    target = "b" * 40

    async def run(cmd, **_kwargs):
        if cmd[:2] == ["git", "fetch"]:
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if cmd == ["git", "rev-parse", "--verify", "origin/main"]:
            return {"returncode": 0, "stdout": target, "stderr": ""}
        if cmd == [
            "git",
            "show",
            f"{target}:scripts/update_migrate.sh",
        ]:
            return {
                "returncode": 0,
                "stdout": "CCM_UPDATE_PROTOCOL_VERSION=2\n",
                "stderr": "",
            }
        raise AssertionError(cmd)

    service._run_cmd = AsyncMock(side_effect=run)

    accepted, error, commit = (
        await service._fetch_and_validate_target_protocol("origin", "main")
    )

    assert accepted is True
    assert error == ""
    assert commit == target


def test_corrupt_or_symlink_lease_is_never_overwritten(tmp_path):
    service = _service(tmp_path)
    service._lease_file.parent.mkdir(parents=True, exist_ok=True)
    service._lease_file.write_text("{broken")
    service._lease_file.chmod(0o600)
    before = service._lease_file.read_bytes()

    assert service._claim_deployment_lease("update") is None
    assert service._lease_file.read_bytes() == before

    service._lease_file.unlink()
    target = tmp_path / "outside.json"
    target.write_text("{}")
    service._lease_file.symlink_to(target)
    assert service._claim_deployment_lease("repair", allow_failed=True) is None
    assert service._lease_file.is_symlink()


def test_symlinked_backup_directory_is_rejected(tmp_path):
    service = _service(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    backup_dir = tmp_path / "backups"
    backup_dir.symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeError, match="备份目录|租约目录"):
        service._claim_deployment_lease("update")

    assert list(outside.iterdir()) == []


def test_corrupt_lease_is_authoritative_over_tmp_success(tmp_path):
    service = _service(tmp_path)
    service._lease_file.parent.mkdir(parents=True, exist_ok=True)
    service._lease_file.write_text("{broken")
    service._lease_file.chmod(0o600)
    _write_json(
        service._status_file,
        {
            "port": service.port,
            "status": "completed",
            "expected_commit": service.running_commit,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )

    service.recover_from_status_file()

    assert service._current is not None
    assert service._current.status == "failed"
    assert service._current.deployment_incomplete is True
    assert "租约" in service._current.error


def test_unknown_lease_owner_liveness_fails_closed(tmp_path):
    service = _service(tmp_path)
    _write_json(
        service._lease_file,
        {
            "status": "running",
            "owner_token": "other",
            "owner_pid": os.getpid(),
            # Missing start identity is unknown, not proof that the owner died.
            "updated_at": "2000-01-01T00:00:00+00:00",
        },
    )

    assert service._claim_deployment_lease("repair", allow_failed=True) is None
    assert service._read_deployment_lease()["owner_token"] == "other"


def test_unknown_provisional_deadline_never_allows_lease_takeover(tmp_path):
    service = _service(tmp_path)
    _write_json(
        service._lease_file,
        {
            "status": "starting",
            "owner_token": "other",
            "owner_pid": 99999999,
            "owner_pid_start": "123",
            "handoff": True,
            "handoff_provisional": True,
            "handoff_ack_deadline": "not-a-date",
            "updated_at": "2000-01-01T00:00:00+00:00",
        },
    )

    assert service._claim_deployment_lease("repair", allow_failed=True) is None
    assert service._read_deployment_lease()["owner_token"] == "other"


def test_repo_lease_serializes_two_ccm_process_objects(tmp_path):
    first = _service(tmp_path)
    second = _service(tmp_path)

    assert first._claim_deployment_lease("update")
    assert second._claim_deployment_lease("restart") is None


def test_incomplete_rolled_back_lease_only_allows_repair_admission(tmp_path):
    service = _service(tmp_path)
    _write_json(
        service._lease_file,
        {
            "status": "rolled_back",
            "owner_token": "finished",
            "deployment_incomplete": True,
            "operation": "repair",
            "old_commit": service.running_commit,
            "new_commit": service.running_commit,
        },
    )

    assert service._claim_deployment_lease("update") is None
    assert service._read_deployment_lease()["owner_token"] == "finished"
    assert service._claim_deployment_lease(
        "repair", allow_failed=True
    )


def test_recovery_prefers_durable_lease_over_newer_tmp_success(tmp_path):
    service = _service(tmp_path, running_commit="b" * 40)
    now = datetime.now(timezone.utc).isoformat()
    lease = {
        "port": service.port,
        "owner_token": "owner",
        "status": "starting",
        "handoff": True,
        "handoff_provisional": True,
        "handoff_ack_deadline": "2999-01-01T00:00:00+00:00",
        "old_commit": "a" * 40,
        "expected_commit": "b" * 40,
        "operation": "update",
        "updated_at": now,
    }
    _write_json(service._lease_file, lease)
    _write_json(
        service._status_file,
        {
            **lease,
            "status": "completed",
            "timestamp": "2999-01-01T00:00:00+00:00",
        },
    )

    service.recover_from_status_file()

    assert service._current is not None
    assert service._current.status == "restarting"
    assert service._lease_token == "owner"


def test_terminal_success_with_wrong_running_sha_becomes_incomplete(tmp_path):
    service = _service(tmp_path, running_commit="c" * 40)
    _write_json(
        service._lease_file,
        {
            "port": service.port,
            "owner_token": "owner",
            "status": "completed",
            "old_commit": "a" * 40,
            "new_commit": "b" * 40,
            "expected_commit": "b" * 40,
            "operation": "update",
            "deployment_incomplete": False,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    service.recover_from_status_file()

    assert service._current is not None
    assert service._current.status == "failed"
    assert service._current.deployment_incomplete is True
    assert "不一致" in service._current.error


def test_legacy_missing_migration_result_recovers_as_unknown(tmp_path):
    service = _service(tmp_path)
    _write_json(
        service._status_file,
        {
            "port": service.port,
            "status": "failed",
            "old_commit": "a" * 40,
            "new_commit": "b" * 40,
            "message": "legacy failure",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )

    service.recover_from_status_file()

    assert service._current is not None
    assert service._current.database_migration_applied is None


def test_fresh_tokenless_legacy_starting_waits_for_exact_terminal(tmp_path):
    service = _service(tmp_path, running_commit="b" * 40)
    base = {
        "port": service.port,
        "status": "starting",
        "old_commit": "a" * 40,
        "new_commit": "b" * 40,
        "expected_commit": "b" * 40,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(service._status_file, base)

    service.recover_from_status_file()
    assert service._current is not None
    assert service._current.status == "restarting"
    assert service._legacy_handoff is True

    _write_json(
        service._status_file,
        {
            **base,
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )
    service._reconcile_external_terminal_status()
    assert service._current.status == "completed"
    assert service._legacy_handoff is False


@pytest.mark.asyncio
async def test_repair_rejects_unsafe_modes_before_any_work(tmp_path):
    service = _service(tmp_path)
    service._automatic_rollback_supported = False
    service._pause_dispatching = AsyncMock()

    unsupported_update = await service.start_update()
    assert "SQLite" in unsupported_update["error"]

    unsupported = await service.start_repair()
    assert "SQLite" in unsupported["error"]
    service._pause_dispatching.assert_not_awaited()

    service._automatic_rollback_supported = True
    skipped = await service.start_repair(skip_frontend_build=True)
    assert "不能跳过" in skipped["error"]
    service._pause_dispatching.assert_not_awaited()


@pytest.mark.asyncio
async def test_external_database_status_keeps_safe_restart_available(tmp_path):
    service = _service(tmp_path)
    service._automatic_rollback_supported = False
    service._disk_commit = AsyncMock(return_value=service.running_commit)
    service._database_revision_status = AsyncMock(
        return_value={
            "database_current_revisions": ["rev1"],
            "database_head_revisions": ["rev1"],
            "database_up_to_date": True,
            "db_current_revision": "rev1",
            "db_head_revision": "rev1",
            "db_up_to_date": True,
            "database_revision_error": "",
        }
    )

    status = await service.get_status()

    assert status["automatic_rollback_supported"] is False
    assert status["update_supported"] is False
    assert status["repair_required"] is False
    assert status["restart_only_safe"] is True


@pytest.mark.asyncio
async def test_external_database_can_use_same_commit_restart(tmp_path):
    service = _service(tmp_path)
    service._automatic_rollback_supported = False
    service._inspect_environment = AsyncMock(
        return_value={
            "repair_required": False,
            "repair_reasons": [],
        }
    )
    service._pause_dispatching = AsyncMock()
    service._get_blocking_tasks = AsyncMock(return_value=[])
    service._disk_commit = AsyncMock(return_value=service.running_commit)

    with patch(
        "backend.services.update_service.asyncio.create_task"
    ) as create_task:
        result = await service.restart()

    assert result["status"] == "started"
    assert service._current is not None
    assert service._current.old_commit == service.running_commit
    assert service._current.new_commit == service.running_commit
    create_task.assert_called_once()
    create_task.call_args.args[0].close()


@pytest.mark.asyncio
async def test_dirty_worktree_files_include_untracked_paths(tmp_path):
    service = _service(tmp_path)
    service._run_cmd = AsyncMock(
        return_value={
            "returncode": 0,
            "stdout": (
                " M backend/main.py\n"
                "?? backend/services/new_module.py\n"
            ),
            "stderr": "",
        }
    )

    result = await UpdateService._dirty_worktree_files(service)

    assert result == [
        " M backend/main.py",
        "?? backend/services/new_module.py",
    ]
    service._run_cmd.assert_awaited_once_with(
        [
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ]
    )


@pytest.mark.asyncio
async def test_restart_rejects_uncommitted_checkout_before_admission(tmp_path):
    service = _service(tmp_path)
    service._dirty_worktree_files = AsyncMock(
        return_value=["?? backend/services/new_module.py"]
    )
    service._inspect_environment = AsyncMock()
    service._pause_dispatching = AsyncMock()

    result = await service.restart()

    assert "未提交" in result["error"]
    assert result["dirty_files"] == [
        "?? backend/services/new_module.py"
    ]
    service._inspect_environment.assert_not_awaited()
    service._pause_dispatching.assert_not_awaited()
    assert not service._lease_file.exists()


@pytest.mark.asyncio
async def test_update_rejects_uncommitted_checkout_before_admission(tmp_path):
    service = _service(tmp_path)
    service._dirty_worktree_files = AsyncMock(
        return_value=[" M backend/main.py"]
    )
    service._pause_dispatching = AsyncMock()

    result = await service.start_update()

    assert "未提交" in result["error"]
    assert result["dirty_files"] == [" M backend/main.py"]
    service._pause_dispatching.assert_not_awaited()
    assert not service._lease_file.exists()


@pytest.mark.asyncio
async def test_update_rechecks_blockers_after_waiting_for_repo_fence(tmp_path):
    """A cross-process Task commit that wins the shared fence blocks mutation."""
    database_path = tmp_path / "race.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as database:
        task = Task(
            title="cross-process launch",
            description="d",
            status="pending",
        )
        database.add(task)
        await database.commit()
        await database.refresh(task)
        task_id = task.id

    dispatcher = MagicMock()
    dispatcher.pause_dispatching = AsyncMock()
    dispatcher.resume_dispatching = MagicMock()
    dispatcher.pending_task_start_ids = AsyncMock(return_value=set())
    service = _service(
        tmp_path,
        db_factory=factory,
        dispatcher=dispatcher,
    )
    first_query_finished = threading.Event()
    shared_fence_acquired = threading.Event()
    thread_errors: list[BaseException] = []

    def cross_process_task_start() -> None:
        try:
            with deployment_task_start_fence(tmp_path):
                shared_fence_acquired.set()
                if not first_query_finished.wait(timeout=5):
                    raise RuntimeError("updater never completed first query")
                connection = sqlite3.connect(database_path)
                try:
                    connection.execute(
                        "UPDATE tasks SET status = 'executing' WHERE id = ?",
                        (task_id,),
                    )
                    connection.commit()
                finally:
                    connection.close()
        except BaseException as exc:  # surfaced in the async test below
            thread_errors.append(exc)

    starter = threading.Thread(target=cross_process_task_start)
    starter.start()
    assert shared_fence_acquired.wait(timeout=5)

    original_blocker_check = service._get_blocking_tasks
    blocker_check_count = 0

    async def blocker_check(*args, **kwargs):
        nonlocal blocker_check_count
        result = await original_blocker_check(*args, **kwargs)
        blocker_check_count += 1
        if blocker_check_count == 1:
            first_query_finished.set()
        return result

    service._get_blocking_tasks = AsyncMock(side_effect=blocker_check)
    service._run_pipeline = AsyncMock()
    try:
        result = await asyncio.wait_for(
            service.start_update(force=True), timeout=5
        )
    finally:
        first_query_finished.set()
        starter.join(timeout=5)
        await engine.dispose()

    assert not thread_errors
    assert not starter.is_alive()
    assert blocker_check_count == 2
    assert result["update_blocked"] is True
    assert result["active_tasks"][0]["id"] == task_id
    assert service._read_deployment_lease()["status"] == "failed"
    assert service._read_deployment_lease()["handoff"] is False
    dispatcher.resume_dispatching.assert_called_once()
    service._run_pipeline.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_post_claim_update_check_terminalizes_lease(tmp_path):
    service = _service(tmp_path)
    service._pause_dispatching = AsyncMock()
    service._resume_dispatching = MagicMock()
    second_check_started = asyncio.Event()
    never_finishes = asyncio.Event()
    calls = 0

    async def blocker_check():
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        second_check_started.set()
        await never_finishes.wait()
        return []

    service._get_blocking_tasks = AsyncMock(side_effect=blocker_check)
    request = asyncio.create_task(service.start_update(force=True))
    await asyncio.wait_for(second_check_started.wait(), timeout=2)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    lease = service._read_deployment_lease()
    assert lease["status"] == "failed"
    assert lease["handoff"] is False
    assert lease["deployment_incomplete"] is False
    assert service._lease_token is None
    service._resume_dispatching.assert_called_once()


def test_failed_admission_terminal_write_keeps_dispatcher_paused(tmp_path):
    service = _service(tmp_path)
    service._lease_token = "owner"
    service._finish_deployment_claim = MagicMock(return_value=False)
    service._resume_dispatching = MagicMock()

    released = service._finish_admission_and_resume(
        claimed=True,
        message="cancelled",
        incomplete=False,
    )

    assert released is False
    service._resume_dispatching.assert_not_called()


@pytest.mark.asyncio
async def test_repair_rejects_uncommitted_checkout_before_admission(tmp_path):
    service = _service(tmp_path)
    service._dirty_worktree_files = AsyncMock(
        return_value=[" M backend/main.py"]
    )
    service._pause_dispatching = AsyncMock()

    result = await service.start_repair()

    assert "未提交" in result["error"]
    assert result["dirty_files"] == [" M backend/main.py"]
    service._pause_dispatching.assert_not_awaited()


@pytest.mark.asyncio
async def test_repair_rechecks_dirty_checkout_before_backup(tmp_path):
    service = _service(tmp_path)
    state = _state(status="running", operation="repair")
    service._dirty_worktree_files = AsyncMock(
        return_value=[" M frontend/package.json"]
    )
    service._backup_database = AsyncMock()

    await service._repair_inner(state, skip_frontend_build=False)

    assert state.status == "failed"
    assert "本地改动" in state.error
    service._backup_database.assert_not_awaited()


def _configure_same_commit_update_pipeline(
    service: UpdateService,
    commit: str,
) -> None:
    service._disk_commit = AsyncMock(return_value=commit)
    service._deployment_base_commit = AsyncMock(return_value=commit)
    service._resolve_remote = AsyncMock(return_value="origin")
    service._fetch_and_validate_target_protocol = AsyncMock(
        return_value=(True, "", commit)
    )

    async def run_command(command, **_kwargs):
        if command == [
            "git",
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
        ]:
            return {
                "returncode": 0,
                "stdout": "main\n",
                "stderr": "",
            }
        if command[:3] == ["git", "merge", "--ff-only"]:
            return {"returncode": 0, "stdout": "", "stderr": ""}
        if command == ["git", "rev-parse", "HEAD"]:
            return {
                "returncode": 0,
                "stdout": f"{commit}\n",
                "stderr": "",
            }
        raise AssertionError(f"unexpected command: {command}")

    service._run_cmd = AsyncMock(side_effect=run_command)


@pytest.mark.asyncio
async def test_same_commit_db_repair_updates_lease_and_worker_operation(
    tmp_path,
):
    commit = "a" * 40
    service = _service(tmp_path, running_commit=commit)
    state = _state(
        status="running",
        old_commit=commit,
        new_commit="",
        operation="update",
    )
    service._current = state
    assert service._claim_deployment_lease("update")
    _configure_same_commit_update_pipeline(service, commit)
    service._database_revision_status = AsyncMock(
        return_value={
            "database_up_to_date": False,
            "database_revision_error": "",
        }
    )
    service._repair_inner = AsyncMock()

    await service._pipeline_inner(
        state,
        skip_frontend_build=False,
        force=False,
    )

    assert state.operation == "repair"
    assert state.deployment_incomplete is True
    lease = service._read_deployment_lease()
    assert lease["operation"] == "repair"
    assert lease["deployment_incomplete"] is True
    service._repair_inner.assert_awaited_once_with(
        state, skip_frontend_build=False
    )

    state.database_migration_required = True
    state.database_migration_applied = None
    with patch.object(
        service, "_systemd_scope", return_value=None
    ), patch(
        "backend.services.update_service.subprocess.Popen"
    ) as popen:
        service._spawn_update_script(
            "migrate",
            state.old_commit,
            state.backup_file,
            state=state,
        )

    worker_argv = popen.call_args.args[0]
    assert worker_argv[-2] == "repair"
    assert service._read_deployment_lease()["operation"] == "repair"


@pytest.mark.asyncio
async def test_forced_same_commit_update_adopts_repair_before_continuing(
    tmp_path,
):
    commit = "a" * 40
    service = _service(tmp_path, running_commit=commit)
    state = _state(
        status="running",
        old_commit=commit,
        new_commit="",
        operation="update",
    )
    service._current = state
    assert service._claim_deployment_lease("update")
    _configure_same_commit_update_pipeline(service, commit)
    service._complete_step = AsyncMock(
        side_effect=RuntimeError("stop after semantic transition")
    )

    with pytest.raises(
        RuntimeError, match="stop after semantic transition"
    ):
        await service._pipeline_inner(
            state,
            skip_frontend_build=False,
            force=True,
        )

    assert state.operation == "repair"
    assert state.deployment_incomplete is True
    lease = service._read_deployment_lease()
    assert lease["operation"] == "repair"
    assert lease["deployment_incomplete"] is True


@pytest.mark.asyncio
async def test_same_commit_repair_transition_failure_stops_pipeline(
    tmp_path,
):
    commit = "a" * 40
    service = _service(tmp_path, running_commit=commit)
    state = _state(
        status="running",
        old_commit=commit,
        new_commit="",
        operation="update",
    )
    service._current = state
    assert service._claim_deployment_lease("update")
    _configure_same_commit_update_pipeline(service, commit)
    service._database_revision_status = AsyncMock(
        return_value={
            "database_up_to_date": False,
            "database_revision_error": "",
        }
    )
    service._repair_inner = AsyncMock()
    service._backup_database = AsyncMock()
    real_lease_update = service._update_deployment_lease

    def update_lease(**updates):
        if updates.get("operation") == "repair":
            return False
        return real_lease_update(**updates)

    service._update_deployment_lease = MagicMock(
        side_effect=update_lease
    )

    await service._pipeline_inner(
        state,
        skip_frontend_build=False,
        force=False,
    )

    assert state.status == "failed"
    assert state.operation == "repair"
    assert state.deployment_incomplete is True
    assert "修复事务" in state.error
    service._repair_inner.assert_not_awaited()
    service._backup_database.assert_not_awaited()
    assert service._read_deployment_lease()["operation"] == "update"


@pytest.mark.asyncio
async def test_restart_requires_repair_for_stale_runtime_code(tmp_path):
    service = _service(tmp_path)
    service._inspect_environment = AsyncMock(
        return_value={
            "repair_required": True,
            "repair_reasons": ["runtime stale"],
            "database_up_to_date": True,
        }
    )
    service._pause_dispatching = AsyncMock()

    result = await service.restart()

    assert result["repair_required"] is True
    service._pause_dispatching.assert_not_awaited()


@pytest.mark.asyncio
async def test_repair_cancellation_after_claim_releases_lease(tmp_path):
    service = _service(tmp_path)
    service._pause_dispatching = AsyncMock()
    service._resume_dispatching = MagicMock()
    service._get_blocking_tasks = AsyncMock(return_value=[])
    entered = asyncio.Event()
    never = asyncio.Event()

    async def blocked_disk_commit():
        entered.set()
        await never.wait()
        return "b" * 40

    service._disk_commit = blocked_disk_commit
    task = asyncio.create_task(service.start_repair())
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    lease = service._read_deployment_lease()
    assert lease["status"] == "failed"
    assert service._lease_token is None
    service._resume_dispatching.assert_called_once()


@pytest.mark.asyncio
async def test_maintenance_repair_never_queries_incompatible_orm(tmp_path):
    def broken_factory():
        raise AssertionError("ORM must not be touched in maintenance mode")

    script_dir = tmp_path / "scripts"
    script_dir.mkdir(parents=True)
    script = script_dir / "update_migrate.sh"
    script.write_text(
        "#!/bin/bash\nCCM_UPDATE_PROTOCOL_VERSION=2\nexit 0\n"
    )
    script.chmod(0o700)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    service = UpdateService(
        broadcaster,
        port=18766,
        project_dir=str(tmp_path),
        db_factory=broken_factory,
        running_commit="a" * 40,
        update_runtime_root=tmp_path / ".update-runtime",
        legacy_update_runtime_root=tmp_path / ".legacy-update-runtime",
    )
    service._status_file = tmp_path / "status.json"
    service._journal_file = tmp_path / "backups" / "status.json"
    service._lease_file = tmp_path / "backups" / "deployment-lease.json"
    service._lease_lock_file = tmp_path / "backups" / "deployment-lease.lock"
    service.maintenance_only = True
    service._pause_dispatching = AsyncMock()
    service._resume_dispatching = MagicMock()
    service._disk_commit = AsyncMock(return_value="b" * 40)
    service._dirty_worktree_files = AsyncMock(return_value=[])

    with patch(
        "backend.services.update_service.asyncio.create_task"
    ) as create_task:
        result = await service.start_repair()

    assert result["status"] == "started"
    create_task.assert_called_once()
    create_task.call_args.args[0].close()


@pytest.mark.asyncio
async def test_update_lease_io_error_reopens_dispatcher_gate(tmp_path):
    service = _service(tmp_path)
    service._pause_dispatching = AsyncMock()
    service._resume_dispatching = MagicMock()
    service._get_blocking_tasks = AsyncMock(return_value=[])
    service._claim_deployment_lease = MagicMock(
        side_effect=OSError("disk full")
    )

    with pytest.raises(OSError, match="disk full"):
        await service.start_update()

    service._resume_dispatching.assert_called_once()


@pytest.mark.asyncio
async def test_late_rollback_blocker_releases_lease_and_preserves_retry_record(
    tmp_path,
):
    service = _service(tmp_path, running_commit="b" * 40)
    service._current = _state(
        status="completed",
        backup_file=str(tmp_path / "before.db"),
        frontend_dist_backup=str(tmp_path / "before-dist"),
        database_migration_required=False,
        database_migration_applied=False,
    )
    blocker = {
        "id": 91,
        "title": "late task",
        "status": "executing",
    }
    # Initial admission, post-exclusive-claim race check, and final shutdown
    # barrier respectively.
    service._get_blocking_tasks = AsyncMock(
        side_effect=[[], [], [blocker]]
    )
    service._pause_dispatching = AsyncMock()
    service._resume_dispatching = MagicMock()
    service._spawn_update_script = MagicMock()

    with patch(
        "backend.services.update_service.asyncio.sleep",
        new=AsyncMock(),
    ):
        result = await service.rollback()

    assert result["update_blocked"] is True
    lease = service._read_deployment_lease()
    assert lease["status"] == "failed"
    assert lease["handoff"] is False
    assert lease["old_commit"] == "a" * 40
    assert lease["new_commit"] == "b" * 40
    assert lease["backup_file"] == str(tmp_path / "before.db")
    assert lease["frontend_dist_backup"] == str(
        tmp_path / "before-dist"
    )
    service._spawn_update_script.assert_not_called()
    service._resume_dispatching.assert_called_once()

    recovered = _service(tmp_path, running_commit="b" * 40)
    recovered.recover_from_status_file()

    assert recovered._current is not None
    assert recovered._current.status == "failed"
    assert recovered._current.old_commit == "a" * 40
    assert recovered._current.new_commit == "b" * 40
    assert recovered._current.backup_file == str(tmp_path / "before.db")
    assert recovered._current.old_commit != recovered._current.new_commit


@pytest.mark.asyncio
async def test_cancelled_post_claim_rollback_preserves_incomplete_fence(
    tmp_path,
):
    service = _service(tmp_path, running_commit="b" * 40)
    service._current = _state(
        status="failed",
        backup_file=str(tmp_path / "before.db"),
        deployment_incomplete=True,
        database_migration_required=False,
        database_migration_applied=False,
    )
    service._pause_dispatching = AsyncMock()
    service._resume_dispatching = MagicMock()
    second_check_started = asyncio.Event()
    never_finishes = asyncio.Event()
    calls = 0

    async def blocker_check():
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        second_check_started.set()
        await never_finishes.wait()
        return []

    service._get_blocking_tasks = AsyncMock(side_effect=blocker_check)
    request = asyncio.create_task(service.rollback())
    await asyncio.wait_for(second_check_started.wait(), timeout=2)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    lease = service._read_deployment_lease()
    assert lease["status"] == "failed"
    assert lease["handoff"] is False
    assert lease["deployment_incomplete"] is True
    assert lease["old_commit"] == "a" * 40
    assert lease["new_commit"] == "b" * 40
    assert lease["backup_file"] == str(tmp_path / "before.db")
    assert service._lease_token is None
    service._resume_dispatching.assert_called_once()


def test_rollback_handoff_targets_old_commit(tmp_path):
    service = _service(tmp_path)
    service._current = _state(status="restarting")
    assert service._claim_deployment_lease("rollback", allow_failed=True)

    service._mark_deployment_handoff("rollback_code")

    lease = service._read_deployment_lease()
    assert lease["expected_commit"] == "a" * 40


def test_spawn_passes_protocol_v2_run_copy_directory(tmp_path):
    service = _service(tmp_path)
    state = _state(status="restarting", operation="restart")
    service._current = state
    assert service._claim_deployment_lease("restart")

    with patch.object(service, "_systemd_scope", return_value=None), patch(
        "backend.services.update_service.subprocess.Popen"
    ) as popen:
        service._spawn_update_script(
            "restart",
            state.old_commit,
            "",
            state=state,
            restart_failure_policy="retry",
        )

    argv = popen.call_args.args[0]
    assert len(argv) >= 19  # bash + script + 18 protocol arguments
    run_dir = Path(argv[-1])
    assert run_dir.name.startswith("ccm-update-run-")
    assert Path(argv[1]).parent == run_dir
    service.close_runtime_snapshot()
    assert Path(argv[1]).is_file()


def test_ambiguous_systemd_launch_keeps_lease_and_run_copy(tmp_path):
    service = _service(tmp_path)
    state = _state(status="restarting", operation="restart")
    service._current = state
    assert service._claim_deployment_lease("restart")
    service._mark_deployment_handoff("restart")
    launcher = MagicMock()
    launcher.wait.side_effect = subprocess.TimeoutExpired(
        cmd="systemd-run", timeout=15
    )
    thread = MagicMock()

    with patch.object(service, "_systemd_scope", return_value="user"), patch(
        "backend.services.update_service.subprocess.Popen",
        return_value=launcher,
    ), patch(
        "backend.services.update_service.threading.Thread",
        return_value=thread,
    ), patch(
        "backend.services.update_service.shutil.rmtree"
    ) as remove:
        service._spawn_update_script(
            "restart",
            state.old_commit,
            "",
            state=state,
            restart_failure_policy="retry",
        )

    lease = service._read_deployment_lease()
    assert lease["status"] == "restarting"
    assert lease["handoff"] is True
    assert Path(lease["run_copy_dir"]).is_dir()
    launcher.kill.assert_not_called()
    remove.assert_not_called()
    thread.start.assert_called_once()


def test_nonzero_systemd_launch_ack_keeps_lease_and_run_copy(tmp_path):
    service = _service(tmp_path)
    state = _state(status="restarting", operation="restart")
    service._current = state
    assert service._claim_deployment_lease("restart")
    service._mark_deployment_handoff("restart")
    launcher = MagicMock()
    launcher.wait.return_value = 1

    with patch.object(service, "_systemd_scope", return_value="user"), patch(
        "backend.services.update_service.subprocess.Popen",
        return_value=launcher,
    ), patch(
        "backend.services.update_service.shutil.rmtree"
    ) as remove:
        service._spawn_update_script(
            "restart",
            state.old_commit,
            "",
            state=state,
            restart_failure_policy="retry",
        )

    lease = service._read_deployment_lease()
    assert lease["status"] == "restarting"
    assert lease["handoff"] is True
    assert Path(lease["run_copy_dir"]).is_dir()
    launcher.kill.assert_not_called()
    remove.assert_not_called()


@pytest.mark.asyncio
async def test_frontend_snapshot_records_that_dist_was_absent(tmp_path):
    service = _service(tmp_path)
    (tmp_path / "frontend").mkdir()

    snapshot = Path(await service._backup_frontend_dist())

    assert snapshot.is_dir()
    assert (snapshot / ".ccm-dist-absent").is_file()
