"""Phase 3 测试：TaskMigrator 状态机 / PUT 触发迁移 / 销毁批量迁回。"""
import asyncio
import threading
from pathlib import Path, PurePosixPath
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update

import backend.main as main_module
import backend.services.task_migrator as task_migrator_module
from backend.models.capability import (
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.task import Task
from backend.models.plan import Plan
from backend.models.plan_agent import PlanAgentRun
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.task_migrator import (
    MigrationError,
    TaskMigrator,
    migration_task_generation,
)
from backend.services.pr_review_runtime import PRE_PR_CODE_REVIEW_TAG
from backend.services.worker_proxy import (
    WorkerProxy,
    capture_worker_destroy_lifecycle_claim,
    get_task_operation_lock,
)
from backend.services.worker_relay import (
    LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY,
    WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY,
)


class FakeRelay:
    def __init__(self):
        self.subscribed: list[tuple[int, int]] = []
        self.unsubscribed: list[tuple[int, int]] = []

    async def subscribe_task(self, worker, task_id):
        self.subscribed.append((worker.id, task_id))

    def unsubscribe_task(self, worker_id, task_id):
        self.unsubscribed.append((worker_id, task_id))


async def _mk_worker(session_factory, **fields) -> Worker:
    fields.setdefault("status", "ready")
    fields.setdefault("private_ip", "10.0.0.9")
    fields.setdefault("auth_token", "t")
    async with session_factory() as db:
        w = Worker(name=fields.pop("name", "w"), **fields)
        db.add(w)
        await db.commit()
        await db.refresh(w)
        return w


async def _mk_task(session_factory, **fields) -> Task:
    fields.setdefault("status", "completed")
    fields.setdefault("description", "d")
    async with session_factory() as db:
        t = Task(title="t", **fields)
        db.add(t)
        await db.commit()
        await db.refresh(t)
        return t


async def _mk_active_resume_outbox(session_factory, task_id: int) -> int:
    digest = "d" * 64
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        invocation = CapabilityInvocation(
            task_id=task.id,
            capability_key="plan",
            source="human_request",
            purpose="advisory",
            status="failed",
            state_version=2,
            idempotency_key=f"migration-outbox-{task.id}",
            input_payload={},
            input_hash=digest,
            subject_kind="task_generation",
            subject_ref={"task_id": task.id},
            subject_hash=digest,
            executor_kind="fake",
            executor_config={},
            executor_config_hash=digest,
            policy_snapshot={},
            policy_hash=digest,
            resume_policy="attach_only",
            max_attempts=1,
            active_task_id=None,
            error_code="settled",
        )
        db.add(invocation)
        await db.flush()
        outbox = CapabilityResumeOutbox(
            task_id=task.id,
            invocation_id=invocation.id,
            active_task_id=task.id,
            active_invocation_id=invocation.id,
            status="pending",
            state_version=1,
            request_task_incarnation_id=task.incarnation_id,
            request_task_retry_count=task.retry_count,
            from_turn_generation=task.turn_generation,
            request_task_session_id=task.session_id,
            request_source_log_id=201,
            request_output_log_id=202,
            request_terminal_log_id=203,
        )
        db.add(outbox)
        await db.commit()
        return outbox.id


def _migrator(db_factory, relay=None) -> TaskMigrator:
    m = TaskMigrator(db_factory=db_factory, relay=relay or FakeRelay(), broadcaster=None)
    # 文件搬运全替身（不碰 SSH/磁盘）
    m._sync_workspace = AsyncMock()
    m._move_session = AsyncMock()
    m._move_codex_session = AsyncMock()
    m._sync_task_fields_from_worker = AsyncMock()
    m._ensure_worker_task = AsyncMock()
    return m


@pytest.mark.asyncio
async def test_api_account_retirement_and_task_migration_are_mutually_exclusive():
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())

    async with migrator._migration_account_guard():
        with pytest.raises(MigrationError, match="migration"):
            async with migrator.api_account_retirement_guard():
                pass

    async with migrator.api_account_retirement_guard():
        with pytest.raises(MigrationError, match="deletion"):
            async with migrator._migration_account_guard():
                pass


async def test_migrate_local_to_worker(db_factory, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, session_id="sess-1")
    relay = FakeRelay()
    m = _migrator(db_factory, relay)
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 9
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    await m.migrate(t.id, w.id)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.worker_id == w.id
    assert task.status == "completed"  # 迁移后状态复原
    assert (w.id, t.id) in relay.subscribed
    m._move_session.assert_called_once()
    m._ensure_worker_task.assert_called_once()


@pytest.mark.parametrize(
    ("tags", "metadata"),
    [
        ([PRE_PR_CODE_REVIEW_TAG], {}),
        (
            [],
            {
                "code_review_run_id": 11,
                "capability_invocation_id": 12,
                "capability_execution_id": 13,
            },
        ),
    ],
)
async def test_pre_pr_review_task_cannot_migrate_out_of_sandbox(
    db_factory,
    session_factory,
    tags,
    metadata,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        tags=tags,
        metadata_=metadata,
    )
    migrator = _migrator(db_factory)

    with pytest.raises(MigrationError, match="Automated PR workflow"):
        await migrator.migrate(task.id, worker.id)

    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()


async def test_auto_capability_policy_rejects_migration_before_side_effects(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        capability_policy={
            "version": 1,
            "max_invocations": 1,
            "capabilities": {"plan": 1},
        },
    )
    relay = FakeRelay()
    migrator = _migrator(db_factory, relay)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="immutable and local-only"):
        await migrator.migrate(task.id, worker.id)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    assert relay.subscribed == []
    assert relay.unsubscribed == []


async def test_waiting_capability_rejects_migration_before_side_effects(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, status="waiting_capability")
    relay = FakeRelay()
    migrator = _migrator(db_factory, relay)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="waiting.*capability"):
        await migrator.migrate(task.id, worker.id)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    assert relay.subscribed == []
    assert relay.unsubscribed == []


async def test_active_capability_resume_outbox_rejects_migration(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory)
    await _mk_active_resume_outbox(session_factory, task.id)
    relay = FakeRelay()
    migrator = _migrator(db_factory, relay)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="active capability resume outbox"):
        await migrator.migrate(task.id, worker.id)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    assert relay.subscribed == []
    assert relay.unsubscribed == []


async def test_capability_resume_outbox_wins_before_migration_claim(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory)
    relay = FakeRelay()
    migrator = _migrator(db_factory, relay)

    async def admit_resume_before_claim(_worker_id):
        await _mk_active_resume_outbox(session_factory, task.id)
        return worker

    migrator._get_worker = AsyncMock(side_effect=admit_resume_before_claim)

    with pytest.raises(MigrationError, match="active capability resume outbox"):
        await migrator.migrate(task.id, worker.id)

    migrator._get_worker.assert_awaited_once_with(worker.id)
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    assert relay.subscribed == []
    assert relay.unsubscribed == []


@pytest.mark.parametrize("source_is_worker", [False, True])
async def test_plan_task_rejects_migration_before_side_effects(
    db_factory,
    session_factory,
    source_is_worker,
):
    source = (
        await _mk_worker(session_factory, name="source", private_ip="10.0.0.8")
        if source_is_worker
        else None
    )
    target = await _mk_worker(
        session_factory,
        name="target",
        private_ip="10.0.0.9",
    )
    task = await _mk_task(
        session_factory,
        mode="plan",
        plan_approved=True,
        plan_content="# Legacy approved Plan",
        worker_id=source.id if source is not None else None,
    )
    relay = FakeRelay()
    migrator = _migrator(db_factory, relay)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="Plan Tasks cannot be migrated"):
        await migrator.migrate(task.id, target.id)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._move_codex_session.assert_not_awaited()
    migrator._sync_task_fields_from_worker.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    assert relay.subscribed == []
    assert relay.unsubscribed == []
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.worker_id == (source.id if source is not None else None)
        assert current.status == "completed"


@pytest.mark.parametrize(
    ("plan_status", "approved"),
    [
        ("in_progress", None),
        ("plan_review", None),
        ("completed", True),
    ],
)
async def test_target_migration_blocks_actionable_related_plans(
    db_factory,
    session_factory,
    plan_status,
    approved,
):
    worker = await _mk_worker(session_factory)
    target = await _mk_task(session_factory)
    await _mk_task(
        session_factory,
        mode="plan",
        status=plan_status,
        plan_target_task_id=target.id,
        plan_approved=approved,
        plan_applied_at=None,
    )

    with pytest.raises(MigrationError, match="待审批或待应用"):
        await _migrator(db_factory).migrate(target.id, worker.id)


async def test_target_migration_blocks_first_class_plan_active_run(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    target = await _mk_task(session_factory)
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Versioned related Plan",
            initial_request="Plan it",
            target_task_id=target.id,
            pipeline_config=pipeline,
            priority=0,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="initial",
            request_text="Plan it",
            pipeline_config=pipeline,
            status="waiting_user",
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()

    with pytest.raises(MigrationError, match="active Run"):
        await _migrator(db_factory).migrate(target.id, worker.id)


async def test_target_migration_rechecks_plan_created_after_claim(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    target = await _mk_task(session_factory, status="completed")
    migrator = _migrator(db_factory)
    real_claim = migrator._claim_migration
    pipeline = default_plan_pipeline_config().model_dump(mode="json")

    async def claim_then_create_plan(observed, **kwargs):
        claimed = await real_claim(observed, **kwargs)
        async with session_factory() as db:
            plan = Plan(
                title="Racing Plan",
                initial_request="Plan during migration",
                target_task_id=target.id,
                pipeline_config=pipeline,
                priority=0,
            )
            db.add(plan)
            await db.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                run_type="initial",
                request_text="Plan during migration",
                pipeline_config=pipeline,
                status="queued",
            )
            db.add(run)
            await db.flush()
            plan.active_run_id = run.id
            await db.commit()
        return claimed

    migrator._claim_migration = claim_then_create_plan

    with pytest.raises(MigrationError, match="active Run"):
        await migrator.migrate(target.id, worker.id)

    async with session_factory() as db:
        restored = await db.get(Task, target.id)
    assert restored.status == "completed"
    assert restored.worker_id is None


async def test_related_plan_cannot_migrate_independently(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    target = await _mk_task(session_factory)
    plan = await _mk_task(
        session_factory,
        mode="plan",
        status="completed",
        plan_target_task_id=target.id,
        plan_approved=True,
        plan_applied_at=None,
    )

    with pytest.raises(MigrationError, match="不能脱离目标 Task"):
        await _migrator(db_factory).migrate(plan.id, worker.id)


@pytest.mark.parametrize(
    "source_is_worker",
    [False, True],
    ids=["local-to-worker", "worker-to-worker"],
)
@pytest.mark.parametrize(
    "local_collision",
    [False, True],
    ids=["missing-local-row", "colliding-local-row"],
)
async def test_coordinated_migration_imports_and_commits_final_skill_tuple(
    db_factory,
    session_factory,
    monkeypatch,
    source_is_worker,
    local_collision,
):
    from backend.models.user_skill import UserSkill

    source = (
        await _mk_worker(session_factory, name="source")
        if source_is_worker
        else None
    )
    destination = await _mk_worker(
        session_factory,
        name="destination",
        private_ip="10.0.0.10",
    )
    task = await _mk_task(
        session_factory,
        worker_id=source.id if source else None,
        provider="claude",
        enabled_skills={},
        selected_user_skills=None,
        metadata_={"existing": "value"},
    )
    snapshots = [{
        "id": 81,
        "name": "Personal Review",
        "description": "Review checklist",
        "content": "Check the final diff.",
    }]
    final_metadata = {
        "existing": "value",
        "ccm_user_skill_snapshots": snapshots,
    }
    if local_collision:
        async with session_factory() as db:
            db.add(UserSkill(
                id=81,
                name="Wrong local collision",
                description="must not replace Manager snapshot",
                content="wrong local body",
            ))
            await db.commit()
    task_updates = {
        "provider": "codex",
        "enabled_skills": {"sub-agent": True},
        "selected_user_skills": [81],
        "metadata_": final_metadata,
    }
    requests = []

    class Response:
        status_code = 201
        text = ""

        def __init__(self, status, retry_count, turn_generation):
            self.status = status
            self.retry_count = retry_count
            self.turn_generation = turn_generation

        def json(self):
            return {
                "status": self.status,
                "retry_count": self.retry_count,
                "turn_generation": self.turn_generation,
            }

        @staticmethod
        def raise_for_status():
            return None

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, *, headers, json):
            requests.append((url, headers, json))
            return Response(
                json["source_status"],
                json["retry_count"],
                json["turn_generation"],
            )

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 17
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    migrator = _migrator(db_factory)
    migrator._ensure_worker_task = (
        TaskMigrator._ensure_worker_task.__get__(migrator, TaskMigrator)
    )

    await migrator.migrate(
        task.id,
        destination.id,
        task_updates=task_updates,
    )

    assert len(requests) == 1
    _url, _headers, payload = requests[0]
    assert payload["provider"] == "codex"
    assert payload["source_status"] == "completed"
    assert payload["enabled_skills"] == {"sub-agent": True}
    assert payload["selected_user_skills"] == [81]
    assert payload["user_skill_snapshots"] == snapshots
    async with session_factory() as db:
        persisted = await db.get(Task, task.id)
    assert persisted.worker_id == destination.id
    assert persisted.provider == payload["provider"]
    assert persisted.enabled_skills == payload["enabled_skills"]
    assert persisted.selected_user_skills == payload["selected_user_skills"]
    assert persisted.metadata_["ccm_user_skill_snapshots"] == snapshots


async def test_coordinated_migration_failure_keeps_original_manager_config(
    db_factory,
    session_factory,
    monkeypatch,
):
    destination = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        provider="claude",
        enabled_skills={"monitor": True},
        selected_user_skills=None,
        metadata_={"original": True},
        status="failed",
    )
    migrator = _migrator(db_factory)
    migrator._ensure_worker_task = AsyncMock(
        side_effect=RuntimeError("destination import failed")
    )
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 17
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    with pytest.raises(RuntimeError, match="destination import failed"):
        await migrator.migrate(
            task.id,
            destination.id,
            task_updates={
                "provider": "codex",
                "enabled_skills": {"sub-agent": True},
                "selected_user_skills": [81],
                "metadata_": {
                    "ccm_user_skill_snapshots": [{
                        "id": 81,
                        "name": "Personal Review",
                        "description": "",
                        "content": "Review.",
                    }],
                },
            },
        )

    async with session_factory() as db:
        persisted = await db.get(Task, task.id)
    assert persisted.worker_id is None
    assert persisted.status == "failed"
    assert persisted.provider == "claude"
    assert persisted.enabled_skills == {"monitor": True}
    assert persisted.selected_user_skills is None
    assert persisted.metadata_ == {"original": True}


async def test_coordinated_migration_claim_cas_preserves_concurrent_config(
    db_factory,
    session_factory,
    monkeypatch,
):
    destination = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        provider="claude",
        enabled_skills={},
    )
    migrator = _migrator(db_factory)
    real_get_worker = migrator._get_worker

    async def update_while_validating(worker_id):
        worker = await real_get_worker(worker_id)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(enabled_skills={"concurrent": True})
            )
            await db.commit()
        return worker

    monkeypatch.setattr(
        migrator,
        "_get_worker",
        update_while_validating,
    )

    with pytest.raises(MigrationError):
        await migrator.migrate(
            task.id,
            destination.id,
            task_updates={"enabled_skills": {"sub-agent": True}},
        )

    async with session_factory() as db:
        persisted = await db.get(Task, task.id)
    assert persisted.worker_id is None
    assert persisted.status == "completed"
    assert persisted.enabled_skills == {"concurrent": True}
    migrator._ensure_worker_task.assert_not_called()


async def test_migrate_worker_to_local(db_factory, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, session_id="sess-1")
    relay = FakeRelay()
    m = _migrator(db_factory, relay)

    await m.migrate(t.id, None)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.worker_id is None
    assert (w.id, t.id) in relay.unsubscribed
    m._sync_task_fields_from_worker.assert_called_once()


async def test_migrate_rejects_executing(db_factory, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, status="executing")
    m = _migrator(db_factory)
    with pytest.raises(MigrationError, match="先停止"):
        await m.migrate(t.id, w.id)


async def test_migrate_rejects_in_progress(db_factory, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, status="in_progress")
    m = _migrator(db_factory)
    with pytest.raises(MigrationError, match="先停止"):
        await m.migrate(t.id, w.id)


async def test_migrate_rejects_local_merging_before_side_effects(
    db_factory,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, status="merging")
    migrator = _migrator(db_factory)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="merging"):
        await migrator.migrate(task.id, worker.id)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._move_codex_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.status == "merging"


@pytest.mark.parametrize("status", ["pending", "in_progress", "executing", "merging", "migrating"])
async def test_migrate_rejects_non_inert_worker_source_before_side_effects(
    db_factory,
    session_factory,
    status,
):
    source = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=source.id,
        status=status,
    )
    migrator = _migrator(db_factory)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="inert"):
        await migrator.migrate(task.id, None)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_task_fields_from_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._move_codex_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == source.id
    assert current.status == status


@pytest.mark.parametrize(
    "marker_key",
    [
        WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY,
        LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY,
    ],
)
async def test_migrate_rejects_worker_execution_quarantine_before_side_effects(
    db_factory,
    session_factory,
    marker_key,
):
    source = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=source.id,
        status="completed",
        metadata_={marker_key: {"malformed": "still fail closed"}},
    )
    migrator = _migrator(db_factory)
    migrator._get_worker = AsyncMock()

    with pytest.raises(MigrationError, match="quarantined"):
        await migrator.migrate(task.id, None)

    migrator._get_worker.assert_not_awaited()
    migrator._sync_task_fields_from_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._move_codex_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == source.id
    assert current.status == "completed"


async def test_migration_rechecks_worker_quarantine_after_claim(
    db_factory,
    session_factory,
    monkeypatch,
):
    """A marker written during Worker validation blocks every copy/import."""

    source = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=source.id,
        status="completed",
        metadata_={"manager": "owned"},
    )
    migrator = _migrator(db_factory)
    real_get_worker = migrator._get_worker

    async def quarantine_while_validating(worker_id):
        worker = await real_get_worker(worker_id)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    metadata_={
                        "manager": "owned",
                        WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY: {
                            "operation": "stop-session",
                        },
                    }
                )
            )
            await db.commit()
        return worker

    monkeypatch.setattr(
        migrator,
        "_get_worker",
        quarantine_while_validating,
    )

    with pytest.raises(MigrationError, match="became quarantined"):
        await migrator.migrate(task.id, None)

    migrator._sync_task_fields_from_worker.assert_not_awaited()
    migrator._sync_workspace.assert_not_awaited()
    migrator._move_session.assert_not_awaited()
    migrator._move_codex_session.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == source.id
    assert current.status == "completed"
    assert WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY in current.metadata_


async def test_migration_claim_cas_preserves_concurrent_dispatcher_claim(
    db_factory, session_factory, monkeypatch,
):
    """A state change during Worker validation must beat migration's CAS."""
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, status="pending")
    m = _migrator(db_factory)
    real_get_worker = m._get_worker

    async def claim_while_validating(worker_id):
        worker = await real_get_worker(worker_id)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == t.id, Task.status == "pending")
                .values(status="in_progress")
            )
            await db.commit()
        return worker

    monkeypatch.setattr(m, "_get_worker", claim_while_validating)

    with pytest.raises(MigrationError, match="并发修改"):
        await m.migrate(t.id, w.id)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "in_progress"
    assert task.worker_id is None
    m._sync_workspace.assert_not_called()


async def test_migration_claim_rejects_same_status_retry_aba(
    db_factory, session_factory, monkeypatch,
):
    """Status equality cannot hide a newer retry generation."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, status="pending")
    migrator = _migrator(db_factory)
    real_get_worker = migrator._get_worker

    async def retry_aba_while_validating(worker_id):
        current_worker = await real_get_worker(worker_id)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(retry_count=Task.retry_count + 1)
            )
            await db.commit()
        return current_worker

    monkeypatch.setattr(
        migrator,
        "_get_worker",
        retry_aba_while_validating,
    )

    with pytest.raises(MigrationError, match="并发修改"):
        await migrator.migrate(task.id, worker.id)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.retry_count == task.retry_count + 1
    migrator._sync_workspace.assert_not_called()


async def test_migration_claim_rejects_turn_generation_only_aba(
    db_factory,
    session_factory,
    monkeypatch,
):
    """An unchanged status/retry tuple cannot hide a newly admitted turn."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        status="pending",
        turn_generation=7,
    )
    migrator = _migrator(db_factory)
    real_get_worker = migrator._get_worker

    async def advance_turn_while_validating(worker_id):
        current_worker = await real_get_worker(worker_id)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(turn_generation=Task.turn_generation + 1)
            )
            await db.commit()
        return current_worker

    monkeypatch.setattr(
        migrator,
        "_get_worker",
        advance_turn_while_validating,
    )

    with pytest.raises(MigrationError, match="并发修改"):
        await migrator.migrate(task.id, worker.id)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.retry_count == task.retry_count
    assert current.turn_generation == task.turn_generation + 1
    migrator._sync_workspace.assert_not_called()


async def test_migration_and_worker_proxy_share_operation_lock(
    db_factory, session_factory, monkeypatch,
):
    """Migration waits for an in-flight Worker mutation on the same task."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory)
    migrator = _migrator(db_factory)
    proxy = WorkerProxy(session_factory, migrator.relay)
    proxy.ensure_worker_project = AsyncMock(return_value=9)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    operation_lock = get_task_operation_lock(task.id)
    assert proxy.task_operation_lock(task.id) is operation_lock
    await operation_lock.acquire()
    migration = asyncio.create_task(migrator.migrate(task.id, worker.id))
    await asyncio.sleep(0)
    assert not migration.done()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "completed"
    assert current.worker_id is None

    operation_lock.release()
    await migration
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "completed"
    assert current.worker_id == worker.id


async def test_migrate_noop_when_already_there(db_factory, session_factory):
    t = await _mk_task(session_factory)  # 本机
    m = _migrator(db_factory)
    await m.migrate(t.id, None)  # 不抛错、无副作用
    m._move_session.assert_not_called()


async def test_migrate_rejects_unready_target(db_factory, session_factory):
    w = await _mk_worker(session_factory, status="stopped")
    t = await _mk_task(session_factory)
    m = _migrator(db_factory)
    with pytest.raises(MigrationError, match="不可用"):
        await m.migrate(t.id, w.id)


async def test_migration_finish_rejects_target_destroying_after_remote_create(
    db_factory,
    session_factory,
    monkeypatch,
):
    """Destroy and the final pointer cut arbitrate on the target Worker row."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, status="completed")
    relay = FakeRelay()
    migrator = _migrator(db_factory, relay)
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 9
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    async def destroy_after_remote_task_created(*_args, **_kwargs):
        async with session_factory() as db:
            claimed = await db.execute(
                update(Worker)
                .where(
                    Worker.id == worker.id,
                    Worker.status == "ready",
                )
                .values(status="destroying")
            )
            assert claimed.rowcount == 1
            await db.commit()

    migrator._ensure_worker_task = AsyncMock(
        side_effect=destroy_after_remote_task_created
    )

    with pytest.raises(MigrationError, match="不再 ready"):
        await migrator.migrate(task.id, worker.id)

    async with session_factory() as db:
        current_task = await db.get(Task, task.id)
        current_worker = await db.get(Worker, worker.id)
    assert current_task.status == "completed"
    assert current_task.worker_id is None
    assert current_worker.status == "destroying"
    assert (worker.id, task.id) in relay.subscribed
    assert (worker.id, task.id) in relay.unsubscribed


async def test_migrate_rejects_unready_source(db_factory, session_factory):
    w = await _mk_worker(session_factory, status="stopped")
    t = await _mk_task(session_factory, worker_id=w.id)
    m = _migrator(db_factory)
    with pytest.raises(MigrationError, match="源 Worker"):
        await m.migrate(t.id, None)


async def test_migrate_failure_restores_status(db_factory, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, session_id="s", status="failed")
    m = _migrator(db_factory)
    m._move_session = AsyncMock(side_effect=RuntimeError("rsync down"))
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())

    with pytest.raises(RuntimeError):
        await m.migrate(t.id, w.id)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "failed"      # 复原
    assert task.worker_id is None       # 指针没切


async def test_migration_cancellation_after_claim_settles_exact_rollback(
    db_factory,
    session_factory,
):
    """Cancellation after claim COMMIT cannot strand the task in migrating."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, status="failed")
    migrator = _migrator(db_factory)
    claim_committed = asyncio.Event()
    release_claim = asyncio.Event()
    rollback_started = asyncio.Event()
    release_rollback = asyncio.Event()
    real_claim = migrator._claim_migration
    real_restore = migrator._restore_migration_claim

    async def claim_then_pause(observed):
        claimed = await real_claim(observed)
        claim_committed.set()
        await release_claim.wait()
        return claimed

    async def restore_then_pause(claimed, restored_status):
        rollback_started.set()
        await release_rollback.wait()
        return await real_restore(claimed, restored_status)

    migrator._claim_migration = claim_then_pause
    migrator._restore_migration_claim = restore_then_pause

    migration = asyncio.create_task(migrator.migrate(task.id, worker.id))
    await asyncio.wait_for(claim_committed.wait(), timeout=1)
    async with session_factory() as db:
        claimed_task = await db.get(Task, task.id)
    assert claimed_task.status == "migrating"

    migration.cancel()
    release_claim.set()
    await asyncio.wait_for(rollback_started.wait(), timeout=1)
    # A second cancellation while rollback is blocked must not interrupt the
    # exact-generation restore.
    migration.cancel()
    release_rollback.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(migration, timeout=1)

    async with session_factory() as db:
        restored = await db.get(Task, task.id)
    assert restored.status == "failed"
    assert restored.worker_id is None
    assert not migrator._locks[task.id].locked()


async def test_migration_failure_does_not_overwrite_concurrent_status(
    db_factory, session_factory, monkeypatch,
):
    """Rollback is a CAS too: a concurrent cancellation must remain final."""
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, session_id="s", status="failed")
    m = _migrator(db_factory)

    async def cancel_then_fail(*_args):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == t.id, Task.status == "migrating")
                .values(status="cancelled")
            )
            await db.commit()
        raise RuntimeError("rsync down")

    m._move_session = AsyncMock(side_effect=cancel_then_fail)
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())

    with pytest.raises(RuntimeError, match="rsync down"):
        await m.migrate(t.id, w.id)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "cancelled"
    assert task.worker_id is None


async def test_migration_rollback_rejects_same_status_generation_aba(
    db_factory, session_factory, monkeypatch,
):
    """Rollback cannot restore an old claim after retry_count changes."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        session_id="s",
        status="failed",
    )
    migrator = _migrator(db_factory)

    async def replace_generation_then_fail(*_args):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id, Task.status == "migrating")
                .values(retry_count=Task.retry_count + 1)
            )
            await db.commit()
        raise RuntimeError("rsync down")

    migrator._move_session = AsyncMock(
        side_effect=replace_generation_then_fail
    )
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 9
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    with pytest.raises(RuntimeError, match="rsync down"):
        await migrator.migrate(task.id, worker.id)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "migrating"
    assert current.retry_count == task.retry_count + 1
    assert current.worker_id is None


async def test_migration_finish_and_rollback_reject_turn_generation_only_aba(
    db_factory,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        turn_generation=11,
    )
    migrator = _migrator(db_factory)
    claimed = await migrator._claim_migration(
        migration_task_generation(task)
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(turn_generation=Task.turn_generation + 1)
        )
        await db.commit()

    restored = await migrator._restore_migration_claim(
        claimed,
        "completed",
    )
    assert restored is False
    with pytest.raises(MigrationError, match="generation"):
        await migrator._finish_migration(
            claimed=claimed,
            target_worker_id=None,
            restored_status="completed",
            provider="claude",
            local_codex_target_home=None,
        )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "migrating"
    assert current.retry_count == task.retry_count
    assert current.turn_generation == task.turn_generation + 1


async def test_worker_sync_response_cannot_borrow_new_manager_generation(
    db_factory, session_factory, monkeypatch,
):
    """A network response is applied only to the claimed migration generation."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="old-session",
    )
    migrator = TaskMigrator(
        db_factory=db_factory,
        relay=FakeRelay(),
        broadcaster=None,
    )
    claimed = await migrator._claim_migration(
        migration_task_generation(task)
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "id": task.id,
                "status": "completed",
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
                "session_id": "stale-worker-session",
            }

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args, **_kwargs):
            async with session_factory() as db:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(retry_count=Task.retry_count + 1)
                )
                await db.commit()
            return Response()

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )

    with pytest.raises(MigrationError, match="并发修改"):
        await migrator._sync_task_fields_from_worker(
            worker,
            claimed,
            expected_remote_status="completed",
        )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "migrating"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "old-session"


@pytest.mark.parametrize(
    "remote_turn_generation",
    [None, 13],
    ids=["missing", "different"],
)
async def test_worker_sync_requires_exact_remote_turn_generation(
    db_factory,
    session_factory,
    monkeypatch,
    remote_turn_generation,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        turn_generation=12,
        session_id="manager-session",
    )
    migrator = TaskMigrator(
        db_factory=db_factory,
        relay=FakeRelay(),
        broadcaster=None,
    )
    claimed = await migrator._claim_migration(
        migration_task_generation(task)
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            payload = {
                "id": task.id,
                "status": "completed",
                "retry_count": task.retry_count,
                "session_id": "remote-session",
            }
            if remote_turn_generation is not None:
                payload["turn_generation"] = remote_turn_generation
            return payload

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )

    with pytest.raises(MigrationError, match="源 Worker task generation"):
        await migrator._sync_task_fields_from_worker(
            worker,
            claimed,
            expected_remote_status="completed",
        )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "migrating"
    assert current.turn_generation == 12
    assert current.session_id == "manager-session"


async def test_worker_sync_explicit_empty_fields_clear_stale_manager_mirror(
    db_factory,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="stale-session",
        last_cwd="/stale/cwd",
        target_repo="/stale/repo",
        error_message="stale error",
    )
    migrator = TaskMigrator(
        db_factory=db_factory,
        relay=FakeRelay(),
        broadcaster=None,
    )
    claimed = await migrator._claim_migration(
        migration_task_generation(task)
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "id": task.id,
                "status": "completed",
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
                "session_id": None,
                "last_cwd": None,
                "target_repo": "",
                "error_message": None,
            }

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )

    await migrator._sync_task_fields_from_worker(
        worker,
        claimed,
        expected_remote_status="completed",
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.session_id is None
        assert current.last_cwd is None
        assert current.target_repo == ""
        assert current.error_message is None


async def test_worker_task_import_is_one_inert_request(
    session_factory, monkeypatch,
):
    w = await _mk_worker(session_factory)
    t = await _mk_task(
        session_factory,
        session_id="s",
        status="completed",
        retry_count=2,
        provider="codex",
        codex_service_tier="priority",
        attention_tag="迁移结束后关注",
    )
    requests = []

    class Response:
        status_code = 201
        text = ""

        def __init__(self, status, retry_count, turn_generation):
            self.status = status
            self.retry_count = retry_count
            self.turn_generation = turn_generation

        def json(self):
            return {
                "status": self.status,
                "retry_count": self.retry_count,
                "turn_generation": self.turn_generation,
                "codex_service_tier": "priority",
            }

        @staticmethod
        def raise_for_status():
            return None

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, *, headers, json):
            requests.append((url, headers, json))
            return Response(
                json["source_status"],
                json["retry_count"],
                json["turn_generation"],
            )

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())

    await migrator._ensure_worker_task(w, t, worker_project_id=17)

    assert len(requests) == 1
    url, _headers, payload = requests[0]
    assert url.endswith("/api/tasks/migration-import")
    assert payload["id"] == t.id
    assert payload["source_status"] == "completed"
    assert payload["project_id"] == 17
    assert payload["retry_count"] == 2
    assert payload["turn_generation"] == t.turn_generation
    assert payload["selected_user_skills"] is None
    assert payload["user_skill_snapshots"] == []
    assert payload["codex_service_tier"] == "priority"
    assert payload["attention_tag"] == "迁移结束后关注"


async def test_worker_task_import_rejects_different_turn_confirmation(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        status="completed",
        turn_generation=21,
    )

    class Response:
        status_code = 201
        text = ""

        @staticmethod
        def json():
            return {
                "status": "completed",
                "retry_count": task.retry_count,
                "turn_generation": 22,
            }

        @staticmethod
        def raise_for_status():
            return None

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, _url, *, headers, json):
            assert json["turn_generation"] == 21
            return Response()

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())

    with pytest.raises(MigrationError, match="exact turn generation"):
        await migrator._ensure_worker_task(
            worker,
            task,
            worker_project_id=17,
        )


async def test_worker_task_import_rejects_different_retry_confirmation(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        status="completed",
        retry_count=4,
        turn_generation=21,
    )

    class Response:
        status_code = 201
        text = ""

        @staticmethod
        def json():
            return {
                "status": "completed",
                "retry_count": 3,
                "turn_generation": 21,
            }

        @staticmethod
        def raise_for_status():
            return None

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, _url, *, headers, json):
            assert json["retry_count"] == 4
            assert json["turn_generation"] == 21
            return Response()

    monkeypatch.setattr(
        task_migrator_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())

    with pytest.raises(MigrationError, match="exact retry generation"):
        await migrator._ensure_worker_task(
            worker,
            task,
            worker_project_id=17,
        )


async def test_put_worker_id_triggers_migration(client, session_factory, monkeypatch):
    await _mk_worker(session_factory, id=7)
    t = await _mk_task(session_factory)
    migrator = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)

    resp = await client.put(f"/api/tasks/{t.id}", json={"worker_id": 7})
    assert resp.status_code == 200, resp.text
    migrator.migrate.assert_called_once_with(t.id, 7)

    # -1 = 切回本机；已在本机 → 不触发
    migrator.migrate.reset_mock()
    resp = await client.put(f"/api/tasks/{t.id}", json={"worker_id": -1})
    assert resp.status_code == 200
    migrator.migrate.assert_not_called()


async def test_put_migration_error_maps_409(client, session_factory, monkeypatch):
    await _mk_worker(session_factory, id=7)
    t = await _mk_task(session_factory)
    migrator = AsyncMock()
    migrator.migrate.side_effect = MigrationError("先停止再切换")
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    resp = await client.put(f"/api/tasks/{t.id}", json={"worker_id": 7})
    assert resp.status_code == 409


async def test_put_without_worker_id_unchanged(client, session_factory, monkeypatch):
    """常规字段更新不碰迁移逻辑。"""
    t = await _mk_task(session_factory)
    migrator = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    resp = await client.put(f"/api/tasks/{t.id}", json={"title": "renamed"})
    assert resp.status_code == 200
    assert resp.json()["title"] == "renamed"
    migrator.migrate.assert_not_called()


async def test_destroy_migrates_tasks_back(db_factory, session_factory, monkeypatch):
    from backend.api.workers import _migrate_back_then_destroy
    w = await _mk_worker(session_factory, status="destroying")
    t1 = await _mk_task(session_factory, worker_id=w.id)
    t2 = await _mk_task(session_factory, worker_id=w.id)

    migrator = AsyncMock()
    # t2 迁移失败也不阻塞销毁
    async def _migrate(task_id, target):
        if task_id == t2.id:
            raise RuntimeError("boom")
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.worker_id = None
            await db.commit()
    migrator.migrate.side_effect = _migrate
    relay = AsyncMock()
    prov = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "worker_relay", relay)

    destroy_claim = capture_worker_destroy_lifecycle_claim(w)
    await _migrate_back_then_destroy(
        prov,
        w.id,
        destroy_claim,
        db_factory=db_factory,
    )

    async with session_factory() as db:
        a = await db.get(Task, t1.id)
        b = await db.get(Task, t2.id)
    assert a.worker_id is None
    assert b.worker_id is None  # 失败也切回指针
    assert "销毁迁移失败" in (b.error_message or "")
    prov.destroy_worker.assert_called_once_with(w.id)
    relay.stop_worker.assert_called_once_with(w.id)


# ---------------------------------------------------------------------------
# Codex session 搬运（rollout 文件在 ~/.codex/sessions/YYYY/MM/DD/）
# ---------------------------------------------------------------------------

async def test_migrate_codex_task_uses_codex_session_mover(db_factory, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, session_id="019f0000-aaaa-bbbb-cccc-000000000001", provider="codex")
    m = _migrator(db_factory)
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 9
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    await m.migrate(t.id, w.id)

    m._move_codex_session.assert_called_once()
    m._move_session.assert_not_called()


async def test_migrate_claude_task_keeps_claude_session_mover(db_factory, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, session_id="sess-claude", provider="claude")
    m = _migrator(db_factory)
    proxy = AsyncMock()
    proxy.ensure_worker_project.return_value = 9
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    await m.migrate(t.id, w.id)

    m._move_session.assert_called_once()
    m._move_codex_session.assert_not_called()


async def test_local_claude_session_moves_sidecar_tree_to_worker(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    session_id = "session-with-sidecar"
    project_dir = tmp_path / ".claude" / "projects" / "encoded"
    sidecar = project_dir / session_id
    tool_result = sidecar / "tool-results" / "large.txt"
    tool_result.parent.mkdir(parents=True)
    tool_result.write_text("large output", encoding="utf-8")
    jsonl = project_dir / f"{session_id}.jsonl"
    jsonl.write_text("{}\n", encoding="utf-8")

    destination = Worker(
        id=8,
        name="destination",
        status="ready",
        private_ip="10.0.0.8",
        auth_token="t",
        ssh_user="ubuntu",
    )
    fake_ssh = AsyncMock()
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())
    monkeypatch.setattr(migrator, "_ssh", lambda _worker: fake_ssh)

    await migrator._move_session(None, destination, session_id)

    fake_ssh.copy_file.assert_awaited_once_with(
        str(jsonl),
        f"/home/ubuntu/.claude/projects/encoded/{session_id}.jsonl",
    )
    fake_ssh.rsync_to.assert_awaited_once_with(
        str(sidecar) + "/",
        f"/home/ubuntu/.claude/projects/encoded/{session_id}/",
        excludes=[],
        timeout=1200,
    )


async def test_remote_claude_session_moves_sidecar_and_cleans_temporary_copy(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    session_id = "remote-session"
    remote_jsonl = (
        f"/home/ubuntu/.claude-account-2/projects/encoded/"
        f"{session_id}.jsonl"
    )
    remote_sidecar = remote_jsonl.removesuffix(".jsonl")
    temporary = tmp_path / "sensitive-download"

    class FakeSSH:
        async def run(self, command):
            if command.startswith("ls "):
                return 0, remote_jsonl + "\n"
            if command.startswith("test -d "):
                return 0, ""
            raise AssertionError(command)

        async def rsync_from(
            self,
            remote_path,
            local_path,
            delete=False,
        ):
            assert delete is False
            if remote_path == remote_jsonl:
                Path(local_path).write_text("{}\n", encoding="utf-8")
                return
            assert remote_path == remote_sidecar + "/"
            result = Path(local_path) / "tool-results" / "large.txt"
            result.parent.mkdir(parents=True, exist_ok=True)
            result.write_text("sensitive", encoding="utf-8")

    source = Worker(
        id=7,
        name="source",
        status="ready",
        private_ip="10.0.0.7",
        auth_token="t",
        ssh_user="ubuntu",
    )
    fake_ssh = FakeSSH()
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())
    monkeypatch.setattr(migrator, "_ssh", lambda _worker: fake_ssh)

    def make_temp(*_args, **_kwargs):
        temporary.mkdir()
        return str(temporary)

    monkeypatch.setattr(
        task_migrator_module.tempfile,
        "mkdtemp",
        make_temp,
    )
    event_loop_thread = threading.get_ident()
    copytree_threads: list[int] = []
    rmtree_threads: list[int] = []
    real_copytree = task_migrator_module.shutil.copytree
    real_rmtree = task_migrator_module.shutil.rmtree

    def tracked_copytree(*args, **kwargs):
        copytree_threads.append(threading.get_ident())
        return real_copytree(*args, **kwargs)

    def tracked_rmtree(*args, **kwargs):
        rmtree_threads.append(threading.get_ident())
        return real_rmtree(*args, **kwargs)

    monkeypatch.setattr(
        task_migrator_module.shutil,
        "copytree",
        tracked_copytree,
    )
    monkeypatch.setattr(
        task_migrator_module.shutil,
        "rmtree",
        tracked_rmtree,
    )

    await migrator._move_session(source, None, session_id)

    target_root = tmp_path / ".claude" / "projects" / "encoded"
    assert (target_root / f"{session_id}.jsonl").read_text() == "{}\n"
    assert (
        target_root / session_id / "tool-results" / "large.txt"
    ).read_text() == "sensitive"
    assert not temporary.exists()
    assert copytree_threads and copytree_threads[0] != event_loop_thread
    assert rmtree_threads and rmtree_threads[0] != event_loop_thread


async def test_local_codex_session_glob_finds_rollout_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    sid = "019f0000-aaaa-bbbb-cccc-000000000002"
    day_dir = tmp_path / ".codex" / "sessions" / "2026" / "07" / "19"
    day_dir.mkdir(parents=True)
    f = day_dir / f"rollout-2026-07-19T01-02-03-{sid}.jsonl"
    f.write_text("{}")

    matches = TaskMigrator._local_codex_session_glob(sid)
    assert matches == [str(f)]
    # 不同 session id 不应命中
    assert TaskMigrator._local_codex_session_glob("other-id") == []


async def test_local_codex_session_glob_finds_account_specific_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    sid = "019f0000-aaaa-bbbb-cccc-000000000003"
    day_dir = tmp_path / ".codex-account-2" / "sessions" / "2026" / "07" / "20"
    day_dir.mkdir(parents=True)
    rollout = day_dir / f"rollout-2026-07-20T01-02-03-{sid}.jsonl"
    rollout.write_text("{}")

    assert TaskMigrator._local_codex_session_glob(sid) == [str(rollout)]
    root, relative = TaskMigrator._codex_sessions_root_and_relative(str(rollout))
    assert root == str(tmp_path / ".codex-account-2" / "sessions")
    assert relative == f"2026/07/20/{rollout.name}"
    assert ".." not in PurePosixPath(relative).parts


async def test_local_account_rollout_moves_to_safe_remote_relative_path(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    sid = "019f0000-aaaa-bbbb-cccc-000000000005"
    day_dir = tmp_path / ".codex-account-2" / "sessions" / "2026" / "07" / "20"
    day_dir.mkdir(parents=True)
    rollout = day_dir / f"rollout-2026-07-20T02-03-04-{sid}.jsonl"
    rollout.write_text("{}")

    fake_ssh = AsyncMock()
    destination = Worker(
        id=8,
        name="destination",
        status="ready",
        private_ip="10.0.0.8",
        auth_token="t",
        ssh_user="ubuntu",
    )
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())
    monkeypatch.setattr(migrator, "_ssh", lambda worker: fake_ssh)

    await migrator._move_codex_session(None, destination, sid)

    expected = (
        "/home/ubuntu/.codex/sessions/2026/07/20/"
        f"rollout-2026-07-20T02-03-04-{sid}.jsonl"
    )
    fake_ssh.copy_file.assert_awaited_once_with(str(rollout), expected)
    assert ".." not in PurePosixPath(expected).parts


async def test_local_codex_migration_selects_copy_with_complete_history(
    tmp_path, monkeypatch,
):
    """Rotation copies remain in old homes; the longest proven prefix wins."""
    monkeypatch.setenv("HOME", str(tmp_path))
    sid = "019f0000-aaaa-bbbb-cccc-000000000006"
    old_dir = tmp_path / ".codex" / "sessions" / "2026" / "07" / "20"
    new_dir = tmp_path / ".codex-codex-3" / "sessions" / "2026" / "07" / "21"
    old_dir.mkdir(parents=True)
    new_dir.mkdir(parents=True)
    old = old_dir / f"rollout-old-{sid}.jsonl"
    newest = new_dir / f"rollout-new-{sid}.jsonl"
    old.write_bytes(b"turn-1\n")
    newest.write_bytes(b"turn-1\nturn-2\n")

    destination = Worker(
        id=9,
        name="destination",
        status="ready",
        private_ip="10.0.0.9",
        auth_token="t",
        ssh_user="ubuntu",
    )
    fake_ssh = AsyncMock()
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())
    monkeypatch.setattr(migrator, "_ssh", lambda worker: fake_ssh)

    await migrator._move_codex_session(None, destination, sid)

    assert fake_ssh.copy_file.await_args.args[0] == str(newest)


def test_codex_migration_refuses_divergent_account_copies(tmp_path):
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    first.write_bytes(b"same-prefix\nA\n")
    second.write_bytes(b"same-prefix\nB\n")

    with pytest.raises(MigrationError, match="分叉 rollout"):
        TaskMigrator._select_authoritative_codex_rollout(
            [str(first), str(second)]
        )


async def test_remote_codex_session_uses_matched_account_sessions_root(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    sid = "019f0000-aaaa-bbbb-cccc-000000000004"
    remote_file = (
        f"/home/ubuntu/.codex-account-3/sessions/2026/07/20/"
        f"rollout-2026-07-20T01-02-03-{sid}.jsonl"
    )

    class FakeSSH:
        def __init__(self):
            self.commands = []

        async def run(self, command):
            self.commands.append(command)
            return 0, remote_file + "\n"

        async def rsync_from(self, remote_path, local_path, delete=False):
            assert remote_path == remote_file
            assert delete is False
            with open(local_path, "w", encoding="utf-8") as stream:
                stream.write("{}")

    source = Worker(
        id=7,
        name="source",
        status="ready",
        private_ip="10.0.0.7",
        auth_token="t",
        ssh_user="ubuntu",
    )
    fake_ssh = FakeSSH()
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())
    monkeypatch.setattr(migrator, "_ssh", lambda worker: fake_ssh)

    await migrator._move_codex_session(source, None, sid)

    target = (
        tmp_path / ".codex" / "sessions" / "2026" / "07" / "20"
        / f"rollout-2026-07-20T01-02-03-{sid}.jsonl"
    )
    assert target.read_text() == "{}"
    assert "find ~/.codex*/sessions" in fake_ssh.commands[0]


async def test_remote_codex_session_downloads_all_copies_and_uses_complete_one(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    sid = "019f0000-aaaa-bbbb-cccc-000000000007"
    old_remote = (
        f"/home/ubuntu/.codex/sessions/2026/07/20/rollout-old-{sid}.jsonl"
    )
    new_remote = (
        f"/home/ubuntu/.codex-codex-3/sessions/2026/07/21/"
        f"rollout-new-{sid}.jsonl"
    )

    class MultiCopySSH:
        async def run(self, _command):
            return 0, f"{old_remote}\n{new_remote}\n"

        async def rsync_from(self, remote_path, local_path, delete=False):
            assert delete is False
            content = b"turn-1\n" if remote_path == old_remote else b"turn-1\nturn-2\n"
            with open(local_path, "wb") as stream:
                stream.write(content)

    source = Worker(
        id=7,
        name="source",
        status="ready",
        private_ip="10.0.0.7",
        auth_token="t",
        ssh_user="ubuntu",
    )
    fake_ssh = MultiCopySSH()
    migrator = TaskMigrator(db_factory=None, relay=FakeRelay())
    monkeypatch.setattr(migrator, "_ssh", lambda worker: fake_ssh)

    await migrator._move_codex_session(source, None, sid)

    target = (
        tmp_path / ".codex" / "sessions" / "2026" / "07" / "21"
        / f"rollout-new-{sid}.jsonl"
    )
    assert target.read_bytes() == b"turn-1\nturn-2\n"
