"""Tests for stale state cleanup, zombie worker prevention, and orphan task handling.

Covers dispatcher ownership recovery and stale-state cleanup:
- Unowned persisted PIDs are quarantined without signalling unknown processes
- Manager-owned in-process generations survive Pause -> Start
- Unowned task claims return to pending for safe retry
- Safety-net instance/task reset after lifecycle ends
- Instance.current_task_id cleanup on task deletion
- Orphaned task handling on stop-session
- Interrupted task status change (pending → completed)
"""
import asyncio
import os
from datetime import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.log_entry import LogEntry
from backend.models.sub_agent import SubAgentSession
from backend.services.dispatcher import (
    GlobalDispatcher,
    _TaskLifecycleGeneration,
)
from backend.services.task_queue import TaskQueue


# === Helpers ===

def _make_dispatcher(db_factory):
    """Create a GlobalDispatcher with mocked dependencies."""
    instance_manager = MagicMock()
    instance_manager.launch = AsyncMock(return_value=12345)
    # Lifecycle completion now waits for the output consumer to finish its
    # final persistence/account-routing work before deciding the task status.
    instance_manager.wait_for_output_consumer = AsyncMock()
    instance_manager.processes = {}
    instance_manager._tasks = {}
    instance_manager.pty_mode_enabled = False
    instance_manager.transient_error_seen = MagicMock(return_value=False)
    instance_manager.get_last_stderr = MagicMock(return_value="")
    instance_manager.get_recent_log_contents = AsyncMock(return_value=[])
    # PTY proactive pool switch path (dispatcher._run_task_lifecycle)
    instance_manager.pty_rate_limit_seen = MagicMock(return_value=False)
    instance_manager._try_proactive_pool_switch = AsyncMock()
    instance_manager._pty_rate_limit_seen = set()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()

    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )


async def _lifecycle_generation(dispatcher, db_factory, task_id):
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        return dispatcher._task_lifecycle_generation(task)


# === _cleanup_stale_state tests ===


@pytest.mark.asyncio
async def test_maintenance_reconciliation_requires_paused_admission(db_factory):
    d = _make_dispatcher(db_factory)
    d._cleanup_stale_state = AsyncMock()

    with pytest.raises(RuntimeError, match="paused task admission"):
        await d.reconcile_stale_state_for_maintenance()

    await d.pause_dispatching()
    await d.reconcile_stale_state_for_maintenance()

    d._cleanup_stale_state.assert_awaited_once_with(
        reconcile_auxiliary=False
    )


@pytest.mark.asyncio
async def test_maintenance_reconcile_preserves_live_auxiliary_rows(db_factory):
    """Manual reconciliation is not a startup sweep of sub-agent sessions."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        rows = [
            SubAgentSession(
                task_id=101,
                description="ccm monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=102,
                description="native agent",
                agent_type="native-agent",
                source="native",
                status="running",
            ),
            SubAgentSession(
                task_id=103,
                description="native monitor",
                agent_type="native-monitor",
                source="native",
                status="running",
            ),
        ]
        db.add_all(rows)
        await db.commit()
        row_ids = [row.id for row in rows]

    await d.pause_dispatching()
    try:
        await d.reconcile_stale_state_for_maintenance()
    finally:
        d.resume_dispatching()

    async with db_factory() as db:
        statuses = [
            (await db.get(SubAgentSession, row_id)).status
            for row_id in row_ids
        ]
    assert statuses == ["running", "running", "running"]


@pytest.mark.asyncio
async def test_startup_cleanup_uses_exact_auxiliary_ownership(db_factory):
    """Stop -> Start preserves live CCM/native rows and clears only stale ones."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        parent = Task(
            title="native parent",
            description="d",
            status="executing",
        )
        db.add(parent)
        await db.flush()
        instance = Instance(
            name="native owner",
            status="running",
            pid=43210,
            current_task_id=parent.id,
        )
        db.add(instance)
        await db.flush()
        parent.instance_id = instance.id
        rows = [
            SubAgentSession(
                task_id=parent.id,
                description="live ccm monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="live ccm sub-agent",
                agent_type="sub_agent",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="live native",
                agent_type="native-agent",
                source="native",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="legacy native monitor",
                agent_type="monitor",
                source="native",
                status="running",
            ),
            SubAgentSession(
                task_id=999,
                description="stale local",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="recoverable scheduled monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
                next_check_at=datetime.utcnow(),
            ),
            SubAgentSession(
                task_id=parent.id,
                description="uncertain active monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
                turn_generation=3,
                active_turn_generation=3,
            ),
            SubAgentSession(
                task_id=998,
                remote_id=88,
                description="remote mirror",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
        ]
        db.add_all(rows)
        await db.commit()
        instance_id = instance.id
        row_ids = [row.id for row in rows]

    d.instance_manager.processes[instance_id] = MagicMock(
        returncode=None
    )
    monitor_lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._monitor_tasks[row_ids[0]] = monitor_lifecycle
    d._sub_agent_processes[row_ids[1]] = MagicMock(returncode=None)
    try:
        await d._cleanup_stale_state()
    finally:
        monitor_lifecycle.cancel()
        await asyncio.gather(
            monitor_lifecycle, return_exceptions=True
        )

    async with db_factory() as db:
        statuses = [
            (await db.get(SubAgentSession, row_id)).status
            for row_id in row_ids
        ]
    assert statuses == [
        "running",
        "running",
        "running",
        "running",
        "failed",
        "running",
        "failed",
        "running",
    ]
    async with db_factory() as db:
        uncertain = await db.get(SubAgentSession, row_ids[6])
    assert "could not be recovered" in uncertain.last_error


@pytest.mark.asyncio
async def test_cleanup_resets_dead_pid_instance(db_factory):
    """An unowned persisted PID is quarantined instead of treated as attachable."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="zombie-worker", status="running", pid=999999, current_task_id=42)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"
        assert inst.pid is None
        assert inst.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_preserves_manager_owned_live_generation(db_factory):
    """Pause -> Start preserves a process/consumer owned by this manager."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="live-task",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        inst = Instance(
            name="alive-worker",
            status="running",
            pid=43210,
            current_task_id=task.id,
        )
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_id = task.id

    d.instance_manager.processes[inst_id] = MagicMock(returncode=None)

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "running"
        assert inst.pid == 43210
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.instance_id == inst_id


@pytest.mark.asyncio
async def test_cleanup_preserves_prelaunch_lifecycle_claim(db_factory):
    """A paused lifecycle may own a slot before InstanceManager maps a process."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        inst = Instance(name="prelaunch", status="idle")
        db.add(inst)
        await db.flush()
        task = Task(
            title="prelaunch",
            description="d",
            status="executing",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        inst_id, task_id = inst.id, task.id

    lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._running_tasks[inst_id] = lifecycle
    try:
        await d._cleanup_stale_state()
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "executing"
            assert task.instance_id == inst_id
    finally:
        lifecycle.cancel()
        await asyncio.gather(lifecycle, return_exceptions=True)


@pytest.mark.asyncio
async def test_cleanup_preserves_reserved_fresh_task_claim(db_factory):
    """Maintenance cannot recover a claim still in project/config preparation."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="reserved-prelaunch", status="idle")
        task = Task(
            title="reserved-prelaunch",
            description="d",
            status="pending",
        )
        db.add_all([instance, task])
        await db.commit()
        instance_id, task_id = instance.id, task.id

    claim_token = None
    try:
        async with db_factory() as db:
            reserved, claim_token = await d._reserve_idle_instance(
                db, instance_id=instance_id
            )
            assert reserved is not None
            claimed = await TaskQueue(db).dequeue(instance_id=instance_id)
            assert claimed is not None
            assert claimed.id == task_id

        await d.pause_dispatching()
        await d.reconcile_stale_state_for_maintenance()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "in_progress"
            assert task.instance_id == instance_id
    finally:
        if claim_token is not None:
            await d._release_instance_reservation(
                instance_id, claim_token
            )
        d.resume_dispatching()


@pytest.mark.asyncio
async def test_cleanup_does_not_rewrite_remote_shared_shadow(db_factory):
    """Shared task lifecycle is remote-authoritative, never locally recovered."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        shadow = Task(
            title="remote shadow",
            description="d",
            status="executing",
            shared_from_id=987654,
        )
        db.add(shadow)
        await db.commit()
        shadow_id = shadow.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        shadow = await db.get(Task, shadow_id)
        assert shadow.status == "executing"
        assert shadow.instance_id is None


@pytest.mark.asyncio
async def test_cleanup_fail_closes_unowned_pid_that_may_be_alive(db_factory):
    """Unknown live PID is never auto-retried, which could duplicate writes."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="orphan", description="d", status="executing")
        db.add(task)
        await db.flush()
        inst = Instance(
            name="unknown-live",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(inst)
        await db.flush()
        task.instance_id = inst.id
        await db.commit()
        task_id, inst_id = task.id, inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"
        assert inst.pid == os.getpid()
        assert inst.current_task_id == task_id
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id == inst_id
        assert "duplicate execution" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_quarantines_idle_row_with_live_orphan_pid(db_factory):
    """``idle`` cannot make a persisted live generation dispatchable."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="dirty idle", description="d", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dirty-idle-owner",
            status="idle",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "error"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == task_id
        assert task.status == "failed"
        assert task.instance_id == instance_id


@pytest.mark.asyncio
async def test_idle_reservation_refuses_orphan_evidence(db_factory):
    """Admission independently rejects dirty idle PID/owner fields."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(
            name="dirty-idle",
            status="idle",
            pid=os.getpid(),
            current_task_id=987654,
        )
        db.add(instance)
        await db.commit()

    async with db_factory() as db:
        assert await d._reserve_idle_instance(db) == (None, None)


@pytest.mark.asyncio
async def test_cleanup_generation_cas_preserves_concurrent_replacement(db_factory):
    """A generation changed after SELECT wins; stale cleanup touches neither owner."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        old_task = Task(title="old owner", description="d", status="executing")
        new_task = Task(title="new owner", description="d", status="executing")
        db.add_all([old_task, new_task])
        await db.flush()
        instance = Instance(
            name="owner-race",
            status="idle",
            pid=os.getpid(),
            current_task_id=old_task.id,
        )
        db.add(instance)
        await db.flush()
        old_task.instance_id = instance.id
        new_task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id
        old_task_id, new_task_id = old_task.id, new_task.id

    original_execute = AsyncSession.execute
    injected = False

    async def execute_with_owner_race(session, statement, *args, **kwargs):
        nonlocal injected
        table = getattr(statement, "table", None)
        if not injected and getattr(table, "name", None) == "instances":
            injected = True
            await original_execute(
                session,
                update(Instance)
                .where(Instance.id == instance_id)
                .values(
                    status="running",
                    pid=os.getpid(),
                    current_task_id=new_task_id,
                ),
            )
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=execute_with_owner_race):
        await d._cleanup_stale_state()

    assert injected
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        old_task = await db.get(Task, old_task_id)
        new_task = await db.get(Task, new_task_id)
        assert instance.status == "running"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == new_task_id
        assert old_task.status == "executing"
        assert new_task.status == "executing"


@pytest.mark.asyncio
async def test_cleanup_instance_cas_includes_started_at_generation(db_factory):
    """Same owner/PID with a new start timestamp is a replacement generation."""
    from datetime import datetime, timedelta

    d = _make_dispatcher(db_factory)
    old_started = datetime(2026, 7, 23, 10, 0, 0)
    new_started = old_started + timedelta(seconds=1)
    async with db_factory() as db:
        task = Task(title="started-at ABA", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="started-at-race",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
            started_at=old_started,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    original_execute = AsyncSession.execute
    injected = False

    async def execute_with_started_at_race(session, statement, *args, **kwargs):
        nonlocal injected
        if (
            not injected
            and getattr(getattr(statement, "table", None), "name", None)
            == "instances"
        ):
            injected = True
            await original_execute(
                session,
                update(Instance)
                .where(Instance.id == instance_id)
                .values(started_at=new_started),
            )
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(
        AsyncSession, "execute", new=execute_with_started_at_race
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "running"
        assert instance.started_at == new_started
        assert task.status == "executing"


@pytest.mark.asyncio
async def test_pending_orphan_quarantine_never_overwrites_new_slot_owner(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="new pending owner", status="pending")
        db.add(task)
        await db.flush()
        orphan = Instance(
            name="old-live-orphan",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        replacement = Instance(name="new-slot", status="idle")
        db.add_all([orphan, replacement])
        await db.flush()
        task.instance_id = replacement.id
        await db.commit()
        task_id = task.id
        orphan_id = orphan.id
        replacement_id = replacement.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        orphan = await db.get(Instance, orphan_id)
        assert task.status == "pending"
        assert task.instance_id == replacement_id
        assert orphan.status == "error"
        assert orphan.pid == os.getpid()


@pytest.mark.asyncio
async def test_cleanup_fail_closes_pending_task_still_owned_by_live_orphan(
    db_factory,
):
    """A stale pending write cannot make an unknown live PID dispatchable."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="dirty pending", description="d", status="pending")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dirty-live-owner",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert "duplicate execution" in task.error_message
        assert instance.status == "error"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_cleanup_resets_instance_with_no_pid(db_factory):
    """A running row without an owned generation is terminal error history."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="no-pid-worker", status="running", pid=None)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"


@pytest.mark.asyncio
async def test_cleanup_resets_stuck_executing_task(db_factory):
    """An unowned executing claim returns to pending, never fake success."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="stuck-task", description="test", status="executing")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "pending"
        assert t.instance_id is None


@pytest.mark.asyncio
async def test_cleanup_fail_closes_active_task_with_routing_marker(
    db_factory,
):
    """A crash-left routing fence must recover to an ack-safe terminal status."""

    d = _make_dispatcher(db_factory)
    marker = {
        "op_id": "staged-before-restart",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    async with db_factory() as db:
        task = Task(
            title="stuck-fenced-task",
            description="test",
            status="executing",
            metadata_={"worker_routing_config_pending": marker},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert task.metadata_["worker_routing_config_pending"] == marker


@pytest.mark.asyncio
async def test_cleanup_fails_multi_owner_corruption_without_replay(db_factory):
    """Multiple dead reverse owners are corruption, not retry permission."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="duplicate owners",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        owners = [
            Instance(
                name=f"duplicate-owner-{index}",
                status="running",
                pid=990000 + index,
                current_task_id=task.id,
            )
            for index in range(3)
        ]
        db.add_all(owners)
        await db.flush()
        task.instance_id = owners[-1].id
        await db.commit()
        task_id = task.id
        owner_ids = [owner.id for owner in owners]

    with patch(
        "backend.services.dispatcher.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert "inconsistent Task/Instance ownership" in task.error_message
        for owner_id in owner_ids:
            owner = await db.get(Instance, owner_id)
            assert owner.status == "error"
            assert owner.pid is None
            assert owner.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_requeues_unique_consistent_dead_owner(db_factory):
    """A unique dead generation retries despite an older terminal log."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="unique owner", status="executing")
        db.add(task)
        await db.flush()
        owner = Instance(
            name="unique-dead-owner",
            status="running",
            pid=991111,
            current_task_id=task.id,
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        db.add(
            LogEntry(
                task_id=task.id,
                instance_id=owner.id,
                event_type="result",
                is_error=True,
            )
        )
        await db.commit()
        task_id, owner_id = task.id, owner.id

    with patch(
        "backend.services.dispatcher.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "pending"
        assert task.instance_id is None
        assert owner.status == "error"
        assert owner.pid is None
        assert owner.current_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("forward_owner", ["none", "different"])
async def test_cleanup_fails_single_mismatched_reverse_owner(
    db_factory,
    forward_owner,
):
    """A reverse owner is retryable only when the Task points back to it."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="mismatched owner", status="executing")
        db.add(task)
        await db.flush()
        reverse_owner = Instance(
            name="reverse-owner",
            status="running",
            pid=992222,
            current_task_id=task.id,
        )
        unrelated = Instance(name="unrelated-owner", status="idle")
        db.add_all([reverse_owner, unrelated])
        await db.flush()
        if forward_owner == "different":
            task.instance_id = unrelated.id
        await db.commit()
        task_id = task.id

    with patch(
        "backend.services.dispatcher.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert "inconsistent Task/Instance ownership" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_preserves_live_owner_while_removing_dead_duplicate(
    db_factory,
):
    """A managed live generation wins over a dead duplicate reverse owner."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="live plus duplicate", status="executing")
        db.add(task)
        await db.flush()
        live_owner = Instance(
            name="managed-live-owner",
            status="running",
            pid=993331,
            current_task_id=task.id,
        )
        dead_duplicate = Instance(
            name="dead-duplicate-owner",
            status="running",
            pid=993332,
            current_task_id=task.id,
        )
        db.add_all([live_owner, dead_duplicate])
        await db.flush()
        task.instance_id = live_owner.id
        await db.commit()
        task_id = task.id
        live_id, dead_id = live_owner.id, dead_duplicate.id

    d.instance_manager.processes[live_id] = MagicMock(returncode=None)
    with patch(
        "backend.services.dispatcher.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        live_owner = await db.get(Instance, live_id)
        dead_duplicate = await db.get(Instance, dead_id)
        assert task.status == "executing"
        assert task.instance_id == live_id
        assert live_owner.status == "running"
        assert live_owner.current_task_id == task_id
        assert dead_duplicate.status == "error"
        assert dead_duplicate.pid is None
        assert dead_duplicate.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_resets_stuck_in_progress_task(db_factory):
    """An unowned in-progress claim returns to pending."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="stuck-task-2", description="test", status="in_progress")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "pending"


@pytest.mark.asyncio
async def test_cleanup_preserves_session_id(db_factory):
    """Stuck task reset preserves session_id so user can resume chat."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="session-task", description="test", status="executing",
                    session_id="abc-123")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "pending"
        assert t.session_id == "abc-123"


@pytest.mark.asyncio
async def test_cleanup_does_not_touch_pending_tasks(db_factory):
    """Pending tasks are not affected by cleanup."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="pending-task", description="test", status="pending")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "pending"


@pytest.mark.asyncio
async def test_cleanup_does_not_touch_completed_tasks(db_factory):
    """Completed tasks are not affected by cleanup."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="done-task", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_cleanup_does_not_touch_idle_instances(db_factory):
    """Idle instances are not affected by cleanup."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="idle-worker", status="idle")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"


@pytest.mark.asyncio
async def test_cleanup_acquires_task_write_before_instance_write(db_factory):
    """Startup reconciliation follows the global Task -> Instance lock order."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="ordered-cleanup", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="ordered-cleanup",
            status="running",
            pid=876543,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()

    original_execute = AsyncSession.execute
    write_tables: list[str] = []

    async def record_writes(session, statement, *args, **kwargs):
        table_name = getattr(
            getattr(statement, "table", None),
            "name",
            None,
        )
        if table_name in {"tasks", "instances"}:
            write_tables.append(table_name)
        return await original_execute(session, statement, *args, **kwargs)

    with (
        patch(
            "backend.services.dispatcher.os.kill",
            side_effect=ProcessLookupError,
        ),
        patch.object(AsyncSession, "execute", new=record_writes),
    ):
        await d._cleanup_stale_state()

    assert "tasks" in write_tables
    assert "instances" in write_tables
    assert write_tables.index("tasks") < write_tables.index("instances")


@pytest.mark.asyncio
async def test_cleanup_called_on_start(db_factory):
    """_cleanup_stale_state is called during dispatcher start()."""
    d = _make_dispatcher(db_factory)

    async def fake_loop():
        await asyncio.sleep(999)
    d._dispatch_loop = fake_loop

    async with db_factory() as db:
        inst = Instance(name="stale-on-start", status="running", pid=999999)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d.start()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"

    await d.stop()


# === _reset_instance_if_stale (safety net) tests ===


@pytest.mark.asyncio
async def test_safety_reset_instance_still_running(db_factory):
    """If instance is still 'running' after lifecycle, safety net resets it."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="stuck-worker", status="running", pid=12345, current_task_id=1)
        db.add(inst)
        task = Task(title="test", description="test", status="executing")
        db.add(task)
        await db.flush()
        inst.current_task_id = task.id
        task.instance_id = inst.id
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    await d._reset_instance_if_stale(
        inst_id, await _lifecycle_generation(d, db_factory, task_id)
    )

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        assert inst.pid is None
        assert inst.current_task_id is None
        t = await db.get(Task, task_id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_safety_reset_writes_task_before_instance(db_factory):
    """The fallback completion cannot invert the lifecycle DB lock order."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="ordered-reset", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="ordered-reset",
            status="running",
            pid=12345,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    original_execute = AsyncSession.execute
    write_tables: list[str] = []

    async def record_writes(session, statement, *args, **kwargs):
        table_name = getattr(
            getattr(statement, "table", None),
            "name",
            None,
        )
        if table_name in {"tasks", "instances"}:
            write_tables.append(table_name)
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=record_writes):
        await d._reset_instance_if_stale(
            instance_id, await _lifecycle_generation(d, db_factory, task_id)
        )

    assert "tasks" in write_tables
    assert "instances" in write_tables
    assert write_tables.index("tasks") < write_tables.index("instances")


@pytest.mark.asyncio
async def test_safety_reset_does_not_complete_unbound_recovery_task(db_factory):
    """An old lifecycle cannot treat ``instance_id IS NULL`` as its owner."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="recovering", description="d", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="old-generation",
            status="running",
            pid=12345,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._reset_instance_if_stale(
        instance_id, await _lifecycle_generation(d, db_factory, task_id)
    )

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "running"
        assert instance.current_task_id == task_id
        assert instance.pid == 12345
        assert task.status == "executing"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_safety_reset_releases_dead_owner_after_retry_advanced(db_factory):
    """A completed retry transition must not strand its previous Instance."""

    from backend.services.instance_manager import InstanceManager

    d = _make_dispatcher(db_factory)
    d.instance_manager = InstanceManager(db_factory, d.broadcaster)
    old_task_started = datetime.utcnow()
    old_instance_started = datetime.utcnow()
    async with db_factory() as db:
        task = Task(
            title="retry-advanced",
            status="executing",
            retry_count=0,
            started_at=old_task_started,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dead-first-attempt",
            status="running",
            pid=812_202,
            current_task_id=task.id,
            started_at=old_instance_started,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        old_generation = d._task_lifecycle_generation(task)
        task_id, instance_id = task.id, instance.id

        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="pending",
                retry_count=1,
                instance_id=None,
                started_at=None,
            )
        )
        await db.commit()

    with patch(
        "backend.services.instance_manager.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._reset_instance_if_stale(instance_id, old_generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "pending"
        assert task.retry_count == 1
        assert task.instance_id is None
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_safety_reset_skips_already_idle_instance(db_factory):
    """If instance is already idle (consume_output cleaned up), safety net is a no-op."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="clean-worker", status="idle")
        db.add(inst)
        task = Task(title="test", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    await d._reset_instance_if_stale(
        inst_id, await _lifecycle_generation(d, db_factory, task_id)
    )

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        t = await db.get(Task, task_id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_safety_reset_old_lifecycle_cannot_clear_recycled_owner(db_factory):
    """An old lifecycle finally must not erase a newer task on the same slot."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        old_task = Task(
            title="old",
            description="d",
            status="executing",
        )
        new_task = Task(
            title="new",
            description="d",
            status="executing",
        )
        db.add_all([old_task, new_task])
        await db.flush()
        inst = Instance(
            name="recycled",
            status="running",
            pid=222,
            current_task_id=new_task.id,
        )
        db.add(inst)
        await db.flush()
        old_task.instance_id = inst.id
        new_task.instance_id = inst.id
        await db.commit()
        old_id, new_id, inst_id = old_task.id, new_task.id, inst.id

    d.instance_manager.processes[inst_id] = MagicMock(returncode=0)
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=asyncio.Lock()
    )

    await d._reset_instance_if_stale(
        inst_id, await _lifecycle_generation(d, db_factory, old_id)
    )

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "running"
        assert inst.current_task_id == new_id
        assert inst.pid == 222
        assert (await db.get(Task, old_id)).status == "executing"
        assert (await db.get(Task, new_id)).status == "executing"


@pytest.mark.asyncio
async def test_safety_reset_cannot_clear_same_task_same_slot_reclaim(
    db_factory,
):
    """Old finally cannot complete/clear a retried generation before spawn."""

    from datetime import datetime, timedelta

    d = _make_dispatcher(db_factory)
    old_task_started = datetime.utcnow() - timedelta(minutes=2)
    old_instance_started = datetime.utcnow() - timedelta(minutes=1)
    new_task_started = datetime.utcnow()
    new_instance_started = datetime.utcnow()
    async with db_factory() as db:
        task = Task(
            title="same-task-reclaim",
            status="executing",
            retry_count=0,
            started_at=old_task_started,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="same-task-reclaim",
            status="running",
            pid=111,
            current_task_id=task.id,
            started_at=old_instance_started,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        old_generation = d._task_lifecycle_generation(task)
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                retry_count=1,
                started_at=new_task_started,
            )
        )
        await db.execute(
            update(Instance)
            .where(Instance.id == instance.id)
            .values(pid=222, started_at=new_instance_started)
        )
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._reset_instance_if_stale(instance_id, old_generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.retry_count == 1
        assert task.started_at == new_task_started
        assert instance.status == "running"
        assert instance.current_task_id == task_id
        assert instance.pid == 222
        assert instance.started_at == new_instance_started


@pytest.mark.asyncio
async def test_safety_reset_handles_db_error(db_factory):
    """Safety net does not raise on DB errors (logs instead)."""
    d = _make_dispatcher(db_factory)
    # Use a nonexistent instance_id — should not raise
    await d._reset_instance_if_stale(
        99999,
        _TaskLifecycleGeneration(
            task_id=99999,
            worker_id=None,
            shared_from_id=None,
            retry_count=0,
            instance_id=99999,
            started_at=None,
            completed_at=None,
        ),
    )


# === Interrupted task status tests ===


@pytest.mark.asyncio
async def test_interrupted_task_marked_completed(db_factory):
    """User-interrupted task (exit code -2/130) is marked completed, not pending."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="int-worker")
        db.add(inst)
        task = Task(title="interrupt-test", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = -2  # SIGINT
    mock_proc.wait = AsyncMock(return_value=-2)
    d.instance_manager.processes = {inst_id: mock_proc}

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"

    # Verify broadcast sent "completed" not "pending"
    calls = d.broadcaster.broadcast.call_args_list
    status_events = [c for c in calls if c[0][0] == "tasks" and c[0][1].get("new_status")]
    last_status = status_events[-1][0][1]["new_status"]
    assert last_status == "completed"


@pytest.mark.asyncio
async def test_interrupted_task_exit_130(db_factory):
    """Exit code 130 (SIGINT) also marks task completed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="int-worker-130")
        db.add(inst)
        task = Task(title="interrupt-130", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 130
    mock_proc.wait = AsyncMock(return_value=130)
    d.instance_manager.processes = {inst_id: mock_proc}

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_interrupted_lifecycle_cannot_overwrite_concurrent_cancel(db_factory):
    """A stale exit-code result must lose to the user's cancelled status CAS."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="cancel-race")
        task = Task(title="cancel-race", description="d", status="pending")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id, task_id, task_obj = inst.id, task.id, task

    class Process:
        returncode = None

        async def wait(self):
            async with db_factory() as db:
                assert await TaskQueue(db).cancel(task_id) is not None
            self.returncode = -2
            return -2

    process = Process()
    d.instance_manager.processes[inst_id] = process
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=asyncio.Lock()
    )

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        assert (await db.get(Task, task_id)).status == "cancelled"
    completed_events = [
        call
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1
        and call.args[0] == "tasks"
        and call.args[1].get("new_status") == "completed"
    ]
    assert not completed_events


# === Lifecycle finally block integration tests ===


@pytest.mark.asyncio
async def test_lifecycle_resets_instance_on_exception(db_factory):
    """Instance is reset to idle even when lifecycle throws an exception."""
    d = _make_dispatcher(db_factory)
    d.instance_manager.launch = AsyncMock(side_effect=RuntimeError("boom"))

    async with db_factory() as db:
        inst = Instance(name="exc-worker", status="running", pid=12345)
        db.add(inst)
        task = Task(title="exc-test", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        assert inst.pid is None


@pytest.mark.asyncio
async def test_lifecycle_success_does_not_double_reset(db_factory):
    """On normal success, instance ends in idle state (consume_output or safety net)."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="success-worker")
        db.add(inst)
        task = Task(title="success-test", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


# === Task deletion clears instance.current_task_id ===


@pytest.mark.asyncio
async def test_delete_task_clears_instance_current_task_id(db_factory):
    """Deleting a task clears current_task_id on any instance pointing to it."""
    async with db_factory() as db:
        inst = Instance(name="ref-worker", current_task_id=None)
        db.add(inst)
        task = Task(title="del-test", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id
        # Set current_task_id after we know the task ID
        inst.current_task_id = task_id
        await db.commit()

    async with db_factory() as db:
        queue = TaskQueue(db)
        result = await queue.delete(task_id)
        assert result is True

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.current_task_id is None


@pytest.mark.asyncio
async def test_delete_task_no_instance_reference(db_factory):
    """Deleting a task with no instance reference works fine."""
    async with db_factory() as db:
        task = Task(title="orphan-task", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    async with db_factory() as db:
        queue = TaskQueue(db)
        result = await queue.delete(task_id)
        assert result is True


# === Stop-session orphan handling ===


@pytest.mark.asyncio
async def test_stop_session_orphaned_task_marked_completed(client, session_factory):
    """Stop-session with no process marks executing task as completed."""
    async with session_factory() as db:
        task = Task(title="orphan-stop", description="test", status="executing",
                    session_id="sess-123")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "completed" in data.get("note", "")

    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "completed"
        assert t.session_id == "sess-123"


@pytest.mark.asyncio
async def test_stop_session_pending_task_returns_error(client, session_factory):
    """Stop-session on a pending task (no process, not executing) returns 400."""
    async with session_factory() as db:
        task = Task(title="pending-stop", description="test", status="pending")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stop_session_completed_task_returns_error(client, session_factory):
    """Stop-session on a completed task (no process) returns 400."""
    async with session_factory() as db:
        task = Task(title="done-stop", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stop_session_in_progress_task_marked_completed(client, session_factory):
    """Stop-session with no process marks in_progress task as completed."""
    async with session_factory() as db:
        task = Task(title="in-progress-stop", description="test", status="in_progress")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    assert resp.json()["ok"] is True


# === Detached PTY recovery ===


@pytest.mark.asyncio
async def test_startup_sub_agent_cleanup_defers_durable_monitor_recovery(
    db_factory,
):
    from backend import main as main_module

    async with db_factory() as db:
        background_task = Task(
            title="terminal foreground with native tail",
            description="work",
            status="completed",
            pty_background_generation="exact-native-epoch",
        )
        ordinary_task = Task(
            title="ordinary terminal parent",
            description="work",
            status="completed",
        )
        db.add_all([background_task, ordinary_task])
        await db.flush()
        rows = [
            SubAgentSession(
                task_id=background_task.id,
                source="native",
                agent_type="native-agent",
                description="dispatcher must recover this row",
                status="running",
            ),
            SubAgentSession(
                task_id=background_task.id,
                source="ccm",
                agent_type="monitor",
                description="sleeping durable CCM monitor",
                status="running",
                next_check_at=datetime.utcnow(),
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="ccm",
                agent_type="monitor",
                description="uncertain active CCM monitor",
                status="running",
                turn_generation=4,
                active_turn_generation=4,
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="native",
                agent_type="native-agent",
                description="no live background generation",
                status="running",
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="ccm",
                agent_type="sub_agent",
                description="ordinary one-shot CCM child",
                status="running",
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="ccm",
                agent_type="monitor",
                remote_id=77,
                description="remote monitor mirror",
                status="running",
            ),
        ]
        db.add_all(rows)
        await db.commit()
        row_ids = [row.id for row in rows]

    with patch.object(main_module, "async_session", db_factory):
        await main_module._cleanup_stale_sub_agents()

    async with db_factory() as db:
        current = [await db.get(SubAgentSession, row_id) for row_id in row_ids]
        assert current[0].status == "running"
        assert current[0].completed_at is None
        assert current[1].status == "running"
        assert current[1].completed_at is None
        assert current[2].status == "running"
        assert current[2].completed_at is None
        assert current[3].status == "completed"
        assert current[3].completed_at is not None
        assert current[4].status == "completed"
        assert current[4].completed_at is not None
        assert current[5].status == "completed"
        assert current[5].completed_at is not None

    dispatcher = _make_dispatcher(db_factory)
    await dispatcher._cleanup_stale_state()
    with patch.object(dispatcher, "start_monitor_session") as start:
        await dispatcher._recover_monitor_sessions()

    assert [call.args[0].id for call in start.call_args_list] == [row_ids[1]]
    async with db_factory() as db:
        uncertain = await db.get(SubAgentSession, row_ids[2])
        assert uncertain.status == "failed"
        assert uncertain.next_check_at is None
        assert "could not be recovered" in (uncertain.last_error or "")


@pytest.mark.asyncio
async def test_startup_fails_closed_orphaned_pty_background_marker(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d.instance_manager.active_pty_background_task_ids.return_value = set()

    async with db_factory() as db:
        task = Task(
            title="orphaned PTY background",
            description="background tail was interrupted",
            status="executing",
            session_id="lost-session",
            pty_background_generation="lost-exact-epoch",
        )
        db.add(task)
        await db.flush()
        native_session = SubAgentSession(
                task_id=task.id,
                source="native",
                agent_type="native-agent",
                description="lost agent",
                status="running",
            )
        db.add(native_session)
        await db.commit()
        task_id = task.id
        native_session_id = native_session.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "failed"
        assert current.pty_background_generation is None
        assert "restarted before Claude PTY background" in (
            current.error_message or ""
        )
        log = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "system_event",
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalar_one()
        assert "restarted before Claude PTY background" in log.content
        assert (
            await db.get(SubAgentSession, native_session_id)
        ).status == "failed"

    assert any(
        call.args[0] == "tasks"
        and call.args[1].get("event") == "status_change"
        and call.args[1].get("task_id") == task_id
        and call.args[1].get("new_status") == "failed"
        for call in d.broadcaster.broadcast.await_args_list
    )


# === Mixed scenario: startup with multiple stale entities ===


@pytest.mark.asyncio
async def test_cleanup_multiple_stale_entities(db_factory):
    """Cleanup handles multiple stale instances and tasks in one pass."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        # Two dead instances
        inst1 = Instance(name="dead-1", status="running", pid=999991)
        inst2 = Instance(name="dead-2", status="running", pid=999992)
        # One alive instance
        inst3 = Instance(name="alive", status="idle")
        # Two stuck tasks
        task1 = Task(title="stuck-1", description="t", status="executing")
        task2 = Task(title="stuck-2", description="t", status="in_progress")
        # One normal task
        task3 = Task(title="normal", description="t", status="pending")
        for obj in [inst1, inst2, inst3, task1, task2, task3]:
            db.add(obj)
        await db.commit()
        for obj in [inst1, inst2, inst3, task1, task2, task3]:
            await db.refresh(obj)
        ids = {
            "inst1": inst1.id, "inst2": inst2.id, "inst3": inst3.id,
            "task1": task1.id, "task2": task2.id, "task3": task3.id,
        }

    await d._cleanup_stale_state()

    async with db_factory() as db:
        assert (await db.get(Instance, ids["inst1"])).status == "error"
        assert (await db.get(Instance, ids["inst2"])).status == "error"
        assert (await db.get(Instance, ids["inst3"])).status == "idle"
        assert (await db.get(Task, ids["task1"])).status == "pending"
        assert (await db.get(Task, ids["task2"])).status == "pending"
        assert (await db.get(Task, ids["task3"])).status == "pending"
