"""Database-enforced Capability Core ownership fences."""

import pytest
from sqlalchemy.exc import IntegrityError

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.task import Task


_HASH = "0" * 64


def _invocation(
    task_id: int,
    key: str,
    *,
    status: str = "queued",
    active_task_id: int | None = None,
) -> CapabilityInvocation:
    return CapabilityInvocation(
        task_id=task_id,
        capability_key="plan",
        source="human_request",
        purpose="advisory",
        status=status,
        state_version=1,
        idempotency_key=key,
        input_payload={},
        input_hash=_HASH,
        subject_kind="task_generation",
        subject_ref={"task_id": task_id},
        subject_hash=_HASH,
        executor_kind="fake",
        executor_config={},
        executor_config_hash=_HASH,
        policy_snapshot={},
        policy_hash=_HASH,
        resume_policy="attach_only",
        max_attempts=1,
        active_task_id=(task_id if active_task_id is None else active_task_id),
    )


def _execution(
    invocation_id: int,
    key: str,
    *,
    attempt: int = 1,
    status: str = "queued",
    active_invocation_id: int | None = None,
) -> CapabilityExecution:
    return CapabilityExecution(
        invocation_id=invocation_id,
        attempt=attempt,
        status=status,
        state_version=1,
        active_invocation_id=(
            invocation_id
            if active_invocation_id is None
            else active_invocation_id
        ),
        idempotency_key=key,
        executor_kind="fake",
        input_hash=_HASH,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_active_task_id", [-1, 0])
async def test_active_invocation_must_own_its_task_slot(
    db_session,
    bad_active_task_id,
):
    task = Task(title="owner fence")
    db_session.add(task)
    await db_session.flush()
    db_session.add(
        _invocation(
            task.id,
            "bad-owner",
            active_task_id=bad_active_task_id,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_active_invocation_requires_non_null_slot(db_session):
    task = Task(title="null fence")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "null-owner")
    invocation.active_task_id = None
    db_session.add(invocation)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_terminal_invocation_must_release_slot(db_session):
    task = Task(title="terminal fence")
    db_session.add(task)
    await db_session.flush()
    db_session.add(
        _invocation(task.id, "terminal-owner", status="failed")
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_invocation_per_task(db_session):
    task = Task(title="unique task slot")
    db_session.add(task)
    await db_session.flush()
    db_session.add_all(
        [
            _invocation(task.id, "one"),
            _invocation(task.id, "two"),
        ]
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_execution_active_slot_owner_and_uniqueness(db_session):
    task = Task(title="execution fence")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "invocation")
    db_session.add(invocation)
    await db_session.flush()
    db_session.add(
        _execution(
            invocation.id,
            "bad-execution-owner",
            active_invocation_id=-1,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_execution_per_invocation(db_session):
    task = Task(title="unique execution slot")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "invocation")
    db_session.add(invocation)
    await db_session.flush()
    db_session.add_all(
        [
            _execution(invocation.id, "execution-one"),
            _execution(invocation.id, "execution-two", attempt=2),
        ]
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()
