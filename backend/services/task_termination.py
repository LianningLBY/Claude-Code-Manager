"""Generation-safe local Task termination primitives.

Task rows and reusable Instance slots form one ownership relationship.  A
terminal status alone does not stop an already-running agent, while looking up
the Instance after committing that status can race with slot reuse.  This
module centralizes the exact-generation fences shared by task APIs and
background callers such as PR Monitor.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, Awaitable, TypeVar

from sqlalchemy import select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.instance import Instance
from backend.models.task import Task
from backend.services.task_queue import (
    PR_REVIEW_SUPERSEDED_METADATA_KEY,
    task_retry_not_superseded_predicate,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from backend.services.worker_relay import WorkerTaskGeneration


class TaskTerminationConflict(RuntimeError):
    """A local Task generation could not be proven safely terminated."""


class TaskQueueTerminationConflict(TaskTerminationConflict):
    """The queued/in-flight message consumer did not settle."""


class TaskGenerationTerminationConflict(TaskTerminationConflict):
    """The Task changed generation during termination."""


class TaskLaunchTerminationConflict(TaskTerminationConflict):
    """A pre-owner process launch could not be proven aborted."""


class TaskProcessTerminationConflict(TaskTerminationConflict):
    """One or more exact Instance generations could not be proven reaped."""

    def __init__(self, instance_ids: list[int]):
        self.instance_ids = instance_ids
        if instance_ids:
            message = (
                "Process cleanup could not be confirmed for instance(s): "
                + ", ".join(map(str, instance_ids))
            )
        else:
            message = "Detached Task/session cleanup could not be confirmed"
        super().__init__(message)


class TaskAuxiliaryTerminationConflict(TaskTerminationConflict):
    """One or more CCM-owned auxiliary sessions could not be reaped."""

    def __init__(self, session_ids: list[int]):
        self.session_ids = session_ids
        super().__init__(
            "Auxiliary cleanup could not be confirmed for session(s): "
            + ", ".join(map(str, session_ids))
        )


class WorkerTaskTerminationConflict(TaskTerminationConflict):
    """A Worker-owned Task could not be authoritatively stopped and mirrored."""


@dataclass(frozen=True)
class TaskTerminationResult:
    task_id: int
    previous_status: str
    terminal_status: str
    transitioned: bool
    stopped: bool
    cleared_messages: int
    retry_count: int
    turn_generation: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    pty_background_generation: str | None


@dataclass(frozen=True)
class WorkerTaskTerminationResult:
    task_id: int
    observed: WorkerTaskGeneration
    resulting: WorkerTaskGeneration


@dataclass(frozen=True)
class LocalTaskGeneration:
    """Exact scalar generation expected by a local termination request."""

    status: str
    retry_count: int
    turn_generation: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    pty_background_generation: str | None


_T = TypeVar("_T")
_MAX_LATE_AUXILIARY_REAP_SWEEPS = 8
_AUXILIARY_TERMINAL_STATUSES = frozenset({"completed", "failed", "stopped"})


async def _finish_despite_cancellation(awaitable: Awaitable[_T]) -> _T:
    """Finish safety-critical cleanup before propagating caller cancellation."""

    operation = asyncio.create_task(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not operation.done():
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError as exc:
            cancellation = exc
    result = operation.result()
    if cancellation is not None:
        raise cancellation
    return result


def _utc_naive(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def local_task_generation(task: Task) -> LocalTaskGeneration:
    return LocalTaskGeneration(
        status=task.status,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        instance_id=task.instance_id,
        started_at=_utc_naive(task.started_at),
        completed_at=_utc_naive(task.completed_at),
        pty_background_generation=task.pty_background_generation,
    )


def normalize_local_task_generation(
    generation: LocalTaskGeneration,
) -> LocalTaskGeneration:
    return LocalTaskGeneration(
        status=generation.status,
        retry_count=generation.retry_count,
        turn_generation=generation.turn_generation,
        instance_id=generation.instance_id,
        started_at=_utc_naive(generation.started_at),
        completed_at=_utc_naive(generation.completed_at),
        pty_background_generation=generation.pty_background_generation,
    )


def local_task_generation_predicates(
    task_id: int,
    generation: LocalTaskGeneration,
) -> list:
    generation = normalize_local_task_generation(generation)
    return [
        Task.id == task_id,
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
        Task.status == generation.status,
        Task.retry_count == generation.retry_count,
        Task.turn_generation == generation.turn_generation,
        (
            Task.instance_id.is_(None)
            if generation.instance_id is None
            else Task.instance_id == generation.instance_id
        ),
        (
            Task.started_at.is_(None)
            if generation.started_at is None
            else Task.started_at == generation.started_at
        ),
        (
            Task.completed_at.is_(None)
            if generation.completed_at is None
            else Task.completed_at == generation.completed_at
        ),
        (
            Task.pty_background_generation.is_(None)
            if generation.pty_background_generation is None
            else Task.pty_background_generation == generation.pty_background_generation
        ),
    ]


def task_generation_fence(task_id: int, task: Task) -> list:
    """Build an exact Task-generation CAS predicate from an observed row."""

    return local_task_generation_predicates(
        task_id,
        local_task_generation(task),
    )


async def stop_task_process(
    task_id: int,
    db: AsyncSession,
    *,
    expected_generations: list[tuple[int, int | None, datetime | None]],
    expected_task_turn_generation: int,
    task_status: str = "completed",
    allow_delivery_effect_stop: bool = False,
) -> bool:
    """Stop only exact Instance generations invalidated by the caller.

    ``Task.instance_id`` is historical after a turn completes and the reusable
    slot may already belong to another task. Callers must snapshot the reverse
    Instance owner rows in the same transaction that terminally CASes the Task,
    then pass them here. Discovering owners after that commit can target a rapid
    retry of the same task id (ABA), even when PID/start fences are later used.
    """

    from backend.main import instance_manager

    stopped = False
    for instance_id, expected_pid, expected_started_at in expected_generations:
        protected_stop_kwargs = (
            {"allow_delivery_effect_stop": True}
            if allow_delivery_effect_stop
            else {}
        )
        generation_stopped = await instance_manager.stop(
            instance_id,
            expected_task_id=task_id,
            expected_task_turn_generation=expected_task_turn_generation,
            expected_pid=expected_pid,
            expected_started_at=expected_started_at,
            task_status=task_status,
            terminal_consumer_timeout=30.0,
            consumer_cancel_timeout=10.0,
            **protected_stop_kwargs,
        )
        if not generation_stopped:
            # ``stop(False)`` can still mean that terminal bookkeeping won
            # and only a later publication lost its fence. Re-read outside
            # the old transaction before attempting orphan reconciliation.
            await db.rollback()
            exact_owner_remains = await db.scalar(
                select(Instance.id).where(
                    Instance.id == instance_id,
                    Instance.current_task_id == task_id,
                    (
                        Instance.pid.is_(None)
                        if expected_pid is None
                        else Instance.pid == expected_pid
                    ),
                    (
                        Instance.started_at.is_(None)
                        if expected_started_at is None
                        else Instance.started_at == expected_started_at
                    ),
                )
            )
            await db.rollback()
        else:
            exact_owner_remains = None
        if not generation_stopped and exact_owner_remains is not None:
            generation_stopped = (
                await instance_manager.reconcile_dead_reverse_task_owner(
                    instance_id,
                    expected_task_id=task_id,
                    expected_pid=expected_pid,
                    expected_started_at=expected_started_at,
                )
            )
        stopped = generation_stopped or stopped
    return stopped


async def settle_task_launch_barrier(
    task_id: int,
    instance_id: int | None,
) -> None:
    """Prove a pre-owner launch aborted after the Task became terminal."""

    if instance_id is None:
        return
    from backend.main import instance_manager

    settled = await instance_manager.wait_for_task_launch_barrier(
        instance_id,
        task_id,
    )
    if not settled:
        raise TaskLaunchTerminationConflict(
            "Task was made terminal, but a pre-owner process launch could not "
            "be proven stopped"
        )


async def remaining_task_process_generations(
    task_id: int,
    db: AsyncSession,
    *,
    expected_generations: list[tuple[int, int | None, datetime | None]],
) -> list[int]:
    """Return exact owner generations that stop could not clear.

    ``InstanceManager.stop(False)`` can mean either "the old generation was
    already gone" or "runtime cleanup could not be proven". A locking/current
    read distinguishes those cases even under MySQL REPEATABLE READ.
    """

    remaining: list[int] = []
    for instance_id, expected_pid, expected_started_at in expected_generations:
        predicates = [
            Instance.id == instance_id,
            Instance.current_task_id == task_id,
            (
                Instance.pid.is_(None)
                if expected_pid is None
                else Instance.pid == expected_pid
            ),
            (
                Instance.started_at.is_(None)
                if expected_started_at is None
                else Instance.started_at == expected_started_at
            ),
        ]
        owner = await db.scalar(
            select(Instance.id).where(*predicates).with_for_update()
        )
        if owner is not None:
            remaining.append(instance_id)
    # Release any row locks before further lifecycle waits/broadcasts.
    await db.rollback()
    return remaining


async def lock_task_generation(
    task_id: int,
    db: AsyncSession,
    *,
    expected_status: str,
    expected_retry_count: int,
    expected_turn_generation: int,
    expected_instance_id: int | None,
    expected_started_at: datetime | None,
    expected_completed_at: datetime | None,
    expected_pty_background_generation: str | None,
) -> Task | None:
    """Lock one exact Task generation until its terminal event is published."""

    predicates = [
        Task.id == task_id,
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
        Task.status == expected_status,
        Task.retry_count == expected_retry_count,
        Task.turn_generation == expected_turn_generation,
        (
            Task.instance_id.is_(None)
            if expected_instance_id is None
            else Task.instance_id == expected_instance_id
        ),
        (
            Task.started_at.is_(None)
            if expected_started_at is None
            else Task.started_at == expected_started_at
        ),
        (
            Task.completed_at.is_(None)
            if expected_completed_at is None
            else Task.completed_at == expected_completed_at
        ),
        (
            Task.pty_background_generation.is_(None)
            if expected_pty_background_generation is None
            else Task.pty_background_generation == expected_pty_background_generation
        ),
    ]
    locked = await db.execute(
        sa_update(Task).where(*predicates).values(status=expected_status)
    )
    if not locked.rowcount:
        await db.rollback()
        return None
    db.expire_all()
    return await db.get(Task, task_id)


async def read_persisted_task_completed_at(
    task_id: int,
    db: AsyncSession,
) -> datetime | None:
    """Read the database-normalized terminal timestamp written here."""

    return await db.scalar(
        select(Task.completed_at)
        .where(
            Task.id == task_id,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
        )
        .with_for_update()
    )


def _auxiliary_runtime_snapshot(
    dispatcher,
    session_ids: list[int],
) -> tuple[set[int], set[int]]:
    """Read and validate Dispatcher-owned exact auxiliary evidence."""

    try:
        snapshot = dispatcher._active_auxiliary_session_ids()
    except Exception as exc:
        raise TaskAuxiliaryTerminationConflict(session_ids) from exc
    if (
        not isinstance(snapshot, tuple)
        or len(snapshot) != 2
        or not all(isinstance(ids, set) for ids in snapshot)
        or any(type(session_id) is not int for ids in snapshot for session_id in ids)
    ):
        raise TaskAuxiliaryTerminationConflict(session_ids)
    return snapshot


def _auxiliary_runtime_present(
    *,
    session_id: int,
    agent_type: str,
    monitor_ids: set[int],
    sub_agent_ids: set[int],
) -> bool:
    """Return exact expected runtime evidence; mismatched evidence is unsafe."""

    if agent_type == "monitor":
        if session_id in sub_agent_ids:
            raise TaskAuxiliaryTerminationConflict([session_id])
        return session_id in monitor_ids
    if agent_type == "sub_agent":
        if session_id in monitor_ids:
            raise TaskAuxiliaryTerminationConflict([session_id])
        return session_id in sub_agent_ids
    raise TaskAuxiliaryTerminationConflict([session_id])


async def _stop_auxiliary_sessions(
    auxiliary_sessions: list[tuple[int, str, str, str]],
) -> set[int]:
    """Prove each CCM child reaped, including DB-terminal live generations.

    A completed/failed/stopped row is only historical when Dispatcher has no
    exact lifecycle/process/thread/home evidence for that session id.  Active
    DB rows still call the idempotent stop path when no registry entry exists,
    covering a pre-registration launch window.  Any retained or mismatched
    runtime evidence after stop is fail-closed.
    """

    from backend.main import dispatcher

    confirmed: set[int] = set()
    all_ids = [session_id for session_id, *_rest in auxiliary_sessions]
    for session_id, agent_type, source, status in auxiliary_sessions:
        if source != "ccm":
            raise TaskAuxiliaryTerminationConflict([session_id])
        monitor_ids, sub_agent_ids = _auxiliary_runtime_snapshot(
            dispatcher,
            all_ids,
        )
        runtime_present = _auxiliary_runtime_present(
            session_id=session_id,
            agent_type=agent_type,
            monitor_ids=monitor_ids,
            sub_agent_ids=sub_agent_ids,
        )
        if not runtime_present and status in _AUXILIARY_TERMINAL_STATUSES:
            confirmed.add(session_id)
            continue
        try:
            if agent_type == "sub_agent":
                await dispatcher.stop_sub_agent_session_process(session_id)
            elif agent_type == "monitor":
                await dispatcher.stop_monitor_session_process(session_id)
            else:
                raise TaskAuxiliaryTerminationConflict([session_id])
        except asyncio.CancelledError:
            raise
        except TaskAuxiliaryTerminationConflict:
            raise
        except Exception as exc:
            raise TaskAuxiliaryTerminationConflict([session_id]) from exc

        monitor_ids, sub_agent_ids = _auxiliary_runtime_snapshot(
            dispatcher,
            all_ids,
        )
        if _auxiliary_runtime_present(
            session_id=session_id,
            agent_type=agent_type,
            monitor_ids=monitor_ids,
            sub_agent_ids=sub_agent_ids,
        ):
            raise TaskAuxiliaryTerminationConflict([session_id])
        confirmed.add(session_id)
    return confirmed


def _auxiliary_sessions_requiring_reap(
    dispatcher,
    auxiliary_sessions: list[tuple[int, str, str, str]],
    confirmed_ids: set[int],
) -> list[tuple[int, str, str, str]]:
    """Include new rows and any confirmed id whose runtime evidence reappeared."""

    all_ids = [session_id for session_id, *_rest in auxiliary_sessions]
    monitor_ids, sub_agent_ids = _auxiliary_runtime_snapshot(
        dispatcher,
        all_ids,
    )
    requiring_reap = []
    for row in auxiliary_sessions:
        session_id, agent_type, source, _status = row
        if source != "ccm":
            raise TaskAuxiliaryTerminationConflict([session_id])
        runtime_present = _auxiliary_runtime_present(
            session_id=session_id,
            agent_type=agent_type,
            monitor_ids=monitor_ids,
            sub_agent_ids=sub_agent_ids,
        )
        if session_id not in confirmed_ids or runtime_present:
            requiring_reap.append(row)
    return requiring_reap


def _same_local_identity(
    task: Task,
    generation: LocalTaskGeneration,
) -> bool:
    """Compare fields that cannot legitimately change during exact stop."""

    generation = normalize_local_task_generation(generation)
    return (
        task.worker_id is None
        and task.shared_from_id is None
        and task.retry_count == generation.retry_count
        and task.turn_generation == generation.turn_generation
        and task.instance_id == generation.instance_id
        and _utc_naive(task.started_at) == generation.started_at
    )


async def _read_pr_review_supersede_generation(
    task_id: int,
    db: AsyncSession,
    *,
    expected_generation: LocalTaskGeneration | None,
    active_statuses: tuple[str, ...],
    terminal_statuses: tuple[str, ...],
) -> LocalTaskGeneration:
    """Take the exact pre-abort generation before installing admission gate.

    Queue abort can time out before any process ownership is known. Persisting
    a retry block before that wait would strand an active Task when cleanup
    cannot even begin.  After abort succeeds, the returned scalar generation
    fences the durable supersede gate and exact owner snapshot.
    """

    await db.rollback()
    db.expire_all()
    predicates = [
        Task.id == task_id,
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
    ]
    if expected_generation is not None:
        predicates = local_task_generation_predicates(
            task_id,
            expected_generation,
        )
    task = (await db.execute(select(Task).where(*predicates))).scalar_one_or_none()
    if task is None:
        await db.rollback()
        raise TaskGenerationTerminationConflict(
            f"Task {task_id} no longer matches the expected local generation"
        )
    if task.status not in active_statuses + terminal_statuses:
        await db.rollback()
        raise TaskGenerationTerminationConflict(
            f"Task {task_id} cannot be superseded from status {task.status}"
        )

    generation = local_task_generation(task)
    await db.rollback()
    return generation


async def _terminate_local_task_generation_impl(
    task_id: int,
    db: AsyncSession,
    *,
    reason: str,
    expected_generation: LocalTaskGeneration | None = None,
    active_statuses: tuple[str, ...] = (
        "pending",
        "in_progress",
        "executing",
        "merging",
    ),
    terminal_statuses: tuple[str, ...] = (
        "completed",
        "failed",
        "cancelled",
        "conflict",
    ),
    allow_delivery_effect_stop: bool = False,
) -> TaskTerminationResult:
    """Safely terminalize one local Task and reap its exact Instance owners.

    Queue admission and any pre-owner launch are settled first.  The exact
    Task/Instance/background generation is then snapshotted, but remains
    non-terminal while its process tree is stopped.  Only a confirmed reap may
    be followed by the terminal metadata CAS/publication.
    """

    from backend.main import dispatcher

    pre_abort_generation = await _read_pr_review_supersede_generation(
        task_id,
        db,
        expected_generation=expected_generation,
        active_statuses=active_statuses,
        terminal_statuses=terminal_statuses,
    )

    try:
        cleared = await dispatcher.abort_task_queue(
            task_id,
            cancel_durable=False,
            durable_db=db,
        )
    except Exception as exc:
        from backend.services.dispatcher import TaskQueueAbortTimeoutError

        if isinstance(exc, TaskQueueAbortTimeoutError):
            raise TaskQueueTerminationConflict(
                f"Task {task_id} queue worker could not be proven stopped"
            ) from exc
        raise

    # A launch already inside the lifecycle lock may finish while queue abort
    # settles.  Wait for that exact slot first, then take a fresh Task→Instance
    # snapshot.  Nothing terminal has been written yet.
    await settle_task_launch_barrier(
        task_id,
        pre_abort_generation.instance_id,
    )
    await db.rollback()
    db.expire_all()
    task = (
        await db.execute(
            select(Task)
            .where(
                *local_task_generation_predicates(
                    task_id,
                    pre_abort_generation,
                )
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        await db.rollback()
        raise TaskGenerationTerminationConflict(
            f"Task {task_id} disappeared or changed execution authority"
        )

    previous_status = task.status
    transitioned = previous_status in active_statuses
    if not transitioned and previous_status not in terminal_statuses:
        await db.rollback()
        raise TaskGenerationTerminationConflict(
            f"Task {task_id} cannot be terminated from status {previous_status}"
        )

    terminal_status = "completed" if transitioned else previous_status
    metadata = dict(task.metadata_ or {})
    marker_already_persisted = metadata.get(PR_REVIEW_SUPERSEDED_METADATA_KEY) is True
    if not marker_already_persisted:
        metadata[PR_REVIEW_SUPERSEDED_METADATA_KEY] = True
        gate = await db.execute(
            sa_update(Task)
            .where(
                *task_generation_fence(task_id, task),
                task_retry_not_superseded_predicate(),
            )
            .values(metadata_=metadata)
        )
        if not gate.rowcount:
            await db.rollback()
            raise TaskGenerationTerminationConflict(
                f"Task {task_id} generation changed while admission was closing"
            )

    owner_rows = await db.execute(
        select(
            Instance.id,
            Instance.pid,
            Instance.started_at,
        )
        .where(Instance.current_task_id == task_id)
        .with_for_update()
    )
    expected_generations = list(owner_rows.all())

    from backend.models.monitor_session import MonitorSession

    auxiliary_rows = await db.execute(
        select(
            MonitorSession.id,
            MonitorSession.agent_type,
            MonitorSession.source,
            MonitorSession.status,
        )
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.source == "ccm",
            MonitorSession.agent_type.in_(("monitor", "sub_agent")),
        )
        .with_for_update()
    )
    auxiliary_sessions = list(auxiliary_rows.all())
    observed_session_id = task.session_id
    observed_generation = local_task_generation(task)
    # The superseded marker is a durable admission gate, not a terminal state.
    # Commit it with the exact owner snapshot before any process I/O so dequeue,
    # retry, migration and launch cannot create a replacement during stop.
    await db.commit()

    if len(expected_generations) > 1:
        # InstanceManager.stop terminalizes one exact owner after reaping it.
        # With a corrupt multi-owner Task that would expose terminal state while
        # another process still runs.  Preserve every owner for explicit
        # reconciliation instead of choosing an unsafe order.
        raise TaskProcessTerminationConflict(
            [instance_id for instance_id, _pid, _started_at in expected_generations]
        )

    # Stop auxiliary process groups while the Task still advertises its real
    # active state.  A failure leaves both Task and auxiliary DB evidence
    # unchanged and retryable.
    reaped_auxiliary_ids = await _stop_auxiliary_sessions(auxiliary_sessions)

    stopped = False
    detached_stop_used = False
    stop_error: Exception | None = None
    if expected_generations:
        try:
            stopped = await stop_task_process(
                task_id,
                db,
                expected_generations=expected_generations,
                expected_task_turn_generation=(
                    observed_generation.turn_generation
                ),
                task_status=(
                    terminal_status
                    if terminal_status in {"completed", "cancelled"}
                    else "completed"
                ),
                allow_delivery_effect_stop=allow_delivery_effect_stop,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # stop() may have reaped and atomically committed its exact terminal
            # Task/Instance generation before a WS publication failed.  Defer
            # classification until the authoritative owner read below: retained
            # owner evidence is failure; an absent owner is safe to finalize and
            # republish through our own marker-aware fence.
            stop_error = exc

        remaining = await remaining_task_process_generations(
            task_id,
            db,
            expected_generations=expected_generations,
        )
        if remaining:
            conflict = TaskProcessTerminationConflict(remaining)
            if stop_error is not None:
                raise conflict from stop_error
            raise conflict
    elif observed_generation.pty_background_generation is not None:
        # A late PTY tail has no reusable Instance owner.  Stop the retained
        # Task/session/epoch object directly; never address a historical slot.
        if not observed_session_id:
            raise TaskProcessTerminationConflict([])
        from backend.main import instance_manager

        detached_stopped = (
            await instance_manager.stop_detached_pty_background_generation(
                task_id,
                observed_session_id,
                observed_generation.pty_background_generation,
                expected_status=observed_generation.status,
                expected_retry_count=observed_generation.retry_count,
                expected_turn_generation=observed_generation.turn_generation,
                expected_instance_id=observed_generation.instance_id,
                expected_started_at=observed_generation.started_at,
                expected_completed_at=observed_generation.completed_at,
                terminal_status=terminal_status if transitioned else None,
                error_message=reason if transitioned else None,
            )
        )
        if not detached_stopped:
            raise TaskProcessTerminationConflict([])
        stopped = True
        detached_stop_used = True

    # A pre-owner launch could have been inside its lifecycle critical section
    # when the durable gate committed.  Re-enter the exact barrier after stop,
    # then the Task→Instance transaction below proves no reverse owner remains.
    await settle_task_launch_barrier(
        task_id,
        observed_generation.instance_id,
    )

    # stop() is itself stop-first and may already have written/published the
    # requested terminal status after reap.  A mocked/no-owner path may leave
    # the original status for us to transition here.  Before that commit,
    # converge any CCM-owned auxiliary row that landed after the first
    # snapshot.  The final Task→Instance→auxiliary lock transaction closes
    # normal admission; a bounded sweep fails closed if some bypass keeps
    # manufacturing children.
    late_auxiliary_sweeps = 0
    while True:
        await db.rollback()
        db.expire_all()
        current = (
            await db.execute(
                select(Task)
                .where(
                    Task.id == task_id,
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if current is None or not _same_local_identity(current, observed_generation):
            await db.rollback()
            raise TaskGenerationTerminationConflict(
                "Task started a newer generation while its old session was stopping"
            )

        current_completed_at = _utc_naive(current.completed_at)
        unchanged_status = (
            current.status == observed_generation.status
            and current_completed_at == observed_generation.completed_at
        )
        stopped_status = (
            transitioned
            and current.status == terminal_status
            and current.completed_at is not None
            and bool(expected_generations or detached_stop_used)
        )
        if not unchanged_status and not stopped_status:
            await db.rollback()
            raise TaskGenerationTerminationConflict(
                "Task changed status while its exact session was stopping"
            )

        current_background_generation = current.pty_background_generation
        if current_background_generation not in {
            None,
            observed_generation.pty_background_generation,
        }:
            await db.rollback()
            raise TaskGenerationTerminationConflict(
                "Task entered a newer PTY background generation while its old "
                "session was stopping"
            )

        # Recheck any reverse owner under the global Task→Instance lock order.
        # This catches a same-Task replacement owner even if it did not reuse
        # the exact old Instance row.
        replacement_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        if replacement_owner is not None:
            await db.rollback()
            raise TaskProcessTerminationConflict([replacement_owner])

        auxiliary_rows = await db.execute(
            select(
                MonitorSession.id,
                MonitorSession.agent_type,
                MonitorSession.source,
                MonitorSession.status,
            )
            .where(
                MonitorSession.task_id == task_id,
                MonitorSession.source == "ccm",
                MonitorSession.agent_type.in_(("monitor", "sub_agent")),
            )
            .with_for_update()
        )
        unreaped_auxiliary_sessions = _auxiliary_sessions_requiring_reap(
            dispatcher,
            list(auxiliary_rows.all()),
            reaped_auxiliary_ids,
        )
        if unreaped_auxiliary_sessions:
            await db.rollback()
            if late_auxiliary_sweeps >= _MAX_LATE_AUXILIARY_REAP_SWEEPS:
                raise TaskAuxiliaryTerminationConflict(
                    [row.id for row in unreaped_auxiliary_sessions]
                )
            reaped_auxiliary_ids.update(
                await _stop_auxiliary_sessions(unreaped_auxiliary_sessions)
            )
            late_auxiliary_sweeps += 1
            continue

        metadata = dict(current.metadata_ or {})
        marker_already_persisted = (
            metadata.get(PR_REVIEW_SUPERSEDED_METADATA_KEY) is True
        )
        metadata[PR_REVIEW_SUPERSEDED_METADATA_KEY] = True
        values = {
            "status": terminal_status,
            "metadata_": metadata,
            "pty_background_generation": None,
        }
        if transitioned:
            values.update(
                completed_at=(
                    current.completed_at
                    if current.status == terminal_status
                    else datetime.utcnow()
                ),
                error_message=reason,
            )
        generation_predicates = task_generation_fence(task_id, current)
        if not marker_already_persisted:
            generation_predicates.append(task_retry_not_superseded_predicate())
        guarded = await db.execute(
            sa_update(Task).where(*generation_predicates).values(**values)
        )
        if not guarded.rowcount:
            await db.rollback()
            raise TaskGenerationTerminationConflict(
                f"Task {task_id} generation changed before terminal commit"
            )

        if reaped_auxiliary_ids:
            await db.execute(
                sa_update(MonitorSession)
                .where(
                    MonitorSession.id.in_(reaped_auxiliary_ids),
                    MonitorSession.task_id == task_id,
                    MonitorSession.source == "ccm",
                    MonitorSession.agent_type.in_(("monitor", "sub_agent")),
                    MonitorSession.status == "running",
                )
                .values(
                    status="cancelled",
                    completed_at=datetime.utcnow(),
                    next_check_at=None,
                    active_turn_generation=None,
                    turn_started_at=None,
                )
            )
        expected_retry_count = observed_generation.retry_count
        expected_turn_generation = observed_generation.turn_generation
        expected_instance_id = observed_generation.instance_id
        expected_started_at = observed_generation.started_at
        expected_completed_at = await read_persisted_task_completed_at(
            task_id,
            db,
        )
        await db.commit()
        break

    # Monitor process reaping intentionally happened while the parent Task was
    # still active.  Only after the terminal DB commit may a Codex Monitor's
    # resumable thread be deleted.  Cleanup failures remain durable on the
    # Monitor row and are retried at service startup.
    cleanup_codex_monitor = getattr(
        dispatcher,
        "_cleanup_codex_monitor_thread",
        None,
    )
    if callable(cleanup_codex_monitor):
        for session_id, agent_type, source, _status in auxiliary_sessions:
            if (
                session_id in reaped_auxiliary_ids
                and agent_type == "monitor"
                and source == "ccm"
            ):
                try:
                    await cleanup_codex_monitor(session_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Terminal Codex Monitor cleanup will be retried: "
                        "session=%s task=%s",
                        session_id,
                        task_id,
                    )

    # A real owner stop publishes its own exact terminal/background event after
    # reap.  The local/no-owner and detached paths have not, so publish once
    # while holding the final marker-aware generation lock.
    manager_already_published = bool(
        expected_generations
        and stop_error is None
        and current_background_generation is None
        and (
            stopped_status
            or (
                not transitioned
                and observed_generation.pty_background_generation is not None
            )
        )
    )
    should_publish = (
        transitioned or observed_generation.pty_background_generation is not None
    ) and not manager_already_published
    if should_publish:
        locked_task = await lock_task_generation(
            task_id,
            db,
            expected_status=terminal_status,
            expected_retry_count=expected_retry_count,
            expected_turn_generation=expected_turn_generation,
            expected_instance_id=expected_instance_id,
            expected_started_at=expected_started_at,
            expected_completed_at=expected_completed_at,
            expected_pty_background_generation=None,
        )
        if locked_task is None:
            raise TaskGenerationTerminationConflict(
                "Task started a newer generation before terminal publication"
            )
        from backend.services.task_events import broadcast_status_change

        try:
            await broadcast_status_change(
                task_id,
                terminal_status,
                background_active=False,
            )
        except BaseException:
            await db.rollback()
            raise
        await db.commit()

    return TaskTerminationResult(
        task_id=task_id,
        previous_status=previous_status,
        terminal_status=terminal_status,
        transitioned=transitioned,
        stopped=stopped,
        cleared_messages=cleared,
        retry_count=expected_retry_count,
        turn_generation=expected_turn_generation,
        instance_id=expected_instance_id,
        started_at=expected_started_at,
        completed_at=expected_completed_at,
        pty_background_generation=None,
    )


async def _terminate_local_task_generation_with_cancellation_lease(
    task_id: int,
    db: AsyncSession,
    *,
    reason: str,
    expected_generation: LocalTaskGeneration | None,
    active_statuses: tuple[str, ...],
    terminal_statuses: tuple[str, ...],
    allow_delivery_effect_stop: bool,
) -> TaskTerminationResult:
    """Fence new messages through the exact terminal generation commit."""

    from backend.main import dispatcher

    async with dispatcher.task_queue_cancellation_lease(task_id):
        return await _terminate_local_task_generation_impl(
            task_id,
            db,
            reason=reason,
            expected_generation=expected_generation,
            active_statuses=active_statuses,
            terminal_statuses=terminal_statuses,
            allow_delivery_effect_stop=allow_delivery_effect_stop,
        )


async def terminate_local_task_generation(
    task_id: int,
    db: AsyncSession,
    *,
    reason: str,
    expected_generation: LocalTaskGeneration | None = None,
    active_statuses: tuple[str, ...] = (
        "pending",
        "in_progress",
        "executing",
        "merging",
    ),
    terminal_statuses: tuple[str, ...] = (
        "completed",
        "failed",
        "cancelled",
        "conflict",
    ),
    allow_delivery_effect_stop: bool = False,
) -> TaskTerminationResult:
    """Run the complete termination transaction despite caller cancellation.

    Cancellation may arrive while a database commit has an indeterminate
    outcome. Shielding only process cleanup would let the request disappear
    after publishing a terminal Task but before reaping its owner. The whole
    queue-abort → generation-CAS → owner-snapshot → reap → publication flow is
    therefore one delayed-cancellation operation.
    """

    return await _finish_despite_cancellation(
        _terminate_local_task_generation_with_cancellation_lease(
            task_id,
            db,
            reason=reason,
            expected_generation=expected_generation,
            active_statuses=active_statuses,
            terminal_statuses=terminal_statuses,
            allow_delivery_effect_stop=allow_delivery_effect_stop,
        )
    )


async def lock_worker_task_generation(
    db: AsyncSession,
    generation,
) -> Task | None:
    """Lock an exact authoritative Worker mirror generation."""

    from backend.services.worker_relay import worker_task_generation_predicates

    guarded = await db.execute(
        sa_update(Task)
        .where(
            *worker_task_generation_predicates(generation),
            Task.shared_from_id.is_(None),
        )
        .values(status=generation.status)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        return None
    db.expire_all()
    return await db.get(Task, generation.task_id)


async def _terminate_worker_task_generation_impl(
    task_id: int,
    db: AsyncSession,
    *,
    operation_locks_held: bool,
) -> WorkerTaskTerminationResult:
    """Stop one Worker Task under migration/proxy locks and mirror its result."""

    from backend.main import task_migrator, worker_proxy
    from backend.services.worker_relay import (
        apply_authoritative_worker_task,
        authoritative_worker_task_values,
        read_worker_task_generation,
        worker_task_generation,
    )

    if worker_proxy is None:
        raise WorkerTaskTerminationConflict("Worker proxy is not available")

    # End the webhook's earlier REPEATABLE READ snapshot before choosing the
    # authoritative Worker assignment.
    await db.rollback()
    db.expire_all()
    initial_task = (
        await db.execute(
            select(Task).where(
                Task.id == task_id,
                Task.worker_id.is_not(None),
                Task.shared_from_id.is_(None),
            )
        )
    ).scalar_one_or_none()
    if initial_task is None or type(initial_task.worker_id) is not int:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            f"Task {task_id} is absent or no longer Worker-authoritative"
        )
    worker_id = initial_task.worker_id
    await db.rollback()

    migration_lock = (
        task_migrator._locks.setdefault(task_id, asyncio.Lock())
        if task_migrator is not None and not operation_locks_held
        else None
    )
    operation_lock = (
        worker_proxy.task_operation_lock(task_id) if not operation_locks_held else None
    )
    async with AsyncExitStack() as stack:
        if not operation_locks_held:
            if migration_lock is not None:
                await stack.enter_async_context(migration_lock)
            await stack.enter_async_context(operation_lock)

        # Revalidate after both locks. Do not retain a database row lock across
        # network I/O; exact response application below is the durable CAS.
        await db.rollback()
        db.expire_all()
        current_task = (
            await db.execute(
                select(Task).where(
                    Task.id == task_id,
                    Task.worker_id == worker_id,
                    Task.shared_from_id.is_(None),
                )
            )
        ).scalar_one_or_none()
        observed = (
            worker_task_generation(
                current_task,
                expected_worker_id=worker_id,
            )
            if current_task is not None
            else None
        )
        if observed is None:
            await db.rollback()
            raise WorkerTaskTerminationConflict(
                f"Task {task_id} Worker assignment changed before stop"
            )
        await db.rollback()

        routing_task = SimpleNamespace(id=task_id, worker_id=worker_id)
        try:
            remote_before = await worker_proxy.proxy_to_worker(
                routing_task,
                "GET",
                f"/api/tasks/{task_id}/terminate-generation",
                require_json=True,
                operation_lock_held=True,
            )
        except Exception as exc:
            raise WorkerTaskTerminationConflict(
                f"Could not read Worker task {task_id} before stop"
            ) from exc

        if not isinstance(remote_before, dict):
            raise WorkerTaskTerminationConflict(
                f"Worker task {task_id} returned an invalid termination snapshot"
            )
        remote_background_generation = remote_before.get("pty_background_generation")
        if "pty_background_generation" not in remote_before or (
            remote_background_generation is not None
            and not isinstance(remote_background_generation, str)
        ):
            raise WorkerTaskTerminationConflict(
                f"Worker task {task_id} omitted its opaque termination generation"
            )

        remote_values = authoritative_worker_task_values(
            remote_before,
            task_id=task_id,
        )
        if (
            remote_values is None
            or remote_before.get("retry_count") != observed.retry_count
            or remote_before.get("turn_generation") != observed.turn_generation
        ):
            raise WorkerTaskTerminationConflict(
                f"Worker task {task_id} generation does not match its Manager mirror"
            )

        remote_status = remote_before["status"]
        terminal_statuses = {"completed", "failed", "cancelled", "conflict"}
        active_statuses = {"pending", "in_progress", "executing", "merging"}
        if remote_status not in active_statuses | terminal_statuses:
            raise WorkerTaskTerminationConflict(
                f"Worker task {task_id} cannot be stopped from {remote_status}"
            )
        try:
            remote_result = await worker_proxy.proxy_to_worker(
                routing_task,
                "POST",
                f"/api/tasks/{task_id}/terminate-generation",
                body={
                    "expected_status": remote_before["status"],
                    "expected_retry_count": remote_before["retry_count"],
                    "expected_turn_generation": remote_before["turn_generation"],
                    "expected_instance_id": remote_before.get("instance_id"),
                    "expected_started_at": remote_before.get("started_at"),
                    "expected_completed_at": remote_before.get("completed_at"),
                    "expected_pty_background_generation": (
                        remote_background_generation
                    ),
                },
                require_json=True,
                operation_lock_held=True,
            )
        except WorkerTaskTerminationConflict:
            raise
        except Exception as exc:
            # A timeout can mean the Worker committed the stop but its response
            # was lost. Never create a replacement on an indeterminate result;
            # a later synchronize retries through the terminal cleanup branch.
            raise WorkerTaskTerminationConflict(
                f"Worker task {task_id} stop was not authoritatively confirmed"
            ) from exc

        result_values = authoritative_worker_task_values(
            remote_result,
            task_id=task_id,
        )
        if (
            result_values is None
            or remote_result.get("retry_count") != observed.retry_count
            or remote_result.get("turn_generation") != observed.turn_generation
            or remote_result.get("status") not in terminal_statuses
            or (remote_result.get("metadata_") or {}).get(
                PR_REVIEW_SUPERSEDED_METADATA_KEY
            )
            is not True
        ):
            raise WorkerTaskTerminationConflict(
                f"Worker task {task_id} returned a non-terminal generation"
            )

        resulting = await apply_authoritative_worker_task(
            db,
            observed,
            remote_result,
            metadata_updates={
                PR_REVIEW_SUPERSEDED_METADATA_KEY: True,
            },
        )
        if resulting is None:
            # Relay may win the same authoritative status update while the
            # HTTP response is in flight. Re-read and either accept that exact
            # Worker retry generation or CAS the current mirror once more.
            await db.rollback()
            current = await read_worker_task_generation(db, task_id, worker_id)
            if (
                current is None
                or current.retry_count != remote_result["retry_count"]
                or current.turn_generation != remote_result["turn_generation"]
            ):
                raise WorkerTaskTerminationConflict(
                    f"Task {task_id} mirror changed before Worker stop applied"
                )
            resulting = await apply_authoritative_worker_task(
                db,
                current,
                remote_result,
                metadata_updates={
                    PR_REVIEW_SUPERSEDED_METADATA_KEY: True,
                },
            )
            if resulting is None:
                raise WorkerTaskTerminationConflict(
                    f"Task {task_id} mirror rejected Worker stop result"
                )

        locked = await lock_worker_task_generation(db, resulting)
        if locked is None:
            raise WorkerTaskTerminationConflict(
                f"Task {task_id} changed generation before terminal publication"
            )
        if resulting.status != observed.status:
            from backend.services.task_events import broadcast_status_change

            await broadcast_status_change(task_id, resulting.status)
        await db.commit()
        return WorkerTaskTerminationResult(
            task_id=task_id,
            observed=observed,
            resulting=resulting,
        )


async def terminate_worker_task_generation(
    task_id: int,
    db: AsyncSession,
    *,
    operation_locks_held: bool = False,
) -> WorkerTaskTerminationResult:
    """Cancellation-safe authoritative stop for a Worker-owned Task."""

    return await _finish_despite_cancellation(
        _terminate_worker_task_generation_impl(
            task_id,
            db,
            operation_locks_held=operation_locks_held,
        )
    )


async def terminate_authoritative_task_generation(
    task_id: int,
    db: AsyncSession,
    *,
    reason: str,
    operation_locks_held: bool = False,
    expected_local_generation: LocalTaskGeneration | None = None,
    allow_delivery_effect_stop: bool = False,
) -> TaskTerminationResult | WorkerTaskTerminationResult:
    """Route termination to the currently authoritative local/Worker owner."""

    await db.rollback()
    authority = (
        await db.execute(
            select(
                Task.worker_id,
                Task.shared_from_id,
            ).where(Task.id == task_id)
        )
    ).one_or_none()
    await db.rollback()
    if authority is None or authority.shared_from_id is not None:
        raise TaskTerminationConflict(
            f"Task {task_id} is absent or is not authoritative on this Manager"
        )
    if authority.worker_id is None:
        return await terminate_local_task_generation(
            task_id,
            db,
            reason=reason,
            expected_generation=expected_local_generation,
            allow_delivery_effect_stop=allow_delivery_effect_stop,
        )
    if expected_local_generation is not None:
        raise TaskGenerationTerminationConflict(
            f"Task {task_id} moved to a Worker after its local generation "
            "was captured"
        )
    if type(authority.worker_id) is not int:
        raise TaskTerminationConflict(
            f"Task {task_id} has an invalid Worker assignment"
        )
    return await terminate_worker_task_generation(
        task_id,
        db,
        operation_locks_held=operation_locks_held,
    )


@asynccontextmanager
async def task_termination_operation_locks(task_ids):
    """Hold migration/proxy mutation locks through supersede replacement."""

    from backend.main import task_migrator
    from backend.services.worker_proxy import get_task_operation_lock

    async with AsyncExitStack() as stack:
        for task_id in sorted(set(task_ids)):
            if task_migrator is not None:
                migration_lock = task_migrator._locks.setdefault(
                    task_id,
                    asyncio.Lock(),
                )
                await stack.enter_async_context(migration_lock)
            # Retry/chat/delete paths use this module-level lock even when the
            # optional Worker runtime is disabled. Supersede must therefore
            # always take the same lock rather than condition ownership on a
            # constructed WorkerProxy instance.
            await stack.enter_async_context(get_task_operation_lock(task_id))
        yield
