"""Process-local admission fence shared by Test Harness owner lifecycles.

Every path that can materialize a Harness Run or an isolated Browser child
uses the same per-owner lock as Task cancellation/deletion.  The context is
re-entrant for the current asyncio Task so high-level pipelines can keep one
lease across prepare -> reserve -> attach -> activate while lower-level
helpers independently enforce the same boundary.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import AsyncIterator

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task


_registry_guard = threading.Lock()
_locks_by_loop: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = weakref.WeakKeyDictionary()
_held_owner_ids: ContextVar[
    tuple[asyncio.Task[object], frozenset[int]] | None
] = ContextVar(
    "test_harness_held_owner_ids",
    default=None,
)


class TestHarnessOwnerFenceError(RuntimeError):
    """The exact Task incarnation/generation no longer owns admission."""


@dataclass(frozen=True, slots=True)
class TestHarnessOwnerIdentity:
    task_id: int
    incarnation_id: str
    retry_count: int
    turn_generation: int
    status: str


def test_harness_owner_identity(task: Task) -> TestHarnessOwnerIdentity:
    if not task.incarnation_id:
        raise TestHarnessOwnerFenceError(
            "Harness owner Task has no durable incarnation identity"
        )
    return TestHarnessOwnerIdentity(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        status=task.status,
    )


async def lock_test_harness_owner(
    db: AsyncSession,
    identity: TestHarnessOwnerIdentity,
) -> Task:
    """Take a cross-process write lock on one exact owner generation.

    The no-op UPDATE is intentional: PostgreSQL/MySQL lock the matched row,
    while SQLite WAL obtains the writer reservation before any Run/Binding
    insert in this transaction. A delete/retry winner makes rowcount zero.
    """

    locked = await db.execute(
        update(Task)
        .where(
            Task.id == identity.task_id,
            Task.incarnation_id == identity.incarnation_id,
            Task.retry_count == identity.retry_count,
            Task.turn_generation == identity.turn_generation,
            Task.status == identity.status,
        )
        .values(status=identity.status)
    )
    if locked.rowcount != 1:
        raise TestHarnessOwnerFenceError(
            "Harness owner Task incarnation or generation changed"
        )
    owner = (
        await db.execute(
            select(Task).where(
                Task.id == identity.task_id,
                Task.incarnation_id == identity.incarnation_id,
            )
        )
    ).scalar_one_or_none()
    if owner is None:
        raise TestHarnessOwnerFenceError("Harness owner Task disappeared")
    return owner


def _owner_lock(task_id: int) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    with _registry_guard:
        locks = _locks_by_loop.setdefault(loop, {})
        return locks.setdefault(task_id, asyncio.Lock())


@asynccontextmanager
async def test_harness_owner_fence(task_id: int) -> AsyncIterator[None]:
    """Serialize materialization and terminalization for one owner Task."""

    if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id < 1:
        raise ValueError("Test Harness owner Task identity is invalid")
    current_task = asyncio.current_task()
    if current_task is None:  # pragma: no cover - an async context has a Task.
        raise RuntimeError("Test Harness owner fence requires an asyncio Task")
    inherited = _held_owner_ids.get()
    # ContextVars are copied into ``asyncio.create_task`` children.  Re-entry
    # belongs only to the exact coroutine Task that acquired the lock; a child
    # inheriting its parent's Context must take the lock normally.
    held = (
        inherited[1]
        if inherited is not None and inherited[0] is current_task
        else frozenset()
    )
    if task_id in held:
        yield
        return

    lock = _owner_lock(task_id)
    async with lock:
        token = _held_owner_ids.set((current_task, held | {task_id}))
        try:
            yield
        finally:
            _held_owner_ids.reset(token)
