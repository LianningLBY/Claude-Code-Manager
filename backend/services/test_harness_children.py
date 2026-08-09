"""Durable ownership and launch fencing for isolated Browser Agent Tasks.

The in-memory Browser Review job is only an execution handle.  This service
persists the authoritative owner -> child relationship before a child can be
claimed, and keeps cancellation/restart recovery independent from that
ephemeral handle.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.database import async_session
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessChildBinding,
    TestHarnessRun,
)
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.task_creation import stage_task_record

logger = logging.getLogger(__name__)

CHILD_RESERVED = "reserved"
CHILD_READY = "ready"
CHILD_RUNNING = "running"
CHILD_STOPPING = "stopping"
CHILD_STOPPED = "stopped"
CHILD_COMPLETED = "completed"
CHILD_STOP_FAILED = "stop_failed"
BROWSER_REVIEW_SKILL = "browser-review"

CHILD_TERMINAL_STATES = frozenset({CHILD_STOPPED, CHILD_COMPLETED})
TASK_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict", "superseded"}
)
TASK_ACTIVE_STATUSES = frozenset(
    {"pending_activation", "pending", "in_progress", "executing", "merging"}
)
HARNESS_OWNER_TERMINAL_STATES = frozenset(
    {"cancelling", "completed", "failed", "cancelled", "superseded"}
)
WORKSPACE_OWNER_TERMINAL_STATES = frozenset(
    {"cancelling", "completed", "failed", "cancelled"}
)
_UNSET = object()


class TestHarnessChildError(RuntimeError):
    """A Browser Agent child could not be safely attached or stopped."""


class TestHarnessChildRecoveryError(TestHarnessChildError):
    """Startup could not prove that every interrupted child was stopped."""


TaskStopper = Callable[[int], Awaitable[None]]


async def browser_child_identity_error(
    db: AsyncSession,
    binding: TestHarnessChildBinding,
    child: Task,
    *,
    expected_states: frozenset[str] | set[str] | tuple[str, ...] | None = None,
    expected_instance_id: int | None | object = _UNSET,
    expected_retry_count: int | object = _UNSET,
    lock_related: bool = False,
) -> str | None:
    """Return why a durable Browser child is no longer safe to launch.

    The binding, not the public Task projection, is the launch authority.  The
    owner/run checks make an owner deletion or a stale Task-id incarnation a
    hard stop even when another process has already selected the child.
    """

    if expected_states is not None and binding.state not in expected_states:
        return f"binding state changed to {binding.state}"
    if binding.child_task_id != child.id:
        return "binding child identity changed"
    if (
        not binding.child_task_incarnation_id
        or child.incarnation_id != binding.child_task_incarnation_id
    ):
        return "child Task incarnation changed"
    if not binding.owner_task_incarnation_id:
        return "owner Task incarnation is not frozen"
    if binding.skill_name != BROWSER_REVIEW_SKILL:
        return "bound Browser skill identity changed"
    if not binding.browser_review_job_id:
        return "bound Browser job identity is missing"
    if (
        child.provider != binding.provider
        or child.model != binding.model
        or child.effort_level != binding.reasoning_effort
        or child.codex_service_tier != binding.codex_service_tier
    ):
        return "Browser Agent provider/model/effort identity drifted"
    if child.enabled_skills != {
        BROWSER_REVIEW_SKILL: binding.browser_review_job_id
    }:
        return "Browser Agent skill/job projection drifted"
    if child.worker_id is not None or child.shared_from_id is not None:
        return "Browser Agent escaped its local isolated execution scope"
    if expected_instance_id is not _UNSET and (
        binding.claimed_instance_id != expected_instance_id
    ):
        return "Browser Agent Instance claim changed"
    if expected_retry_count is not _UNSET and (
        binding.claimed_retry_count != expected_retry_count
    ):
        return "Browser Agent retry generation changed"

    metadata = dict(child.metadata_ or {})
    expected_metadata = {
        "browser_review_job_id": binding.browser_review_job_id,
        "test_harness_run_id": binding.harness_run_id,
        "workspace_review_run_id": binding.workspace_review_run_id,
        "test_harness_parent_task_id": binding.owner_task_id,
        "workspace_review_parent_task_id": binding.owner_task_id,
        "isolated_browser_agent": True,
    }
    if any(metadata.get(key) != value for key, value in expected_metadata.items()):
        return "Browser Agent durable metadata projection drifted"

    def _stmt(model: type, identity: object):
        statement = select(model).where(model.id == identity)
        return statement.with_for_update() if lock_related else statement

    owner = (
        await db.execute(_stmt(Task, binding.owner_task_id))
    ).scalar_one_or_none()
    if owner is None or owner.incarnation_id != binding.owner_task_incarnation_id:
        return "Browser Agent owner Task disappeared or changed incarnation"

    if binding.harness_run_id:
        run = (
            await db.execute(_stmt(TestHarnessRun, binding.harness_run_id))
        ).scalar_one_or_none()
        if (
            run is None
            or run.task_id != binding.owner_task_id
            or run.task_incarnation_id != binding.owner_task_incarnation_id
            or run.status in HARNESS_OWNER_TERMINAL_STATES
            or run.agent_task_id != child.id
            or run.browser_review_job_id != binding.browser_review_job_id
        ):
            return "Harness run owner/generation is no longer active"
    if binding.workspace_review_run_id:
        workspace_run = (
            await db.execute(
                _stmt(WorkspaceReviewRun, binding.workspace_review_run_id)
            )
        ).scalar_one_or_none()
        if (
            workspace_run is None
            or workspace_run.task_id != binding.owner_task_id
            or workspace_run.task_incarnation_id
            != binding.owner_task_incarnation_id
            or workspace_run.status in WORKSPACE_OWNER_TERMINAL_STATES
            or workspace_run.agent_task_id != child.id
            or workspace_run.browser_review_job_id
            != binding.browser_review_job_id
        ):
            return "Workspace review owner/generation is no longer active"
    return None


def fail_browser_child_identity(
    binding: TestHarnessChildBinding,
    child: Task,
    error: str,
) -> None:
    """Terminalize a drifted child in the caller's fenced transaction."""

    now = datetime.utcnow()
    if child.status in TASK_ACTIVE_STATUSES:
        child.status = "failed"
        child.completed_at = now
        child.error_message = error[:4000]
    binding.state = CHILD_STOPPED
    binding.stop_requested_at = binding.stop_requested_at or now
    binding.completed_at = now
    binding.error = error[:4000]


class TestHarnessChildService:
    """Own the durable lifecycle of isolated Browser Agent Tasks."""

    def __init__(
        self,
        *,
        db_factory: async_sessionmaker[AsyncSession] = async_session,
        task_stopper: TaskStopper | None = None,
    ) -> None:
        self.db_factory = db_factory
        self._task_stopper = task_stopper
        self._stop_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def reserve_child(
        self,
        *,
        owner_task_id: int,
        browser_review_job_id: str,
        child_values: Mapping[str, Any],
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> tuple[Task, TestHarnessChildBinding]:
        """Atomically persist a non-runnable child and its durable owner."""

        if not harness_run_id and not workspace_review_run_id:
            raise TestHarnessChildError("Browser child requires a durable run owner")
        if not browser_review_job_id:
            raise TestHarnessChildError("Browser child requires a Browser Review job")

        values = dict(child_values)
        metadata = dict(values.get("metadata_") or {})
        metadata.update(
            {
                "browser_review_job_id": browser_review_job_id,
                "test_harness_run_id": harness_run_id,
                "workspace_review_run_id": workspace_review_run_id,
                "test_harness_parent_task_id": owner_task_id,
                "workspace_review_parent_task_id": owner_task_id,
                "isolated_browser_agent": True,
            }
        )
        values.update(status="pending_activation", metadata_=metadata)

        async with self.db_factory() as db:
            owner = (
                await db.execute(
                    select(Task)
                    .where(Task.id == owner_task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if owner is None or not owner.incarnation_id:
                raise TestHarnessChildError(
                    "Browser child owner Task disappeared or lacks an incarnation"
                )
            # SQLite ignores ``FOR UPDATE``. This exact no-op update gives the
            # transaction a writer fence before any owner/run edge is created.
            fenced = await db.execute(
                update(Task)
                .where(
                    Task.id == owner_task_id,
                    Task.incarnation_id == owner.incarnation_id,
                )
                .values(incarnation_id=owner.incarnation_id)
            )
            if fenced.rowcount != 1:
                raise TestHarnessChildError(
                    "Browser child owner Task changed while being fenced"
                )

            run: TestHarnessRun | None = None
            workspace_run: WorkspaceReviewRun | None = None
            if harness_run_id:
                run = (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(TestHarnessRun.id == harness_run_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    run is None
                    or run.task_id != owner_task_id
                    or run.status in HARNESS_OWNER_TERMINAL_STATES
                    or (
                        run.task_incarnation_id is not None
                        and run.task_incarnation_id != owner.incarnation_id
                    )
                ):
                    raise TestHarnessChildError(
                        "Harness run disappeared, terminated, or changed owner "
                        "while reserving child"
                    )
                run.task_incarnation_id = owner.incarnation_id
            if workspace_review_run_id:
                workspace_run = (
                    await db.execute(
                        select(WorkspaceReviewRun)
                        .where(WorkspaceReviewRun.id == workspace_review_run_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    workspace_run is None
                    or workspace_run.task_id != owner_task_id
                    or workspace_run.status in WORKSPACE_OWNER_TERMINAL_STATES
                    or (
                        workspace_run.task_incarnation_id is not None
                        and workspace_run.task_incarnation_id
                        != owner.incarnation_id
                    )
                ):
                    raise TestHarnessChildError(
                        "Workspace review disappeared, terminated, or changed "
                        "owner while reserving child"
                    )
                workspace_run.task_incarnation_id = owner.incarnation_id

            child = await stage_task_record(db, **values)
            binding = TestHarnessChildBinding(
                id=uuid.uuid4().hex,
                harness_run_id=harness_run_id,
                workspace_review_run_id=workspace_review_run_id,
                owner_task_id=owner_task_id,
                owner_task_incarnation_id=owner.incarnation_id,
                child_task_id=child.id,
                child_task_incarnation_id=child.incarnation_id,
                browser_review_job_id=browser_review_job_id,
                provider=child.provider,
                model=child.model,
                reasoning_effort=child.effort_level,
                codex_service_tier=child.codex_service_tier,
                skill_name=BROWSER_REVIEW_SKILL,
                state=CHILD_RESERVED,
            )
            db.add(binding)
            if run is not None:
                run.agent_task_id = child.id
                run.browser_review_job_id = browser_review_job_id
            if workspace_run is not None:
                workspace_run.agent_task_id = child.id
                workspace_run.browser_review_job_id = browser_review_job_id
            identity_error = await browser_child_identity_error(
                db,
                binding,
                child,
                expected_states={CHILD_RESERVED},
            )
            if identity_error is not None:
                raise TestHarnessChildError(identity_error)
            await db.commit()
            await db.refresh(child)
            await db.refresh(binding)
            return child, binding

    async def activate(self, binding_id: str) -> TestHarnessChildBinding:
        """Publish the child to TaskQueue only after its job is attached."""

        async with self.db_factory() as db:
            observed = await db.get(TestHarnessChildBinding, binding_id)
            if observed is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            owner = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == observed.owner_task_id,
                        Task.incarnation_id
                        == observed.owner_task_incarnation_id,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if owner is None:
                raise TestHarnessChildError(
                    "Browser child owner Task disappeared before activation"
                )
            fenced = await db.execute(
                update(Task)
                .where(
                    Task.id == observed.owner_task_id,
                    Task.incarnation_id
                    == observed.owner_task_incarnation_id,
                )
                .values(incarnation_id=observed.owner_task_incarnation_id)
            )
            if fenced.rowcount != 1:
                raise TestHarnessChildError(
                    "Browser child owner changed before activation"
                )
            if observed.harness_run_id:
                await db.execute(
                    select(TestHarnessRun)
                    .where(TestHarnessRun.id == observed.harness_run_id)
                    .with_for_update()
                )
            if observed.workspace_review_run_id:
                await db.execute(
                    select(WorkspaceReviewRun)
                    .where(
                        WorkspaceReviewRun.id
                        == observed.workspace_review_run_id
                    )
                    .with_for_update()
                )
            child = (
                await db.execute(
                    select(Task)
                    .where(Task.id == observed.child_task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if child is None:
                raise TestHarnessChildError("Browser child disappeared")
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if binding.state == CHILD_READY:
                return binding
            if binding.state != CHILD_RESERVED:
                raise TestHarnessChildError(
                    f"Browser child cannot activate from state {binding.state}"
                )
            if child.status != "pending_activation":
                raise TestHarnessChildError(
                    "Browser child disappeared or escaped its activation gate"
                )
            identity_error = await browser_child_identity_error(
                db,
                binding,
                child,
                expected_states={CHILD_RESERVED},
            )
            if identity_error is not None:
                fail_browser_child_identity(binding, child, identity_error)
                await db.commit()
                raise TestHarnessChildError(identity_error)
            now = datetime.utcnow()
            child.status = "pending"
            binding.state = CHILD_READY
            binding.activated_at = now
            binding.error = None
            if binding.workspace_review_run_id:
                workspace_run = await db.get(
                    WorkspaceReviewRun, binding.workspace_review_run_id
                )
                if workspace_run is not None:
                    workspace_run.status = "reviewing"
                    workspace_run.stage = "browser_agent_queued"
            await db.commit()
            await db.refresh(binding)
            return binding

    async def abort_reservation(self, binding_id: str, exc: BaseException) -> None:
        """Close a child that failed before its launch gate opened."""

        error = _safe_error(exc)
        async with self.db_factory() as db:
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                return
            child = await db.get(Task, binding.child_task_id)
            if child is not None and child.status == "pending_activation":
                child.status = "cancelled"
                child.completed_at = datetime.utcnow()
                child.error_message = error
            binding.state = CHILD_STOPPED
            binding.stop_requested_at = binding.stop_requested_at or datetime.utcnow()
            binding.completed_at = datetime.utcnow()
            binding.error = error
            await db.commit()

    async def mark_terminal_by_child(
        self,
        child_task_id: int,
        *,
        task_status: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist natural Task completion independently of the job watcher."""

        async with self.db_factory() as db:
            binding = await db.scalar(
                select(TestHarnessChildBinding).where(
                    TestHarnessChildBinding.child_task_id == child_task_id
                )
            )
            if binding is None or binding.state in CHILD_TERMINAL_STATES:
                return
            if task_status is None:
                child = await db.get(Task, child_task_id)
                task_status = child.status if child is not None else None
                error = error or (child.error_message if child is not None else None)
            if task_status not in TASK_TERMINAL_STATUSES:
                return
            binding.state = (
                CHILD_STOPPED if task_status == "cancelled" else CHILD_COMPLETED
            )
            binding.completed_at = datetime.utcnow()
            binding.error = error
            await db.commit()

    async def stop_for_harness_run(self, run_id: str, *, reason: str) -> bool:
        binding_id = await self._binding_id(harness_run_id=run_id)
        if binding_id is None:
            return False
        await self.stop_binding(binding_id, reason=reason)
        return True

    async def stop_for_workspace_run(self, run_id: str, *, reason: str) -> bool:
        binding_id = await self._binding_id(workspace_review_run_id=run_id)
        if binding_id is None:
            return False
        await self.stop_binding(binding_id, reason=reason)
        return True

    async def stop_for_owner(self, task_id: int, *, reason: str) -> int:
        async with self.db_factory() as db:
            binding_ids = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding.id).where(
                            TestHarnessChildBinding.owner_task_id == task_id,
                            TestHarnessChildBinding.state.not_in(
                                CHILD_TERMINAL_STATES
                            ),
                        )
                    )
                ).scalars()
            )
        for binding_id in binding_ids:
            await self.stop_binding(binding_id, reason=reason)
        return len(binding_ids)

    async def stop_binding(self, binding_id: str, *, reason: str) -> None:
        """Stop and verify one exact child generation before returning."""

        lock = await self._stop_lock(binding_id)
        async with lock:
            await _finish_despite_cancellation(
                self._stop_binding_impl(binding_id, reason=reason)
            )

    async def _stop_binding_impl(self, binding_id: str, *, reason: str) -> None:
        child_task_id: int
        job_id: str
        async with self.db_factory() as db:
            observed = await db.get(TestHarnessChildBinding, binding_id)
            if observed is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            owner_fence = await db.execute(
                update(Task)
                .where(
                    Task.id == observed.owner_task_id,
                    Task.incarnation_id
                    == observed.owner_task_incarnation_id,
                )
                .values(incarnation_id=observed.owner_task_incarnation_id)
            )
            if owner_fence.rowcount != 1:
                raise TestHarnessChildError(
                    "Browser child owner disappeared before cleanup"
                )
            if observed.harness_run_id:
                await db.execute(
                    select(TestHarnessRun)
                    .where(TestHarnessRun.id == observed.harness_run_id)
                    .with_for_update()
                )
            if observed.workspace_review_run_id:
                await db.execute(
                    select(WorkspaceReviewRun)
                    .where(
                        WorkspaceReviewRun.id
                        == observed.workspace_review_run_id
                    )
                    .with_for_update()
                )
            child_fence = await db.execute(
                update(Task)
                .where(
                    Task.id == observed.child_task_id,
                    Task.incarnation_id
                    == observed.child_task_incarnation_id,
                )
                .values(incarnation_id=observed.child_task_incarnation_id)
            )
            if child_fence.rowcount != 1:
                raise TestHarnessChildError(
                    "Browser child generation disappeared before cleanup"
                )
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if binding.state in CHILD_TERMINAL_STATES:
                return
            binding.state = CHILD_STOPPING
            binding.stop_requested_at = binding.stop_requested_at or datetime.utcnow()
            binding.error = reason[:4000]
            child_task_id = binding.child_task_id
            job_id = binding.browser_review_job_id
            await db.commit()

        try:
            from backend.services.browser_review_jobs import browser_review_job_manager

            await browser_review_job_manager.mark_cancelling(job_id)
            await self._stop_task(child_task_id)
            await browser_review_job_manager.cancel(job_id)
            await self._verify_child_terminal(child_task_id)
        except BaseException as exc:
            async with self.db_factory() as db:
                binding = await db.get(TestHarnessChildBinding, binding_id)
                if binding is not None and binding.state not in CHILD_TERMINAL_STATES:
                    binding.state = CHILD_STOP_FAILED
                    binding.error = _safe_error(exc)
                    await db.commit()
            raise TestHarnessChildError(
                f"Browser child {child_task_id} cleanup could not be proven: "
                f"{_safe_error(exc)}"
            ) from exc

        async with self.db_factory() as db:
            binding = await db.get(TestHarnessChildBinding, binding_id)
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared after stop")
            child = await db.get(Task, child_task_id)
            binding.state = (
                CHILD_COMPLETED
                if child is not None and child.status != "cancelled"
                else CHILD_STOPPED
            )
            binding.completed_at = datetime.utcnow()
            binding.error = None
            await db.commit()

    async def recover_interrupted(self) -> int:
        """Reap all nonterminal/legacy children before Dispatcher starts."""

        recovered = 0
        failures: list[str] = []
        await self._adopt_legacy_children(failures)
        async with self.db_factory() as db:
            bindings = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding).where(
                            TestHarnessChildBinding.state.not_in(
                                CHILD_TERMINAL_STATES
                            )
                        )
                    )
                ).scalars()
            )
            binding_ids = [binding.id for binding in bindings]
        for binding_id in binding_ids:
            try:
                await self.stop_binding(
                    binding_id,
                    reason="Manager restarted before Browser Agent cleanup completed",
                )
                recovered += 1
            except Exception as exc:
                logger.exception("Could not recover Browser child %s", binding_id)
                failures.append(f"{binding_id}: {_safe_error(exc)}")
        if failures:
            raise TestHarnessChildRecoveryError(
                "Interrupted Browser Agent cleanup failed: " + "; ".join(failures)
            )
        return recovered

    async def _adopt_legacy_children(self, failures: list[str]) -> None:
        """Fence active pre-migration isolated Tasks so they cannot launch."""

        async with self.db_factory() as db:
            tasks = list(
                (
                    await db.execute(
                        select(Task).where(Task.status.in_(TASK_ACTIVE_STATUSES))
                    )
                ).scalars()
            )
            for task in tasks:
                metadata = dict(task.metadata_ or {})
                if metadata.get("isolated_browser_agent") is not True:
                    continue
                existing = await db.scalar(
                    select(TestHarnessChildBinding.id).where(
                        TestHarnessChildBinding.child_task_id == task.id
                    )
                )
                if existing is not None:
                    continue
                harness_run_id = _metadata_id(metadata.get("test_harness_run_id"))
                workspace_run_id = _metadata_id(
                    metadata.get("workspace_review_run_id")
                )
                job_id = _metadata_id(metadata.get("browser_review_job_id"))
                owner_task_id = metadata.get("test_harness_parent_task_id") or metadata.get(
                    "workspace_review_parent_task_id"
                )
                if (
                    not (harness_run_id or workspace_run_id)
                    or not job_id
                    or type(owner_task_id) is not int
                ):
                    failures.append(
                        f"Task {task.id}: legacy Browser child identity is incomplete"
                    )
                    continue
                owner = await db.get(Task, owner_task_id)
                db.add(
                    TestHarnessChildBinding(
                        id=uuid.uuid4().hex,
                        harness_run_id=harness_run_id,
                        workspace_review_run_id=workspace_run_id,
                        owner_task_id=owner_task_id,
                        owner_task_incarnation_id=(
                            owner.incarnation_id if owner is not None else None
                        ),
                        child_task_id=task.id,
                        child_task_incarnation_id=task.incarnation_id,
                        browser_review_job_id=job_id,
                        provider=task.provider,
                        model=task.model,
                        reasoning_effort=task.effort_level,
                        codex_service_tier=task.codex_service_tier,
                        skill_name=BROWSER_REVIEW_SKILL,
                        state=CHILD_STOP_FAILED,
                        error="Adopted during startup recovery",
                    )
                )
            await db.commit()

    async def _binding_id(
        self,
        *,
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> str | None:
        async with self.db_factory() as db:
            predicates = []
            if harness_run_id:
                predicates.append(
                    TestHarnessChildBinding.harness_run_id == harness_run_id
                )
            if workspace_review_run_id:
                predicates.append(
                    TestHarnessChildBinding.workspace_review_run_id
                    == workspace_review_run_id
                )
            if not predicates:
                return None
            return await db.scalar(
                select(TestHarnessChildBinding.id).where(*predicates)
            )

    async def _stop_task(self, task_id: int) -> None:
        if self._task_stopper is not None:
            await self._task_stopper(task_id)
            return
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if task is None or task.status in TASK_TERMINAL_STATUSES:
                return
            if task.status == "pending_activation":
                owner = await db.scalar(
                    select(Instance.id).where(Instance.current_task_id == task_id)
                )
                if owner is not None:
                    raise TestHarnessChildError(
                        "A gated Browser child unexpectedly owns an Instance"
                    )
                task.status = "cancelled"
                task.completed_at = datetime.utcnow()
                task.error_message = "Browser Agent stopped before activation"
                await db.commit()
                return
        from backend.api.tasks import _cancel_local_task_under_cancellation_lease
        from backend.main import dispatcher

        async with self.db_factory() as db:
            async with dispatcher.task_queue_cancellation_lease(task_id):
                await _cancel_local_task_under_cancellation_lease(task_id, db)

    async def _verify_child_terminal(self, task_id: int) -> None:
        async with self.db_factory() as db:
            child = await db.get(Task, task_id)
            if child is not None and child.status not in TASK_TERMINAL_STATUSES:
                raise TestHarnessChildError(
                    f"Browser child remained {child.status} after cancellation"
                )
            owner = await db.scalar(
                select(Instance.id).where(Instance.current_task_id == task_id)
            )
            if owner is not None:
                raise TestHarnessChildError(
                    f"Browser child still owns Instance {owner} after cancellation"
                )

    async def _stop_lock(self, binding_id: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._stop_locks.setdefault(binding_id, asyncio.Lock())


async def _finish_despite_cancellation(awaitable: Awaitable[None]) -> None:
    operation = asyncio.create_task(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not operation.done():
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError as exc:
            cancellation = exc
    operation.result()
    if cancellation is not None:
        raise cancellation


def _metadata_id(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _safe_error(exc: BaseException) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text[:4000]


test_harness_child_service = TestHarnessChildService()
