"""Transactional core for provider-neutral task capabilities.

The service owns durable state and idempotency only.  It deliberately does not
start an executor while a database transaction is open.  Delivery controllers
claim queued executions through this in-process API and invoke the registered
adapter after the claim has committed.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import secrets
from typing import Any, Literal

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.capability import (
    ACTIVE_EXECUTION_STATUSES,
    ACTIVE_INVOCATION_STATUSES,
    TERMINAL_INVOCATION_STATUSES,
    CapabilityExecution,
    CapabilityInvocation,
)
from backend.models.task import Task
from backend.services.capability_events import broadcast_capability_event
from backend.services.capability_registry import CAPABILITY_KEY_RE, resolve_capability


class CapabilityError(RuntimeError):
    """Base class for errors that API/controller callers may map explicitly."""


class CapabilityDisabledError(CapabilityError):
    pass


class CapabilityNotFoundError(CapabilityError):
    pass


class CapabilityConflictError(CapabilityError):
    pass


class CapabilityValidationError(CapabilityError):
    pass


class CapabilityUnsupportedScopeError(CapabilityError):
    pass


class CapabilityUnavailableError(CapabilityError):
    pass


_task_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
MAX_CAPABILITY_REQUEST_BYTES = 32 * 1024


def capability_task_lock(task_id: int) -> asyncio.Lock:
    """Return the process-local half of the per-Task admission fence."""

    return _task_locks[task_id]


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise CapabilityValidationError(
            "Capability payload must be finite JSON data"
        ) from exc


def capability_value_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _validate_request(request_payload: dict) -> tuple[dict, str]:
    if not isinstance(request_payload, dict):
        raise CapabilityValidationError("Capability request must be a JSON object")
    frozen = deepcopy(request_payload)
    canonical = _canonical_json(frozen).encode("utf-8")
    if len(canonical) > MAX_CAPABILITY_REQUEST_BYTES:
        raise CapabilityValidationError(
            f"Capability request exceeds {MAX_CAPABILITY_REQUEST_BYTES} UTF-8 bytes"
        )
    return frozen, hashlib.sha256(canonical).hexdigest()


def _validate_hash(value: str, *, field: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise CapabilityValidationError(f"{field} must be a SHA-256 hex digest")
    return normalized


def _task_subject(task: Task) -> tuple[dict, str]:
    subject = {
        "task_id": task.id,
        "retry_count": task.retry_count,
        "instance_id": task.instance_id,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "session_id": task.session_id,
    }
    return subject, capability_value_hash(subject)


def _ensure_local_task(task: Task) -> None:
    if task.worker_id is not None:
        raise CapabilityUnsupportedScopeError(
            "Capabilities cannot be created on a remote Worker task"
        )
    if task.shared_from_id is not None:
        raise CapabilityUnsupportedScopeError(
            "Capabilities cannot be created on a shared shadow task"
        )
    if task.status == "migrating":
        raise CapabilityUnsupportedScopeError(
            "Capabilities cannot be created while a task is migrating"
        )


def _same_logical_request(
    invocation: CapabilityInvocation,
    *,
    capability_key: str,
    source: str,
    purpose: str,
    resume_policy: str,
    input_hash: str,
) -> bool:
    return (
        invocation.capability_key == capability_key
        and invocation.source == source
        and invocation.purpose == purpose
        and invocation.resume_policy == resume_policy
        and invocation.input_hash == input_hash
    )


async def _find_idempotent(
    db: AsyncSession,
    *,
    task_id: int,
    idempotency_key: str,
) -> CapabilityInvocation | None:
    return (
        await db.execute(
            select(CapabilityInvocation).where(
                CapabilityInvocation.task_id == task_id,
                CapabilityInvocation.idempotency_key == idempotency_key,
            )
        )
    ).scalar_one_or_none()


async def _lock_task(db: AsyncSession, task_id: int) -> Task:
    """Acquire the first lock in Task -> Invocation -> Execution order."""

    guarded = await db.execute(
        update(Task).where(Task.id == task_id).values(status=Task.status)
    )
    if not guarded.rowcount:
        raise CapabilityNotFoundError("Task not found")
    task = (
        await db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise CapabilityNotFoundError("Task not found")
    return task


async def _create_invocation(
    db: AsyncSession,
    *,
    task_id: int,
    capability_key: str,
    request_payload: dict,
    idempotency_key: str,
    source: Literal["human_request", "delivery_controller"],
    purpose: Literal["advisory", "required_gate"],
    resume_policy: Literal["attach_only", "controller"],
    requested_by_user_id: int | None,
    request_source_log_id: int | None = None,
) -> tuple[CapabilityInvocation, bool]:
    capability_key = capability_key.strip()
    idempotency_key = idempotency_key.strip()
    if not CAPABILITY_KEY_RE.fullmatch(capability_key):
        raise CapabilityValidationError("Invalid capability key")
    if not idempotency_key or len(idempotency_key) > 128:
        raise CapabilityValidationError("Invalid idempotency key")
    payload, input_hash = _validate_request(request_payload)

    # Replay is deliberately checked before the rollout switch. Disabling new
    # work must not turn a lost HTTP response into a second logical request.
    existing = await _find_idempotent(
        db,
        task_id=task_id,
        idempotency_key=idempotency_key,
    )
    if existing is not None:
        if not _same_logical_request(
            existing,
            capability_key=capability_key,
            source=source,
            purpose=purpose,
            resume_policy=resume_policy,
            input_hash=input_hash,
        ):
            raise CapabilityConflictError(
                "Idempotency key was already used for a different request"
            )
        return existing, False

    if not settings.capability_core_enabled:
        raise CapabilityDisabledError("Capability Core is disabled")

    definition = resolve_capability(capability_key)
    if definition is None:
        raise CapabilityUnavailableError(
            f"Capability {capability_key!r} is not registered"
        )

    async with capability_task_lock(task_id):
        try:
            task = await _lock_task(db, task_id)
            _ensure_local_task(task)

            existing = await _find_idempotent(
                db,
                task_id=task_id,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                if not _same_logical_request(
                    existing,
                    capability_key=capability_key,
                    source=source,
                    purpose=purpose,
                    resume_policy=resume_policy,
                    input_hash=input_hash,
                ):
                    raise CapabilityConflictError(
                        "Idempotency key was already used for a different request"
                    )
                await db.commit()
                return existing, False

            active_id = await db.scalar(
                select(CapabilityInvocation.id)
                .where(CapabilityInvocation.active_task_id == task_id)
                .limit(1)
            )
            if active_id is not None:
                raise CapabilityConflictError(
                    f"Task already has active capability invocation {active_id}"
                )

            subject_ref, subject_hash = _task_subject(task)
            executor_config = deepcopy(definition.executor_config)
            policy_snapshot = deepcopy(definition.policy_snapshot)
            invocation = CapabilityInvocation(
                task_id=task.id,
                capability_key=definition.capability_key,
                source=source,
                purpose=purpose,
                status="queued",
                state_version=1,
                idempotency_key=idempotency_key,
                input_payload=payload,
                input_hash=input_hash,
                subject_kind="task_generation",
                subject_ref=subject_ref,
                subject_hash=subject_hash,
                executor_kind=definition.executor_kind,
                executor_config=executor_config,
                executor_config_hash=capability_value_hash(executor_config),
                policy_snapshot=policy_snapshot,
                policy_hash=capability_value_hash(policy_snapshot),
                resume_policy=resume_policy,
                max_attempts=definition.max_attempts,
                active_task_id=task.id,
                requested_by_user_id=requested_by_user_id,
                request_task_retry_count=task.retry_count,
                request_task_instance_id=task.instance_id,
                request_task_started_at=task.started_at,
                request_task_session_id=task.session_id,
                request_source_log_id=request_source_log_id,
            )
            db.add(invocation)
            await db.flush()
            db.add(
                CapabilityExecution(
                    invocation_id=invocation.id,
                    attempt=1,
                    status="queued",
                    state_version=1,
                    active_invocation_id=invocation.id,
                    idempotency_key=f"{invocation.id}:1",
                    executor_kind=invocation.executor_kind,
                    input_hash=invocation.input_hash,
                )
            )
            await db.commit()
        except CapabilityError:
            await db.rollback()
            raise
        except IntegrityError as exc:
            await db.rollback()
            concurrent = await _find_idempotent(
                db,
                task_id=task_id,
                idempotency_key=idempotency_key,
            )
            if concurrent is not None and _same_logical_request(
                concurrent,
                capability_key=capability_key,
                source=source,
                purpose=purpose,
                resume_policy=resume_policy,
                input_hash=input_hash,
            ):
                return concurrent, False
            raise CapabilityConflictError(
                "A concurrent capability request won admission"
            ) from exc

    await broadcast_capability_event(
        "capability_invocation_created",
        invocation,
        created=True,
    )
    return invocation, True


async def create_human_invocation(
    db: AsyncSession,
    *,
    task_id: int,
    capability_key: str,
    request_payload: dict,
    idempotency_key: str,
    requested_by_user_id: int | None,
) -> tuple[CapabilityInvocation, bool]:
    """Create the only public contract: advisory + attach-only."""

    return await _create_invocation(
        db,
        task_id=task_id,
        capability_key=capability_key,
        request_payload=request_payload,
        idempotency_key=idempotency_key,
        source="human_request",
        purpose="advisory",
        resume_policy="attach_only",
        requested_by_user_id=requested_by_user_id,
    )


async def create_controller_invocation(
    db: AsyncSession,
    *,
    task_id: int,
    capability_key: str,
    request_payload: dict,
    idempotency_key: str,
    purpose: Literal["advisory", "required_gate"] = "required_gate",
    request_source_log_id: int | None = None,
) -> tuple[CapabilityInvocation, bool]:
    """In-process entry point reserved for a delivery-loop controller."""

    return await _create_invocation(
        db,
        task_id=task_id,
        capability_key=capability_key,
        request_payload=request_payload,
        idempotency_key=idempotency_key,
        source="delivery_controller",
        purpose=purpose,
        resume_policy="controller",
        requested_by_user_id=None,
        request_source_log_id=request_source_log_id,
    )


async def create_agent_invocation(*args, **kwargs):
    """Reject Agent/MCP calls until CCM has an exact native-turn fence."""

    raise CapabilityUnsupportedScopeError(
        "agent_request requires an exact task-turn generation and is not available"
    )


async def get_invocation(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityInvocation:
    invocation = await db.get(CapabilityInvocation, invocation_id)
    if invocation is None:
        raise CapabilityNotFoundError("Capability invocation not found")
    return invocation


async def list_task_invocations(
    db: AsyncSession,
    task_id: int,
) -> list[CapabilityInvocation]:
    return list(
        (
            await db.execute(
                select(CapabilityInvocation)
                .where(CapabilityInvocation.task_id == task_id)
                .order_by(
                    CapabilityInvocation.created_at.desc(),
                    CapabilityInvocation.id.desc(),
                )
            )
        ).scalars()
    )


async def active_execution_for(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityExecution | None:
    return (
        await db.execute(
            select(CapabilityExecution).where(
                CapabilityExecution.invocation_id == invocation_id,
                CapabilityExecution.active_invocation_id == invocation_id,
            )
        )
    ).scalar_one_or_none()


async def _invocation_task_id(
    db: AsyncSession,
    invocation_id: int,
) -> int:
    task_id = await db.scalar(
        select(CapabilityInvocation.task_id).where(
            CapabilityInvocation.id == invocation_id
        )
    )
    if task_id is None:
        raise CapabilityNotFoundError("Capability invocation not found")
    return task_id


async def _lock_aggregate(
    db: AsyncSession,
    invocation_id: int,
) -> tuple[Task, CapabilityInvocation, list[CapabilityExecution]]:
    task_id = await _invocation_task_id(db, invocation_id)
    task = await _lock_task(db, task_id)
    invocation = (
        await db.execute(
            select(CapabilityInvocation)
            .where(CapabilityInvocation.id == invocation_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if invocation is None or invocation.task_id != task.id:
        raise CapabilityNotFoundError("Capability invocation not found")
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == invocation.id)
                .order_by(CapabilityExecution.attempt)
                .with_for_update()
            )
        ).scalars()
    )
    return task, invocation, executions


def _expect_version(actual: int, expected: int, *, resource: str) -> None:
    if actual != expected:
        raise CapabilityConflictError(
            f"Stale {resource} state version: expected {expected}, current {actual}"
        )


def _active_execution(
    invocation: CapabilityInvocation,
    executions: list[CapabilityExecution],
) -> CapabilityExecution:
    active = [
        execution
        for execution in executions
        if execution.active_invocation_id == invocation.id
    ]
    if len(active) != 1:
        raise CapabilityConflictError(
            "Capability invocation does not have exactly one active execution"
        )
    return active[0]


async def _commit_transition(
    db: AsyncSession,
    invocation: CapabilityInvocation,
    *,
    event_type: str,
) -> None:
    invocation.updated_at = datetime.utcnow()
    await db.commit()
    await broadcast_capability_event(event_type, invocation)


async def claim_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    handle_kind: str,
    handle_id: str,
    handle_generation: int | None = None,
    lease_token: str | None = None,
    lease_expires_at: datetime | None = None,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    if not handle_kind.strip() or not handle_id.strip():
        raise CapabilityValidationError("Executor handle kind and id are required")
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if invocation.status != "queued" or execution.status != "queued":
                raise CapabilityConflictError("Capability execution is not claimable")
            now = datetime.utcnow()
            invocation.status = "running"
            invocation.state_version += 1
            execution.status = "running"
            execution.state_version += 1
            execution.handle_kind = handle_kind.strip()
            execution.handle_id = handle_id.strip()
            execution.handle_generation = handle_generation
            execution.lease_token = lease_token or secrets.token_hex(32)
            execution.lease_expires_at = lease_expires_at
            execution.heartbeat_at = now
            execution.started_at = now
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_running",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def mark_execution_waiting(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if invocation.status != "running" or execution.status != "running":
                raise CapabilityConflictError("Capability execution is not running")
            invocation.status = "waiting_user"
            invocation.state_version += 1
            execution.status = "waiting_user"
            execution.state_version += 1
            execution.heartbeat_at = datetime.utcnow()
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_waiting_user",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def resume_waiting_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    """CAS a user-answered execution back to running."""

    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if (
                invocation.status != "waiting_user"
                or execution.status != "waiting_user"
            ):
                raise CapabilityConflictError(
                    "Capability execution is not waiting for user input"
                )
            invocation.status = "running"
            invocation.state_version += 1
            execution.status = "running"
            execution.state_version += 1
            execution.heartbeat_at = datetime.utcnow()
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_running",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


# Descriptive alias for adapters that report observed state rather than an
# input-answer action.
mark_execution_running = resume_waiting_execution


async def complete_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    output_kind: str,
    output_id: int,
    output_hash: str,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    if not output_kind.strip():
        raise CapabilityValidationError("Output kind is required")
    if isinstance(output_id, bool) or not isinstance(output_id, int) or output_id <= 0:
        raise CapabilityValidationError("output_id must be a positive integer")
    output_hash = _validate_hash(output_hash, field="output_hash")
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if invocation.status not in {"running", "waiting_user"} or execution.status not in {"running", "waiting_user"}:
                raise CapabilityConflictError("Capability execution cannot complete from its current state")
            now = datetime.utcnow()
            execution.status = "completed"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.output_kind = output_kind.strip()
            execution.output_id = output_id
            execution.output_hash = output_hash
            execution.completed_at = now
            invocation.status = "ready"
            invocation.state_version += 1
            invocation.result_kind = execution.output_kind
            invocation.result_id = execution.output_id
            invocation.result_hash = execution.output_hash
            invocation.ready_at = now
            # ready intentionally keeps active_task_id. Only a successful
            # consumer acknowledgement releases admission for the next call.
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_ready",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def fail_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    error_code: str,
    error_message: str,
    retry: bool = True,
) -> tuple[CapabilityInvocation, CapabilityExecution, CapabilityExecution | None]:
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if execution.status not in ACTIVE_EXECUTION_STATUSES:
                raise CapabilityConflictError("Capability execution is already terminal")
            now = datetime.utcnow()
            execution.status = "failed"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.error_code = error_code[:64] or "executor_failed"
            execution.error_message = error_message
            execution.completed_at = now
            await db.flush()

            replacement = None
            if retry and execution.attempt < invocation.max_attempts:
                next_attempt = execution.attempt + 1
                replacement = CapabilityExecution(
                    invocation_id=invocation.id,
                    attempt=next_attempt,
                    status="queued",
                    state_version=1,
                    active_invocation_id=invocation.id,
                    idempotency_key=f"{invocation.id}:{next_attempt}",
                    executor_kind=invocation.executor_kind,
                    input_hash=invocation.input_hash,
                )
                db.add(replacement)
                invocation.status = "queued"
                invocation.error_code = None
                invocation.error_message = None
            else:
                invocation.status = "failed"
                invocation.active_task_id = None
                invocation.error_code = execution.error_code
                invocation.error_message = error_message
                invocation.completed_at = now
            invocation.state_version += 1
            await _commit_transition(
                db,
                invocation,
                event_type=(
                    "capability_invocation_retrying"
                    if replacement is not None
                    else "capability_invocation_failed"
                ),
            )
            return invocation, execution, replacement
        except CapabilityError:
            await db.rollback()
            raise
        except IntegrityError as exc:
            await db.rollback()
            raise CapabilityConflictError(
                "Concurrent capability execution retry won"
            ) from exc


async def consume_ready_invocation(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_state_version: int,
) -> CapabilityInvocation:
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, _ = await _lock_aggregate(db, invocation_id)
            _expect_version(invocation.state_version, expected_state_version, resource="invocation")
            if invocation.status != "ready":
                raise CapabilityConflictError("Capability result is not ready")
            invocation.status = "completed"
            invocation.state_version += 1
            invocation.active_task_id = None
            invocation.completed_at = datetime.utcnow()
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_completed",
            )
            return invocation
        except CapabilityError:
            await db.rollback()
            raise


async def cancel_invocation(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_state_version: int,
) -> CapabilityInvocation:
    """Request cancellation; queued/ready work terminates synchronously."""

    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            _expect_version(invocation.state_version, expected_state_version, resource="invocation")
            if invocation.status in TERMINAL_INVOCATION_STATUSES:
                await db.commit()
                return invocation

            now = datetime.utcnow()
            active = [
                execution
                for execution in executions
                if execution.active_invocation_id == invocation.id
            ]
            execution = active[0] if len(active) == 1 else None
            if invocation.status in {"queued", "ready", "resuming"}:
                if execution is not None:
                    execution.status = "cancelled"
                    execution.state_version += 1
                    execution.active_invocation_id = None
                    execution.completed_at = now
                invocation.status = "cancelled"
                invocation.active_task_id = None
                invocation.completed_at = now
                event_type = "capability_invocation_cancelled"
            elif execution is not None and execution.status in {
                "running",
                "waiting_user",
            }:
                execution.status = "cancelling"
                execution.state_version += 1
                invocation.status = "cancelling"
                event_type = "capability_invocation_cancelling"
            else:
                raise CapabilityConflictError(
                    "Capability invocation cannot be cancelled safely"
                )
            invocation.state_version += 1
            await _commit_transition(db, invocation, event_type=event_type)
            return invocation
        except CapabilityError:
            await db.rollback()
            raise


async def mark_execution_cancelled(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    """Finalize cancellation after the adapter proves its handle is stopped."""

    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if invocation.status != "cancelling" or execution.status != "cancelling":
                raise CapabilityConflictError("Capability cancellation is not pending")
            now = datetime.utcnow()
            execution.status = "cancelled"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.completed_at = now
            invocation.status = "cancelled"
            invocation.state_version += 1
            invocation.active_task_id = None
            invocation.completed_at = now
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_cancelled",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise
