"""Transactional aggregate operations for first-class versioned Plans."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanInputRequest,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan_resource import (
    PlanInputAnswer,
    PlanInputRequestResponse,
    PlanQuestion,
    PlanResource,
    PlanRunResource,
    PlanStepResource,
    PlanVersionResource,
)


ACTIVE_RUN_STATUSES = frozenset({"queued", "running", "waiting_user"})
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
_plan_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def plan_operation_lock(plan_id: int) -> asyncio.Lock:
    return _plan_locks[plan_id]


async def _fence_target_task(
    db: AsyncSession,
    *,
    target_task_id: int | None,
    expected_worker_id: int | None,
) -> None:
    """Serialize a new active Run against an exact Task migration claim."""

    if target_task_id is None:
        return
    worker_predicate = (
        Task.worker_id.is_(None)
        if expected_worker_id is None
        else Task.worker_id == expected_worker_id
    )
    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == target_task_id,
            Task.status != "migrating",
            worker_predicate,
        )
        # A matched-row UPDATE takes the same database write lock used by the
        # migration claim without changing user-visible Task state.
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan target is changing execution location")


def _public_attachments(items: list[dict] | None) -> list[dict] | None:
    if not items:
        return None
    return [
        {
            key: item[key]
            for key in ("url", "name", "is_image")
            if key in item
        }
        for item in items
        if isinstance(item, dict)
    ] or None


def input_request_resource(
    input_request: PlanInputRequest,
) -> PlanInputRequestResponse:
    return PlanInputRequestResponse.model_validate(input_request).model_copy(
        update={"attachments": _public_attachments(input_request.attachments)}
    )


async def create_plan_with_run(
    db: AsyncSession,
    *,
    title: str,
    initial_request: str,
    attachments: list[dict] | None,
    target_task_id: int | None,
    project_id: int | None,
    target_repo: str | None,
    target_branch: str | None,
    worker_id: int | None,
    priority: int,
    timeout_hours: float | None,
    created_by: int | None,
    pipeline_config: dict,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
    forked_from_version_id: int | None = None,
    base_version_id: int | None = None,
    run_type: str = "initial",
) -> tuple[Plan, PlanAgentRun]:
    now = datetime.utcnow()
    await _fence_target_task(
        db,
        target_task_id=target_task_id,
        expected_worker_id=worker_id,
    )
    plan = Plan(
        title=title[:200],
        initial_request=initial_request,
        initial_attachments=attachments or None,
        target_task_id=target_task_id,
        project_id=project_id,
        target_repo=target_repo,
        target_branch=target_branch,
        worker_id=worker_id,
        priority=priority,
        timeout_hours=timeout_hours,
        created_by=created_by,
        pipeline_config=pipeline_config,
        forked_from_version_id=forked_from_version_id,
        created_at=now,
        updated_at=now,
    )
    db.add(plan)
    await db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        plan_task_id=None,
        run_type=run_type,
        status="queued",
        current_stage="planner",
        base_version_id=base_version_id,
        request_text=initial_request,
        attachments=attachments or None,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        worker_id=worker_id,
        pipeline_config=pipeline_config,
        round=1,
        generation=0,
        max_interactions=max(0, min(5, settings.plan_max_interactions)),
        updated_at=now,
    )
    db.add(run)
    await db.flush()
    plan.active_run_id = run.id
    await db.commit()
    await db.refresh(plan)
    await db.refresh(run)
    return plan, run


async def create_plan_run(
    db: AsyncSession,
    *,
    plan: Plan,
    run_type: str,
    request_text: str,
    attachments: list[dict] | None,
    base_version_id: int | None,
    expected_current_version_id: int | None,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
) -> PlanAgentRun:
    """Create one Run under the Plan's durable active-run fence."""

    if plan.archived_at is not None:
        raise HTTPException(409, "Archived Plan cannot start a Run")
    if plan.active_run_id is not None:
        raise HTTPException(409, f"Plan already has active Run #{plan.active_run_id}")
    if expected_current_version_id != plan.current_version_id:
        raise HTTPException(409, "Plan current Version changed")
    if base_version_id is not None:
        base = await db.get(PlanVersion, base_version_id)
        if base is None or base.plan_id != plan.id:
            raise HTTPException(400, "Base Version does not belong to this Plan")

    await _fence_target_task(
        db,
        target_task_id=plan.target_task_id,
        expected_worker_id=plan.worker_id,
    )

    now = datetime.utcnow()
    run = PlanAgentRun(
        plan_id=plan.id,
        plan_task_id=None,
        run_type=run_type,
        status="queued",
        current_stage="planner",
        base_version_id=base_version_id,
        request_text=request_text,
        attachments=attachments or None,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        worker_id=plan.worker_id,
        pipeline_config=plan.pipeline_config,
        round=1,
        generation=0,
        max_interactions=max(0, min(5, settings.plan_max_interactions)),
        updated_at=now,
    )
    db.add(run)
    await db.flush()
    claimed = await db.execute(
        update(Plan)
        .where(
            Plan.id == plan.id,
            Plan.active_run_id.is_(None),
            (
                Plan.current_version_id.is_(None)
                if expected_current_version_id is None
                else Plan.current_version_id == expected_current_version_id
            ),
            Plan.lock_version == plan.lock_version,
        )
        .values(
            active_run_id=run.id,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if claimed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan changed while creating the Run")
    await db.commit()
    await db.refresh(run)
    return run


async def create_version_for_step(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    step: PlanAgentStep,
    content: str,
    repo_revision: dict | None,
) -> PlanVersion:
    """Persist a Planner result exactly once and advance current Version."""

    existing = (
        await db.execute(
            select(PlanVersion).where(PlanVersion.produced_by_step_id == step.id)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    next_number = int(
        await db.scalar(
            select(func.coalesce(func.max(PlanVersion.version_number), 0)).where(
                PlanVersion.plan_id == plan.id
            )
        )
        or 0
    ) + 1
    previous_id = plan.current_version_id
    version = PlanVersion(
        plan_id=plan.id,
        version_number=next_number,
        parent_version_id=previous_id,
        produced_by_run_id=run.id,
        produced_by_step_id=step.id,
        content=content,
        context_session_id=run.context_session_id,
        context_log_id=run.context_log_id,
        context_snapshot=run.context_snapshot,
        repo_revision=repo_revision,
        human_decision="pending",
    )
    db.add(version)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        found = (
            await db.execute(
                select(PlanVersion).where(PlanVersion.produced_by_step_id == step.id)
            )
        ).scalar_one_or_none()
        if found is None:
            raise
        return found
    if previous_id is not None:
        await db.execute(
            update(PlanVersion)
            .where(
                PlanVersion.id == previous_id,
                PlanVersion.plan_id == plan.id,
                PlanVersion.superseded_by_version_id.is_(None),
            )
            .values(superseded_by_version_id=version.id)
        )
    changed = await db.execute(
        update(Plan)
        .where(Plan.id == plan.id, Plan.active_run_id == run.id)
        .values(
            current_version_id=version.id,
            lock_version=Plan.lock_version + 1,
            updated_at=datetime.utcnow(),
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise RuntimeError("Plan Run lost ownership before Version commit")
    step.plan_version_id = version.id
    run.result_version_id = version.id
    await db.commit()
    await db.refresh(version)
    return version


def _answer_map(answers: Iterable[PlanInputAnswer | dict]) -> dict[str, object]:
    result: dict[str, object] = {}
    for answer in answers:
        item = answer.model_dump() if isinstance(answer, PlanInputAnswer) else answer
        question_id = item.get("question_id")
        if not isinstance(question_id, str) or question_id in result:
            raise HTTPException(422, "Answers must use unique valid question_id values")
        result[question_id] = item.get("value")
    return result


def validate_input_answers(
    questions: list[dict], answers: Iterable[PlanInputAnswer | dict]
) -> list[dict]:
    """Validate all questions without imposing a question-count limit."""

    parsed = [PlanQuestion.model_validate(question) for question in questions]
    by_id = {question.id: question for question in parsed}
    values = _answer_map(answers)
    unknown = set(values) - set(by_id)
    if unknown:
        raise HTTPException(422, f"Unknown question ids: {sorted(unknown)}")
    normalized: list[dict] = []
    for question in parsed:
        value = values.get(question.id)
        if question.required and (
            value is None or value == "" or value == []
        ):
            raise HTTPException(422, f"Question {question.id!r} requires an answer")
        if value is None:
            normalized.append({"question_id": question.id, "value": None})
            continue
        if question.response_type == "text":
            if not isinstance(value, str) or len(value) > 50_000:
                raise HTTPException(422, f"Question {question.id!r} requires text")
        elif question.response_type == "single_choice":
            allowed = {option.value for option in question.options}
            if not isinstance(value, str) or value not in allowed:
                raise HTTPException(422, f"Question {question.id!r} has an invalid choice")
        else:
            allowed = {option.value for option in question.options}
            if (
                not isinstance(value, list)
                or any(not isinstance(item, str) or item not in allowed for item in value)
                or len(value) != len(set(value))
            ):
                raise HTTPException(422, f"Question {question.id!r} has invalid choices")
        normalized.append({"question_id": question.id, "value": value})
    return normalized


async def answer_input_request(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    input_request: PlanInputRequest,
    expected_generation: int,
    idempotency_key: str,
    answers: Iterable[PlanInputAnswer | dict],
    response_text: str | None,
    attachments: list[dict] | None,
    answered_by: int | None,
) -> PlanInputRequest:
    if input_request.answer_idempotency_key == idempotency_key and input_request.status == "answered":
        return input_request
    if plan.active_run_id != run.id or run.status != "waiting_user":
        raise HTTPException(409, "Plan Run is no longer waiting for input")
    if run.generation != expected_generation:
        raise HTTPException(409, "Plan Run generation changed")
    if run.open_input_request_id != input_request.id or input_request.status != "open":
        raise HTTPException(409, "Input request is no longer open")
    normalized = validate_input_answers(input_request.questions, answers)
    now = datetime.utcnow()
    updated = await db.execute(
        update(PlanInputRequest)
        .where(
            PlanInputRequest.id == input_request.id,
            PlanInputRequest.run_id == run.id,
            PlanInputRequest.status == "open",
        )
        .values(
            status="answered",
            answers=normalized,
            response_text=response_text,
            attachments=attachments or None,
            answered_by=answered_by,
            answered_at=now,
            answer_idempotency_key=idempotency_key,
        )
    )
    resumed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run.id,
            PlanAgentRun.plan_id == plan.id,
            PlanAgentRun.status == "waiting_user",
            PlanAgentRun.generation == expected_generation,
            PlanAgentRun.open_input_request_id == input_request.id,
        )
        .values(
            status="queued",
            current_stage="planner",
            open_input_request_id=None,
            generation=PlanAgentRun.generation + 1,
            updated_at=now,
        )
    )
    if updated.rowcount != 1 or resumed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Input request was answered concurrently")
    await db.commit()
    await db.refresh(input_request)
    return input_request


async def decide_version(
    db: AsyncSession,
    *,
    plan: Plan,
    version: PlanVersion,
    decision: str,
    decided_by: int | None,
    expected_current_version_id: int,
) -> PlanVersion:
    if plan.current_version_id != expected_current_version_id or version.id != expected_current_version_id:
        raise HTTPException(409, "Plan current Version changed")
    if plan.active_run_id is not None:
        raise HTTPException(409, "Plan has an active Run")
    if version.review_verdict not in {"approve", "disabled", "exhausted"} and not version.review_exhausted:
        raise HTTPException(409, "Version is not ready for a human decision")
    if version.human_decision != "pending":
        if version.human_decision == decision:
            return version
        raise HTTPException(409, f"Version was already {version.human_decision}")
    changed = await db.execute(
        update(PlanVersion)
        .where(
            PlanVersion.id == version.id,
            PlanVersion.plan_id == plan.id,
            PlanVersion.human_decision == "pending",
            PlanVersion.superseded_by_version_id.is_(None),
        )
        .values(
            human_decision=decision,
            decided_at=datetime.utcnow(),
            decided_by=decided_by,
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Version decision changed concurrently")
    await db.commit()
    await db.refresh(version)
    return version


async def cancel_run(
    db: AsyncSession, *, plan: Plan, run: PlanAgentRun
) -> PlanAgentRun:
    if plan.active_run_id != run.id or run.status not in ACTIVE_RUN_STATUSES:
        if run.status == "cancelled":
            return run
        raise HTTPException(409, "Plan Run is no longer active")
    now = datetime.utcnow()
    execution_seconds = float(run.execution_seconds or 0)
    if run.last_execution_started_at is not None:
        execution_seconds += max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
    if run.open_input_request_id is not None:
        await db.execute(
            update(PlanInputRequest)
            .where(
                PlanInputRequest.id == run.open_input_request_id,
                PlanInputRequest.status.in_(["prepared", "open"]),
            )
            .values(status="cancelled", cancelled_at=now)
        )
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run.id,
            PlanAgentRun.plan_id == plan.id,
            PlanAgentRun.status.in_(ACTIVE_RUN_STATUSES),
        )
        .values(
            status="cancelled",
            open_input_request_id=None,
            instance_id=None,
            execution_seconds=execution_seconds,
            last_execution_started_at=None,
            generation=PlanAgentRun.generation + 1,
            error="Cancelled by user",
            updated_at=now,
            finished_at=now,
        )
    )
    released = await db.execute(
        update(Plan)
        .where(Plan.id == plan.id, Plan.active_run_id == run.id)
        .values(
            active_run_id=None,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if changed.rowcount != 1 or released.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan Run changed while cancelling")
    await db.commit()
    await db.refresh(run)
    return run


async def resolve_legacy_task(db: AsyncSession, task_id: int) -> PlanLegacyTaskLink | None:
    return await db.get(PlanLegacyTaskLink, task_id)


async def approved_versions_for_message(
    db: AsyncSession,
    *,
    target,
    version_ids: list[int] | None,
    confirmed_stale_version_ids: list[int] | None = None,
) -> list[tuple[Plan, PlanVersion]]:
    """Resolve exact approved Versions in caller order for one chat turn."""

    raw_ids = version_ids or []
    ids: list[int] = []
    for value in raw_ids:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("plan_version_ids must contain positive integers")
        if value in ids:
            raise ValueError("plan_version_ids must not contain duplicates")
        ids.append(value)
    if not ids:
        return []
    versions = {
        row.id: row
        for row in (
            await db.execute(select(PlanVersion).where(PlanVersion.id.in_(ids)))
        ).scalars()
    }
    plan_ids = {row.plan_id for row in versions.values()}
    plans = {
        row.id: row
        for row in (
            await db.execute(select(Plan).where(Plan.id.in_(plan_ids)))
        ).scalars()
    }
    confirmed = set(confirmed_stale_version_ids or [])
    from backend.services.plan_tasks import capture_repo_revision, latest_task_log_id

    current_log_id = await latest_task_log_id(db, target.id)
    result: list[tuple[Plan, PlanVersion]] = []
    for version_id in ids:
        version = versions.get(version_id)
        plan = plans.get(version.plan_id) if version is not None else None
        if version is None or plan is None:
            raise ValueError(f"Plan Version #{version_id} was not found")
        if plan.target_task_id != target.id:
            raise ValueError(
                f"Plan Version #{version_id} is not associated with Task #{target.id}"
            )
        if version.human_decision != "approved" or not version.content:
            raise ValueError(f"Plan Version #{version_id} is not approved and ready")
        applied = await db.scalar(
            select(PlanApplication.id).where(
                PlanApplication.plan_version_id == version.id
            ).limit(1)
        )
        if applied is not None:
            raise ValueError(f"Plan Version #{version_id} has already been applied")
        reasons: list[str] = []
        if version.context_session_id != target.session_id:
            reasons.append("session_changed")
        if (current_log_id or 0) > (version.context_log_id or 0):
            reasons.append("conversation_advanced")
        current_repo = None
        if plan.worker_id is None:
            current_repo = await capture_repo_revision(target.last_cwd or plan.target_repo)
            if version.repo_revision is not None and current_repo != version.repo_revision:
                reasons.append("repository_changed")
        if reasons and version.id not in confirmed:
            staleness = {
                "stale": True,
                "reasons": reasons,
                "current_log_id": current_log_id,
                "current_repo_revision": current_repo,
            }
            error = ValueError(
                f"Plan Version #{version.id} context changed; confirm stale application"
            )
            setattr(error, "staleness", staleness)
            setattr(error, "plan_version_id", version.id)
            raise error
        result.append((plan, version))
    return result


def versioned_plan_snapshots(
    approved: list[tuple[Plan, PlanVersion]],
) -> list[dict[str, object]]:
    return [
        {
            # Legacy display readers require id/title/content. ``id`` remains
            # the stable Plan id while the new fields preserve exact identity.
            "id": plan.id,
            "plan_id": plan.id,
            "version_id": version.id,
            "version_number": version.version_number,
            "title": plan.title or f"Plan #{plan.id}",
            "content": version.content,
        }
        for plan, version in approved
    ]


def build_versioned_plan_prompt(
    approved: list[tuple[Plan, PlanVersion]], user_prompt: str
) -> str:
    if not approved:
        return user_prompt
    parts = [
        "[Approved Plan Versions explicitly selected by the user for this turn]",
        (
            "The Versions below are immutable context for the current instruction. "
            "Approval alone grants no permission beyond that instruction."
        ),
    ]
    for plan, version in approved:
        parts.append(
            f'<approved_plan plan_id="{plan.id}" version_id="{version.id}" '
            f'version="{version.version_number}">\n{version.content}\n</approved_plan>'
        )
    parts.extend(["[User instruction for this turn]", user_prompt])
    return "\n\n".join(parts)


async def _version_resource(
    db: AsyncSession, version: PlanVersion | None
) -> PlanVersionResource | None:
    if version is None:
        return None
    applied = (
        await db.scalar(
            select(PlanApplication.id).where(
                PlanApplication.plan_version_id == version.id
            ).limit(1)
        )
        is not None
    )
    return PlanVersionResource.model_validate(version).model_copy(
        update={"applied": applied}
    )


async def _run_resource(
    db: AsyncSession, run: PlanAgentRun | None, *, include_audit: bool = False
) -> PlanRunResource | None:
    if run is None:
        return None
    steps: list[PlanStepResource] = []
    inputs: list[PlanInputRequestResponse] = []
    if include_audit:
        steps = [
            PlanStepResource.model_validate(row)
            for row in (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars()
        ]
        inputs = [
            input_request_resource(row)
            for row in (
                await db.execute(
                    select(PlanInputRequest)
                    .where(PlanInputRequest.run_id == run.id)
                    .order_by(PlanInputRequest.id)
                )
            ).scalars()
        ]
    return PlanRunResource.model_validate(run).model_copy(
        update={"steps": steps, "input_requests": inputs}
    )


async def plan_resource(
    db: AsyncSession, plan: Plan, *, include_audit: bool = False
) -> PlanResource:
    current = (
        await db.get(PlanVersion, plan.current_version_id)
        if plan.current_version_id is not None
        else None
    )
    active = (
        await db.get(PlanAgentRun, plan.active_run_id)
        if plan.active_run_id is not None
        else None
    )
    latest = (
        await db.execute(
            select(PlanAgentRun)
            .where(PlanAgentRun.plan_id == plan.id)
            .order_by(PlanAgentRun.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    open_input = None
    if active is not None and active.open_input_request_id is not None:
        open_input = await db.get(PlanInputRequest, active.open_input_request_id)

    applied = False
    if current is not None:
        applied = (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == current.id
                ).limit(1)
            )
            is not None
        )
    if plan.archived_at is not None:
        display_state = "archived"
    elif active is not None and active.status == "waiting_user":
        display_state = "waiting_user"
    elif active is not None and active.status in {"queued", "running"}:
        display_state = active.current_stage or "running"
    elif current is not None and applied:
        display_state = "applied"
    elif current is not None and current.human_decision == "approved":
        display_state = "approved"
    elif current is not None and current.human_decision == "rejected":
        display_state = "rejected"
    elif current is not None and (
        current.review_verdict in {"approve", "disabled", "exhausted"}
        or current.review_exhausted
    ):
        display_state = "awaiting_review"
    elif latest is not None and latest.status in {"failed", "cancelled"}:
        display_state = latest.status
    else:
        display_state = "draft"

    payload = {
        column: getattr(plan, column)
        for column in (
            "id", "title", "initial_request", "initial_attachments",
            "target_task_id", "project_id", "target_repo", "target_branch",
            "worker_id", "priority", "timeout_hours", "created_by",
            "current_version_id", "active_run_id", "forked_from_version_id",
            "archived_at", "closed_at", "lock_version", "created_at", "updated_at",
        )
    }
    payload["initial_attachments"] = _public_attachments(plan.initial_attachments)
    legacy = (
        await db.scalar(
            select(PlanLegacyTaskLink.legacy_task_id)
            .where(PlanLegacyTaskLink.plan_id == plan.id)
            .limit(1)
        )
        is not None
    )
    return PlanResource(
        **payload,
        display_state=display_state,
        legacy=legacy,
        latest_run_status=latest.status if latest else None,
        latest_run_error=latest.error if latest else None,
        current_version=await _version_resource(db, current),
        active_run=await _run_resource(db, active, include_audit=include_audit),
        open_input_request=(
            input_request_resource(open_input)
            if open_input is not None
            else None
        ),
    )


async def run_resource(
    db: AsyncSession, run: PlanAgentRun, *, include_audit: bool = True
) -> PlanRunResource:
    resource = await _run_resource(db, run, include_audit=include_audit)
    assert resource is not None
    return resource


async def version_resource(
    db: AsyncSession, version: PlanVersion
) -> PlanVersionResource:
    resource = await _version_resource(db, version)
    assert resource is not None
    return resource


async def apply_worker_plan_outcome(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    worker_id: int,
    expected_generation: int,
    payload: dict,
) -> PlanAgentRun:
    """Import one exact Worker pause while keeping Manager ids authoritative."""

    if payload.get("protocol") != 1:
        raise RuntimeError("Worker Plan outcome protocol mismatch")
    base_worker_version_id = payload.get("base_worker_version_id")
    if isinstance(base_worker_version_id, bool) or (
        base_worker_version_id is not None
        and not isinstance(base_worker_version_id, int)
    ):
        raise RuntimeError("Worker Plan outcome has invalid base Version identity")
    manager_base = (
        await db.get(PlanVersion, run.base_version_id)
        if run.base_version_id is not None
        else None
    )
    if manager_base is not None and manager_base.plan_id != plan.id and run.run_type != "fork":
        raise RuntimeError("Plan Run base Version belongs to another Plan")
    remote = PlanRunResource.model_validate(payload.get("run"))
    remote_versions = [
        PlanVersionResource.model_validate(item)
        for item in payload.get("versions", [])
    ]
    if (
        plan.worker_id != worker_id
        or run.worker_id != worker_id
        or plan.active_run_id != run.id
        or run.status != "running"
        or run.generation != expected_generation
        or remote.id != run.id
        or remote.plan_id != plan.id
        or remote.status not in {"waiting_user", "completed", "failed", "cancelled"}
        or remote.generation < expected_generation
    ):
        raise RuntimeError("Worker Plan outcome no longer owns this Run generation")

    step_by_remote: dict[int, PlanAgentStep] = {}
    for item in remote.steps:
        if item.run_id != remote.id or item.plan_id != plan.id:
            raise RuntimeError("Worker Plan Step belongs to another Run or Plan")
        step = (
            await db.execute(
                select(PlanAgentStep).where(
                    PlanAgentStep.worker_id == worker_id,
                    PlanAgentStep.worker_step_id == item.id,
                )
            )
        ).scalar_one_or_none()
        if step is None:
            step = PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                worker_id=worker_id,
                worker_step_id=item.id,
                generation=item.generation,
                step_type=item.step_type,
                round=item.round,
                provider=item.provider,
                model=item.model,
                effort=item.effort,
                route_slot=item.route_slot,
                status=item.status,
                output=item.output,
                error=item.error,
                started_at=item.started_at,
                finished_at=item.finished_at,
            )
            db.add(step)
            await db.flush()
        elif (
            step.run_id != run.id
            or step.plan_id != plan.id
            or step.step_type != item.step_type
            or step.round != item.round
            or step.generation != item.generation
            or step.provider != item.provider
            or step.model != item.model
            or step.effort != item.effort
            or step.route_slot != item.route_slot
            or step.status != item.status
            or step.output != item.output
            or step.error != item.error
        ):
            raise RuntimeError("Worker Plan Step mapping collides with another Run")
        step_by_remote[item.id] = step

    version_by_remote: dict[int, PlanVersion] = {}
    for item in sorted(remote_versions, key=lambda version: version.version_number):
        if item.plan_id != plan.id:
            raise RuntimeError("Worker Plan Version belongs to another Plan")
        version = (
            await db.execute(
                select(PlanVersion).where(
                    PlanVersion.worker_id == worker_id,
                    PlanVersion.worker_version_id == item.id,
                )
            )
        ).scalar_one_or_none()
        parent = (
            manager_base
            if item.parent_version_id is not None
            and item.parent_version_id == base_worker_version_id
            else version_by_remote.get(item.parent_version_id)
        )
        if item.parent_version_id is not None and parent is None:
            raise RuntimeError("Worker Plan Version parent was not imported")
        produced = step_by_remote.get(item.produced_by_step_id)
        reviewed = step_by_remote.get(item.reviewed_by_step_id)
        if version is None:
            version = PlanVersion(
                plan_id=plan.id,
                worker_id=worker_id,
                worker_version_id=item.id,
                version_number=item.version_number,
                parent_version_id=parent.id if parent is not None else None,
                produced_by_run_id=run.id,
                produced_by_step_id=produced.id if produced is not None else None,
                content=item.content,
                # Manager log/session ids are the authoritative staleness
                # coordinate; Worker-local ids are not comparable here.
                context_session_id=run.context_session_id,
                context_log_id=run.context_log_id,
                # Context snapshots remain Manager-owned and are deliberately
                # not exposed by the public Version resource protocol.
                context_snapshot=run.context_snapshot,
                repo_revision=item.repo_revision,
                human_decision="pending",
                created_at=item.created_at,
            )
            db.add(version)
            await db.flush()
            if (
                parent is manager_base
                and manager_base is not None
                and manager_base.plan_id == plan.id
                and manager_base.superseded_by_version_id is None
            ):
                manager_base.superseded_by_version_id = version.id
        elif (
            version.plan_id != plan.id
            or version.version_number != item.version_number
            or version.content != item.content
        ):
            raise RuntimeError("Worker Plan Version mapping changed immutable content")
        version.review_verdict = item.review_verdict
        version.review_feedback = item.review_feedback
        version.reviewed_by_step_id = reviewed.id if reviewed is not None else None
        version.review_exhausted = item.review_exhausted
        version.reviewed_at = item.reviewed_at
        version_by_remote[item.id] = version
        if produced is not None:
            produced.plan_version_id = version.id

    for item in remote_versions:
        version = version_by_remote[item.id]
        successor = version_by_remote.get(item.superseded_by_version_id)
        if item.superseded_by_version_id is not None and successor is None:
            raise RuntimeError("Worker Plan Version successor was not imported")
        if successor is not None:
            version.superseded_by_version_id = successor.id

    input_by_remote: dict[int, PlanInputRequest] = {}
    for item in remote.input_requests:
        input_request = (
            await db.execute(
                select(PlanInputRequest).where(
                    PlanInputRequest.worker_id == worker_id,
                    PlanInputRequest.worker_input_request_id == item.id,
                )
            )
        ).scalar_one_or_none()
        source = step_by_remote.get(item.source_step_id)
        if source is None:
            raise RuntimeError("Worker InputRequest has no imported source Step")
        if input_request is None:
            input_request = PlanInputRequest(
                plan_id=plan.id,
                run_id=run.id,
                worker_id=worker_id,
                worker_input_request_id=item.id,
                source_step_id=source.id,
                requested_by=item.requested_by,
                reason=item.reason,
                questions=[question.model_dump(mode="json") for question in item.questions],
                status=item.status,
                answers=item.answers,
                response_text=item.response_text,
                attachments=item.attachments,
                answered_by=item.answered_by,
                idempotency_key=f"worker:{worker_id}:input:{item.id}",
                opened_at=item.opened_at,
                answered_at=item.answered_at,
                created_at=item.created_at,
            )
            db.add(input_request)
            await db.flush()
        elif input_request.run_id != run.id or input_request.plan_id != plan.id:
            raise RuntimeError("Worker InputRequest mapping collides with another Run")
        input_by_remote[item.id] = input_request
        source.input_request_id = input_request.id

    latest = max(
        version_by_remote.values(),
        key=lambda version: version.version_number,
        default=None,
    )
    result_version = version_by_remote.get(remote.result_version_id)
    run.current_stage = remote.current_stage
    run.round = remote.round
    run.generation = remote.generation
    run.execution_seconds = remote.execution_seconds
    run.last_execution_started_at = None
    run.result_version_id = result_version.id if result_version is not None else None
    run.interaction_count = remote.interaction_count
    run.review_verdict = remote.review_verdict
    run.review_feedback = remote.review_feedback
    run.review_exhausted = remote.review_exhausted
    run.error = remote.error
    run.updated_at = datetime.utcnow()
    if latest is not None:
        plan.current_version_id = latest.id

    if remote.status == "waiting_user":
        open_input = input_by_remote.get(remote.open_input_request_id)
        if open_input is None or open_input.status != "open":
            raise RuntimeError("Worker waiting Run has no exact open InputRequest")
        run.status = "waiting_user"
        run.open_input_request_id = open_input.id
    else:
        if remote.status == "completed" and result_version is None:
            raise RuntimeError("Worker completed Run has no exact result Version")
        run.status = remote.status
        run.open_input_request_id = None
        run.finished_at = remote.finished_at or datetime.utcnow()
        plan.active_run_id = None
    plan.lock_version += 1
    plan.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(run)
    return run
