"""Independent Plan Task creation, history, revision, and execution APIs."""

from copy import deepcopy
from contextlib import AsyncExitStack
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import get_current_user_id, require_task_access, require_task_control
from backend.api.uploads import (
    UploadAttachmentValidationError,
    validate_upload_attachments,
)
from backend.config import settings
from backend.database import get_db
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import (
    PlanPipelineConfig,
    resolve_plan_pipeline_config,
)
from backend.schemas.task import TaskResponse
from backend.services.plan_tasks import (
    ACTIVE_PLAN_STATUSES,
    MAX_ACTIVE_PLANS_PER_TASK,
    capture_task_context,
    capture_repo_revision,
    latest_task_log_id,
    mark_plan_superseded,
    plan_staleness,
)
from backend.services.plan_pipeline_settings import effective_plan_pipeline_config
from backend.services.task_queue import TaskQueue
from backend.services.worker_proxy import get_task_operation_lock


router = APIRouter(prefix="/api/tasks", tags=["plans"])


class RelatedPlanCreate(BaseModel):
    input: str = Field(min_length=1, max_length=200_000)
    title: str | None = Field(default=None, max_length=200)
    file_paths: list[str] | None = None
    image_paths: list[str] | None = None
    attachments: list[dict] | None = None
    provider: str | None = None
    model: str | None = None
    effort_level: str | None = None
    pipeline_config: PlanPipelineConfig | None = None
    supersedes_plan_task_id: int | None = None


class PlanRevisionRequest(BaseModel):
    feedback: str = Field(min_length=1, max_length=50_000)
    title: str | None = Field(default=None, max_length=200)
    pipeline_config: PlanPipelineConfig | None = None


class PlanExecutionResponse(BaseModel):
    plan_task: TaskResponse
    execution_task: TaskResponse


class PlanAgentStepResponse(BaseModel):
    id: int
    step_type: str
    round: int
    provider: str
    model: str | None
    effort: str | None
    route_slot: str | None
    status: str
    output: str | None
    error: str | None
    started_at: datetime
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class PlanAgentRunResponse(BaseModel):
    id: int
    plan_task_id: int
    status: str
    combo_used: str | None
    planner_provider: str | None
    planner_model: str | None
    planner_effort: str | None
    reviewer_provider: str | None
    reviewer_model: str | None
    reviewer_effort: str | None
    pipeline_config: dict | None
    round: int
    review_verdict: str | None
    review_feedback: str | None
    review_exhausted: bool
    error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    steps: list[PlanAgentStepResponse]


def _queue(db: AsyncSession) -> TaskQueue:
    return TaskQueue(db)


async def _wake_dispatcher() -> None:
    try:
        from backend.main import dispatcher

        if dispatcher:
            dispatcher.wake()
    except Exception:
        pass


def _require_revisable_plan(plan: Task) -> None:
    if plan.mode != "plan":
        raise HTTPException(400, "Task is not a Plan")
    if plan.status == "superseded":
        successor_id = (plan.metadata_ or {}).get(
            "plan_superseded_by_task_id"
        )
        detail = "Plan has been superseded"
        if isinstance(successor_id, int):
            detail += f" by Plan #{successor_id}"
        raise HTTPException(409, detail)
    if plan.status != "plan_review":
        raise HTTPException(400, "Task is not in plan review state")


def _plan_upload_fields(
    task: Task,
) -> tuple[list[str] | None, list[str] | None, list[dict] | None]:
    """Normalize both current and legacy Task attachment metadata."""

    metadata = task.metadata_ or {}
    file_paths = metadata.get("file_paths") or metadata.get("image_paths")
    attachments = metadata.get("attachments")
    if not file_paths:
        return None, None, attachments
    if metadata.get("file_paths") is not None:
        return file_paths, metadata.get("image_paths") or [], attachments
    if isinstance(attachments, list) and len(attachments) == len(file_paths):
        image_paths = [
            path
            for path, attachment in zip(file_paths, attachments, strict=True)
            if isinstance(attachment, dict)
            and attachment.get("is_image") is True
        ]
        return file_paths, image_paths, attachments
    return file_paths, file_paths, attachments


async def _create_related_plan(
    *,
    db: AsyncSession,
    request: Request,
    target: Task,
    body: RelatedPlanCreate,
) -> Task:
    active_count = await db.scalar(
        select(func.count(Task.id)).where(
            Task.plan_target_task_id == target.id,
            Task.mode == "plan",
            Task.status.in_(ACTIVE_PLAN_STATUSES),
        )
    )
    if int(active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
        raise HTTPException(
            429,
            f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
        )

    supersedes = None
    if body.supersedes_plan_task_id is not None:
        supersedes = await db.get(Task, body.supersedes_plan_task_id)
        if (
            supersedes is None
            or supersedes.mode != "plan"
            or supersedes.plan_target_task_id != target.id
        ):
            raise HTTPException(400, "Superseded Plan does not belong to this Task")
        await require_task_control(request, supersedes, db)
        _require_revisable_plan(supersedes)
    superseded_id = supersedes.id if supersedes is not None else None

    try:
        uploads = validate_upload_attachments(
            file_paths=body.file_paths,
            image_paths=body.image_paths,
            attachments=body.attachments,
        )
    except UploadAttachmentValidationError as exc:
        raise HTTPException(400, str(exc)) from exc

    pipeline = resolve_plan_pipeline_config(
        body.pipeline_config,
        base_config=await effective_plan_pipeline_config(db),
        legacy_provider=body.provider,
        legacy_model=body.model,
        legacy_effort=body.effort_level,
    )
    provider = pipeline.planner.primary.provider
    model = pipeline.planner.primary.model
    effort = pipeline.planner.primary.effort
    # Plan Agents use isolated read-only turns. Fast requires the app-server
    # proof chain and must never be silently downgraded.
    codex_service_tier = "default"
    from backend.api.tasks import _validate_task_service_tier_configuration

    try:
        routes = (
            pipeline.planner.primary,
            pipeline.planner.fallback,
            pipeline.reviewer.primary,
            pipeline.reviewer.fallback,
        )
        for route in routes:
            _validate_task_service_tier_configuration(
                provider=route.provider,
                model=route.model,
                codex_service_tier=codex_service_tier,
                mode="plan",
                goal_evaluator_model=None,
            )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    context_log_id = await latest_task_log_id(db, target.id)
    context_snapshot = await capture_task_context(
        db,
        target.id,
        through_log_id=context_log_id,
        max_chars=settings.plan_transcript_max_chars,
    )
    repo_revision = (
        None
        if target.worker_id is not None
        else await capture_repo_revision(target.last_cwd or target.target_repo)
    )
    plan = Task(
        title=(
            body.title.strip()
            if body.title and body.title.strip()
            else (
                f"Plan for #{target.id}: {target.title.strip()}"
                if target.title and target.title.strip()
                else f"Plan for #{target.id}"
            )
        )[:200],
        description=body.input.strip(),
        status="pending",
        priority=target.priority,
        project_id=target.project_id,
        target_repo=target.target_repo,
        target_branch=target.target_branch,
        merge_status="pending",
        worker_id=target.worker_id,
        created_by=get_current_user_id(request),
        max_retries=target.max_retries,
        mode="plan",
        provider=provider,
        model=model,
        codex_service_tier=codex_service_tier,
        effort_level=effort,
        thinking_budget=None,
        timeout_hours=target.timeout_hours,
        enable_workflows=False,
        enabled_skills={},
        selected_user_skills=[],
        metadata_={
            "created_from_plan_target_task_id": target.id,
            **(
                {
                    "file_paths": [upload.path for upload in uploads],
                    "image_paths": [
                        upload.path for upload in uploads if upload.is_image
                    ],
                    "attachments": [
                        upload.public_dict() for upload in uploads
                    ],
                }
                if uploads
                else {}
            ),
            **(
                {"revised_from_plan_task_id": supersedes.id}
                if supersedes is not None
                else {}
            ),
        },
        plan_target_task_id=target.id,
        plan_context_session_id=target.session_id,
        plan_context_log_id=context_log_id,
        plan_context_snapshot=context_snapshot,
        plan_repo_revision=repo_revision,
        supersedes_plan_task_id=(
            supersedes.id if supersedes is not None else None
        ),
        plan_pipeline_config=pipeline.model_dump(mode="json"),
    )
    db.add(plan)
    await db.flush()
    if supersedes is not None and not await mark_plan_superseded(
        db,
        supersedes,
        successor_id=plan.id,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Plan changed while its revision was being created",
        )
    await db.commit()
    await db.refresh(plan)
    if superseded_id is not None:
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(superseded_id, "superseded")
    if plan.project_id:
        try:
            from backend.services.task_sharing import auto_share_new_task

            await auto_share_new_task(db, plan.id, plan.project_id)
        except Exception:
            pass
    await _wake_dispatcher()
    return plan


@router.post(
    "/{target_task_id}/plans",
    response_model=TaskResponse,
    status_code=201,
)
async def create_related_plan(
    target_task_id: int,
    body: RelatedPlanCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if {
        "pipeline_config",
        "provider",
        "model",
        "effort_level",
    } & body.model_fields_set:
        raise HTTPException(
            422,
            "Planner and Reviewer routes are configured globally",
        )
    target = await db.get(Task, target_task_id)
    if target is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, target, db)
    if not target.session_id:
        raise HTTPException(400, "Run the target Task before creating a session Plan")
    if target.shared_from_id is not None:
        raise HTTPException(409, "Shared shadow tasks cannot own Plan Tasks")
    lock_ids = {target_task_id}
    if body.supersedes_plan_task_id is not None:
        lock_ids.add(body.supersedes_plan_task_id)
    async with AsyncExitStack() as stack:
        for task_id in sorted(lock_ids):
            await stack.enter_async_context(get_task_operation_lock(task_id))
        db.expire_all()
        target = await db.get(Task, target_task_id)
        if target is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, target, db)
        return await _create_related_plan(
            db=db,
            request=request,
            target=target,
            body=body,
        )


@router.get("/{target_task_id}/plans", response_model=list[TaskResponse])
async def list_related_plans(
    target_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    target = await db.get(Task, target_task_id)
    if target is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, target, db)
    rows = await db.execute(
        select(Task)
        .where(
            Task.plan_target_task_id == target_task_id,
            Task.mode == "plan",
        )
        .order_by(Task.created_at.desc(), Task.id.desc())
    )
    plans = list(rows.scalars().all())
    # A related Plan inherits the target's visibility, but do not accidentally
    # expose a row whose ownership/routing was corrupted independently.
    for plan in plans:
        await require_task_access(request, plan, db)
    return plans


@router.get("/{plan_task_id}/plan/staleness")
async def get_plan_staleness(
    plan_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    plan = await db.get(Task, plan_task_id)
    if plan is None:
        raise HTTPException(404, "Plan Task not found")
    await require_task_access(request, plan, db)
    if plan.mode != "plan":
        raise HTTPException(400, "Task is not a Plan")
    if plan.plan_target_task_id is not None:
        target = await db.get(Task, plan.plan_target_task_id)
        if target is None:
            raise HTTPException(409, "Plan target no longer exists")
        await require_task_access(request, target, db)
    if plan.worker_id is not None:
        from backend.api.tasks import _proxy

        result = await _proxy(
            plan,
            "GET",
            f"/api/tasks/{plan_task_id}/plan/staleness",
        )
        if not isinstance(result, dict) or "stale" not in result:
            raise HTTPException(502, "Worker returned invalid Plan staleness")
        return result
    return await plan_staleness(db, plan)


@router.get(
    "/{plan_task_id}/plan/runs",
    response_model=list[PlanAgentRunResponse],
)
async def list_plan_runs(
    plan_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    plan = await db.get(Task, plan_task_id)
    if plan is None:
        raise HTTPException(404, "Plan Task not found")
    await require_task_access(request, plan, db)
    if plan.mode != "plan":
        raise HTTPException(400, "Task is not a Plan")
    if plan.worker_id is not None:
        from backend.api.tasks import _proxy

        result = await _proxy(
            plan,
            "GET",
            f"/api/tasks/{plan_task_id}/plan/runs",
        )
        if not isinstance(result, list):
            raise HTTPException(502, "Worker returned invalid Plan run history")
        return result

    runs = list(
        (
            await db.execute(
                select(PlanAgentRun)
                .where(PlanAgentRun.plan_task_id == plan_task_id)
                .order_by(PlanAgentRun.id.desc())
            )
        ).scalars().all()
    )
    if not runs:
        return []
    step_rows = list(
        (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id.in_([run.id for run in runs]))
                .order_by(PlanAgentStep.id)
            )
        ).scalars().all()
    )
    by_run: dict[int, list[PlanAgentStep]] = {}
    for step in step_rows:
        by_run.setdefault(step.run_id, []).append(step)
    return [
        PlanAgentRunResponse(
            **{
                column: getattr(run, column)
                for column in (
                    "id",
                    "plan_task_id",
                    "status",
                    "combo_used",
                    "planner_provider",
                    "planner_model",
                    "planner_effort",
                    "reviewer_provider",
                    "reviewer_model",
                    "reviewer_effort",
                    "pipeline_config",
                    "round",
                    "review_verdict",
                    "review_feedback",
                    "review_exhausted",
                    "error",
                    "created_at",
                    "updated_at",
                    "finished_at",
                )
            },
            steps=by_run.get(run.id, []),
        )
        for run in runs
    ]


@router.post(
    "/{plan_task_id}/plan/revise",
    response_model=TaskResponse,
    status_code=201,
)
async def revise_plan(
    plan_task_id: int,
    body: PlanRevisionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    source = await db.get(Task, plan_task_id)
    if source is None:
        raise HTTPException(404, "Plan Task not found")
    await require_task_control(request, source, db)
    _require_revisable_plan(source)
    target_id = source.plan_target_task_id or source.id
    async with AsyncExitStack() as stack:
        for task_id in sorted({source.id, target_id}):
            await stack.enter_async_context(get_task_operation_lock(task_id))
        db.expire_all()
        current_source = await db.get(Task, plan_task_id)
        current_target = (
            await db.get(Task, current_source.plan_target_task_id)
            if current_source is not None
            and current_source.plan_target_task_id is not None
            else current_source
        )
        if current_source is None or current_target is None:
            raise HTTPException(409, "Plan or target disappeared during revision")
        await require_task_control(request, current_target, db)
        await require_task_control(request, current_source, db)
        _require_revisable_plan(current_source)

        prompt = (
            f"{current_source.description or ''}\n\n"
            "Previous Plan:\n"
            f"{current_source.plan_content or '(no completed plan)'}\n\n"
            "User revision feedback:\n"
            f"{body.feedback.strip()}"
        )
        revision_pipeline = resolve_plan_pipeline_config(
            body.pipeline_config or current_source.plan_pipeline_config,
            legacy_provider=current_source.provider,
            legacy_model=current_source.model,
            legacy_effort=current_source.effort_level,
        )
        if current_source.plan_target_task_id is None:
            revision = Task(
                title=(
                    body.title.strip()
                    if body.title and body.title.strip()
                    else f"Revision of Plan #{current_source.id}"
                )[:200],
                description=prompt,
                status="pending",
                priority=current_source.priority,
                project_id=current_source.project_id,
                target_repo=current_source.target_repo,
                target_branch=current_source.target_branch,
                merge_status="pending",
                worker_id=current_source.worker_id,
                created_by=get_current_user_id(request),
                max_retries=current_source.max_retries,
                mode="plan",
                provider=revision_pipeline.planner.primary.provider,
                model=revision_pipeline.planner.primary.model,
                codex_service_tier=current_source.codex_service_tier,
                effort_level=revision_pipeline.planner.primary.effort,
                plan_pipeline_config=(
                    revision_pipeline.model_dump(mode="json")
                ),
                timeout_hours=current_source.timeout_hours,
                enable_workflows=False,
                enabled_skills={},
                selected_user_skills=[],
                metadata_={
                    "revised_from_plan_task_id": current_source.id
                },
                plan_context_session_id=None,
                plan_context_log_id=None,
                plan_repo_revision=await capture_repo_revision(
                    current_source.last_cwd or current_source.target_repo
                ),
                supersedes_plan_task_id=current_source.id,
            )
            db.add(revision)
            await db.flush()
            if not await mark_plan_superseded(
                db,
                current_source,
                successor_id=revision.id,
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Plan changed while its revision was being created",
                )
            await db.commit()
            await db.refresh(revision)
            from backend.services.task_events import broadcast_status_change

            await broadcast_status_change(
                plan_task_id,
                "superseded",
            )
            if revision.project_id:
                try:
                    from backend.services.task_sharing import auto_share_new_task

                    await auto_share_new_task(
                        db,
                        revision.id,
                        revision.project_id,
                    )
                except Exception:
                    pass
            await _wake_dispatcher()
            return revision

        revision_files, revision_images, revision_attachments = (
            _plan_upload_fields(current_source)
        )
        return await _create_related_plan(
            db=db,
            request=request,
            target=current_target,
            body=RelatedPlanCreate(
                input=prompt,
                title=body.title or f"Revision of Plan #{current_source.id}",
                file_paths=revision_files,
                image_paths=revision_images,
                attachments=revision_attachments,
                provider=current_source.provider,
                model=current_source.model,
                effort_level=current_source.effort_level,
                pipeline_config=revision_pipeline,
                supersedes_plan_task_id=current_source.id,
            ),
        )


@router.post(
    "/{plan_task_id}/plan/create-execution-task",
    response_model=PlanExecutionResponse,
    status_code=201,
)
async def create_plan_execution_task(
    plan_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    async with get_task_operation_lock(plan_task_id):
        plan = await db.get(Task, plan_task_id)
        if plan is None:
            raise HTTPException(404, "Plan Task not found")
        await require_task_control(request, plan, db)
        if plan.mode != "plan" or plan.plan_target_task_id is not None:
            raise HTTPException(400, "Only standalone Plans create execution Tasks")
        if plan.plan_approved is not True or not plan.plan_content:
            raise HTTPException(400, "Plan must be approved before execution")
        if plan.plan_execution_task_id is not None:
            existing = await db.get(Task, plan.plan_execution_task_id)
            if existing is None:
                raise HTTPException(409, "Recorded execution Task no longer exists")
            return PlanExecutionResponse(plan_task=plan, execution_task=existing)

        metadata = deepcopy(plan.metadata_ or {})
        metadata["created_from_plan_task_id"] = plan.id
        execution = Task(
            title=f"Execute Plan #{plan.id}: {plan.title}"[:200],
            description=(
                "[Approved implementation plan]\n"
                "The user explicitly created this execution Task from the "
                "approved Plan below. Implement it now, adapting only when the "
                "repository requires it.\n\n"
                f"<plan>\n{plan.plan_content}\n</plan>\n\n"
                "[Original planning request]\n"
                f"{plan.description or ''}"
            ),
            status="pending",
            priority=plan.priority,
            project_id=plan.project_id,
            target_repo=plan.target_repo,
            target_branch=plan.target_branch,
            merge_status="pending",
            worker_id=plan.worker_id,
            created_by=get_current_user_id(request),
            max_retries=plan.max_retries,
            mode="auto",
            provider=plan.provider,
            model=plan.model,
            codex_service_tier=plan.codex_service_tier,
            effort_level=plan.effort_level,
            thinking_budget=plan.thinking_budget,
            system_prompt_mode=plan.system_prompt_mode,
            timeout_hours=plan.timeout_hours,
            enable_workflows=plan.enable_workflows,
            enabled_skills=deepcopy(plan.enabled_skills),
            selected_user_skills=deepcopy(plan.selected_user_skills),
            tags=deepcopy(plan.tags),
            metadata_=metadata,
        )
        db.add(execution)
        await db.flush()
        linked = await db.execute(
            update(Task)
            .where(
                Task.id == plan.id,
                Task.plan_execution_task_id.is_(None),
                Task.plan_approved.is_(True),
            )
            .values(plan_execution_task_id=execution.id)
        )
        if linked.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Plan execution Task was created concurrently")
        await db.commit()
        await db.refresh(plan)
        await db.refresh(execution)
        if execution.project_id:
            try:
                from backend.services.task_sharing import auto_share_new_task

                await auto_share_new_task(
                    db,
                    execution.id,
                    execution.project_id,
                )
            except Exception:
                pass
    await _wake_dispatcher()
    return PlanExecutionResponse(plan_task=plan, execution_task=execution)
