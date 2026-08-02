"""Canonical first-class Plan, Version, Run, and InputRequest APIs."""

from copy import deepcopy
from datetime import datetime
import hashlib
import os

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    has_project_access,
    has_worker_access,
    is_admin,
    require_project_access,
    require_internal_service,
    require_task_access,
    require_task_control,
    require_worker_target_access,
)
from backend.api.uploads import (
    UploadAttachmentValidationError,
    validate_upload_attachments,
)
from backend.config import settings
from backend.database import get_db
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.project import Project
from backend.schemas.plan import resolve_plan_pipeline_config
from backend.schemas.plan_resource import (
    PlanCreateRequest,
    PlanDecisionRequest,
    PlanExecutionCreateRequest,
    PlanExecutionResource,
    PlanForkRequest,
    PlanInputAnswerRequest,
    PlanInputRequestResponse,
    PlanPatchRequest,
    PlanResource,
    PlanRunCreateRequest,
    PlanRunResource,
    PlanVersionResource,
    WorkerPlanRunImportRequest,
    WorkerPlanVersionImportRequest,
    WorkerPlanVersionSeed,
)
from backend.services.plan_pipeline_settings import effective_plan_pipeline_config
from backend.services.plan_service import (
    ACTIVE_RUN_STATUSES,
    answer_input_request,
    cancel_run,
    create_plan_run,
    create_plan_with_run,
    decide_version,
    input_request_resource,
    plan_operation_lock,
    plan_resource,
    resolve_legacy_task,
    run_resource,
    version_resource,
)
from backend.services.plan_tasks import (
    MAX_ACTIVE_PLANS_PER_TASK,
    capture_repo_revision,
    capture_task_context,
    latest_task_log_id,
)
from backend.services.plan_staleness import version_staleness
from backend.services.plan_events import broadcast_plan_event
from backend.services.plan_input_safety import contains_high_confidence_secret


router = APIRouter(tags=["plan-resources"])


class _WorkerRepoRevisionRequest(BaseModel):
    project_id: int | None = None
    target_task_id: int | None = None


def _reject_durable_plan_secrets(*values: object) -> None:
    if contains_high_confidence_secret(values):
        raise HTTPException(
            422,
            "Plan text cannot store API keys or access tokens. "
            "Save the credential in Settings → Secrets and refer to it by name.",
        )


async def _wake_dispatcher() -> None:
    try:
        from backend.main import dispatcher

        if dispatcher:
            dispatcher.wake()
    except Exception:
        pass


async def _materialize_worker_version(
    db: AsyncSession,
    *,
    plan: Plan,
    seed: WorkerPlanVersionSeed,
) -> PlanVersion:
    """Idempotently restore one immutable Manager Version on this Worker."""

    version = (
        await db.execute(
            select(PlanVersion).where(
                PlanVersion.plan_id == plan.id,
                PlanVersion.version_number == seed.version_number,
            )
        )
    ).scalar_one_or_none()
    if version is None:
        version = PlanVersion(
            plan_id=plan.id,
            version_number=seed.version_number,
            content=seed.content,
            context_session_id=seed.context_session_id,
            context_log_id=seed.context_log_id,
            context_snapshot=seed.context_snapshot,
            repo_revision=seed.repo_revision,
            reviewer_repo_revision=seed.reviewer_repo_revision,
            review_verdict=seed.review_verdict,
            review_feedback=seed.review_feedback,
            review_exhausted=seed.review_exhausted,
            reviewed_at=seed.reviewed_at,
            human_decision=seed.human_decision,
        )
        db.add(version)
        await db.flush()
    elif version.content != seed.content:
        raise HTTPException(
            409,
            "Worker Plan Version number collides with different immutable content",
        )
    elif (
        version.human_decision not in {"pending", seed.human_decision}
        and seed.human_decision != "pending"
    ):
        raise HTTPException(409, "Worker Plan Version decision conflicts with Manager")
    if seed.human_decision != "pending":
        version.human_decision = seed.human_decision
    version.review_verdict = seed.review_verdict
    version.review_feedback = seed.review_feedback
    version.review_exhausted = seed.review_exhausted
    version.reviewed_at = seed.reviewed_at
    version.reviewer_repo_revision = seed.reviewer_repo_revision
    plan.current_version_id = version.id
    plan.updated_at = datetime.utcnow()
    return version


def _validated_uploads(body) -> list[dict] | None:
    try:
        uploads = validate_upload_attachments(
            file_paths=body.file_paths,
            image_paths=body.image_paths,
            attachments=body.attachments,
        )
    except UploadAttachmentValidationError as exc:
        raise HTTPException(400, str(exc)) from exc
    return [
        {**item.public_dict(), "path": item.path}
        for item in uploads
    ] or None


def _validate_attachment_manifest(
    uploads: list[dict] | None,
    manifest: list[dict] | None,
) -> list[dict]:
    expected = manifest or []
    paths = [item["path"] for item in (uploads or [])]
    if len(expected) != len(paths):
        raise HTTPException(409, "Plan attachment manifest count does not match uploads")
    receipt: list[dict] = []
    for index, path in enumerate(paths):
        item = expected[index]
        if not isinstance(item, dict) or os.path.abspath(path) != item.get("path"):
            raise HTTPException(409, "Plan attachment manifest path/order mismatch")
        digest = hashlib.sha256()
        size = 0
        try:
            with open(path, "rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
        except OSError as exc:
            raise HTTPException(409, "Plan attachment is unavailable on Worker") from exc
        row = {"path": os.path.abspath(path), "size": size, "sha256": digest.hexdigest()}
        if row != item:
            raise HTTPException(409, "Plan attachment digest/size mismatch")
        receipt.append(row)
    return receipt


async def _has_plan_access(
    request: Request, plan: Plan, db: AsyncSession, *, control: bool
) -> bool:
    if not settings.auth_token or is_admin(request):
        return True
    if plan.target_task_id is not None:
        target = await db.get(Task, plan.target_task_id)
        if target is None:
            return False
        try:
            if control:
                await require_task_control(request, target, db)
            else:
                await require_task_access(request, target, db)
            return True
        except HTTPException:
            return False
    user_id = get_current_user_id(request)
    if user_id is not None and plan.created_by == user_id:
        return True
    if plan.worker_id is not None and await has_worker_access(request, plan.worker_id, db):
        return True
    if plan.project_id is not None and await has_project_access(request, plan.project_id, db):
        return True
    return False


async def _require_plan(
    request: Request,
    db: AsyncSession,
    plan_id: int,
    *,
    control: bool = False,
) -> Plan:
    plan = await db.get(Plan, plan_id)
    if plan is None:
        raise HTTPException(404, "Plan not found")
    if not await _has_plan_access(request, plan, db, control=control):
        raise HTTPException(403, "No permission to control this Plan" if control else "No access to this Plan")
    return plan


async def _require_version(
    request: Request,
    db: AsyncSession,
    version_id: int,
    *,
    control: bool = False,
) -> tuple[Plan, PlanVersion]:
    version = await db.get(PlanVersion, version_id)
    if version is None:
        raise HTTPException(404, "Plan Version not found")
    plan = await _require_plan(request, db, version.plan_id, control=control)
    return plan, version


async def _capture_context_for_plan(
    db: AsyncSession,
    *,
    target: Task | None,
    target_repo: str | None,
    worker_id: int | None,
) -> tuple[str | None, int | None, str | None, dict | None]:
    session_id = target.session_id if target is not None else None
    log_id = await latest_task_log_id(db, target.id) if target is not None else None
    snapshot = (
        await capture_task_context(
            db,
            target.id,
            through_log_id=log_id,
            max_chars=settings.plan_transcript_max_chars,
        )
        if target is not None
        else None
    )
    repo_revision = (
        None
        if worker_id is not None
        else await capture_repo_revision(
            (target.last_cwd or target.target_repo) if target is not None else target_repo
        )
    )
    return session_id, log_id, snapshot, repo_revision


async def _version_staleness(
    db: AsyncSession, plan: Plan, version: PlanVersion
) -> dict:
    return await version_staleness(db, plan, version)


@router.post("/api/plans/worker-repo-revision")
async def worker_repo_revision(
    body: _WorkerRepoRevisionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return a Worker-local fingerprint without exposing repository content."""

    require_internal_service(request)
    target = await db.get(Task, body.target_task_id) if body.target_task_id else None
    if body.target_task_id is not None and target is None:
        raise HTTPException(409, "Worker target Task is missing")
    project = await db.get(Project, body.project_id) if body.project_id else None
    if body.project_id is not None and project is None:
        raise HTTPException(409, "Worker Project is missing")
    path = (
        target.last_cwd or target.target_repo
        if target is not None
        else project.local_path if project is not None else None
    )
    return {"repo_revision": await capture_repo_revision(path)}


@router.get("/api/plans/worker-application-receipts/{receipt_key}")
async def worker_application_receipt(
    receipt_key: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    receipt = (
        await db.execute(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise HTTPException(404, "Plan application receipt not found")
    return {
        "receipt_key": receipt.receipt_key,
        "target_task_id": receipt.target_task_id,
        "plan_version_ids": receipt.plan_version_ids,
        "status": receipt.status,
        "response": receipt.response,
    }


@router.post("/api/plans", response_model=PlanResource, status_code=201)
async def create_plan(
    body: PlanCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.input, body.title)
    target = None
    if body.target_task_id is not None:
        target = await db.get(Task, body.target_task_id)
        if target is None:
            raise HTTPException(404, "Target Task not found")
        await require_task_control(request, target, db)
        if not target.session_id:
            raise HTTPException(400, "Run the target Task before creating a session Plan")
        if target.shared_from_id is not None:
            raise HTTPException(409, "Shared shadow tasks cannot own Plans")
        if target.status == "migrating":
            raise HTTPException(409, "Plan target is changing execution location")
        active_count = await db.scalar(
            select(func.count(Plan.id)).where(
                Plan.target_task_id == target.id,
                Plan.archived_at.is_(None),
                Plan.active_run_id.isnot(None),
            )
        )
        if int(active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
            raise HTTPException(429, f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans")
        project_id = target.project_id
        target_repo = target.target_repo
        target_branch = target.target_branch
        worker_id = target.worker_id
        priority = target.priority
        timeout_hours = target.timeout_hours
    else:
        project_id = body.project_id
        target_repo = body.target_repo
        target_branch = body.target_branch
        worker_id = body.worker_id
        priority = body.priority
        timeout_hours = body.timeout_hours
        if project_id is not None:
            from backend.models.project import Project

            project = await db.get(Project, project_id)
            if project is None:
                raise HTTPException(404, "Project not found")
            if body.worker_id is not None and body.worker_id != project.worker_id:
                raise HTTPException(
                    400,
                    "Plan Worker must match the selected Project location",
                )
            # A Project is the authorization boundary for its checkout. Never
            # let a member pair shared Project access with an arbitrary path.
            target_repo = project.local_path
            worker_id = project.worker_id
        if settings.auth_token:
            if project_id is not None:
                await require_project_access(request, project_id, db)
            else:
                await require_worker_target_access(request, worker_id, db)

    uploads = _validated_uploads(body)
    pipeline = resolve_plan_pipeline_config(
        None,
        base_config=await effective_plan_pipeline_config(db),
    )
    context = await _capture_context_for_plan(
        db, target=target, target_repo=target_repo, worker_id=worker_id
    )
    title = (
        body.title.strip()
        if body.title and body.title.strip()
        else (
            f"Plan for #{target.id}: {target.title}" if target is not None else body.input.strip().splitlines()[0]
        )
    )[:200]
    plan, _run = await create_plan_with_run(
        db,
        title=title,
        initial_request=body.input.strip(),
        attachments=uploads,
        target_task_id=target.id if target is not None else None,
        project_id=project_id,
        target_repo=target_repo,
        target_branch=target_branch,
        worker_id=worker_id,
        priority=priority,
        timeout_hours=timeout_hours,
        created_by=get_current_user_id(request),
        pipeline_config=pipeline.model_dump(mode="json"),
        context_session_id=context[0],
        context_log_id=context[1],
        context_snapshot=context[2],
        repo_revision=context[3],
    )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_created", plan_id=plan.id, target_task_id=plan.target_task_id
    )
    return await plan_resource(db, plan, include_audit=True)


@router.post("/api/plans/worker-import")
async def import_worker_plan_run(
    body: WorkerPlanRunImportRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create one inert Manager-owned mirror, then let this Worker dispatch it."""

    require_internal_service(request)
    uploads = _validated_uploads(body)
    attachment_receipt = _validate_attachment_manifest(
        uploads, body.attachment_manifest
    )
    project = await db.get(Project, body.project_id) if body.project_id is not None else None
    if body.project_id is not None and project is None:
        raise HTTPException(409, "Worker Plan project is missing")
    target = await db.get(Task, body.target_task_id) if body.target_task_id is not None else None
    if body.target_task_id is not None and target is None:
        raise HTTPException(409, "Worker Plan target Task is missing")
    target_repo = (
        (target.last_cwd or target.target_repo)
        if target is not None
        else (project.local_path if project is not None else None)
    )

    plan = await db.get(Plan, body.plan_id)
    if plan is None:
        plan = Plan(
            id=body.plan_id,
            title=body.title,
            initial_request=body.initial_request,
            initial_attachments=uploads,
            target_task_id=body.target_task_id,
            project_id=body.project_id,
            target_repo=target_repo,
            target_branch=body.target_branch,
            worker_id=None,
            relay_origin="manager_v1",
            priority=body.priority,
            timeout_hours=body.timeout_hours,
            created_by=None,
            pipeline_config=body.pipeline_config.model_dump(mode="json"),
        )
        db.add(plan)
        try:
            await db.flush()
        except Exception as exc:
            await db.rollback()
            raise HTTPException(409, "Worker Plan id collides with local data") from exc
    elif (
        plan.relay_origin != "manager_v1"
        or plan.initial_request != body.initial_request
        or plan.target_task_id != body.target_task_id
        or plan.project_id != body.project_id
        or plan.target_branch != body.target_branch
        or plan.priority != body.priority
        or plan.timeout_hours != body.timeout_hours
        or plan.pipeline_config != body.pipeline_config.model_dump(mode="json")
    ):
        raise HTTPException(409, "Worker Plan mirror identity changed")
    plan.title = body.title

    existing = await db.get(PlanAgentRun, body.run_id)
    if existing is not None:
        if existing.plan_id != plan.id or existing.relay_origin != "manager_v1":
            raise HTTPException(409, "Worker Plan Run id collides with local data")
        await db.commit()
        return {
            "protocol": 1,
            "base_worker_version_id": existing.base_version_id,
            "attachment_receipt": attachment_receipt,
            "run": (await run_resource(db, existing)).model_dump(mode="json"),
        }
    if plan.active_run_id is not None:
        raise HTTPException(409, f"Worker Plan already has active Run #{plan.active_run_id}")

    base_version = None
    if body.base_version is not None:
        base_version = await _materialize_worker_version(
            db,
            plan=plan,
            seed=body.base_version,
        )

    run = PlanAgentRun(
        id=body.run_id,
        plan_id=plan.id,
        plan_task_id=None,
        run_type=body.run_type,
        source_run_id=body.source_run_id,
        base_version_id=base_version.id if base_version is not None else None,
        request_text=body.request_text,
        attachments=uploads,
        context_session_id=body.context_session_id,
        context_log_id=body.context_log_id,
        context_snapshot=body.context_snapshot,
        repo_revision=body.repo_revision,
        current_stage="planner",
        generation=body.run_generation,
        worker_id=None,
        relay_origin="manager_v1",
        max_interactions=body.max_interactions,
        pipeline_config=body.pipeline_config.model_dump(mode="json"),
        status="queued",
        round=1,
    )
    db.add(run)
    try:
        await db.flush()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(409, "Worker Plan Run id collides with local data") from exc
    plan.active_run_id = run.id
    plan.updated_at = datetime.utcnow()
    await db.commit()
    await _wake_dispatcher()
    return {
        "protocol": 1,
        "base_worker_version_id": run.base_version_id,
        "attachment_receipt": attachment_receipt,
        "run": (await run_resource(db, run)).model_dump(mode="json"),
    }


@router.post(
    "/api/plans/worker-materialize-version",
    response_model=PlanVersionResource,
)
async def materialize_worker_plan_version(
    body: WorkerPlanVersionImportRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Materialize exact approved content before a Worker chat application."""

    require_internal_service(request)
    project = (
        await db.get(Project, body.project_id)
        if body.project_id is not None
        else None
    )
    if body.project_id is not None and project is None:
        raise HTTPException(409, "Worker Plan project is missing")
    target = (
        await db.get(Task, body.target_task_id)
        if body.target_task_id is not None
        else None
    )
    if body.target_task_id is not None and target is None:
        raise HTTPException(409, "Worker Plan target Task is missing")
    target_repo = (
        (target.last_cwd or target.target_repo)
        if target is not None
        else (project.local_path if project is not None else None)
    )
    plan = await db.get(Plan, body.plan_id)
    pipeline = body.pipeline_config.model_dump(mode="json")
    if plan is None:
        plan = Plan(
            id=body.plan_id,
            title=body.title,
            initial_request=body.initial_request,
            target_task_id=body.target_task_id,
            project_id=body.project_id,
            target_repo=target_repo,
            target_branch=body.target_branch,
            worker_id=None,
            relay_origin="manager_v1",
            priority=body.priority,
            timeout_hours=body.timeout_hours,
            created_by=None,
            pipeline_config=pipeline,
        )
        db.add(plan)
        try:
            await db.flush()
        except Exception as exc:
            await db.rollback()
            raise HTTPException(409, "Worker Plan id collides with local data") from exc
    elif (
        plan.relay_origin != "manager_v1"
        or plan.initial_request != body.initial_request
        or plan.target_task_id != body.target_task_id
        or plan.project_id != body.project_id
        or plan.target_branch != body.target_branch
        or plan.pipeline_config != pipeline
    ):
        raise HTTPException(409, "Worker Plan mirror identity changed")
    plan.title = body.title
    if plan.active_run_id is not None:
        raise HTTPException(409, "Worker Plan has an active Run")
    version = await _materialize_worker_version(
        db,
        plan=plan,
        seed=body.version,
    )
    await db.commit()
    await db.refresh(version)
    return await version_resource(db, version)


@router.get("/api/plans", response_model=list[PlanResource])
async def list_plans(
    request: Request,
    target_task_id: int | None = None,
    kind: str | None = Query(default=None, pattern="^(standalone|related)$"),
    display_state: str | None = None,
    project_id: int | None = None,
    include_archived: bool = False,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    query = select(Plan)
    if target_task_id is not None:
        query = query.where(Plan.target_task_id == target_task_id)
    if project_id is not None:
        query = query.where(Plan.project_id == project_id)
    if kind == "standalone":
        query = query.where(Plan.target_task_id.is_(None))
    elif kind == "related":
        query = query.where(Plan.target_task_id.isnot(None))
    if not include_archived:
        query = query.where(Plan.archived_at.is_(None))
    rows = list((await db.execute(query.order_by(Plan.updated_at.desc(), Plan.id.desc()))).scalars())
    resources: list[PlanResource] = []
    for plan in rows:
        if not await _has_plan_access(request, plan, db, control=False):
            continue
        resource = await plan_resource(db, plan)
        if display_state is None or resource.display_state == display_state:
            resources.append(resource)
    return resources[offset:offset + limit]


@router.get("/api/plans/count")
async def count_plans(
    request: Request,
    target_task_id: int | None = None,
    kind: str | None = Query(default=None, pattern="^(standalone|related)$"),
    display_state: str | None = None,
    project_id: int | None = None,
    include_archived: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """Count the same ACL-filtered projection exposed by ``list_plans``."""

    query = select(Plan)
    if target_task_id is not None:
        query = query.where(Plan.target_task_id == target_task_id)
    if project_id is not None:
        query = query.where(Plan.project_id == project_id)
    if kind == "standalone":
        query = query.where(Plan.target_task_id.is_(None))
    elif kind == "related":
        query = query.where(Plan.target_task_id.isnot(None))
    if not include_archived:
        query = query.where(Plan.archived_at.is_(None))
    rows = list((await db.execute(query)).scalars())
    total = 0
    for plan in rows:
        if not await _has_plan_access(request, plan, db, control=False):
            continue
        if display_state is not None:
            resource = await plan_resource(db, plan)
            if resource.display_state != display_state:
                continue
        total += 1
    return {"total": total}


@router.get("/api/plans/resolve-legacy-task/{task_id}", response_model=PlanResource)
async def resolve_legacy_plan_task(
    task_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    link = await resolve_legacy_task(db, task_id)
    if link is None:
        raise HTTPException(404, "Legacy Plan Task link not found")
    plan = await _require_plan(request, db, link.plan_id)
    return await plan_resource(db, plan, include_audit=True)


@router.get("/api/plans/{plan_id}", response_model=PlanResource)
async def get_plan_resource(
    plan_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    plan = await _require_plan(request, db, plan_id)
    return await plan_resource(db, plan, include_audit=True)


@router.patch("/api/plans/{plan_id}", response_model=PlanResource)
async def patch_plan(
    plan_id: int,
    body: PlanPatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.title)
    async with plan_operation_lock(plan_id):
        plan = await _require_plan(request, db, plan_id, control=True)
        if body.archived is True and plan.active_run_id is not None:
            raise HTTPException(409, "Cancel the active Plan Run before archiving")
        values: dict = {
            "lock_version": Plan.lock_version + 1,
            "updated_at": datetime.utcnow(),
        }
        if body.title is not None:
            values["title"] = body.title.strip()
        if body.archived is not None:
            values["archived_at"] = datetime.utcnow() if body.archived else None
        changed = await db.execute(
            update(Plan)
            .where(Plan.id == plan.id, Plan.lock_version == body.expected_lock_version)
            .values(**values)
        )
        if changed.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Plan changed concurrently")
        await db.commit()
        plan = await db.get(Plan, plan_id)
        resource = await plan_resource(db, plan, include_audit=True)
    await broadcast_plan_event(
        event="plan_archived" if plan.archived_at else "plan_restored",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        archived=plan.archived_at is not None,
    )
    return resource


@router.post("/api/plans/{plan_id}/runs", response_model=PlanRunResource, status_code=201)
async def create_run(
    plan_id: int,
    body: PlanRunCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.request)
    uploads = _validated_uploads(body)
    async with plan_operation_lock(plan_id):
        plan = await _require_plan(request, db, plan_id, control=True)
        target = await db.get(Task, plan.target_task_id) if plan.target_task_id is not None else None
        if plan.target_task_id is not None and target is None:
            raise HTTPException(409, "Plan target no longer exists")
        if target is not None and target.status == "migrating":
            raise HTTPException(409, "Plan target is changing execution location")
        if target is not None:
            # Inactive Plan history is Manager-owned. A new Run follows the
            # target's current Worker and rehydrates its exact base Version.
            plan.worker_id = target.worker_id
        source_run = None
        if body.run_type == "retry":
            if not is_admin(request):
                raise HTTPException(403, "Only administrators can retry failed Plan Runs")
            if body.source_run_id is None:
                raise HTTPException(422, "retry requires source_run_id")
            source_run = await db.get(PlanAgentRun, body.source_run_id)
            if (
                source_run is None
                or source_run.plan_id != plan.id
                or source_run.status != "failed"
                or source_run.finished_at is None
            ):
                raise HTTPException(409, "Retry source must be a terminal failed Run")
        elif body.source_run_id is not None:
            raise HTTPException(422, "source_run_id is only valid for retry")
        context = await _capture_context_for_plan(
            db, target=target, target_repo=plan.target_repo, worker_id=plan.worker_id
        )
        run = await create_plan_run(
            db,
            plan=plan,
            run_type=body.run_type,
            request_text=body.request.strip(),
            attachments=uploads,
            base_version_id=body.base_version_id,
            expected_current_version_id=body.expected_current_version_id,
            context_session_id=context[0],
            context_log_id=context[1],
            context_snapshot=context[2],
            repo_revision=context[3],
            source_run_id=source_run.id if source_run is not None else None,
        )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_run_created",
        plan_id=plan_id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        status=run.status,
    )
    return await run_resource(db, run)


@router.post("/api/plans/{plan_id}/fork", response_model=PlanResource, status_code=201)
async def fork_plan(
    plan_id: int,
    body: PlanForkRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.title, body.request)
    source = await _require_plan(request, db, plan_id, control=True)
    version = await db.get(PlanVersion, body.base_version_id)
    if version is None or version.plan_id != source.id:
        raise HTTPException(400, "Fork Version does not belong to this Plan")
    target = await db.get(Task, source.target_task_id) if source.target_task_id is not None else None
    if target is not None and target.status == "migrating":
        raise HTTPException(409, "Plan target is changing execution location")
    fork_worker_id = target.worker_id if target is not None else source.worker_id
    context = await _capture_context_for_plan(
        db, target=target, target_repo=source.target_repo, worker_id=fork_worker_id
    )
    request_text = body.request.strip() if body.request else (
        f"Fork this planning direction from v{version.version_number}.\n\n{version.content}"
    )
    fork, _run = await create_plan_with_run(
        db,
        title=(body.title.strip() if body.title else f"Fork of {source.title}")[:200],
        initial_request=request_text,
        attachments=deepcopy(source.initial_attachments),
        target_task_id=source.target_task_id,
        project_id=source.project_id,
        target_repo=source.target_repo,
        target_branch=source.target_branch,
        worker_id=fork_worker_id,
        priority=source.priority,
        timeout_hours=source.timeout_hours,
        created_by=get_current_user_id(request),
        pipeline_config=deepcopy(source.pipeline_config),
        context_session_id=context[0], context_log_id=context[1],
        context_snapshot=context[2], repo_revision=context[3],
        forked_from_version_id=version.id,
        base_version_id=version.id,
        run_type="fork",
    )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_created",
        plan_id=fork.id,
        target_task_id=fork.target_task_id,
        forked_from_plan_id=source.id,
    )
    return await plan_resource(db, fork, include_audit=True)


@router.get("/api/plans/{plan_id}/versions", response_model=list[PlanVersionResource])
async def list_versions(
    plan_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    await _require_plan(request, db, plan_id)
    rows = (
        await db.execute(
            select(PlanVersion).where(PlanVersion.plan_id == plan_id).order_by(PlanVersion.version_number.desc())
        )
    ).scalars()
    return [await version_resource(db, row) for row in rows]


@router.get("/api/plan-versions/{version_id}", response_model=PlanVersionResource)
async def get_version(
    version_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    _plan, version = await _require_version(request, db, version_id)
    return await version_resource(db, version)


@router.get("/api/plan-versions/{version_id}/staleness")
async def get_version_staleness(
    version_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    plan, version = await _require_version(request, db, version_id)
    return await _version_staleness(db, plan, version)


async def _decide(
    *, version_id: int, body: PlanDecisionRequest, request: Request,
    db: AsyncSession, decision: str,
) -> PlanVersionResource:
    plan, _ = await _require_version(request, db, version_id, control=True)
    async with plan_operation_lock(plan.id):
        plan, version = await _require_version(request, db, version_id, control=True)
        stale = await _version_staleness(db, plan, version)
        if stale["hard_conflict"]:
            raise HTTPException(
                409,
                {"code": "plan_hard_conflict", "message": "Plan Version cannot be decided", **stale},
            )
        if decision == "approved" and stale["stale"] and not body.confirm_stale:
            raise HTTPException(409, {"message": "Plan Version context is stale", **stale})
        version = await decide_version(
            db, plan=plan, version=version, decision=decision,
            decided_by=get_current_user_id(request),
            expected_current_version_id=body.expected_current_version_id,
        )
    await broadcast_plan_event(
        event="plan_version_decided",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        version_id=version.id,
        decision=decision,
    )
    return await version_resource(db, version)


@router.post("/api/plan-versions/{version_id}/approve", response_model=PlanVersionResource)
async def approve_version(
    version_id: int, body: PlanDecisionRequest, request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _decide(
        version_id=version_id, body=body, request=request, db=db, decision="approved"
    )


@router.post("/api/plan-versions/{version_id}/reject", response_model=PlanVersionResource)
async def reject_version(
    version_id: int, body: PlanDecisionRequest, request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _decide(
        version_id=version_id, body=body, request=request, db=db, decision="rejected"
    )


@router.get("/api/plans/{plan_id}/runs", response_model=list[PlanRunResource])
async def list_runs(
    plan_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    await _require_plan(request, db, plan_id)
    rows = (
        await db.execute(
            select(PlanAgentRun).where(PlanAgentRun.plan_id == plan_id).order_by(PlanAgentRun.id.desc())
        )
    ).scalars()
    return [await run_resource(db, row) for row in rows]


@router.get("/api/plan-runs/{run_id}", response_model=PlanRunResource)
async def get_run(
    run_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise HTTPException(404, "Plan Run not found")
    await _require_plan(request, db, run.plan_id)
    return await run_resource(db, run)


@router.post("/api/plan-runs/{run_id}/cancel", response_model=PlanRunResource)
async def cancel_plan_run(
    run_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise HTTPException(404, "Plan Run not found")
    async with plan_operation_lock(run.plan_id):
        plan = await _require_plan(request, db, run.plan_id, control=True)
        run = await db.get(PlanAgentRun, run_id)
        owned_instance_id = run.instance_id
        worker_id = run.worker_id
        if worker_id is not None and run.status in ACTIVE_RUN_STATUSES:
            from backend.main import dispatcher, worker_proxy

            if worker_proxy is None:
                raise HTTPException(503, "Worker Plan runtime is unavailable")
            try:
                # Do not publish a local cancellation until the owning Worker
                # has acknowledged the exact remote Run stop request.
                await worker_proxy.cancel_versioned_plan_run(worker_id, run_id)
                if dispatcher is not None:
                    await dispatcher.stop_plan_run_lifecycle(run_id, None)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(
                    503,
                    f"Worker Plan Run could not be cancelled safely: {exc}",
                ) from exc
        run = await cancel_run(db, plan=plan, run=run)
    try:
        from backend.main import dispatcher
        stopped = (
            await dispatcher.stop_plan_run_lifecycle(run_id, owned_instance_id)
            if dispatcher is not None and worker_id is None
            else worker_id is not None
        )
        if not stopped:
            from backend.services.plan_agent_runner import cancel_plan_run_runtime
            await cancel_plan_run_runtime(run_id)
        if owned_instance_id is not None:
            owner = await db.get(Instance, owned_instance_id, with_for_update=True)
            if owner is not None and owner.current_plan_run_id == run_id:
                if owner.current_task_id is not None or owner.pid is not None:
                    await db.rollback()
                    raise RuntimeError(
                        f"Plan Run #{run_id} Instance owner is not safe to release"
                    )
                owner.current_plan_run_id = None
                owner.status = "idle"
                await db.commit()
    except Exception as exc:
        # Cancellation's generation fence prevents replay, but an unconfirmed
        # native/Instance owner must remain visible as a deployment blocker.
        raise HTTPException(
            409,
            f"Plan Run was cancelled, but runtime cleanup is not confirmed: {exc}",
        ) from exc
    await broadcast_plan_event(
        event="plan_run_status_changed",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        status=run.status,
    )
    return await run_resource(db, run)


@router.post(
    "/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
    response_model=PlanInputRequestResponse,
)
async def answer_plan_input(
    run_id: int,
    request_id: int,
    body: PlanInputAnswerRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise HTTPException(404, "Plan Run not found")
    uploads = _validated_uploads(body)
    if body.attachment_manifest is not None:
        _validate_attachment_manifest(uploads, body.attachment_manifest)
    async with plan_operation_lock(run.plan_id):
        plan = await _require_plan(request, db, run.plan_id, control=True)
        run = await db.get(PlanAgentRun, run_id)
        input_request = await db.get(PlanInputRequest, request_id)
        if input_request is None or input_request.run_id != run.id or input_request.plan_id != plan.id:
            raise HTTPException(404, "Plan InputRequest not found")
        answered = await answer_input_request(
            db,
            plan=plan,
            run=run,
            input_request=input_request,
            expected_generation=body.expected_run_generation,
            idempotency_key=body.idempotency_key,
            answers=body.answers,
            response_text=body.response_text,
            attachments=uploads,
            answered_by=get_current_user_id(request),
        )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_input_answered",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        input_request_id=answered.id,
    )
    return input_request_resource(answered)


@router.post(
    "/api/plan-versions/{version_id}/create-execution-task",
    response_model=PlanExecutionResource,
    status_code=201,
)
async def create_execution_task(
    version_id: int,
    body: PlanExecutionCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    plan, _ = await _require_version(request, db, version_id, control=True)
    async with plan_operation_lock(plan.id):
        plan, version = await _require_version(request, db, version_id, control=True)
        if plan.target_task_id is not None:
            raise HTTPException(400, "Only standalone Plans create execution Tasks")
        if plan.current_version_id != body.expected_current_version_id or version.id != body.expected_current_version_id:
            raise HTTPException(
                409,
                {
                    "code": "plan_version_changed",
                    "message": "Plan current Version changed",
                    "plan_id": plan.id,
                    "current_version_id": plan.current_version_id,
                    "active_run_id": plan.active_run_id,
                },
            )
        stale = await _version_staleness(db, plan, version)
        if stale["hard_conflict"]:
            raise HTTPException(
                409,
                {"code": "plan_hard_conflict", "message": "Execution target is unavailable", **stale},
            )
        if stale["stale"] and not body.confirm_stale:
            raise HTTPException(
                409,
                {"code": "plan_stale", "message": "Plan Version context is stale", **stale},
            )
        if version.human_decision == "pending" and body.approve_if_pending:
            version = await decide_version(
                db,
                plan=plan,
                version=version,
                decision="approved",
                decided_by=get_current_user_id(request),
                expected_current_version_id=body.expected_current_version_id,
            )
        if version.human_decision != "approved":
            raise HTTPException(409, "Plan Version must be approved")
        existing = (
            await db.execute(
                select(PlanApplication).where(PlanApplication.plan_version_id == version.id)
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing.application_type != "execution_task" or existing.execution_task_id is None:
                raise HTTPException(409, "Plan Version was already applied")
            execution_id = existing.execution_task_id
        else:
            execution = Task(
                title=f"Execute {plan.title} · v{version.version_number}"[:200],
                description=(
                    "[Approved implementation plan]\n"
                    "Implement the exact approved Plan Version below.\n\n"
                    f"<plan id=\"{plan.id}\" version=\"{version.version_number}\">\n"
                    f"{version.content}\n</plan>\n\n"
                    f"[Original planning request]\n{plan.initial_request}"
                ),
                status="pending",
                priority=plan.priority,
                project_id=plan.project_id,
                target_repo=plan.target_repo,
                target_branch=plan.target_branch,
                merge_status="pending",
                worker_id=plan.worker_id,
                created_by=get_current_user_id(request),
                mode="auto",
                metadata_={
                    "created_from_plan_id": plan.id,
                    "created_from_plan_version_id": version.id,
                },
            )
            db.add(execution)
            await db.flush()
            db.add(PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="execution_task",
                execution_task_id=execution.id,
                applied_by=get_current_user_id(request),
            ))
            try:
                await db.commit()
            except Exception:
                await db.rollback()
                raise
            execution_id = execution.id
    await _wake_dispatcher()
    refreshed_plan = await db.get(Plan, plan.id)
    refreshed_version = await db.get(PlanVersion, version.id)
    await broadcast_plan_event(
        event="plan_version_applied",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        version_id=version.id,
        execution_task_id=execution_id,
    )
    return PlanExecutionResource(
        plan=await plan_resource(db, refreshed_plan, include_audit=True),
        version=await version_resource(db, refreshed_version),
        execution_task_id=execution_id,
    )
