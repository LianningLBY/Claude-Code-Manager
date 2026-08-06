"""Human-facing endpoints for the generic Capability Core."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    require_task_access,
    require_task_control,
)
from backend.database import get_db
from backend.models.task import Task
from backend.schemas.capability import (
    CapabilityExecutionResource,
    CapabilityInvocationCancel,
    CapabilityInvocationConsume,
    CapabilityInvocationCreate,
    CapabilityInvocationCreateResource,
    CapabilityInvocationResource,
    CapabilityResultResource,
    CodeReviewResultResource,
)
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityDisabledError,
    CapabilityError,
    CapabilityNotFoundError,
    CapabilityUnavailableError,
    CapabilityUnsupportedScopeError,
    CapabilityValidationError,
    active_execution_for,
    cancel_invocation,
    consume_ready_invocation,
    create_human_invocation,
    get_invocation,
    list_task_invocations,
)


router = APIRouter(prefix="/api", tags=["capabilities"])


def _http_error(exc: CapabilityError) -> HTTPException:
    if isinstance(exc, CapabilityNotFoundError):
        return HTTPException(404, str(exc))
    if isinstance(exc, CapabilityConflictError):
        return HTTPException(409, str(exc))
    if isinstance(exc, CapabilityValidationError):
        return HTTPException(422, str(exc))
    if isinstance(exc, CapabilityUnsupportedScopeError):
        return HTTPException(409, str(exc))
    if isinstance(exc, (CapabilityDisabledError, CapabilityUnavailableError)):
        return HTTPException(503, str(exc))
    return HTTPException(500, "Capability operation failed")


async def _resource(
    db: AsyncSession,
    invocation,
) -> CapabilityInvocationResource:
    resource = CapabilityInvocationResource.model_validate(invocation)
    execution = await active_execution_for(db, invocation.id)
    if execution is not None:
        resource.active_execution = CapabilityExecutionResource.model_validate(
            execution
        )
    return resource


async def _exact_completed_execution(db: AsyncSession, invocation):
    """Prove that Core's result tuple names one completed execution."""

    from backend.models.capability import CapabilityExecution

    if (
        invocation.result_kind is None
        or invocation.result_id is None
        or invocation.result_hash is None
    ):
        raise CapabilityConflictError("Capability result is not ready")
    rows = list(
        (
            await db.execute(
                select(CapabilityExecution).where(
                    CapabilityExecution.invocation_id == invocation.id,
                    CapabilityExecution.status == "completed",
                    CapabilityExecution.output_kind == invocation.result_kind,
                    CapabilityExecution.output_id == invocation.result_id,
                    CapabilityExecution.output_hash == invocation.result_hash,
                )
            )
        ).scalars()
    )
    if len(rows) != 1:
        raise CapabilityConflictError(
            "Capability result lost its exact completed execution"
        )
    return rows[0]


async def _result_resource(
    db: AsyncSession,
    invocation,
) -> CapabilityResultResource:
    execution = await _exact_completed_execution(db, invocation)
    result_kind = invocation.result_kind
    result_id = invocation.result_id
    result_hash = invocation.result_hash
    assert result_kind is not None
    assert result_id is not None
    assert result_hash is not None

    if result_kind == "code_review_result":
        from backend.models.code_review import CodeReviewResult, CodeReviewRun

        result = await db.get(CodeReviewResult, result_id)
        run = (
            await db.get(CodeReviewRun, result.run_id)
            if result is not None
            else None
        )
        if (
            result is None
            or run is None
            or result.capability_invocation_id != invocation.id
            or result.capability_execution_id != execution.id
            or result.developer_task_id != invocation.task_id
            or result.result_hash != result_hash
            or run.id != result.run_id
            or run.capability_invocation_id != invocation.id
            or run.capability_execution_id != execution.id
            or run.developer_task_id != invocation.task_id
            or run.reviewer_task_id != result.reviewer_task_id
            or run.subject_hash != result.subject_hash
        ):
            raise CapabilityConflictError(
                "Code Review result identity does not match its Invocation"
            )
        data = CodeReviewResultResource.model_validate(result).model_dump(
            mode="json"
        )
        resource_url = f"/api/capability-invocations/{invocation.id}/result"
    elif result_kind == "plan_version":
        from backend.models.plan import Plan, PlanVersion
        from backend.models.plan_agent import PlanAgentRun
        from backend.services.plan_capability import (
            PLAN_RUN_HANDLE_KIND,
            plan_version_output_hash,
        )
        from backend.services.plan_service import version_resource

        run_id = None
        if (
            execution.handle_kind == PLAN_RUN_HANDLE_KIND
            and execution.handle_id is not None
        ):
            try:
                parsed_run_id = int(execution.handle_id)
            except (TypeError, ValueError):
                pass
            else:
                if parsed_run_id > 0 and str(parsed_run_id) == execution.handle_id:
                    run_id = parsed_run_id
        run = await db.get(PlanAgentRun, run_id) if run_id is not None else None
        version = await db.get(PlanVersion, result_id)
        plan = (
            await db.get(Plan, run.plan_id)
            if run is not None and run.plan_id is not None
            else None
        )
        if (
            invocation.capability_key != "plan"
            or run is None
            or version is None
            or plan is None
            or execution.handle_id != str(run.id)
            or run.capability_execution_id != execution.id
            or run.run_type != "capability"
            or run.plan_id != plan.id
            or run.result_version_id != version.id
            or version.plan_id != plan.id
            or version.produced_by_run_id != run.id
            or plan.target_task_id != invocation.task_id
        ):
            raise CapabilityConflictError(
                "Plan result identity does not match its Invocation"
            )
        if plan_version_output_hash(version) != result_hash:
            raise CapabilityConflictError(
                "Plan result hash does not match its authoritative PlanVersion"
            )
        data = (await version_resource(db, version)).model_dump(mode="json")
        resource_url = f"/api/plan-versions/{version.id}"
    else:
        raise CapabilityConflictError(
            f"Capability result kind {result_kind!r} has no public resource"
        )

    return CapabilityResultResource(
        invocation_id=invocation.id,
        invocation_status=invocation.status,
        kind=result_kind,
        id=result_id,
        hash=result_hash,
        resource_url=resource_url,
        data=data,
    )


@router.post(
    "/tasks/{task_id}/capability-invocations",
    response_model=CapabilityInvocationCreateResource,
)
async def create_capability_invocation(
    task_id: int,
    body: CapabilityInvocationCreate,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    from backend.api.tasks import _require_not_delivery_owned_task

    _require_not_delivery_owned_task(
        task,
        action="given ad-hoc capability invocations",
    )
    try:
        invocation, created = await create_human_invocation(
            db,
            task_id=task_id,
            capability_key=body.capability,
            request_payload=body.request,
            idempotency_key=body.idempotency_key,
            requested_by_user_id=get_current_user_id(request),
        )
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    response.status_code = 201 if created else 200
    return CapabilityInvocationCreateResource(
        invocation=await _resource(db, invocation),
        created=created,
    )


@router.get(
    "/tasks/{task_id}/capability-invocations",
    response_model=list[CapabilityInvocationResource],
)
async def list_capability_invocations(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    invocations = await list_task_invocations(db, task_id)
    return [await _resource(db, invocation) for invocation in invocations]


@router.get(
    "/capability-invocations/{invocation_id}",
    response_model=CapabilityInvocationResource,
)
async def read_capability_invocation(
    invocation_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        invocation = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, invocation.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    return await _resource(db, invocation)


@router.get(
    "/capability-invocations/{invocation_id}/result",
    response_model=CapabilityResultResource,
)
async def read_capability_result(
    invocation_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        invocation = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, invocation.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    try:
        return await _result_resource(db, invocation)
    except CapabilityError as exc:
        raise _http_error(exc) from exc


@router.post(
    "/capability-invocations/{invocation_id}/consume",
    response_model=CapabilityInvocationResource,
)
async def consume_capability_invocation(
    invocation_id: int,
    body: CapabilityInvocationConsume,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        observed = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, observed.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    from backend.api.tasks import _require_not_delivery_owned_task

    _require_not_delivery_owned_task(
        task,
        action="had capability results consumed outside its Delivery Run",
    )
    try:
        invocation = await consume_ready_invocation(
            db,
            invocation_id=invocation_id,
            expected_state_version=body.expected_state_version,
        )
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    return await _resource(db, invocation)


@router.post(
    "/capability-invocations/{invocation_id}/cancel",
    response_model=CapabilityInvocationResource,
)
async def cancel_capability_invocation(
    invocation_id: int,
    body: CapabilityInvocationCancel,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        observed = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, observed.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    from backend.api.tasks import _require_not_delivery_owned_task

    _require_not_delivery_owned_task(
        task,
        action="had capabilities cancelled outside its Delivery Run",
    )
    try:
        invocation = await cancel_invocation(
            db,
            invocation_id=invocation_id,
            expected_state_version=body.expected_state_version,
        )
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    return await _resource(db, invocation)
