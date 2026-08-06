from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    require_admin,
    require_internal_service,
    require_task_access,
    require_task_control,
)
from backend.database import get_db
from backend.models.task import Task
from backend.schemas.task_ssh_grant import (
    TaskSSHExecuteRequest,
    TaskSSHExecuteResponse,
    TaskSSHGrantReplace,
    TaskSSHGrantResponse,
)
from backend.services.ssh_profiles import executor_for_profile
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    replace_task_ssh_grants,
    resolve_task_ssh_profile,
    task_ssh_grant_snapshots,
)


router = APIRouter(prefix="/api/tasks/{task_id}", tags=["task-ssh"])


def _access_error(exc: TaskSSHAccessError) -> HTTPException:
    return HTTPException(exc.status_code, exc.detail)


async def _task_or_404(db: AsyncSession, task_id: int) -> Task:
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    return task


@router.get("/ssh-grants", response_model=list[TaskSSHGrantResponse])
async def list_task_ssh_grants(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await _task_or_404(db, task_id)
    await require_task_access(request, task, db)
    return await task_ssh_grant_snapshots(db, task)


@router.put("/ssh-grants", response_model=list[TaskSSHGrantResponse])
async def update_task_ssh_grants(
    task_id: int,
    body: TaskSSHGrantReplace,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    task = await _task_or_404(db, task_id)
    await require_task_control(request, task, db)
    try:
        return await replace_task_ssh_grants(
            db,
            task,
            body.grants,
            created_by=get_current_user_id(request),
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc


@router.get("/ssh-access", response_model=list[TaskSSHGrantResponse])
async def internal_task_ssh_access(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    task = await _task_or_404(db, task_id)
    return await task_ssh_grant_snapshots(db, task)


@router.post(
    "/ssh-access/{profile_id}/execute",
    response_model=TaskSSHExecuteResponse,
)
async def internal_task_ssh_execute(
    task_id: int,
    profile_id: int,
    body: TaskSSHExecuteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    try:
        profile = await resolve_task_ssh_profile(
            db,
            task_id=task_id,
            profile_id=profile_id,
            required_capability="exec",
        )
        result = await executor_for_profile(profile).run_result(
            body.command,
            timeout=body.timeout_seconds,
            max_output_bytes=body.max_output_bytes,
            sensitive=True,
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc
    except TimeoutError as exc:
        raise HTTPException(504, "SSH command timed out") from exc
    except Exception as exc:
        # Managed profile endpoints intentionally never reflect credential
        # paths, Paramiko messages, or command contents to Task callers.
        raise HTTPException(400, "SSH command failed to start") from exc
    return TaskSSHExecuteResponse(
        exit_code=result.exit_code,
        stdout=result.stdout,
        stderr=result.stderr,
        truncated=result.truncated,
        duration_ms=result.duration_ms,
    )
