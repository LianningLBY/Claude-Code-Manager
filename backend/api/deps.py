"""Shared FastAPI dependencies for user context and resource ownership."""

from fastapi import HTTPException, Request
from sqlalchemy import select, update


MANAGED_SSH_AUTH_REQUIRED_DETAIL = (
    "Managed SSH requires AUTH_TOKEN to be configured"
)


def require_managed_ssh_auth_configured() -> None:
    """Keep Manager-held SSH credentials closed in legacy open mode."""

    from backend.config import settings

    if not settings.auth_token:
        raise HTTPException(503, MANAGED_SSH_AUTH_REQUIRED_DETAIL)


def get_current_user_id(request: Request) -> int | None:
    return getattr(request.state, "user_id", None)


def get_current_user_role(request: Request) -> str:
    return getattr(request.state, "user_role", "member")


def is_admin(request: Request) -> bool:
    """Both admin and super_admin have admin-level permissions."""
    return get_current_user_role(request) in ("admin", "super_admin")


def is_super_admin(request: Request) -> bool:
    """Only super_admin can promote users to admin."""
    return get_current_user_role(request) == "super_admin"


def require_admin(request: Request):
    """Raise 403 if not admin/super_admin."""
    # Scoped child credentials have already been restricted to an exact
    # method/path by authentication middleware.  Let those callbacks traverse
    # routers that are otherwise admin-only; this does not grant access to any
    # additional route.
    if getattr(request.state, "auth_type", None) == "internal_service":
        return
    if not is_admin(request):
        raise HTTPException(403, "Admin only")


def _require_forwarded_task_incarnation(request: Request, task) -> None:
    expected = getattr(request, "headers", {}).get(
        "x-ccm-task-incarnation"
    )
    if expected is None:
        return
    if not expected or expected != getattr(task, "incarnation_id", None):
        raise HTTPException(409, "Worker Task incarnation changed")


def require_internal_service(request: Request) -> None:
    """Allow scoped CCM callbacks (and the legacy deployment credential).

    Auth-disabled deployments intentionally retain their historical open
    semantics. New child processes receive an exact-route credential labelled
    ``internal_service``; ``token`` remains for deployment/Worker compatibility.
    """
    from backend.config import settings

    if settings.auth_token and getattr(request.state, "auth_type", None) not in {
        "token",
        "internal_service",
    }:
        raise HTTPException(403, "Internal service authentication required")


def _internal_task_access_allowed(request: Request, task) -> bool:
    if getattr(request.state, "auth_type", None) != "internal_service":
        return False
    from backend.services.internal_service_auth import (
        internal_task_id,
    )

    claims = getattr(request.state, "internal_service_claims", None)
    return bool(
        internal_task_id(claims) == task.id
        and getattr(claims, "task_incarnation_id", None)
        == getattr(task, "incarnation_id", None)
    )


def internal_task_incarnation_id(
    request: Request,
    task_id: int,
) -> str | None:
    """Return the exact Task incarnation carried by a scoped callback."""

    if getattr(request.state, "auth_type", None) != "internal_service":
        return None
    claims = getattr(request.state, "internal_service_claims", None)
    incarnation_id = getattr(claims, "task_incarnation_id", None)
    if (
        getattr(claims, "task_id", None) != task_id
        or not incarnation_id
    ):
        raise HTTPException(403, "Internal service Task identity mismatch")
    return incarnation_id


async def require_internal_task_incarnation(
    request: Request,
    task_id: int,
    db,
    *,
    write_fence: bool = False,
):
    """Revalidate a scoped callback in the endpoint's own transaction.

    Middleware rejection is an early filter, not an authorization commit:
    another process can delete/import/reuse an integer Task id between the
    middleware session and the route session. Lock the exact incarnation in
    the transaction that reads or mutates the callback owner.
    """

    if getattr(request.state, "auth_type", None) != "internal_service":
        return None
    from backend.models.task import Task

    incarnation_id = internal_task_incarnation_id(request, task_id)
    assert incarnation_id is not None
    claims = getattr(request.state, "internal_service_claims", None)
    retry_count = getattr(claims, "task_retry_count", None)
    turn_generation = getattr(claims, "task_turn_generation", None)
    task_status = getattr(claims, "task_status", None)
    generation_values = (retry_count, turn_generation, task_status)
    if any(value is not None for value in generation_values) and any(
        value is None for value in generation_values
    ):
        raise HTTPException(403, "Internal service Task generation is invalid")
    identity_predicates = [
        Task.id == task_id,
        Task.incarnation_id == incarnation_id,
    ]
    if retry_count is not None:
        identity_predicates.extend((
            Task.retry_count == retry_count,
            Task.turn_generation == turn_generation,
            Task.status == task_status,
        ))
    stale_detail = (
        "Internal service SSH Task generation is stale"
        if getattr(claims, "audience", None) == "ccm_ssh"
        else (
            "Internal service Task generation is stale"
            if retry_count is not None
            else "Internal service Task incarnation is stale"
        )
    )
    if write_fence:
        # ``FOR UPDATE`` is ignored by SQLite. A no-op exact-identity UPDATE is
        # the portable writer barrier: delete/import/retry/next-turn/status
        # transition either wins before it (and this callback rejects) or
        # waits until the callback transaction has committed/rolled back.
        fenced = await db.execute(
            update(Task)
            .where(*identity_predicates)
            .values(status=Task.status)
        )
        if fenced.rowcount != 1:
            raise HTTPException(403, stale_detail)
        task = await db.get(Task, task_id, populate_existing=True)
    else:
        task = await db.scalar(
            select(Task)
            .where(*identity_predicates)
            .with_for_update()
        )
    if task is None:
        raise HTTPException(403, stale_detail)
    return task


def _member_group_ids(user_id: int):
    from backend.models.user_group import UserGroupMember

    return select(UserGroupMember.group_id).where(
        UserGroupMember.user_id == user_id
    )


async def has_worker_access(
    request: Request,
    worker_id: int | None,
    db,
) -> bool:
    """Return whether the current identity may target one exact Worker.

    ``None`` means execution on the Manager itself and is therefore
    administrator-only.  Project access is handled separately: a member may
    still create work for a shared *local* Project, but cannot target the
    Manager for an unrelated task.
    """
    if is_admin(request):
        return True
    if worker_id is None:
        return False
    user_id = get_current_user_id(request)
    if not user_id:
        return False
    from backend.models.worker import Worker

    worker = await db.get(Worker, worker_id)
    return bool(worker and worker.owner_user_id == user_id)


async def require_worker_target_access(
    request: Request,
    worker_id: int | None,
    db,
) -> None:
    if worker_id is not None:
        from backend.models.worker import Worker

        if await db.get(Worker, worker_id) is None:
            raise HTTPException(404, "Worker not found")
    if not await has_worker_access(request, worker_id, db):
        raise HTTPException(403, "No access to target Worker")


async def has_project_access(
    request: Request,
    project_id: int,
    db,
) -> bool:
    """Return whether the current identity may access one exact Project."""
    if is_admin(request):
        return True
    user_id = get_current_user_id(request)
    if not user_id:
        return False

    from backend.models.project import Project
    from backend.models.team_share import TeamProjectShare
    from backend.models.worker import Worker

    project = await db.get(Project, project_id)
    if project is None:
        return False
    if project.worker_id is not None:
        worker = await db.get(Worker, project.worker_id)
        if worker and worker.owner_user_id == user_id:
            return True

    user_group_ids = _member_group_ids(user_id)
    shared = (
        await db.execute(
            select(TeamProjectShare.id)
            .where(
                TeamProjectShare.project_id == project_id,
                (
                    (TeamProjectShare.target_type == "user")
                    & (TeamProjectShare.target_id == user_id)
                )
                | (
                    (TeamProjectShare.target_type == "group")
                    & TeamProjectShare.target_id.in_(user_group_ids)
                ),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return shared is not None


async def require_project_access(
    request: Request,
    project_id: int,
    db,
) -> None:
    if not await has_project_access(request, project_id, db):
        raise HTTPException(403, "No access to this project")


async def _task_access_allowed(
    request: Request,
    task,
    db,
    *,
    allow_chat_share: bool,
) -> bool:
    if _internal_task_access_allowed(request, task):
        return True
    if is_admin(request):
        return True
    user_id = get_current_user_id(request)
    if not user_id:
        return False
    if task.created_by == user_id:
        return True
    if task.worker_id is not None and await has_worker_access(
        request,
        task.worker_id,
        db,
    ):
        return True
    if task.project_id and await has_project_access(
        request,
        task.project_id,
        db,
    ):
        return True
    if not allow_chat_share:
        return False

    from backend.models.team_share import TeamTaskShare

    user_group_ids = _member_group_ids(user_id)
    shared = (
        await db.execute(
            select(TeamTaskShare.id)
            .where(
                TeamTaskShare.task_id == task.id,
                TeamTaskShare.permission == "chat",
                (
                    (TeamTaskShare.target_type == "user")
                    & (TeamTaskShare.target_id == user_id)
                )
                | (
                    (TeamTaskShare.target_type == "group")
                    & TeamTaskShare.target_id.in_(user_group_ids)
                ),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return shared is not None


async def require_task_access(request: Request, task, db):
    """Allow task owners/project collaborators and chat-only recipients."""
    _require_forwarded_task_incarnation(request, task)
    if not await _task_access_allowed(
        request,
        task,
        db,
        allow_chat_share=True,
    ):
        raise HTTPException(403, "No access to this task")


async def require_task_control(request: Request, task, db):
    """Require ownership/collaboration rights, excluding chat-only shares."""
    _require_forwarded_task_incarnation(request, task)
    if not await _task_access_allowed(
        request,
        task,
        db,
        allow_chat_share=False,
    ):
        raise HTTPException(403, "No permission to control this task")


async def require_worker_access(request: Request, worker):
    """Raise 403 if user has no access to this worker."""
    if is_admin(request):
        return
    user_id = get_current_user_id(request)
    if worker.owner_user_id == user_id:
        return
    raise HTTPException(403, "No access to this worker")
