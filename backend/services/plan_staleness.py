"""One authoritative freshness and hard-conflict check for Plan Versions."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.plan import Plan, PlanVersion
from backend.models.project import Project
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.plan_tasks import capture_repo_revision, latest_task_log_id


async def version_staleness(
    db: AsyncSession,
    plan: Plan,
    version: PlanVersion,
) -> dict:
    """Return confirmable staleness separately from non-bypassable conflicts."""

    reasons: list[str] = []
    hard_conflicts: list[str] = []
    current_log_id = None
    current_session_id = None
    current_repo = None
    target = None

    if version.plan_id != plan.id:
        hard_conflicts.append("version_plan_mismatch")

    if plan.target_task_id is not None:
        target = await db.get(Task, plan.target_task_id)
        if target is None:
            hard_conflicts.append("target_task_missing")
        else:
            current_log_id = await latest_task_log_id(db, target.id)
            current_session_id = target.session_id
            if current_session_id != version.context_session_id:
                reasons.append("session_changed")
            if (current_log_id or 0) > (version.context_log_id or 0):
                reasons.append("conversation_advanced")

    project = (
        await db.get(Project, plan.project_id)
        if plan.project_id is not None
        else None
    )
    if plan.project_id is not None and project is None:
        hard_conflicts.append("project_missing")

    effective_worker_id = target.worker_id if target is not None else plan.worker_id
    if effective_worker_id is not None:
        worker = await db.get(Worker, effective_worker_id)
        if worker is None:
            hard_conflicts.append("worker_missing")
        elif worker.status != "ready":
            hard_conflicts.append("worker_unavailable")
        elif "project_missing" not in hard_conflicts:
            try:
                from backend.main import worker_proxy

                if worker_proxy is None:
                    raise RuntimeError("Worker proxy is disabled")
                current_repo = await worker_proxy.get_plan_repo_revision(
                    worker=worker,
                    manager_project_id=plan.project_id,
                    target_task_id=plan.target_task_id,
                )
            except Exception:
                hard_conflicts.append("worker_repo_unavailable")
    else:
        path = None
        if target is not None:
            path = target.last_cwd or target.target_repo
        elif project is not None:
            path = project.local_path
        else:
            path = plan.target_repo
        current_repo = await capture_repo_revision(path)

    if current_repo is not None and not isinstance(current_repo, dict):
        hard_conflicts.append("repository_fingerprint_invalid")
        current_repo = None
    if current_repo is None:
        hard_conflicts.append("repository_unavailable")
    elif version.repo_revision is None:
        hard_conflicts.append("captured_repository_state_missing")
    elif current_repo != version.repo_revision:
        if current_repo.get("available") is not True:
            hard_conflicts.append("repository_unavailable")
        else:
            reasons.append("repository_changed")

    # Preserve stable ordering while avoiding duplicate diagnostic codes.
    reasons = list(dict.fromkeys(reasons))
    hard_conflicts = list(dict.fromkeys(hard_conflicts))
    return {
        "stale": bool(reasons),
        "reasons": reasons,
        "hard_conflict": bool(hard_conflicts),
        "hard_conflicts": hard_conflicts,
        "can_confirm": not hard_conflicts,
        "captured_session_id": version.context_session_id,
        "current_session_id": current_session_id,
        "captured_log_id": version.context_log_id,
        "current_log_id": current_log_id,
        "captured_repo_revision": version.repo_revision,
        "current_repo_revision": current_repo,
    }
