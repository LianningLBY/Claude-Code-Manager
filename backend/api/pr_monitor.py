import asyncio
import hashlib
import hmac
import logging
import re
import secrets
from datetime import datetime
from weakref import WeakKeyDictionary

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy import (
    and_,
    delete as sa_delete,
    desc,
    func,
    or_,
    select,
    update as sa_update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRFindingRebuttal,
    PRMergeQueueAction,
    PRReview,
    PRReviewerRun,
    PRMonitorRun,
    PRRepairWake,
)
from backend.models.task import Task
from backend.services.pr_review_actions import (
    FindingActionConflict,
    lock_pr_repo_action_boundary,
)
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    has_worker_access,
    require_project_access,
    require_worker_target_access,
    require_task_control,
)
from backend.schemas.pr_monitor import (
    MonitoredRepoCreate,
    MonitoredRepoUpdate,
    MonitoredRepoResponse,
    MonitoredRepoSecretResponse,
    PRReviewResponse,
    PRReviewDetailResponse,
    PRReviewerRunResponse,
    PRFindingResponse,
    PRFindingActionResponse,
    FindingActionRequest,
    HumanAdviceRequest,
    ConfirmFixRequest,
    PRFindingRebuttalCreate,
    PRFindingRebuttalResponse,
    PRMonitorBindRequest,
    PRMonitorRunResponse,
    PRRepairWakeResponse,
    PRMergeQueueActionResponse,
)

logger = logging.getLogger(__name__)

_GH_LOGIN_CACHE: str | None = None
_GIT_COMMIT_SHA_RE = re.compile(r"[0-9a-fA-F]{40}\Z")
_PR_SYNCHRONIZE_LOCKS: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = WeakKeyDictionary()


def _pr_repo_write_lock(repo_id: int) -> asyncio.Lock:
    """Serialize one monitor's webhook/delete barrier in this process."""

    loop = asyncio.get_running_loop()
    locks = _PR_SYNCHRONIZE_LOCKS.setdefault(loop, {})
    return locks.setdefault(repo_id, asyncio.Lock())


def _action_response_payload(action: PRFindingAction) -> dict:
    """Expose repair metadata without leaking the stored patch or nonce."""

    payload = PRFindingActionResponse.model_validate(action).model_dump()
    raw_result = dict(action.result or {})
    payload["result"] = {
        key: value
        for key, value in raw_result.items()
        if key not in {
            "patch",
            "confirmation_token",
            "action_nonce",
            "push_owner_token",
        }
    } or None
    if (
        action.status == "awaiting_confirmation"
        and action.confirmed_at is None
        and action.task_id is not None
    ):
        payload["diff_download_url"] = f"/api/pr-monitor/actions/{action.id}/diff"
    return payload


def _parse_commit_sha(value: object, field_name: str) -> str:
    """Return a canonical webhook commit SHA or reject the signed payload."""
    if not isinstance(value, str) or _GIT_COMMIT_SHA_RE.fullmatch(value) is None:
        raise HTTPException(
            400,
            f"pull_request.{field_name}.sha must be exactly 40 hexadecimal characters",
        )
    return value.lower()


def _gh_login() -> str:
    """本机 gh CLI 登录的用户名（缓存；未登录返回空串）。"""
    global _GH_LOGIN_CACHE
    if _GH_LOGIN_CACHE is None:
        import subprocess
        try:
            r = subprocess.run(
                ["gh", "api", "user", "-q", ".login"],
                capture_output=True, text=True, timeout=10,
            )
            _GH_LOGIN_CACHE = r.stdout.strip() if r.returncode == 0 else ""
        except Exception:
            _GH_LOGIN_CACHE = ""
    return _GH_LOGIN_CACHE


def _require_current_webhook_signature(
    repo: MonitoredRepo,
    *,
    body: bytes,
    signature_header: str,
) -> None:
    """Verify the delivery against the exact locked monitor generation."""

    if not signature_header.startswith("sha256="):
        raise HTTPException(403, "Missing or invalid signature")
    expected_sig = "sha256=" + hmac.new(
        repo.webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature_header, expected_sig):
        raise HTTPException(403, "Invalid signature")


def _webhook_policy_rejection(
    repo: MonitoredRepo,
    *,
    base_branch: str,
    pr_author: str,
) -> str | None:
    """Return why a signed PR is outside the monitor's current policy."""

    if base_branch != repo.default_branch:
        return f"target branch: {base_branch}"
    allowed = repo.allowed_authors or []
    if allowed and pr_author not in allowed:
        return f"author not allowed: {pr_author}"
    own_login = _gh_login()
    if own_login and pr_author == own_login and pr_author not in allowed:
        return f"self PR (gh login: {own_login})"
    return None


router = APIRouter(prefix="/api/pr-monitor", tags=["pr-monitor"])
webhook_router = APIRouter(prefix="/api/github", tags=["pr-monitor"])


async def _find_processed_review(
    db: AsyncSession,
    repo_id: int,
    pr_number: int,
    base_sha: str,
    head_sha: str,
    delivery_id: str | None,
) -> PRReview | None:
    """Find an existing review for this snapshot or exact webhook delivery."""
    duplicate_keys = [
        and_(
            PRReview.repo_id == repo_id,
            PRReview.pr_number == pr_number,
            PRReview.base_sha == base_sha,
            PRReview.head_sha == head_sha,
        )
    ]
    if delivery_id:
        duplicate_keys.append(
            and_(
                PRReview.repo_id == repo_id,
                PRReview.delivery_id == delivery_id,
            )
        )

    result = await db.execute(
        select(PRReview)
        .where(or_(*duplicate_keys))
        .order_by(desc(PRReview.id))
        .limit(1)
    )
    return result.scalar_one_or_none()


def _duplicate_review_response(
    review: PRReview,
    delivery_id: str | None,
) -> dict:
    same_delivery = bool(delivery_id and review.delivery_id == delivery_id)
    return {
        "status": "ignored",
        "reason": (
            "webhook delivery already processed"
            if same_delivery
            else "PR snapshot already reviewed"
        ),
        "review_id": review.id,
    }


@router.get("/webhook-info")
async def webhook_info():
    """Return the public webhook URL (from PUBLIC_BASE_URL), or null if unset."""
    base = settings.public_base_url.strip().rstrip("/")
    return {"webhook_url": f"{base}/api/github/webhook" if base else None}


@router.get("/repos", response_model=list[MonitoredRepoResponse])
async def list_repos(request: Request, db: AsyncSession = Depends(get_db)):
    user_role = get_current_user_role(request)
    user_id = get_current_user_id(request)
    stmt = select(MonitoredRepo).order_by(desc(MonitoredRepo.created_at))
    if user_role not in ("admin", "super_admin"):
        from backend.models.worker import Worker
        owned_worker_ids = select(Worker.id).where(Worker.owner_user_id == user_id)
        stmt = stmt.where(MonitoredRepo.worker_id.in_(owned_worker_ids))
    result = await db.execute(stmt)
    return result.scalars().all()


async def _require_pr_monitor_access(
    request: Request,
    db: AsyncSession,
    repo: MonitoredRepo,
) -> None:
    """Require ownership of this repo's exact execution Worker."""
    if not await has_worker_access(request, repo.worker_id, db):
        raise HTTPException(403, "No access to this PR monitor")


_ACTIVE_REVIEW_STATUSES = (
    "pending",
    "waiting_ci",
    "reviewing",
    "publishing",
    "superseding",
)
_ACTIVE_ADJUDICATION_STATUSES = ("pending", "adjudicating", "accepted")
_ACTIVE_FINDING_ACTION_STATUSES = (
    "pending",
    "running",
    "awaiting_confirmation",
    "cancelling",
)
_STARTED_REPAIR_STATUSES = (
    "delivering",
    "accepted",
    "awaiting_push",
    "running",  # compatibility with rows written by older deployments
)
_STARTED_MERGE_QUEUE_STATUSES = ("enqueuing", "queued", "checking")
_EXTERNALLY_BUSY_RUN_STATUSES = ("resolving_fixed_threads", "repair_migrating")


async def _active_finding_action_for_repo(
    db: AsyncSession,
    repo_id: int,
) -> int | None:
    """Lock and return any action that still owns a Finding side effect."""

    return (await db.execute(
        select(PRFindingAction.id)
        .join(PRFinding, PRFinding.id == PRFindingAction.finding_id)
        .join(PRReview, PRReview.id == PRFinding.pr_review_id)
        .where(
            PRReview.repo_id == repo_id,
            or_(
                PRFindingAction.active_fix_finding_id.is_not(None),
                PRFindingAction.status.in_(_ACTIVE_FINDING_ACTION_STATUSES),
            ),
        )
        .order_by(PRFindingAction.id.asc())
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()


async def _quiesce_monitor_runs(
    db: AsyncSession,
    *,
    repo_id: int,
    run_id: int | None = None,
    reason: str,
) -> None:
    """Atomically stop undispatched effects or reject an in-flight effect.

    The caller must hold the repository row lock.  Pending Repair and Merge
    Queue rows have not crossed an external-effect boundary and can therefore
    be withdrawn.  Once a Task/publication/queue operation has started we
    fail closed instead of presenting a pause/disable control that did not
    actually stop work.
    """

    run_filter = (
        PRMonitorRun.id == run_id
        if run_id is not None
        else PRMonitorRun.repo_id == repo_id
    )
    runs = list((await db.execute(
        select(PRMonitorRun).where(run_filter).with_for_update()
    )).scalars())
    if run_id is not None and not runs:
        raise HTTPException(404, "PR Monitor Run not found")
    if run_id is not None and (
        runs[0].status in {"merged", "closed"} or runs[0].completed_at is not None
    ):
        raise HTTPException(409, "Cannot pause a terminal PR Monitor Run")
    run_ids = [item.id for item in runs]

    review_filter = PRReview.repo_id == repo_id
    if run_id is not None:
        current_review_ids = [
            item.current_review_id for item in runs if item.current_review_id is not None
        ]
        review_filter = or_(
            PRReview.monitor_run_id == run_id,
            PRReview.id.in_(current_review_ids) if current_review_ids else False,
        )
    active_review = (await db.execute(
        select(PRReview.id)
        .where(review_filter, PRReview.status.in_(_ACTIVE_REVIEW_STATUSES))
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()

    active_fix_action = await _active_finding_action_for_repo(db, repo_id)
    active_adjudication = active_repair = active_merge = None
    if run_ids:
        active_adjudication = (await db.execute(
            select(PRFindingRebuttal.id)
            .where(
                PRFindingRebuttal.monitor_run_id.in_(run_ids),
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        active_repair = (await db.execute(
            select(PRRepairWake.id)
            .where(
                PRRepairWake.monitor_run_id.in_(run_ids),
                PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        active_merge = (await db.execute(
            select(PRMergeQueueAction.id)
            .where(
                PRMergeQueueAction.monitor_run_id.in_(run_ids),
                PRMergeQueueAction.status.in_(_STARTED_MERGE_QUEUE_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()

    busy_run = next(
        (item for item in runs if item.status in _EXTERNALLY_BUSY_RUN_STATUSES),
        None,
    )
    if any((
        active_review,
        active_fix_action,
        active_adjudication,
        active_repair,
        active_merge,
        busy_run,
    )):
        raise HTTPException(
            409,
            "Cannot pause PR Monitor while review, Finding repair, "
            "adjudication, Repair, thread resolution, or Merge Queue work is active",
        )

    if run_ids:
        await db.execute(
            sa_update(PRRepairWake)
            .where(
                PRRepairWake.monitor_run_id.in_(run_ids),
                PRRepairWake.status == "pending",
            )
            .values(status="shadow", last_error=reason)
        )
        await db.execute(
            sa_update(PRMergeQueueAction)
            .where(
                PRMergeQueueAction.monitor_run_id.in_(run_ids),
                PRMergeQueueAction.status == "pending",
            )
            .values(status="paused", last_error=reason)
        )
    for run in runs:
        if run.status not in {"merged", "closed"}:
            run.status = "paused"
            run.pause_reason = reason
            run.state_version += 1


async def _withdraw_pending_repairs(db: AsyncSession, *, repo_id: int) -> None:
    """Withdraw automatic Repair work before disabling that policy."""

    runs = list((await db.execute(
        select(PRMonitorRun)
        .where(PRMonitorRun.repo_id == repo_id)
        .with_for_update()
    )).scalars())
    run_ids = [item.id for item in runs]
    if not run_ids:
        return
    active = (await db.execute(
        select(PRRepairWake.id)
        .where(
            PRRepairWake.monitor_run_id.in_(run_ids),
            PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
        )
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()
    if active is not None or any(
        item.status == "repair_migrating" for item in runs
    ):
        raise HTTPException(409, "Cannot disable automatic Repair while Repair work is active")
    await db.execute(
        sa_update(PRRepairWake)
        .where(
            PRRepairWake.monitor_run_id.in_(run_ids),
            PRRepairWake.status == "pending",
        )
        .values(status="shadow", last_error="auto_repair_disabled")
    )
    for run in runs:
        if run.status == "repair_pending":
            run.status = "waiting_for_fix"
            run.state_version += 1


async def _withdraw_pending_merge_actions(db: AsyncSession, *, repo_id: int) -> None:
    """Withdraw automatic queue admission before changing queue policy."""

    runs = list((await db.execute(
        select(PRMonitorRun)
        .where(PRMonitorRun.repo_id == repo_id)
        .with_for_update()
    )).scalars())
    run_ids = [item.id for item in runs]
    if not run_ids:
        return
    active = (await db.execute(
        select(PRMergeQueueAction.id)
        .where(
            PRMergeQueueAction.monitor_run_id.in_(run_ids),
            PRMergeQueueAction.status.in_(_STARTED_MERGE_QUEUE_STATUSES),
        )
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()
    if active is not None:
        raise HTTPException(409, "Cannot change Merge Queue policy while queue work is active")
    await db.execute(
        sa_update(PRMergeQueueAction)
        .where(
            PRMergeQueueAction.monitor_run_id.in_(run_ids),
            PRMergeQueueAction.status == "pending",
        )
        .values(status="shadow", last_error="merge_queue_policy_changed")
    )
    for run in runs:
        if run.status == "merge_queue_pending":
            run.status = "ready_to_merge"
            run.state_version += 1


@router.post("/repos", response_model=MonitoredRepoSecretResponse)
async def create_repo(request: Request, body: MonitoredRepoCreate, db: AsyncSession = Depends(get_db)):
    if body.review_mode == "single" and body.wait_for_ci:
        raise HTTPException(400, "wait_for_ci requires review_mode=panel")
    if body.review_mode == "single" and body.auto_repair:
        raise HTTPException(400, "auto_repair requires review_mode=panel")
    if body.wait_for_ci and not body.required_checks:
        raise HTTPException(400, "wait_for_ci requires at least one required check")
    if body.auto_merge and body.review_mode != "single":
        raise HTTPException(400, "legacy auto_merge requires review_mode=single")
    if body.auto_merge and body.merge_queue_mode != "manual":
        raise HTTPException(400, "legacy auto_merge and Merge Queue are mutually exclusive")
    if body.merge_queue_mode != "manual" and (
        body.review_mode != "panel" or not body.wait_for_ci
    ):
        raise HTTPException(400, "Merge Queue requires panel review and exact-head CI")
    worker_id = body.worker_id
    await require_worker_target_access(request, worker_id, db)
    if body.project_id is not None:
        from backend.models.project import Project

        project = await db.get(Project, body.project_id)
        if project is None:
            raise HTTPException(404, "Project not found")
        await require_project_access(request, project.id, db)
        if project.worker_id != worker_id:
            raise HTTPException(
                400,
                "PR monitor Worker must match the selected Project location",
            )

    # Authorize the exact target first so the global uniqueness check cannot
    # be used by another Worker owner to enumerate monitored repositories.
    existing = await db.execute(
        select(MonitoredRepo).where(MonitoredRepo.repo_full_name == body.repo_full_name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(409, f"Repository '{body.repo_full_name}' already monitored")

    repo = MonitoredRepo(
        repo_full_name=body.repo_full_name,
        project_id=body.project_id,
        worker_id=worker_id,
        auto_merge=body.auto_merge,
        provider=body.provider,
        review_model=body.review_model,
        review_effort=body.review_effort,
        review_mode=body.review_mode,
        wait_for_ci=body.wait_for_ci,
        required_checks=[item.model_dump() for item in body.required_checks],
        auto_repair=body.auto_repair,
        max_repair_attempts=body.max_repair_attempts,
        merge_queue_mode=body.merge_queue_mode,
        default_branch=body.default_branch,
        allowed_authors=body.allowed_authors,
        webhook_secret=secrets.token_hex(32),
    )
    db.add(repo)
    await db.commit()
    await db.refresh(repo)
    return repo


@router.get("/repos/{repo_id}", response_model=MonitoredRepoResponse)
async def get_repo(
    repo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    return repo


@router.put("/repos/{repo_id}", response_model=MonitoredRepoResponse)
async def update_repo(
    repo_id: int,
    body: MonitoredRepoUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)

    update_data = body.model_dump(exclude_unset=True)
    if "required_checks" in update_data:
        update_data["required_checks"] = [
            item.model_dump() if hasattr(item, "model_dump") else item
            for item in update_data["required_checks"]
        ]
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _require_pr_monitor_access(request, db, repo)

        effective_mode = update_data.get("review_mode", repo.review_mode)
        effective_wait = update_data.get("wait_for_ci", repo.wait_for_ci)
        effective_auto_repair = update_data.get("auto_repair", repo.auto_repair)
        effective_checks = update_data.get("required_checks", repo.required_checks or [])
        if effective_mode == "single" and effective_wait:
            raise HTTPException(400, "wait_for_ci requires review_mode=panel")
        if effective_mode == "single" and effective_auto_repair:
            raise HTTPException(400, "auto_repair requires review_mode=panel")
        if effective_wait and not effective_checks:
            raise HTTPException(400, "wait_for_ci requires at least one required check")
        effective_auto_merge = update_data.get("auto_merge", repo.auto_merge)
        effective_merge_queue = update_data.get("merge_queue_mode", repo.merge_queue_mode)
        if effective_auto_merge and effective_mode != "single":
            raise HTTPException(400, "legacy auto_merge requires review_mode=single")
        if effective_auto_merge and effective_merge_queue != "manual":
            raise HTTPException(400, "legacy auto_merge and Merge Queue are mutually exclusive")
        if effective_merge_queue != "manual" and (
            effective_mode != "panel" or not effective_wait
        ):
            raise HTTPException(400, "Merge Queue requires panel review and exact-head CI")

        frozen_review_policy = {
            "review_mode",
            "wait_for_ci",
            "required_checks",
            "provider",
            "review_model",
            "review_effort",
            "auto_merge",
            "default_branch",
            "project_id",
        }
        changed_review_policy = {
            key
            for key in frozen_review_policy & update_data.keys()
            if update_data[key] != getattr(repo, key)
        }
        if changed_review_policy:
            active_review = (await db.execute(
                select(PRReview.id)
                .where(
                    PRReview.repo_id == repo_id,
                    PRReview.status.in_(_ACTIVE_REVIEW_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            active_run = (await db.execute(
                select(PRMonitorRun.id)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            if active_review is not None or active_run is not None:
                raise HTTPException(
                    409,
                    "Review policy is frozen for the lifetime of an active PR Monitor Run",
                )

        disabling = update_data.get("enabled") is False and repo.enabled
        if disabling:
            await _quiesce_monitor_runs(
                db,
                repo_id=repo_id,
                reason="repo_disabled",
            )
        else:
            if update_data.get("auto_repair") is False and repo.auto_repair:
                await _withdraw_pending_repairs(db, repo_id=repo_id)
            if (
                "merge_queue_mode" in update_data
                and effective_merge_queue != repo.merge_queue_mode
            ) or "required_checks" in changed_review_policy:
                await _withdraw_pending_merge_actions(db, repo_id=repo_id)

        project_id = update_data.get("project_id")
        if project_id is not None:
            from backend.models.project import Project

            project = await db.get(Project, project_id)
            if project is None:
                raise HTTPException(404, "Project not found")
            await require_project_access(request, project_id, db)
            if project.worker_id != repo.worker_id:
                raise HTTPException(
                    400,
                    "PR monitor Worker must match the selected Project location",
                )
        for key, value in update_data.items():
            setattr(repo, key, value)

        await db.commit()
        await db.refresh(repo)
        return repo


@router.delete("/repos/{repo_id}")
async def delete_repo(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)

    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _require_pr_monitor_access(request, db, locked_repo)
        active_fix_action = await _active_finding_action_for_repo(db, repo_id)
        if active_fix_action is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Cannot delete a PR monitor while a Finding repair action is active",
            )
        reviews = (await db.execute(
            select(PRReview).where(PRReview.repo_id == repo_id)
        )).scalars().all()
        active = [
            review
            for review in reviews
            if review.status in {
                "pending",
                "waiting_ci",
                "reviewing",
                "publishing",
                "superseding",
            }
        ]
        if active:
            await db.rollback()
            raise HTTPException(
                409,
                "Cannot delete a PR monitor while review Tasks, publication, "
                "or synchronize recovery are active",
            )
        review_ids = [review.id for review in reviews]
        monitor_run_ids = list((await db.execute(
            select(PRMonitorRun.id).where(PRMonitorRun.repo_id == repo_id)
        )).scalars())
        if monitor_run_ids:
            active_wake = (await db.execute(select(PRRepairWake.id).where(
                PRRepairWake.monitor_run_id.in_(monitor_run_ids),
                PRRepairWake.status.in_(("pending", *_STARTED_REPAIR_STATUSES)),
            ).limit(1))).scalar_one_or_none()
            active_merge = (await db.execute(select(PRMergeQueueAction.id).where(
                PRMergeQueueAction.monitor_run_id.in_(monitor_run_ids),
                PRMergeQueueAction.status.in_(("pending", *_STARTED_MERGE_QUEUE_STATUSES)),
            ).limit(1))).scalar_one_or_none()
            active_rebuttal = (await db.execute(select(PRFindingRebuttal.id).where(
                PRFindingRebuttal.monitor_run_id.in_(monitor_run_ids),
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            ).limit(1))).scalar_one_or_none()
            resolving_run = (await db.execute(select(PRMonitorRun.id).where(
                PRMonitorRun.id.in_(monitor_run_ids),
                PRMonitorRun.status == "resolving_fixed_threads",
            ).limit(1))).scalar_one_or_none()
            unresolved_thread = None
            if review_ids:
                unresolved_thread = (await db.execute(select(PRFinding.id).where(
                    PRFinding.pr_review_id.in_(review_ids),
                    PRFinding.thread_status.in_((
                        "published_inline",
                        "published_fallback",
                    )),
                ).limit(1))).scalar_one_or_none()
            if (
                active_wake
                or active_merge
                or active_rebuttal
                or resolving_run
                or unresolved_thread
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Cannot delete a PR monitor while Repair, adjudication, "
                    "Finding resolution, or Merge Queue work is active",
                )
        if monitor_run_ids:
            await db.execute(
                sa_delete(PRMergeQueueAction).where(
                    PRMergeQueueAction.monitor_run_id.in_(monitor_run_ids)
                )
            )
            await db.execute(
                sa_delete(PRRepairWake).where(
                    PRRepairWake.monitor_run_id.in_(monitor_run_ids)
                )
            )
        if review_ids:
            run_ids = list((await db.execute(
                select(PRReviewerRun.id).where(
                    PRReviewerRun.pr_review_id.in_(review_ids)
                )
            )).scalars())
            finding_ids = list((await db.execute(
                select(PRFinding.id).where(
                    PRFinding.pr_review_id.in_(review_ids)
                )
            )).scalars())
            if finding_ids:
                # Do not rely on database-level cascades here.  SQLite
                # deployments may predate foreign_keys=ON, and orphaned
                # terminal actions would retain globally unique idempotency
                # keys after their monitor is deleted.
                await db.execute(
                    sa_delete(PRFindingAction).where(
                        PRFindingAction.finding_id.in_(finding_ids)
                    )
                )
                await db.execute(
                    sa_delete(PRFindingRebuttal).where(
                        PRFindingRebuttal.finding_id.in_(finding_ids)
                    )
                )
                await db.execute(
                    sa_delete(PRFinding).where(
                        PRFinding.id.in_(finding_ids)
                    )
                )
            await db.execute(
                sa_delete(PRReviewerRun).where(
                    PRReviewerRun.pr_review_id.in_(review_ids)
                )
            )
        for review in reviews:
            await db.delete(review)
        if monitor_run_ids:
            await db.execute(
                sa_delete(PRMonitorRun).where(PRMonitorRun.id.in_(monitor_run_ids))
            )

        await db.delete(locked_repo)
        await db.commit()
    return {"ok": True}


@router.post("/repos/{repo_id}/toggle", response_model=MonitoredRepoResponse)
async def toggle_repo(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _require_pr_monitor_access(request, db, repo)
        if repo.enabled:
            await _quiesce_monitor_runs(
                db,
                repo_id=repo_id,
                reason="repo_disabled",
            )
        repo.enabled = not repo.enabled
        await db.commit()
        await db.refresh(repo)
        return repo


@router.post(
    "/repos/{repo_id}/regenerate-secret",
    response_model=MonitoredRepoSecretResponse,
)
async def regenerate_secret(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _require_pr_monitor_access(request, db, repo)
        if await _active_finding_action_for_repo(db, repo_id) is not None:
            raise HTTPException(
                409,
                "Cannot rotate the webhook secret while a Finding repair action is active",
            )
        repo.webhook_secret = secrets.token_hex(32)
        await db.commit()
        await db.refresh(repo)
        return repo


@router.get("/repos/{repo_id}/reviews", response_model=list[PRReviewResponse])
async def list_reviews(
    repo_id: int,
    request: Request,
    page: int = 1,
    size: int = 20,
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)

    offset = (page - 1) * size
    result = await db.execute(
        select(PRReview)
        .where(PRReview.repo_id == repo_id)
        .order_by(desc(PRReview.created_at))
        .offset(offset)
        .limit(size)
    )
    return result.scalars().all()


@router.get("/reviews/{review_id}", response_model=PRReviewDetailResponse)
async def get_review(
    review_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    review = await db.get(PRReview, review_id)
    if not review:
        raise HTTPException(404, "Review not found")
    repo = await db.get(MonitoredRepo, review.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    runs = list((await db.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review.id)
        .order_by(PRReviewerRun.id)
    )).scalars())
    findings = list((await db.execute(
        select(PRFinding)
        .where(PRFinding.pr_review_id == review.id)
        .order_by(PRFinding.id)
    )).scalars())
    rebuttals = list((await db.execute(
        select(PRFindingRebuttal)
        .where(PRFindingRebuttal.pr_review_id == review.id)
        .order_by(PRFindingRebuttal.id)
    )).scalars())
    actions = list((await db.execute(
        select(PRFindingAction)
        .where(PRFindingAction.finding_id.in_([item.id for item in findings]))
        .order_by(PRFindingAction.id)
    )).scalars()) if findings else []
    by_run: dict[int, list[PRFinding]] = {}
    by_finding: dict[int, list[PRFindingRebuttal]] = {}
    latest_action: dict[int, PRFindingAction] = {}
    for action in actions:
        latest_action[action.finding_id] = action
    for rebuttal in rebuttals:
        by_finding.setdefault(rebuttal.finding_id, []).append(rebuttal)
    for finding in findings:
        by_run.setdefault(finding.reviewer_run_id, []).append(finding)
    payload = PRReviewResponse.model_validate(review).model_dump()
    from backend.services.pr_review_actions import is_current_review_snapshot

    payload["is_current_snapshot"] = await is_current_review_snapshot(db, review)
    payload["reviewer_runs"] = [
        PRReviewerRunResponse.model_validate(run).model_copy(
            update={"findings": [
                PRFindingResponse.model_validate(finding).model_copy(update={
                    "rebuttals": [
                        PRFindingRebuttalResponse.model_validate(item)
                        for item in by_finding.get(finding.id, [])
                    ],
                    "latest_action": (
                        PRFindingActionResponse.model_validate(
                            _action_response_payload(latest_action[finding.id])
                        )
                        if finding.id in latest_action else None
                    ),
                })
                for finding in by_run.get(run.id, [])
            ]}
        )
        for run in runs
    ]
    return payload


async def _load_authorized_finding(
    request: Request,
    db: AsyncSession,
    finding_id: int,
) -> tuple[PRFinding, PRReview, MonitoredRepo]:
    finding = await db.get(PRFinding, finding_id)
    review = (
        await db.get(PRReview, finding.pr_review_id)
        if finding is not None else None
    )
    repo = (
        await db.get(MonitoredRepo, review.repo_id)
        if review is not None else None
    )
    if finding is None or review is None or repo is None:
        raise HTTPException(404, "Finding not found")
    await _require_pr_monitor_access(request, db, repo)
    return finding, review, repo


async def _perform_immediate_finding_action(
    *,
    request: Request,
    db: AsyncSession,
    finding_id: int,
    action_type: str,
    idempotency_key: str,
    human_advice: str | None = None,
) -> dict:
    finding, review, repo = await _load_authorized_finding(
        request, db, finding_id
    )
    from backend.services.pr_review_actions import (
        FindingActionConflict,
        create_immediate_finding_action,
    )

    finding_row_id = finding.id
    review_row_id = review.id
    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            action = await create_immediate_finding_action(
                db,
                finding_id=finding_row_id,
                review_id=review_row_id,
                action_type=action_type,
                idempotency_key=idempotency_key,
                actor_user_id=actor_user_id,
                human_advice=human_advice,
            )
        except FindingActionConflict as exc:
            raise HTTPException(409, str(exc)) from exc
    return _action_response_payload(action)


@router.post(
    "/findings/{finding_id}/ignore",
    response_model=PRFindingActionResponse,
)
async def ignore_review_finding(
    finding_id: int,
    body: FindingActionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _perform_immediate_finding_action(
        request=request,
        db=db,
        finding_id=finding_id,
        action_type="ignore",
        idempotency_key=body.idempotency_key,
    )


@router.post(
    "/findings/{finding_id}/advice",
    response_model=PRFindingActionResponse,
)
async def save_review_finding_advice(
    finding_id: int,
    body: HumanAdviceRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _perform_immediate_finding_action(
        request=request,
        db=db,
        finding_id=finding_id,
        action_type="human_advice",
        idempotency_key=body.idempotency_key,
        human_advice=body.advice,
    )


@router.post(
    "/findings/{finding_id}/fix",
    response_model=PRFindingActionResponse,
)
async def create_review_finding_fix(
    finding_id: int,
    body: FindingActionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    finding, review, repo = await _load_authorized_finding(
        request, db, finding_id
    )
    from backend.services.pr_review_actions import FindingActionConflict
    from backend.services.pr_review_fix import FixConfirmationError, create_fix_task
    from backend.services.pr_review_service import GhError

    finding_row_id = finding.id
    review_row_id = review.id
    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            action = await create_fix_task(
                db,
                finding_id=finding_row_id,
                review_id=review_row_id,
                repo_id=repo_id,
                idempotency_key=body.idempotency_key,
                actor_user_id=actor_user_id,
            )
        except (FindingActionConflict, FixConfirmationError) as exc:
            raise HTTPException(409, str(exc)) from exc
        except GhError as exc:
            raise HTTPException(409, f"PR repair input is no longer available: {exc}") from exc
    return _action_response_payload(action)


async def _load_authorized_finding_action(
    request: Request,
    db: AsyncSession,
    action_id: int,
) -> tuple[PRFindingAction, PRFinding, PRReview, MonitoredRepo]:
    action = await db.get(PRFindingAction, action_id)
    if action is None:
        raise HTTPException(404, "Finding action not found")
    finding, review, repo = await _load_authorized_finding(
        request, db, action.finding_id
    )
    return action, finding, review, repo


@router.get(
    "/actions/{action_id}",
    response_model=PRFindingActionResponse,
)
async def get_review_finding_action(
    action_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request, db, action_id
    )
    await db.rollback()
    try:
        from backend.database import async_session
        from backend.main import worker_relay
        from backend.services.pr_review_fix import reconcile_finding_action

        await reconcile_finding_action(
            async_session,
            action_id,
            worker_relay=worker_relay,
        )
    except Exception:
        # The periodic reconciler remains authoritative; a transient Worker or
        # GitHub outage must not turn an otherwise readable audit row into 500.
        logger.exception(
            "On-read PR finding action recovery failed for action %s",
            action_id,
        )
    db.expire_all()
    action, _, _, _ = await _load_authorized_finding_action(
        request,
        db,
        action_id,
    )
    return _action_response_payload(action)


@router.get("/actions/{action_id}/diff", response_class=PlainTextResponse)
async def download_review_finding_diff(
    action_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request, db, action_id
    )
    patch_text = (action.result or {}).get("patch")
    if (
        action.status != "awaiting_confirmation"
        or action.confirmed_at is not None
        or not isinstance(patch_text, str)
        or action.patch_sha256
        != hashlib.sha256(patch_text.encode("utf-8")).hexdigest()
    ):
        raise HTTPException(409, "Validated PR fix diff is not available")
    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        from backend.services.pr_review_actions import (
            FindingActionConflict,
            lock_pr_repo_action_boundary,
        )

        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        if not locked_repo.enabled:
            raise HTTPException(409, "PR monitor is disabled")
        action = (
            await db.execute(
                select(PRFindingAction)
                .where(PRFindingAction.id == action_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        patch_text = (action.result or {}).get("patch") if action else None
        if (
            action is None
            or action.status != "awaiting_confirmation"
            or action.confirmed_at is not None
            or not isinstance(patch_text, str)
            or action.patch_sha256
            != hashlib.sha256(patch_text.encode("utf-8")).hexdigest()
        ):
            raise HTTPException(409, "Validated PR fix diff is not available")
        confirmation_token = (action.result or {}).get("confirmation_token")
        if not isinstance(confirmation_token, str):
            raise HTTPException(409, "Validated PR fix confirmation is unavailable")
        receipt = secrets.token_urlsafe(32)
        action.download_receipt_hash = hashlib.sha256(
            receipt.encode("utf-8")
        ).hexdigest()
        action.downloaded_by_user_id = actor_user_id
        from backend.services.pr_review_service import _database_now

        action.downloaded_at = await _database_now(db)
        await db.commit()
        return PlainTextResponse(
            patch_text,
            media_type="text/x-diff; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'inline; filename="pr-fix-{action_id}.diff"'
                ),
                "Cache-Control": "no-store",
                "X-CCM-PR-Fix-Receipt": receipt,
                "X-CCM-PR-Fix-Token": confirmation_token,
            },
        )


@router.post(
    "/actions/{action_id}/confirm",
    response_model=PRFindingActionResponse,
)
async def confirm_review_finding_fix(
    action_id: int,
    body: ConfirmFixRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request, db, action_id
    )
    from backend.services.pr_review_fix import FixConfirmationError, confirm_fix

    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            completed = await confirm_fix(
                db,
                action_id=action_id,
                confirmation_token=body.confirmation_token,
                patch_sha256=body.patch_sha256,
                download_receipt=body.download_receipt,
                confirmed_by_user_id=actor_user_id,
            )
        except FixConfirmationError as exc:
            raise HTTPException(409, str(exc)) from exc
    return _action_response_payload(completed)


@router.post(
    "/actions/{action_id}/cancel",
    response_model=PRFindingActionResponse,
)
async def cancel_review_finding_fix(
    action_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request,
        db,
        action_id,
    )
    from backend.services.pr_review_fix import (
        FixConfirmationError,
        cancel_fix_action,
    )

    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            cancelled = await cancel_fix_action(
                db,
                action_id=action_id,
                cancelled_by_user_id=actor_user_id,
            )
        except FixConfirmationError as exc:
            raise HTTPException(409, str(exc)) from exc
    return _action_response_payload(cancelled)


@router.post(
    "/findings/{finding_id}/rebut",
    response_model=PRFindingRebuttalResponse,
)
async def submit_finding_rebuttal(
    finding_id: int,
    body: PRFindingRebuttalCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Submit exact-subject evidence to an isolated adjudicator."""
    from backend.models.task import Task
    from backend.services.pr_review_adjudication import create_rebuttal_task
    from backend.services.pr_review_service import (
        _gh_pr_view,
        _validated_pr_snapshot,
        prepare_pr_review_context,
    )

    finding = await db.get(PRFinding, finding_id)
    if finding is None:
        raise HTTPException(404, "Finding not found")
    review = await db.get(PRReview, finding.pr_review_id)
    run = await db.get(PRMonitorRun, review.monitor_run_id) if review else None
    repo = await db.get(MonitoredRepo, review.repo_id) if review else None
    if review is None or run is None or repo is None:
        raise HTTPException(409, "Finding lifecycle is incomplete")
    await _require_pr_monitor_access(request, db, repo)
    if (
        review.status not in {"commented", "approved"}
        or run.status not in {"waiting_for_fix", "paused"}
        or run.current_review_id != review.id
        or run.current_head_sha != finding.head_sha
    ):
        raise HTTPException(409, "Finding belongs to a superseded PR head")
    if finding.severity not in {"critical", "high", "medium"} or finding.status != "open":
        raise HTTPException(409, "Only an open blocking Finding can be rebutted")
    if run.developer_task_id is None:
        raise HTTPException(409, "Bind the original Developer Task before rebuttal")
    developer = await db.get(Task, run.developer_task_id)
    if developer is None:
        raise HTTPException(409, "Bound Developer Task no longer exists")
    await require_task_control(request, developer, db)
    snapshot = _validated_pr_snapshot(
        await _gh_pr_view(review.pr_number, repo.repo_full_name)
    )
    if (
        snapshot.get("state") != "OPEN"
        or snapshot.get("merged_at") is not None
        or snapshot.get("is_draft") is not False
        or snapshot.get("base_sha") != review.base_sha
        or snapshot.get("head_sha") != review.head_sha
    ):
        raise HTTPException(409, "GitHub PR subject changed before adjudication")
    context = await prepare_pr_review_context(repo, {
        "number": review.pr_number,
        "base_sha": review.base_sha,
        "head_sha": review.head_sha,
        "title": review.pr_title,
        "author": review.pr_author,
        "url": review.pr_url,
    })
    # Context preparation may perform remote reads.  Refresh the GitHub
    # subject once more before entering the portable database writer fence;
    # holding SQLite's global writer slot across network I/O would block
    # unrelated writes without actually locking the remote PR.
    snapshot = _validated_pr_snapshot(
        await _gh_pr_view(review.pr_number, repo.repo_full_name)
    )
    if (
        snapshot.get("state") != "OPEN"
        or snapshot.get("merged_at") is not None
        or snapshot.get("is_draft") is not False
        or snapshot.get("base_sha") != review.base_sha
        or snapshot.get("head_sha") != review.head_sha
    ):
        raise HTTPException(409, "GitHub PR subject changed before adjudication")
    repo_id = repo.id
    review_id = review.id
    run_id = run.id
    developer_id = developer.id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict:
            repo = None
        run = (await db.execute(
            select(PRMonitorRun)
            .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        review = (await db.execute(
            select(PRReview)
            .where(PRReview.id == review_id, PRReview.repo_id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        finding = (await db.execute(
            select(PRFinding)
            .where(PRFinding.id == finding_id, PRFinding.pr_review_id == review_id)
            .with_for_update()
        )).scalar_one_or_none()
        developer = (await db.execute(
            select(Task).where(Task.id == developer_id).with_for_update()
        )).scalar_one_or_none()
        if repo is None or run is None or review is None or finding is None:
            raise HTTPException(409, "Finding lifecycle changed before adjudication")
        await _require_pr_monitor_access(request, db, repo)
        if developer is None:
            raise HTTPException(409, "Bound Developer Task no longer exists")
        await require_task_control(request, developer, db)
        if not repo.enabled:
            raise HTTPException(409, "PR monitor is disabled")
        if (
            review.status not in {"commented", "approved"}
            or run.status not in {"waiting_for_fix", "paused"}
            or run.current_review_id != review.id
            or run.current_base_sha != review.base_sha
            or run.current_head_sha != review.head_sha
            or finding.base_sha != review.base_sha
            or finding.head_sha != review.head_sha
            or run.developer_task_id != developer.id
        ):
            raise HTTPException(409, "Finding belongs to a superseded PR subject")
        if finding.severity not in {"critical", "high", "medium"} or finding.status != "open":
            raise HTTPException(409, "Only an open blocking Finding can be rebutted")
        active_fix = (await db.execute(
            select(PRFindingAction.id)
            .where(
                PRFindingAction.finding_id == finding.id,
                or_(
                    PRFindingAction.active_fix_finding_id == finding.id,
                    PRFindingAction.status.in_(_ACTIVE_FINDING_ACTION_STATUSES),
                ),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        if active_fix is not None:
            raise HTTPException(
                409,
                "This Finding already has an active AI repair",
            )
        active = (await db.execute(
            select(PRFindingRebuttal.id)
            .where(
                PRFindingRebuttal.finding_id == finding.id,
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        if active is not None:
            raise HTTPException(409, "This Finding already has an active adjudication")
        return await create_rebuttal_task(
            db,
            repo=repo,
            run=run,
            review=review,
            finding=finding,
            developer_task=developer,
            evidence=body.evidence,
            material=context["material"],
        )


@router.get("/runs/{run_id}", response_model=PRMonitorRunResponse)
async def get_monitor_run(
    run_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    wakes = list((await db.execute(
        select(PRRepairWake)
        .where(PRRepairWake.monitor_run_id == run.id)
        .order_by(desc(PRRepairWake.id))
    )).scalars())
    payload = PRMonitorRunResponse.model_validate(run).model_dump()
    payload["wakes"] = [PRRepairWakeResponse.model_validate(item) for item in wakes]
    merge_actions = list((await db.execute(
        select(PRMergeQueueAction)
        .where(PRMergeQueueAction.monitor_run_id == run.id)
        .order_by(desc(PRMergeQueueAction.id))
    )).scalars())
    payload["merge_actions"] = [
        PRMergeQueueActionResponse.model_validate(item) for item in merge_actions
    ]
    return payload


@router.post("/runs/{run_id}/bind-developer", response_model=PRMonitorRunResponse)
async def bind_monitor_developer(
    run_id: int,
    body: PRMonitorBindRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    from backend.models.task import Task
    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot
    from backend.services.worker_proxy import get_task_operation_lock

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    task = await db.get(Task, body.task_id)
    if repo is None or task is None:
        raise HTTPException(404, "Repository or Developer Task not found")
    await _require_pr_monitor_access(request, db, repo)
    await require_task_control(request, task, db)
    repo_id = repo.id
    task_id = task.id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        async with get_task_operation_lock(task_id):
            repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            run = (await db.execute(
                select(PRMonitorRun)
                .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            task = (await db.execute(
                select(Task).where(Task.id == task_id).with_for_update()
            )).scalar_one_or_none()
            if repo is None or run is None or task is None:
                raise HTTPException(404, "Repository, Run, or Developer Task not found")
            await _require_pr_monitor_access(request, db, repo)
            await require_task_control(request, task, db)
            if not repo.enabled:
                raise HTTPException(409, "Cannot bind a Developer while the monitor is disabled")
            if run.status in {"merged", "closed"} or run.completed_at is not None:
                raise HTTPException(409, "Cannot bind a Developer to a terminal PR Monitor Run")
            if run.status in _EXTERNALLY_BUSY_RUN_STATUSES:
                raise HTTPException(409, "Cannot bind a Developer while monitor effects are active")
            active_effect = (await db.execute(
                select(PRRepairWake.id)
                .where(
                    PRRepairWake.monitor_run_id == run.id,
                    PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            active_adjudication = (await db.execute(
                select(PRFindingRebuttal.id)
                .where(
                    PRFindingRebuttal.monitor_run_id == run.id,
                    PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            if active_effect is not None or active_adjudication is not None:
                raise HTTPException(409, "Cannot bind a Developer while monitor effects are active")
            if "pr-review" in (task.tags or []):
                raise HTTPException(400, "A Reviewer Task cannot be bound as the Developer")
            if repo.project_id is None or task.project_id != repo.project_id:
                raise HTTPException(400, "Developer Task must belong to the monitored Project")
            if not task.session_id or not task.last_cwd:
                raise HTTPException(409, "Developer Task has no resumable session/cwd yet")
            if not run.head_branch or task.result_branch != run.head_branch:
                raise HTTPException(409, "Developer Task branch does not match the PR head branch")
            if repo.auto_repair and (
                not run.head_repo_full_name
                or run.head_repo_full_name.lower() != repo.repo_full_name.lower()
            ):
                raise HTTPException(
                    409,
                    "Automatic Repair requires a proven same-repository PR head",
                )
            snapshot = _validated_pr_snapshot(
                await _gh_pr_view(run.pr_number, repo.repo_full_name)
            )
            if (
                snapshot.get("state") != "OPEN"
                or snapshot.get("merged_at") is not None
                or snapshot.get("is_draft") is not False
                or snapshot.get("base_sha") != run.current_base_sha
                or snapshot.get("head_sha") != run.current_head_sha
            ):
                raise HTTPException(409, "GitHub PR subject changed while binding; wait for synchronize")
            conflict = (await db.execute(
                select(PRMonitorRun.id)
                .where(
                    PRMonitorRun.developer_task_id == task.id,
                    PRMonitorRun.id != run.id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            if conflict is not None:
                raise HTTPException(409, "Developer Task is already bound to another active PR")
            run.developer_task_id = task.id
            run.binding_verified_at = datetime.utcnow()
            run.state_version += 1
            shadows = list((await db.execute(select(PRRepairWake).where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.trigger_head_sha == run.current_head_sha,
                PRRepairWake.status == "shadow",
            ).with_for_update())).scalars())
            for wake in shadows:
                wake.developer_task_id = task.id
                if repo.auto_repair and run.repair_attempts < run.max_repair_attempts:
                    wake.status = "pending"
                    wake.last_error = None
            if repo.auto_repair and shadows and run.repair_attempts < run.max_repair_attempts:
                run.status = "repair_pending"
            await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/pause", response_model=PRMonitorRunResponse)
async def pause_monitor_run(run_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    repo_id = repo.id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict:
            raise HTTPException(404, "Repository not found")
        await _require_pr_monitor_access(request, db, locked_repo)
        await _quiesce_monitor_runs(
            db,
            repo_id=repo_id,
            run_id=run_id,
            reason="manual",
        )
        await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/unbind-developer", response_model=PRMonitorRunResponse)
async def unbind_monitor_developer(run_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.services.worker_proxy import get_task_operation_lock

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    if run.developer_task_id is None:
        raise HTTPException(409, "PR Monitor Run has no bound Developer")
    repo_id = repo.id
    task_id = run.developer_task_id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        async with get_task_operation_lock(task_id):
            repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            run = (await db.execute(
                select(PRMonitorRun)
                .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            if repo is None or run is None:
                raise HTTPException(404, "Repository or PR Monitor Run not found")
            await _require_pr_monitor_access(request, db, repo)
            if run.developer_task_id != task_id:
                raise HTTPException(409, "Developer binding changed concurrently")
            if run.status in {"merged", "closed"} or run.completed_at is not None:
                raise HTTPException(409, "Cannot unbind a terminal PR Monitor Run")
            active_repair = (await db.execute(select(PRRepairWake.id).where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
            ).limit(1).with_for_update())).scalar_one_or_none()
            active_adjudication = (await db.execute(
                select(PRFindingRebuttal.id).where(
                    PRFindingRebuttal.monitor_run_id == run.id,
                    PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
                ).limit(1).with_for_update()
            )).scalar_one_or_none()
            active_merge = (await db.execute(
                select(PRMergeQueueAction.id).where(
                    PRMergeQueueAction.monitor_run_id == run.id,
                    PRMergeQueueAction.status.in_(_STARTED_MERGE_QUEUE_STATUSES),
                ).limit(1).with_for_update()
            )).scalar_one_or_none()
            active_publication = (await db.execute(
                select(PRReview.id).where(
                    PRReview.monitor_run_id == run.id,
                    PRReview.status.in_(("publishing", "superseding")),
                ).limit(1).with_for_update()
            )).scalar_one_or_none()
            if any((active_repair, active_adjudication, active_merge, active_publication)):
                raise HTTPException(409, "Cannot unbind while monitor effects are active")
            wakes = list((await db.execute(select(PRRepairWake).where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.status.in_(("pending", "shadow")),
            ).with_for_update())).scalars())
            for wake in wakes:
                wake.developer_task_id = None
                wake.status = "shadow"
                if wake.last_error is None:
                    wake.last_error = "developer_unbound"
            run.developer_task_id = None
            run.binding_verified_at = None
            if run.status == "repair_pending":
                run.status = "waiting_for_fix"
            run.state_version += 1
            await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/resume", response_model=PRMonitorRunResponse)
async def resume_monitor_run(run_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.models.task import Task
    from backend.services.pr_merge_queue import (
        _read_merge_group_ref,
        _read_queue_entry,
    )
    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    repo_id = repo.id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        repo = (await db.execute(
            select(MonitoredRepo)
            .where(MonitoredRepo.id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        run = (await db.execute(
            select(PRMonitorRun)
            .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        if repo is None or run is None:
            raise HTTPException(404, "Repository or PR Monitor Run not found")
        await _require_pr_monitor_access(request, db, repo)
        if not repo.enabled:
            raise HTTPException(409, "Enable the PR monitor before resuming a Run")
        if run.status != "paused":
            raise HTTPException(409, "Only a paused PR Monitor Run can be resumed")

        current_action = (await db.execute(select(PRMergeQueueAction).where(
            PRMergeQueueAction.monitor_run_id == run.id,
            PRMergeQueueAction.trigger_base_sha == run.current_base_sha,
            PRMergeQueueAction.trigger_head_sha == run.current_head_sha,
            PRMergeQueueAction.status == "paused",
        ).order_by(desc(PRMergeQueueAction.id)).with_for_update())).scalars().first()
        current_wake = (await db.execute(select(PRRepairWake).where(
            PRRepairWake.monitor_run_id == run.id,
            PRRepairWake.trigger_base_sha == run.current_base_sha,
            PRRepairWake.trigger_head_sha == run.current_head_sha,
            PRRepairWake.status.in_(("shadow", "failed")),
        ).order_by(desc(PRRepairWake.id)).with_for_update())).scalars().first()
        if current_action is not None:
            try:
                snapshot = _validated_pr_snapshot(
                    await _gh_pr_view(run.pr_number, repo.repo_full_name)
                )
            except Exception as exc:
                await db.rollback()
                raise HTTPException(
                    409,
                    "GitHub PR state could not be confirmed while resuming "
                    "Merge Queue",
                ) from exc
            if (
                snapshot.get("state") != "OPEN"
                or snapshot.get("merged_at") is not None
                or snapshot.get("is_draft") is not False
                or snapshot.get("base_sha") != current_action.trigger_base_sha
                or snapshot.get("head_sha") != current_action.trigger_head_sha
            ):
                raise HTTPException(409, "GitHub PR subject changed while resuming Merge Queue")
            try:
                entry = await _read_queue_entry(
                    repo.repo_full_name, run.pr_number
                )
            except Exception as exc:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Remote Merge Queue state could not be confirmed",
                ) from exc
            if entry is not None and (
                entry.base_sha != current_action.trigger_base_sha
                or entry.head_sha != current_action.trigger_head_sha
            ):
                raise HTTPException(409, "Remote Merge Queue entry is for another subject")
            if entry is not None and entry.state in {"UNMERGEABLE", "LOCKED"}:
                raise HTTPException(409, f"Remote Merge Queue entry is {entry.state.lower()}")
            if entry is not None:
                try:
                    merge_group = await _read_merge_group_ref(
                        repo.repo_full_name,
                        default_branch=repo.default_branch,
                        pr_number=run.pr_number,
                    )
                except Exception as exc:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Remote Merge Queue merge-group state could not be "
                        "confirmed",
                    ) from exc
            else:
                merge_group = None
            if entry is None:
                current_action.status = "pending"
                current_action.github_queue_entry_id = None
                current_action.merge_group_sha = None
                current_action.merge_group_ref = None
                current_action.ci_status = None
                current_action.ci_details = None
                run.status = "merge_queue_pending"
            elif merge_group is None:
                current_action.status = "queued"
                current_action.github_queue_entry_id = entry.id
                current_action.merge_group_sha = None
                current_action.merge_group_ref = None
                current_action.ci_status = None
                current_action.ci_details = None
                run.status = "merge_queued"
            else:
                current_action.status = "checking"
                current_action.github_queue_entry_id = entry.id
                current_action.merge_group_sha = merge_group[0]
                current_action.merge_group_ref = merge_group[1]
                current_action.ci_status = "pending"
                current_action.ci_details = None
                run.status = "merge_group_checking"
            current_action.last_error = None
        elif current_wake is not None and repo.auto_repair and run.developer_task_id is not None:
            task = await db.get(Task, run.developer_task_id)
            if task is None or not task.session_id or not task.last_cwd:
                raise HTTPException(409, "Bound Developer Task is not resumable")
            if run.repair_attempts >= run.max_repair_attempts:
                raise HTTPException(409, "Automatic repair budget is exhausted")
            snapshot = _validated_pr_snapshot(
                await _gh_pr_view(run.pr_number, repo.repo_full_name)
            )
            if (
                snapshot.get("state") != "OPEN"
                or snapshot.get("merged_at") is not None
                or snapshot.get("is_draft") is not False
                or snapshot.get("base_sha") != current_wake.trigger_base_sha
                or snapshot.get("head_sha") != current_wake.trigger_head_sha
            ):
                raise HTTPException(
                    409,
                    "GitHub PR subject changed while resuming Repair",
                )
            current_wake.status = "pending"
            current_wake.delivery_token = secrets.token_hex(24)
            current_wake.last_error = None
            current_wake.developer_task_id = task.id
            run.status = "repair_pending"
        else:
            if current_wake is not None:
                current_wake.status = "shadow"
            review = (
                await db.get(PRReview, run.current_review_id)
                if run.current_review_id is not None
                else None
            )
            blockers = []
            if review is not None:
                blockers = list((await db.execute(select(PRFinding.id).where(
                    PRFinding.pr_review_id == review.id,
                    PRFinding.severity.in_(("critical", "high", "medium")),
                    or_(
                        PRFinding.status == "open",
                        PRFinding.thread_status != "resolved",
                    ),
                ))).scalars())
            if review is None:
                run.status = "observing"
            elif review.status == "waiting_ci":
                run.status = "waiting_ci"
            elif review.status in {"pending", "reviewing"}:
                run.status = "reviewing"
            elif review.status in {"approved", "commented"} and not blockers:
                run.status = "ready_to_merge"
            else:
                run.status = "waiting_for_fix"
        run.pause_reason = None
        run.state_version += 1
        await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/enqueue-merge", response_model=PRMonitorRunResponse)
async def enqueue_monitor_merge(
    run_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    review = await db.get(PRReview, run.current_review_id) if run.current_review_id else None
    if repo is None or review is None:
        raise HTTPException(409, "PR Monitor Gate subject is incomplete")
    await _require_pr_monitor_access(request, db, repo)
    if run.status != "ready_to_merge":
        raise HTTPException(409, "The exact PR head is not ready to enter Merge Queue")
    action = (await db.execute(select(PRMergeQueueAction).where(
        PRMergeQueueAction.monitor_run_id == run.id,
        PRMergeQueueAction.trigger_head_sha == run.current_head_sha,
    ))).scalar_one_or_none()
    if action is None:
        action = PRMergeQueueAction(
            monitor_run_id=run.id, review_id=review.id,
            trigger_base_sha=run.current_base_sha,
            trigger_head_sha=run.current_head_sha,
            status="pending", action_nonce=secrets.token_hex(24),
        )
        db.add(action)
    elif action.status == "shadow":
        action.status = "pending"
    else:
        raise HTTPException(409, f"Merge Queue action is already {action.status}")
    run.status = "merge_queue_pending"
    run.state_version += 1
    await db.commit()
    return await get_monitor_run(run.id, request, db)


# --- Webhook endpoint ---

@webhook_router.post("/webhook")
async def github_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    body = await request.body()

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON payload")

    repo_full_name = payload.get("repository", {}).get("full_name")
    if not repo_full_name:
        return {"status": "ignored", "reason": "no repository info"}

    result = await db.execute(
        select(MonitoredRepo).where(MonitoredRepo.repo_full_name == repo_full_name)
    )
    repo = result.scalar_one_or_none()
    if not repo or not repo.enabled:
        return {"status": "ignored", "reason": "repository not monitored or disabled"}

    signature_header = request.headers.get("X-Hub-Signature-256", "")
    _require_current_webhook_signature(
        repo,
        body=body,
        signature_header=signature_header,
    )

    event_type = request.headers.get("X-GitHub-Event", "")
    if event_type == "merge_group":
        if payload.get("action") != "checks_requested":
            return {"status": "ignored", "reason": f"merge_group action: {payload.get('action', '')}"}
        merge_group = payload.get("merge_group")
        if not isinstance(merge_group, dict):
            raise HTTPException(400, "merge_group must be an object")
        merge_sha = _parse_commit_sha(merge_group.get("head_sha"), "merge_group")
        merge_ref = merge_group.get("head_ref")
        if not isinstance(merge_ref, str) or not merge_ref or len(merge_ref) > 500:
            raise HTTPException(400, "merge_group.head_ref is invalid")
        from backend.services.pr_merge_queue import bind_merge_group

        repo_id = repo.id
        await db.rollback()
        async with _pr_repo_write_lock(repo_id):
            locked_repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            if locked_repo is None or not locked_repo.enabled:
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "repository not monitored or disabled",
                }
            _require_current_webhook_signature(
                locked_repo,
                body=body,
                signature_header=signature_header,
            )
            bound = await bind_merge_group(
                db,
                repo=locked_repo,
                head_sha=merge_sha,
                head_ref=merge_ref,
            )
        return {
            "status": "accepted" if bound else "ignored",
            "reason": None if bound else "no unique queued PR for merge group",
        }

    # Only handle pull_request events beyond this point.
    if event_type != "pull_request":
        return {"status": "ignored", "reason": f"event type: {event_type}"}

    action = payload.get("action", "")
    if action not in ("opened", "synchronize"):
        return {"status": "ignored", "reason": f"action: {action}"}

    pr = payload.get("pull_request")
    if not isinstance(pr, dict):
        raise HTTPException(400, "pull_request must be an object")

    base = pr.get("base")
    head = pr.get("head")
    base_sha = _parse_commit_sha(
        base.get("sha") if isinstance(base, dict) else None,
        "base",
    )
    head_sha = _parse_commit_sha(
        head.get("sha") if isinstance(head, dict) else None,
        "head",
    )

    # Skip draft PRs
    if pr.get("draft", False):
        return {"status": "ignored", "reason": "draft PR"}

    base_branch = base.get("ref", "") if isinstance(base, dict) else ""
    pr_author = pr.get("user", {}).get("login", "")
    policy_rejection = _webhook_policy_rejection(
        repo,
        base_branch=base_branch,
        pr_author=pr_author,
    )
    if policy_rejection is not None:
        return {"status": "ignored", "reason": policy_rejection}

    pr_number = pr.get("number")
    delivery_id = (request.headers.get("X-GitHub-Delivery", "") or "").strip() or None
    repo_id = repo.id
    repo_name = repo.repo_full_name

    if (
        not isinstance(pr_number, int)
        or isinstance(pr_number, bool)
        or pr_number <= 0
    ):
        raise HTTPException(400, "pull_request.number must be a positive integer")

    pr_title = pr.get("title", "")
    pr_url = pr.get("html_url", "")
    head_repo = head.get("repo") if isinstance(head, dict) else None
    head_repo_full_name = (
        head_repo.get("full_name") if isinstance(head_repo, dict) else None
    )
    head_branch = head.get("ref") if isinstance(head, dict) else None
    if head_repo_full_name is not None and (
        not isinstance(head_repo_full_name, str)
        or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", head_repo_full_name) is None
    ):
        raise HTTPException(400, "pull_request.head.repo.full_name is invalid")
    if head_branch is not None and (
        not isinstance(head_branch, str)
        or not head_branch
        or len(head_branch) > 200
        or "\x00" in head_branch
    ):
        raise HTTPException(400, "pull_request.head.ref is invalid")

    # Fast-path idempotency check. The database uniqueness constraints below
    # are still required because two deliveries can race between this SELECT
    # and the INSERT performed by create_pr_review_task.
    processed_review = await _find_processed_review(
        db,
        repo_id,
        pr_number,
        base_sha,
        head_sha,
        delivery_id,
    )
    if processed_review:
        logger.info(
            "Ignored duplicate PR webhook for %s#%d at %s...%s (review %d)",
            repo_name,
            pr_number,
            base_sha,
            head_sha,
            processed_review.id,
        )
        return _duplicate_review_response(processed_review, delivery_id)

    if action == "synchronize":
        from backend.services.pr_review_service import (
            create_pr_review_task,
            prepare_pr_review_context,
            verify_pr_review_snapshot_current,
        )

        replacement_data = {
            "number": pr_number,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "delivery_id": delivery_id,
            "title": pr_title,
            "author": pr_author,
            "url": pr_url,
            "head_repo_full_name": head_repo_full_name,
            "head_branch": head_branch,
        }
        # Fetch and validate every model-visible byte before terminating the
        # old generation. A transient GitHub/context failure therefore leaves
        # the still-running review untouched and lets GitHub retry delivery.
        prepared_context = await prepare_pr_review_context(
            repo,
            replacement_data,
        )
        await db.rollback()

        async with _pr_repo_write_lock(repo_id):
            db.expire_all()
            # This row lock is the cross-process write barrier. The lightweight
            # GitHub guard runs inside it, after context preparation, so a slow
            # older webhook cannot overwrite a newer durable intent.
            locked_repo = (
                await db.execute(
                    select(MonitoredRepo)
                    .where(MonitoredRepo.id == repo_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if locked_repo is None or not locked_repo.enabled:
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "repository not monitored or disabled",
                }
            # Context capture may take long enough for an administrator to
            # rotate the secret or edit admission policy.  The repository row
            # lock is the delivery's linearization point, so repeat both checks
            # here before persisting a supersede intent or stopping old work.
            _require_current_webhook_signature(
                locked_repo,
                body=body,
                signature_header=signature_header,
            )
            policy_rejection = _webhook_policy_rejection(
                locked_repo,
                base_branch=base_branch,
                pr_author=pr_author,
            )
            if policy_rejection is not None:
                await db.rollback()
                return {"status": "ignored", "reason": policy_rejection}
            await verify_pr_review_snapshot_current(
                locked_repo,
                replacement_data,
            )

            # Re-run idempotency at the actual write barrier. This prevents a
            # duplicate same-snapshot synchronize from claiming its own newly
            # created review as an older generation.
            processed_review = await _find_processed_review(
                db,
                repo_id,
                pr_number,
                base_sha,
                head_sha,
                delivery_id,
            )
            if processed_review is not None:
                duplicate_response = _duplicate_review_response(
                    processed_review,
                    delivery_id,
                )
                await db.rollback()
                return duplicate_response

            active_repair = (await db.execute(
                select(PRRepairWake)
                .join(PRMonitorRun, PRMonitorRun.id == PRRepairWake.monitor_run_id)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.pr_number == pr_number,
                    PRRepairWake.status == "accepted",
                )
                .order_by(desc(PRRepairWake.id))
            )).scalars().first()
            active_adjudication = (await db.execute(
                select(PRFindingRebuttal)
                .join(PRReview, PRReview.id == PRFindingRebuttal.pr_review_id)
                .where(
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    PRFindingRebuttal.status == "adjudicating",
                )
                .order_by(desc(PRFindingRebuttal.id))
            )).scalars().first()
            active_review_predicate = PRReview.status.in_(
                ("pending", "waiting_ci", "reviewing", "superseding")
            )
            if active_repair is not None and active_repair.review_id is not None:
                active_review_predicate = or_(
                    active_review_predicate,
                    PRReview.id == active_repair.review_id,
                )
            if active_adjudication is not None:
                active_review_predicate = or_(
                    active_review_predicate,
                    PRReview.id == active_adjudication.pr_review_id,
                )
            active_result = await db.execute(
                select(PRReview).where(
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    active_review_predicate,
                )
            )
            observed_reviews = list(active_result.scalars().all())

            # A publishing row is a durable external-action outbox. Never
            # supersede it while a GitHub write may be in flight; it remains
            # pinned to the old head and reconciles independently.
            if not observed_reviews:
                try:
                    review = await create_pr_review_task(
                        db,
                        locked_repo,
                        replacement_data,
                        prepared_context=prepared_context,
                    )
                except IntegrityError:
                    await db.rollback()
                    winner = await _find_processed_review(
                        db,
                        repo_id,
                        pr_number,
                        base_sha,
                        head_sha,
                        delivery_id,
                    )
                    if winner is not None:
                        return _duplicate_review_response(
                            winner,
                            delivery_id,
                        )
                    raise
                return {"status": "accepted", "review_id": review.id}

            # Persist the immutable replacement intent before touching any old
            # Task. Each row is exact-CASed from the state observed under the
            # repository barrier. A stale webhook cannot overwrite a newer
            # superseding token, and a partial claim is rolled back atomically.
            superseding_token = secrets.token_hex(24)
            superseding_started_at = datetime.utcnow()
            superseding_snapshot = {
                "version": 2,
                "pr_data": replacement_data,
                "prepared_context": prepared_context,
            }
            active_review_generations = []
            for old in observed_reviews:
                predicates = [
                    PRReview.id == old.id,
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    PRReview.status == old.status,
                    (
                        PRReview.task_id.is_(None)
                        if old.task_id is None
                        else PRReview.task_id == old.task_id
                    ),
                ]
                if old.status == "superseding":
                    predicates.extend(
                        (
                            (
                                PRReview.superseding_token.is_(None)
                                if old.superseding_token is None
                                else PRReview.superseding_token
                                == old.superseding_token
                            ),
                            (
                                PRReview.superseding_started_at.is_(None)
                                if old.superseding_started_at is None
                                else PRReview.superseding_started_at
                                == old.superseding_started_at
                            ),
                        )
                    )
                claimed = await db.execute(
                    sa_update(PRReview)
                    .where(*predicates)
                    .values(
                        status="superseding",
                        superseding_snapshot=superseding_snapshot,
                        superseding_token=superseding_token,
                        superseding_started_at=superseding_started_at,
                    )
                )
                if claimed.rowcount != 1:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "A newer PR synchronize intent won the write barrier; "
                        "this stale delivery was not applied",
                    )
                active_review_generations.append(
                    (old.id, old.task_id, old.status)
                )
            await db.commit()

            claimed_rows = await db.execute(
                select(PRReview.id).where(
                    PRReview.id.in_(
                        review_id
                        for review_id, _task_id, _status
                        in active_review_generations
                    ),
                    PRReview.status == "superseding",
                    PRReview.superseding_token == superseding_token,
                )
            )
            claimed_ids = set(claimed_rows.scalars().all())
            expected_ids = {
                review_id
                for review_id, _task_id, _status
                in active_review_generations
            }
            if claimed_ids != expected_ids:
                await db.rollback()
                raise HTTPException(
                    409,
                    "A newer PR synchronize intent replaced this delivery; "
                    "durable recovery will finish the newer snapshot",
                )

            if active_repair is not None:
                from backend.services.pr_monitor_loop import (
                    record_repair_push_observed,
                )

                active_repair_id = active_repair.id
                active_repair_head_sha = active_repair.trigger_head_sha
                await record_repair_push_observed(
                    db,
                    wake_id=active_repair_id,
                    previous_head_sha=active_repair_head_sha,
                    new_head_sha=head_sha,
                )
                active_repair = await db.get(
                    PRRepairWake,
                    active_repair_id,
                    populate_existing=True,
                )

            repair_developer_task_id = (
                active_repair.developer_task_id
                if active_repair is not None
                else None
            )
            repair_retry_count = (
                active_repair.accepted_task_retry_count
                if active_repair is not None
                else None
            )
            repair_session_id = (
                active_repair.accepted_session_id
                if active_repair is not None
                else None
            )
            completed_repair_developer_task_id = (
                repair_developer_task_id
                if active_repair is not None and active_repair.status == "completed"
                else None
            )

            from backend.services.task_termination import (
                TaskTerminationResult,
                TaskTerminationConflict,
                lock_task_generation,
                lock_worker_task_generation,
                task_termination_operation_locks,
                terminate_authoritative_task_generation,
            )

            task_ids = {
                task_id
                for _review_id, task_id, _status in active_review_generations
                if task_id is not None
            }
            panel_task_ids = (await db.execute(
                select(PRReviewerRun.task_id).where(
                    PRReviewerRun.pr_review_id.in_(expected_ids),
                    PRReviewerRun.task_id.is_not(None),
                )
            )).scalars().all()
            task_ids.update(panel_task_ids)
            if repair_developer_task_id is not None:
                repair_task = await db.get(Task, repair_developer_task_id)
                if (
                    repair_task is not None
                    and repair_task.status in ("in_progress", "executing")
                    and repair_task.retry_count == repair_retry_count
                    and repair_task.session_id == repair_session_id
                ):
                    task_ids.add(repair_task.id)
            if active_adjudication is not None and active_adjudication.task_id is not None:
                adjudicator_task = await db.get(Task, active_adjudication.task_id)
                if adjudicator_task is not None and adjudicator_task.status in (
                    "pending", "in_progress", "executing", "completed"
                ):
                    task_ids.add(adjudicator_task.id)
            # Worker migration and remote task mutations must remain excluded
            # until the replacement review commit releases the exact Task row
            # locks below. Otherwise a remote retry can run before its delayed
            # Manager mirror update is blocked by our database transaction.
            async with task_termination_operation_locks(task_ids):
                termination_results = {}
                for old_task_id in sorted(task_ids):
                    try:
                        termination_results[old_task_id] = (
                            await terminate_authoritative_task_generation(
                                old_task_id,
                                db,
                                reason="Superseded by new push",
                                operation_locks_held=True,
                            )
                        )
                    except TaskTerminationConflict as exc:
                        await db.rollback()
                        logger.warning(
                            "Refused to supersede PR review panel: task %d cleanup "
                            "was not confirmed: %s",
                            old_task_id,
                            exc,
                        )
                        raise HTTPException(
                            409,
                            "Previous PR review task cleanup could not be "
                            "confirmed; durable replacement recovery will retry",
                        ) from exc

                # Reacquire every exact resulting generation in stable order
                # and retain the row + operation locks through replacement
                # creation. A retry in the post-cleanup window then fails this
                # webhook rather than reviving the old review alongside its
                # replacement.
                for old_task_id in sorted(termination_results):
                    terminated = termination_results[old_task_id]
                    if isinstance(terminated, TaskTerminationResult):
                        locked_task = await lock_task_generation(
                            old_task_id,
                            db,
                            expected_status=terminated.terminal_status,
                            expected_retry_count=terminated.retry_count,
                            expected_instance_id=terminated.instance_id,
                            expected_started_at=terminated.started_at,
                            expected_completed_at=terminated.completed_at,
                            expected_pty_background_generation=(
                                terminated.pty_background_generation
                            ),
                        )
                    else:
                        locked_task = await lock_worker_task_generation(
                            db,
                            terminated.resulting,
                        )
                    if locked_task is None:
                        await db.rollback()
                        raise HTTPException(
                            409,
                            "Previous PR review task started a newer generation; "
                            "durable replacement recovery will retry",
                        )
                    if (
                        old_task_id == completed_repair_developer_task_id
                    ):
                        from backend.services.pr_monitor_loop import (
                            restore_repair_developer_task,
                        )

                        restore_repair_developer_task(locked_task)

                # The first repository row lock was released by the durable
                # intent commit. Reacquire it before the final review updates
                # and replacement INSERT so a newer webhook on another Manager
                # cannot observe the old token, wait here, then lose its intent
                # after this transaction commits.
                current_repo = (
                    await db.execute(
                        select(MonitoredRepo)
                        .where(MonitoredRepo.id == repo_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if current_repo is None or not current_repo.enabled:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "PR monitor changed during synchronize; durable "
                        "replacement recovery will retry",
                    )

                for (
                    review_id,
                    old_task_id,
                    _old_status,
                ) in active_review_generations:
                    review_predicates = [
                        PRReview.id == review_id,
                        PRReview.status == "superseding",
                        PRReview.superseding_token == superseding_token,
                        (
                            PRReview.task_id.is_(None)
                            if old_task_id is None
                            else PRReview.task_id == old_task_id
                        ),
                    ]
                    superseded = await db.execute(
                        sa_update(PRReview)
                        .where(*review_predicates)
                        .values(
                            status="superseded",
                            completed_at=datetime.utcnow(),
                            superseding_snapshot=None,
                            superseding_token=None,
                            superseding_started_at=None,
                        )
                    )
                    if not superseded.rowcount:
                        await db.rollback()
                        raise HTTPException(
                            409,
                            "Previous PR review changed while it was being "
                            "stopped; durable replacement recovery will retry",
                        )
                    if old_task_id is not None:
                        logger.info(
                            "Safely stopped task %d (superseded PR review)",
                            old_task_id,
                        )

                await db.execute(
                    sa_update(PRReviewerRun)
                    .where(
                        PRReviewerRun.pr_review_id.in_(expected_ids),
                        PRReviewerRun.status.in_(
                            ("pending", "reviewing", "passed", "changes_required")
                        ),
                    )
                    .values(
                        status="superseded",
                        completed_at=datetime.utcnow(),
                    )
                )

                # Termination commits/expirations invalidate the repo ORM
                # identity. Keep supersede writes uncommitted and let
                # replacement creation commit both review generations.
                try:
                    review = await create_pr_review_task(
                        db,
                        current_repo,
                        replacement_data,
                        prepared_context=prepared_context,
                    )
                except IntegrityError as exc:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Another synchronize created the replacement snapshot; "
                        "durable recovery will reconcile the old generation",
                    ) from exc
                return {"status": "accepted", "review_id": review.id}

    # Opened deliveries do not replace another live generation.
    active_result = await db.execute(
        select(PRReview).where(
            PRReview.repo_id == repo.id,
            PRReview.pr_number == pr_number,
            PRReview.status.in_(
                ["pending", "waiting_ci", "reviewing", "publishing", "superseding"]
            ),
        )
    )
    active_reviews = active_result.scalars().all()
    if active_reviews:
        return {"status": "ignored", "reason": "review already in progress"}
    if action == "opened":
        # Also skip if a completed review already exists for this PR
        completed_result = await db.execute(
            select(func.count()).select_from(PRReview).where(
                PRReview.repo_id == repo.id,
                PRReview.pr_number == pr_number,
                PRReview.status.in_(["approved", "merged", "commented"]),
            )
        )
        if completed_result.scalar():
            return {"status": "ignored", "reason": "PR already reviewed"}

    # Import and call service
    from backend.services.pr_review_service import (
        create_pr_review_task,
        prepare_pr_review_context,
        verify_pr_review_snapshot_current,
    )

    review_data = {
        "number": pr_number,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "delivery_id": delivery_id,
        "title": pr_title,
        "author": pr_author,
        "url": pr_url,
        "head_repo_full_name": head_repo_full_name,
        "head_branch": head_branch,
    }
    prepared_context = await prepare_pr_review_context(repo, review_data)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        db.expire_all()
        locked_repo = (
            await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if locked_repo is None or not locked_repo.enabled:
            await db.rollback()
            return {
                "status": "ignored",
                "reason": "repository not monitored or disabled",
            }
        _require_current_webhook_signature(
            locked_repo,
            body=body,
            signature_header=signature_header,
        )
        policy_rejection = _webhook_policy_rejection(
            locked_repo,
            base_branch=base_branch,
            pr_author=pr_author,
        )
        if policy_rejection is not None:
            await db.rollback()
            return {"status": "ignored", "reason": policy_rejection}
        await verify_pr_review_snapshot_current(locked_repo, review_data)
        processed_review = await _find_processed_review(
            db,
            repo_id,
            pr_number,
            base_sha,
            head_sha,
            delivery_id,
        )
        if processed_review is not None:
            duplicate_response = _duplicate_review_response(
                processed_review,
                delivery_id,
            )
            await db.rollback()
            return duplicate_response
        active_now = await db.execute(
            select(PRReview.id)
            .where(
                PRReview.repo_id == repo_id,
                PRReview.pr_number == pr_number,
                PRReview.status.in_(
                    ("pending", "waiting_ci", "reviewing", "publishing", "superseding")
                ),
            )
            .limit(1)
        )
        if active_now.scalar_one_or_none() is not None:
            await db.rollback()
            return {
                "status": "ignored",
                "reason": "review already in progress",
            }
        completed_now = await db.execute(
            select(PRReview.id)
            .where(
                PRReview.repo_id == repo_id,
                PRReview.pr_number == pr_number,
                PRReview.status.in_(("approved", "merged", "commented")),
            )
            .limit(1)
        )
        if completed_now.scalar_one_or_none() is not None:
            await db.rollback()
            return {"status": "ignored", "reason": "PR already reviewed"}
        try:
            review = await create_pr_review_task(
                db,
                locked_repo,
                review_data,
                prepared_context=prepared_context,
            )
        except IntegrityError:
            # A concurrent Manager may have won the same database uniqueness
            # key despite the process-local companion lock.
            await db.rollback()
            processed_review = await _find_processed_review(
                db,
                repo_id,
                pr_number,
                base_sha,
                head_sha,
                delivery_id,
            )
            if processed_review:
                logger.info(
                    "Ignored concurrently duplicated PR webhook for %s#%d at "
                    "%s...%s (review %d)",
                    repo_name,
                    pr_number,
                    base_sha,
                    head_sha,
                    processed_review.id,
                )
                return _duplicate_review_response(
                    processed_review,
                    delivery_id,
                )
            raise

    return {"status": "accepted", "review_id": review.id}
