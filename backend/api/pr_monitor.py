import asyncio
import hashlib
import hmac
import logging
import re
import secrets
from datetime import datetime
from weakref import WeakKeyDictionary

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import and_, desc, func, or_, select, update as sa_update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.pr_monitor import MonitoredRepo, PRReview
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    has_worker_access,
    require_project_access,
    require_worker_target_access,
)
from backend.schemas.pr_monitor import (
    MonitoredRepoCreate,
    MonitoredRepoUpdate,
    MonitoredRepoResponse,
    MonitoredRepoDetailResponse,
    PRReviewResponse,
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


@router.post("/repos", response_model=MonitoredRepoDetailResponse)
async def create_repo(request: Request, body: MonitoredRepoCreate, db: AsyncSession = Depends(get_db)):
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
        default_branch=body.default_branch,
        allowed_authors=body.allowed_authors,
        webhook_secret=secrets.token_hex(32),
    )
    db.add(repo)
    await db.commit()
    await db.refresh(repo)
    return repo


@router.get("/repos/{repo_id}", response_model=MonitoredRepoDetailResponse)
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


@router.put("/repos/{repo_id}", response_model=MonitoredRepoDetailResponse)
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
        locked_repo = (
            await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if locked_repo is None:
            raise HTTPException(404, "Repository not found")
        await _require_pr_monitor_access(request, db, locked_repo)
        reviews = (await db.execute(
            select(PRReview).where(PRReview.repo_id == repo_id)
        )).scalars().all()
        active = [
            review
            for review in reviews
            if review.status in {
                "pending",
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
        for review in reviews:
            await db.delete(review)

        await db.delete(locked_repo)
        await db.commit()
    return {"ok": True}


@router.post("/repos/{repo_id}/toggle", response_model=MonitoredRepoResponse)
async def toggle_repo(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    repo.enabled = not repo.enabled
    await db.commit()
    await db.refresh(repo)
    return repo


@router.post("/repos/{repo_id}/regenerate-secret", response_model=MonitoredRepoDetailResponse)
async def regenerate_secret(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
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


@router.get("/reviews/{review_id}", response_model=PRReviewResponse)
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
    return review


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

    # HMAC-SHA256 signature verification
    signature_header = request.headers.get("X-Hub-Signature-256", "")
    if not signature_header.startswith("sha256="):
        raise HTTPException(403, "Missing or invalid signature")

    expected_sig = "sha256=" + hmac.new(
        repo.webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature_header, expected_sig):
        raise HTTPException(403, "Invalid signature")

    # Only handle pull_request events
    event_type = request.headers.get("X-GitHub-Event", "")
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

    # Check target branch
    base_branch = base.get("ref", "") if isinstance(base, dict) else ""
    if base_branch != repo.default_branch:
        return {"status": "ignored", "reason": f"target branch: {base_branch}"}

    # Check allowed authors
    pr_author = pr.get("user", {}).get("login", "")
    allowed = repo.allowed_authors or []
    if allowed and pr_author not in allowed:
        return {"status": "ignored", "reason": f"author not allowed: {pr_author}"}

    # 自动屏蔽本机 gh 登录账号的 PR：审核者与作者同账号时 GitHub 禁止
    # self-approval，审了也无法 approve；除非白名单显式包含该账号
    own_login = _gh_login()
    if own_login and pr_author == own_login and pr_author not in allowed:
        return {"status": "ignored", "reason": f"self PR (gh login: {own_login})"}

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

            active_result = await db.execute(
                select(PRReview).where(
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    PRReview.status.in_(
                        ("pending", "reviewing", "superseding")
                    ),
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
            # Worker migration and remote task mutations must remain excluded
            # until the replacement review commit releases the exact Task row
            # locks below. Otherwise a remote retry can run before its delayed
            # Manager mirror update is blocked by our database transaction.
            async with task_termination_operation_locks(task_ids):
                termination_results = {}
                for (
                    review_id,
                    old_task_id,
                    _old_status,
                ) in active_review_generations:
                    if (
                        old_task_id is None
                        or old_task_id in termination_results
                    ):
                        continue
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
                            "Refused to supersede PR review %d: task %d cleanup "
                            "was not confirmed: %s",
                            review_id,
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
                ["pending", "reviewing", "publishing", "superseding"]
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
                    ("pending", "reviewing", "publishing", "superseding")
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
