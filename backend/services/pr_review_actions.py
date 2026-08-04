"""Audited, idempotent actions for one structured PR review finding."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRMonitorRun,
    PRReview,
)


class FindingActionConflict(RuntimeError):
    """The requested finding action is not valid for the current snapshot."""


_ACTIONABLE_REVIEW_STATUSES = {"approved", "merged", "commented"}
_ACTIVE_FIX_STATUSES = {"pending", "running", "awaiting_confirmation"}


async def is_current_review_snapshot(
    db: AsyncSession,
    review: PRReview,
) -> bool:
    if review.monitor_run_id is not None:
        monitor_run = await db.get(PRMonitorRun, review.monitor_run_id)
        if monitor_run is not None:
            return (
                monitor_run.current_review_id == review.id
                and monitor_run.current_head_sha == review.head_sha
            )
    newer = (
        await db.execute(
            select(PRReview.id)
            .where(
                PRReview.repo_id == review.repo_id,
                PRReview.pr_number == review.pr_number,
                PRReview.id > review.id,
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return newer is None


def _validate_action_state(
    review: PRReview,
    finding: PRFinding,
    *,
    action_type: str,
) -> None:
    if (
        review.status not in _ACTIONABLE_REVIEW_STATUSES
        or not isinstance(review.head_sha, str)
        or len(review.head_sha) != 40
    ):
        raise FindingActionConflict(
            "This review snapshot is not available for finding actions"
        )
    allowed_statuses = {kind: {"open"} for kind in (
        "ignore", "human_advice", "ai_fix"
    )}
    if finding.status not in allowed_statuses[action_type]:
        raise FindingActionConflict(
            f"Finding cannot accept {action_type} from status {finding.status}"
        )


async def create_immediate_finding_action(
    db: AsyncSession,
    *,
    finding_id: int,
    review_id: int,
    action_type: str,
    idempotency_key: str,
    actor_user_id: int | None,
    human_advice: str | None = None,
) -> PRFindingAction:
    """Persist an ignore/advice decision without mutating the Panel gate."""

    if action_type not in {"ignore", "human_advice"}:
        raise ValueError("unsupported immediate finding action")
    existing = (
        await db.execute(
            select(PRFindingAction).where(
                PRFindingAction.idempotency_key == idempotency_key
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.finding_id != finding_id
            or existing.action_type != action_type
        ):
            raise FindingActionConflict("Idempotency key is already in use")
        return existing

    finding = (
        await db.execute(
            select(PRFinding)
            .where(
                PRFinding.id == finding_id,
                PRFinding.pr_review_id == review_id,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    review = (
        await db.execute(
            select(PRReview)
            .where(PRReview.id == review_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if finding is None or review is None:
        raise FindingActionConflict("Finding is no longer available")
    locked_repo = (
        await db.execute(
            select(MonitoredRepo)
            .where(MonitoredRepo.id == review.repo_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if locked_repo is None:
        raise FindingActionConflict("Review repository is no longer available")
    if not await is_current_review_snapshot(db, review):
        raise FindingActionConflict(
            "This finding belongs to a superseded PR snapshot"
        )
    _validate_action_state(review, finding, action_type=action_type)
    active_fix = (
        await db.execute(
            select(PRFindingAction.id)
            .where(
                PRFindingAction.finding_id == finding.id,
                PRFindingAction.action_type == "ai_fix",
                PRFindingAction.status.in_(_ACTIVE_FIX_STATUSES),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if active_fix is not None:
        raise FindingActionConflict(
            "Finding already has an active AI repair; complete or cancel it first"
        )

    now = datetime.utcnow()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type=action_type,
        status="completed",
        idempotency_key=idempotency_key,
        actor_user_id=actor_user_id,
        human_advice=human_advice,
        expected_head_sha=review.head_sha,
        result={"finding_status": (
            "ignored" if action_type == "ignore" else "advice_provided"
        )},
        completed_at=now,
    )
    db.add(action)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        winner = (
            await db.execute(
                select(PRFindingAction).where(
                    PRFindingAction.idempotency_key == idempotency_key
                )
            )
        ).scalar_one_or_none()
        if (
            winner is not None
            and winner.finding_id == finding_id
            and winner.action_type == action_type
        ):
            return winner
        raise FindingActionConflict("Idempotency key is already in use")
    await db.refresh(action)
    return action
