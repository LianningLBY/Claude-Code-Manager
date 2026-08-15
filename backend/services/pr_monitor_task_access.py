"""Authorization identity for PR Monitor-owned execution Tasks.

PR Monitor Tasks are Controller implementation records, not collaborative
Project Tasks.  Their synthetic Project is only an execution/UI grouping and
must never turn a ``TeamProjectShare`` into access to raw reviewer prompts,
patches, or chat history.
"""

from __future__ import annotations

from sqlalchemy import or_, select


def pr_monitor_owned_task_predicate(task_id):
    """Return the durable SQL identity for every PR Monitor child Task."""

    from backend.models.pr_monitor import (
        PRFindingAction,
        PRFindingRebuttal,
        PRMonitorTaskTombstone,
        PRReview,
        PRReviewerRun,
    )

    return or_(
        select(PRReview.id)
        .where(PRReview.task_id == task_id)
        .correlate_except(PRReview)
        .exists(),
        select(PRReviewerRun.id)
        .where(PRReviewerRun.task_id == task_id)
        .correlate_except(PRReviewerRun)
        .exists(),
        select(PRFindingAction.id)
        .where(PRFindingAction.task_id == task_id)
        .correlate_except(PRFindingAction)
        .exists(),
        select(PRFindingRebuttal.id)
        .where(PRFindingRebuttal.task_id == task_id)
        .correlate_except(PRFindingRebuttal)
        .exists(),
        select(PRMonitorTaskTombstone.task_id)
        .where(PRMonitorTaskTombstone.task_id == task_id)
        .correlate_except(PRMonitorTaskTombstone)
        .exists(),
    )


async def is_pr_monitor_owned_task(db, task) -> bool:
    """Fail closed for linked and recognizably staged PR Monitor Tasks.

    The durable owner links classify legacy rows and survive presentation
    edits.  Runtime markers cover a corrupt/partially cleaned owner graph and
    the short interval after a Task is staged but before its owner field is
    assigned inside the same transaction.
    """

    from backend.services.pr_review_runtime import (
        is_pr_review_fix_task,
        is_pr_review_task,
    )

    # Pre-PR Code Review Capability Tasks have their own CodeReviewRun
    # ownership contract and remain ordinary creator-visible resources.  Do
    # not use the broader ``is_pr_sandbox_task`` runtime classifier here.
    if is_pr_review_task(task) or is_pr_review_fix_task(task):
        return True
    task_id = getattr(task, "id", None)
    if isinstance(task_id, bool) or not isinstance(task_id, int):
        return True
    linked = await db.scalar(
        select(task_id).where(pr_monitor_owned_task_predicate(task_id)).limit(1)
    )
    return linked is not None
