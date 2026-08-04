"""Durable GitHub Merge Queue controller for exact PR/merge-group subjects."""

from __future__ import annotations

import re
import secrets
from datetime import datetime, timedelta

from sqlalchemy import or_, select, update

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
)


_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")


async def _read_queue_entry(repo_name: str, pr_number: int) -> tuple[str, str] | None:
    from backend.services.pr_review_service import _gh_api_value

    owner, name = repo_name.split("/", 1)
    query = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id mergeQueueEntry{id state}}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": query,
        "variables": {"owner": owner, "name": name, "number": pr_number},
    })
    try:
        pr = result["data"]["repository"]["pullRequest"]
        entry = pr.get("mergeQueueEntry")
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub Merge Queue query is malformed") from exc
    if entry is None:
        return None
    if not isinstance(entry.get("id"), str) or not isinstance(entry.get("state"), str):
        raise ValueError("GitHub Merge Queue entry is malformed")
    return entry["id"], entry["state"]


async def _enqueue(repo_name: str, pr_number: int, head_sha: str) -> tuple[str, str]:
    from backend.services.pr_review_service import _gh_api_value

    existing = await _read_queue_entry(repo_name, pr_number)
    if existing is not None:
        return existing
    owner, name = repo_name.split("/", 1)
    node_query = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id headRefOid}}}"""
    node_result = await _gh_api_value("graphql", payload={
        "query": node_query,
        "variables": {"owner": owner, "name": name, "number": pr_number},
    })
    try:
        pr = node_result["data"]["repository"]["pullRequest"]
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub pull request node response is malformed") from exc
    if pr.get("headRefOid", "").lower() != head_sha or not isinstance(pr.get("id"), str):
        raise ValueError("GitHub pull request node is not the exact queued head")
    mutation = """mutation($pullRequestId:ID!,$expectedHeadOid:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$pullRequestId,expectedHeadOid:$expectedHeadOid}){mergeQueueEntry{id state}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": mutation,
        "variables": {"pullRequestId": pr["id"], "expectedHeadOid": head_sha},
    })
    try:
        entry = result["data"]["enqueuePullRequest"]["mergeQueueEntry"]
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub enqueuePullRequest response is malformed") from exc
    if not isinstance(entry.get("id"), str) or not isinstance(entry.get("state"), str):
        raise ValueError("GitHub did not confirm Merge Queue entry")
    return entry["id"], entry["state"]


async def bind_merge_group(
    db, *, repo: MonitoredRepo, head_sha: str, head_ref: str,
) -> bool:
    """Bind one signed merge_group webhook to one unambiguous queued PR."""
    if not _SHA_RE.fullmatch(head_sha) or not isinstance(head_ref, str):
        raise ValueError("merge_group subject is malformed")
    actions = list((await db.execute(
        select(PRMergeQueueAction)
        .join(PRMonitorRun, PRMonitorRun.id == PRMergeQueueAction.monitor_run_id)
        .where(
            PRMonitorRun.repo_id == repo.id,
            PRMergeQueueAction.status.in_(("queued", "checking")),
        )
    )).scalars())
    matches = []
    for action in actions:
        run = await db.get(PRMonitorRun, action.monitor_run_id)
        if run is not None and f"pr-{run.pr_number}-" in head_ref:
            matches.append((action, run))
    if len(matches) != 1:
        return False
    action, run = matches[0]
    action.merge_group_sha = head_sha
    action.merge_group_ref = head_ref[:500]
    action.status = "checking"
    action.ci_status = "pending"
    run.status = "merge_group_checking"
    run.state_version += 1
    await db.commit()
    return True


async def reconcile_merge_queue(db_factory) -> int:
    from backend.services.pr_review_panel import fetch_exact_head_ci
    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot

    progressed = 0
    async with db_factory() as db:
        action_ids = list((await db.execute(select(PRMergeQueueAction.id).where(
            PRMergeQueueAction.status.in_(("pending", "enqueuing", "queued", "checking"))
        ).order_by(PRMergeQueueAction.id))).scalars())
        for action_id_candidate in action_ids:
            action = await db.get(
                PRMergeQueueAction, action_id_candidate, populate_existing=True
            )
            if action is None or action.status not in {
                "pending", "enqueuing", "queued", "checking"
            }:
                continue
            run = await db.get(PRMonitorRun, action.monitor_run_id)
            review = await db.get(PRReview, action.review_id)
            repo = await db.get(MonitoredRepo, run.repo_id) if run else None
            if run is None or review is None or repo is None:
                continue
            if run.current_head_sha != action.trigger_head_sha or run.current_review_id != review.id:
                action.status = "superseded"
                action.completed_at = datetime.utcnow()
                continue
            try:
                snapshot = _validated_pr_snapshot(
                    await _gh_pr_view(run.pr_number, repo.repo_full_name)
                )
            except Exception as exc:
                action.last_error = f"merge_queue_pr_read_failed:{type(exc).__name__}"
                continue
            if snapshot["state"] == "MERGED":
                action.status = "merged"
                action.completed_at = datetime.utcnow()
                run.status = "merged"
                run.completed_at = datetime.utcnow()
                run.state_version += 1
                progressed += 1
                continue
            if (
                snapshot["state"] != "OPEN" or snapshot["is_draft"]
                or snapshot["head_sha"] != action.trigger_head_sha
            ):
                action.status = "paused"
                action.last_error = "merge_queue_pr_subject_changed"
                run.status = "paused"
                run.pause_reason = action.last_error
                continue
            if action.status in {"pending", "enqueuing"}:
                now = datetime.utcnow()
                lease_token = secrets.token_hex(24)
                action_id = action.id
                run_id = run.id
                enqueue_repo_name = repo.repo_full_name
                enqueue_pr_number = run.pr_number
                enqueue_head_sha = action.trigger_head_sha
                claimed = await db.execute(update(PRMergeQueueAction).where(
                    PRMergeQueueAction.id == action_id,
                    PRMergeQueueAction.status.in_(("pending", "enqueuing")),
                    or_(
                        PRMergeQueueAction.lease_token.is_(None),
                        PRMergeQueueAction.lease_expires_at < now,
                    ),
                ).values(
                    status="enqueuing",
                    attempt_count=PRMergeQueueAction.attempt_count + 1,
                    lease_token=lease_token,
                    lease_expires_at=now + timedelta(minutes=3),
                ))
                if claimed.rowcount != 1:
                    await db.rollback()
                    continue
                await db.commit()
                try:
                    entry_id, _state = await _enqueue(
                        enqueue_repo_name, enqueue_pr_number, enqueue_head_sha
                    )
                except Exception as exc:
                    action = await db.get(PRMergeQueueAction, action_id, populate_existing=True)
                    if action is not None and action.lease_token == lease_token:
                        action.last_error = f"merge_queue_enqueue_failed:{type(exc).__name__}:{str(exc)[:300]}"
                        action.lease_token = None
                        action.lease_expires_at = None
                    continue
                action = await db.get(PRMergeQueueAction, action_id, populate_existing=True)
                if action is None or action.lease_token != lease_token:
                    continue
                run = await db.get(PRMonitorRun, run_id, populate_existing=True)
                if run is None or run.current_head_sha != enqueue_head_sha:
                    action.status = "superseded"
                    action.lease_token = None
                    action.lease_expires_at = None
                    continue
                action.github_queue_entry_id = entry_id
                action.status = "queued"
                action.lease_token = None
                action.lease_expires_at = None
                action.last_error = None
                run.status = "merge_queued"
                run.state_version += 1
                progressed += 1
            elif action.status == "checking" and action.merge_group_sha:
                ci_status, summary, details = await fetch_exact_head_ci(
                    repo.repo_full_name,
                    action.merge_group_sha,
                    repo.required_checks,
                )
                action.ci_status = ci_status
                action.ci_details = details
                if ci_status == "failed":
                    conclusions = {
                        item.get("conclusion")
                        for item in details.get("observed", [])
                        if isinstance(item, dict) and item.get("state") == "failed"
                    }
                    infrastructure = bool(conclusions & {
                        "cancelled", "timed_out", "startup_failure", "stale", "action_required"
                    })
                    action.status = "paused" if infrastructure else "failed"
                    action.last_error = (
                        "merge_group_infrastructure_failed:" if infrastructure
                        else "merge_group_code_failed:"
                    ) + summary[:500]
                    if infrastructure:
                        run.status = "paused"
                        run.pause_reason = "merge_group_infrastructure_failed"
                    else:
                        review.ci_status = "failed"
                        review.ci_summary = f"Merge group {action.merge_group_sha}: {summary}"
                        review.ci_details = {
                            "subject_kind": "merge_group",
                            "merge_group_sha": action.merge_group_sha,
                            **details,
                        }
                        await db.flush()
                        from backend.services.pr_monitor_loop import record_blocking_evidence
                        await record_blocking_evidence(
                            db, review_id=review.id,
                            reason_kind="merge_group_ci_failed",
                        )
                elif ci_status == "passed":
                    run.status = "merge_group_passed"
                progressed += 1
        await db.commit()
    return progressed
