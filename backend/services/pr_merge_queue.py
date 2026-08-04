"""Durable GitHub Merge Queue controller for exact PR/merge-group subjects."""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import quote

from sqlalchemy import func, or_, select, update

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
)


_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_QUEUE_ENTRY_STATES = {
    "QUEUED",
    "AWAITING_CHECKS",
    "MERGEABLE",
    "UNMERGEABLE",
    "LOCKED",
}
_QUEUE_ENTRY_BLOCKED_STATES = {"UNMERGEABLE", "LOCKED"}


async def _database_now(db) -> datetime:
    value = (await db.execute(select(func.current_timestamp()))).scalar_one()
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("Database clock returned an invalid timestamp")
    return value.replace(tzinfo=None)


@dataclass(frozen=True)
class QueueEntry:
    id: str
    state: str
    base_sha: str
    head_sha: str


async def _read_queue_entry(repo_name: str, pr_number: int) -> QueueEntry | None:
    from backend.services.pr_review_service import _gh_api_value

    owner, name = repo_name.split("/", 1)
    query = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id mergeQueueEntry{id state baseCommit{oid} headCommit{oid}}}}}"""
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
    base_commit = entry.get("baseCommit")
    head_commit = entry.get("headCommit")
    base_sha = base_commit.get("oid") if isinstance(base_commit, dict) else None
    head_sha = head_commit.get("oid") if isinstance(head_commit, dict) else None
    if (
        not isinstance(entry.get("id"), str)
        or not entry["id"]
        or not isinstance(entry.get("state"), str)
        or entry["state"].upper() not in _QUEUE_ENTRY_STATES
        or not isinstance(base_sha, str)
        or _SHA_RE.fullmatch(base_sha.lower()) is None
        or not isinstance(head_sha, str)
        or _SHA_RE.fullmatch(head_sha.lower()) is None
    ):
        raise ValueError("GitHub Merge Queue entry is malformed")
    return QueueEntry(
        id=entry["id"],
        state=entry["state"].upper(),
        base_sha=base_sha.lower(),
        head_sha=head_sha.lower(),
    )


async def _read_merge_group_ref(
    repo_name: str,
    *,
    default_branch: str,
    pr_number: int,
) -> tuple[str, str] | None:
    """Resolve the one current synthetic merge-group ref for a PR."""

    from backend.services.pr_review_service import _gh_api_value

    short_prefix = f"gh-readonly-queue/{default_branch}/pr-{pr_number}-"
    endpoint_prefix = quote(f"heads/{short_prefix}", safe="/")
    value = await _gh_api_value(
        f"repos/{repo_name}/git/matching-refs/{endpoint_prefix}",
        max_output_bytes=4 * 1024 * 1024,
    )
    if not isinstance(value, list):
        raise ValueError("GitHub matching-refs response is malformed")
    full_prefix = f"refs/heads/{short_prefix}"
    matches: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("GitHub matching-ref item is malformed")
        ref = item.get("ref")
        obj = item.get("object")
        sha = obj.get("sha") if isinstance(obj, dict) else None
        if (
            not isinstance(ref, str)
            or not ref.startswith(full_prefix)
            or len(ref) > 500
            or not isinstance(sha, str)
            or _SHA_RE.fullmatch(sha.lower()) is None
        ):
            raise ValueError("GitHub matching-ref identity is malformed")
        matches.append((sha.lower(), ref))
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError("GitHub Merge Queue ref is ambiguous")
    return matches[0]


async def _enqueue(
    repo_name: str,
    pr_number: int,
    base_sha: str,
    head_sha: str,
) -> QueueEntry:
    from backend.services.pr_review_service import _gh_api_value

    existing = await _read_queue_entry(repo_name, pr_number)
    if existing is not None:
        if existing.base_sha != base_sha or existing.head_sha != head_sha:
            raise ValueError("Existing Merge Queue entry is not the exact subject")
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
    if (
        not isinstance(entry.get("id"), str)
        or not entry["id"]
        or not isinstance(entry.get("state"), str)
        or entry["state"].upper() not in _QUEUE_ENTRY_STATES
    ):
        raise ValueError("GitHub did not confirm Merge Queue entry")
    # Re-read the durable entry because the mutation response does not expose
    # its exact base/head commits.  Queue admission is not accepted without
    # proving both immutable subject components.
    confirmed = await _read_queue_entry(repo_name, pr_number)
    if (
        confirmed is None
        or confirmed.id != entry["id"]
        or confirmed.base_sha != base_sha
        or confirmed.head_sha != head_sha
    ):
        raise ValueError("GitHub did not confirm the exact queued subject")
    return confirmed


async def bind_merge_group(
    db, *, repo: MonitoredRepo, head_sha: str, head_ref: str,
) -> bool:
    """Bind one signed merge_group webhook to one unambiguous queued PR."""
    if not repo.enabled:
        return False
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
        expected_prefix = (
            f"refs/heads/gh-readonly-queue/{repo.default_branch}/"
            f"pr-{run.pr_number}-"
            if run is not None
            else ""
        )
        if run is not None and head_ref.startswith(expected_prefix):
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
        # Each durable action owns its transaction. A lost lease or rollback
        # on a later action must never erase an earlier action's progress.
        async with db_factory() as db:
            preliminary = await db.get(PRMergeQueueAction, action_id_candidate)
            preliminary_run = (
                await db.get(PRMonitorRun, preliminary.monitor_run_id)
                if preliminary is not None
                else None
            )
            if preliminary is None or preliminary_run is None:
                await db.rollback()
                continue
            repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == preliminary_run.repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            action = (await db.execute(
                select(PRMergeQueueAction)
                .where(PRMergeQueueAction.id == action_id_candidate)
                .with_for_update()
            )).scalar_one_or_none()
            run = (
                (await db.execute(
                    select(PRMonitorRun)
                    .where(PRMonitorRun.id == action.monitor_run_id)
                    .with_for_update()
                )).scalar_one_or_none()
                if action is not None
                else None
            )
            review = await db.get(PRReview, action.review_id) if action is not None else None
            if (
                action is None
                or action.status not in {"pending", "enqueuing", "queued", "checking"}
                or run is None
                or review is None
                or repo is None
            ):
                await db.rollback()
                continue
            if not repo.enabled:
                action.status = "paused"
                action.last_error = "repo_disabled"
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue
            if (
                run.current_base_sha != action.trigger_base_sha
                or run.current_head_sha != action.trigger_head_sha
                or run.current_review_id != review.id
            ):
                action.status = "superseded"
                action.completed_at = datetime.utcnow()
                await db.commit()
                continue
            try:
                snapshot = _validated_pr_snapshot(
                    await _gh_pr_view(run.pr_number, repo.repo_full_name)
                )
            except Exception as exc:
                action.last_error = (
                    f"merge_queue_pr_read_failed:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                await db.commit()
                continue
            if snapshot["state"] == "MERGED":
                action.status = "merged"
                action.completed_at = datetime.utcnow()
                action.last_error = None
                run.status = "merged"
                run.completed_at = datetime.utcnow()
                run.state_version += 1
                await db.commit()
                progressed += 1
                continue
            if (
                snapshot["state"] != "OPEN"
                or snapshot["is_draft"]
                or snapshot["base_sha"] != action.trigger_base_sha
                or snapshot["head_sha"] != action.trigger_head_sha
            ):
                action.status = "paused"
                action.last_error = "merge_queue_pr_subject_changed"
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue

            if action.status in {"pending", "enqueuing"}:
                now = await _database_now(db)
                lease_token = secrets.token_hex(24)
                action_id = action.id
                run_id = run.id
                enqueue_repo_name = repo.repo_full_name
                enqueue_pr_number = run.pr_number
                enqueue_base_sha = action.trigger_base_sha
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
                    lease_expires_at=now + timedelta(minutes=10),
                ))
                if claimed.rowcount != 1:
                    await db.rollback()
                    continue
                await db.commit()
                try:
                    entry = await _enqueue(
                        enqueue_repo_name,
                        enqueue_pr_number,
                        enqueue_base_sha,
                        enqueue_head_sha,
                    )
                except Exception as exc:
                    action = await db.get(PRMergeQueueAction, action_id, populate_existing=True)
                    if action is not None and action.lease_token == lease_token:
                        action.last_error = (
                            f"merge_queue_enqueue_failed:{type(exc).__name__}:"
                            f"{str(exc)[:300]}"
                        )
                        action.lease_token = None
                        action.lease_expires_at = None
                        await db.commit()
                    else:
                        await db.rollback()
                    continue
                action = await db.get(PRMergeQueueAction, action_id, populate_existing=True)
                run = await db.get(PRMonitorRun, run_id, populate_existing=True)
                if action is None or action.lease_token != lease_token:
                    await db.rollback()
                    continue
                if (
                    run is None
                    or run.current_base_sha != enqueue_base_sha
                    or run.current_head_sha != enqueue_head_sha
                ):
                    action.status = "superseded"
                    action.lease_token = None
                    action.lease_expires_at = None
                    await db.commit()
                    continue
                action.github_queue_entry_id = entry.id
                action.lease_token = None
                action.lease_expires_at = None
                if entry.state in _QUEUE_ENTRY_BLOCKED_STATES:
                    action.status = "paused"
                    action.last_error = f"merge_queue_entry_{entry.state.lower()}"
                    run.status = "paused"
                    run.pause_reason = action.last_error
                else:
                    action.status = "queued"
                    action.last_error = None
                    run.status = "merge_queued"
                    run.pause_reason = None
                run.state_version += 1
                await db.commit()
                progressed += 1
                continue

            try:
                entry = await _read_queue_entry(repo.repo_full_name, run.pr_number)
            except Exception as exc:
                action.last_error = (
                    f"merge_queue_entry_read_failed:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                await db.commit()
                continue
            entry_subject_changed = entry is not None and (
                entry.base_sha != action.trigger_base_sha
                or entry.head_sha != action.trigger_head_sha
            )
            if entry is None or entry_subject_changed:
                # Queue removal commonly races the final merge. Re-read the PR
                # after observing the gap so a completed merge cannot become a
                # permanently paused action that is no longer reconciled.
                try:
                    fresh_snapshot = _validated_pr_snapshot(
                        await _gh_pr_view(run.pr_number, repo.repo_full_name)
                    )
                except Exception as exc:
                    action.last_error = (
                        f"merge_queue_pr_reread_failed:{type(exc).__name__}:"
                        f"{str(exc)[:300]}"
                    )
                    await db.commit()
                    continue
                if (
                    fresh_snapshot["state"] == "MERGED"
                    and fresh_snapshot["base_sha"] == action.trigger_base_sha
                    and fresh_snapshot["head_sha"] == action.trigger_head_sha
                ):
                    action.status = "merged"
                    action.completed_at = datetime.utcnow()
                    action.last_error = None
                    run.status = "merged"
                    run.completed_at = datetime.utcnow()
                    run.pause_reason = None
                    run.state_version += 1
                    await db.commit()
                    progressed += 1
                    continue
                action.status = "paused"
                action.last_error = (
                    "merge_queue_entry_subject_changed"
                    if entry_subject_changed
                    else "merge_queue_entry_disappeared"
                )
                if entry is not None:
                    action.github_queue_entry_id = entry.id
                action.merge_group_sha = None
                action.merge_group_ref = None
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue
            if entry.state in _QUEUE_ENTRY_BLOCKED_STATES:
                action.status = "paused"
                action.last_error = f"merge_queue_entry_{entry.state.lower()}"
                action.github_queue_entry_id = entry.id
                action.merge_group_sha = None
                action.merge_group_ref = None
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue
            entry_changed = action.github_queue_entry_id != entry.id
            action.github_queue_entry_id = entry.id
            try:
                merge_group = await _read_merge_group_ref(
                    repo.repo_full_name,
                    default_branch=repo.default_branch,
                    pr_number=run.pr_number,
                )
            except ValueError as exc:
                action.status = "paused"
                action.last_error = f"merge_group_ref_invalid:{str(exc)[:300]}"
                run.status = "paused"
                run.pause_reason = "merge_group_ref_invalid"
                run.state_version += 1
                await db.commit()
                continue
            except Exception as exc:
                action.last_error = (
                    f"merge_group_ref_read_failed:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                await db.commit()
                continue
            if merge_group is None:
                changed = (
                    action.status != "queued"
                    or action.merge_group_sha is not None
                    or entry_changed
                )
                action.status = "queued"
                action.merge_group_sha = None
                action.merge_group_ref = None
                action.ci_status = None
                action.ci_details = None
                action.last_error = None
                run.status = "merge_queued"
                run.pause_reason = None
                if changed:
                    run.state_version += 1
                    progressed += 1
                await db.commit()
                continue

            merge_sha, merge_ref = merge_group
            group_changed = (
                action.merge_group_sha != merge_sha
                or action.merge_group_ref != merge_ref
                or action.status != "checking"
                or entry_changed
            )
            action.merge_group_sha = merge_sha
            action.merge_group_ref = merge_ref
            action.status = "checking"
            if group_changed:
                action.ci_status = "pending"
                action.ci_details = None
                run.status = "merge_group_checking"
                run.pause_reason = None
                run.state_version += 1
            try:
                ci_status, summary, details = await fetch_exact_head_ci(
                    repo.repo_full_name,
                    merge_sha,
                    repo.required_checks or [],
                )
            except Exception as exc:
                action.last_error = (
                    "merge_group_ci_read_failed:"
                    f"{type(exc).__name__}:{str(exc)[:300]}"
                )
                await db.commit()
                if group_changed:
                    progressed += 1
                continue
            action.ci_status = ci_status
            action.ci_details = details
            action.last_error = None
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
                    review.ci_summary = f"Merge group {merge_sha}: {summary}"
                    review.ci_details = {
                        "subject_kind": "merge_group",
                        "merge_group_sha": merge_sha,
                        **details,
                    }
                    await db.flush()
                    from backend.services.pr_monitor_loop import record_blocking_evidence

                    await record_blocking_evidence(
                        db,
                        review_id=review.id,
                        reason_kind="merge_group_ci_failed",
                    )
            elif ci_status == "passed":
                run.status = "merge_group_passed"
                run.pause_reason = None
            await db.commit()
            progressed += 1
    return progressed
