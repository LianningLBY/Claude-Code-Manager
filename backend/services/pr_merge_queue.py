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
    created_by_call: bool = False
    pull_request_id: str | None = None


class QueueEntryCleanupError(RuntimeError):
    """A new remote queue entry could not be proven removed."""

    def __init__(self, message: str, *, entry_id: str):
        super().__init__(message)
        self.entry_id = entry_id


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
        if not isinstance(pr, dict):
            raise TypeError
        entry = pr.get("mergeQueueEntry")
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub Merge Queue query is malformed") from exc
    if entry is None:
        return None
    if not isinstance(entry, dict):
        raise ValueError("GitHub Merge Queue entry is malformed")
    base_commit = entry.get("baseCommit")
    head_commit = entry.get("headCommit")
    base_sha = base_commit.get("oid") if isinstance(base_commit, dict) else None
    head_sha = head_commit.get("oid") if isinstance(head_commit, dict) else None
    if (
        not isinstance(pr.get("id"), str)
        or not pr["id"]
        or not isinstance(entry.get("id"), str)
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
        pull_request_id=pr["id"],
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


async def _dequeue_queue_entry(
    repo_name: str,
    pr_number: int,
    pull_request_id: str,
    entry_id: str,
) -> None:
    """Remove one exact queue entry and prove that exact id disappeared."""

    from backend.services.pr_review_service import _gh_api_value

    mutation = """mutation($id:ID!){dequeuePullRequest(input:{id:$id}){mergeQueueEntry{id}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": mutation,
        "variables": {"id": pull_request_id},
    })
    try:
        payload = result["data"]["dequeuePullRequest"]
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub dequeuePullRequest response is malformed") from exc
    if not isinstance(payload, dict):
        raise ValueError("GitHub did not confirm Merge Queue dequeue")
    remaining = await _read_queue_entry(repo_name, pr_number)
    if remaining is not None and remaining.id == entry_id:
        raise ValueError("GitHub Merge Queue entry remained after dequeue")


async def _remove_new_queue_entry_or_raise(
    repo_name: str,
    pr_number: int,
    pull_request_id: str,
    entry_id: str,
    *,
    reason: str,
) -> None:
    try:
        await _dequeue_queue_entry(
            repo_name, pr_number, pull_request_id, entry_id
        )
    except Exception as exc:
        raise QueueEntryCleanupError(
            f"{reason}; exact remote cleanup failed: "
            f"{type(exc).__name__}:{str(exc)[:300]}",
            entry_id=entry_id,
        ) from exc


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
    node_query = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id baseRefOid headRefOid}}}"""
    node_result = await _gh_api_value("graphql", payload={
        "query": node_query,
        "variables": {"owner": owner, "name": name, "number": pr_number},
    })
    try:
        pr = node_result["data"]["repository"]["pullRequest"]
        if not isinstance(pr, dict):
            raise TypeError
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub pull request node response is malformed") from exc
    if (
        not isinstance(pr.get("id"), str)
        or not pr["id"]
        or not isinstance(pr.get("baseRefOid"), str)
        or pr["baseRefOid"].lower() != base_sha
        or not isinstance(pr.get("headRefOid"), str)
        or pr["headRefOid"].lower() != head_sha
    ):
        raise ValueError("GitHub pull request node is not the exact queued subject")
    mutation = """mutation($pullRequestId:ID!,$expectedHeadOid:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$pullRequestId,expectedHeadOid:$expectedHeadOid}){mergeQueueEntry{id state}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": mutation,
        "variables": {"pullRequestId": pr["id"], "expectedHeadOid": head_sha},
    })
    try:
        entry = result["data"]["enqueuePullRequest"]["mergeQueueEntry"]
        if not isinstance(entry, dict):
            raise TypeError
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
    try:
        confirmed = await _read_queue_entry(repo_name, pr_number)
    except Exception as exc:
        await _remove_new_queue_entry_or_raise(
            repo_name,
            pr_number,
            pr["id"],
            entry["id"],
            reason=(
                "GitHub queue subject confirmation failed: "
                f"{type(exc).__name__}:{str(exc)[:300]}"
            ),
        )
        raise ValueError(
            "GitHub queue subject confirmation failed; new entry was removed"
        ) from exc
    if (
        confirmed is None
        or confirmed.id != entry["id"]
        or confirmed.base_sha != base_sha
        or confirmed.head_sha != head_sha
    ):
        await _remove_new_queue_entry_or_raise(
            repo_name,
            pr_number,
            pr["id"],
            entry["id"],
            reason="GitHub did not confirm the exact queued subject",
        )
        raise ValueError(
            "GitHub did not confirm the exact queued subject; new entry was removed"
        )
    return QueueEntry(
        id=confirmed.id,
        state=confirmed.state,
        base_sha=confirmed.base_sha,
        head_sha=confirmed.head_sha,
        created_by_call=True,
        pull_request_id=pr["id"],
    )


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


async def _lock_queue_effect_rows(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
):
    """Fresh ``Repo -> Action -> Run -> Review`` effect barrier."""

    await db.rollback()
    repo = (await db.execute(
        select(MonitoredRepo)
        .where(MonitoredRepo.id == repo_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    action = (await db.execute(
        select(PRMergeQueueAction)
        .where(PRMergeQueueAction.id == action_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    run = (await db.execute(
        select(PRMonitorRun)
        .where(PRMonitorRun.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    review = (await db.execute(
        select(PRReview)
        .where(PRReview.id == review_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    return repo, action, run, review


async def _record_enqueue_failure(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
    lease_token: str,
    message: str,
    unsafe_entry_id: str | None = None,
) -> None:
    repo, action, run, review = await _lock_queue_effect_rows(
        db,
        repo_id=repo_id,
        action_id=action_id,
        run_id=run_id,
        review_id=review_id,
    )
    if action is None:
        await db.rollback()
        return
    if unsafe_entry_id is not None:
        # Remote state could still merge.  Keep that risk explicit and never
        # let a later retry create a second queue effect automatically.
        if action.status not in {"merged", "superseded"}:
            action.status = "paused"
            action.last_error = message[:1000]
            action.github_queue_entry_id = unsafe_entry_id
            action.lease_token = None
            action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = message[:1000]
            run.state_version += 1
        await db.commit()
        return
    if action.status == "enqueuing" and action.lease_token == lease_token:
        action.last_error = message[:1000]
        action.lease_token = None
        action.lease_expires_at = None
        await db.commit()
    else:
        await db.rollback()


async def _abort_enqueue_after_lifecycle_change(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
    repo_name: str,
    pr_number: int,
    lease_token: str,
    entry: QueueEntry,
    reason: str,
) -> None:
    cleanup_error: str | None = None
    if entry.created_by_call:
        try:
            if entry.pull_request_id is None:
                raise ValueError("new queue entry has no pull request node id")
            await _dequeue_queue_entry(
                repo_name, pr_number, entry.pull_request_id, entry.id
            )
        except Exception as exc:
            cleanup_error = (
                "merge_queue_remote_cleanup_failed:"
                f"{type(exc).__name__}:{str(exc)[:500]}"
            )

    repo, action, run, review = await _lock_queue_effect_rows(
        db,
        repo_id=repo_id,
        action_id=action_id,
        run_id=run_id,
        review_id=review_id,
    )
    if action is None:
        await db.rollback()
        return
    if cleanup_error is not None:
        if action.status not in {"merged", "superseded"}:
            action.status = "paused"
            action.last_error = cleanup_error
            action.github_queue_entry_id = entry.id
            action.lease_token = None
            action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = cleanup_error
            run.state_version += 1
        await db.commit()
        return

    if not entry.created_by_call:
        # This exact entry predated CCM's call (for example a manual enqueue),
        # so ownership is not ours to revoke.  Surface it without pretending
        # the local pause disabled the remote entry.
        message = f"merge_queue_existing_entry_lifecycle_changed:{reason}"
        if action.status not in {"merged", "superseded"}:
            action.status = "paused"
            action.last_error = message[:1000]
            action.github_queue_entry_id = entry.id
            action.lease_token = None
            action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = message[:1000]
            run.state_version += 1
        await db.commit()
        return

    # Our new entry was proven absent.  Preserve a concurrent pause/disable;
    # only withdraw the exact lease still owned by this reconciler.
    if action.status == "enqueuing" and action.lease_token == lease_token:
        action.status = "paused"
        action.last_error = f"merge_queue_enqueue_aborted:{reason}"[:1000]
        action.lease_token = None
        action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"paused", "merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = action.last_error
            run.state_version += 1
    await db.commit()


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
                repo_id = repo.id
                review_id = review.id
                enqueue_repo_name = repo.repo_full_name
                enqueue_pr_number = run.pr_number
                enqueue_base_sha = action.trigger_base_sha
                enqueue_head_sha = action.trigger_head_sha
                expected_run_status = run.status
                expected_run_state_version = run.state_version
                expected_review_status = review.status
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
                except QueueEntryCleanupError as exc:
                    await _record_enqueue_failure(
                        db,
                        repo_id=repo_id,
                        action_id=action_id,
                        run_id=run_id,
                        review_id=review_id,
                        lease_token=lease_token,
                        message=(
                            "merge_queue_remote_cleanup_failed:"
                            f"{type(exc).__name__}:{str(exc)[:700]}"
                        ),
                        unsafe_entry_id=exc.entry_id,
                    )
                    continue
                except Exception as exc:
                    await _record_enqueue_failure(
                        db,
                        repo_id=repo_id,
                        action_id=action_id,
                        run_id=run_id,
                        review_id=review_id,
                        lease_token=lease_token,
                        message=(
                            f"merge_queue_enqueue_failed:{type(exc).__name__}:"
                            f"{str(exc)[:300]}"
                        ),
                    )
                    continue
                repo, action, run, review = await _lock_queue_effect_rows(
                    db,
                    repo_id=repo_id,
                    action_id=action_id,
                    run_id=run_id,
                    review_id=review_id,
                )
                finalize_now = await _database_now(db)
                lifecycle_error = None
                if repo is None or action is None or run is None or review is None:
                    lifecycle_error = "lifecycle_missing"
                elif not repo.enabled:
                    lifecycle_error = "repo_disabled"
                elif repo.merge_queue_mode != "auto":
                    lifecycle_error = "merge_queue_policy_changed"
                elif action.status != "enqueuing":
                    lifecycle_error = f"action_{action.status}"
                elif action.lease_token != lease_token:
                    lifecycle_error = "lease_owner_changed"
                elif (
                    action.lease_expires_at is None
                    or action.lease_expires_at <= finalize_now
                ):
                    lifecycle_error = "lease_expired"
                elif (
                    action.monitor_run_id != run.id
                    or action.review_id != review.id
                    or action.trigger_base_sha != enqueue_base_sha
                    or action.trigger_head_sha != enqueue_head_sha
                ):
                    lifecycle_error = "action_subject_changed"
                elif (
                    run.status != expected_run_status
                    or run.state_version != expected_run_state_version
                    or run.current_base_sha != enqueue_base_sha
                    or run.current_head_sha != enqueue_head_sha
                    or run.current_review_id != review.id
                ):
                    lifecycle_error = "run_changed"
                elif (
                    review.monitor_run_id != run.id
                    or review.status != expected_review_status
                    or review.base_sha != enqueue_base_sha
                    or review.head_sha != enqueue_head_sha
                ):
                    lifecycle_error = "review_changed"
                if lifecycle_error is not None:
                    # A newer lease owner may already have adopted this exact
                    # entry.  It alone decides the effect; the stale caller
                    # must neither finalize nor dequeue it.
                    newer_owner = bool(
                        action is not None
                        and action.status == "enqueuing"
                        and action.lease_token not in {None, lease_token}
                    )
                    await db.commit()
                    if not newer_owner:
                        await _abort_enqueue_after_lifecycle_change(
                            db,
                            repo_id=repo_id,
                            action_id=action_id,
                            run_id=run_id,
                            review_id=review_id,
                            repo_name=enqueue_repo_name,
                            pr_number=enqueue_pr_number,
                            lease_token=lease_token,
                            entry=entry,
                            reason=lifecycle_error,
                        )
                    continue

                action_status = (
                    "paused"
                    if entry.state in _QUEUE_ENTRY_BLOCKED_STATES
                    else "queued"
                )
                action_error = (
                    f"merge_queue_entry_{entry.state.lower()}"
                    if entry.state in _QUEUE_ENTRY_BLOCKED_STATES
                    else None
                )
                action_changed = await db.execute(
                    update(PRMergeQueueAction)
                    .where(
                        PRMergeQueueAction.id == action.id,
                        PRMergeQueueAction.monitor_run_id == run.id,
                        PRMergeQueueAction.review_id == review.id,
                        PRMergeQueueAction.status == "enqueuing",
                        PRMergeQueueAction.lease_token == lease_token,
                        PRMergeQueueAction.lease_expires_at.is_not(None),
                        PRMergeQueueAction.lease_expires_at > finalize_now,
                        PRMergeQueueAction.trigger_base_sha == enqueue_base_sha,
                        PRMergeQueueAction.trigger_head_sha == enqueue_head_sha,
                    )
                    .values(
                        github_queue_entry_id=entry.id,
                        lease_token=None,
                        lease_expires_at=None,
                        status=action_status,
                        last_error=action_error,
                    )
                    .execution_options(synchronize_session=False)
                )
                if action_changed.rowcount != 1:
                    await db.rollback()
                    await _abort_enqueue_after_lifecycle_change(
                        db,
                        repo_id=repo_id,
                        action_id=action_id,
                        run_id=run_id,
                        review_id=review_id,
                        repo_name=enqueue_repo_name,
                        pr_number=enqueue_pr_number,
                        lease_token=lease_token,
                        entry=entry,
                        reason="action_cas_lost",
                    )
                    continue
                run_status = (
                    "paused"
                    if entry.state in _QUEUE_ENTRY_BLOCKED_STATES
                    else "merge_queued"
                )
                run_changed = await db.execute(
                    update(PRMonitorRun)
                    .where(
                        PRMonitorRun.id == run.id,
                        PRMonitorRun.repo_id == repo.id,
                        PRMonitorRun.status == expected_run_status,
                        PRMonitorRun.state_version == expected_run_state_version,
                        PRMonitorRun.current_base_sha == enqueue_base_sha,
                        PRMonitorRun.current_head_sha == enqueue_head_sha,
                        PRMonitorRun.current_review_id == review.id,
                    )
                    .values(
                        status=run_status,
                        pause_reason=action_error,
                        state_version=PRMonitorRun.state_version + 1,
                    )
                    .execution_options(synchronize_session=False)
                )
                if run_changed.rowcount != 1:
                    await db.rollback()
                    await _abort_enqueue_after_lifecycle_change(
                        db,
                        repo_id=repo_id,
                        action_id=action_id,
                        run_id=run_id,
                        review_id=review_id,
                        repo_name=enqueue_repo_name,
                        pr_number=enqueue_pr_number,
                        lease_token=lease_token,
                        entry=entry,
                        reason="run_cas_lost",
                    )
                    continue
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
