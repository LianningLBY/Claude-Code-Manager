import pytest
from sqlalchemy import select

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
)
from backend.services.pr_merge_queue import bind_merge_group, reconcile_merge_queue
from backend.services.pr_monitor_loop import record_gate_pass


BASE = "a" * 40
HEAD = "b" * 40
MERGE = "c" * 40


@pytest.mark.asyncio
async def test_fake_pr_enters_queue_checks_merge_group_and_confirms_merge(
    db_session, db_factory, monkeypatch
):
    repo = MonitoredRepo(
        repo_full_name="fake/queue", webhook_secret="s" * 64,
        review_mode="panel", wait_for_ci=True,
        required_checks=[{"kind": "check_run", "name": "tests", "app_slug": "github-actions"}],
        merge_queue_mode="auto",
    )
    db_session.add(repo)
    await db_session.commit()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=12, current_base_sha=BASE,
        current_head_sha=HEAD,
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=12,
        base_sha=BASE, head_sha=HEAD, pr_title="queue", pr_author="bot",
        pr_url="https://example.invalid/fake/queue/pull/12", status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    await db_session.commit()
    await record_gate_pass(db_session, review.id)
    action = (await db_session.execute(select(PRMergeQueueAction))).scalar_one()
    assert action.status == "pending"

    merged = False

    async def fake_pr_view(_number, _repo):
        return {
            "state": "MERGED" if merged else "OPEN",
            "mergedAt": "2026-08-04T00:00:00Z" if merged else None,
            "baseRefOid": BASE,
            "headRefOid": HEAD,
            "isDraft": False,
            "mergeCommit": {"oid": MERGE} if merged else None,
        }

    async def fake_graphql(_endpoint, *, payload=None, **_kwargs):
        query = payload["query"]
        if "mergeQueueEntry{id state}" in query and "enqueuePullRequest" not in query:
            return {"data": {"repository": {"pullRequest": {"id": "PR1", "mergeQueueEntry": None}}}}
        if "headRefOid" in query:
            return {"data": {"repository": {"pullRequest": {"id": "PR1", "headRefOid": HEAD}}}}
        return {"data": {"enqueuePullRequest": {"mergeQueueEntry": {"id": "MQ1", "state": "QUEUED"}}}}

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_review_service._gh_api_value", fake_graphql)
    assert await reconcile_merge_queue(db_factory) == 1
    action = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    assert action.status == "queued"
    assert action.github_queue_entry_id == "MQ1"

    assert await bind_merge_group(
        db_session, repo=repo, head_sha=MERGE,
        head_ref="refs/heads/gh-readonly-queue/main/pr-12-deadbeef",
    ) is True

    async def fake_ci(_repo, sha, _required):
        assert sha == MERGE
        return "passed", "Passed", {"head_sha": sha, "observed": []}

    monkeypatch.setattr("backend.services.pr_review_panel.fetch_exact_head_ci", fake_ci)
    assert await reconcile_merge_queue(db_factory) == 1
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert refreshed_run.status == "merge_group_passed"

    merged = True
    assert await reconcile_merge_queue(db_factory) == 1
    action = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert action.status == "merged"
    assert refreshed_run.status == "merged"
