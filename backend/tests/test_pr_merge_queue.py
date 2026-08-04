import pytest
from sqlalchemy import select

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
)
from backend.services.pr_merge_queue import (
    QueueEntry,
    QueueEntryCleanupError,
    _enqueue,
    bind_merge_group,
    reconcile_merge_queue,
)
from backend.services.pr_monitor_loop import record_gate_pass


BASE = "a" * 40
HEAD = "b" * 40
MERGE = "c" * 40


async def _seed_queue_action(
    db,
    *,
    name: str,
    number: int,
    status: str = "queued",
    base_sha: str = BASE,
    head_sha: str = HEAD,
    merge_sha: str | None = None,
):
    repo = MonitoredRepo(
        repo_full_name=f"fake/{name}",
        webhook_secret="s" * 64,
        review_mode="panel",
        wait_for_ci=True,
        required_checks=[{
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }],
        merge_queue_mode="auto",
    )
    db.add(repo)
    await db.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=number,
        current_base_sha=base_sha,
        current_head_sha=head_sha,
        status="merge_group_checking" if status == "checking" else "merge_queued",
    )
    db.add(run)
    await db.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=number,
        base_sha=base_sha,
        head_sha=head_sha,
        pr_title=name,
        pr_author="bot",
        pr_url=f"https://example.invalid/{name}/pull/{number}",
        status="commented",
    )
    db.add(review)
    await db.flush()
    run.current_review_id = review.id
    action = PRMergeQueueAction(
        monitor_run_id=run.id,
        review_id=review.id,
        trigger_base_sha=base_sha,
        trigger_head_sha=head_sha,
        status=status,
        action_nonce=f"{number:024d}"[-24:],
        github_queue_entry_id=f"MQ-{number}",
        merge_group_sha=merge_sha,
        merge_group_ref=(
            f"refs/heads/gh-readonly-queue/main/pr-{number}-old"
            if merge_sha
            else None
        ),
    )
    db.add(action)
    await db.commit()
    return repo, run, review, action


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

    async def fake_enqueue(_repo, _number, base_sha, head_sha):
        assert (base_sha, head_sha) == (BASE, HEAD)
        return QueueEntry("MQ1", "QUEUED", BASE, HEAD)

    async def fake_entry(_repo, _number):
        return QueueEntry("MQ1", "AWAITING_CHECKS", BASE, HEAD)

    async def fake_group(_repo, *, default_branch, pr_number):
        assert (default_branch, pr_number) == ("main", 12)
        return MERGE, "refs/heads/gh-readonly-queue/main/pr-12-deadbeef"

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._enqueue", fake_enqueue)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", fake_entry)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_merge_group_ref", fake_group)
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


@pytest.mark.asyncio
async def test_merge_queue_pauses_when_remote_base_changes_without_head_change(
    db_session, db_factory, monkeypatch
):
    repo = MonitoredRepo(
        repo_full_name="fake/base-drift", webhook_secret="s" * 64,
        review_mode="panel", merge_queue_mode="auto",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=13, current_base_sha=BASE,
        current_head_sha=HEAD,
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=13,
        base_sha=BASE, head_sha=HEAD, pr_title="queue", pr_author="bot",
        pr_url="https://example.invalid/fake/base-drift/pull/13",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    await db_session.commit()
    await record_gate_pass(db_session, review.id)
    action = (await db_session.execute(select(PRMergeQueueAction))).scalar_one()

    async def fake_pr_view(_number, _repo):
        return {
            "state": "OPEN", "mergedAt": None,
            "baseRefOid": "d" * 40, "headRefOid": HEAD,
            "isDraft": False, "mergeCommit": None,
        }

    enqueue_calls = 0

    async def fake_enqueue(*_args, **_kwargs):
        nonlocal enqueue_calls
        enqueue_calls += 1
        return "MQ-UNEXPECTED", "QUEUED"

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._enqueue", fake_enqueue)

    assert await reconcile_merge_queue(db_factory) == 0
    action = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert action.status == "paused"
    assert action.last_error == "merge_queue_pr_subject_changed"
    assert run.status == "paused"
    assert enqueue_calls == 0


@pytest.mark.asyncio
async def test_merge_queue_ci_read_failure_does_not_starve_later_actions(
    db_session, db_factory, monkeypatch
):
    rows = []
    for index, (base_sha, head_sha, merge_sha) in enumerate((
        (BASE, HEAD, MERGE),
        ("d" * 40, "e" * 40, "f" * 40),
    ), start=20):
        repo = MonitoredRepo(
            repo_full_name=f"fake/queue-{index}", webhook_secret="s" * 64,
            review_mode="panel", merge_queue_mode="auto",
        )
        db_session.add(repo)
        await db_session.flush()
        run = PRMonitorRun(
            repo_id=repo.id, pr_number=index, current_base_sha=base_sha,
            current_head_sha=head_sha, status="merge_group_checking",
        )
        db_session.add(run)
        await db_session.flush()
        review = PRReview(
            monitor_run_id=run.id, repo_id=repo.id, pr_number=index,
            base_sha=base_sha, head_sha=head_sha, pr_title="queue",
            pr_author="bot", pr_url=f"https://example.invalid/pull/{index}",
            status="commented",
        )
        db_session.add(review)
        await db_session.flush()
        run.current_review_id = review.id
        action = PRMergeQueueAction(
            monitor_run_id=run.id, review_id=review.id,
            trigger_base_sha=base_sha, trigger_head_sha=head_sha,
                status="checking", action_nonce=str(index) * 24,
                merge_group_sha=merge_sha,
                merge_group_ref=f"refs/heads/gh-readonly-queue/main/pr-{index}-group",
                github_queue_entry_id=f"MQ-{index}",
            )
        db_session.add(action)
        rows.append((repo, run, action))
    await db_session.commit()

    async def fake_pr_view(number, _repo):
        repo, run, _action = next(row for row in rows if row[1].pr_number == number)
        return {
            "state": "OPEN", "mergedAt": None,
            "baseRefOid": run.current_base_sha,
            "headRefOid": run.current_head_sha,
            "isDraft": False, "mergeCommit": None,
        }

    async def fake_ci(_repo, sha, _required):
        if sha == MERGE:
            raise RuntimeError("temporary GitHub failure")
        return "passed", "Passed", {"head_sha": sha, "observed": []}

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_review_panel.fetch_exact_head_ci", fake_ci)

    async def fake_entry(_repo, number):
        _repo_row, run, action = next(row for row in rows if row[1].pr_number == number)
        return QueueEntry(
            action.github_queue_entry_id or f"MQ-{number}",
            "AWAITING_CHECKS",
            run.current_base_sha,
            run.current_head_sha,
        )

    async def fake_group(_repo, *, pr_number, **_kwargs):
        _repo_row, _run, action = next(row for row in rows if row[1].pr_number == pr_number)
        return action.merge_group_sha, f"refs/heads/gh-readonly-queue/main/pr-{pr_number}-group"

    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", fake_entry)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_merge_group_ref", fake_group)

    assert await reconcile_merge_queue(db_factory) == 1
    first_action = await db_session.get(
        PRMergeQueueAction, rows[0][2].id, populate_existing=True
    )
    first_run = await db_session.get(PRMonitorRun, rows[0][1].id, populate_existing=True)
    second_run = await db_session.get(PRMonitorRun, rows[1][1].id, populate_existing=True)
    assert first_action.status == "checking"
    assert first_action.last_error.startswith("merge_group_ci_read_failed:RuntimeError")
    assert first_run.status == "merge_group_checking"
    assert second_run.status == "merge_group_passed"


@pytest.mark.asyncio
async def test_queue_reconciler_recovers_missing_webhook_and_remote_rebuild(
    db_session, db_factory, monkeypatch
):
    _repo, run, _review, action = await _seed_queue_action(
        db_session, name="recover-group", number=31,
    )
    remote = {"entry": "MQ-31", "merge": MERGE, "suffix": "first"}

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": BASE,
            "headRefOid": HEAD, "isDraft": False, "mergeCommit": None,
        }

    async def fake_entry(_repo_name, _number):
        return QueueEntry(remote["entry"], "AWAITING_CHECKS", BASE, HEAD)

    async def fake_group(_repo_name, **_kwargs):
        return (
            remote["merge"],
            f"refs/heads/gh-readonly-queue/main/pr-31-{remote['suffix']}",
        )

    observed = []

    async def fake_ci(_repo_name, sha, _required):
        observed.append(sha)
        return "passed", "Passed", {"head_sha": sha, "observed": []}

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", fake_entry)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_merge_group_ref", fake_group)
    monkeypatch.setattr("backend.services.pr_review_panel.fetch_exact_head_ci", fake_ci)

    assert await reconcile_merge_queue(db_factory) == 1
    recovered = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    assert recovered.status == "checking"
    assert recovered.merge_group_sha == MERGE
    assert observed == [MERGE]

    remote.update(entry="MQ-31-rebuilt", merge="d" * 40, suffix="rebuilt")
    assert await reconcile_merge_queue(db_factory) == 1
    rebuilt = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    assert rebuilt.github_queue_entry_id == "MQ-31-rebuilt"
    assert rebuilt.merge_group_sha == "d" * 40
    assert rebuilt.merge_group_ref.endswith("-rebuilt")
    assert observed == [MERGE, "d" * 40]


@pytest.mark.asyncio
async def test_queue_entry_disappearance_rechecks_and_records_racing_merge(
    db_session, db_factory, monkeypatch
):
    _repo, run, _review, action = await _seed_queue_action(
        db_session, name="racing-merge", number=32,
    )
    reads = 0

    async def fake_pr_view(_number, _repo_name):
        nonlocal reads
        reads += 1
        merged = reads >= 2
        return {
            "state": "MERGED" if merged else "OPEN",
            "mergedAt": "2026-08-04T00:00:00Z" if merged else None,
            "baseRefOid": BASE, "headRefOid": HEAD, "isDraft": False,
            "mergeCommit": {"oid": MERGE} if merged else None,
        }

    async def no_entry(_repo_name, _number):
        return None

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", no_entry)

    assert await reconcile_merge_queue(db_factory) == 1
    refreshed = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert reads == 2
    assert refreshed.status == "merged"
    assert refreshed_run.status == "merged"


@pytest.mark.asyncio
async def test_queue_entry_disappearance_pauses_when_pr_remains_open(
    db_session, db_factory, monkeypatch
):
    _repo, run, _review, action = await _seed_queue_action(
        db_session, name="dequeued", number=33,
    )

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": BASE,
            "headRefOid": HEAD, "isDraft": False, "mergeCommit": None,
        }

    async def no_entry(_repo_name, _number):
        return None

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", no_entry)

    assert await reconcile_merge_queue(db_factory) == 0
    refreshed = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert refreshed.status == "paused"
    assert refreshed.last_error == "merge_queue_entry_disappeared"
    assert refreshed_run.status == "paused"


@pytest.mark.asyncio
@pytest.mark.parametrize("remote_state", ["UNMERGEABLE", "LOCKED"])
async def test_queue_blocked_remote_states_pause_fail_closed(
    db_session, db_factory, monkeypatch, remote_state
):
    _repo, run, _review, action = await _seed_queue_action(
        db_session, name=f"blocked-{remote_state.lower()}", number=34,
    )

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": BASE,
            "headRefOid": HEAD, "isDraft": False, "mergeCommit": None,
        }

    async def fake_entry(_repo_name, _number):
        return QueueEntry("MQ-blocked", remote_state, BASE, HEAD)

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", fake_entry)

    assert await reconcile_merge_queue(db_factory) == 0
    refreshed = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert refreshed.status == "paused"
    assert refreshed.last_error == f"merge_queue_entry_{remote_state.lower()}"
    assert refreshed_run.status == "paused"


@pytest.mark.asyncio
async def test_later_lost_lease_cannot_rollback_earlier_action_progress(
    db_session, db_factory, monkeypatch
):
    from datetime import datetime, timedelta

    first = await _seed_queue_action(
        db_session, name="first-commit", number=35, status="checking", merge_sha=MERGE,
    )
    second = await _seed_queue_action(
        db_session, name="later-lease", number=36, status="enqueuing",
        base_sha="d" * 40, head_sha="e" * 40,
    )
    second[3].lease_token = "held-by-other"
    second[3].lease_expires_at = datetime.utcnow() + timedelta(hours=1)
    await db_session.commit()

    async def fake_pr_view(number, _repo_name):
        base_sha, head_sha = (
            (BASE, HEAD) if number == 35 else ("d" * 40, "e" * 40)
        )
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": base_sha,
            "headRefOid": head_sha, "isDraft": False, "mergeCommit": None,
        }

    async def fake_entry(_repo_name, number):
        assert number == 35
        return QueueEntry("MQ-35", "AWAITING_CHECKS", BASE, HEAD)

    async def fake_group(_repo_name, **_kwargs):
        return MERGE, "refs/heads/gh-readonly-queue/main/pr-35-group"

    async def fake_ci(*_args):
        return "passed", "Passed", {"head_sha": MERGE, "observed": []}

    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_queue_entry", fake_entry)
    monkeypatch.setattr("backend.services.pr_merge_queue._read_merge_group_ref", fake_group)
    monkeypatch.setattr("backend.services.pr_review_panel.fetch_exact_head_ci", fake_ci)

    assert await reconcile_merge_queue(db_factory) == 1
    first_run = await db_session.get(PRMonitorRun, first[1].id, populate_existing=True)
    second_action = await db_session.get(
        PRMergeQueueAction, second[3].id, populate_existing=True
    )
    assert first_run.status == "merge_group_passed"
    assert second_action.status == "enqueuing"
    assert second_action.lease_token == "held-by-other"


@pytest.mark.asyncio
async def test_enqueue_lease_uses_database_clock_and_covers_confirmation(
    db_session, db_factory, monkeypatch
):
    from datetime import datetime, timedelta

    _repo, _run, _review, action = await _seed_queue_action(
        db_session, name="db-clock", number=37, status="pending",
    )
    database_now = datetime(2030, 1, 2, 3, 4, 5)

    async def fake_clock(_db):
        return database_now

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": BASE,
            "headRefOid": HEAD, "isDraft": False, "mergeCommit": None,
        }

    async def fake_enqueue(_repo_name, _number, base_sha, head_sha):
        assert (base_sha, head_sha) == (BASE, HEAD)
        async with db_factory() as observer:
            claimed = await observer.get(PRMergeQueueAction, action.id)
            assert claimed.status == "enqueuing"
            assert claimed.lease_expires_at == database_now + timedelta(minutes=10)
        return QueueEntry("MQ-37", "QUEUED", BASE, HEAD)

    monkeypatch.setattr("backend.services.pr_merge_queue._database_now", fake_clock)
    monkeypatch.setattr("backend.services.pr_review_service._gh_pr_view", fake_pr_view)
    monkeypatch.setattr("backend.services.pr_merge_queue._enqueue", fake_enqueue)

    assert await reconcile_merge_queue(db_factory) == 1
    refreshed = await db_session.get(PRMergeQueueAction, action.id, populate_existing=True)
    assert refreshed.status == "queued"
    assert refreshed.lease_token is None
    assert refreshed.lease_expires_at is None


@pytest.mark.asyncio
async def test_enqueue_rechecks_base_before_mutation(monkeypatch):
    calls = []

    async def no_existing(_repo_name, _number):
        return None

    async def fake_gh(_endpoint, *, payload, **_kwargs):
        calls.append(payload)
        query = payload["query"]
        assert "baseRefOid" in query
        assert "enqueuePullRequest" not in query
        return {"data": {"repository": {"pullRequest": {
            "id": "PR-node", "baseRefOid": "d" * 40,
            "headRefOid": HEAD,
        }}}}

    monkeypatch.setattr(
        "backend.services.pr_merge_queue._read_queue_entry", no_existing
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    with pytest.raises(ValueError, match="exact queued subject"):
        await _enqueue("fake/base-race", 41, BASE, HEAD)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_new_wrong_subject_entry_is_dequeued_but_manual_entry_is_not(
    monkeypatch
):
    queue_reads = iter((
        None,
        QueueEntry("MQ-new", "QUEUED", "d" * 40, HEAD),
        None,
    ))
    mutations = []

    async def read_queue(_repo_name, _number):
        return next(queue_reads)

    async def fake_gh(_endpoint, *, payload, **_kwargs):
        query = payload["query"]
        if "enqueuePullRequest" in query:
            mutations.append("enqueue")
            return {"data": {"enqueuePullRequest": {
                "mergeQueueEntry": {"id": "MQ-new", "state": "QUEUED"}
            }}}
        if "dequeuePullRequest" in query:
            mutations.append(("dequeue", payload["variables"]["id"]))
            return {"data": {"dequeuePullRequest": {
                "mergeQueueEntry": {"id": "MQ-new"}
            }}}
        return {"data": {"repository": {"pullRequest": {
            "id": "PR-node", "baseRefOid": BASE, "headRefOid": HEAD,
        }}}}

    monkeypatch.setattr(
        "backend.services.pr_merge_queue._read_queue_entry", read_queue
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    with pytest.raises(ValueError, match="new entry was removed"):
        await _enqueue("fake/post-mutation-race", 42, BASE, HEAD)
    assert mutations == ["enqueue", ("dequeue", "PR-node")]

    async def manual_entry(_repo_name, _number):
        return QueueEntry("MQ-manual", "QUEUED", BASE, HEAD)

    async def unexpected_gh(*_args, **_kwargs):
        raise AssertionError("a pre-existing manual entry must not be mutated")

    monkeypatch.setattr(
        "backend.services.pr_merge_queue._read_queue_entry", manual_entry
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", unexpected_gh
    )
    existing = await _enqueue("fake/manual", 43, BASE, HEAD)
    assert existing.id == "MQ-manual"
    assert existing.created_by_call is False


@pytest.mark.asyncio
async def test_failed_wrong_subject_cleanup_pauses_high_risk_action(
    db_session, db_factory, monkeypatch
):
    _repo, run, _review, action = await _seed_queue_action(
        db_session, name="cleanup-failed", number=44, status="pending",
    )

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": BASE,
            "headRefOid": HEAD, "isDraft": False, "mergeCommit": None,
        }

    async def unsafe_enqueue(*_args, **_kwargs):
        raise QueueEntryCleanupError(
            "wrong subject; dequeue could not be confirmed",
            entry_id="MQ-risk",
        )

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_pr_view", fake_pr_view
    )
    monkeypatch.setattr(
        "backend.services.pr_merge_queue._enqueue", unsafe_enqueue
    )
    assert await reconcile_merge_queue(db_factory) == 0
    refreshed = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    refreshed_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert refreshed.status == "paused"
    assert refreshed.github_queue_entry_id == "MQ-risk"
    assert refreshed.last_error.startswith("merge_queue_remote_cleanup_failed:")
    assert refreshed_run.status == "paused"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["disable", "pause"])
async def test_enqueue_finalize_cannot_revive_changed_lifecycle(
    db_session, db_factory, monkeypatch, change
):
    repo, run, _review, action = await _seed_queue_action(
        db_session, name=f"finalize-{change}", number=45 if change == "disable" else 46,
        status="pending",
    )
    dequeued = []

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN", "mergedAt": None, "baseRefOid": BASE,
            "headRefOid": HEAD, "isDraft": False, "mergeCommit": None,
        }

    async def fake_enqueue(*_args, **_kwargs):
        async with db_factory() as concurrent:
            changed_repo = await concurrent.get(MonitoredRepo, repo.id)
            changed_action = await concurrent.get(PRMergeQueueAction, action.id)
            changed_run = await concurrent.get(PRMonitorRun, run.id)
            if change == "disable":
                changed_repo.enabled = False
            else:
                changed_action.status = "paused"
                changed_action.last_error = "manual_pause"
                changed_action.lease_token = None
                changed_action.lease_expires_at = None
            changed_run.status = "paused"
            changed_run.pause_reason = f"concurrent_{change}"
            changed_run.state_version += 1
            await concurrent.commit()
        return QueueEntry(
            "MQ-owned", "QUEUED", BASE, HEAD, True, "PR-owned"
        )

    async def fake_dequeue(_repo_name, _number, pull_request_id, entry_id):
        assert pull_request_id == "PR-owned"
        dequeued.append(entry_id)

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_pr_view", fake_pr_view
    )
    monkeypatch.setattr(
        "backend.services.pr_merge_queue._enqueue", fake_enqueue
    )
    monkeypatch.setattr(
        "backend.services.pr_merge_queue._dequeue_queue_entry", fake_dequeue
    )
    assert await reconcile_merge_queue(db_factory) == 0
    refreshed = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    refreshed_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert dequeued == ["MQ-owned"]
    assert refreshed.status == "paused"
    assert refreshed.status != "queued"
    assert refreshed_run.status == "paused"
    assert refreshed_run.status != "merge_queued"
