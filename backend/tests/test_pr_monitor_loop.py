"""Durable PR lifecycle and Shadow Repair evidence tests."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from sqlalchemy import select

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMergeQueueAction,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.models.test_harness import TestHarnessRun as HarnessRun
from backend.models.worker import Worker
from backend.services.pr_monitor_loop import (
    _repair_remote_subject_is_current as _real_repair_remote_subject_is_current,
    attach_review_to_run,
    reconcile_terminal_review_runs,
    record_blocking_evidence,
    record_gate_pass,
    record_review_error,
)
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
)
from backend.tests.worker_termination_helpers import (
    persist_active_manager_receipt,
)


BASE = "a" * 40
HEAD = "b" * 40


@pytest.fixture(autouse=True)
def _stub_repair_remote_subject(monkeypatch):
    """Unit tests opt into remote drift explicitly; default is exact/open."""

    probe = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "backend.services.pr_monitor_loop._repair_remote_subject_is_current",
        probe,
    )
    return probe


@pytest.mark.asyncio
async def test_terminal_pass_review_recovers_monitor_run_gate(
    db_session, db_factory
):
    repo = MonitoredRepo(
        repo_full_name="owner/pass-gap", webhook_secret="s" * 64,
        review_mode="panel", merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=7, current_base_sha=BASE,
        current_head_sha=HEAD, status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=7,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="pass", pr_author="alice",
        pr_url="https://github.com/owner/pass-gap/pull/7",
        status="approved", action_taken="lgtm_comment",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    await db_session.commit()

    assert await reconcile_terminal_review_runs(db_factory) == 1
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert recovered.status == "ready_to_merge"
    assert await reconcile_terminal_review_runs(db_factory) == 0


@pytest.mark.asyncio
async def test_terminal_error_review_recovery_pauses_exact_monitor_once(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/error-gap",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=19,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=19,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="failed review",
        pr_author="alice",
        pr_url="https://github.com/owner/error-gap/pull/19",
        status="error",
        action_taken="error",
        review_summary="review model transport failed",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    await db_session.commit()

    assert await reconcile_terminal_review_runs(db_factory) == 1
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert recovered.status == "paused"
    assert recovered.pause_reason == (
        f"review_error:{review.id}:review model transport failed"
    )
    recovered_version = recovered.state_version

    assert await reconcile_terminal_review_runs(db_factory) == 0
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert recovered.state_version == recovered_version


@pytest.mark.asyncio
async def test_stale_error_review_cannot_pause_replacement_webhook_generation(
    db_session,
):
    repo = MonitoredRepo(
        repo_full_name="owner/error-replaced",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    old_review = PRReview(
        repo_id=repo.id,
        pr_number=20,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="old failed review",
        pr_author="alice",
        pr_url="https://github.com/owner/error-replaced/pull/20",
        status="error",
        action_taken="error",
        review_summary="late model failure",
    )
    db_session.add(old_review)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=20,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        current_review_id=old_review.id,
        status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    old_review.monitor_run_id = run.id
    replacement = PRReview(
        repo_id=repo.id,
        pr_number=20,
        base_ref="main",
        base_sha=BASE,
        head_sha="c" * 40,
        pr_title="replacement review",
        pr_author="alice",
        pr_url="https://github.com/owner/error-replaced/pull/20",
        status="reviewing",
    )
    db_session.add(replacement)
    await db_session.commit()
    await attach_review_to_run(db_session, repo=repo, review=replacement)

    current = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    current_version = current.state_version
    assert await record_review_error(db_session, review_id=old_review.id) is False
    current = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert current.current_review_id == replacement.id
    assert current.current_head_sha == replacement.head_sha
    assert current.status == "reviewing"
    assert current.pause_reason is None
    assert current.state_version == current_version


@pytest.mark.asyncio
async def test_reconciler_discards_error_candidate_replaced_after_scan(
    db_session,
    db_factory,
):
    """A candidate captured before synchronize cannot pause its replacement."""

    repo = MonitoredRepo(
        repo_full_name="owner/error-scan-race",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=21,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=21,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="old failed review",
        pr_author="alice",
        pr_url="https://github.com/owner/error-scan-race/pull/21",
        status="error",
        action_taken="error",
        review_summary="late failure from the old head",
    )
    db_session.add(old_review)
    await db_session.flush()
    run.current_review_id = old_review.id
    await db_session.commit()
    repo_id = repo.id
    run_id = run.id
    old_review_id = old_review.id

    candidate_scanned = asyncio.Event()
    replacement_committed = asyncio.Event()
    factory_calls = 0

    @asynccontextmanager
    async def barrier_factory():
        nonlocal factory_calls
        factory_calls += 1
        call_number = factory_calls
        async with db_factory() as db:
            yield db
        if call_number == 1:
            # ``reconcile_terminal_review_runs`` has materialized review_ids and
            # closed that read transaction, but has not opened the per-candidate
            # row-lock transaction yet.
            candidate_scanned.set()
            await replacement_committed.wait()

    reconcile_task = asyncio.create_task(
        reconcile_terminal_review_runs(barrier_factory)
    )
    await asyncio.wait_for(candidate_scanned.wait(), timeout=2)

    replacement_id = None
    try:
        async with db_factory() as replacement_db:
            locked_repo = await replacement_db.get(MonitoredRepo, repo_id)
            replacement = PRReview(
                repo_id=repo_id,
                pr_number=21,
                base_ref="main",
                base_sha=BASE,
                head_sha="c" * 40,
                pr_title="replacement review",
                pr_author="alice",
                pr_url=(
                    "https://github.com/owner/error-scan-race/pull/21"
                ),
                status="reviewing",
            )
            replacement_db.add(replacement)
            await replacement_db.flush()
            replacement_id = replacement.id
            await attach_review_to_run(
                replacement_db,
                repo=locked_repo,
                review=replacement,
            )
    finally:
        replacement_committed.set()

    assert await asyncio.wait_for(reconcile_task, timeout=2) == 0
    assert factory_calls == 2
    async with db_factory() as assertion_db:
        current = await assertion_db.get(PRMonitorRun, run_id)
        old = await assertion_db.get(PRReview, old_review_id)
        assert current.current_review_id == replacement_id
        assert current.current_head_sha == "c" * 40
        assert current.status == "reviewing"
        assert current.pause_reason is None
        assert old.status == "error"
        assert old.action_taken == "error"


@pytest.mark.asyncio
async def test_gate_pass_preserves_confirmed_legacy_merge_as_run_terminal(
    db_session,
):
    repo = MonitoredRepo(
        repo_full_name="owner/legacy-merge", webhook_secret="s" * 64,
        review_mode="panel", auto_merge=True, merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=71, current_base_sha=BASE,
        current_head_sha=HEAD, status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=71,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="merged", pr_author="alice",
        pr_url="https://github.com/owner/legacy-merge/pull/71",
        status="merged", action_taken="approved_merged",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    await db_session.commit()

    await record_gate_pass(db_session, review.id)
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    actions = list((await db_session.execute(select(PRMergeQueueAction))).scalars())
    assert recovered.status == "merged"
    assert recovered.completed_at is not None
    assert actions == []


@pytest.mark.asyncio
async def test_terminal_blocking_review_recovers_repair_evidence(
    db_session, db_factory
):
    repo = MonitoredRepo(
        repo_full_name="owner/block-gap", webhook_secret="s" * 64,
        review_mode="panel", auto_repair=False,
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=8, current_base_sha=BASE,
        current_head_sha=HEAD, status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=8,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="blocked", pr_author="alice",
        pr_url="https://github.com/owner/block-gap/pull/8",
        status="commented", action_taken="review_comments",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id, role="senior_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="c" * 64,
        guide_pack_hash="d" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=review.id, reviewer_run_id=reviewer.id,
        fingerprint="e" * 64, thread_nonce="1" * 48,
        role=reviewer.role, severity="high", category="correctness",
        path="app.py", line=4, title="Wrong branch",
        evidence="The false branch returns success.",
        impact="Invalid input is accepted.", required_fix="Return an error.",
        test="Exercise invalid input.", base_sha=BASE, head_sha=HEAD,
        thread_status="published_inline",
    ))
    await db_session.commit()

    assert await reconcile_terminal_review_runs(db_factory) == 1
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    wake = (await db_session.execute(select(PRRepairWake))).scalar_one()
    assert recovered.status == "waiting_for_fix"
    assert wake.status == "shadow"
    assert wake.reason_kind == "review_blocked"
    assert await reconcile_terminal_review_runs(db_factory) == 0


@pytest.mark.asyncio
async def test_shadow_repair_is_idempotent_and_new_head_supersedes_it(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=False,
        max_repair_attempts=3,
    )
    db_session.add(repo)
    await db_session.commit()
    review = PRReview(
        repo_id=repo.id,
        pr_number=9,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="change",
        pr_author="alice",
        pr_url="https://github.com/owner/repo/pull/9",
        status="commented",
        ci_status="passed",
    )
    db_session.add(review)
    await db_session.commit()
    run = await attach_review_to_run(db_session, repo=repo, review=review)
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="c" * 64,
        guide_pack_hash="d" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="e" * 64,
        thread_nonce="1" * 48,
        role=reviewer.role,
        severity="medium",
        category="correctness",
        path="app.py",
        line=4,
        title="Wrong branch",
        evidence="The false branch returns success.",
        impact="Invalid input is accepted.",
        required_fix="Return an error.",
        test="Exercise invalid input.",
        base_sha=BASE,
        head_sha=HEAD,
    ))
    await db_session.commit()

    first = await record_blocking_evidence(db_session, review_id=review.id, reason_kind="review_blocked")
    second = await record_blocking_evidence(db_session, review_id=review.id, reason_kind="review_blocked")
    assert first is not None and second is not None and first.id == second.id
    assert first.status == "shadow"
    assert run.status == "waiting_for_fix"
    assert first.evidence["findings"][0]["fingerprint"] == "e" * 64

    replacement = PRReview(
        repo_id=repo.id,
        pr_number=9,
        base_ref="main",
        base_sha=BASE,
        head_sha="f" * 40,
        pr_title="change",
        pr_author="alice",
        pr_url="https://github.com/owner/repo/pull/9",
        status="waiting_ci",
    )
    db_session.add(replacement)
    await db_session.commit()
    await attach_review_to_run(db_session, repo=repo, review=replacement)
    old_wake = await db_session.get(PRRepairWake, first.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert old_wake.status == "superseded"
    assert refreshed_run.current_head_sha == "f" * 40
    assert refreshed_run.status == "waiting_ci"
    assert len(list((await db_session.execute(select(PRRepairWake))).scalars())) == 1


@pytest.mark.asyncio
async def test_same_shas_new_base_ref_supersedes_old_repair_wake(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/base-shift", webhook_secret="s" * 64,
        review_mode="panel", auto_repair=False,
    )
    db_session.add(repo)
    await db_session.flush()
    old_review = PRReview(
        repo_id=repo.id, pr_number=90, base_ref="main", base_sha=BASE, head_sha=HEAD,
        pr_title="old base", pr_author="alice",
        pr_url="https://github.com/owner/base-shift/pull/90",
        status="commented",
    )
    db_session.add(old_review)
    await db_session.flush()
    run = await attach_review_to_run(db_session, repo=repo, review=old_review)
    wake = PRRepairWake(
        monitor_run_id=run.id, review_id=old_review.id,
        trigger_base_sha=BASE, trigger_head_sha=HEAD,
        reason_kind="review_blocked", evidence_hash="e" * 64,
        evidence={"subject": {"base_sha": BASE, "head_sha": HEAD}},
        status="pending", delivery_token="d" * 48,
    )
    db_session.add(wake)
    await db_session.commit()

    replacement = PRReview(
        repo_id=repo.id, pr_number=90, base_ref="release", base_sha=BASE, head_sha=HEAD,
        pr_title="new base", pr_author="alice",
        pr_url="https://github.com/owner/base-shift/pull/90",
        status="waiting_ci",
    )
    db_session.add(replacement)
    await db_session.commit()
    await attach_review_to_run(db_session, repo=repo, review=replacement)

    stale = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    refreshed = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert stale.status == "superseded"
    assert refreshed.current_base_sha == BASE
    assert refreshed.current_head_sha == HEAD


@pytest.mark.asyncio
async def test_repair_remote_subject_rejects_same_shas_new_base_ref(monkeypatch):
    repo = MonitoredRepo(
        repo_full_name="owner/ref-drift",
        # Mutable repository defaults cannot redefine the frozen Review.
        default_branch="release",
    )
    review = PRReview(
        pr_number=91,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
    )

    async def fake_pr_view(_number, _repo_name):
        return {
            "state": "OPEN",
            "mergedAt": None,
            "baseRefName": "release",
            "baseRefOid": BASE,
            "headRefOid": HEAD,
            "isDraft": False,
            "mergeCommit": None,
        }

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_pr_view", fake_pr_view
    )
    assert await _real_repair_remote_subject_is_current(repo, review) is False


@pytest.mark.asyncio
async def test_pending_repair_base_ref_drift_pauses_without_delivery(
    db_session, db_factory, _stub_repair_remote_subject
):
    from backend.services.pr_monitor_loop import reconcile_repair_wakes

    repo = MonitoredRepo(
        repo_full_name="owner/repair-ref-drift",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
    )
    developer = Task(
        title="Developer",
        description="change",
        status="completed",
        session_id="repair-ref-session",
        last_cwd="/workspace/repair-ref",
    )
    db_session.add_all((repo, developer))
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=92,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        developer_task_id=developer.id,
        status="repair_pending",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=92,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="repair ref drift",
        pr_author="alice",
        pr_url="https://github.com/owner/repair-ref-drift/pull/92",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    wake = PRRepairWake(
        monitor_run_id=run.id,
        review_id=review.id,
        developer_task_id=developer.id,
        trigger_base_sha=BASE,
        trigger_head_sha=HEAD,
        reason_kind="review_blocked",
        evidence_hash="f" * 64,
        evidence={
            "subject": {
                "base_ref": "main",
                "base_sha": BASE,
                "head_sha": HEAD,
            }
        },
        status="pending",
        delivery_token="e" * 48,
    )
    db_session.add(wake)
    await db_session.commit()

    _stub_repair_remote_subject.return_value = False
    dispatcher = AsyncMock()
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 0

    stale = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    paused = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert stale.status == "superseded"
    assert stale.last_error == "repair_pr_subject_changed"
    assert paused.status == "paused"
    assert paused.pause_reason == "repair_pr_subject_changed"
    dispatcher.enqueue_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_repair_wake_has_durable_acceptance_and_awaits_new_push(
    db_session, db_factory
):
    from backend.services.pr_monitor_loop import (
        admit_repair_wake,
        finish_repair_wake,
        record_repair_push_observed,
        reconcile_repair_wakes,
        restore_repair_developer_task,
    )

    repo = MonitoredRepo(
        repo_full_name="owner/auto",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
        max_repair_attempts=3,
    )
    task = Task(
        title="Developer",
        description="Implement change",
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        session_id="session-1",
        last_cwd="/workspace/repo",
        started_at=datetime.utcnow() - timedelta(minutes=10),
        completed_at=datetime.utcnow() - timedelta(minutes=9),
    )
    db_session.add_all([repo, task])
    await db_session.commit()
    review = PRReview(
        repo_id=repo.id,
        pr_number=10,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="change",
        pr_author="alice",
        pr_url="https://github.com/owner/auto/pull/10",
        status="commented",
        ci_status="failed",
        ci_summary="tests failed",
    )
    db_session.add(review)
    await db_session.commit()
    run = await attach_review_to_run(db_session, repo=repo, review=review)
    run.developer_task_id = task.id
    await db_session.commit()
    wake = await record_blocking_evidence(
        db_session, review_id=review.id, reason_kind="ci_failed"
    )
    assert wake is not None and wake.status == "pending"

    dispatcher = AsyncMock()
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 1
    delivered = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    assert delivered.status == "delivering"
    queued = dispatcher.enqueue_message.await_args.kwargs
    assert queued["task_id"] == task.id
    assert queued["source"].startswith(f"pr-repair:{wake.id}:")
    assert "Do not create another PR" in queued["prompt"]

    assert await admit_repair_wake(
        db_session,
        wake_id=wake.id,
        delivery_token=wake.delivery_token,
        task=task,
    ) is True
    admitted = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    assert admitted.accepted_task_completed_at == task.completed_at
    assert await admit_repair_wake(
        db_session,
        wake_id=wake.id,
        delivery_token=wake.delivery_token,
        task=task,
    ) is False
    task.status = "executing"
    task.completed_at = None
    await db_session.commit()
    task.status = "completed"
    task.completed_at = datetime.utcnow()
    await db_session.commit()
    await finish_repair_wake(
        db_session,
        wake_id=wake.id,
        delivery_token=wake.delivery_token,
        task_id=task.id,
    )
    refreshed = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert refreshed.status == "awaiting_push"
    assert refreshed_run.repair_attempts == 1

    # Crash after the exact terminal committed but before finish_repair_wake:
    # recovery consumes that terminal once instead of delivering the repair
    # prompt a second time.
    refreshed.status = "accepted"
    refreshed_run.status = "repairing"
    refreshed_run.repair_attempts = 0
    await db_session.commit()
    dispatcher.has_task_queue_work.return_value = False
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 0
    terminal_recovered = await db_session.get(
        PRRepairWake, wake.id, populate_existing=True
    )
    terminal_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert terminal_recovered.status == "awaiting_push"
    assert terminal_run.repair_attempts == 1
    assert dispatcher.enqueue_message.await_count == 1

    # A push webhook may arrive before the resumed turn emits its terminal.
    # The synchronize handler records that success before stopping the now
    # stale generation; its later terminal must not turn the Wake into failed.
    refreshed.status = "accepted"
    refreshed.completed_at = None
    refreshed_run.status = "repairing"
    refreshed_run.repair_attempts = 0
    task.status = "executing"
    await db_session.commit()
    wake_id = wake.id
    run_id = run.id
    task_id = task.id
    delivery_token = wake.delivery_token
    async with db_factory() as stale_finish_db:
        # Simulate another Manager having read the accepted Wake before the
        # webhook commits its stronger push evidence.
        await stale_finish_db.get(PRRepairWake, wake_id)
        await stale_finish_db.get(PRMonitorRun, run_id)
        await stale_finish_db.get(Task, task_id)
        async with db_factory() as push_db:
            assert await record_repair_push_observed(
                push_db,
                wake_id=wake_id,
                previous_head_sha=HEAD,
                new_head_sha="c" * 40,
            ) is True
        task.status = "completed"
        task.error_message = "Superseded by new push"
        task.completed_at = datetime.utcnow()
        await db_session.commit()
        await finish_repair_wake(
            stale_finish_db,
            wake_id=wake_id,
            delivery_token=delivery_token,
            task_id=task_id,
        )
    pushed = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    pushed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert pushed.status == "completed"
    assert pushed.last_error is None
    assert pushed_run.repair_attempts == 1

    # The reverse interleaving is safe too: a stale webhook session must
    # observe awaiting_push after the terminal CAS, complete it, and avoid a
    # second repair-attempt increment.
    previous_completion = task.completed_at
    pushed.status = "accepted"
    pushed.completed_at = None
    pushed_run.status = "repairing"
    pushed_run.repair_attempts = 0
    pushed.accepted_task_started_at = task.started_at
    pushed.accepted_task_completed_at = previous_completion
    task.status = "executing"
    task.completed_at = None
    await db_session.commit()
    async with db_factory() as stale_push_db:
        await stale_push_db.get(PRRepairWake, wake_id)
        await stale_push_db.get(PRMonitorRun, run_id)
        await stale_push_db.get(Task, task_id)
        task.status = "completed"
        task.completed_at = datetime.utcnow()
        await db_session.commit()
        await finish_repair_wake(
            db_session,
            wake_id=wake_id,
            delivery_token=delivery_token,
            task_id=task_id,
        )
        assert await record_repair_push_observed(
            stale_push_db,
            wake_id=wake_id,
            previous_head_sha=HEAD,
            new_head_sha="d" * 40,
        ) is True
    pushed = await db_session.get(PRRepairWake, wake_id, populate_existing=True)
    pushed_run = await db_session.get(PRMonitorRun, run_id, populate_existing=True)
    assert pushed.status == "completed"
    assert pushed_run.repair_attempts == 1
    task.metadata_ = {"pr_review_superseded": True, "keep": "value"}
    task.error_message = "Superseded by new push"
    assert restore_repair_developer_task(task) is True
    assert task.metadata_ == {"keep": "value"}
    assert task.error_message is None

    # Simulate a Manager crash after durable acceptance but before the new
    # turn's launch claim. The previous completed Task generation is exactly
    # the admission baseline, so it must be re-delivered without consuming
    # another repair attempt.
    refreshed.status = "accepted"
    refreshed_run.status = "repairing"
    refreshed.accepted_worker_id = task.worker_id
    refreshed.accepted_task_retry_count = task.retry_count
    refreshed.accepted_session_id = task.session_id
    refreshed.accepted_task_started_at = task.started_at
    refreshed.accepted_task_completed_at = task.completed_at
    attempts_before_recovery = refreshed_run.repair_attempts
    await db_session.commit()
    dispatcher.has_task_queue_work.return_value = False
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 1
    recovered = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    assert recovered.status == "delivering"
    assert dispatcher.enqueue_message.await_count == 2
    recovered_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert recovered_run.repair_attempts == attempts_before_recovery


@pytest.mark.asyncio
@pytest.mark.parametrize("wake_status", ["delivering", "accepted"])
async def test_repair_recovery_waits_for_terminal_task_harness_graph(
    db_session,
    db_factory,
    wake_status,
):
    from backend.services.pr_monitor_loop import reconcile_repair_wakes

    repo = MonitoredRepo(
        repo_full_name=f"owner/harness-{wake_status}",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
    )
    completed_at = datetime.utcnow() - timedelta(minutes=1)
    task = Task(
        title="Harness-owned Developer",
        description="repair after browser validation",
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        session_id=f"harness-{wake_status}-session",
        last_cwd=f"/workspace/harness-{wake_status}",
        started_at=completed_at - timedelta(minutes=1),
        completed_at=completed_at,
        metadata_={
            "pr_review_superseded": True,
            "keep": "harness-owner",
        },
    )
    db_session.add_all([repo, task])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=93 if wake_status == "delivering" else 94,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        developer_task_id=task.id,
        status="repairing" if wake_status == "accepted" else "repair_pending",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=run.pr_number,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="Harness validation",
        pr_author="alice",
        pr_url=(
            f"https://github.com/{repo.repo_full_name}/pull/{run.pr_number}"
        ),
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    wake = PRRepairWake(
        monitor_run_id=run.id,
        review_id=review.id,
        developer_task_id=task.id,
        trigger_base_sha=BASE,
        trigger_head_sha=HEAD,
        reason_kind="review_blocked",
        evidence_hash=("d" if wake_status == "delivering" else "e") * 64,
        evidence={"harness": wake_status},
        status=wake_status,
        delivery_token=("a" if wake_status == "delivering" else "b") * 48,
        accepted_worker_id=(task.worker_id if wake_status == "accepted" else None),
        accepted_task_retry_count=(
            task.retry_count if wake_status == "accepted" else None
        ),
        accepted_session_id=(
            task.session_id if wake_status == "accepted" else None
        ),
        accepted_task_started_at=(
            task.started_at if wake_status == "accepted" else None
        ),
        accepted_task_completed_at=(
            task.completed_at if wake_status == "accepted" else None
        ),
    )
    harness_run_id = ("1" if wake_status == "delivering" else "2") * 32
    db_session.add_all(
        [
            wake,
            HarnessRun(
                id=harness_run_id,
                task_id=task.id,
                owner_task_incarnation_id=task.incarnation_id,
                owner_task_retry_count=task.retry_count,
                owner_task_turn_generation=task.turn_generation,
                owner_task_status=task.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "Validate the repaired UI"},
                runtime_config={"provider": "codex"},
                request_fingerprint=(
                    "3" if wake_status == "delivering" else "4"
                )
                * 64,
                root_run_id=harness_run_id,
                status="running",
                stage="waiting_for_browser",
            ),
        ]
    )
    await db_session.commit()

    dispatcher = AsyncMock()
    dispatcher.has_task_queue_work.return_value = False
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 0

    blocked_wake = await db_session.get(
        PRRepairWake,
        wake.id,
        populate_existing=True,
    )
    blocked_task = await db_session.get(Task, task.id, populate_existing=True)
    assert blocked_wake.status == wake_status
    assert blocked_task.status == "completed"
    assert blocked_task.metadata_ == {
        "pr_review_superseded": True,
        "keep": "harness-owner",
    }
    dispatcher.enqueue_message.assert_not_awaited()

    harness = await db_session.get(HarnessRun, harness_run_id)
    harness.status = "completed"
    harness.stage = "completed"
    harness.cleanup_status = "completed"
    harness.completed_at = datetime.utcnow()
    await db_session.commit()

    assert await reconcile_repair_wakes(db_factory, dispatcher) == 1
    redelivered = await db_session.get(
        PRRepairWake,
        wake.id,
        populate_existing=True,
    )
    restored_task = await db_session.get(Task, task.id, populate_existing=True)
    assert redelivered.status == "delivering"
    assert restored_task.metadata_ == {"keep": "harness-owner"}
    dispatcher.enqueue_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_repair_recovery_preserves_concurrent_harness_terminal_gate(
    db_session,
    db_factory,
):
    from backend.services.pr_monitor_loop import reconcile_repair_wakes

    repo = MonitoredRepo(
        repo_full_name="owner/harness-terminal-gate",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
    )
    task = Task(
        title="Terminal-gated Developer",
        description="preserve the concurrent Harness terminalizer",
        status="completed",
        session_id="harness-terminal-gate-session",
        last_cwd="/workspace/harness-terminal-gate",
        started_at=datetime.utcnow() - timedelta(minutes=2),
        completed_at=datetime.utcnow() - timedelta(minutes=1),
        metadata_={"pr_review_superseded": True, "keep": "value"},
    )
    db_session.add_all([repo, task])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=95,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        developer_task_id=task.id,
        status="repair_pending",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=95,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="Terminal gate",
        pr_author="alice",
        pr_url="https://github.com/owner/harness-terminal-gate/pull/95",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    wake = PRRepairWake(
        monitor_run_id=run.id,
        review_id=review.id,
        developer_task_id=task.id,
        trigger_base_sha=BASE,
        trigger_head_sha=HEAD,
        reason_kind="review_blocked",
        evidence_hash="f" * 64,
        evidence={"terminal_gate": True},
        status="pending",
        delivery_token="c" * 48,
    )
    db_session.add(wake)
    await db_session.commit()

    terminal_gate = {
        "incarnation_id": task.incarnation_id,
        "retry_count": task.retry_count,
        "turn_generation": task.turn_generation,
        "status": task.status,
        "reason": "test_harness_completed",
    }
    async with db_factory() as recovery_db:
        # Keep the old metadata alive in the identity map.  This is the exact
        # crash-recovery shape that used to replace the whole JSON object and
        # erase a terminal gate committed by another Manager.
        stale_task = await recovery_db.get(Task, task.id)
        assert stale_task.metadata_ == {
            "pr_review_superseded": True,
            "keep": "value",
        }
        await recovery_db.commit()
        async with db_factory() as terminalizer_db:
            terminalized = await terminalizer_db.get(Task, task.id)
            terminalized.metadata_ = {
                **terminalized.metadata_,
                TEST_HARNESS_TERMINAL_GATE_KEY: terminal_gate,
            }
            await terminalizer_db.commit()

        @asynccontextmanager
        async def stale_db_factory():
            yield recovery_db

        dispatcher = AsyncMock()
        dispatcher.has_task_queue_work.return_value = False
        assert await reconcile_repair_wakes(stale_db_factory, dispatcher) == 1

    restored = await db_session.get(Task, task.id, populate_existing=True)
    delivered = await db_session.get(
        PRRepairWake,
        wake.id,
        populate_existing=True,
    )
    assert delivered.status == "delivering"
    assert restored.metadata_ == {
        "keep": "value",
        TEST_HARNESS_TERMINAL_GATE_KEY: terminal_gate,
    }
    dispatcher.enqueue_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_repair_terminal_yields_to_manager_termination_receipt(
    db_session,
    db_factory,
):
    from backend.services.pr_monitor_loop import finish_repair_wake

    worker = Worker(
        name="repair receipt worker",
        status="ready",
        private_ip="10.0.0.91",
        auth_token="receipt-token",
    )
    repo = MonitoredRepo(
        repo_full_name="owner/receipt-repair",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
    )
    db_session.add_all([worker, repo])
    await db_session.flush()
    started_at = datetime.utcnow() - timedelta(minutes=2)
    accepted_completed_at = started_at + timedelta(seconds=10)
    task = Task(
        title="receipt-owned repair terminal",
        description="repair",
        status="completed",
        worker_id=worker.id,
        session_id="repair-receipt-session",
        started_at=started_at,
        completed_at=accepted_completed_at + timedelta(seconds=20),
    )
    db_session.add(task)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=92,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="receipt repair",
        pr_author="alice",
        pr_url="https://github.com/owner/receipt-repair/pull/92",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=92,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        current_review_id=review.id,
        developer_task_id=task.id,
        status="repairing",
    )
    db_session.add(run)
    await db_session.flush()
    wake = PRRepairWake(
        monitor_run_id=run.id,
        review_id=review.id,
        developer_task_id=task.id,
        trigger_base_sha=BASE,
        trigger_head_sha=HEAD,
        reason_kind="review_blocked",
        evidence_hash="e" * 64,
        evidence={"receipt": True},
        status="accepted",
        delivery_token="d" * 48,
        accepted_worker_id=worker.id,
        accepted_task_retry_count=task.retry_count,
        accepted_session_id=task.session_id,
        accepted_task_started_at=task.started_at,
        accepted_task_completed_at=accepted_completed_at,
    )
    db_session.add(wake)
    await db_session.commit()
    task_id = task.id
    wake_id = wake.id
    run_id = run.id

    await persist_active_manager_receipt(db_factory, task_id)
    await finish_repair_wake(
        db_session,
        wake_id=wake_id,
        delivery_token="d" * 48,
        task_id=task_id,
    )

    current_wake = await db_session.get(
        PRRepairWake,
        wake_id,
        populate_existing=True,
    )
    current_run = await db_session.get(
        PRMonitorRun,
        run_id,
        populate_existing=True,
    )
    assert current_wake.status == "accepted"
    assert current_run.status == "repairing"
    assert current_run.repair_attempts == 0


@pytest.mark.asyncio
async def test_concurrent_repair_admission_has_one_exact_winner(
    db_session, db_factory
):
    from backend.services.pr_monitor_loop import admit_repair_wake

    repo = MonitoredRepo(
        repo_full_name="owner/admit-race", webhook_secret="s" * 64,
        review_mode="panel", auto_repair=True,
    )
    task = Task(
        title="Developer", description="change", status="completed",
        session_id="admit-race-session", last_cwd="/workspace/admit-race",
        started_at=datetime.utcnow() - timedelta(minutes=2),
        completed_at=datetime.utcnow() - timedelta(minutes=1),
    )
    db_session.add_all([repo, task])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=81, current_base_sha=BASE,
        current_head_sha=HEAD, developer_task_id=task.id,
        status="repair_pending",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=81,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="race", pr_author="alice",
        pr_url="https://github.com/owner/admit-race/pull/81",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    wake = PRRepairWake(
        monitor_run_id=run.id, review_id=review.id,
        developer_task_id=task.id, trigger_base_sha=BASE,
        trigger_head_sha=HEAD, reason_kind="review_blocked",
        evidence_hash="e" * 64, evidence={"findings": []},
        status="delivering", delivery_token="d" * 48,
    )
    db_session.add(wake)
    await db_session.commit()

    async def attempt():
        async with db_factory() as db:
            candidate = await db.get(Task, task.id)
            return await admit_repair_wake(
                db,
                wake_id=wake.id,
                delivery_token=wake.delivery_token,
                task=candidate,
            )

    outcomes = await asyncio.gather(attempt(), attempt())
    assert sorted(outcomes) == [False, True]
    admitted = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    assert admitted.status == "accepted"
    assert admitted.accepted_task_completed_at == task.completed_at


@pytest.mark.asyncio
async def test_exact_project_branch_auto_binds_unique_developer_task(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/bind", webhook_secret="s" * 64,
        review_mode="panel", project_id=77,
    )
    developer = Task(
        title="Developer", description="change", status="completed",
        project_id=77, result_branch="task/exact-pr", session_id="session-bind",
        last_cwd="/workspace/bind",
    )
    db_session.add_all([repo, developer])
    await db_session.commit()
    review = PRReview(
        repo_id=repo.id, pr_number=11, base_ref="main", base_sha=BASE, head_sha=HEAD,
        pr_title="bind", pr_author="alice",
        pr_url="https://github.com/owner/bind/pull/11", status="waiting_ci",
    )
    db_session.add(review)
    await db_session.commit()
    run = await attach_review_to_run(
        db_session, repo=repo, review=review,
        pr_data={
            "head_repo_full_name": "owner/bind",
            "head_branch": "task/exact-pr",
        },
    )
    assert run.developer_task_id == developer.id
    assert run.binding_verified_at is not None


@pytest.mark.asyncio
async def test_remote_developer_is_authoritatively_migrated_before_repair_delivery(
    db_session, db_factory, monkeypatch
):
    from types import SimpleNamespace
    from backend.services.pr_monitor_loop import reconcile_repair_wakes

    repo = MonitoredRepo(
        repo_full_name="owner/remote", webhook_secret="s" * 64,
        review_mode="panel", auto_repair=True,
    )
    developer = Task(
        title="Remote Developer", description="change", status="completed",
        worker_id=9, session_id="remote-session", last_cwd="/workspace/remote",
    )
    db_session.add_all([repo, developer])
    await db_session.commit()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=13, current_base_sha=BASE,
        current_head_sha=HEAD, developer_task_id=developer.id,
        status="repair_pending",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=13,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="remote", pr_author="alice",
        pr_url="https://github.com/owner/remote/pull/13", status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    wake = PRRepairWake(
        monitor_run_id=run.id, review_id=review.id,
        developer_task_id=developer.id, trigger_base_sha=BASE,
        trigger_head_sha=HEAD, reason_kind="review_blocked",
        evidence_hash="e" * 64, evidence={"findings": []}, status="pending",
        delivery_token="d" * 48,
    )
    db_session.add(wake)
    await db_session.commit()

    async def migrate(task_id, target):
        assert task_id == developer.id and target is None
        async with db_factory() as migration_db:
            task = await migration_db.get(Task, task_id)
            task.worker_id = None
            await migration_db.commit()

    monkeypatch.setattr("backend.main.task_migrator", SimpleNamespace(migrate=migrate))
    dispatcher = AsyncMock()
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 1
    migrated = await db_session.get(Task, developer.id, populate_existing=True)
    delivered = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    assert migrated.worker_id is None
    assert delivered.status == "delivering"
    dispatcher.enqueue_message.assert_awaited_once()
