"""Durable PR lifecycle and Shadow Repair evidence tests."""

import pytest
from unittest.mock import AsyncMock
from sqlalchemy import select

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.services.pr_monitor_loop import attach_review_to_run, record_blocking_evidence


BASE = "a" * 40
HEAD = "b" * 40


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
    )
    db_session.add_all([repo, task])
    await db_session.commit()
    review = PRReview(
        repo_id=repo.id,
        pr_number=10,
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

    # A push webhook may arrive before the resumed turn emits its terminal.
    # The synchronize handler records that success before stopping the now
    # stale generation; its later terminal must not turn the Wake into failed.
    refreshed.status = "accepted"
    refreshed.completed_at = None
    refreshed_run.status = "repairing"
    refreshed_run.repair_attempts = 0
    task.status = "executing"
    await db_session.commit()
    assert await record_repair_push_observed(
        db_session,
        wake_id=wake.id,
        previous_head_sha=HEAD,
        new_head_sha="c" * 40,
    ) is True
    task.status = "completed"
    task.error_message = "Superseded by new push"
    await db_session.commit()
    await finish_repair_wake(
        db_session,
        wake_id=wake.id,
        delivery_token=wake.delivery_token,
        task_id=task.id,
    )
    pushed = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    pushed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert pushed.status == "completed"
    assert pushed.last_error is None
    assert pushed_run.repair_attempts == 1
    task.metadata_ = {"pr_review_superseded": True, "keep": "value"}
    task.error_message = "Superseded by new push"
    assert restore_repair_developer_task(task) is True
    assert task.metadata_ == {"keep": "value"}
    assert task.error_message is None

    # Simulate a Manager crash after durable acceptance but before the turn
    # terminal was recorded. With no queue/turn evidence after restart, the
    # same nonce-bearing Wake is recovered and delivered once more.
    refreshed.status = "accepted"
    refreshed_run.status = "repairing"
    await db_session.commit()
    dispatcher.has_task_queue_work.return_value = False
    assert await reconcile_repair_wakes(db_factory, dispatcher) == 1
    recovered = await db_session.get(PRRepairWake, wake.id, populate_existing=True)
    assert recovered.status == "delivering"
    assert dispatcher.enqueue_message.await_count == 2


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
        repo_id=repo.id, pr_number=11, base_sha=BASE, head_sha=HEAD,
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
