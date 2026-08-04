import hashlib
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task


BASE_SHA = "1" * 40
HEAD_SHA = "a" * 40


def _patch_text() -> str:
    return (
        "diff --git a/backend/example.py b/backend/example.py\n"
        "--- a/backend/example.py\n"
        "+++ b/backend/example.py\n"
        "@@ -1 +1 @@\n"
        "-raise RuntimeError()\n"
        "+return default_value\n"
    )


def _patch_terminal() -> str:
    return f"PR_REVIEW_PATCH_BEGIN\n{_patch_text()}PR_REVIEW_PATCH_END"


async def _seed_finding(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="codex",
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=7,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_title="Fix issue",
        pr_author="alice",
        pr_url="https://github.com/owner/repo/pull/7",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    monitor_run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=7,
        current_base_sha=BASE_SHA,
        current_head_sha=HEAD_SHA,
        current_review_id=review.id,
        head_repo_full_name="fork-owner/repo",
        head_branch="feature/fix",
    )
    db_session.add(monitor_run)
    await db_session.flush()
    review.monitor_run_id = monitor_run.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior",
        provider="codex",
        status="completed",
        prompt_policy_hash="p" * 64,
        guide_pack_hash="g" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="f" * 64,
        role="senior",
        severity="high",
        category="correctness",
        path="backend/example.py",
        line=12,
        title="Unhandled empty value",
        evidence="Empty values raise unexpectedly.",
        impact="Valid requests fail.",
        required_fix="Return the documented default.",
        test="Cover the empty-value branch.",
        thread_nonce="n" * 48,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    )
    db_session.add(finding)
    await db_session.commit()
    return repo, review, finding


def test_patch_protocol_accepts_only_the_exact_allowed_file():
    from backend.services.pr_review_fix import parse_patch_output

    assert parse_patch_output(
        _patch_terminal(), allowed_files={"backend/example.py"}
    ) == _patch_text()


def test_patch_protocol_rejects_unbounded_model_chatter():
    from backend.services.pr_review_fix import PatchProtocolError, parse_patch_output

    with pytest.raises(PatchProtocolError, match="exactly one"):
        parse_patch_output(
            f"explanation\n{_patch_terminal()}",
            allowed_files={"backend/example.py"},
        )


@pytest.mark.asyncio
async def test_immediate_action_is_idempotent_and_keeps_panel_gate_open(db_session):
    from backend.services.pr_review_actions import create_immediate_finding_action

    _, review, finding = await _seed_finding(db_session)
    first = await create_immediate_finding_action(
        db_session,
        finding_id=finding.id,
        review_id=review.id,
        action_type="human_advice",
        idempotency_key="advice-action-7",
        actor_user_id=12,
        human_advice="Preserve the documented fallback.",
    )
    second = await create_immediate_finding_action(
        db_session,
        finding_id=finding.id,
        review_id=review.id,
        action_type="human_advice",
        idempotency_key="advice-action-7",
        actor_user_id=12,
        human_advice="Preserve the documented fallback.",
    )

    assert first.id == second.id
    await db_session.refresh(finding)
    assert finding.status == "open"


@pytest.mark.asyncio
async def test_immediate_action_cannot_hide_an_active_ai_fix(db_session):
    from backend.services.pr_review_actions import (
        FindingActionConflict,
        create_immediate_finding_action,
    )

    _, review, finding = await _seed_finding(db_session)
    db_session.add(PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="awaiting_confirmation",
        idempotency_key="active-fix-7",
        expected_head_sha=HEAD_SHA,
    ))
    await db_session.commit()

    with pytest.raises(FindingActionConflict, match="active AI repair"):
        await create_immediate_finding_action(
            db_session,
            finding_id=finding.id,
            review_id=review.id,
            action_type="human_advice",
            idempotency_key="advice-during-fix-7",
            actor_user_id=12,
            human_advice="Try another approach.",
        )


@pytest.mark.asyncio
async def test_create_fix_task_captures_route_and_uses_tool_free_tag(db_session):
    from backend.services import pr_review_fix

    repo, review, finding = await _seed_finding(db_session)
    with (
        patch.object(pr_review_fix, "_verify_current_snapshot", AsyncMock()),
        patch.object(
            pr_review_fix,
            "_load_current_head_route",
            AsyncMock(return_value=("fork-owner/repo", "feature/fix", HEAD_SHA)),
        ),
        patch.object(
            pr_review_fix,
            "_fetch_exact_head_file",
            AsyncMock(return_value="raise RuntimeError()\n"),
        ),
        patch("backend.main.dispatcher", MagicMock(wake=MagicMock())),
    ):
        action = await pr_review_fix.create_fix_task(
            db_session,
            finding_id=finding.id,
            review_id=review.id,
            repo_id=repo.id,
            idempotency_key="fix-action-7",
            actor_user_id=12,
        )

    task = await db_session.get(Task, action.task_id)
    assert action.status == "running"
    assert action.result["head_repo_full_name"] == "fork-owner/repo"
    assert action.result["head_ref"] == "feature/fix"
    assert task.tags == ["pr-review-fix"]
    assert task.metadata_["pr_finding_action_id"] == action.id
    assert "backend/example.py" in task.description


@pytest.mark.asyncio
async def test_fix_completion_stages_hash_bound_confirmation(db_session):
    from backend.services import pr_review_fix

    repo, review, finding = await _seed_finding(db_session)
    task = Task(
        title="fix",
        description="immutable",
        status="completed",
        retry_count=2,
        started_at=datetime.utcnow() - timedelta(seconds=5),
        completed_at=datetime.utcnow(),
        metadata_={
            "pr_finding_action_id": 1,
            "expected_head_sha": HEAD_SHA,
        },
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key="fix-completion-7",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        result={
            "allowed_files": [finding.path],
            "action_nonce": "nonce-7",
            "head_repo_full_name": "fork-owner/repo",
            "head_ref": "feature/fix",
        },
    )
    db_session.add(action)
    await db_session.flush()
    task.metadata_ = {
        "pr_finding_action_id": action.id,
        "expected_head_sha": HEAD_SHA,
    }
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=2,
        event_type="result",
        content=_patch_terminal(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()

    with (
        patch.object(pr_review_fix, "_verify_current_snapshot", AsyncMock()),
        patch.object(pr_review_fix, "_validate_patch_applies", AsyncMock()),
    ):
        await pr_review_fix.handle_fix_task_completion(
            db_session,
            action_id=action.id,
            task_id=task.id,
            retry_count=2,
        )

    await db_session.refresh(action)
    assert action.status == "awaiting_confirmation"
    assert action.patch_sha256 == hashlib.sha256(_patch_text().encode()).hexdigest()
    assert action.result["confirmation_token"]


@pytest.mark.asyncio
async def test_task_callback_cannot_overwrite_push_owner(db_session):
    from backend.services import pr_review_fix

    _, _, finding = await _seed_finding(db_session)
    task = Task(
        title="late",
        description="late",
        status="failed",
        retry_count=1,
    )
    db_session.add(task)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="running",
        idempotency_key="push-owner-7",
        task_id=task.id,
        expected_head_sha=HEAD_SHA,
        operation_token="push-owner",
        operation_expires_at=datetime.utcnow() + timedelta(minutes=5),
    )
    db_session.add(action)
    await db_session.commit()

    await pr_review_fix.handle_fix_task_failure(
        db_session,
        action_id=action.id,
        task_id=task.id,
        retry_count=1,
        error="late callback",
    )

    await db_session.refresh(action)
    assert action.status == "running"
    assert action.operation_token == "push-owner"
