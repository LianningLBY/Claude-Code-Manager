"""Independent reviewer panel, structured finding, and CI Gate tests."""

import json
import base64
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.services import pr_review_panel
from backend.services import pr_review_service
from backend.services.pr_monitor_loop import attach_review_to_run
from backend.tests.worker_termination_helpers import (
    persist_active_worker_receipt,
)


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40
PR_DATA = {
    "number": 17,
    "base_sha": BASE_SHA,
    "head_sha": HEAD_SHA,
    "delivery_id": "panel-delivery-17",
    "title": "Panel change",
    "author": "alice",
    "url": "https://github.com/owner/repo/pull/17",
}


def _context():
    return {
        "repo_name": "owner/repo",
        "pr_number": 17,
        "base_ref": "main",
        "base_sha": BASE_SHA,
        "head_sha": HEAD_SHA,
        "guidance": {"CLAUDE.md": "Keep the gate strict.", "PROGRESS.md": None},
        "material": {
            "number": 17,
            "title": "Panel change",
            "body": "",
            "author": "alice",
            "base_ref": "main",
            "head_ref": "feature",
            "files": [{"path": "backend/example.py", "additions": 2, "deletions": 1}],
            "patch": "diff --git a/backend/example.py b/backend/example.py\n",
            "changed_file_contents": [{
                "path": "backend/example.py",
                "base": {"present": True, "available": True, "content": "OLD_FULL_FILE_SENTINEL"},
                "head": {"present": True, "available": True, "content": "NEW_FULL_FILE_SENTINEL"},
            }],
        },
    }


@pytest.mark.asyncio
async def test_panel_review_and_run_are_one_admission_transaction(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/repo", webhook_secret="s" * 64,
        provider="claude", review_mode="panel", wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.commit()

    with patch.object(pr_review_panel, "_wake_dispatcher") as wake:
        review = await pr_review_service.create_pr_review_task(
            db_session, repo, PR_DATA, prepared_context=_context(),
        )
    run = (await db_session.execute(select(PRMonitorRun))).scalar_one()
    assert review.monitor_run_id == run.id
    assert run.current_review_id == review.id
    assert run.current_base_sha == BASE_SHA
    assert run.current_head_sha == HEAD_SHA
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_panel_attach_failure_rolls_back_review_tasks_and_never_wakes(
    db_session,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo", webhook_secret="s" * 64,
        provider="claude", review_mode="panel", wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.commit()

    with (
        patch(
            "backend.services.pr_monitor_loop.attach_review_to_run",
            AsyncMock(side_effect=RuntimeError("simulated attach crash")),
        ),
        patch.object(pr_review_panel, "_wake_dispatcher") as wake,
    ):
        with pytest.raises(RuntimeError, match="attach crash"):
            await pr_review_service.create_pr_review_task(
                db_session, repo, PR_DATA, prepared_context=_context(),
            )
    await db_session.rollback()

    assert list((await db_session.execute(select(PRReview))).scalars()) == []
    assert list((await db_session.execute(select(PRReviewerRun))).scalars()) == []
    assert list((await db_session.execute(select(Task))).scalars()) == []
    assert list((await db_session.execute(select(PRMonitorRun))).scalars()) == []
    wake.assert_not_called()


def _output(role: str, *, blocker: bool = False) -> str:
    findings = []
    verdict = "pass"
    if blocker:
        verdict = "changes_required"
        findings = [{
            "severity": "medium",
            "category": "concurrency",
            "path": "backend/example.py",
            "line": 12,
            "hunk": None,
            "title": "Lost wake-up",
            "evidence": "The state commit happens after the wake call.",
            "impact": "A restart can strand the review.",
            "required_fix": "Commit the state before waking the dispatcher.",
            "test": "Crash after commit and assert startup recovery wakes it.",
        }]
    value = {
        "schema_version": 1,
        "subject": {"kind": "pr_head", "base_sha": BASE_SHA, "head_sha": HEAD_SHA},
        "role": role,
        "verdict": verdict,
        "summary": f"{role} completed",
        "findings": findings,
    }
    return (
        "PR_REVIEW_PANEL_BEGIN\n"
        + json.dumps(value, separators=(",", ":"))
        + "\nPR_REVIEW_PANEL_END\nPR_REVIEW_RESULT: panel_complete"
    )


async def _create_recoverable_panel_run(
    db_session,
    *,
    worker_id: int | None,
) -> tuple[PRReview, PRReviewerRun, Task]:
    repo = MonitoredRepo(
        repo_full_name=f"owner/recovery-{worker_id}",
        webhook_secret="s" * 64,
        provider="claude",
        review_mode="panel",
        wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=17,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_title="Recovery review",
        pr_author="alice",
        pr_url=f"https://github.com/owner/recovery-{worker_id}/pull/17",
        status="reviewing",
        action_nonce="a" * 48,
    )
    db_session.add(review)
    await db_session.flush()
    task = Task(
        title="recoverable reviewer",
        description="immutable review",
        status="completed",
        provider="claude",
        worker_id=worker_id,
        retry_count=0,
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        tags=["pr-review"],
    )
    waiting_task = Task(
        title="still-running reviewer",
        description="immutable review",
        status="pending",
        provider="claude",
        worker_id=worker_id,
        retry_count=0,
        tags=["pr-review"],
    )
    db_session.add_all([task, waiting_task])
    await db_session.flush()
    run = PRReviewerRun(
        pr_review_id=review.id,
        role="principal_engineer",
        task_id=task.id,
        provider="claude",
        status="pending",
        prompt_policy_hash="b" * 64,
        guide_pack_hash="c" * 64,
    )
    waiting_run = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        task_id=waiting_task.id,
        provider="claude",
        status="pending",
        prompt_policy_hash="d" * 64,
        guide_pack_hash="e" * 64,
    )
    db_session.add_all([run, waiting_run])
    await db_session.commit()
    return review, run, task


def test_panel_prompts_share_engineering_standard_and_keep_distinct_litmus():
    prompts = {}
    for role in pr_review_panel.REVIEWER_ROLES:
        prompt, policy_hash, guide_hash = pr_review_panel.build_panel_review_prompt(
            repo_name="owner/repo",
            pr_number=17,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            role=role,
            guidance=_context()["guidance"],
            material=_context()["material"],
        )
        assert len(policy_hash) == 64
        assert len(guide_hash) == 64
        prompts[role] = prompt

    shared_requirements = (
        "Honor cohesion within a module; reject unrelated coupling",
        "Honor clear layers; reject dependency tangles",
        "An application must never call its own HTTP endpoint",
        "Honor capability reuse; reject copy-and-rebuild",
        "Honor unit extension; reject feature sprawl",
        "Honor one established pattern; reject each contributor inventing another",
        "Honor timely deletion of dead code; reject preserving old baggage",
        "Honor the simplest sufficient design; reject speculative over-design",
        "author can either fix it or rebut it with concrete evidence",
    )
    for prompt in prompts.values():
        normalized_prompt = " ".join(prompt.split())
        for requirement in shared_requirements:
            assert " ".join(requirement.split()) in normalized_prompt

    normalized = {
        role: " ".join(prompt.split()) for role, prompt in prompts.items()
    }
    assert "Persona: Principal Engineer — design review, big scope" in normalized["principal_engineer"]
    assert "Never claim repo-wide evidence you were not given" in normalized["principal_engineer"]
    assert "adding a second way to do a solved thing" in normalized["principal_engineer"]
    assert "Persona: Senior Engineer — logic, implementation, and quality" in normalized["senior_engineer"]
    assert "Read every supplied patch" in normalized["senior_engineer"]
    assert "an untestable seam, or a security mistake" in normalized["senior_engineer"]
    assert "Persona: QA Engineer — does it work, is it tested, will it break?" in normalized["qa_engineer"]
    assert "tests that fake the expected result" in normalized["qa_engineer"]
    assert "NEW_FULL_FILE_SENTINEL" in prompts["senior_engineer"]
    assert "NEW_FULL_FILE_SENTINEL" not in prompts["principal_engineer"]
    assert "NEW_FULL_FILE_SENTINEL" not in prompts["qa_engineer"]


@pytest.mark.asyncio
async def test_changed_file_capture_uses_exact_base_and_head_blobs():
    from backend.services import pr_review_service

    base_tree_sha = "c" * 40
    head_tree_sha = "d" * 40
    old_blob_sha = "e" * 40
    new_blob_sha = "f" * 40
    old = b"old contents\n"
    new = b"new contents\n"
    responses = {
        f"repos/owner/repo/git/commits/{BASE_SHA}": {"sha": BASE_SHA, "tree": {"sha": base_tree_sha}},
        f"repos/owner/repo/git/commits/{HEAD_SHA}": {"sha": HEAD_SHA, "tree": {"sha": head_tree_sha}},
        f"repos/owner/repo/git/trees/{base_tree_sha}?recursive=1": {"sha": base_tree_sha, "truncated": False, "tree": [{"path": "app.py", "type": "blob", "mode": "100644", "sha": old_blob_sha, "size": len(old)}]},
        f"repos/owner/repo/git/trees/{head_tree_sha}?recursive=1": {"sha": head_tree_sha, "truncated": False, "tree": [{"path": "app.py", "type": "blob", "mode": "100644", "sha": new_blob_sha, "size": len(new)}]},
        f"repos/owner/repo/git/blobs/{old_blob_sha}": {"sha": old_blob_sha, "size": len(old), "encoding": "base64", "content": base64.b64encode(old).decode()},
        f"repos/owner/repo/git/blobs/{new_blob_sha}": {"sha": new_blob_sha, "size": len(new), "encoding": "base64", "content": base64.b64encode(new).decode()},
    }
    async def response(endpoint, **_kwargs):
        return responses[endpoint]
    with patch.object(pr_review_service, "_gh_api_json", AsyncMock(side_effect=response)):
        captured = await pr_review_service._fetch_changed_file_contents(
            repo_name="owner/repo",
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            files=[{"path": "app.py", "additions": 1, "deletions": 1}],
        )
    assert captured[0]["base"]["content"] == old.decode()
    assert captured[0]["head"]["content"] == new.decode()
    assert captured[0]["base"]["blob_sha"] == old_blob_sha
    assert captured[0]["head"]["blob_sha"] == new_blob_sha


@pytest.mark.asyncio
async def test_pr_files_rest_pagination_captures_all_266_paths_and_rename():
    pages = []
    for start, stop in ((0, 100), (100, 200), (200, 266)):
        page = []
        for index in range(start, stop):
            item = {
                "filename": f"src/file-{index}.py",
                "status": "modified",
                "additions": index + 1,
                "deletions": index,
            }
            page.append(item)
        pages.append(page)
    pages[-1][-1] = {
        "filename": "src/new-name.py",
        "previous_filename": "src/old-name.py",
        "status": "renamed",
        "additions": 3,
        "deletions": 2,
    }

    api = AsyncMock(side_effect=pages)
    with patch.object(pr_review_service, "_gh_api_value", api):
        files = await pr_review_service._fetch_pr_files(
            repo_name="owner/repo",
            pr_number=17,
            changed_files=266,
        )

    assert len(files) == 266
    assert files[0] == {
        "path": "src/file-0.py",
        "additions": 1,
        "deletions": 0,
    }
    assert files[-1] == {
        "path": "src/new-name.py",
        "previous_path": "src/old-name.py",
        "additions": 3,
        "deletions": 2,
    }
    assert [call.args[0] for call in api.await_args_list] == [
        "repos/owner/repo/pulls/17/files?per_page=100&page=1",
        "repos/owner/repo/pulls/17/files?per_page=100&page=2",
        "repos/owner/repo/pulls/17/files?per_page=100&page=3",
    ]


@pytest.mark.asyncio
async def test_pr_files_rest_pagination_rejects_count_mismatch_and_duplicates():
    first_page = [
        {
            "filename": f"src/file-{index}.py",
            "status": "modified",
            "additions": 1,
            "deletions": 0,
        }
        for index in range(100)
    ]
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(side_effect=[first_page, []]),
    ):
        with pytest.raises(
            pr_review_service.GhError,
            match="does not match changedFiles",
        ):
            await pr_review_service._fetch_pr_files(
                repo_name="owner/repo",
                pr_number=17,
                changed_files=101,
            )

    duplicate = {
        "filename": "src/same.py",
        "status": "modified",
        "additions": 1,
        "deletions": 0,
    }
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[duplicate, duplicate]),
    ):
        with pytest.raises(pr_review_service.GhError, match="duplicate paths"):
            await pr_review_service._fetch_pr_files(
                repo_name="owner/repo",
                pr_number=17,
                changed_files=2,
            )


@pytest.mark.asyncio
async def test_pr_files_rest_pagination_rejects_more_than_capture_limit():
    api = AsyncMock()
    with patch.object(pr_review_service, "_gh_api_value", api):
        with pytest.raises(
            pr_review_service.GhError,
            match="more than 300 files",
        ):
            await pr_review_service._fetch_pr_files(
                repo_name="owner/repo",
                pr_number=17,
                changed_files=301,
            )
    api.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_file_capture_uses_previous_path_for_rename_base():
    base_tree_sha = "1" * 40
    head_tree_sha = "2" * 40
    old_blob_sha = "3" * 40
    new_blob_sha = "4" * 40
    old = b"old renamed contents\n"
    new = b"new renamed contents\n"
    responses = {
        f"repos/owner/repo/git/commits/{BASE_SHA}": {
            "sha": BASE_SHA,
            "tree": {"sha": base_tree_sha},
        },
        f"repos/owner/repo/git/commits/{HEAD_SHA}": {
            "sha": HEAD_SHA,
            "tree": {"sha": head_tree_sha},
        },
        f"repos/owner/repo/git/trees/{base_tree_sha}?recursive=1": {
            "sha": base_tree_sha,
            "truncated": False,
            "tree": [{
                "path": "old.py",
                "type": "blob",
                "mode": "100644",
                "sha": old_blob_sha,
                "size": len(old),
            }],
        },
        f"repos/owner/repo/git/trees/{head_tree_sha}?recursive=1": {
            "sha": head_tree_sha,
            "truncated": False,
            "tree": [{
                "path": "new.py",
                "type": "blob",
                "mode": "100644",
                "sha": new_blob_sha,
                "size": len(new),
            }],
        },
        f"repos/owner/repo/git/blobs/{old_blob_sha}": {
            "sha": old_blob_sha,
            "size": len(old),
            "encoding": "base64",
            "content": base64.b64encode(old).decode(),
        },
        f"repos/owner/repo/git/blobs/{new_blob_sha}": {
            "sha": new_blob_sha,
            "size": len(new),
            "encoding": "base64",
            "content": base64.b64encode(new).decode(),
        },
    }

    async def response(endpoint, **_kwargs):
        return responses[endpoint]

    with patch.object(
        pr_review_service,
        "_gh_api_json",
        AsyncMock(side_effect=response),
    ):
        captured = await pr_review_service._fetch_changed_file_contents(
            repo_name="owner/repo",
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            files=[{
                "path": "new.py",
                "previous_path": "old.py",
                "additions": 1,
                "deletions": 1,
            }],
        )

    assert captured == [{
        "path": "new.py",
        "previous_path": "old.py",
        "base": {
            "present": True,
            "mode": "100644",
            "blob_sha": old_blob_sha,
            "byte_length": len(old),
            "available": True,
            "sha256": hashlib.sha256(old).hexdigest(),
            "content": old.decode(),
        },
        "head": {
            "present": True,
            "mode": "100644",
            "blob_sha": new_blob_sha,
            "byte_length": len(new),
            "available": True,
            "sha256": hashlib.sha256(new).hexdigest(),
            "content": new.decode(),
        },
    }]


def test_parse_panel_output_enforces_subject_role_and_blocking_verdict():
    parsed = pr_review_panel.parse_panel_output(
        _output("qa_engineer", blocker=True),
        role="qa_engineer",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    )
    assert parsed["verdict"] == "changes_required"
    assert parsed["findings"][0]["severity"] == "medium"

    wrong = _output("senior_engineer").replace(HEAD_SHA, "c" * 40)
    with pytest.raises(ValueError, match="subject"):
        pr_review_panel.parse_panel_output(
            wrong,
            role="senior_engineer",
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
        )


@pytest.mark.asyncio
async def test_panel_creates_three_independent_tasks_and_gates_findings(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=False,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_pr_review_panel(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review.id)
        .order_by(PRReviewerRun.id)
    )).scalars())
    assert [run.role for run in runs] == list(pr_review_panel.REVIEWER_ROLES)
    assert len({run.task_id for run in runs}) == 3
    tasks = [await db_session.get(Task, run.task_id) for run in runs]
    review_id = review.id
    run_task_specs = [
        (run.id, run.role, task.id)
        for run, task in zip(runs, tasks)
    ]
    assert all("filesystem, shell, network, GitHub" in task.description for task in tasks)
    from backend.api.tasks import _require_pr_review_chat_allowed
    for task in tasks:
        with pytest.raises(HTTPException) as blocked:
            await _require_pr_review_chat_allowed(db_session, task.id)
        assert blocked.value.status_code == 409

    with (
            patch(
                "backend.services.pr_review_service._gh_authenticated_login",
                AsyncMock(return_value="ccm-reviewer"),
            ),
            patch(
                "backend.services.pr_review_service._freeze_safe_merge_method",
                AsyncMock(return_value="merge"),
            ),
        patch(
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for index, (run_id, role, task_id) in enumerate(run_task_specs):
            task = await db_session.get(Task, task_id, populate_existing=True)
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(role, blocker=index == 2),
                timestamp=now,
            ))
            await db_session.commit()
            await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=run_id,
                task_id=task_id,
                retry_count=task.retry_count,
                db_factory=db_factory,
            )

    refreshed = await db_session.get(PRReview, review_id, populate_existing=True)
    findings = list((await db_session.execute(
        select(PRFinding).where(PRFinding.pr_review_id == review_id)
    )).scalars())
    assert refreshed.status == "publishing"
    assert refreshed.pending_action == "review_comments"
    assert len(findings) == 1
    assert findings[0].role == "qa_engineer"
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_clean_panel_arms_frozen_direct_auto_merge(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=True,
        merge_queue_mode="manual",
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_pr_review_panel(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    review_id = review.id
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review_id)
        .order_by(PRReviewerRun.id)
    )).scalars())
    tasks = [await db_session.get(Task, run.task_id) for run in runs]
    assert all(task.metadata_["pr_auto_merge"] is True for task in tasks)
    run_task_specs = [
        (run.id, run.role, task.id, task.retry_count)
        for run, task in zip(runs, tasks)
    ]
    freeze = AsyncMock(side_effect=[
        pr_review_service.GhError("GitHub API HTTP 503"),
        "fast-forward",
    ])

    with (
            patch(
                "backend.services.pr_review_service._gh_authenticated_login",
                AsyncMock(return_value="ccm-reviewer"),
            ),
            patch(
                "backend.services.pr_review_service._freeze_safe_merge_method",
                freeze,
            ),
        patch(
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for run_id, role, task_id, retry_count in run_task_specs:
            task = await db_session.get(
                Task,
                task_id,
                populate_existing=True,
            )
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(role),
                timestamp=now,
            ))
            await db_session.commit()
            result = await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=run_id,
                task_id=task_id,
                retry_count=retry_count,
                db_factory=db_factory,
            )
            if role == pr_review_panel.REVIEWER_ROLES[-1]:
                assert result is False
                transient = await db_session.get(
                    PRReview,
                    review_id,
                    populate_existing=True,
                )
                assert transient.status == "reviewing"
                result = await pr_review_panel.check_and_update_reviewer_run(
                    db_session,
                    reviewer_run_id=run_id,
                    task_id=task_id,
                    retry_count=retry_count,
                    db_factory=db_factory,
                )
                assert result is True

    refreshed = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    assert refreshed.status == "publishing"
    assert refreshed.pending_action == "approved_merged"
    assert refreshed.merge_method == "fast-forward"
    assert freeze.await_count == 2
    publish.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auto_merge", "thread_status"),
    [
        (True, "published_inline"),
        (True, "pending"),
        (False, "pending"),
    ],
)
async def test_clean_panel_waits_for_old_finding_threads_before_any_publication(
    db_session,
    db_factory,
    auto_merge,
    thread_status,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=auto_merge,
        merge_queue_mode="manual",
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_service.create_pr_review_task(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    monitor_run = await db_session.get(PRMonitorRun, review.monitor_run_id)
    old_review = PRReview(
        monitor_run_id=monitor_run.id,
        repo_id=repo.id,
        pr_number=review.pr_number,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha="c" * 40,
        pr_title="older blocked head",
        pr_author="alice",
        pr_url=review.pr_url,
        status="commented",
        action_taken="review_comments",
    )
    db_session.add(old_review)
    await db_session.flush()
    old_reviewer = PRReviewerRun(
        pr_review_id=old_review.id,
        role="qa_engineer",
        provider="claude",
        status="changes_required",
        prompt_policy_hash="4" * 64,
        guide_pack_hash="5" * 64,
    )
    db_session.add(old_reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=old_review.id,
        reviewer_run_id=old_reviewer.id,
        fingerprint="6" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="backend/old.py",
        line=9,
        title="old blocking thread",
        evidence="The old head had a race.",
        impact="The race could lose work.",
        required_fix="Serialize the state transition.",
        test="Exercise the old interleaving.",
        base_sha=BASE_SHA,
        head_sha="c" * 40,
        thread_nonce="7" * 48,
        thread_status=thread_status,
        github_comment_id=(991 if thread_status == "published_inline" else None),
    ))
    await db_session.commit()
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review.id)
        .order_by(PRReviewerRun.id)
    )).scalars())

    with (
        patch(
            "backend.services.pr_review_service._gh_authenticated_login",
            AsyncMock(return_value="ccm-reviewer"),
        ),
        patch(
            "backend.services.pr_review_service._freeze_safe_merge_method",
            AsyncMock(return_value="merge"),
        ),
        patch(
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for reviewer_run in runs:
            task = await db_session.get(
                Task,
                reviewer_run.task_id,
                populate_existing=True,
            )
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(reviewer_run.role),
                timestamp=now,
            ))
            await db_session.commit()
            await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=reviewer_run.id,
                task_id=task.id,
                retry_count=task.retry_count,
                db_factory=db_factory,
            )

    current = await db_session.get(PRReview, review.id, populate_existing=True)
    lifecycle = await db_session.get(
        PRMonitorRun,
        monitor_run.id,
        populate_existing=True,
    )
    assert current.status == "publishing"
    assert current.pending_action == (
        "waiting_threads:approved_merged"
        if auto_merge
        else "waiting_threads:lgtm_comment"
    )
    assert lifecycle.status == "resolving_fixed_threads"
    publish.assert_not_awaited()


def test_finding_fingerprint_distinguishes_location_root_cause_and_path_case():
    finding = {
        "severity": "medium",
        "category": "correctness",
        "path": "backend/Case.py",
        "line": 10,
        "hunk": None,
        "title": "Incorrect transition",
        "evidence": "State A is committed after wake B.",
        "impact": "The worker can become stranded.",
        "required_fix": "Commit A before wake B.",
        "test": "Crash at the transition boundary.",
    }
    base = pr_review_panel._finding_fingerprint("senior_engineer", finding)
    assert base != pr_review_panel._finding_fingerprint(
        "senior_engineer",
        {**finding, "line": 20},
    )
    assert base != pr_review_panel._finding_fingerprint(
        "senior_engineer",
        {**finding, "evidence": "State C overwrites state D."},
    )
    assert base != pr_review_panel._finding_fingerprint(
        "senior_engineer",
        {**finding, "path": "backend/case.py"},
    )


@pytest.mark.asyncio
async def test_worker_panel_recovery_defers_only_until_history_arrives(
    db_session,
    db_factory,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=77,
    )
    with patch(
        "backend.services.pr_review_service._broadcast_review_update",
        AsyncMock(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 0

        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="result",
            role="assistant",
            content=_output(run.role),
            timestamp=task.started_at,
        ))
        await db_session.commit()
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1

    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    refreshed_run = await db_session.get(
        PRReviewerRun,
        run.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "reviewing"
    assert refreshed_run.status == "passed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker_id", "candidate", "expected_error"),
    [
        (88, "malformed terminal candidate", "no valid strict terminal"),
        (None, None, "no terminal output candidates"),
    ],
)
async def test_panel_recovery_fails_closed_for_malformed_or_local_missing_output(
    db_session,
    db_factory,
    worker_id,
    candidate,
    expected_error,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=worker_id,
    )
    monitor = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db_session.add(monitor)
    await db_session.flush()
    review.monitor_run_id = monitor.id
    if candidate is not None:
        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="result",
            role="assistant",
            content=candidate,
            timestamp=task.started_at,
        ))
    await db_session.commit()
    with patch(
        "backend.services.pr_review_service._broadcast_review_update",
        AsyncMock(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1

    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    refreshed_run = await db_session.get(
        PRReviewerRun,
        run.id,
        populate_existing=True,
    )
    refreshed_monitor = await db_session.get(
        PRMonitorRun,
        monitor.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "error"
    assert refreshed_run.status == "error"
    assert expected_error in refreshed_run.error_message
    assert refreshed_monitor.status == "paused"
    assert refreshed_monitor.pause_reason.startswith(
        f"review_error:{review.id}:"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "review model returned a terminal provider error",
        "review transport disconnected after retries",
    ],
)
async def test_reviewer_task_failure_pauses_exact_monitor_once(
    db_session,
    failure,
):
    review, reviewer_run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    monitor = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db_session.add(monitor)
    await db_session.flush()
    review.monitor_run_id = monitor.id
    review_id = review.id
    reviewer_run_id = reviewer_run.id
    task_id = task.id
    monitor_id = monitor.id
    await db_session.commit()

    assert await pr_review_panel.fail_reviewer_run(
        db_session,
        reviewer_run_id=reviewer_run_id,
        task_id=task_id,
        expected_status=task.status,
        retry_count=task.retry_count,
        expected_started_at=task.started_at,
        expected_completed_at=task.completed_at,
        error=failure,
    ) == review_id
    refreshed = await db_session.get(
        PRMonitorRun,
        monitor_id,
        populate_existing=True,
    )
    assert refreshed.status == "paused"
    assert failure[:500] in (refreshed.pause_reason or "")
    terminal_version = refreshed.state_version

    assert await pr_review_panel.fail_reviewer_run(
        db_session,
        reviewer_run_id=reviewer_run_id,
        task_id=task_id,
        expected_status=task.status,
        retry_count=task.retry_count,
        expected_started_at=task.started_at,
        expected_completed_at=task.completed_at,
        error=failure,
    ) is None
    refreshed = await db_session.get(
        PRMonitorRun,
        monitor_id,
        populate_existing=True,
    )
    assert refreshed.state_version == terminal_version


@pytest.mark.asyncio
async def test_reviewer_completion_revalidates_stale_second_session(
    db_session,
    db_factory,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count,
        event_type="result",
        role="assistant",
        content=_output(run.role),
        timestamp=task.started_at,
    ))
    await db_session.commit()

    async with db_factory() as stale_db:
        stale_run = await stale_db.get(PRReviewerRun, run.id)
        assert stale_run.status == "pending"
        async with db_factory() as first_db:
            with patch(
                "backend.services.pr_review_service._broadcast_review_update",
                AsyncMock(),
            ):
                assert await pr_review_panel.check_and_update_reviewer_run(
                    first_db,
                    reviewer_run_id=run.id,
                    task_id=task.id,
                    retry_count=task.retry_count,
                    db_factory=db_factory,
                ) is True
                assert await pr_review_panel.check_and_update_reviewer_run(
                    stale_db,
                    reviewer_run_id=run.id,
                    task_id=task.id,
                    retry_count=task.retry_count,
                    db_factory=db_factory,
                ) is False

    findings = list((await db_session.execute(
        select(PRFinding).where(PRFinding.reviewer_run_id == run.id)
    )).scalars())
    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "reviewing"
    assert findings == []


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["completed", "failed"])
async def test_panel_terminal_consumers_yield_to_active_termination_receipt(
    db_session,
    db_factory,
    task_status,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    task.status = task_status
    if task_status == "completed":
        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="result",
            role="assistant",
            content=_output(run.role),
            timestamp=task.started_at,
        ))
    await db_session.commit()
    review_id = review.id
    run_id = run.id
    task_id = task.id
    retry_count = task.retry_count
    await persist_active_worker_receipt(db_factory, task_id)

    if task_status == "completed":
        changed = await pr_review_panel.check_and_update_reviewer_run(
            db_session,
            reviewer_run_id=run_id,
            task_id=task_id,
            retry_count=retry_count,
            db_factory=db_factory,
        )
    else:
        changed = await pr_review_panel.fail_reviewer_run(
            db_session,
            reviewer_run_id=run_id,
            task_id=task_id,
            expected_status=task.status,
            retry_count=task.retry_count,
            expected_started_at=task.started_at,
            expected_completed_at=task.completed_at,
            error="receipt owns failure arbitration",
        )

    assert not changed
    current_review = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    current_run = await db_session.get(
        PRReviewerRun,
        run_id,
        populate_existing=True,
    )
    assert current_review.status == "reviewing"
    assert current_run.status == "pending"


@pytest.mark.asyncio
async def test_panel_completion_final_cas_yields_to_receipt_race(
    tmp_path,
):
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'panel-receipt-race.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as consumer:
            review, run, task = await _create_recoverable_panel_run(
                consumer,
                worker_id=None,
            )
            consumer.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(run.role),
                timestamp=task.started_at,
            ))
            await consumer.commit()
            review_id = review.id
            run_id = run.id
            task_id = task.id
            retry_count = task.retry_count
            original_guard = pr_review_panel._guard_exact_terminal_task

            async def receipt_wins_before_final_cas(
                db,
                guarded_task,
                *,
                statuses,
            ):
                assert guarded_task.id == task_id
                await persist_active_worker_receipt(sessions, task_id)
                return await original_guard(
                    db,
                    guarded_task,
                    statuses=statuses,
                )

            with patch.object(
                pr_review_panel,
                "_guard_exact_terminal_task",
                side_effect=receipt_wins_before_final_cas,
            ):
                assert await pr_review_panel.check_and_update_reviewer_run(
                    consumer,
                    reviewer_run_id=run_id,
                    task_id=task_id,
                    retry_count=retry_count,
                    db_factory=sessions,
                ) is False

        async with sessions() as verifier:
            current_review = await verifier.get(PRReview, review_id)
            current_run = await verifier.get(PRReviewerRun, run_id)
            assert current_review.status == "reviewing"
            assert current_run.status == "pending"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_panel_startup_recovery_holds_shared_task_operation_lock(
    db_session,
    db_factory,
):
    _, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count,
        event_type="result",
        role="assistant",
        content=_output(run.role),
        timestamp=task.started_at,
    ))
    await db_session.commit()
    task_id = task.id
    from backend.services.worker_proxy import get_task_operation_lock

    original_read = pr_review_panel._read_panel_terminal

    async def assert_locked(*args, **kwargs):
        assert get_task_operation_lock(task_id).locked()
        return await original_read(*args, **kwargs)

    with patch.object(
        pr_review_panel,
        "_read_panel_terminal",
        side_effect=assert_locked,
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1


@pytest.mark.asyncio
async def test_panel_failure_recovery_rejects_new_retry_generation(
    db_session,
    db_factory,
):
    review, reviewer_run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    task.status = "failed"
    await db_session.commit()
    review_id = review.id
    reviewer_run_id = reviewer_run.id
    task_id = task.id

    @asynccontextmanager
    async def retry_wins_after_recovery_scan():
        async with db_factory() as concurrent:
            current = await concurrent.get(Task, task_id)
            current.retry_count += 1
            current.status = "completed"
            current.started_at = datetime.utcnow()
            current.completed_at = datetime.utcnow()
            await concurrent.commit()
        yield

    with patch(
        "backend.services.worker_proxy.get_task_operation_lock",
        side_effect=lambda _task_id: retry_wins_after_recovery_scan(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 0

    current_review = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    current_run = await db_session.get(
        PRReviewerRun,
        reviewer_run_id,
        populate_existing=True,
    )
    current_task = await db_session.get(Task, task_id, populate_existing=True)
    assert current_review.status == "reviewing"
    assert current_run.status == "pending"
    assert current_task.status == "completed"
    assert current_task.retry_count == 1


@pytest.mark.asyncio
async def test_fetch_exact_head_ci_combines_checks_and_statuses():
    responses = [
        {"total_count": 1, "check_runs": [{"id": 12, "name": "tests", "status": "completed", "conclusion": "success", "app": {"id": 15368, "slug": "github-actions"}, "output": {"title": "Tests", "summary": "All passed"}}]},
        {"state": "success", "statuses": [{"id": 13, "context": "lint", "state": "success", "creator": {"login": "ci-bot"}}]},
    ]
    with patch(
        "backend.services.pr_review_service._gh_api_json",
        AsyncMock(side_effect=responses),
    ):
        status, summary, details = await pr_review_panel.fetch_exact_head_ci(
            "owner/repo",
            HEAD_SHA,
            [
                {"kind": "check_run", "name": "tests", "app_slug": "github-actions"},
                {"kind": "status", "name": "lint", "app_slug": "ci-bot"},
            ],
        )
    assert status == "passed"
    assert summary == "2 required exact-head CI checks passed"
    assert [item["state"] for item in details["observed"]] == ["passed", "passed"]
    assert details["observed"][0]["app_id"] == 15368
    assert details["observed"][0]["output"]["summary"] == "All passed"


@pytest.mark.asyncio
async def test_exact_base_guide_manifest_adds_only_declared_regular_files():
    from backend.services import pr_review_service

    tree_sha = "c" * 40
    ccm_sha = "d" * 40
    manifest_sha = "e" * 40
    guide_sha = "f" * 40
    manifest_raw = json.dumps({
        "version": 1,
        "documents": [{
            "path": "docs/architecture/invariants.md",
            "roles": ["principal_engineer", "senior_engineer"],
        }],
    }).encode()
    guide_raw = b"State commits before wake-up."
    responses = [
        {"sha": BASE_SHA, "tree": {"sha": tree_sha}},
        {"sha": tree_sha, "truncated": False, "tree": [
            {"path": ".ccm", "type": "tree", "mode": "040000", "sha": ccm_sha},
        ]},
        {"sha": ccm_sha, "truncated": False, "tree": [
            {"path": "review-guides.json", "type": "blob", "mode": "100644", "sha": manifest_sha, "size": len(manifest_raw)},
        ]},
        {"sha": manifest_sha, "size": len(manifest_raw), "encoding": "base64", "content": base64.b64encode(manifest_raw).decode()},
        {"sha": tree_sha, "truncated": False, "tree": [
            {"path": "docs/architecture/invariants.md", "type": "blob", "mode": "100644", "sha": guide_sha, "size": len(guide_raw)},
        ]},
        {"sha": guide_sha, "size": len(guide_raw), "encoding": "base64", "content": base64.b64encode(guide_raw).decode()},
    ]
    with patch.object(
        pr_review_service,
        "_gh_api_json",
        AsyncMock(side_effect=responses),
    ):
        guides = await pr_review_service._fetch_base_guidance("owner/repo", BASE_SHA)
    assert guides == {
        "CLAUDE.md": None,
        "PROGRESS.md": None,
        "docs/architecture/invariants.md": guide_raw.decode(),
        "__ccm_review_guide_roles__": {
            "docs/architecture/invariants.md": [
                "principal_engineer",
                "senior_engineer",
            ]
        },
    }


@pytest.mark.asyncio
async def test_waiting_ci_reconciler_starts_panel_only_after_pass(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=True,
        auto_merge=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        PR_DATA,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data=PR_DATA,
    )
    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=("passed", "1 required exact-head CI checks passed", {"head_sha": HEAD_SHA, "required": [], "observed": []})),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            AsyncMock(return_value=_context()),
        ),
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 1

    refreshed = await db_session.get(PRReview, review.id, populate_existing=True)
    runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == review.id)
    )).scalars())
    assert refreshed.status == "reviewing"
    assert refreshed.ci_status == "passed"
    assert len(runs) == 3
    tasks = [await db_session.get(Task, run.task_id) for run in runs]
    assert all(task.metadata_["pr_auto_merge"] is True for task in tasks)


@pytest.mark.asyncio
async def test_waiting_ci_reconciler_requires_exact_monitor_run_fence(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/missing-run",
        webhook_secret="s" * 64,
        provider="claude",
        review_mode="panel",
        wait_for_ci=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    pr_data = {
        **PR_DATA,
        "url": "https://github.com/owner/missing-run/pull/17",
    }
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        pr_data,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    await db_session.commit()

    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=(
                "passed",
                "1 required exact-head CI checks passed",
                {"head_sha": HEAD_SHA, "required": [], "observed": []},
            )),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            AsyncMock(return_value={
                **_context(),
                "repo_name": "owner/missing-run",
            }),
        ),
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 0

    refreshed = await db_session.get(PRReview, review.id, populate_existing=True)
    runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == review.id)
    )).scalars())
    assert refreshed.status == "waiting_ci"
    assert runs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("lifecycle_change", ["disable", "supersede"])
async def test_waiting_ci_reconciler_rechecks_lifecycle_after_context_fetch(
    db_session,
    db_factory,
    lifecycle_change,
):
    repo = MonitoredRepo(
        repo_full_name=f"owner/waiting-{lifecycle_change}",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.flush()
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        PR_DATA,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    run = await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data=PR_DATA,
    )
    ids = {"repo": repo.id, "review": review.id, "run": run.id}

    async def change_lifecycle(*_args, **_kwargs):
        async with db_factory() as concurrent:
            changed_repo = await concurrent.get(MonitoredRepo, ids["repo"])
            changed_review = await concurrent.get(PRReview, ids["review"])
            changed_run = await concurrent.get(PRMonitorRun, ids["run"])
            if lifecycle_change == "disable":
                changed_repo.enabled = False
            else:
                changed_review.status = "superseded"
                changed_run.status = "reviewing"
                changed_run.current_head_sha = "c" * 40
                changed_run.state_version += 1
            await concurrent.commit()
        return _context()

    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=(
                "passed",
                "1 required exact-head CI checks passed",
                {"head_sha": HEAD_SHA, "required": [], "observed": []},
            )),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            change_lifecycle,
        ),
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 0

    reviewer_runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == ids["review"])
    )).scalars())
    refreshed_repo = await db_session.get(
        MonitoredRepo,
        ids["repo"],
        populate_existing=True,
    )
    refreshed_review = await db_session.get(
        PRReview,
        ids["review"],
        populate_existing=True,
    )
    assert reviewer_runs == []
    if lifecycle_change == "disable":
        assert refreshed_repo.enabled is False
        assert refreshed_review.status == "waiting_ci"
    else:
        assert refreshed_review.status == "superseded"
