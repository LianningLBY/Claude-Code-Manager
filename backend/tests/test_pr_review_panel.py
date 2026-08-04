"""Independent reviewer panel, structured finding, and CI Gate tests."""

import json
import base64
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.services import pr_review_panel


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
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for index, (run, task) in enumerate(zip(runs, tasks)):
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(run.role, blocker=index == 2),
                timestamp=now,
            ))
            await db_session.commit()
            await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=run.id,
                task_id=task.id,
                retry_count=task.retry_count,
                db_factory=db_factory,
            )

    refreshed = await db_session.get(PRReview, review.id, populate_existing=True)
    findings = list((await db_session.execute(
        select(PRFinding).where(PRFinding.pr_review_id == review.id)
    )).scalars())
    assert refreshed.status == "publishing"
    assert refreshed.pending_action == "review_comments"
    assert len(findings) == 1
    assert findings[0].role == "qa_engineer"
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_exact_head_ci_combines_checks_and_statuses():
    responses = [
        {"total_count": 1, "check_runs": [{"id": 12, "name": "tests", "status": "completed", "conclusion": "success", "app": {"slug": "github-actions"}, "output": {"title": "Tests", "summary": "All passed"}}]},
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
        auto_merge=False,
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
