"""Security and generation tests for PR Monitor review orchestration."""

import asyncio
import base64
import hashlib
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select, update

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import MonitoredRepo, PRFinding, PRReview, PRReviewerRun
from backend.models.task import Task
from backend.services import pr_review_service
from backend.services.pr_review_service import (
    GhError,
    build_review_prompt,
    check_and_update_review,
    create_pr_review_task,
)


PR_DATA = {
    "number": 7,
    "base_sha": "a" * 40,
    "head_sha": "b" * 40,
    "delivery_id": "delivery-7",
    "title": "Fix bug",
    "author": "alice",
    "url": "https://github.com/owner/repo/pull/7",
}
TREE_SHA = "c" * 40
CLAUDE_BLOB_SHA = "d" * 40
PROGRESS_BLOB_SHA = "e" * 40
ACTION_NONCE = "f" * 48
PUBLISHING_STARTED_AT = datetime(2026, 7, 31, 0, 0, 0)
ACTOR = "ccm-bot"


def _make_repo(**overrides) -> MonitoredRepo:
    values = {
        "repo_full_name": "owner/repo",
        "webhook_secret": "s" * 64,
        "auto_merge": False,
        "default_branch": "main",
        "allowed_authors": [],
        "review_model": "claude-sonnet-4-6",
        "provider": "claude",
    }
    values.update(overrides)
    return MonitoredRepo(**values)


def _snapshot(
    *,
    state="OPEN",
    base_sha=PR_DATA["base_sha"],
    head_sha=PR_DATA["head_sha"],
    is_draft=False,
    merged_at=None,
):
    return {
        "state": state,
        "baseRefOid": base_sha,
        "headRefOid": head_sha,
        "isDraft": is_draft,
        "mergedAt": merged_at,
    }


def _review_response(
    *,
    state="APPROVED",
    head_sha=PR_DATA["head_sha"],
    body=f"review\n\nCCM review nonce: {ACTION_NONCE}",
):
    return {
        "id": 91,
        "state": state,
        "commit_id": head_sha,
        "body": body,
        "user": {"login": ACTOR},
        "submitted_at": "2026-07-31T00:00:01Z",
    }


def _comment_response(
    *,
    body=f"comment\n\nCCM review nonce: {ACTION_NONCE}",
):
    return {
        "id": 92,
        "state": "COMMENTED",
        "commit_id": PR_DATA["head_sha"],
        "body": body,
        "user": {"login": ACTOR},
        "submitted_at": "2026-07-31T00:00:01Z",
    }


def _terminal_output(result="lgtm_comment", body="Looks good."):
    return (
        "PR_REVIEW_BODY_BEGIN\n"
        f"{body}\n"
        "PR_REVIEW_BODY_END\n"
        f"PR_REVIEW_RESULT: {result}"
    )


def _blob_payload(sha: str, content: bytes) -> dict:
    return {
        "sha": sha,
        "size": len(content),
        "encoding": "base64",
        "content": base64.b64encode(content).decode("ascii"),
    }


def _guidance_api_side_effect(
    claude: bytes = b"# Rules\nUse tests.",
    progress: bytes = b"# Lessons\nPin snapshots.",
):
    entries = [
        {
            "path": "CLAUDE.md",
            "type": "blob",
            "mode": "100644",
            "sha": CLAUDE_BLOB_SHA,
            "size": len(claude),
        },
        {
            "path": "PROGRESS.md",
            "type": "blob",
            "mode": "100755",
            "sha": PROGRESS_BLOB_SHA,
            "size": len(progress),
        },
    ]
    return [
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        {"sha": TREE_SHA, "truncated": False, "tree": entries},
        _blob_payload(CLAUDE_BLOB_SHA, claude),
        _blob_payload(PROGRESS_BLOB_SHA, progress),
    ]


def _prepared_context(
    guidance: dict[str, str | None] | None = None,
) -> dict:
    return {
        "repo_name": "owner/repo",
        "pr_number": PR_DATA["number"],
        "base_sha": PR_DATA["base_sha"],
        "head_sha": PR_DATA["head_sha"],
        "guidance": guidance or {
            "CLAUDE.md": None,
            "PROGRESS.md": None,
        },
        "material": {
            "number": PR_DATA["number"],
            "title": PR_DATA["title"],
            "body": "Description",
            "author": PR_DATA["author"],
            "base_ref": "main",
            "head_ref": "feature",
            "files": [{
                "path": "backend/app.py",
                "additions": 2,
                "deletions": 1,
            }],
            "patch": "diff --git a/backend/app.py b/backend/app.py\n",
        },
    }


def _publisher_kwargs(**overrides) -> dict:
    values = {
        "repo_name": "owner/repo",
        "pr_number": PR_DATA["number"],
        "base_sha": PR_DATA["base_sha"],
        "head_sha": PR_DATA["head_sha"],
        "result": "lgtm_comment",
        "review_body": "",
        "auto_merge": False,
        "nonce": ACTION_NONCE,
        "actor": ACTOR,
        "current_actor": ACTOR,
        "publishing_started_at": PUBLISHING_STARTED_AT,
        "ensure_current": AsyncMock(return_value=True),
    }
    values.update(overrides)
    return values


@pytest_asyncio.fixture
async def repo(db_session):
    value = _make_repo()
    db_session.add(value)
    await db_session.commit()
    await db_session.refresh(value)
    return value


@pytest.fixture
def no_broadcast():
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with patch("backend.main.broadcaster", broadcaster):
        yield broadcaster


async def _make_review(
    db,
    repo,
    *,
    auto_merge=False,
    retry_count=2,
    task_status="completed",
    nonce=ACTION_NONCE,
):
    started_at = datetime.utcnow()
    task = Task(
        title="PR review task",
        description="review",
        status=task_status,
        retry_count=retry_count,
        started_at=started_at,
        completed_at=(
            started_at + timedelta(seconds=2)
            if task_status == "completed"
            else None
        ),
        metadata_={
            "pr_auto_merge": auto_merge,
            "pr_action_nonce": nonce,
        },
    )
    db.add(task)
    await db.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=PR_DATA["number"],
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        delivery_id=PR_DATA["delivery_id"],
        pr_title=PR_DATA["title"],
        pr_author=PR_DATA["author"],
        pr_url=PR_DATA["url"],
        status="reviewing",
        task_id=task.id,
        action_nonce=nonce,
    )
    db.add(review)
    await db.commit()
    await db.refresh(task)
    await db.refresh(review)
    return review, task


async def _add_terminal_log(
    db,
    task: Task,
    *,
    result="lgtm_comment",
    body="Looks good.",
    retry_count: int | None = None,
) -> LogEntry:
    entry = LogEntry(
        task_id=task.id,
        task_retry_count=(
            task.retry_count if retry_count is None else retry_count
        ),
        event_type="result",
        content=_terminal_output(result, body),
        timestamp=task.started_at + timedelta(seconds=1),
    )
    db.add(entry)
    await db.commit()
    return entry


async def _arm_publishing(
    db,
    review: PRReview,
    task: Task,
    *,
    action="lgtm_comment",
    body="Looks good.",
) -> None:
    review.status = "publishing"
    review.pending_action = action
    review.pending_review_body = body
    review.publishing_actor = ACTOR
    review.publishing_retry_count = task.retry_count
    review.publishing_task_started_at = task.started_at
    review.publishing_started_at = PUBLISHING_STARTED_AT
    await db.commit()
    await db.refresh(review)


# ---------------------------------------------------------------------------
# Prompt and backend-fetched base guidance
# ---------------------------------------------------------------------------


def test_build_review_prompt_injects_verified_documents_as_json():
    documents = {
        "CLAUDE.md": "Rule: use tests.\n`$(never-run)`",
        "PROGRESS.md": "Lesson: keep snapshots pinned.",
    }
    prompt = build_review_prompt(
        _make_repo(auto_merge=False),
        PR_DATA,
        guidance_documents=documents,
    )

    assert f"Captured base commit: `{PR_DATA['base_sha']}`" in prompt
    assert f"Captured head commit: `{PR_DATA['head_sha']}`" in prompt
    assert "Do not read `CLAUDE.md`, `AGENTS.md`, or `PROGRESS.md`" in prompt
    assert "CCM already fetched the exact root tree" in prompt
    assert "Do not run `gh pr review`, `gh pr comment`, `gh pr merge`" in prompt
    assert "PR_REVIEW_RESULT: lgtm_comment" in prompt

    injected = prompt.split(
        "<ccm_verified_base_guidance>\n", 1
    )[1].split("\n</ccm_verified_base_guidance>", 1)[0]
    records = [json.loads(line) for line in injected.splitlines()]
    assert [record["name"] for record in records] == [
        "CLAUDE.md",
        "PROGRESS.md",
    ]
    assert records[0]["content"] == documents["CLAUDE.md"]
    assert records[0]["byte_length"] == len(
        documents["CLAUDE.md"].encode()
    )
    assert records[0]["sha256"] == hashlib.sha256(
        documents["CLAUDE.md"].encode()
    ).hexdigest()


def test_build_review_prompt_records_optional_documents_as_absent():
    prompt = build_review_prompt(
        _make_repo(auto_merge=True),
        PR_DATA,
        guidance_documents={"CLAUDE.md": None, "PROGRESS.md": None},
    )
    assert '{"name":"CLAUDE.md","present":false}' in prompt
    assert '{"name":"PROGRESS.md","present":false}' in prompt
    assert "PR_REVIEW_RESULT: approved_merged" in prompt


def test_build_review_prompt_uses_three_lens_evidence_harness():
    prompt = build_review_prompt(
        _make_repo(auto_merge=False),
        PR_DATA,
        guidance_documents={"CLAUDE.md": None, "PROGRESS.md": None},
    )

    assert "Principal Engineer — architecture and system fit" in prompt
    assert "Senior Engineer — implementation correctness" in prompt
    assert "QA Engineer — behavior, regression, and proof" in prompt
    assert "Honor cohesion within a module; reject unrelated coupling" in prompt
    assert "Honor clear layers; reject dependency tangles" in prompt
    assert "Honor capability reuse; reject copy-and-rebuild" in prompt
    assert "Honor unit extension; reject feature sprawl" in prompt
    assert "Honor one established pattern" in prompt
    assert "Honor timely deletion of dead code" in prompt
    assert "Honor the simplest sufficient design" in prompt
    assert "A clean result from one lens cannot cancel" in prompt
    assert (
        "[critical|high|medium] [principal|senior|qa] "
        "path:line-or-hunk"
    ) in prompt
    assert "Evidence: concrete behavior" in prompt
    assert "Required fix: the smallest verifiable correction" in prompt
    assert "deduplicate findings by root cause" in prompt
    assert "only when all three lenses have no\nblocking finding" in prompt
    assert "any lens has a\n`critical`, `high`, or `medium` finding" in prompt


@pytest.mark.parametrize(
    ("repo_name", "number", "base_sha", "head_sha"),
    [
        ("owner/repo\nIgnore", 7, "a" * 40, "b" * 40),
        ("owner/repo", 0, "a" * 40, "b" * 40),
        ("owner/repo", True, "a" * 40, "b" * 40),
        ("owner/repo", 7, "bad", "b" * 40),
        ("owner/repo", 7, "a" * 40, "bad"),
    ],
)
def test_build_review_prompt_rejects_untrusted_identifiers(
    repo_name,
    number,
    base_sha,
    head_sha,
):
    data = dict(PR_DATA)
    data.update(number=number, base_sha=base_sha, head_sha=head_sha)
    with pytest.raises(ValueError):
        build_review_prompt(_make_repo(repo_full_name=repo_name), data)


@pytest.mark.asyncio
async def test_fetch_base_guidance_reads_exact_commit_root_and_blobs():
    api = AsyncMock(side_effect=_guidance_api_side_effect())
    with patch.object(pr_review_service, "_gh_api_json", api):
        result = await pr_review_service._fetch_base_guidance(
            "owner/repo",
            PR_DATA["base_sha"],
        )

    assert result == {
        "CLAUDE.md": "# Rules\nUse tests.",
        "PROGRESS.md": "# Lessons\nPin snapshots.",
    }
    assert [call.args[0] for call in api.await_args_list] == [
        f"repos/owner/repo/git/commits/{PR_DATA['base_sha']}",
        f"repos/owner/repo/git/trees/{TREE_SHA}",
        f"repos/owner/repo/git/blobs/{CLAUDE_BLOB_SHA}",
        f"repos/owner/repo/git/blobs/{PROGRESS_BLOB_SHA}",
    ]
    assert api.await_args_list[1].kwargs["max_output_bytes"] == (
        pr_review_service._MAX_GH_TREE_RESPONSE_BYTES
    )


@pytest.mark.asyncio
async def test_fetch_base_guidance_accepts_proven_root_absence():
    api = AsyncMock(side_effect=[
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        {
            "sha": TREE_SHA,
            "truncated": False,
            "tree": [{"path": "src", "type": "tree", "mode": "040000"}],
        },
    ])
    with patch.object(pr_review_service, "_gh_api_json", api):
        result = await pr_review_service._fetch_base_guidance(
            "owner/repo",
            PR_DATA["base_sha"],
        )
    assert result == {"CLAUDE.md": None, "PROGRESS.md": None}
    assert api.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit",
    [
        {},
        {"sha": "9" * 40, "tree": {"sha": TREE_SHA}},
        {"sha": PR_DATA["base_sha"], "tree": {"sha": "bad"}},
    ],
)
async def test_fetch_base_guidance_rejects_mismatched_commit(commit):
    with patch.object(
        pr_review_service,
        "_gh_api_json",
        AsyncMock(return_value=commit),
    ):
        with pytest.raises(GhError, match="commit response"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tree",
    [
        {"sha": TREE_SHA, "truncated": True, "tree": []},
        {"sha": "9" * 40, "truncated": False, "tree": []},
        {"sha": TREE_SHA, "truncated": False, "tree": "not-a-list"},
    ],
)
async def test_fetch_base_guidance_rejects_unproven_tree(tree):
    api = AsyncMock(side_effect=[
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        tree,
    ])
    with patch.object(pr_review_service, "_gh_api_json", api):
        with pytest.raises(GhError, match="tree response"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["120000", "160000", "040000"])
async def test_fetch_base_guidance_rejects_symlink_or_non_regular(mode):
    api = AsyncMock(side_effect=[
        {"sha": PR_DATA["base_sha"], "tree": {"sha": TREE_SHA}},
        {
            "sha": TREE_SHA,
            "truncated": False,
            "tree": [{
                "path": "CLAUDE.md",
                "type": "blob",
                "mode": mode,
                "sha": CLAUDE_BLOB_SHA,
                "size": 1,
            }],
        },
    ])
    with patch.object(pr_review_service, "_gh_api_json", api):
        with pytest.raises(GhError, match="unsafe root guidance"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


@pytest.mark.parametrize(
    ("entry_overrides", "blob_overrides", "error"),
    [
        ({}, {"content": "%%%"}, "base64"),
        (
            {"size": 4},
            {"content": base64.b64encode(b"bad\x00").decode(), "size": 4},
            "NUL",
        ),
        ({}, {"content": base64.b64encode(b"\xff").decode(), "size": 1}, "UTF-8"),
        ({}, {"sha": "9" * 40}, "malformed"),
        ({"size": 2}, {"size": 2}, "declared size"),
        (
            {"size": pr_review_service._MAX_GUIDANCE_FILE_BYTES + 1},
            {},
            "oversized",
        ),
    ],
)
def test_decode_guidance_blob_fails_closed(
    entry_overrides,
    blob_overrides,
    error,
):
    content = b"x"
    entry = {
        "sha": CLAUDE_BLOB_SHA,
        "size": len(content),
        **entry_overrides,
    }
    blob = {
        **_blob_payload(CLAUDE_BLOB_SHA, content),
        **blob_overrides,
    }
    with pytest.raises(GhError, match=error):
        pr_review_service._decode_guidance_blob(
            name="CLAUDE.md",
            entry=entry,
            blob=blob,
        )


@pytest.mark.asyncio
async def test_fetch_base_guidance_enforces_combined_limit():
    each = b"x" * (200 * 1024)
    api = AsyncMock(side_effect=_guidance_api_side_effect(each, each))
    with patch.object(pr_review_service, "_gh_api_json", api):
        with pytest.raises(GhError, match="combined"):
            await pr_review_service._fetch_base_guidance(
                "owner/repo",
                PR_DATA["base_sha"],
            )


def _compare_identity(
    *,
    base_sha=PR_DATA["base_sha"],
    head_sha=PR_DATA["head_sha"],
    total_commits=1,
    commits=None,
    url=None,
):
    endpoint = (
        "repos/owner/repo/compare/"
        f"{PR_DATA['base_sha']}...{PR_DATA['head_sha']}"
    )
    return {
        "base_commit": {"sha": base_sha},
        "commits": commits if commits is not None else [{"sha": head_sha}],
        "total_commits": total_commits,
        "url": url or f"https://api.github.com/{endpoint}",
    }


@pytest.mark.asyncio
async def test_fetch_patch_uses_immutable_captured_sha_endpoint():
    endpoint = (
        "repos/owner/repo/compare/"
        f"{PR_DATA['base_sha']}...{PR_DATA['head_sha']}"
    )
    patch_bytes = (
        f"From {PR_DATA['head_sha']} Mon Sep 17 00:00:00 2001\n"
        "Subject: [PATCH] pinned\n\n"
        "diff --git a/app.py b/app.py\n"
    ).encode()
    api = AsyncMock(return_value=_compare_identity())
    runner = AsyncMock(return_value=(0, patch_bytes, b""))
    with (
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(pr_review_service, "_run_gh", runner),
    ):
        result = await pr_review_service._fetch_immutable_compare_patch(
            repo_name="owner/repo",
            base_sha=PR_DATA["base_sha"],
            head_sha=PR_DATA["head_sha"],
        )

    assert result == patch_bytes.decode()
    api.assert_awaited_once_with(
        f"{endpoint}?per_page=100&page=1",
        max_output_bytes=pr_review_service._MAX_GH_COMPARE_RESPONSE_BYTES,
    )
    runner.assert_awaited_once_with(
        "api",
        endpoint,
        "-H",
        "Accept: application/vnd.github.v3.patch",
        timeout=60,
    )
    assert str(PR_DATA["number"]) not in runner.await_args.args


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity", "error"),
    [
        (_compare_identity(base_sha="9" * 40), "identity response"),
        (_compare_identity(head_sha="9" * 40), "captured head"),
        (
            _compare_identity(
                url="https://api.github.com/repos/owner/repo/"
                f"compare/{PR_DATA['base_sha']}...{'9' * 40}"
            ),
            "identity response",
        ),
    ],
)
async def test_fetch_patch_rejects_compare_identity_mismatch(
    identity,
    error,
):
    runner = AsyncMock()
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value=identity),
        ),
        patch.object(pr_review_service, "_run_gh", runner),
    ):
        with pytest.raises(GhError, match=error):
            await pr_review_service._fetch_immutable_compare_patch(
                repo_name="owner/repo",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
            )
    runner.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_patch_rejects_patch_for_a_different_head():
    patch_bytes = (
        f"From {'9' * 40} Mon Sep 17 00:00:00 2001\n"
        "Subject: [PATCH] wrong\n"
    ).encode()
    with (
        patch.object(
            pr_review_service,
            "_gh_api_json",
            AsyncMock(return_value=_compare_identity()),
        ),
        patch.object(
            pr_review_service,
            "_run_gh",
            AsyncMock(return_value=(0, patch_bytes, b"")),
        ),
    ):
        with pytest.raises(GhError, match="patch identity"):
            await pr_review_service._fetch_immutable_compare_patch(
                repo_name="owner/repo",
                base_sha=PR_DATA["base_sha"],
                head_sha=PR_DATA["head_sha"],
            )


# ---------------------------------------------------------------------------
# Task creation: prefetch first, inject exact docs, freeze nonce/policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_pr_review_task_prefetches_guidance_and_freezes_nonce(
    db_session,
    repo,
):
    documents = {
        "CLAUDE.md": "Always test.",
        "PROGRESS.md": "Never trust head docs.",
    }
    prepared = _prepared_context(documents)
    broadcaster = MagicMock(broadcast=AsyncMock())
    dispatcher = MagicMock(wake=MagicMock())
    with (
        patch.object(
            pr_review_service,
            "prepare_pr_review_context",
            AsyncMock(return_value=prepared),
        ) as prepare,
        patch.object(
            pr_review_service.secrets,
            "token_hex",
            return_value=ACTION_NONCE,
        ),
        patch("backend.main.broadcaster", broadcaster),
        patch("backend.main.dispatcher", dispatcher),
    ):
        review = await create_pr_review_task(db_session, repo, PR_DATA)

    task = await db_session.get(Task, review.task_id)
    assert review.status == "reviewing"
    assert review.base_sha == PR_DATA["base_sha"]
    assert review.head_sha == PR_DATA["head_sha"]
    assert review.action_nonce == ACTION_NONCE
    assert task.tags == ["pr-review"]
    assert task.metadata_ == {
        "pr_review_id": review.id,
        "pr_base_sha": PR_DATA["base_sha"],
        "pr_head_sha": PR_DATA["head_sha"],
        "pr_auto_merge": False,
        "pr_action_nonce": ACTION_NONCE,
    }
    assert "Always test." in task.description
    assert "Never trust head docs." in task.description
    prepare.assert_awaited_once_with(repo, PR_DATA)
    dispatcher.wake.assert_called_once()
    broadcaster.broadcast.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_pr_review_task_fetch_failure_stages_nothing(
    db_session,
    repo,
):
    with patch.object(
        pr_review_service,
        "prepare_pr_review_context",
        AsyncMock(side_effect=GhError("tree unavailable")),
    ):
        with pytest.raises(GhError, match="tree unavailable"):
            await create_pr_review_task(db_session, repo, PR_DATA)

    assert (await db_session.execute(select(PRReview))).scalars().all() == []
    assert (await db_session.execute(select(Task))).scalars().all() == []


@pytest.mark.asyncio
async def test_create_pr_review_task_codex_uses_codex_default(
    db_session,
):
    repo = _make_repo(provider="codex", review_model=None)
    db_session.add(repo)
    await db_session.commit()
    with (
        patch.object(
            pr_review_service,
            "prepare_pr_review_context",
            AsyncMock(return_value=_prepared_context()),
        ),
        patch.object(
            pr_review_service.secrets,
            "token_hex",
            return_value=ACTION_NONCE,
        ),
    ):
        review = await create_pr_review_task(db_session, repo, PR_DATA)
    task = await db_session.get(Task, review.task_id)
    assert task.provider == "codex"
    assert task.model


# ---------------------------------------------------------------------------
# Strict terminal recommendation and exact retry generation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("content", "expected_result", "expected_body", "error_fragment"),
    [
        (
            _terminal_output("lgtm_comment", "Looks good."),
            "lgtm_comment",
            "Looks good.",
            None,
        ),
        (
            _terminal_output("review_comments", "Fix race."),
            "review_comments",
            "Fix race.",
            None,
        ),
        (
            _terminal_output("approved_merged", ""),
            "approved_merged",
            "",
            None,
        ),
        ("PR_REVIEW_RESULT: lgtm_comment", None, None, "exactly one"),
        (
            _terminal_output() + "\n",
            None,
            None,
            "exactly one",
        ),
        (
            _terminal_output() + "\n" + _terminal_output(),
            None,
            None,
            "exactly one",
        ),
        (
            _terminal_output("review_comments", ""),
            None,
            None,
            "non-empty",
        ),
        (
            _terminal_output("lgtm_comment", "bad\x00body"),
            None,
            None,
            "NUL",
        ),
    ],
)
def test_parse_pr_review_output_is_strict(
    content,
    expected_result,
    expected_body,
    error_fragment,
):
    result, body, error = pr_review_service._parse_pr_review_output(content)
    assert result == expected_result
    assert body == expected_body
    if error_fragment is None:
        assert error is None
    else:
        assert error_fragment in error


def test_parse_pr_review_output_rejects_oversized_body():
    result, body, error = pr_review_service._parse_pr_review_output(
        _terminal_output(
            "review_comments",
            "x" * (pr_review_service._MAX_REVIEW_BODY_BYTES + 1),
        )
    )
    assert result is None and body is None
    assert "61440-byte" in error


@pytest.mark.asyncio
async def test_read_terminal_output_uses_only_exact_retry_generation(
    db_session,
    repo,
):
    review, task = await _make_review(
        db_session,
        repo,
        retry_count=5,
    )
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content=_terminal_output("lgtm_comment", "Current."),
            timestamp=task.started_at + timedelta(seconds=1),
        ),
        # Higher id and a current-looking timestamp must not let retry 4 win.
        LogEntry(
            task_id=task.id,
            task_retry_count=4,
            event_type="result",
            content=_terminal_output("review_comments", "Stale."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
    ])
    await db_session.commit()

    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            5,
        )
    )
    assert (result, body, error) == ("lgtm_comment", "Current.", None)
    assert review.task_id == task.id


@pytest.mark.asyncio
async def test_read_terminal_output_ignores_late_backfilled_chatter(
    db_session,
    repo,
):
    _review, task = await _make_review(
        db_session,
        repo,
        retry_count=5,
    )
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="result",
            role="assistant",
            content=_terminal_output("lgtm_comment", "Verified."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
        # An older Worker message may be appended later with a higher local id.
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content="Still checking the patch.",
            timestamp=task.started_at + timedelta(seconds=1),
        ),
    ])
    await db_session.commit()

    output = await pr_review_service._read_terminal_pr_review_result(
        db_session,
        task.id,
        5,
    )

    assert output == ("lgtm_comment", "Verified.", None)


@pytest.mark.asyncio
async def test_read_terminal_output_rejects_conflicting_strict_blocks(
    db_session,
    repo,
):
    _review, task = await _make_review(
        db_session,
        repo,
        retry_count=5,
    )
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content=_terminal_output("lgtm_comment", "First."),
            timestamp=task.started_at + timedelta(seconds=1),
        ),
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="result",
            role="assistant",
            content=_terminal_output("review_comments", "Second."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
    ])
    await db_session.commit()

    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            5,
        )
    )

    assert result is None and body is None
    assert "conflicting terminal outputs" in error


@pytest.mark.asyncio
async def test_read_terminal_output_rejects_unscoped_legacy_log(
    db_session,
    repo,
):
    _review, task = await _make_review(
        db_session,
        repo,
        retry_count=6,
    )
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=None,
        event_type="result",
        content=_terminal_output(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()

    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            6,
        )
    )
    assert result is None and body is None
    assert "no terminal output" in error


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_count", [None, -1, True])
async def test_read_terminal_output_requires_explicit_retry_generation(
    db_session,
    repo,
    retry_count,
):
    _review, task = await _make_review(db_session, repo)
    result, body, error = (
        await pr_review_service._read_terminal_pr_review_result(
            db_session,
            task.id,
            retry_count,
        )
    )
    assert result is None and body is None
    assert "missing or invalid" in error


# ---------------------------------------------------------------------------
# Backend-only GitHub publishing (structured stdin JSON, pinned commit/nonce)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gh_api_json_sends_dynamic_body_only_over_stdin():
    malicious = "Review `touch /tmp/nope` and $(touch /tmp/nope)"
    process = SimpleNamespace(returncode=0)
    seen = {}

    async def communicate(value=None):
        seen["stdin"] = value
        return b'{"id":1}', b""

    process.communicate = communicate
    spawn = AsyncMock(return_value=process)
    with patch.object(
        pr_review_service.asyncio,
        "create_subprocess_exec",
        spawn,
    ):
        result = await pr_review_service._gh_api_json(
            "repos/owner/repo/pulls/7/reviews",
            method="POST",
            payload={"body": malicious},
        )

    assert result == {"id": 1}
    argv = spawn.await_args.args
    assert argv == (
        "gh",
        "api",
        "--method",
        "POST",
        "repos/owner/repo/pulls/7/reviews",
        "--input",
        "-",
    )
    assert malicious not in " ".join(argv)
    assert json.loads(seen["stdin"]) == {"body": malicious}
    assert spawn.await_args.kwargs["stdin"] is asyncio.subprocess.PIPE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stdout", "stderr", "returncode", "match"),
    [
        (b"not-json", b"", 0, "invalid gh output"),
        (b"[]", b"", 0, "expected a JSON object"),
        (b"", b"HTTP 401: bad credentials", 1, "HTTP 401"),
    ],
)
async def test_gh_api_json_fails_closed(
    stdout,
    stderr,
    returncode,
    match,
):
    process = SimpleNamespace(returncode=returncode)

    async def communicate(_value=None):
        return stdout, stderr

    process.communicate = communicate
    with patch.object(
        pr_review_service.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=process),
    ):
        with pytest.raises(GhError, match=match):
            await pr_review_service._gh_api_json("repos/owner/repo")


@pytest.mark.asyncio
async def test_publish_changes_review_uses_pinned_commit_nonce_and_json():
    gh_view = AsyncMock(return_value=_snapshot())
    api = AsyncMock(return_value=_review_response(
        state="CHANGES_REQUESTED",
        body=f"Fix the race.\n\nCCM review nonce: {ACTION_NONCE}",
    ))
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs(
        result="review_comments",
        review_body="Fix the race.",
    )
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("commented", "review_comments")
    find_review.assert_awaited_once()
    find_merge.assert_not_awaited()
    kwargs["ensure_current"].assert_awaited_once()
    assert api.await_args.args == ("repos/owner/repo/pulls/7/reviews",)
    assert api.await_args.kwargs == {
        "method": "POST",
        "payload": {
            "body": f"Fix the race.\n\nCCM review nonce: {ACTION_NONCE}",
            "commit_id": PR_DATA["head_sha"],
            "event": "REQUEST_CHANGES",
        },
    }


@pytest.mark.asyncio
async def test_publish_lgtm_creates_backend_approval():
    api = AsyncMock(return_value=_review_response())
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs(
        review_body="agent body is not approval evidence",
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)
    assert result == ("approved", "lgtm_comment")
    find_merge.assert_not_awaited()
    payload = api.await_args.kwargs["payload"]
    assert payload["event"] == "APPROVE"
    assert payload["commit_id"] == PR_DATA["head_sha"]
    assert f"CCM review nonce: {ACTION_NONCE}" in payload["body"]
    assert "agent body" not in payload["body"]


@pytest.mark.asyncio
async def test_publish_self_approval_falls_back_to_validated_comment():
    api = AsyncMock(side_effect=[
        GhError("Can not approve your own pull request"),
        _comment_response(),
    ])
    find_review = AsyncMock(side_effect=[None, None])
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(side_effect=[_snapshot(), _snapshot()]),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)
    assert result == ("approved", "lgtm_comment")
    assert api.await_args_list[1].args == (
        "repos/owner/repo/pulls/7/reviews",
    )
    assert api.await_args_list[1].kwargs["method"] == "POST"
    assert api.await_args_list[1].kwargs["payload"]["event"] == "COMMENT"
    assert (
        api.await_args_list[1].kwargs["payload"]["commit_id"]
        == PR_DATA["head_sha"]
    )
    assert kwargs["ensure_current"].await_count == 2
    find_merge.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_self_request_changes_preserves_findings_in_comment():
    api = AsyncMock(side_effect=[
        GhError("Review Can not request changes on your own pull request"),
        _comment_response(body=(
            "blocking findings\n\nCCM review nonce: " + ACTION_NONCE
        )),
    ])
    find_review = AsyncMock(side_effect=[None, None])
    kwargs = _publisher_kwargs(
        result="review_comments",
        review_body="blocking findings",
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(side_effect=[_snapshot(), _snapshot()]),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)
    assert result == ("commented", "review_comments")
    payload = api.await_args_list[1].kwargs["payload"]
    assert payload["event"] == "COMMENT"
    assert payload["commit_id"] == PR_DATA["head_sha"]
    assert "blocking findings" in payload["body"]
    assert f"CCM review nonce: {ACTION_NONCE}" in payload["body"]
    assert kwargs["ensure_current"].await_count == 2


@pytest.mark.asyncio
async def test_publish_auto_merge_pins_head_and_confirms_merge():
    api = AsyncMock(side_effect=[
        _review_response(),
        {"merged": True, "message": "Pull Request successfully merged"},
    ])
    gh_view = AsyncMock(return_value=_snapshot())
    find_review = AsyncMock(return_value=None)
    find_merge = AsyncMock(side_effect=[False, True])
    kwargs = _publisher_kwargs(
        result="approved_merged",
        auto_merge=True,
    )
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)
    assert result == ("merged", "approved_merged")
    merge_call = api.await_args_list[1]
    assert merge_call.args == ("repos/owner/repo/pulls/7/merge",)
    assert merge_call.kwargs["method"] == "PUT"
    assert merge_call.kwargs["payload"]["sha"] == PR_DATA["head_sha"]
    assert ACTION_NONCE in merge_call.kwargs["payload"]["commit_message"]
    assert find_merge.await_count == 2
    assert kwargs["ensure_current"].await_count == 2


@pytest.mark.asyncio
async def test_publish_existing_nonce_evidence_does_not_repeat_write():
    api = AsyncMock()
    gh_view = AsyncMock()
    find_merge = AsyncMock()
    kwargs = _publisher_kwargs()
    with (
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            find_merge,
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("approved", "lgtm_comment")
    api.assert_not_awaited()
    gh_view.assert_not_awaited()
    find_merge.assert_not_awaited()
    kwargs["ensure_current"].assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_rotated_actor_reconciles_old_actor_evidence():
    api = AsyncMock()
    kwargs = _publisher_kwargs(current_actor="replacement-bot")
    with (
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value="APPROVED"),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        result = await pr_review_service._publish_review_action(**kwargs)

    assert result == ("approved", "lgtm_comment")
    api.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_rotated_actor_without_evidence_refuses_new_write():
    api = AsyncMock()
    kwargs = _publisher_kwargs(current_actor="replacement-bot")
    with (
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
    ):
        with pytest.raises(GhError, match="identity changed"):
            await pr_review_service._publish_review_action(**kwargs)

    api.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_snapshot_change_blocks_write():
    api = AsyncMock()
    kwargs = _publisher_kwargs()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot(head_sha="9" * 40)),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        with pytest.raises(GhError, match="snapshot changed"):
            await pr_review_service._publish_review_action(**kwargs)
    api.assert_not_awaited()
    kwargs["ensure_current"].assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_rejects_mismatched_created_review_evidence():
    api = AsyncMock(return_value=_review_response(head_sha="9" * 40))
    kwargs = _publisher_kwargs()
    with (
        patch.object(
            pr_review_service,
            "_gh_pr_view",
            AsyncMock(return_value=_snapshot()),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        with pytest.raises(GhError, match="mismatched review evidence"):
            await pr_review_service._publish_review_action(**kwargs)


# ---------------------------------------------------------------------------
# Completion orchestration: exact generation + frozen policy/nonce
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_review_reads_exact_terminal_body_and_publishes(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=3)
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=3,
        event_type="result",
        content=_terminal_output("lgtm_comment", "No blocking findings."),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()
    expected_task_started_at = task.started_at

    async def publish_after_durable_claim(**_kwargs):
        await db_session.refresh(review)
        assert review.status == "publishing"
        assert review.pending_action == "lgtm_comment"
        assert review.pending_review_body == "No blocking findings."
        assert review.publishing_actor == ACTOR
        assert review.publishing_retry_count == 3
        assert review.publishing_task_started_at == expected_task_started_at
        assert review.publishing_started_at is not None
        return "approved", "lgtm_comment"

    publish = AsyncMock(side_effect=publish_after_durable_claim)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=3,
        )

    await db_session.refresh(review)
    assert review.status == "approved"
    assert review.action_taken == "lgtm_comment"
    assert review.completed_at is not None
    assert review.pending_action is None
    assert review.pending_review_body is None
    assert review.publishing_actor is None
    publish.assert_awaited_once()
    assert publish.await_args.kwargs["review_body"] == "No blocking findings."
    assert publish.await_args.kwargs["actor"] == ACTOR
    assert callable(publish.await_args.kwargs["ensure_current"])
    assert no_broadcast.broadcast.await_count == 2


@pytest.mark.asyncio
async def test_check_review_ignores_newer_output_from_old_retry(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=5)
    db_session.add_all([
        LogEntry(
            task_id=task.id,
            task_retry_count=5,
            event_type="message",
            role="assistant",
            content=_terminal_output("lgtm_comment", "Current."),
            timestamp=task.started_at + timedelta(seconds=1),
        ),
        LogEntry(
            task_id=task.id,
            task_retry_count=4,
            event_type="result",
            content=_terminal_output("review_comments", "Stale."),
            timestamp=task.started_at + timedelta(seconds=2),
        ),
    ])
    await db_session.commit()
    publish = AsyncMock(return_value=("approved", "lgtm_comment"))
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=5,
        )
    assert publish.await_args.kwargs["review_body"] == "Current."


@pytest.mark.asyncio
async def test_check_review_unscoped_log_fails_without_github_write(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, retry_count=6)
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=None,
        event_type="result",
        content=_terminal_output(),
        timestamp=task.started_at + timedelta(seconds=1),
    ))
    await db_session.commit()
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=6,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    assert "no terminal output" in review.review_summary
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_missing_retry_generation_fails_closed(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=None,
        )
    await db_session.refresh(review)
    # Without an exact generation even the terminal error CAS is rejected.
    assert review.status == "reviewing"
    publish.assert_not_awaited()
    no_broadcast.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_missing_nonce_fails_without_publish(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo, nonce="bad")
    await _add_terminal_log(db_session, task)
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    assert "one-time action nonce" in review.review_summary
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auto_merge", "terminal_result"),
    [(False, "approved_merged"), (True, "lgtm_comment")],
)
async def test_check_review_enforces_frozen_action_policy(
    db_session,
    repo,
    no_broadcast,
    auto_merge,
    terminal_result,
):
    review, task = await _make_review(
        db_session,
        repo,
        auto_merge=auto_merge,
    )
    await _add_terminal_log(
        db_session,
        task,
        result=terminal_result,
    )
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_discards_non_owner_task(
    db_session,
    repo,
    no_broadcast,
):
    review, _task = await _make_review(db_session, repo)
    other = Task(
        title="other",
        status="completed",
        retry_count=1,
        started_at=datetime.utcnow(),
    )
    db_session.add(other)
    await db_session.commit()
    await db_session.refresh(other)
    await _add_terminal_log(db_session, other, retry_count=1)
    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=other.id,
            terminal_task_retry_count=1,
        )
    await db_session.refresh(review)
    assert review.status == "reviewing"
    publish.assert_not_awaited()
    no_broadcast.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_review_unconfirmed_publish_error_stays_publishing(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(side_effect=GhError("network timeout")),
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "publishing"
    assert review.action_taken is None
    assert review.pending_action == "lgtm_comment"
    assert review.action_nonce == ACTION_NONCE
    assert "nonce reconciliation" in review.review_summary


@pytest.mark.asyncio
async def test_check_review_terminal_snapshot_error_finishes_error(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(side_effect=GhError(
                "GitHub PR snapshot changed before the backend action"
            )),
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )
    await db_session.refresh(review)
    assert review.status == "error"
    assert review.action_taken == "error"
    assert "snapshot changed" in review.review_summary


@pytest.mark.asyncio
async def test_check_review_actor_rotation_without_evidence_finishes_error(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(side_effect=[ACTOR, "replacement-bot"]),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(
                side_effect=GhError(
                    "GitHub publishing identity changed before durable "
                    "review evidence was found"
                )
            ),
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task.id,
            terminal_task_retry_count=task.retry_count,
        )

    await db_session.refresh(review)
    assert review.status == "error"
    assert review.action_taken == "error"
    assert "identity changed" in review.review_summary


@pytest.mark.asyncio
async def test_check_review_cannot_commit_after_background_handoff_arms(
    db_session,
    repo,
    no_broadcast,
):
    review, task = await _make_review(db_session, repo)
    await _add_terminal_log(db_session, task)
    task_id = task.id
    task_retry_count = task.retry_count
    pending = False

    async def publish_then_arm(**_kwargs):
        nonlocal pending
        pending = True
        await db_session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(pty_background_generation="bg-new")
        )
        await db_session.commit()
        return "approved", "lgtm_comment"

    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            side_effect=publish_then_arm,
        ),
    ):
        await check_and_update_review(
            db_session,
            review.id,
            "owner/repo",
            terminal_task_id=task_id,
            terminal_task_retry_count=task_retry_count,
            background_handoff_pending=lambda: pending,
        )
    await db_session.refresh(review)
    assert review.status == "publishing"
    assert review.pending_action == "lgtm_comment"
    assert review.completed_at is None
    assert no_broadcast.broadcast.await_count == 1


@pytest.mark.asyncio
async def test_recover_publishing_pr_reviews_reuses_nonce_without_write(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id

    api = AsyncMock()
    gh_view = AsyncMock()
    find_review = AsyncMock(return_value="APPROVED")
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(pr_review_service, "_gh_api_json", api),
        patch.object(pr_review_service, "_gh_pr_view", gh_view),
        patch.object(
            pr_review_service,
            "_find_review_evidence",
            find_review,
        ),
        patch.object(
            pr_review_service,
            "_find_merge_evidence",
            AsyncMock(),
        ),
    ):
        recovered = await pr_review_service.recover_publishing_pr_reviews(
            session_factory
        )

    assert recovered == 1
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "approved"
        assert stored.action_taken == "lgtm_comment"
        assert stored.pending_action is None
        assert stored.publishing_actor is None
    find_review.assert_awaited_once()
    api.assert_not_awaited()
    gh_view.assert_not_awaited()


@pytest.mark.asyncio
async def test_recover_publishing_pr_reviews_counts_only_terminal_rows(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id

    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            AsyncMock(side_effect=GhError("temporary network failure")),
        ),
    ):
        recovered = await pr_review_service.recover_publishing_pr_reviews(
            session_factory
        )

    assert recovered == 0
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "publishing"
        assert "nonce reconciliation" in stored.review_summary


@pytest.mark.asyncio
async def test_publication_lease_fences_concurrent_processes(
    session_factory,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _arm_publishing(db, review, task)
        review_id = review.id
        task_id = task.id
        retry_count = task.retry_count
        started_at = task.started_at

    async with session_factory() as first:
        first_token = await pr_review_service._acquire_publication_lease(
            first,
            review_id,
        )
    assert first_token is not None

    async with session_factory() as second:
        assert (
            await pr_review_service._acquire_publication_lease(
                second,
                review_id,
            )
            is None
        )

    async with session_factory() as db:
        await db.execute(
            update(PRReview)
            .where(PRReview.id == review_id)
            .values(
                publishing_lease_expires_at=(
                    datetime.utcnow() - timedelta(seconds=1)
                )
            )
        )
        await db.commit()

    async with session_factory() as second:
        second_token = await pr_review_service._acquire_publication_lease(
            second,
            review_id,
        )
        assert second_token is not None
        assert second_token != first_token
        assert not await pr_review_service._publication_is_current(
            second,
            review_id=review_id,
            task_id=task_id,
            retry_count=retry_count,
            task_started_at=started_at,
            nonce=ACTION_NONCE,
            lease_token=first_token,
        )


@pytest.mark.asyncio
async def test_recover_incomplete_review_claims_exact_completed_generation(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        await _add_terminal_log(db, task)
        review_id = review.id

    publish = AsyncMock(return_value=("approved", "lgtm_comment"))
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value=ACTOR),
        ),
        patch.object(
            pr_review_service,
            "_publish_review_action",
            publish,
        ),
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 1
    publish.assert_awaited_once()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "approved"


@pytest.mark.asyncio
async def test_periodic_pr_recovery_invokes_finding_action_recovery(
    session_factory,
    no_broadcast,
    monkeypatch,
):
    import backend.main as main_module
    from backend.services import pr_review_fix

    relay = object()
    recover_finding_actions = AsyncMock(return_value=3)
    monkeypatch.setattr(main_module, "worker_relay", relay)
    monkeypatch.setattr(
        pr_review_fix,
        "recover_incomplete_finding_actions",
        recover_finding_actions,
        raising=False,
    )

    recovered = await pr_review_service.recover_incomplete_pr_reviews(
        session_factory
    )

    assert recovered == 3
    recover_finding_actions.assert_awaited_once_with(
        session_factory,
        worker_relay=relay,
    )


@pytest.mark.asyncio
async def test_recover_incomplete_worker_review_defers_missing_history(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, _task = await _make_review(db, repo)
        review_id = review.id

    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 0
    publish.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "reviewing"


@pytest.mark.asyncio
async def test_recover_worker_review_defers_early_assistant_chatter(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(db, repo)
        review_id = review.id
        task.worker_id = 44
        db.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="message",
            role="assistant",
            content="I am still reviewing the patch.",
            timestamp=task.started_at + timedelta(seconds=1),
        ))
        await db.commit()

    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 0
    publish.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "reviewing"


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["failed", "cancelled", "conflict"])
async def test_recover_incomplete_terminal_failure_finishes_review_error(
    session_factory,
    no_broadcast,
    task_status,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, _task = await _make_review(
            db,
            repo,
            task_status=task_status,
        )
        review_id = review.id

    recovered = await pr_review_service.recover_incomplete_pr_reviews(
        session_factory
    )

    assert recovered == 1
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "error"
        assert stored.action_taken == "error"
        assert task_status in stored.review_summary


@pytest.mark.asyncio
async def test_recover_incomplete_superseded_terminal_never_publishes(
    session_factory,
    no_broadcast,
):
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="failed",
        )
        task.metadata_ = {
            **(task.metadata_ or {}),
            "pr_review_superseded": True,
        }
        review_id = review.id
        await db.commit()

    publish = AsyncMock()
    with patch.object(
        pr_review_service,
        "_publish_review_action",
        publish,
    ):
        recovered = await pr_review_service.recover_incomplete_pr_reviews(
            session_factory
        )

    assert recovered == 1
    publish.assert_not_awaited()
    async with session_factory() as db:
        stored = await db.get(PRReview, review_id)
        assert stored.status == "superseded"


@pytest.mark.asyncio
async def test_recover_superseding_intent_creates_replacement_after_cleanup(
    session_factory,
    no_broadcast,
):
    from backend.services.task_termination import TaskTerminationResult

    replacement = {
        **PR_DATA,
        "head_sha": "9" * 40,
        "delivery_id": "delivery-replacement",
    }
    context = _prepared_context()
    context["head_sha"] = replacement["head_sha"]
    async with session_factory() as db:
        repo = _make_repo()
        db.add(repo)
        await db.commit()
        await db.refresh(repo)
        review, task = await _make_review(
            db,
            repo,
            task_status="pending",
        )
        task_id = task.id
        review_id = review.id
        review.status = "superseding"
        review.superseding_snapshot = {
            "version": 2,
            "pr_data": replacement,
            "prepared_context": context,
        }
        review.superseding_token = "1" * 48
        review.superseding_started_at = (
            datetime.utcnow() - timedelta(minutes=5)
        )
        await db.commit()

    async def terminate(task_id_arg, db, **_kwargs):
        assert task_id_arg == task_id
        current = await db.get(Task, task_id)
        previous = current.status
        current.status = "completed"
        current.completed_at = datetime.utcnow()
        await db.commit()
        return TaskTerminationResult(
            task_id=task_id,
            previous_status=previous,
            terminal_status="completed",
            transitioned=True,
            stopped=False,
            cleared_messages=0,
            retry_count=current.retry_count,
            instance_id=current.instance_id,
            started_at=current.started_at,
            completed_at=current.completed_at,
            pty_background_generation=None,
        )

    with patch(
            "backend.services.task_termination."
            "terminate_authoritative_task_generation",
            side_effect=terminate,
        ):
        recovered = await pr_review_service.recover_superseding_pr_reviews(
            session_factory,
            grace_seconds=0,
        )

    assert recovered == 1
    async with session_factory() as db:
        old = await db.get(PRReview, review_id)
        reviews = (
            await db.execute(
                select(PRReview).where(PRReview.repo_id == old.repo_id)
            )
        ).scalars().all()
        assert old.status == "superseded"
        assert old.superseding_snapshot is None
        assert old.superseding_token is None
        assert old.superseding_started_at is None
        assert len(reviews) == 2
        new = next(review for review in reviews if review.id != review_id)
        assert new.status == "reviewing"
    assert new.head_sha == replacement["head_sha"]


def _thread_finding(*, line=12):
    return PRFinding(
        id=41,
        pr_review_id=1,
        reviewer_run_id=2,
        fingerprint="1" * 64,
        thread_nonce="2" * 48,
        role="senior_engineer",
        severity="high",
        category="correctness",
        path="backend/app.py",
        line=line,
        title="Broken validation",
        evidence="The invalid branch returns success.",
        impact="Bad input is persisted.",
        required_fix="Return a validation error.",
        test="Exercise the invalid branch.",
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
    )


@pytest.mark.asyncio
async def test_blocking_finding_publishes_independent_inline_thread():
    finding = _thread_finding()
    response = {
        "id": 99,
        "body": pr_review_service._finding_thread_body(finding),
        "user": {"login": ACTOR},
        "html_url": "https://github.test/comment/99",
        "commit_id": PR_DATA["head_sha"],
        "path": finding.path,
    }
    with (
        patch.object(pr_review_service, "_gh_api_value", AsyncMock(return_value=[[]])),
        patch.object(pr_review_service, "_gh_api_json", AsyncMock(return_value=response)) as post,
    ):
        result = await pr_review_service._publish_one_finding_thread(
            repo_name="owner/repo",
            pr_number=7,
            finding=finding,
            actor=ACTOR,
            ensure_current=AsyncMock(return_value=True),
        )
    assert result == ("published_inline", 99, "https://github.test/comment/99", None)
    assert post.await_args.kwargs["payload"]["line"] == 12
    assert post.await_args.kwargs["payload"]["commit_id"] == PR_DATA["head_sha"]


@pytest.mark.asyncio
async def test_unlocatable_finding_falls_back_without_clearing_blocker():
    finding = _thread_finding(line=None)
    response = {
        "id": 100,
        "body": pr_review_service._finding_thread_body(finding),
        "user": {"login": ACTOR},
        "html_url": "https://github.test/comment/100",
    }
    with (
        patch.object(pr_review_service, "_gh_api_value", AsyncMock(return_value=[[]])),
        patch.object(pr_review_service, "_gh_api_json", AsyncMock(return_value=response)) as post,
    ):
        result = await pr_review_service._publish_one_finding_thread(
            repo_name="owner/repo",
            pr_number=7,
            finding=finding,
            actor=ACTOR,
            ensure_current=AsyncMock(return_value=True),
        )
    assert result[0] == "published_fallback"
    assert "blocker remains open" in result[3]
    assert "/issues/7/comments" in post.await_args.args[0]


@pytest.mark.asyncio
async def test_finding_publication_survives_exact_guard_rollback(db_session):
    """A fresh publication guard rolls back and expires ORM state by design."""

    repo = _make_repo(review_mode="panel")
    db_session.add(repo)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=7,
        base_sha=PR_DATA["base_sha"],
        head_sha=PR_DATA["head_sha"],
        pr_title="fixture",
        pr_author="alice",
        pr_url="https://github.test/owner/repo/pull/7",
        status="publishing",
    )
    db_session.add(review)
    await db_session.flush()
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="3" * 64,
        guide_pack_hash="4" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = _thread_finding(line=8)
    finding.id = None
    finding.pr_review_id = review.id
    finding.reviewer_run_id = reviewer.id
    db_session.add(finding)
    await db_session.commit()
    review_id = review.id
    finding_id = finding.id

    async def exact_guard():
        await db_session.rollback()
        return True

    async def post_comment(_endpoint, *, payload=None, **_kwargs):
        return {
            "id": 101,
            "body": payload["body"],
            "user": {"login": ACTOR},
            "html_url": "https://github.test/comment/101",
            "commit_id": PR_DATA["head_sha"],
            "path": "backend/app.py",
        }

    with (
        patch.object(pr_review_service, "_gh_api_value", AsyncMock(return_value=[[]])),
        patch.object(pr_review_service, "_gh_api_json", side_effect=post_comment),
    ):
        await pr_review_service._publish_blocking_finding_threads(
            db_session,
            review_id=review_id,
            repo_name="owner/repo",
            pr_number=7,
            actor=ACTOR,
            ensure_current=exact_guard,
        )

    published = await db_session.get(PRFinding, finding_id, populate_existing=True)
    assert published.thread_status == "published_inline"
    assert published.github_comment_id == 101
