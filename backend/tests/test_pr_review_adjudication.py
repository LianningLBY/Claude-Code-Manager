import asyncio
from datetime import datetime, timedelta

import pytest

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingRebuttal,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.services.pr_review_adjudication import (
    complete_adjudication,
    parse_adjudication_output,
    reconcile_fixed_finding_resolutions,
    reconcile_rebuttal_resolutions,
)
from backend.services.pr_monitor_loop import record_gate_pass


BASE = "a" * 40
HEAD = "b" * 40


def _output(fingerprint: str, verdict: str = "accepted") -> str:
    return (
        "PR_REBUTTAL_ADJUDICATION_BEGIN\n"
        f'{{"schema_version":1,"subject":{{"base_sha":"{BASE}","head_sha":"{HEAD}"}},'
        f'"finding_fingerprint":"{fingerprint}","verdict":"{verdict}",'
        '"reason":"The exact changed-file evidence proves the guarded path."}\n'
        "PR_REBUTTAL_ADJUDICATION_END\n"
        "PR_REVIEW_RESULT: rebuttal_adjudicated"
    )


async def _accepted_resolution_fixture(
    db_session,
    *,
    repo_name: str,
    thread_status: str,
    github_comment_id: int,
):
    repo = MonitoredRepo(
        repo_full_name=repo_name,
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    developer = Task(
        title="Developer",
        description="change",
        status="completed",
        session_id=f"session-{repo_name}",
        last_cwd="/fake/repo",
    )
    db_session.add_all([repo, developer])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=17,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        developer_task_id=developer.id,
        status="adjudicating",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=17,
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="fixture",
        pr_author="bot",
        pr_url=f"https://example.invalid/{repo_name}/pull/17",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="f" * 64,
        role="senior_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=None if thread_status == "published_fallback" else 3,
        title="bad guard",
        evidence="guard missing",
        impact="unsafe",
        required_fix="add guard",
        test="exercise invalid input",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="3" * 48,
        status="resolved_rebutted",
        thread_status=thread_status,
        github_comment_id=github_comment_id,
    )
    db_session.add(finding)
    await db_session.flush()
    rebuttal = PRFindingRebuttal(
        finding_id=finding.id,
        pr_review_id=review.id,
        monitor_run_id=run.id,
        developer_task_id=developer.id,
        attempt=1,
        base_sha=BASE,
        head_sha=HEAD,
        evidence="Concrete exact code evidence.",
        evidence_hash="4" * 64,
        status="accepted",
        verdict="accepted",
        result_body="The exact evidence disproves the Finding.",
        resolution_nonce="5" * 48,
        resolution_actor="ccm-bot",
    )
    db_session.add(rebuttal)
    await db_session.commit()
    return repo, run, review, finding, rebuttal


@pytest.mark.asyncio
async def test_accepted_rebuttal_resolves_exact_github_thread_and_gate(
    db_session, db_factory, monkeypatch
):
    repo = MonitoredRepo(
        repo_full_name="fake/repo", webhook_secret="s" * 64,
        review_mode="panel", auto_repair=True,
    )
    developer = Task(
        title="Developer", description="change", status="completed",
        session_id="dev-session", last_cwd="/fake/repo",
    )
    db_session.add_all([repo, developer])
    await db_session.commit()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=7, current_base_sha=BASE,
        current_head_sha=HEAD, developer_task_id=developer.id,
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=7,
        base_sha=BASE, head_sha=HEAD, pr_title="fake", pr_author="bot",
        pr_url="https://example.invalid/fake/repo/pull/7", status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    run.status = "adjudicating"
    reviewer = PRReviewerRun(
        pr_review_id=review.id, role="senior_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id, reviewer_run_id=reviewer.id,
        fingerprint="f" * 64, role="senior_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="bad guard",
        evidence="guard missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="3" * 48, thread_status="published_inline",
        github_comment_id=123,
    )
    db_session.add(finding)
    await db_session.flush()
    started = datetime.utcnow() - timedelta(seconds=1)
    adjudicator = Task(
        title="Adjudicator", description="judge", status="completed",
        retry_count=0, started_at=started,
        metadata_={"pr_review_id": review.id}, tags=["pr-review"],
    )
    db_session.add(adjudicator)
    await db_session.flush()
    rebuttal = PRFindingRebuttal(
        finding_id=finding.id, pr_review_id=review.id, monitor_run_id=run.id,
        developer_task_id=developer.id, task_id=adjudicator.id, attempt=1,
        base_sha=BASE, head_sha=HEAD, evidence="Concrete exact code evidence.",
        evidence_hash="4" * 64, status="adjudicating", resolution_nonce="5" * 48,
    )
    db_session.add(rebuttal)
    db_session.add(LogEntry(
        task_id=adjudicator.id, task_retry_count=0, event_type="result",
        role="assistant", content=_output(finding.fingerprint),
        timestamp=datetime.utcnow(), is_error=False,
    ))
    await db_session.commit()

    assert parse_adjudication_output(_output(finding.fingerprint), finding=finding)["verdict"] == "accepted"
    await complete_adjudication(
        db_session, adjudication_id=rebuttal.id,
        task_id=adjudicator.id, retry_count=0,
    )
    assert (await db_session.get(PRFinding, finding.id, populate_existing=True)).status == "resolved_rebutted"

    calls = []

    async def fake_gh(endpoint, *, payload=None, **_kwargs):
        calls.append((endpoint, payload))
        if "mutation" in payload["query"]:
            return {"data": {"resolveReviewThread": {"thread": {"id": "T1", "isResolved": True}}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{"id": "T1", "isResolved": False, "comments": {"nodes": [{"databaseId": 123}]}}],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr("backend.services.pr_review_service._gh_api_value", fake_gh)
    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login", fake_login
    )
    assert await reconcile_rebuttal_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert resolved.thread_status == "resolved"
    assert resolved.github_thread_node_id == "T1"
    assert refreshed_run.status == "ready_to_merge"
    refreshed_rebuttal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert refreshed_rebuttal.status == "resolved"
    assert resolved.resolution_lease_token is None
    assert resolved.resolution_lease_expires_at is None
    assert len(calls) == 2


def test_adjudication_rejects_wrong_subject():
    finding = type("Finding", (), {
        "base_sha": BASE, "head_sha": HEAD, "fingerprint": "f" * 64,
    })()
    with pytest.raises(ValueError, match="fixed contract"):
        parse_adjudication_output(
            _output("f" * 64).replace(HEAD, "c" * 40), finding=finding
        )


@pytest.mark.asyncio
async def test_green_new_head_resolves_old_thread_before_merge_gate(
    db_session, db_factory, monkeypatch
):
    new_head = "c" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/repo", webhook_secret="s" * 64,
        review_mode="panel", merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=8, current_base_sha=BASE,
        current_head_sha=new_head, status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=8,
        base_sha=BASE, head_sha=HEAD, pr_title="old", pr_author="bot",
        pr_url="https://example.invalid/fake/repo/pull/8", status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=8,
        base_sha=BASE, head_sha=new_head, pr_title="fixed", pr_author="bot",
        pr_url="https://example.invalid/fake/repo/pull/8", status="approved",
        action_taken="lgtm_comment",
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id, role="qa_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=old_review.id, reviewer_run_id=reviewer.id,
        fingerprint="9" * 64, role="qa_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="bad guard",
        evidence="guard missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="8" * 48, thread_status="published_inline",
        github_comment_id=456,
    )
    db_session.add(finding)
    await db_session.commit()

    await record_gate_pass(db_session, current_review.id)
    waiting = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert waiting.status == "resolving_fixed_threads"

    calls = []

    async def fake_gh(endpoint, *, payload=None, **_kwargs):
        calls.append((endpoint, payload))
        if "mutation" in payload["query"]:
            return {"data": {"resolveReviewThread": {"thread": {
                "id": "OLD-T1", "isResolved": True,
            }}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{
                "id": "OLD-T1", "isResolved": False,
                "comments": {"nodes": [{"databaseId": 456}]},
            }],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr("backend.services.pr_review_service._gh_api_value", fake_gh)
    assert await reconcile_fixed_finding_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    ready = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert resolved.status == "resolved_fixed"
    assert resolved.thread_status == "resolved"
    assert resolved.github_thread_node_id == "OLD-T1"
    assert resolved.thread_resolved_at is not None
    assert ready.status == "ready_to_merge"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_fixed_thread_recovery_advances_gate_after_last_resolution_commit(
    db_session, db_factory
):
    """A crash after the final Finding commit must not strand the run."""

    new_head = "c" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/recovery", webhook_secret="s" * 64,
        review_mode="panel", merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=9, current_base_sha=BASE,
        current_head_sha=new_head, status="resolving_fixed_threads",
    )
    db_session.add(run)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=9,
        base_sha=BASE, head_sha=HEAD, pr_title="old", pr_author="bot",
        pr_url="https://example.invalid/fake/recovery/pull/9",
        status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=9,
        base_sha=BASE, head_sha=new_head, pr_title="fixed", pr_author="bot",
        pr_url="https://example.invalid/fake/recovery/pull/9",
        status="approved", action_taken="lgtm_comment",
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id, role="qa_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=old_review.id, reviewer_run_id=reviewer.id,
        fingerprint="7" * 64, role="qa_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="fixed guard",
        evidence="guard was missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="6" * 48, status="resolved_fixed",
        thread_status="resolved", github_comment_id=789,
        thread_resolved_at=datetime.utcnow(),
    ))
    await db_session.commit()

    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert recovered.status == "ready_to_merge"


@pytest.mark.asyncio
async def test_rebuttal_fallback_resolution_lease_allows_exactly_one_post(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/fallback-race",
        thread_status="published_fallback",
        github_comment_id=801,
    )
    post_started = asyncio.Event()
    allow_post = asyncio.Event()
    post_calls = 0

    async def fake_gh(endpoint, *, method="GET", payload=None, **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            post_started.set()
            await allow_post.wait()
            return {"id": 9001}
        return [[]]

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    first = asyncio.create_task(reconcile_rebuttal_resolutions(db_factory))
    await asyncio.wait_for(post_started.wait(), timeout=2)
    second = asyncio.create_task(reconcile_rebuttal_resolutions(db_factory))
    assert await asyncio.wait_for(second, timeout=2) == 0
    allow_post.set()
    assert await asyncio.wait_for(first, timeout=2) == 1

    assert post_calls == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    terminal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert resolved.thread_status == "resolved"
    assert terminal.status == "resolved"


@pytest.mark.asyncio
async def test_cancelled_resolution_leaves_lease_for_expiry_recovery(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/cancelled-resolution",
        thread_status="published_fallback",
        github_comment_id=804,
    )
    post_started = asyncio.Event()
    never_finishes = asyncio.Event()

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        if method == "POST":
            post_started.set()
            await never_finishes.wait()
            return {"id": 9003}
        return [[]]

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    reconciliation = asyncio.create_task(
        reconcile_rebuttal_resolutions(db_factory)
    )
    await asyncio.wait_for(post_started.wait(), timeout=2)
    reconciliation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reconciliation

    leased = await db_session.get(PRFinding, finding.id, populate_existing=True)
    still_accepted = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert leased.thread_status == "published_fallback"
    assert leased.resolution_lease_token is not None
    assert leased.resolution_lease_expires_at is not None
    assert still_accepted.status == "accepted"


@pytest.mark.asyncio
async def test_fixed_inline_resolution_lease_allows_exactly_one_mutation(
    db_session, db_factory, monkeypatch
):
    new_head = "d" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/inline-race",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=18,
        current_base_sha=BASE,
        current_head_sha=new_head,
        status="resolving_fixed_threads",
    )
    db_session.add(run)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=18,
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="old",
        pr_author="bot",
        pr_url="https://example.invalid/fake/inline-race/pull/18",
        status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=18,
        base_sha=BASE,
        head_sha=new_head,
        pr_title="fixed",
        pr_author="bot",
        pr_url="https://example.invalid/fake/inline-race/pull/18",
        status="approved",
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id,
        role="qa_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="6" * 64,
        guide_pack_hash="7" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=old_review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="8" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=3,
        title="old bug",
        evidence="bug existed",
        impact="unsafe",
        required_fix="fix it",
        test="regression",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="9" * 48,
        thread_status="published_inline",
        github_comment_id=802,
    )
    db_session.add(finding)
    await db_session.commit()

    mutation_started = asyncio.Event()
    allow_mutation = asyncio.Event()
    mutation_calls = 0

    async def fake_gh(_endpoint, *, payload=None, **_kwargs):
        nonlocal mutation_calls
        if "mutation" in payload["query"]:
            mutation_calls += 1
            mutation_started.set()
            await allow_mutation.wait()
            return {"data": {"resolveReviewThread": {"thread": {
                "id": "OLD-T2", "isResolved": True,
            }}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{
                "id": "OLD-T2",
                "isResolved": False,
                "comments": {"nodes": [{"databaseId": 802}]},
            }],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    first = asyncio.create_task(reconcile_fixed_finding_resolutions(db_factory))
    await asyncio.wait_for(mutation_started.wait(), timeout=2)
    second = asyncio.create_task(reconcile_fixed_finding_resolutions(db_factory))
    assert await asyncio.wait_for(second, timeout=2) == 0
    allow_mutation.set()
    assert await asyncio.wait_for(first, timeout=2) == 1

    assert mutation_calls == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    assert resolved.thread_status == "resolved"
    assert resolved.resolution_lease_token is None


@pytest.mark.asyncio
async def test_expired_rebuttal_resolution_lease_recovers_existing_effect(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/expired-lease",
        thread_status="published_fallback",
        github_comment_id=803,
    )
    finding.resolution_lease_token = "a" * 48
    finding.resolution_lease_expires_at = datetime.utcnow() - timedelta(minutes=1)
    await db_session.commit()
    post_calls = 0

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9002}
        marker = (
            f"<!-- ccm-finding-resolution:{rebuttal.resolution_nonce};"
            f"head:{finding.head_sha};fingerprint:{finding.fingerprint} -->"
        )
        return [[{
            "id": 9001,
            "body": marker,
            "user": {"login": "ccm-bot"},
        }]]

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    assert await reconcile_rebuttal_resolutions(db_factory) == 1
    assert post_calls == 0
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    terminal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert resolved.thread_status == "resolved"
    assert resolved.resolution_lease_token is None
    assert terminal.status == "resolved"
