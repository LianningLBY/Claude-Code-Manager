from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessAttempt as AttemptModel,
    TestHarnessEvidence as EvidenceModel,
    TestHarnessRun as RunModel,
)
from backend.services.browser_review import BrowserReviewOptions
from backend.services.browser_review_jobs import BrowserReviewJob
from backend.services.test_harness import (
    TestHarnessError as HarnessError,
    TestHarnessIdempotencyError as HarnessIdempotencyError,
    TestHarnessService as HarnessService,
)
from backend.services.test_harness_contracts import (
    TestHarnessSpec as HarnessSpec,
    normalize_findings,
)
from backend.services.test_harness_artifacts import TestHarnessArtifactStore as ArtifactStore


async def _task(db_factory) -> int:
    async with db_factory() as db:
        task = Task(
            title="Harness owner",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="high",
        )
        db.add(task)
        await db.commit()
        return task.id


def _completed_job(tmp_path, *, title: str, severity: str = "high") -> BrowserReviewJob:
    job_id = uuid.uuid4().hex
    output = tmp_path / job_id
    output.mkdir(mode=0o700)
    output.joinpath("initial.png").write_bytes(b"\x89PNG\r\n\x1a\ninitial image")
    output.joinpath("final.png").write_bytes(b"\x89PNG\r\n\x1a\nfinal image")
    output.joinpath("report.md").write_text("# Result\n\nVerdict: pass", encoding="utf-8")
    findings = normalize_findings(
        [
            {
                "scenario_id": "primary-flow",
                "severity": severity,
                "category": "functional",
                "title": title,
                "route": "/settings",
                "locator": "button.save",
                "expected": "Saved state is visible",
                "actual": "No confirmation is visible",
                "reproduction": ["Open settings", "Press Save"],
                "evidence": ["final.png"],
                "confidence": 0.9,
            }
        ]
    )
    now = datetime.utcnow().isoformat()
    return BrowserReviewJob(
        id=job_id,
        options=BrowserReviewOptions(
            url="http://127.0.0.1:5173",
            network_policy="managed_preview",
            goal="Verify settings",
            model="gpt-5.6-sol",
            reasoning_effort="high",
            output_dir=output,
        ),
        capture_only=False,
        provider="codex",
        codex_service_tier="default",
        status="completed",
        stage="completed",
        verdict="passed",
        findings=findings,
        coverage={"scenarios": ["primary-flow"]},
        latest_screenshot="final.png",
        steps=3,
        actions=1,
        created_at=now,
        started_at=now,
        completed_at=now,
    )


@pytest.mark.asyncio
async def test_fixed_url_run_is_idempotent_and_persists_structured_evidence(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    artifact_store = ArtifactStore(tmp_path / "archive", retention_days=1)
    service = HarnessService(
        db_factory=db_factory,
        poll_interval=0.01,
        artifact_store=artifact_store,
    )
    spec = HarnessSpec(
        target_kind="fixed_url",
        target={"url": "http://127.0.0.1:5173"},
        goal="Verify settings",
        idempotency_key="settings-v1",
    )
    run = await service.start_task_run(task_id=task_id, spec=spec)
    same = await service.start_task_run(task_id=task_id, spec=spec)
    assert same.id == run.id

    with pytest.raises(HarnessIdempotencyError):
        await service.start_task_run(
            task_id=task_id,
            spec=HarnessSpec(
                target_kind="fixed_url",
                target={"url": "http://127.0.0.1:5173"},
                goal="Different immutable input",
                idempotency_key="settings-v1",
            ),
        )

    job = _completed_job(tmp_path, title="Save confirmation is missing")
    await service.attach_browser_job(run_id=run.id, job=job, watch_terminal=False)
    payload = await service.get_run(run.id)

    assert payload is not None
    assert payload["status"] == "completed"
    assert payload["verdict"] == "passed"
    assert payload["browser_review"]["coverage"] == {"scenarios": ["primary-flow"]}
    assert payload["findings"][0]["title"] == "Save confirmation is missing"
    assert {item["name"] for item in payload["evidence"]} >= {
        "initial.png",
        "final.png",
        "report.md",
    }
    sequences = [event["sequence"] for event in payload["events"]]
    assert sequences == list(range(1, len(sequences) + 1))
    assert await service.resolve_evidence(run.id, "final.png") is not None

    async with db_factory() as db:
        evidence = await db.scalar(
            select(EvidenceModel).where(
                EvidenceModel.run_id == run.id,
                EvidenceModel.name == "final.png",
            )
        )
        assert evidence is not None
        assert not evidence.storage_path.startswith("/")

    assert job.options.output_dir is not None
    for source_file in job.options.output_dir.iterdir():
        source_file.unlink()
    job.options.output_dir.rmdir()
    restarted = HarnessService(
        db_factory=db_factory,
        artifact_store=artifact_store,
    )
    opened = await restarted.open_evidence(run.id, "final.png")
    assert opened is not None
    try:
        assert b"".join(opened.chunks()).startswith(b"\x89PNG")
    finally:
        opened.close()

    job.findings = []
    await service.sync_browser_job(job)
    cleared = await service.get_run(run.id)
    assert cleared is not None
    assert cleared["findings"] == []

    async with db_factory() as db:
        evidence_rows = list(
            (
                await db.execute(
                    select(EvidenceModel).where(
                        EvidenceModel.run_id == run.id
                    )
                )
            ).scalars()
        )
        for evidence in evidence_rows:
            evidence.created_at = datetime.utcnow() - timedelta(days=2)
        await db.commit()
    assert await restarted.cleanup_evidence() >= 3
    assert await restarted.open_evidence(run.id, "final.png") is None


@pytest.mark.asyncio
async def test_repeat_and_compare_use_stable_finding_fingerprints(db_factory, tmp_path):
    task_id = await _task(db_factory)
    service = HarnessService(
        db_factory=db_factory,
        poll_interval=0.01,
        artifact_store=ArtifactStore(tmp_path / "archive"),
    )
    first = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "http://127.0.0.1:5173"},
            goal="Verify settings",
        ),
    )
    await service.attach_browser_job(
        run_id=first.id,
        job=_completed_job(tmp_path, title="Save confirmation is missing", severity="high"),
        watch_terminal=False,
    )

    repeated = await service.repeat(first.id)
    assert repeated.parent_run_id == first.id
    assert repeated.root_run_id == first.id
    assert repeated.attempt_number == 2
    await service.attach_browser_job(
        run_id=repeated.id,
        job=_completed_job(tmp_path, title="Save confirmation is missing", severity="medium"),
        watch_terminal=False,
    )

    comparison = await service.compare(first.id, repeated.id)
    assert comparison["new"] == []
    assert len(comparison["persisting"]) == 1
    assert comparison["resolved"] == []


@pytest.mark.asyncio
async def test_sync_terminal_workspace_run_records_cleanup_event(db_factory):
    task_id = await _task(db_factory)
    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "http://127.0.0.1:5173"},
            goal="Verify the finished workspace result",
        ),
    )
    now = datetime.utcnow()
    workspace_run = SimpleNamespace(
        id=uuid.uuid4().hex,
        status="completed",
        stage="completed",
        cleanup_status="completed",
        cleanup_error=None,
        browser_review_job_id=None,
        agent_task_id=321,
        git_head="a" * 40,
        workspace_fingerprint="b" * 64,
        stale=False,
        report="# Result\n\nVerdict: pass",
        error=None,
        started_at=now,
        completed_at=now,
    )

    await service._sync_workspace_run(run.id, workspace_run)

    payload = await service.get_run(run.id)
    assert payload is not None
    assert payload["status"] == "completed"
    assert payload["cleanup_status"] == "completed"
    assert payload["events"][-1]["event_type"] == "cleanup"
    assert payload["events"][-1]["title"] == "隔离预览已清理"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_kind", "target"),
    [
        ("pull_request", {"pr_number": 99, "remote": "origin"}),
        ("git_ref", {"ref": "feature", "remote": "origin", "fetch": True}),
    ],
)
async def test_untrusted_git_run_is_rejected_before_persistence(
    db_factory,
    target_kind,
    target,
):
    task_id = await _task(db_factory)
    service = HarnessService(db_factory=db_factory)

    with pytest.raises(HarnessError, match="isolated sandbox"):
        await service.start_task_run(
            task_id=task_id,
            spec=HarnessSpec(
                target_kind=target_kind,
                target=target,
                goal="Do not execute this branch",
            ),
        )

    async with db_factory() as db:
        assert await db.scalar(select(RunModel.id)) is None


@pytest.mark.asyncio
async def test_restart_reconciles_managed_job_files_before_marking_run_interrupted(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(
        db_factory=db_factory,
        artifact_store=store,
        retention_interval=0,
    )
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Recover the last screenshot",
        ),
    )
    job_id = "c" * 32
    attempt_id = "d" * 32
    job_dir = store.create_job_dir(job_id)
    job_dir.joinpath("final.png").write_bytes(b"\x89PNG\r\n\x1a\nrecovered")
    async with db_factory() as db:
        db.add(
            AttemptModel(
                id=attempt_id,
                run_id=run.id,
                ordinal=1,
                status="running",
                stage="browser_ready",
                provider="codex",
                model="gpt-5.6-sol",
                reasoning_effort="medium",
                codex_service_tier="default",
                browser_review_job_id=job_id,
                artifact_root=str(job_dir),
                result_data={},
            )
        )
        await db.commit()

    assert await service.recover_interrupted_runs() == 1
    recovered = await service.get_run(run.id)
    assert recovered is not None
    assert recovered["status"] == "failed"
    assert {item["name"] for item in recovered["evidence"]} == {"final.png"}
    opened = await service.open_evidence(run.id, "final.png")
    assert opened is not None
    try:
        assert b"".join(opened.chunks()).endswith(b"recovered")
    finally:
        opened.close()
    async with db_factory() as db:
        attempt = await db.get(AttemptModel, attempt_id)
        assert attempt is not None
        assert attempt.artifact_root == store.run_prefix(
            task_id=task_id,
            run_id=run.id,
            attempt_id=attempt_id,
        )
