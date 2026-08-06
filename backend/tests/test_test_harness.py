from __future__ import annotations

from datetime import datetime
import uuid

import pytest

from backend.models.task import Task
from backend.services.browser_review import BrowserReviewOptions
from backend.services.browser_review_jobs import BrowserReviewJob
from backend.services.test_harness import (
    TestHarnessIdempotencyError as HarnessIdempotencyError,
    TestHarnessService as HarnessService,
)
from backend.services.test_harness_contracts import (
    TestHarnessSpec as HarnessSpec,
    normalize_findings,
)


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
    output.joinpath("initial.png").write_bytes(b"initial image")
    output.joinpath("final.png").write_bytes(b"final image")
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
    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
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


@pytest.mark.asyncio
async def test_repeat_and_compare_use_stable_finding_fingerprints(db_factory, tmp_path):
    task_id = await _task(db_factory)
    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
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
