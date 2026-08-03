from datetime import datetime
import hashlib
import json
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.global_settings import GlobalSettings
from backend.models.plan import Plan, PlanApplication, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_agent_runner import PlanAgentRunner
from backend.services.plan_service import (
    apply_worker_plan_outcome,
    materialize_execution_task,
)


async def _target(client, session_factory) -> Task:
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Versioned Plan target",
            "description": "Initial task request",
            "target_repo": "/tmp",
        },
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.session_id = "session-plan-v2"
        task.status = "completed"
        db.add(LogEntry(
            instance_id=1,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Existing context",
        ))
        await db.commit()
        await db.refresh(task)
        db.expunge(task)
        return task


async def _finish_current_run_with_version(
    session_factory,
    *,
    plan_id: int,
    content: str = "# Ready Plan",
) -> int:
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, plan.active_run_id)
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=run.id,
            content=content,
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            repo_revision=run.repo_revision,
            reviewer_repo_revision=run.repo_revision,
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = None
        run.status = "completed"
        run.current_stage = "complete"
        run.result_version_id = version.id
        run.finished_at = datetime.utcnow()
        await db.commit()
        return version.id


@pytest.mark.asyncio
async def test_public_plan_routes_are_global_only_and_frozen(
    client, session_factory
):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    pipeline["max_interactions"] = 5
    pipeline["planner"]["primary"] = {
        "provider": "codex",
        "model": "gpt-5.6-terra",
        "effort": "ultra",
    }
    async with session_factory() as db:
        db.add(GlobalSettings(id=1, plan_pipeline_config=pipeline))
        await db.commit()

    overridden = await client.post(
        "/api/plans",
        json={
            "input": "Attempt a per-Plan route",
            "target_repo": "/tmp",
            "pipeline_config": default_plan_pipeline_config().model_dump(mode="json"),
        },
    )
    assert overridden.status_code == 422
    assert "extra_forbidden" in overridden.text

    created = await client.post(
        "/api/plans",
        json={"input": "Use the global route", "target_repo": "/tmp"},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    assert payload["pipeline_config"] == pipeline
    assert payload["active_run"]["max_interactions"] == 5

    async with session_factory() as db:
        settings_row = await db.get(GlobalSettings, 1)
        changed = default_plan_pipeline_config().model_dump(mode="json")
        changed["max_interactions"] = 0
        settings_row.plan_pipeline_config = changed
        await db.commit()

    frozen = await client.get(f"/api/plans/{payload['id']}")
    assert frozen.json()["pipeline_config"] == pipeline


@pytest.mark.asyncio
async def test_plan_and_run_requests_reject_blank_text(client, session_factory):
    blank_plan = await client.post("/api/plans", json={"input": "   \n  "})
    assert blank_plan.status_code == 422

    created = await client.post("/api/plans", json={"input": "Valid request"})
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, plan.active_run_id)
        run.status = "failed"
        run.current_stage = "failed"
        run.finished_at = datetime.utcnow()
        plan.active_run_id = None
        await db.commit()

    blank_run = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={"run_type": "user_revision", "request": "   "},
    )
    assert blank_run.status_code == 422


@pytest.mark.asyncio
async def test_plan_catalog_search_and_archived_only_match_list_and_count(client):
    archived = await client.post(
        "/api/plans",
        json={"input": "Needle migration details", "title": "Archived artifact"},
    )
    active = await client.post(
        "/api/plans",
        json={"input": "Unrelated active request", "title": "Active Plan"},
    )
    assert archived.status_code == 201, archived.text
    assert active.status_code == 201, active.text
    archived_payload = archived.json()

    cancelled = await client.post(
        f"/api/plan-runs/{archived_payload['active_run']['id']}/cancel"
    )
    assert cancelled.status_code == 200, cancelled.text
    current = await client.get(f"/api/plans/{archived_payload['id']}")
    assert current.status_code == 200, current.text
    archived_result = await client.patch(
        f"/api/plans/{archived_payload['id']}",
        json={
            "archived": True,
            "expected_lock_version": current.json()["lock_version"],
        },
    )
    assert archived_result.status_code == 200, archived_result.text

    default_rows = await client.get(
        "/api/plans", params={"q": "needle migration"}
    )
    assert default_rows.status_code == 200
    assert default_rows.json() == []

    rows = await client.get(
        "/api/plans", params={"archived_only": True, "q": "needle migration"}
    )
    count = await client.get(
        "/api/plans/count",
        params={"archived_only": True, "q": "needle migration"},
    )
    assert [item["id"] for item in rows.json()] == [archived_payload["id"]]
    assert count.json() == {"total": 1}

    running_rows = await client.get(
        "/api/plans", params={"display_state": "planner,reviewer"}
    )
    running_count = await client.get(
        "/api/plans/count", params={"display_state": "planner,reviewer"}
    )
    assert [item["id"] for item in running_rows.json()] == [active.json()["id"]]
    assert running_count.json() == {"total": 1}


@pytest.mark.asyncio
async def test_retry_requires_exact_terminal_failed_source(client, session_factory):
    created = await client.post(
        "/api/plans",
        json={"input": "Retry safely", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    source_run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        source = await db.get(PlanAgentRun, source_run_id)
        source.status = "failed"
        source.current_stage = "failed"
        source.error = "transient worker failure"
        source.finished_at = datetime.utcnow()
        plan.active_run_id = None
        await db.commit()

    missing = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={"run_type": "retry", "request": "Retry"},
    )
    assert missing.status_code == 422
    wrong_type = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "refresh_context",
            "request": "Refresh",
            "source_run_id": source_run_id,
        },
    )
    assert wrong_type.status_code == 422
    retry = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "retry",
            "request": "Retry",
            "source_run_id": source_run_id,
        },
    )
    assert retry.status_code == 201, retry.text
    assert retry.json()["source_run_id"] == source_run_id


@pytest.mark.asyncio
async def test_plan_input_rejects_high_confidence_credentials(
    client, session_factory
):
    rejected_create = await client.post(
        "/api/plans",
        json={
            "input": (
                "Use ghp_abcdefghijklmnopqrstuvwxyz1234567890ABCD directly"
            )
        },
    )
    assert rejected_create.status_code == 422
    assert "Settings" in rejected_create.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0

    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Need a safe reference", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        run.status = "waiting_user"
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=run.generation,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run.id,
            source_step_id=step.id,
            requested_by="planner",
            questions=[{
                "id": "credential_reference",
                "header": "Credential",
                "question": "Name the configured credential reference",
                "response_type": "text",
                "options": [],
                "required": True,
            }],
            status="open",
            idempotency_key=f"secret-guard:{run.id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()
        request_id = input_request.id
        generation = run.generation

    rejected = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json={
            "expected_run_generation": generation,
            "idempotency_key": "credential-answer",
            "answers": [{
                "question_id": "credential_reference",
                "value": "ghp_abcdefghijklmnopqrstuvwxyz1234567890ABCD",
            }],
        },
    )
    assert rejected.status_code == 422
    assert "Settings" in rejected.text
    async with session_factory() as db:
        request_row = await db.get(PlanInputRequest, request_id)
        run = await db.get(PlanAgentRun, run_id)
        assert request_row.status == "open"
        assert request_row.answers is None
        assert run.status == "waiting_user"

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        plan = await db.get(Plan, plan_id)
        run.status = "failed"
        run.current_stage = "failed"
        run.finished_at = datetime.utcnow()
        plan.active_run_id = None
        await db.commit()

    rejected_revision = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": (
                "Use ghp_abcdefghijklmnopqrstuvwxyz1234567890ABCD directly"
            ),
        },
    )
    assert rejected_revision.status_code == 422
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(PlanAgentRun.id)).where(
                PlanAgentRun.plan_id == plan_id
            )
        ) == 1


@pytest.mark.asyncio
async def test_stale_confirmation_and_missing_target_hard_conflict(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Approve exact context", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    async with session_factory() as db:
        db.add(LogEntry(
            instance_id=1,
            task_id=target.id,
            event_type="user_message",
            role="user",
            content="Context changed after planning",
        ))
        await db.commit()

    stale = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={"expected_current_version_id": version_id},
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["can_confirm"] is True
    approved = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={
            "expected_current_version_id": version_id,
            "confirm_stale": True,
        },
    )
    assert approved.status_code == 200, approved.text

    second = await client.post(
        "/api/plans",
        json={"input": "Do not approve a missing target", "target_task_id": target.id},
    )
    second_plan_id = second.json()["id"]
    second_version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=second_plan_id,
    )
    async with session_factory() as db:
        target_row = await db.get(Task, target.id)
        await db.delete(target_row)
        await db.commit()
    hard = await client.post(
        f"/api/plan-versions/{second_version_id}/approve",
        json={
            "expected_current_version_id": second_version_id,
            "confirm_stale": True,
        },
    )
    assert hard.status_code == 409
    assert hard.json()["detail"]["hard_conflict"] is True
    assert "target_task_missing" in hard.json()["detail"]["hard_conflicts"]


@pytest.mark.asyncio
async def test_missing_legacy_repository_snapshot_is_confirmable_not_blocking(
    client,
    session_factory,
):
    async def create_legacy_version(title: str) -> tuple[int, int]:
        created = await client.post(
            "/api/plans",
            json={"input": title, "target_repo": "/tmp"},
        )
        assert created.status_code == 201, created.text
        plan_id = created.json()["id"]
        version_id = await _finish_current_run_with_version(
            session_factory,
            plan_id=plan_id,
        )
        async with session_factory() as db:
            version = await db.get(PlanVersion, version_id)
            version.repo_revision = None
            version.reviewer_repo_revision = None
            await db.commit()
        return plan_id, version_id

    with patch(
        "backend.services.plan_staleness.capture_repo_revision",
        new=AsyncMock(return_value={
            "available": True,
            "head": "current-head",
            "dirty_sha256": "clean",
        }),
    ):
        _reject_plan_id, reject_version_id = await create_legacy_version(
            "Reject migrated Version",
        )
        stale = await client.get(
            f"/api/plan-versions/{reject_version_id}/staleness"
        )
        assert stale.status_code == 200, stale.text
        assert stale.json()["stale"] is True
        assert stale.json()["hard_conflict"] is False
        assert stale.json()["can_confirm"] is True
        assert stale.json()["reasons"] == [
            "captured_repository_state_missing"
        ]

        rejected = await client.post(
            f"/api/plan-versions/{reject_version_id}/reject",
            json={"expected_current_version_id": reject_version_id},
        )
        assert rejected.status_code == 200, rejected.text
        assert rejected.json()["human_decision"] == "rejected"

        _approve_plan_id, approve_version_id = await create_legacy_version(
            "Approve migrated Version",
        )
        unconfirmed = await client.post(
            f"/api/plan-versions/{approve_version_id}/approve",
            json={"expected_current_version_id": approve_version_id},
        )
        assert unconfirmed.status_code == 409
        assert unconfirmed.json()["detail"]["can_confirm"] is True

        approved = await client.post(
            f"/api/plan-versions/{approve_version_id}/approve",
            json={
                "expected_current_version_id": approve_version_id,
                "confirm_stale": True,
            },
        )
        assert approved.status_code == 200, approved.text
        assert approved.json()["human_decision"] == "approved"

        _execution_plan_id, execution_version_id = await create_legacy_version(
            "Execute migrated Version",
        )
        blocked_execution = await client.post(
            f"/api/plan-versions/{execution_version_id}/create-execution-task",
            json={
                "expected_current_version_id": execution_version_id,
                "approve_if_pending": True,
            },
        )
        assert blocked_execution.status_code == 409
        assert blocked_execution.json()["detail"]["can_confirm"] is True

        confirmed_execution = await client.post(
            f"/api/plan-versions/{execution_version_id}/create-execution-task",
            json={
                "expected_current_version_id": execution_version_id,
                "approve_if_pending": True,
                "confirm_stale": True,
            },
        )
        assert confirmed_execution.status_code == 201, confirmed_execution.text


@pytest.mark.asyncio
async def test_approve_and_create_execution_is_atomic_and_history_stays_linked(
    client, session_factory
):
    created = await client.post(
        "/api/plans",
        json={"input": "Create an execution task", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    executed = await client.post(
        f"/api/plan-versions/{version_id}/create-execution-task",
        json={
            "expected_current_version_id": version_id,
            "approve_if_pending": True,
        },
    )
    assert executed.status_code == 201, executed.text
    execution_task_id = executed.json()["execution_task_id"]
    assert executed.json()["version"]["human_decision"] == "approved"

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version2 = PlanVersion(
            plan_id=plan.id,
            version_number=2,
            parent_version_id=version_id,
            content="# New current version",
            repo_revision={"available": False, "reason": "not_git"},
            review_verdict="approve",
        )
        db.add(version2)
        await db.flush()
        plan.current_version_id = version2.id
        await db.commit()

    resource = await client.get(f"/api/plans/{plan_id}")
    assert resource.status_code == 200, resource.text
    payload = resource.json()
    assert payload["application"] is None
    assert payload["applications"][0]["plan_version_id"] == version_id
    assert payload["applications"][0]["execution_task_id"] == execution_task_id
    assert payload["applications"][0]["execution_task_available"] is True

    versions = await client.get(f"/api/plans/{plan_id}/versions")
    assert versions.status_code == 200, versions.text
    version_states = {
        item["version_number"]: item["display_state"]
        for item in versions.json()
    }
    assert version_states == {1: "applied", 2: "awaiting_review"}

    async with session_factory() as db:
        execution_task = await db.get(Task, execution_task_id)
        await db.delete(execution_task)
        await db.commit()

    missing_target = await client.get(f"/api/plans/{plan_id}")
    assert missing_target.status_code == 200, missing_target.text
    missing_application = missing_target.json()["applications"][0]
    assert missing_application["execution_task_available"] is False


@pytest.mark.asyncio
async def test_undecided_superseded_version_has_derived_historical_state(
    client,
    session_factory,
):
    created = await client.post(
        "/api/plans",
        json={"input": "Revise an undecided Version", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version1_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
        content="# Undecided v1",
    )
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version1 = await db.get(PlanVersion, version1_id)
        version2 = PlanVersion(
            plan_id=plan.id,
            version_number=2,
            parent_version_id=version1.id,
            content="# Current v2",
            review_verdict="approve",
        )
        db.add(version2)
        await db.flush()
        version1.superseded_by_version_id = version2.id
        plan.current_version_id = version2.id
        await db.commit()

    versions = await client.get(f"/api/plans/{plan_id}/versions")
    assert versions.status_code == 200, versions.text
    by_number = {item["version_number"]: item for item in versions.json()}
    assert by_number[1]["human_decision"] == "pending"
    assert by_number[1]["display_state"] == "superseded"
    assert by_number[2]["display_state"] == "awaiting_review"


@pytest.mark.asyncio
async def test_execution_task_materializer_is_directly_callable_and_idempotent(
    client, session_factory
):
    created = await client.post(
        "/api/plans",
        json={"input": "Expose a stable execution seam", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )

    async with session_factory() as db:
        first = await materialize_execution_task(
            db,
            plan_id=plan_id,
            version_id=version_id,
            expected_current_version_id=version_id,
            confirm_stale=False,
            approve_if_pending=True,
            actor_id=42,
            execution_metadata={
                "auto_run_id": "auto-7",
                "created_from_plan_id": -1,
            },
        )
        replay = await materialize_execution_task(
            db,
            plan_id=plan_id,
            version_id=version_id,
            expected_current_version_id=version_id,
            confirm_stale=False,
            approve_if_pending=False,
            actor_id=42,
            execution_metadata={"auto_run_id": "ignored-on-replay"},
        )

        assert first.created is True
        assert replay.created is False
        assert replay.task.id == first.task.id
        assert replay.application.id == first.application.id
        assert first.task.metadata_["auto_run_id"] == "auto-7"
        assert first.task.metadata_["created_from_plan_id"] == plan_id
        assert first.task.metadata_["created_from_plan_version_id"] == version_id
        assert await db.scalar(
            select(func.count(PlanApplication.id)).where(
                PlanApplication.plan_version_id == version_id
            )
        ) == 1


@pytest.mark.asyncio
async def test_worker_import_creates_idempotent_inert_mirror(client, session_factory):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 1,
        "plan_id": 5101,
        "run_id": 5201,
        "run_generation": 4,
        "title": "Relayed Plan",
        "initial_request": "Design on the Worker",
        "priority": 2,
        "pipeline_config": pipeline,
        "run_type": "initial",
        "request_text": "Design on the Worker",
        "max_interactions": 3,
    }
    created = await client.post("/api/plans/worker-import", json=body)
    assert created.status_code == 200, created.text
    assert created.json()["run"]["status"] == "queued"

    replay = await client.post("/api/plans/worker-import", json=body)
    assert replay.status_code == 200, replay.text
    async with session_factory() as db:
        plan = await db.get(Plan, 5101)
        run = await db.get(PlanAgentRun, 5201)
        assert plan.relay_origin == "manager_v1"
        assert plan.worker_id is None
        assert plan.active_run_id == run.id
        assert run.relay_origin == "manager_v1"
        assert run.generation == 4
        assert await db.scalar(select(func.count(Plan.id))) == 1
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 1


@pytest.mark.asyncio
async def test_worker_import_requires_exact_attachment_digest(client):
    uploaded = await client.post(
        "/api/uploads",
        files={"files": ("requirements.txt", b"exact bytes", "text/plain")},
    )
    assert uploaded.status_code == 200, uploaded.text
    item = uploaded.json()[0]
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 1,
        "plan_id": 5151,
        "run_id": 5251,
        "run_generation": 0,
        "title": "Attachment Plan",
        "initial_request": "Use the attachment",
        "priority": 0,
        "pipeline_config": pipeline,
        "run_type": "initial",
        "request_text": "Use the attachment",
        "max_interactions": 3,
        "file_paths": [item["path"]],
        "image_paths": [],
        "attachments": [{
            "url": item["url"],
            "name": item["filename"],
            "is_image": False,
        }],
        "attachment_manifest": [{
            "path": item["path"],
            "size": len(b"exact bytes"),
            "sha256": "0" * 64,
        }],
    }
    rejected = await client.post("/api/plans/worker-import", json=body)
    assert rejected.status_code == 409
    assert "digest/size" in rejected.text

    body["attachment_manifest"][0]["sha256"] = hashlib.sha256(
        b"exact bytes"
    ).hexdigest()
    accepted = await client.post("/api/plans/worker-import", json=body)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["attachment_receipt"] == body["attachment_manifest"]


@pytest.mark.asyncio
async def test_worker_materializes_exact_version_idempotently(client, session_factory):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 1,
        "plan_id": 5301,
        "title": "Migrated Plan",
        "initial_request": "Plan before migration",
        "priority": 0,
        "pipeline_config": pipeline,
        "version": {
            "source_version_id": 5401,
            "version_number": 3,
            "content": "# Immutable v3",
            "context_session_id": "session-before-migration",
            "context_log_id": 88,
            "context_snapshot": "private relay context",
            "review_verdict": "approve",
            "review_exhausted": False,
            "human_decision": "approved",
        },
    }
    created = await client.post(
        "/api/plans/worker-materialize-version",
        json=body,
    )
    assert created.status_code == 200, created.text
    remote_version_id = created.json()["id"]
    replay = await client.post(
        "/api/plans/worker-materialize-version",
        json=body,
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == remote_version_id

    async with session_factory() as db:
        plan = await db.get(Plan, 5301)
        version = await db.get(PlanVersion, remote_version_id)
        assert plan.current_version_id == version.id
        assert version.version_number == 3
        assert version.content == "# Immutable v3"
        assert version.human_decision == "approved"
        assert await db.scalar(
            select(func.count(PlanVersion.id)).where(PlanVersion.plan_id == plan.id)
        ) == 1


@pytest.mark.asyncio
async def test_worker_outcome_maps_exact_audit_and_preserves_manager_context(
    session_factory,
):
    now = datetime.utcnow()
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Manager authority",
            initial_request="Plan this",
            worker_id=7,
            pipeline_config=pipeline,
            priority=0,
        )
        db.add(plan)
        await db.flush()
        base = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Manager base",
            context_session_id="manager-session",
            context_log_id=70,
            human_decision="approved",
        )
        db.add(base)
        await db.flush()
        plan.current_version_id = base.id
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=7,
            run_type="initial",
            base_version_id=base.id,
            request_text="Plan this",
            context_session_id="manager-session",
            context_log_id=91,
            context_snapshot="manager-only context",
            pipeline_config=pipeline,
            status="running",
            current_stage="planner",
            generation=2,
            max_interactions=3,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        plan_id = plan.id
        run_id = run.id
        base_version_id = base.id

    payload = {
        "protocol": 1,
        "base_worker_version_id": 800,
        "run": {
            "id": run_id,
            "plan_id": plan_id,
            "run_type": "initial",
            "status": "waiting_user",
            "current_stage": "reviewer",
            "base_version_id": None,
            "result_version_id": 801,
            "request_text": "Plan this",
            "round": 1,
            "generation": 3,
            "instance_id": None,
            "worker_id": None,
            "open_input_request_id": 901,
            "interaction_count": 1,
            "max_interactions": 3,
            "execution_seconds": 12.5,
            "last_execution_started_at": None,
            "review_verdict": None,
            "review_feedback": None,
            "review_exhausted": False,
            "error": None,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "finished_at": None,
            "steps": [
                {
                    "id": 701,
                    "run_id": run_id,
                    "plan_id": plan_id,
                    "plan_version_id": 801,
                    "input_request_id": None,
                    "step_type": "planner",
                    "round": 1,
                    "generation": 3,
                    "provider": "codex",
                    "model": "gpt-test",
                    "effort": "high",
                    "route_slot": "primary",
                    "status": "completed",
                    "output": "planner output",
                    "error": None,
                    "started_at": now.isoformat(),
                    "finished_at": now.isoformat(),
                },
                {
                    "id": 702,
                    "run_id": run_id,
                    "plan_id": plan_id,
                    "plan_version_id": None,
                    "input_request_id": 901,
                    "step_type": "reviewer",
                    "round": 1,
                    "generation": 3,
                    "provider": "claude",
                    "model": "claude-test",
                    "effort": "medium",
                    "route_slot": "fallback",
                    "status": "completed",
                    "output": "need input",
                    "error": None,
                    "started_at": now.isoformat(),
                    "finished_at": now.isoformat(),
                },
            ],
            "input_requests": [
                {
                    "id": 901,
                    "plan_id": plan_id,
                    "run_id": run_id,
                    "source_step_id": 702,
                    "requested_by": "reviewer",
                    "reason": "Need deployment target",
                    "questions": [
                        {
                            "id": "target",
                            "header": "Target",
                            "question": "Where should this run?",
                            "response_type": "text",
                            "options": [],
                            "required": True,
                        }
                    ],
                    "status": "open",
                    "answers": None,
                    "response_text": None,
                    "attachments": None,
                    "answered_by": None,
                    "opened_at": now.isoformat(),
                    "answered_at": None,
                    "created_at": now.isoformat(),
                }
            ],
        },
        "versions": [
            {
                "id": 801,
                "plan_id": plan_id,
                "version_number": 2,
                "parent_version_id": 800,
                "produced_by_run_id": run_id,
                "produced_by_step_id": 701,
                "content": "# Worker version",
                "context_session_id": "worker-session",
                "context_log_id": 1234,
                "repo_revision": {"commit": "abc"},
                "review_verdict": None,
                "review_feedback": None,
                "reviewed_by_step_id": 702,
                "review_exhausted": False,
                "reviewed_at": None,
                "human_decision": "pending",
                "decided_at": None,
                "decided_by": None,
                "superseded_by_version_id": None,
                "applied": False,
                "created_at": now.isoformat(),
            }
        ],
    }
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        await apply_worker_plan_outcome(
            db,
            plan=plan,
            run=run,
            worker_id=7,
            expected_generation=2,
            payload=payload,
        )

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        version = await db.get(PlanVersion, plan.current_version_id)
        input_request = await db.get(PlanInputRequest, run.open_input_request_id)
        assert run.status == "waiting_user"
        assert run.generation == 3
        assert version.worker_id == 7
        assert version.worker_version_id == 801
        assert version.parent_version_id == base_version_id
        assert version.context_session_id == "manager-session"
        assert version.context_log_id == 91
        assert version.context_snapshot == "manager-only context"
        assert input_request.worker_input_request_id == 901
        assert input_request.status == "open"
        base = await db.get(PlanVersion, base_version_id)
        assert base.superseded_by_version_id == version.id


@pytest.mark.asyncio
async def test_canonical_create_and_revision_keep_stable_plan_identity(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Design the change", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    plan_id = payload["id"]
    first_run_id = payload["active_run"]["id"]
    assert payload["target_task_id"] == target.id
    assert payload["display_state"] == "planner"

    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(Task.id)).where(Task.mode == "plan")
        ) == 0
        plan = await db.get(Plan, plan_id)
        first_run = await db.get(PlanAgentRun, first_run_id)
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=first_run.id,
            content="# v1",
            context_session_id=first_run.context_session_id,
            context_log_id=first_run.context_log_id,
            repo_revision=first_run.repo_revision,
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = None
        first_run.status = "completed"
        first_run.current_stage = "complete"
        first_run.result_version_id = version.id
        first_run.finished_at = datetime.utcnow()
        await db.commit()
        version_id = version.id

    revised = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": "Add rollback details",
            "base_version_id": version_id,
            "expected_current_version_id": version_id,
        },
    )
    assert revised.status_code == 201, revised.text
    revised_payload = revised.json()
    assert revised_payload["plan_id"] == plan_id
    assert revised_payload["id"] != first_run_id
    assert revised_payload["base_version_id"] == version_id

    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 1
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 2
        assert await db.scalar(
            select(func.count(Task.id)).where(Task.mode == "plan")
        ) == 0


@pytest.mark.asyncio
async def test_related_plan_creation_rejects_migrating_target(
    client, session_factory
):
    target = await _target(client, session_factory)
    async with session_factory() as db:
        current = await db.get(Task, target.id)
        current.status = "migrating"
        await db.commit()

    response = await client.post(
        "/api/plans",
        json={"input": "Do not race migration", "target_task_id": target.id},
    )

    assert response.status_code == 409
    assert "changing execution location" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0


@pytest.mark.asyncio
async def test_input_request_accepts_many_questions_and_resumes_same_run(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Need user choices", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]

    questions = [
        {
            "id": f"question_{index}",
            "header": f"Q{index}",
            "question": f"Provide required value {index}",
            "response_type": "text",
            "options": [],
            "required": True,
        }
        for index in range(8)
    ]
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        run.status = "waiting_user"
        run.current_stage = "planner"
        run.generation = 7
        run.interaction_count = 1
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=7,
            provider="claude",
            model="test",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run.id,
            source_step_id=step.id,
            requested_by="planner",
            reason="All eight values are necessary",
            questions=questions,
            status="open",
            idempotency_key=f"run:{run.id}:step:{step.id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()
        request_id = input_request.id

    body = {
        "expected_run_generation": 7,
        "idempotency_key": "answer-many-questions",
        "answers": [
            {"question_id": item["id"], "value": f"answer-{index}"}
            for index, item in enumerate(questions)
        ],
    }
    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json=body,
    )
    assert answered.status_code == 200, answered.text
    assert len(answered.json()["answers"]) == 8

    replay = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json=body,
    )
    assert replay.status_code == 200, replay.text
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        assert run.plan_id == plan_id
        assert run.status == "queued"
        assert run.generation == 8
        assert run.open_input_request_id is None
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 1


@pytest.mark.asyncio
async def test_exact_approved_version_is_applied_to_real_user_message(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Plan exact application", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=run.id,
            content="# Exact immutable content",
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            repo_revision=run.repo_revision,
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = None
        run.status = "completed"
        run.current_stage = "complete"
        run.result_version_id = version.id
        run.finished_at = datetime.utcnow()
        await db.commit()
        version_id = version.id

    approved = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={"expected_current_version_id": version_id, "confirm_stale": False},
    )
    assert approved.status_code == 200, approved.text

    with patch("backend.main.dispatcher.enqueue_message", new=AsyncMock()):
        sent = await client.post(
            f"/api/tasks/{target.id}/chat",
            json={"message": "Implement it", "plan_version_ids": [version_id]},
        )
    assert sent.status_code == 200, sent.text
    assert sent.json()["applied_plan_version_ids"] == [version_id]

    async with session_factory() as db:
        application = (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
        ).scalar_one()
        log = await db.get(LogEntry, application.user_log_id)
        snapshot = json.loads(log.raw_json)["applied_plans"][0]
        assert snapshot["plan_id"] == plan_id
        assert snapshot["version_id"] == version_id
        assert snapshot["version_number"] == 1
        assert snapshot["content"] == "# Exact immutable content"

    duplicate = await client.post(
        f"/api/tasks/{target.id}/chat",
        json={"message": "Again", "plan_version_ids": [version_id]},
    )
    assert duplicate.status_code == 400


@pytest.mark.asyncio
async def test_instance_capacity_owner_is_task_xor_plan_run(db_session):
    instance = Instance(name="slot", status="running", current_plan_run_id=4)
    db_session.add(instance)
    await db_session.commit()
    assert instance.current_task_id is None
    assert instance.current_plan_run_id == 4
    db_session.add(Instance(
        name="invalid-slot",
        status="running",
        current_task_id=3,
        current_plan_run_id=4,
    ))
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_plan_application_target_shape_is_database_enforced(db_session):
    db_session.add(PlanApplication(
        plan_id=1,
        plan_version_id=1,
        application_type="chat_message",
        execution_task_id=99,
    ))
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_plan_resources_never_expose_internal_attachment_paths(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Inspect attached requirements", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    internal = {
        "url": "/api/uploads/example.txt",
        "name": "example.txt",
        "is_image": False,
        "path": "/private/uploads/example.txt",
    }
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        plan.initial_attachments = [internal]
        run.status = "waiting_user"
        step = PlanAgentStep(
            run_id=run_id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=run.generation,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run_id,
            source_step_id=step.id,
            requested_by="planner",
            reason="Need confirmation",
            questions=[{
                "id": "confirm",
                "header": "Confirm",
                "question": "Confirm the requirement",
                "response_type": "text",
                "options": [],
                "required": True,
            }],
            status="open",
            attachments=[internal],
            idempotency_key=f"test-path:{run_id}",
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()

    resource = await client.get(f"/api/plans/{plan_id}")
    assert resource.status_code == 200, resource.text
    payload = resource.json()
    assert payload["initial_attachments"] == [{
        "url": "/api/uploads/example.txt",
        "name": "example.txt",
        "is_image": False,
    }]
    assert "path" not in payload["open_input_request"]["attachments"][0]
    run_resource = await client.get(f"/api/plan-runs/{run_id}")
    assert "path" not in run_resource.json()["input_requests"][0]["attachments"][0]


@pytest.mark.asyncio
async def test_interaction_round_limit_fails_without_limiting_question_count(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Need one more round", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        owner = Instance(name="limited-plan-slot", status="running")
        db.add(owner)
        await db.flush()
        run = await db.get(PlanAgentRun, run_id)
        run.status = "running"
        run.generation = 4
        run.instance_id = owner.id
        run.interaction_count = 3
        run.max_interactions = 3
        run.last_execution_started_at = datetime.utcnow()
        owner.current_plan_run_id = run_id
        step = PlanAgentStep(
            run_id=run_id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=4,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.commit()
        await db.refresh(step)
        step_id = step.id
        owner_id = owner.id

    runner = PlanAgentRunner(
        db_factory=session_factory,
        instance_manager=AsyncMock(),
    )
    outcome = await runner._open_input_request(
        run_id=run_id,
        generation=4,
        source_step=PlanAgentStep(id=step_id),
        requested_by="planner",
        reason="One more interaction is necessary",
        questions=[{
            "id": f"q{index}",
            "header": f"Q{index}",
            "question": f"Decision {index}",
            "response_type": "text",
            "options": [],
            "required": True,
        } for index in range(20)],
        max_interactions=3,
    )
    assert outcome == "failed"
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        owner = await db.get(Instance, owner_id)
        assert run.status == "failed"
        assert "3 user-interaction round limit" in run.error
        assert plan.active_run_id is None
        assert owner.status == "idle"
        assert owner.current_plan_run_id is None
        assert await db.scalar(select(func.count(PlanInputRequest.id)).where(
            PlanInputRequest.run_id == run_id
        )) == 0


@pytest.mark.asyncio
async def test_versioned_run_pauses_twice_and_resumes_same_pipeline(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Design an interactive rollout", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    instance = Instance(name="plan-slot", status="idle")
    async with session_factory() as db:
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    planner_questions = [
        {
            "id": f"decision_{index}",
            "header": f"Q{index}",
            "question": f"Required decision {index}",
            "response_type": "text",
            "options": [],
            "required": True,
        }
        for index in range(8)
    ]
    outputs = [
        {
            "action": "request_input",
            "reason": "These decisions affect the architecture",
            "questions": planner_questions,
        },
        {"action": "propose", "plan": "# Version 1\nInitial decisions included."},
        {
            "action": "request_input",
            "reason": "Reviewer found one unresolved deployment constraint",
            "questions": [{
                "id": "maintenance_window",
                "header": "Rollout",
                "question": "Which maintenance window should the Plan use?",
                "response_type": "text",
                "options": [],
                "required": True,
            }],
        },
        {
            "action": "propose",
            "plan": "# Version 2\nIncludes every decision and the Sunday window.",
        },
        {"action": "approve", "feedback": "Self-contained and testable"},
    ]
    prompts: list[str] = []

    async def fake_stage(**kwargs):
        prompts.append(kwargs["prompt"])
        output = outputs.pop(0)
        async with session_factory() as db:
            db.add(PlanAgentStep(
                run_id=kwargs["run_id"],
                plan_id=kwargs["plan_id"],
                step_type=kwargs["step_type"],
                round=kwargs["round_number"],
                generation=kwargs["generation"],
                provider="claude",
                model="test-model",
                route_slot="primary",
                status="completed",
                output=json.dumps(output),
                finished_at=datetime.utcnow(),
            ))
            await db.commit()
        return output, json.dumps(output), object(), "primary", "test-account"

    async def claim_current_run():
        async with session_factory() as db:
            run = await db.get(PlanAgentRun, run_id)
            owner = await db.get(Instance, instance_id)
            assert run.status == "queued"
            assert owner.status == "idle"
            run.status = "running"
            run.generation += 1
            run.instance_id = instance_id
            run.last_execution_started_at = datetime.utcnow()
            owner.status = "running"
            owner.current_plan_run_id = run_id
            await db.commit()

    runner = PlanAgentRunner(
        db_factory=session_factory,
        instance_manager=AsyncMock(),
    )
    runner._run_stage = fake_stage

    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "waiting_user"
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        owner = await db.get(Instance, instance_id)
        first_request = await db.get(PlanInputRequest, run.open_input_request_id)
        assert run.status == "waiting_user"
        assert run.instance_id is None
        assert len(first_request.questions) == 8
        assert owner.status == "idle"
        assert owner.current_plan_run_id is None
        first_generation = run.generation

    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{first_request.id}/answer",
        json={
            "expected_run_generation": first_generation,
            "idempotency_key": "first-answer",
            "answers": [
                {"question_id": question["id"], "value": f"value-{index}"}
                for index, question in enumerate(planner_questions)
            ],
        },
    )
    assert answered.status_code == 200, answered.text

    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "queued"
    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "waiting_user"
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        second_request = await db.get(PlanInputRequest, run.open_input_request_id)
        second_generation = run.generation
        assert second_request.requested_by == "reviewer"

    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{second_request.id}/answer",
        json={
            "expected_run_generation": second_generation,
            "idempotency_key": "reviewer-answer",
            "answers": [{
                "question_id": "maintenance_window",
                "value": "Sunday 02:00 UTC",
            }],
        },
    )
    assert answered.status_code == 200, answered.text

    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "queued"
    assert "Sunday 02:00 UTC" in prompts[-1]
    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "completed"

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        versions = list((await db.execute(
            select(PlanVersion)
            .where(PlanVersion.plan_id == plan_id)
            .order_by(PlanVersion.version_number)
        )).scalars())
        requests = list((await db.execute(
            select(PlanInputRequest)
            .where(PlanInputRequest.run_id == run_id)
            .order_by(PlanInputRequest.id)
        )).scalars())
        assert plan.active_run_id is None
        assert plan.current_version_id == versions[1].id
        assert run.status == "completed"
        assert run.interaction_count == 2
        assert [item.status for item in requests] == ["answered", "answered"]
        assert [item.version_number for item in versions] == [1, 2]
        assert versions[0].superseded_by_version_id == versions[1].id
        assert versions[1].review_verdict == "approve"
        assert versions[1].human_decision == "pending"
