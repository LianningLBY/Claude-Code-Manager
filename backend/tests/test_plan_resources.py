from datetime import datetime
import json
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan import Plan, PlanApplication, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_agent_runner import PlanAgentRunner
from backend.services.plan_service import apply_worker_plan_outcome


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
