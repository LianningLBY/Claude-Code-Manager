"""Plan Capability adapter transaction and lifecycle tests."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.instance import Instance
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import default_plan_pipeline_config
from backend.services import plan_capability as adapter_module
from backend.services import capability_service
from backend.services.capability_registry import (
    register_capability,
    resolve_capability,
    unregister_capability,
)
from backend.services.plan_capability import (
    PlanCapabilityCancellationUnconfirmed,
    PlanCapabilityExecutor,
    plan_capability_definition,
)
from backend.services.plan_service import answer_input_request, plan_operation_lock


@pytest.fixture(autouse=True)
def plan_capability_runtime(monkeypatch):
    previous_flag = settings.capability_core_enabled
    previous_definition = resolve_capability("plan")
    settings.capability_core_enabled = True
    unregister_capability("plan")
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    pipeline["reviewer"]["enabled"] = True
    register_capability(
        plan_capability_definition(
            pipeline_config=pipeline,
            max_attempts=2,
        )
    )
    monkeypatch.setattr(
        adapter_module,
        "capture_repo_revision",
        AsyncMock(return_value={"available": True, "head": "abc"}),
    )
    monkeypatch.setattr(
        adapter_module,
        "broadcast_plan_event",
        AsyncMock(),
    )
    yield
    unregister_capability("plan")
    if previous_definition is not None:
        register_capability(previous_definition)
    settings.capability_core_enabled = previous_flag


async def _create_invocation(db_session) -> tuple[Task, CapabilityInvocation]:
    task = Task(
        title="Implement the capability",
        description="Build the requested feature safely",
        target_repo="/repo",
        target_branch="main",
        provider="codex",
    )
    db_session.add(task)
    await db_session.commit()
    invocation, created = await capability_service.create_human_invocation(
        db_session,
        task_id=task.id,
        capability_key="plan",
        request_payload={"prompt": "Produce an implementation plan"},
        idempotency_key=f"plan-{task.id}",
        requested_by_user_id=7,
    )
    assert created is True
    return task, invocation


async def _execution(db_session, invocation_id: int) -> CapabilityExecution:
    execution = await capability_service.active_execution_for(
        db_session, invocation_id
    )
    assert execution is not None
    return execution


async def _handled_run(
    db_session,
    invocation_id: int,
) -> tuple[CapabilityExecution, PlanAgentRun, Plan]:
    execution = (
        await db_session.execute(
            select(CapabilityExecution)
            .where(CapabilityExecution.invocation_id == invocation_id)
            .order_by(CapabilityExecution.attempt.desc())
            .limit(1)
        )
    ).scalar_one()
    assert execution.handle_id is not None
    run = await db_session.get(PlanAgentRun, int(execution.handle_id))
    assert run is not None and run.plan_id is not None
    plan = await db_session.get(Plan, run.plan_id)
    assert plan is not None
    return execution, run, plan


@pytest.mark.asyncio
async def test_plan_and_handle_creation_roll_back_together(db_session, monkeypatch):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    executor = PlanCapabilityExecutor()
    original_stage = adapter_module.stage_plan_with_run

    async def fail_after_staging(*args, **kwargs):
        await original_stage(*args, **kwargs)
        raise capability_service.CapabilityConflictError("claim lost")

    monkeypatch.setattr(adapter_module, "stage_plan_with_run", fail_after_staging)

    with pytest.raises(capability_service.CapabilityConflictError, match="claim lost"):
        await executor.ensure_started(db_session, invocation_id=invocation_id)

    assert await db_session.scalar(select(func.count(Plan.id))) == 0
    assert await db_session.scalar(select(func.count(PlanAgentRun.id))) == 0
    execution = await _execution(db_session, invocation_id)
    assert execution.status == "queued"
    assert execution.handle_id is None


@pytest.mark.asyncio
async def test_ensure_started_replays_exact_durable_handle(db_session):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()

    first = await executor.ensure_started(db_session, invocation_id=invocation.id)
    second = await executor.ensure_started(db_session, invocation_id=invocation.id)

    assert first.status == second.status == "running"
    assert first.plan_id == second.plan_id
    assert first.run_id == second.run_id
    assert first.execution_id == second.execution_id
    assert await db_session.scalar(select(func.count(Plan.id))) == 1
    assert await db_session.scalar(select(func.count(PlanAgentRun.id))) == 1
    execution, run, _plan = await _handled_run(db_session, invocation.id)
    assert run.run_type == "capability"
    assert run.capability_execution_id == execution.id


@pytest.mark.asyncio
async def test_concurrent_start_creates_one_reverse_bound_plan(
    db_session,
    db_factory,
):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id

    async def start():
        async with db_factory() as session:
            return await PlanCapabilityExecutor().ensure_started(
                session,
                invocation_id=invocation_id,
            )

    first, second = await asyncio.gather(start(), start())
    assert first.run_id == second.run_id
    assert first.execution_id == second.execution_id
    assert await db_session.scalar(select(func.count(Plan.id))) == 1
    run = await db_session.get(
        PlanAgentRun,
        first.run_id,
        populate_existing=True,
    )
    assert run.capability_execution_id == first.execution_id


@pytest.mark.asyncio
async def test_plan_staging_uses_request_task_snapshot(db_session):
    task, invocation = await _create_invocation(db_session)
    task.target_repo = "/changed-after-request"
    task.target_branch = "changed"
    task.title = "changed title"
    await db_session.commit()

    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation.id,
    )
    _execution_row, run, plan = await _handled_run(db_session, invocation.id)

    assert started.status == "running"
    assert plan.target_repo == "/repo"
    assert plan.target_branch == "main"
    assert plan.title == f"Plan for #{task.id}: Implement the capability"
    assert run.context_session_id == invocation.request_task_session_id


@pytest.mark.asyncio
async def test_waiting_plan_maps_to_waiting_and_answered_run_resumes(db_session):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution_row, run, _plan = await _handled_run(db_session, invocation.id)

    run.status = "waiting_user"
    run.open_input_request_id = 41
    await db_session.commit()
    waiting = await executor.observe(db_session, invocation_id=invocation.id)
    assert waiting.status == "waiting_user"
    assert waiting.run_status == "waiting_user"
    assert waiting.input_request_id == 41

    run.status = "queued"
    run.open_input_request_id = None
    run.generation += 1
    await db_session.commit()
    resumed = await executor.observe(db_session, invocation_id=invocation.id)
    assert resumed.status == "running"
    assert resumed.run_status == "queued"
    assert resumed.run_generation == 1


async def _complete_run(
    db_session,
    invocation_id: int,
    *,
    run_verdict: str = "approve",
    version_verdict: str = "approve",
    exhausted: bool = False,
    exact_identity: bool = True,
    with_result: bool = True,
) -> int | None:
    _execution_row, run, plan = await _handled_run(db_session, invocation_id)
    version_id = None
    if with_result:
        planner_step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=run.generation,
            step_type="planner",
            round=run.round,
            provider="codex",
            status="completed",
            output="# Exact implementation plan",
            finished_at=datetime.utcnow(),
        )
        reviewer_step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=run.generation,
            step_type="reviewer",
            round=run.round,
            provider="codex",
            status="completed",
            output='{"action":"approve"}',
            finished_at=datetime.utcnow(),
        )
        db_session.add_all([planner_step, reviewer_step])
        await db_session.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=run.id if exact_identity else None,
            produced_by_step_id=planner_step.id,
            content="# Exact implementation plan",
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            repo_revision=run.repo_revision,
            reviewer_repo_revision=run.repo_revision,
            review_verdict=version_verdict,
            review_feedback="looks good",
            reviewed_by_step_id=reviewer_step.id,
            review_exhausted=exhausted,
            reviewed_at=datetime.utcnow(),
        )
        db_session.add(version)
        await db_session.flush()
        planner_step.plan_version_id = version.id
        run.draft_step_id = planner_step.id
        version_id = version.id
        plan.current_version_id = version.id
    plan.active_run_id = None
    run.status = "completed"
    run.current_stage = "complete"
    run.result_version_id = version_id
    run.review_verdict = run_verdict
    run.review_exhausted = exhausted
    run.finished_at = datetime.utcnow()
    await db_session.commit()
    return version_id


@pytest.mark.asyncio
async def test_only_exact_approved_version_completes_capability(db_session):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    version_id = await _complete_run(db_session, invocation.id)

    ready = await executor.observe(db_session, invocation_id=invocation.id)

    assert ready.status == "ready"
    assert ready.output_version_id == version_id
    assert ready.output_hash is not None and len(ready.output_hash) == 64
    execution = await db_session.get(CapabilityExecution, ready.execution_id)
    assert execution.output_kind == "plan_version"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "completion",
    [
        {"with_result": False},
        {"exact_identity": False},
        {"run_verdict": "revise", "version_verdict": "revise"},
        {"exhausted": True, "run_verdict": "revise", "version_verdict": "exhausted"},
    ],
    ids=["missing-result", "wrong-identity", "revise", "review-exhausted"],
)
async def test_unapproved_or_inexact_completed_run_fails_closed(
    db_session,
    completion,
):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    await _complete_run(db_session, invocation.id, **completion)

    failed = await executor.observe(db_session, invocation_id=invocation.id)

    assert failed.status == "failed"
    assert failed.error_code in {
        "plan_result_invalid",
        "plan_review_not_approved",
    }
    stored = await db_session.get(CapabilityInvocation, invocation.id)
    assert stored.active_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper",
    ["planner_type", "reviewer_run", "produced_step", "reviewed_step"],
)
async def test_wrong_planner_or_reviewer_step_identity_fails_closed(
    db_session,
    tamper,
):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    version_id = await _complete_run(db_session, invocation.id)
    version = await db_session.get(PlanVersion, version_id)
    _execution_row, run, _plan = await _handled_run(db_session, invocation.id)
    planner = await db_session.get(PlanAgentStep, version.produced_by_step_id)
    reviewer = await db_session.get(PlanAgentStep, version.reviewed_by_step_id)
    assert planner is not None and reviewer is not None

    if tamper == "planner_type":
        planner.step_type = "reviewer"
    elif tamper == "reviewer_run":
        reviewer.run_id = run.id + 999
    elif tamper == "produced_step":
        version.produced_by_step_id = reviewer.id
    else:
        version.reviewed_by_step_id = planner.id
    await db_session.commit()

    failed = await executor.observe(db_session, invocation_id=invocation.id)
    assert failed.status == "failed"
    assert failed.error_code == "plan_result_invalid"


@pytest.mark.asyncio
async def test_cancel_proves_plan_run_stopped_before_capability_terminal(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    stopper = AsyncMock(return_value=True)
    executor = PlanCapabilityExecutor(stop_callback=stopper)
    await executor.ensure_started(db_session, invocation_id=invocation_id)
    _execution_row, run, _plan = await _handled_run(db_session, invocation_id)
    run.status = "running"
    run.generation = 3
    await db_session.commit()

    without_stopper = PlanCapabilityExecutor()
    with pytest.raises(PlanCapabilityCancellationUnconfirmed, match="stop callback"):
        await without_stopper.cancel(db_session, invocation_id=invocation_id)
    still_cancelling = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert still_cancelling.status == "cancelling"

    cancelled = await executor.cancel(db_session, invocation_id=invocation_id)
    assert cancelled.status == "cancelled"
    assert cancelled.run_status == "cancelled"
    stopper.assert_awaited_once_with(run.id, None)
    stored_run = await db_session.get(PlanAgentRun, run.id)
    assert stored_run.status == "cancelled"


@pytest.mark.asyncio
async def test_cancel_stop_false_retains_durable_cancelling_fence(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    _execution_row, run, _plan = await _handled_run(db_session, invocation_id)
    run.status = "running"
    await db_session.commit()

    rejected = PlanCapabilityExecutor(stop_callback=AsyncMock(return_value=False))
    with pytest.raises(
        PlanCapabilityCancellationUnconfirmed,
        match="stop was not confirmed",
    ):
        await rejected.cancel(db_session, invocation_id=invocation_id)

    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_run = await db_session.get(
        PlanAgentRun,
        run.id,
        populate_existing=True,
    )
    assert stored_invocation.status == "cancelling"
    assert stored_run.status == "cancelling"

    accepted_stopper = AsyncMock(return_value=True)
    cancelled = await PlanCapabilityExecutor(
        stop_callback=accepted_stopper
    ).cancel(db_session, invocation_id=invocation_id)
    assert cancelled.status == "cancelled"
    accepted_stopper.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_checks_every_instance_reverse_owner(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    _execution_row, run, _plan = await _handled_run(db_session, invocation_id)
    owner = Instance(
        name="late-owner",
        status="running",
        pid=999,
        current_plan_run_id=run.id,
    )
    db_session.add(owner)
    await db_session.commit()

    with pytest.raises(
        PlanCapabilityCancellationUnconfirmed,
        match="still owns live Instance",
    ):
        await PlanCapabilityExecutor().cancel(
            db_session,
            invocation_id=invocation_id,
        )
    current = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert current.status == "cancelling"


@pytest.mark.asyncio
async def test_capability_cancel_fence_rejects_waiting_input_answer(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation_id)
    _execution_row, run, plan = await _handled_run(db_session, invocation_id)
    input_request = PlanInputRequest(
        plan_id=plan.id,
        run_id=run.id,
        source_step_id=1,
        requested_by="planner",
        questions=[],
        status="open",
        idempotency_key=f"cap-input-{run.id}",
        opened_at=datetime.utcnow(),
    )
    db_session.add(input_request)
    await db_session.flush()
    run.status = "waiting_user"
    run.open_input_request_id = input_request.id
    await db_session.commit()
    waiting = await executor.observe(db_session, invocation_id=invocation_id)
    assert waiting.status == "waiting_user"

    capability = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    await capability_service.cancel_invocation(
        db_session,
        invocation_id=invocation_id,
        expected_state_version=capability.state_version,
    )
    async with plan_operation_lock(plan.id):
        with pytest.raises(Exception) as raised:
            await answer_input_request(
                db_session,
                plan=plan,
                run=run,
                input_request=input_request,
                expected_generation=run.generation,
                idempotency_key="answer-after-cancel",
                answers=[],
                response_text=None,
                attachments=None,
                answered_by=7,
            )
    assert getattr(raised.value, "status_code", None) == 409
