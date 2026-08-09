"""Crash-safe cancellation regressions for ordinary first-class Plan Runs."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.instance import Instance
from backend.models.plan import Plan
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
)
from backend.services.dispatcher import GlobalDispatcher
from backend.services.plan_service import cancel_run


def _dispatcher(db_factory) -> GlobalDispatcher:
    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=manager,
        broadcaster=broadcaster,
    )


async def _seed_run(
    db_factory,
    *,
    name: str,
    status: str = "running",
    generation: int = 4,
    cancellation_target_generation: int | None = None,
    receipt_status: str = "launching",
    run_type: str = "initial",
    capability_execution_id: int | None = None,
) -> SimpleNamespace:
    runtime_generation = (
        cancellation_target_generation
        if cancellation_target_generation is not None
        else generation
    )
    async with db_factory() as db:
        plan = Plan(
            title=name,
            initial_request="Cancel this Plan safely",
            pipeline_config={},
        )
        owner = Instance(name=f"{name}-owner", status="running", pid=None)
        db.add_all([plan, owner])
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type=run_type,
            capability_execution_id=capability_execution_id,
            status=status,
            current_stage="planner",
            generation=generation,
            cancellation_target_generation=cancellation_target_generation,
            instance_id=owner.id,
            pipeline_config={},
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        owner.current_plan_run_id = run.id
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=runtime_generation,
            step_type="planner",
            round=1,
            provider="codex",
            status="running",
        )
        db.add(step)
        await db.flush()
        receipt = PlanAgentRuntimeReceipt(
            run_id=run.id,
            step_id=step.id,
            run_generation=runtime_generation,
            attempt_index=1,
            provider="codex",
            runtime_token=uuid.uuid4().hex,
            prepared_boot_id="00000000-0000-0000-0000-000000000001",
            prepared_start_ticks=1,
            prepared_uid=os.getuid(),
            status=receipt_status,
            codex_home=(
                "/managed/test-codex-home"
                if receipt_status == "launching"
                else None
            ),
            codex_thread_id=(
                f"test-thread-{run.id}"
                if receipt_status == "launching"
                else None
            ),
            cleanup_error=(
                "runtime cleanup remains uncertain"
                if receipt_status == "cleanup_failed"
                else None
            ),
            cleaned_at=(
                datetime.utcnow() if receipt_status == "cleaned" else None
            ),
        )
        db.add(receipt)
        await db.commit()
        return SimpleNamespace(
            plan_id=plan.id,
            run_id=run.id,
            owner_id=owner.id,
            step_id=step.id,
            receipt_id=receipt.id,
            generation=generation,
            runtime_generation=runtime_generation,
        )


async def _assert_fenced_owner_graph(db_factory, graph) -> None:
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        owner = await db.get(Instance, graph.owner_id)
        assert plan is not None and plan.active_run_id == graph.run_id
        assert run is not None and run.status == "cancelling"
        assert run.generation == graph.generation + 1
        assert run.cancellation_target_generation == graph.generation
        assert run.instance_id == graph.owner_id
        assert run.last_execution_started_at is not None
        assert run.finished_at is None
        assert owner is not None and owner.current_plan_run_id == graph.run_id
        assert owner.status == "running"


@pytest.mark.asyncio
async def test_ordinary_cancel_service_fences_generation_and_is_idempotent(db_factory):
    """Cancellation must preserve exact G ownership until cleanup is proven."""

    graph = await _seed_run(
        db_factory,
        name="ordinary-durable-fence",
        generation=7,
        receipt_status="launching",
    )

    for _ in range(2):
        async with db_factory() as db:
            plan = await db.get(Plan, graph.plan_id)
            run = await db.get(PlanAgentRun, graph.run_id)
            assert plan is not None and run is not None
            await cancel_run(db, plan=plan, run=run)

        await _assert_fenced_owner_graph(db_factory, graph)


@pytest.mark.asyncio
async def test_ordinary_cancel_api_never_terminalizes_unconfirmed_cleanup(
    client,
    session_factory,
    monkeypatch,
):
    """A failed stop stays retryable without advancing or discarding G."""

    graph = await _seed_run(
        session_factory,
        name="ordinary-stop-unconfirmed",
        generation=9,
        receipt_status="launching",
    )
    stop = AsyncMock(side_effect=RuntimeError("runtime stop is unconfirmed"))
    monkeypatch.setattr(
        "backend.main.dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop),
    )

    first = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")
    second = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert first.status_code == 409, first.text
    assert second.status_code == 409, second.text
    assert "runtime cleanup is not confirmed" in first.text
    assert stop.await_count == 2
    await _assert_fenced_owner_graph(session_factory, graph)


@pytest.mark.asyncio
async def test_ordinary_cancel_api_stops_fenced_generation_before_finalizing(
    client,
    session_factory,
    monkeypatch,
):
    """A clean G releases both owners and publishes cancelled only afterward."""

    graph = await _seed_run(
        session_factory,
        name="ordinary-clean-finalize",
        generation=12,
        receipt_status="cleaned",
    )
    observed: list[tuple[object, ...]] = []

    async def stop_after_observing_fence(run_id: int, instance_id: int | None) -> bool:
        async with session_factory() as db:
            plan = await db.get(Plan, graph.plan_id)
            run = await db.get(PlanAgentRun, graph.run_id)
            owner = await db.get(Instance, graph.owner_id)
            observed.append(
                (
                    run_id,
                    instance_id,
                    plan.active_run_id if plan is not None else None,
                    run.status if run is not None else None,
                    run.generation if run is not None else None,
                    run.cancellation_target_generation if run is not None else None,
                    run.instance_id if run is not None else None,
                    owner.current_plan_run_id if owner is not None else None,
                )
            )
        return True

    monkeypatch.setattr(
        "backend.main.dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=stop_after_observing_fence),
    )

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 200, response.text
    assert observed == [
        (
            graph.run_id,
            graph.owner_id,
            graph.run_id,
            "cancelling",
            graph.generation + 1,
            graph.generation,
            graph.owner_id,
            graph.run_id,
        )
    ]
    async with session_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        owner = await db.get(Instance, graph.owner_id)
        step = await db.get(PlanAgentStep, graph.step_id)
        receipt = await db.get(PlanAgentRuntimeReceipt, graph.receipt_id)
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "cancelled"
        assert run.generation == graph.generation + 1
        assert run.cancellation_target_generation is None
        assert run.instance_id is None
        assert run.last_execution_started_at is None
        assert run.finished_at is not None
        assert owner is not None and owner.current_plan_run_id is None
        assert owner.status == "idle"
        assert step is not None and step.status == "cancelled"
        assert receipt is not None and receipt.status == "cleaned"


@pytest.mark.asyncio
async def test_ordinary_cancel_finalization_locks_run_before_plan_in_each_transaction(
    client,
    session_factory,
    monkeypatch,
):
    """Owner release and terminal publication both use Run -> Plan order."""

    graph = await _seed_run(
        session_factory,
        name="ordinary-cancel-lock-order",
        generation=13,
        receipt_status="cleaned",
    )
    monkeypatch.setattr(
        "backend.main.dispatcher",
        SimpleNamespace(stop_plan_run_lifecycle=AsyncMock(return_value=True)),
    )
    original_get = AsyncSession.get
    locked_entities: list[type] = []

    async def traced_get(self, entity, ident, **kwargs):
        if kwargs.get("with_for_update") and entity in {PlanAgentRun, Plan}:
            locked_entities.append(entity)
        return await original_get(self, entity, ident, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", traced_get)

    response = await client.post(f"/api/plan-runs/{graph.run_id}/cancel")

    assert response.status_code == 200, response.text
    assert locked_entities == [PlanAgentRun, Plan, PlanAgentRun, Plan]


@pytest.mark.asyncio
async def test_cold_recovery_finalizes_exact_clean_ordinary_cancellation(
    db_factory,
    monkeypatch,
):
    """Manager restart converges an exact ordinary cancelling G -> G+1 graph."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run(
        db_factory,
        name="ordinary-cold-cancel",
        status="cancelling",
        generation=16,
        cancellation_target_generation=15,
        receipt_status="cleaned",
    )
    dispatcher = _dispatcher(db_factory)

    await dispatcher._recover_versioned_plan_runs()
    await dispatcher._recover_versioned_plan_runs()

    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        owner = await db.get(Instance, graph.owner_id)
        step = await db.get(PlanAgentStep, graph.step_id)
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "cancelled"
        assert run.generation == 16
        assert run.cancellation_target_generation is None
        assert run.instance_id is None
        assert run.last_execution_started_at is None
        assert owner is not None and owner.current_plan_run_id is None
        assert owner.status == "idle"
        assert step is not None and step.status == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["malformed_generation", "capability_owned"])
async def test_cold_recovery_does_not_reconcile_unsafe_ordinary_cancel_shape(
    db_factory,
    monkeypatch,
    shape,
):
    """Malformed generations and Capability-owned Runs stay fail-closed."""

    from backend.services import plan_agent_runner, plan_runtime_receipt

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    reconcile = AsyncMock(return_value=True)
    monkeypatch.setattr(
        plan_runtime_receipt,
        "reconcile_runtime_generation",
        reconcile,
    )
    graph = await _seed_run(
        db_factory,
        name=f"ordinary-cold-unsafe-{shape}",
        status="cancelling",
        generation=20,
        cancellation_target_generation=19,
        receipt_status="cleaned",
        run_type="capability" if shape == "capability_owned" else "initial",
        capability_execution_id=(987_654 if shape == "capability_owned" else None),
    )
    if shape == "malformed_generation":
        async with db_factory() as db:
            await db.execute(text("PRAGMA ignore_check_constraints = ON"))
            await db.execute(
                update(PlanAgentRun)
                .where(PlanAgentRun.id == graph.run_id)
                .values(generation=21)
            )
            await db.commit()
            await db.execute(text("PRAGMA ignore_check_constraints = OFF"))
    dispatcher = _dispatcher(db_factory)

    await dispatcher._recover_versioned_plan_runs()

    reconcile.assert_not_awaited()
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        owner = await db.get(Instance, graph.owner_id)
        step = await db.get(PlanAgentStep, graph.step_id)
        assert plan is not None and plan.active_run_id == graph.run_id
        assert run is not None and run.status == "cancelling"
        assert run.generation == (21 if shape == "malformed_generation" else 20)
        assert run.cancellation_target_generation == 19
        assert run.instance_id == graph.owner_id
        assert owner is not None and owner.current_plan_run_id == graph.run_id
        assert owner.status == "running"
        assert step is not None and step.status == "running"
