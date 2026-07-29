import os
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select, update

from backend.models.log_entry import LogEntry
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.services.plan_tasks import capture_repo_revision


async def _target_with_session(client, session_factory) -> tuple[int, str]:
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Target",
            "description": "initial request",
            "target_repo": "/tmp",
        },
    )
    task_id = response.json()["id"]
    session_id = "target-session-1"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                session_id=session_id,
                status="completed",
                completed_at=None,
            )
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="user_message",
                role="user",
                content="A real follow-up",
            )
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content="Existing session context",
            )
        )
        await db.commit()
    return task_id, session_id


@pytest.mark.asyncio
async def test_related_plans_are_independent_and_limited(
    client,
    session_factory,
):
    target_id, session_id = await _target_with_session(
        client,
        session_factory,
    )
    plan_ids = []
    for index in range(3):
        response = await client.post(
            f"/api/tasks/{target_id}/plans",
            json={"input": f"Plan request {index}"},
        )
        assert response.status_code == 201
        data = response.json()
        plan_ids.append(data["id"])
        assert data["mode"] == "plan"
        assert data["plan_target_task_id"] == target_id
        assert "plan_context_snapshot" not in data
        # The immutable transcript is deliberately not exposed in Task list
        # payloads; verify its durable capture directly.
        async with session_factory() as db:
            plan = await db.get(Task, data["id"])
            assert plan.plan_context_session_id == session_id
            assert "initial request" in plan.plan_context_snapshot
            assert "A real follow-up" in plan.plan_context_snapshot
            assert "Existing session context" in plan.plan_context_snapshot

    fourth = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "one too many"},
    )
    assert fourth.status_code == 429
    generic_bypass = await client.post(
        "/api/tasks",
        json={
            "title": "Bypass attempt",
            "description": "one too many through generic create",
            "mode": "plan",
            "plan_target_task_id": target_id,
        },
    )
    assert generic_bypass.status_code == 429

    history = await client.get(f"/api/tasks/{target_id}/plans")
    assert history.status_code == 200
    assert {item["id"] for item in history.json()} == set(plan_ids)

    async with session_factory() as db:
        target = await db.get(Task, target_id)
    assert target.status == "completed"
    assert target.session_id == session_id


@pytest.mark.asyncio
async def test_plan_tasks_never_silently_downgrade_codex_fast(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == target_id)
            .values(
                provider="codex",
                model="gpt-5.6-sol",
                codex_service_tier="priority",
            )
        )
        await db.commit()

    related = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Plan this Fast target safely"},
    )
    assert related.status_code == 201, related.text
    assert related.json()["codex_service_tier"] == "default"

    standalone = await client.post(
        "/api/tasks",
        json={
            "title": "No hidden Fast downgrade",
            "description": "Plan it",
            "mode": "plan",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )
    assert standalone.status_code == 422
    assert "Fast is not supported" in standalone.text


@pytest.mark.asyncio
async def test_repo_fingerprint_detects_repeated_edits_to_same_dirty_path(
    tmp_path,
):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "plan-test@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Plan Test"],
        cwd=tmp_path,
        check=True,
    )
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-qm", "base"],
        cwd=tmp_path,
        check=True,
    )

    tracked.write_text("edit-one\n", encoding="utf-8")
    first_mtime = tracked.stat().st_mtime_ns
    first = await capture_repo_revision(str(tmp_path))

    # Keep the same porcelain status and byte length; only the actual dirty
    # worktree generation changes.
    tracked.write_text("edit-two\n", encoding="utf-8")
    os.utime(
        tracked,
        ns=(first_mtime + 1_000_000, first_mtime + 1_000_000),
    )
    second = await capture_repo_revision(str(tmp_path))

    assert first["head"] == second["head"]
    assert first["dirty_sha256"] != second["dirty_sha256"]


@pytest.mark.asyncio
async def test_related_plan_approval_requires_stale_confirmation_and_no_turn(
    client,
    session_factory,
):
    target_id, session_id = await _target_with_session(
        client,
        session_factory,
    )
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Design this carefully"},
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(status="plan_review", plan_content="Approved candidate")
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=target_id,
                event_type="user_message",
                role="user",
                content="Newer context",
            )
        )
        await db.commit()
        before_logs = await db.scalar(
            select(func.count(LogEntry.id)).where(
                LogEntry.task_id == target_id
            )
        )

    stale = await client.post(f"/api/tasks/{plan_id}/plan/approve")
    assert stale.status_code == 409
    assert "conversation_changed" in stale.json()["detail"]["staleness"]["reasons"]

    with patch("backend.main.dispatcher") as dispatcher:
        approved = await client.post(
            f"/api/tasks/{plan_id}/plan/approve",
            json={"confirm_stale": True},
        )
    assert approved.status_code == 200
    assert approved.json()["status"] == "completed"
    assert approved.json()["plan_approved"] is True
    dispatcher.enqueue_message.assert_not_called()
    dispatcher.wake.assert_not_called()

    async with session_factory() as db:
        target = await db.get(Task, target_id)
        after_logs = await db.scalar(
            select(func.count(LogEntry.id)).where(
                LogEntry.task_id == target_id
            )
        )
    assert target.status == "completed"
    assert target.session_id == session_id
    assert after_logs == before_logs


@pytest.mark.asyncio
async def test_approved_plan_is_applied_only_with_selected_user_message(
    client,
    session_factory,
):
    target_id, session_id = await _target_with_session(
        client,
        session_factory,
    )
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Make a plan"},
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(
                status="completed",
                plan_content="1. Change API\n2. Add tests",
                plan_approved=True,
            )
        )
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.enqueue_message = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with (
        patch("backend.main.dispatcher", dispatcher),
        patch("backend.main.broadcaster", broadcaster),
    ):
        response = await client.post(
            f"/api/tasks/{target_id}/chat",
            json={
                "message": "Please implement it",
                "plan_task_ids": [plan_id],
            },
        )
    assert response.status_code == 200
    assert response.json()["applied_plan_task_ids"] == [plan_id]
    prompt = dispatcher.enqueue_message.call_args.kwargs["prompt"]
    assert f'<approved_plan task_id="{plan_id}">' in prompt
    assert "1. Change API" in prompt
    assert prompt.index("1. Change API") < prompt.index("Please implement it")

    async with session_factory() as db:
        plan = await db.get(Task, plan_id)
        applied_log = await db.get(LogEntry, plan.plan_applied_log_id)
    assert plan.plan_applied_at is not None
    assert plan.plan_applied_to_session_id == session_id
    assert applied_log.content.endswith("Please implement it")

    second = await client.post(
        f"/api/tasks/{target_id}/chat",
        json={"message": "again", "plan_task_ids": [plan_id]},
    )
    assert second.status_code == 400
    assert "already been applied" in second.json()["detail"]


@pytest.mark.asyncio
async def test_plan_application_is_restored_when_dispatcher_is_shutting_down(
    client,
    session_factory,
):
    target_id, _ = await _target_with_session(client, session_factory)
    created = await client.post(
        f"/api/tasks/{target_id}/plans",
        json={"input": "Make a shutdown-safe plan"},
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(
                status="completed",
                plan_content="A plan that must remain attachable",
                plan_approved=True,
            )
        )
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.enqueue_message = AsyncMock(
        side_effect=RuntimeError(
            "Dispatcher is shutting down; message admission is closed"
        )
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with (
        patch("backend.main.dispatcher", dispatcher),
        patch("backend.main.broadcaster", broadcaster),
    ):
        response = await client.post(
            f"/api/tasks/{target_id}/chat",
            json={
                "message": "Please implement it",
                "plan_task_ids": [plan_id],
            },
        )

    assert response.status_code == 409
    async with session_factory() as db:
        plan = await db.get(Task, plan_id)
    assert plan.plan_applied_at is None
    assert plan.plan_applied_to_session_id is None
    assert plan.plan_applied_log_id is None


@pytest.mark.asyncio
async def test_cancel_active_plan_reaps_legacy_ralph_lifecycle_first(
    client,
    session_factory,
):
    import backend.main

    created = await client.post(
        "/api/tasks",
        json={
            "title": "Cancellable Plan",
            "description": "Plan safely",
            "target_repo": "/tmp",
            "mode": "plan",
        },
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(status="executing", instance_id=77)
        )
        await db.commit()

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.dispatcher,
            "stop_plan_agent_lifecycle",
            new_callable=AsyncMock,
            return_value=False,
        ) as dispatcher_stop,
        patch.object(
            backend.main.ralph_loop,
            "stop_plan_agent_lifecycle",
            new_callable=AsyncMock,
            return_value=True,
        ) as ralph_stop,
        patch(
            "backend.api.tasks._settle_task_launch_barrier",
            new_callable=AsyncMock,
        ),
    ):
        response = await client.post(f"/api/tasks/{plan_id}/cancel")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "cancelled"
    dispatcher_stop.assert_awaited_once_with(plan_id, 77)
    ralph_stop.assert_awaited_once_with(plan_id)


@pytest.mark.asyncio
async def test_standalone_plan_creates_one_idempotent_execution_task(
    client,
    session_factory,
):
    created = await client.post(
        "/api/tasks",
        json={
            "title": "Standalone",
            "description": "Plan a migration",
            "target_repo": "/tmp",
            "mode": "plan",
        },
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan_id)
            .values(status="plan_review", plan_content="Migration plan")
        )
        await db.commit()
    approved = await client.post(f"/api/tasks/{plan_id}/plan/approve")
    assert approved.status_code == 200

    first = await client.post(
        f"/api/tasks/{plan_id}/plan/create-execution-task"
    )
    second = await client.post(
        f"/api/tasks/{plan_id}/plan/create-execution-task"
    )
    assert first.status_code == 201
    assert second.status_code == 201
    first_task = first.json()["execution_task"]
    second_task = second.json()["execution_task"]
    assert first_task["id"] == second_task["id"]
    assert first_task["mode"] == "auto"
    assert "Migration plan" in first_task["description"]


@pytest.mark.asyncio
async def test_plan_run_history_returns_steps(
    client,
    session_factory,
):
    created = await client.post(
        "/api/tasks",
        json={
            "title": "Audited Plan",
            "description": "Plan it",
            "target_repo": "/tmp",
            "mode": "plan",
        },
    )
    plan_id = created.json()["id"]
    async with session_factory() as db:
        run = PlanAgentRun(
            plan_task_id=plan_id,
            status="completed",
            combo_used="codex+codex",
        )
        db.add(run)
        await db.flush()
        db.add(
            PlanAgentStep(
                run_id=run.id,
                step_type="planner",
                round=1,
                provider="codex",
                status="completed",
                output='{"plan":"ok"}',
            )
        )
        await db.commit()

    response = await client.get(f"/api/tasks/{plan_id}/plan/runs")
    assert response.status_code == 200
    assert response.json()[0]["steps"][0]["step_type"] == "planner"
