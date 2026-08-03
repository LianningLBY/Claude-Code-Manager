import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from backend.config import settings
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import PlanPipelineConfig
from backend.services.codex_app_server import CodexTurnProcess
from backend.services.plan_agent_runner import (
    PLANNER_SCHEMA,
    PLANNER_SCHEMA_V2,
    REVIEWER_SCHEMA_V2,
    PlanAgentError,
    PlanAgentRunner,
    PlanRouteUnavailable,
    _build_command,
    _extract_provider_content,
    _plan_request_with_attachments,
    _validate_structured,
    _validate_structured_v2,
)


def test_claude_plan_command_is_read_only():
    command = _build_command(
        provider="claude",
        model="claude-opus-4-6",
        effort="high",
        schema=PLANNER_SCHEMA,
    )

    assert command[0] == settings.claude_binary
    assert command[command.index("--permission-mode") + 1] == "plan"
    assert "--no-session-persistence" in command
    assert "--safe-mode" in command
    assert command[command.index("--tools") + 1] == "Read,Grep,Glob"
    assert "Bash" in command[command.index("--disallowed-tools") + 1]
    assert "--dangerously-skip-permissions" not in command


def test_structured_output_parsers_accept_native_provider_envelopes():
    claude_raw = json.dumps({
        "type": "result",
        "structured_output": {"plan": "Do the work safely"},
    })
    claude_content = _extract_provider_content("claude", claude_raw)
    assert _validate_structured("planner", claude_content) == {
        "plan": "Do the work safely"
    }

    codex_raw = "\n".join([
        json.dumps({"type": "thread.started"}),
        json.dumps({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": '{"verdict":"approve","feedback":"Looks good"}',
            },
        }),
    ])
    codex_content = _extract_provider_content("codex", codex_raw)
    assert _validate_structured("reviewer", codex_content) == {
        "verdict": "approve",
        "feedback": "Looks good",
    }


def test_interactive_planner_accepts_all_known_questions_without_count_limit():
    questions = [
        {
            "id": f"required_{index}",
            "header": f"Q{index}",
            "question": f"Required decision {index}",
            "response_type": "text",
            "options": [],
            "required": True,
        }
        for index in range(12)
    ]
    payload = {
        "action": "request_input",
        "reason": "All decisions materially affect the Plan",
        "questions": questions,
    }

    assert PLANNER_SCHEMA_V2["type"] == "object"
    assert REVIEWER_SCHEMA_V2["type"] == "object"
    assert not {"oneOf", "allOf", "anyOf"} & PLANNER_SCHEMA_V2.keys()
    assert not {"oneOf", "allOf", "anyOf"} & REVIEWER_SCHEMA_V2.keys()
    assert "maxItems" not in PLANNER_SCHEMA_V2["properties"]["questions"]
    assert _validate_structured_v2("planner", json.dumps(payload)) == payload


def test_interactive_schema_projects_known_inactive_action_fields():
    assert _validate_structured_v2(
        "planner",
        json.dumps({
            "action": "propose",
            "plan": "Schema smoke test passed.",
            "reason": "Known optional field emitted by Claude structured output",
        }),
    ) == {
        "action": "propose",
        "plan": "Schema smoke test passed.",
    }

    with pytest.raises(ValueError, match="invalid fields"):
        _validate_structured_v2(
            "planner",
            json.dumps({
                "action": "propose",
                "plan": "Do the work",
                "unexpected": "must remain rejected",
            }),
        )


def test_plan_request_includes_user_attachment_paths_and_names():
    task = Task(
        description="Review the proposed UI",
        metadata_={
            "file_paths": ["/srv/uploads/mockup.png", "/srv/uploads/notes.txt"],
            "attachments": [
                {"name": "modal mockup.png", "is_image": True},
                {"name": "interaction notes.txt", "is_image": False},
            ],
        },
    )

    request = _plan_request_with_attachments(task)

    assert "Review the proposed UI" in request
    assert "modal mockup.png: /srv/uploads/mockup.png" in request
    assert "interaction notes.txt: /srv/uploads/notes.txt" in request
    assert "untrusted reference data" in request


@pytest.mark.asyncio
async def test_pipeline_rejects_unknown_planner_provider(db_factory):
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    task = Task(
        title="Invalid route",
        description="Plan this",
        mode="plan",
        provider="unexpected",
    )

    with pytest.raises(PlanAgentError, match="provider must be"):
        await runner.run(task, cwd="/tmp")


@pytest.mark.asyncio
async def test_cancelled_pipeline_marks_active_step_cancelled(db_factory):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    async with db_factory() as db:
        task = Task(
            title="Cancelled Plan",
            description="Stop this Plan",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            model="claude-fable-5",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=asyncio.CancelledError())

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    with pytest.raises(asyncio.CancelledError):
        await runner.run(task, cwd="/tmp")

    async with db_factory() as db:
        run = (
            await db.execute(
                select(PlanAgentRun).where(
                    PlanAgentRun.plan_task_id == task_id
                )
            )
        ).scalar_one()
        step = (
            await db.execute(
                select(PlanAgentStep).where(PlanAgentStep.run_id == run.id)
            )
        ).scalar_one()
    assert run.status == "cancelled"
    assert run.error == "Plan pipeline cancelled"
    assert step.status == "cancelled"
    assert step.error == "Plan step cancelled"


@pytest.mark.asyncio
async def test_codex_plan_uses_disposable_read_only_app_server_thread(
    db_factory,
):
    calls: list[str | None] = []
    deleted: list[tuple[str, str]] = []
    interrupted = AsyncMock()
    process = CodexTurnProcess(
        123,
        interrupted,
        thread_id="plan-thread",
    )
    process.feed({
        "type": "item.completed",
        "item": {
            "type": "agent_message",
            "text": '{"plan":"safe plan"}',
        },
    })
    process.finish(0)

    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "plan-thread"))

    async def delete_thread(home, thread_id):
        deleted.append((home, thread_id))

    registry.delete_thread = delete_thread

    class Manager:
        @asynccontextmanager
        async def codex_home_app_server_guard(self, home):
            calls.append(home)
            yield home

        def _ensure_codex_app_server_registry(self):
            return registry

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )

    stdout, stderr, returncode = await runner._run_codex_turn(
        task_id=7,
        home="/canonical/default-codex-home",
        model="gpt-5.6-sol",
        effort="xhigh",
        cwd="/tmp",
        prompt="plan safely",
        schema=PLANNER_SCHEMA,
        timeout=10,
    )

    assert returncode == 0
    assert stderr == b""
    assert b"safe plan" in stdout
    assert calls == [
        "/canonical/default-codex-home",
        "/canonical/default-codex-home",
    ]
    assert deleted == [
        ("/canonical/default-codex-home", "plan-thread")
    ]
    kwargs = registry.start_turn.await_args.kwargs
    assert kwargs["sandbox_mode"] == "read-only"
    assert kwargs["disable_project_config"] is True
    assert kwargs["disable_user_mcp"] is True
    assert kwargs["disable_autonomous_features"] is True
    assert kwargs["output_schema"] == PLANNER_SCHEMA
    assert kwargs["resume_session_id"] is None


def test_retained_plan_agent_is_exposed_as_update_blocker(monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher

    dispatcher = MagicMock(spec=GlobalDispatcher)
    dispatcher._active_auxiliary_session_ids.return_value = (set(), set())
    dispatcher._monitor_processes = {}
    dispatcher._monitor_turn_handles = {}
    dispatcher._monitor_active_turns = set()
    monkeypatch.setattr(
        "backend.services.plan_agent_runner.active_plan_agent_task_ids",
        lambda: {42},
    )

    blockers = GlobalDispatcher.active_auxiliary_blockers(dispatcher)

    assert blockers == [{
        "id": 42,
        "title": "Plan Agent Task #42",
        "status": "running_auxiliary",
        "kind": "plan_agent",
    }]


@pytest.mark.asyncio
async def test_pipeline_revises_then_persists_audited_approval(
    db_factory,
):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 2,
    })

    async with db_factory() as db:
        task = Task(
            title="Plan",
            description="Design the change",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            model="claude-fable-5",
            effort_level="high",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=broadcaster,
    )
    runner._run_route = AsyncMock(side_effect=[
        ({"plan": "Plan v1"}, '{"plan":"Plan v1"}', "claude-1"),
        (
            {"verdict": "revise", "feedback": "Add rollback"},
            '{"verdict":"revise","feedback":"Add rollback"}',
            "codex-1",
        ),
        (
            {"plan": "Plan v2 with rollback"},
            '{"plan":"Plan v2 with rollback"}',
            "claude-1",
        ),
        (
            {"verdict": "approve", "feedback": "Complete"},
            '{"verdict":"approve","feedback":"Complete"}',
            "codex-1",
        ),
    ])

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Plan v2 with rollback"
    assert result.verdict == "approve"
    assert result.review_exhausted is False
    assert runner._run_route.await_count == 4
    second_planner_prompt = (
        runner._run_route.await_args_list[2].kwargs["prompt"]
    )
    assert "Add rollback" in second_planner_prompt

    async with db_factory() as db:
        run = (
            await db.execute(
                select(PlanAgentRun).where(
                    PlanAgentRun.plan_task_id == task_id
                )
            )
        ).scalar_one()
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars().all()
        )
        task_state = await db.get(Task, task_id)
    assert run.status == "completed"
    assert run.round == 2
    assert task_state.plan_stage == "completed"
    assert task_state.plan_stage_round == 2
    assert task_state.plan_stage_provider == "codex"
    assert task_state.plan_stage_model == "gpt-5.6-sol"
    assert task_state.plan_stage_effort == "xhigh"
    assert task_state.plan_stage_route_slot == "primary"
    assert run.review_verdict == "approve"
    assert [step.step_type for step in steps] == [
        "planner",
        "reviewer",
        "planner",
        "reviewer",
    ]
    assert all(step.status == "completed" for step in steps)
    assert [step.route_slot for step in steps] == ["primary"] * 4
    assert [step.provider for step in steps] == [
        "claude",
        "codex",
        "claude",
        "codex",
    ]
    assert run.pipeline_config == pipeline.model_dump(mode="json")
    stage_events = [
        call.args[1]
        for call in broadcaster.broadcast.await_args_list
        if call.args[1]["event"] == "plan_stage_change"
    ]
    assert [
        (event["plan_stage"], event["plan_stage_round"])
        for event in stage_events
    ] == [
        ("planning", 1),
        ("reviewing", 1),
        ("planning", 2),
        ("reviewing", 2),
        ("completed", 2),
    ]
    assert [
        (
            event.get("plan_stage_provider"),
            event.get("plan_stage_model"),
            event.get("plan_stage_route_slot"),
        )
        for event in stage_events[:-1]
    ] == [
        ("claude", "claude-fable-5", "primary"),
        ("codex", "gpt-5.6-sol", "primary"),
        ("claude", "claude-fable-5", "primary"),
        ("codex", "gpt-5.6-sol", "primary"),
    ]


@pytest.mark.asyncio
async def test_maximum_two_rounds_never_starts_a_third_planner(db_factory):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 2,
    })
    async with db_factory() as db:
        task = Task(
            title="Bounded Plan",
            description="Plan twice only",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=[
        ({"plan": "Plan v1"}, '{"plan":"Plan v1"}', "claude-1"),
        (
            {"verdict": "revise", "feedback": "Revise once"},
            '{"verdict":"revise","feedback":"Revise once"}',
            "codex-1",
        ),
        ({"plan": "Plan v2"}, '{"plan":"Plan v2"}', "claude-1"),
        (
            {"verdict": "revise", "feedback": "Still revise"},
            '{"verdict":"revise","feedback":"Still revise"}',
            "codex-1",
        ),
    ])

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Plan v2"
    assert result.review_exhausted is True
    assert runner._run_route.await_count == 4


@pytest.mark.asyncio
async def test_stage_uses_fallback_only_after_primary_route_is_unavailable(
    db_factory,
):
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    async with db_factory() as db:
        task = Task(
            title="Fallback Plan",
            description="Plan with fallback",
            target_repo="/tmp",
            mode="plan",
            provider="claude",
            model="claude-fable-5",
            effort_level="high",
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=[
        PlanRouteUnavailable(
            "Fable unavailable",
            provider="claude",
        ),
        (
            {"plan": "Fallback plan"},
            '{"plan":"Fallback plan"}',
            "codex-1",
        ),
    ])
    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Fallback plan"
    async with db_factory() as db:
        run = (
            await db.execute(
                select(PlanAgentRun).where(
                    PlanAgentRun.plan_task_id == task_id
                )
            )
        ).scalar_one()
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars().all()
        )
        task_state = await db.get(Task, task_id)
    assert [step.route_slot for step in steps] == [
        "primary",
        "fallback",
    ]
    assert [step.status for step in steps] == ["failed", "completed"]
    assert run.planner_provider == "codex"
    assert run.planner_model == "gpt-5.6-terra"
    assert task_state.plan_stage_provider == "codex"
    assert task_state.plan_stage_model == "gpt-5.6-terra"
    assert task_state.plan_stage_route_slot == "fallback"


@pytest.mark.asyncio
async def test_route_exhausts_quota_limited_accounts_before_model_fallback(
    db_factory,
):
    pool = MagicMock()
    pool.select.side_effect = ["/codex/one", "/codex/two"]
    pool.canonical_home.side_effect = lambda home: home
    pool.account_id_for_home.side_effect = {
        "/codex/one": "one",
        "/codex/two": "two",
    }.get
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        codex_pool=pool,
    )
    runner._run_fixed_route_with_retry = AsyncMock(side_effect=[
        PlanAgentError(
            "quota",
            provider="codex",
            stderr="You have hit your usage limit",
        ),
        ({"plan": "second account"}, '{"plan":"second account"}'),
    ])
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })

    result, _raw, account_id = await runner._run_route(
        task_id=19,
        route=pipeline.planner.primary,
        cwd="/tmp",
        prompt="plan",
        schema=PLANNER_SCHEMA,
        timeout=30,
    )

    assert result == {"plan": "second account"}
    assert account_id == "two"
    assert pool.select.call_args_list[1].kwargs["exclude"] == {"one"}
    pool.mark_rate_limited.assert_called_once_with("/codex/one")


@pytest.mark.asyncio
async def test_stage_fails_after_primary_and_fallback_are_unavailable(
    db_factory,
):
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_route = AsyncMock(side_effect=[
        PlanRouteUnavailable("primary unavailable", provider="claude"),
        PlanRouteUnavailable("fallback unavailable", provider="codex"),
    ])
    pipeline = PlanPipelineConfig.model_validate({
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": False,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 0,
    })
    task = Task(
        id=23,
        title="terminal fallback",
        description="plan",
        mode="plan",
    )
    run_id = await runner._create_run(task=task, pipeline=pipeline)

    with pytest.raises(
        PlanRouteUnavailable,
        match="primary and fallback routes are unavailable",
    ):
        await runner._run_stage(
            run_id=run_id,
            task_id=task.id,
            step_type="planner",
            round_number=1,
            routes=pipeline.planner,
            cwd="/tmp",
            prompt="plan",
            schema=PLANNER_SCHEMA,
            timeout=30,
        )

    async with db_factory() as db:
        steps = (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id == run_id)
                .order_by(PlanAgentStep.id)
            )
        ).scalars().all()
    assert [step.route_slot for step in steps] == ["primary", "fallback"]
    assert [step.status for step in steps] == ["failed", "failed"]
