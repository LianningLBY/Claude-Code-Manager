import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from backend.config import settings
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.services.plan_agent_runner import (
    PLANNER_SCHEMA,
    REVIEWER_SCHEMA,
    PlanAgentError,
    PlanAgentRunner,
    _build_command,
    _extract_provider_content,
    _validate_structured,
)


def test_claude_plan_command_is_read_only():
    command = _build_command(
        provider="claude",
        model="claude-opus-4-6",
        effort="high",
        schema=PLANNER_SCHEMA,
        cloudrouter_api=False,
        cwd="/repo",
    )

    assert command[0] == settings.claude_binary
    assert command[command.index("--permission-mode") + 1] == "plan"
    assert "--no-session-persistence" in command
    assert "--safe-mode" in command
    assert command[command.index("--tools") + 1] == "Read,Grep,Glob"
    assert "Bash" in command[command.index("--disallowed-tools") + 1]
    assert "--dangerously-skip-permissions" not in command


def test_codex_plan_command_is_read_only():
    command = _build_command(
        provider="codex",
        model="gpt-5.6-sol",
        effort="ultra",
        schema=REVIEWER_SCHEMA,
        cloudrouter_api=False,
        cwd="/repo",
    )

    assert command[:2] == [settings.codex_binary, "exec"]
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert "--ephemeral" in command
    assert "--ignore-user-config" in command
    assert "--output-schema" in command
    assert "features.multi_agent=false" in command
    assert "--dangerously-bypass-approvals-and-sandbox" not in command


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
async def test_native_codex_plan_uses_default_home_exec_guard(db_factory):
    calls: list[str | None] = []

    class Manager:
        @asynccontextmanager
        async def codex_home_exec_guard(self, home):
            calls.append(home)
            yield "/canonical/default-codex-home"

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=Manager(),
    )

    async with runner._runtime_admission(
        provider="codex",
        home=None,
        model="gpt-5.6-sol",
    ) as (admitted_home, cloudrouter_api):
        assert admitted_home == "/canonical/default-codex-home"
        assert cloudrouter_api is False

    assert calls == [None]


@pytest.mark.asyncio
async def test_pipeline_revises_then_persists_audited_approval(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "plan_reviewer_enabled", True)
    monkeypatch.setattr(settings, "plan_reviewer_provider", "codex")
    monkeypatch.setattr(settings, "plan_reviewer_model", "gpt-5.6-sol")
    monkeypatch.setattr(settings, "plan_reviewer_effort", "xhigh")
    monkeypatch.setattr(settings, "plan_max_revision_cycles", 2)

    async with db_factory() as db:
        task = Task(
            title="Plan",
            description="Design the change",
            target_repo="/tmp",
            mode="plan",
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=MagicMock(),
    )
    runner._run_step_with_retry = AsyncMock(side_effect=[
        ({"plan": "Plan v1"}, '{"plan":"Plan v1"}'),
        (
            {"verdict": "revise", "feedback": "Add rollback"},
            '{"verdict":"revise","feedback":"Add rollback"}',
        ),
        ({"plan": "Plan v2 with rollback"}, '{"plan":"Plan v2 with rollback"}'),
        (
            {"verdict": "approve", "feedback": "Complete"},
            '{"verdict":"approve","feedback":"Complete"}',
        ),
    ])

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    result = await runner.run(task, cwd="/tmp")

    assert result.plan_content == "Plan v2 with rollback"
    assert result.verdict == "approve"
    assert result.review_exhausted is False
    assert runner._run_step_with_retry.await_count == 4
    second_planner_prompt = (
        runner._run_step_with_retry.await_args_list[2].kwargs["prompt"]
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
    assert run.status == "completed"
    assert run.round == 2
    assert run.review_verdict == "approve"
    assert [step.step_type for step in steps] == [
        "planner",
        "reviewer",
        "planner",
        "reviewer",
    ]
    assert all(step.status == "completed" for step in steps)
