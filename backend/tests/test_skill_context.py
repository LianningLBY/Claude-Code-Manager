"""Provider-neutral task Skill context tests."""

from unittest.mock import patch

import pytest

from backend.models.task import Task
from backend.models.user_skill import UserSkill
from backend.services.skill_context import (
    USER_SKILL_SNAPSHOTS_METADATA_KEY,
    build_task_skill_context,
    normalize_user_skill_ids,
    wrap_skill_context,
)
from backend.services.skill_loader import (
    Skill,
    SkillCCM,
    build_skill_prompt_content,
)


def _skill(
    name: str,
    description: str,
    *,
    body: str = "",
    always: bool = False,
) -> Skill:
    return Skill(
        name=name,
        description=description,
        body=body,
        ccm=SkillCCM(always=always),
    )


def test_disabled_skills_are_not_declared_but_always_skills_remain():
    content = build_skill_prompt_content(
        {
            "review": _skill("review", "Review changes"),
            "baseline": _skill(
                "baseline",
                "Always follow this",
                body="Baseline body",
                always=True,
            ),
        },
        {"review": False},
    )

    assert "**review**" not in content
    assert "**baseline**" in content
    assert "Baseline body" in content


def test_user_skill_id_normalization_is_ordered_and_deduplicated():
    assert normalize_user_skill_ids([3, "2", 3, 0, -1, True, "bad"]) == [3, 2]


@pytest.mark.asyncio
async def test_claude_and_codex_share_task_directory_semantics(db_session):
    user_skill = UserSkill(
        name="Personal review",
        description="Apply my review checklist",
        content="PRIVATE FULL USER SKILL BODY",
    )
    db_session.add(user_skill)
    await db_session.flush()
    task = Task(
        title="skill task",
        description="review",
        provider="claude",
        enabled_skills={"review": True, "monitor": True},
        selected_user_skills=[user_skill.id, user_skill.id],
    )
    db_session.add(task)
    await db_session.commit()

    discovered = {
        "review": _skill("review", "Review changes"),
        "monitor": _skill("monitor", "Watch work in background"),
        "disabled": _skill("disabled", "Must stay hidden"),
    }
    with patch(
        "backend.services.skill_context.discover_skills",
        return_value=discovered,
    ):
        claude_context = await build_task_skill_context(
            db_session,
            task_id=task.id,
            provider="claude",
            project_dir=None,
        )
        codex_context = await build_task_skill_context(
            db_session,
            task_id=task.id,
            provider="codex",
            project_dir=None,
        )

    for context in (claude_context, codex_context):
        assert context.count("**review**") == 1
        assert context.count("**Personal review**") == 1
        assert "**disabled**" not in context
        assert "PRIVATE FULL USER SKILL BODY" not in context
        assert "ccm_read_user_skill" in context
    assert "**monitor**" in claude_context
    assert "**monitor**" not in codex_context


@pytest.mark.asyncio
async def test_worker_snapshot_is_authoritative_without_local_user_skill(
    db_session,
):
    task = Task(
        title="worker snapshot",
        description="use snapshot",
        provider="codex",
        selected_user_skills=[91],
        metadata_={
            USER_SKILL_SNAPSHOTS_METADATA_KEY: [{
                "id": 91,
                "name": "Manager skill",
                "description": "Copied to Worker",
                "content": "Manager-only body",
            }],
        },
    )
    db_session.add(task)
    await db_session.commit()

    with patch(
        "backend.services.skill_context.discover_skills",
        return_value={},
    ):
        context = await build_task_skill_context(
            db_session,
            task_id=task.id,
            provider="codex",
            project_dir=None,
        )

    assert "**Manager skill** (id=91): Copied to Worker" in context
    assert "Manager-only body" not in context


@pytest.mark.asyncio
async def test_empty_worker_snapshot_does_not_fall_back_to_local_id(
    db_session,
):
    local_skill = UserSkill(
        id=92,
        name="Worker-local collision",
        description="Must not be exposed",
        content="LOCAL COLLIDING BODY",
    )
    task = Task(
        title="worker snapshot collision",
        description="use snapshot",
        provider="codex",
        selected_user_skills=[92],
        metadata_={USER_SKILL_SNAPSHOTS_METADATA_KEY: []},
    )
    db_session.add_all([local_skill, task])
    await db_session.commit()

    with patch(
        "backend.services.skill_context.discover_skills",
        return_value={},
    ):
        context = await build_task_skill_context(
            db_session,
            task_id=task.id,
            provider="codex",
            project_dir=None,
        )

    assert "Worker-local collision" not in context
    assert "LOCAL COLLIDING BODY" not in context


def test_exec_prompt_wraps_canonical_context_once():
    wrapped = wrap_skill_context("do work", "## Available Skills\n- review")

    assert wrapped.count("<ccm-task-skill-context>") == 1
    assert wrapped.count("</ccm-task-skill-context>") == 1
    assert wrapped.endswith("\n\ndo work")
