"""Inject user-created skills into agent prompt."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from sqlalchemy import bindparam, create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from backend.database import _PROJECT_ROOT, _async_url_to_sync


def _sync_database_url(db_url: str) -> str:
    """Return a synchronous URL with SQLite paths anchored like database.py."""
    url = make_url(_async_url_to_sync(db_url))
    if url.drivername == "sqlite" and url.database and url.database != ":memory:":
        path = Path(url.database).expanduser()
        if not path.is_absolute():
            path = _PROJECT_ROOT / path
        url = url.set(database=str(path.resolve()))
    return url.render_as_string(hide_password=False)


def _write_user_skill_prompt(task_id: int, skills) -> str | None:
    if not skills:
        return None

    lines = [
        "## User Skills\n",
        "The following user-defined skills are available for this task.",
        "Use the MCP tool ccm_read_user_skill(id) to load full content when needed.\n",
    ]
    for skill in skills:
        desc = (skill["description"] or "").strip().replace("\n", " ")[:100]
        lines.append(f"- **{skill['name']}** (id={skill['id']}): {desc}")
    lines.append("")

    content = "\n".join(lines)
    fd, path = tempfile.mkstemp(
        prefix=f"ccm-user-skills-{task_id}-",
        suffix=".md",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as prompt_file:
            prompt_file.write(content)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return path


def _normalize_skill_ids(raw_skill_ids) -> list[int]:
    skill_ids = (
        json.loads(raw_skill_ids)
        if isinstance(raw_skill_ids, str)
        else list(raw_skill_ids)
    )
    return [int(skill_id) for skill_id in skill_ids]


async def build_user_skill_prompt(task_id: int, db_factory) -> str | None:
    """Build the user-skill prompt through the application's async DB path."""
    from backend.models.task import Task
    from backend.models.user_skill import UserSkill

    try:
        async with db_factory() as db:
            raw_skill_ids = await db.scalar(
                select(Task.selected_user_skills).where(Task.id == task_id)
            )
            if not raw_skill_ids:
                return None
            skill_ids = _normalize_skill_ids(raw_skill_ids)
            if not skill_ids:
                return None
            rows = (
                await db.execute(
                    select(
                        UserSkill.id,
                        UserSkill.name,
                        UserSkill.description,
                    ).where(UserSkill.id.in_(skill_ids))
                )
            ).mappings().all()
    except (SQLAlchemyError, ModuleNotFoundError, TypeError, ValueError):
        # User skills enhance a launch, but a missing legacy table or malformed
        # selection must not prevent the coding agent from starting.
        return None

    skills_by_id = {row["id"]: row for row in rows}
    selected_skills = [
        skills_by_id[skill_id]
        for skill_id in skill_ids
        if skill_id in skills_by_id
    ]
    try:
        return _write_user_skill_prompt(task_id, selected_skills)
    except OSError:
        return None


def build_user_skill_prompt_sync(task_id: int) -> str | None:
    """Build a prompt file with user skill L0 directory (sync, for _build_command).

    Returns path to temp file, or None if no user skills selected.
    """
    from backend.config import settings
    engine = None
    try:
        engine = create_engine(
            _sync_database_url(settings.database_url),
            poolclass=NullPool,
        )
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT selected_user_skills FROM tasks "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            ).mappings().first()
            if not row or not row["selected_user_skills"]:
                return None
            raw_skill_ids = row["selected_user_skills"]
            skill_ids = _normalize_skill_ids(raw_skill_ids)
            if not skill_ids:
                return None
            query = text(
                "SELECT id, name, description FROM user_skills "
                "WHERE id IN :skill_ids"
            ).bindparams(bindparam("skill_ids", expanding=True))
            skills = conn.execute(
                query,
                {"skill_ids": skill_ids},
            ).mappings().all()
    except (SQLAlchemyError, ModuleNotFoundError, TypeError, ValueError):
        # Fail-open：注入用户技能是增强项，DB 文件缺失/表不存在（全新部署、
        # 测试 worktree）绝不能炸掉 launch 本身
        return None
    finally:
        if engine is not None:
            engine.dispose()

    skills_by_id = {skill["id"]: skill for skill in skills}
    selected_skills = [
        skills_by_id[skill_id]
        for skill_id in skill_ids
        if skill_id in skills_by_id
    ]
    try:
        return _write_user_skill_prompt(task_id, selected_skills)
    except OSError:
        return None
