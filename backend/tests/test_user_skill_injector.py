"""Cross-database regressions for user-skill prompt injection."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from backend.config import settings
from backend.models.task import Task
from backend.models.user_skill import UserSkill
from backend.services import user_skill_injector


def test_builds_prompt_from_sqlite(tmp_path, monkeypatch):
    db_path = tmp_path / "skills.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            selected_user_skills JSON
        );
        CREATE TABLE user_skills (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        );
        INSERT INTO tasks VALUES (7, '[2]');
        INSERT INTO user_skills VALUES (2, 'Review', 'Check edge cases');
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        settings,
        "database_url",
        f"sqlite+aiosqlite:///{db_path}",
    )

    prompt_path = user_skill_injector.build_user_skill_prompt_sync(7)
    try:
        assert prompt_path is not None
        content = Path(prompt_path).read_text()
        assert "**Review** (id=2): Check edge cases" in content
    finally:
        if prompt_path:
            os.unlink(prompt_path)


def test_postgres_async_url_uses_sync_driver(monkeypatch):
    observed: dict[str, str] = {}

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def first(self):
            return self._rows[0] if self._rows else None

        def all(self):
            return self._rows

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, statement, _params):
            sql = str(statement)
            if "FROM tasks" in sql:
                return _Result([{"selected_user_skills": [3]}])
            return _Result(
                [{"id": 3, "name": "PG Skill", "description": "loaded"}]
            )

    class _Engine:
        def connect(self):
            return _Connection()

        def dispose(self):
            return None

    def _create_engine(url, **_kwargs):
        observed["url"] = url
        return _Engine()

    monkeypatch.setattr(
        settings,
        "database_url",
        "postgresql+asyncpg://user:pass@db/ccm",
    )
    monkeypatch.setattr(user_skill_injector, "create_engine", _create_engine)

    prompt_path = user_skill_injector.build_user_skill_prompt_sync(9)
    try:
        assert prompt_path is not None
        assert observed["url"].startswith("postgresql://")
        assert "PG Skill" in Path(prompt_path).read_text()
    finally:
        if prompt_path:
            os.unlink(prompt_path)


@pytest.mark.asyncio
async def test_async_builder_uses_application_session_factory(
    db_session,
    db_factory,
):
    skill = UserSkill(
        name="Async Review",
        description="Use the configured async driver",
        content="full content",
    )
    task = Task(
        title="user-skill async lookup",
        description="test",
        status="pending",
        selected_user_skills=[],
    )
    db_session.add_all([skill, task])
    await db_session.flush()
    task.selected_user_skills = [skill.id]
    await db_session.commit()

    prompt_path = await user_skill_injector.build_user_skill_prompt(
        task.id,
        db_factory,
    )
    try:
        assert prompt_path is not None
        assert "Async Review" in Path(prompt_path).read_text()
    finally:
        if prompt_path:
            os.unlink(prompt_path)
