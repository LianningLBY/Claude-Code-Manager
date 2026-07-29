"""Task-scoped Skill context shared by Claude and Codex transports."""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task
from backend.models.user_skill import UserSkill
from backend.services.skill_loader import (
    build_skill_prompt_content,
    discover_skills,
)

logger = logging.getLogger(__name__)

USER_SKILL_SNAPSHOTS_METADATA_KEY = "ccm_user_skill_snapshots"
WORKER_MANAGED_TASK_METADATA_KEY = "ccm_worker_managed_task"
CODEX_UNSUPPORTED_SKILLS = frozenset({"monitor"})
CODEX_UNSUPPORTED_MAIN_TOOLS = frozenset(
    {"create_monitor", "check_monitors", "stop_monitor"}
)

_CONTEXT_START = "<ccm-task-skill-context>"
_CONTEXT_END = "</ccm-task-skill-context>"


@dataclass(frozen=True, slots=True)
class UserSkillSnapshot:
    id: int
    name: str
    description: str
    content: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "content": self.content,
        }


def is_worker_managed_task_metadata(
    metadata: Mapping[str, Any] | None,
) -> bool:
    """Identify a Task copy whose executable row is owned by a Manager.

    New Manager-to-Worker writes carry an explicit marker.  The snapshot key
    remains a compatibility marker for copies created before PR7B2; Worker
    forwarding has always sent that key even when the selected list was empty.
    """

    return bool(
        isinstance(metadata, Mapping)
        and (
            metadata.get(WORKER_MANAGED_TASK_METADATA_KEY) is True
            or USER_SKILL_SNAPSHOTS_METADATA_KEY in metadata
        )
    )


def codex_monitor_supported_for_scope(
    *,
    provider: str | None,
    worker_id: int | None = None,
    shared_from_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    codex_main_mcp_enabled: bool = False,
) -> bool:
    """Return whether this exact Task scope may expose CCM Monitor.

    Claude Monitor remains unchanged.  Codex Monitor is deliberately limited
    to a local, non-shared Task whose main-task MCP capability is known to be
    enabled.  Manager-owned Worker copies remain closed even though their
    Worker-local database row has ``worker_id=NULL``.
    """

    if (provider or "claude").lower() != "codex":
        return True
    return bool(
        codex_main_mcp_enabled
        and worker_id is None
        and shared_from_id is None
        and not is_worker_managed_task_metadata(metadata)
    )


def skill_supported(
    provider: str | None,
    skill_name: str,
    *,
    codex_monitor_enabled: bool = False,
) -> bool:
    return not (
        (provider or "claude").lower() == "codex"
        and skill_name in CODEX_UNSUPPORTED_SKILLS
        and not codex_monitor_enabled
    )


def supported_skill_names(
    provider: str | None,
    names: Iterable[str],
    *,
    codex_monitor_enabled: bool = False,
) -> list[str]:
    return [
        name
        for name in names
        if skill_supported(
            provider,
            name,
            codex_monitor_enabled=codex_monitor_enabled,
        )
    ]


def filter_enabled_skills(
    provider: str | None,
    enabled_skills: Mapping[str, bool] | None,
    *,
    codex_monitor_enabled: bool = False,
) -> dict[str, bool]:
    return {
        name: bool(enabled)
        for name, enabled in (enabled_skills or {}).items()
        if enabled
        and skill_supported(
            provider,
            name,
            codex_monitor_enabled=codex_monitor_enabled,
        )
    }


def normalize_user_skill_ids(values: Sequence[Any] | None) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in values or ():
        if isinstance(value, bool):
            continue
        try:
            skill_id = int(value)
        except (TypeError, ValueError):
            continue
        if skill_id <= 0 or skill_id in seen:
            continue
        seen.add(skill_id)
        result.append(skill_id)
    return result


def user_skill_snapshot_from_mapping(
    value: Mapping[str, Any],
) -> UserSkillSnapshot | None:
    try:
        skill_id = int(value.get("id"))
    except (TypeError, ValueError):
        return None
    if skill_id <= 0:
        return None
    name = value.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    description = value.get("description")
    content = value.get("content")
    return UserSkillSnapshot(
        id=skill_id,
        name=name.strip()[:100],
        description=description if isinstance(description, str) else "",
        content=content if isinstance(content, str) else "",
    )


def user_skill_snapshots_from_metadata(
    metadata: Mapping[str, Any] | None,
) -> list[UserSkillSnapshot]:
    raw = (metadata or {}).get(USER_SKILL_SNAPSHOTS_METADATA_KEY)
    if not isinstance(raw, list):
        return []
    snapshots: list[UserSkillSnapshot] = []
    seen: set[int] = set()
    for value in raw:
        if not isinstance(value, Mapping):
            continue
        snapshot = user_skill_snapshot_from_mapping(value)
        if snapshot is None or snapshot.id in seen:
            continue
        seen.add(snapshot.id)
        snapshots.append(snapshot)
    return snapshots


async def load_user_skill_snapshots(
    db: AsyncSession,
    selected_ids: Sequence[Any] | None,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> list[UserSkillSnapshot]:
    """Resolve selected User Skills in task order.

    A Worker task carries authoritative Manager snapshots in metadata.  Local
    tasks normally have no snapshots and resolve their current database rows.
    Missing/deleted ids are omitted with a warning; database failures remain
    fatal so an explicitly selected context is never silently dropped.
    """

    ordered_ids = normalize_user_skill_ids(selected_ids)
    if not ordered_ids:
        return []

    snapshots_are_authoritative = (
        isinstance(metadata, Mapping)
        and USER_SKILL_SNAPSHOTS_METADATA_KEY in metadata
    )
    by_id = {
        snapshot.id: snapshot
        for snapshot in user_skill_snapshots_from_metadata(metadata)
        if snapshot.id in ordered_ids
    }
    missing = [skill_id for skill_id in ordered_ids if skill_id not in by_id]
    if missing and not snapshots_are_authoritative:
        rows = (
            await db.execute(select(UserSkill).where(UserSkill.id.in_(missing)))
        ).scalars().all()
        for row in rows:
            by_id[row.id] = UserSkillSnapshot(
                id=row.id,
                name=row.name,
                description=row.description or "",
                content=row.content or "",
            )

    unresolved = [skill_id for skill_id in ordered_ids if skill_id not in by_id]
    if unresolved:
        logger.warning("Selected User Skills no longer exist: %s", unresolved)
    return [by_id[skill_id] for skill_id in ordered_ids if skill_id in by_id]


async def build_user_skill_snapshot_payload(
    db: AsyncSession,
    selected_ids: Sequence[Any] | None,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return [
        snapshot.as_dict()
        for snapshot in await load_user_skill_snapshots(
            db,
            selected_ids,
            metadata=metadata,
        )
    ]


def render_user_skill_directory(skills: Sequence[UserSkillSnapshot]) -> str:
    if not skills:
        return ""
    lines = [
        "## User Skills\n",
        "The following user-defined skills are available for this task.",
        "Use ccm_read_user_skill(id) to load full content when needed.\n",
    ]
    for skill in skills:
        desc = skill.description.strip().replace("\n", " ")[:100]
        lines.append(f"- **{skill.name}** (id={skill.id}): {desc}")
    lines.append("")
    return "\n".join(lines)


async def build_task_skill_context(
    db: AsyncSession,
    *,
    task_id: int,
    provider: str | None,
    project_dir: str | Path | None,
    enabled_skills: Mapping[str, bool] | None = None,
) -> str:
    """Build one deterministic context from the persisted Task generation."""

    task = await db.get(Task, task_id)
    if task is None:
        return ""
    provider = (provider or task.provider or "claude").lower()
    from backend.config import settings

    codex_monitor_enabled = codex_monitor_supported_for_scope(
        provider=provider,
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
    )
    effective_skills = filter_enabled_skills(
        provider,
        enabled_skills if enabled_skills is not None else task.enabled_skills,
        codex_monitor_enabled=codex_monitor_enabled,
    )
    discovered = discover_skills(
        project_dir=project_dir,
        exclude=(
            set(CODEX_UNSUPPORTED_SKILLS)
            if provider == "codex" and not codex_monitor_enabled
            else None
        ),
    )
    plugin_context = build_skill_prompt_content(discovered, effective_skills)
    user_skills = await load_user_skill_snapshots(
        db,
        task.selected_user_skills,
        metadata=task.metadata_,
    )
    user_context = render_user_skill_directory(user_skills)
    return "\n\n".join(
        part.strip() for part in (plugin_context, user_context) if part.strip()
    )


def wrap_skill_context(prompt: str, context: str | None) -> str:
    """Prefix context for transports without a dedicated context channel."""

    if not context or not context.strip():
        return prompt
    return (
        f"{_CONTEXT_START}\n{context.strip()}\n{_CONTEXT_END}\n\n{prompt}"
    )


def write_skill_context_file(
    context: str | None,
    task_id: int | None,
) -> str:
    if not context or not context.strip():
        return ""
    suffix = f"-{task_id}" if task_id else ""
    fd, path = tempfile.mkstemp(
        prefix=f"ccm-skills{suffix}-",
        suffix=".md",
    )
    os.close(fd)
    with open(path, "w", encoding="utf-8") as stream:
        stream.write(context.strip() + "\n")
    return path
