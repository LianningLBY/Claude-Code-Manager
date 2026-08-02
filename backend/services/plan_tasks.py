"""Shared helpers for independent Plan Task creation and staleness checks."""

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.log_entry import LogEntry
from backend.models.task import Task


ACTIVE_PLAN_STATUSES = frozenset({"pending", "in_progress", "executing"})
MAX_ACTIVE_PLANS_PER_TASK = 3
PLAN_CONTEXT_SNAPSHOT_MAX_CHARS = 60_000


async def mark_plan_superseded(
    db: AsyncSession,
    source: Task,
    *,
    successor_id: int,
    completed_at: datetime | None = None,
) -> bool:
    """Atomically retire one reviewable Plan in favor of its successor.

    The status predicate is the durable race fence against a concurrent
    approve, reject, or second revision. The caller commits this update in the
    same transaction that creates ``successor_id``.
    """

    metadata = dict(source.metadata_ or {})
    metadata["plan_superseded_by_task_id"] = successor_id
    changed = await db.execute(
        update(Task)
        .where(
            Task.id == source.id,
            Task.mode == "plan",
            Task.status == "plan_review",
        )
        .values(
            status="superseded",
            completed_at=completed_at or datetime.utcnow(),
            metadata_=metadata,
        )
        .execution_options(synchronize_session=False)
    )
    return changed.rowcount == 1


def _worktree_stat_fingerprint(cwd: Path, status_raw: bytes) -> bytes:
    """Add no-follow file metadata to porcelain status.

    Porcelain records alone only say that a path is modified. Two successive
    edits to the same dirty path would otherwise produce the same digest.
    Size/mtime/mode (plus a symlink target) distinguish normal worktree edits
    without reading or persisting file contents.
    """

    digest = hashlib.sha256()
    root = os.fsencode(cwd)
    for record in status_raw.split(b"\0"):
        if len(record) < 4 or record[2:3] != b" ":
            continue
        relative = record[3:]
        if (
            not relative
            or os.path.isabs(relative)
            or relative == b".."
            or relative.startswith(b".." + os.sep.encode())
        ):
            continue
        path = os.path.normpath(os.path.join(root, relative))
        try:
            stat_result = os.lstat(path)
        except OSError:
            digest.update(relative + b"\0missing\0")
            continue
        digest.update(relative)
        digest.update(
            (
                f"\0{stat_result.st_mode}:{stat_result.st_size}:"
                f"{stat_result.st_mtime_ns}\0"
            ).encode()
        )
        if os.path.islink(path):
            try:
                digest.update(os.readlink(path))
            except OSError:
                digest.update(b"<unreadable-link>")
    return digest.digest()


async def latest_task_log_id(db: AsyncSession, task_id: int) -> int | None:
    return await db.scalar(
        select(func.max(LogEntry.id)).where(
            LogEntry.task_id == task_id,
            LogEntry.role.in_(("user", "assistant")),
            LogEntry.event_type.in_(("message", "user_message")),
        )
    )


async def capture_task_context(
    db: AsyncSession,
    task_id: int,
    *,
    through_log_id: int | None = None,
    max_chars: int = PLAN_CONTEXT_SNAPSHOT_MAX_CHARS,
) -> str:
    """Capture a stable, bounded transcript for an independent Plan."""

    target = await db.get(Task, task_id)
    query = (
        select(LogEntry.id, LogEntry.role, LogEntry.content)
        .where(
            LogEntry.task_id == task_id,
            LogEntry.event_type.in_(("message", "user_message")),
            LogEntry.role.in_(("user", "assistant")),
        )
        .order_by(LogEntry.id)
    )
    if through_log_id is not None:
        query = query.where(LogEntry.id <= through_log_id)
    rows = list((await db.execute(query)).all())
    parts = []
    if target is not None and target.description:
        parts.append(f"user (initial task): {target.description}")
    parts.extend(
        f"{role}: {content}"
        for _, role, content in rows
        if role and content
    )
    transcript = "\n\n".join(parts)
    bounded = max(1_000, max_chars)
    if len(transcript) > bounded:
        return (
            "[Earlier transcript omitted due to size]\n\n"
            + transcript[-bounded:]
        )
    return transcript


async def capture_repo_revision(path: str | None) -> dict | None:
    """Capture a cheap, non-secret repo freshness fingerprint.

    The full porcelain output may contain user filenames, so persist only its
    digest. A missing/non-git path is represented explicitly rather than
    pretending it is a stable empty repository.
    """

    if not path:
        return None
    cwd = Path(path).expanduser()
    if not cwd.is_dir():
        return {"available": False, "reason": "missing"}

    async def run_git(*args: str) -> tuple[int, bytes]:
        process = None
        communicate_task = None
        try:
            git_env = {
                key: value
                for key, value in os.environ.items()
                if key.upper() not in {"CLAUDECODE", "CLAUDE_CODE"}
            }
            # `git status` may otherwise refresh/write the index, and a
            # repository-local fsmonitor command is executable configuration.
            # Fingerprinting for a read-only Plan must do neither.
            git_env["GIT_OPTIONAL_LOCKS"] = "0"
            process = await asyncio.create_subprocess_exec(
                "git",
                "-c",
                "core.fsmonitor=false",
                *args,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=git_env,
            )
            communicate_task = asyncio.create_task(process.communicate())
            stdout, _ = await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=5,
            )
            return process.returncode or 0, stdout
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            if communicate_task is not None:
                await asyncio.gather(communicate_task, return_exceptions=True)
            raise
        except asyncio.TimeoutError:
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            if communicate_task is not None:
                await asyncio.gather(communicate_task, return_exceptions=True)
            return -1, b""
        except OSError:
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            if communicate_task is not None:
                await asyncio.gather(communicate_task, return_exceptions=True)
            return -1, b""

    head_rc, head_raw = await run_git("rev-parse", "--verify", "HEAD")
    status_rc, status_raw = await run_git(
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    )
    if head_rc != 0 and status_rc != 0:
        return {"available": False, "reason": "not_git"}
    return {
        "available": True,
        "head": (
            head_raw.decode("utf-8", errors="replace").strip()
            if head_rc == 0
            else None
        ),
        "dirty_sha256": (
            hashlib.sha256(
                status_raw
                + _worktree_stat_fingerprint(cwd, status_raw)
            ).hexdigest()
            if status_rc == 0
            else None
        ),
    }


async def plan_staleness(
    db: AsyncSession,
    plan: Task,
    *,
    current_target: Task | None = None,
) -> dict:
    """Return durable reasons why a completed Plan may be out of date."""

    target = current_target
    if target is None:
        target_id = plan.plan_target_task_id or plan.id
        target = await db.get(Task, target_id)
    if target is None:
        return {
            "stale": True,
            "reasons": ["target_missing"],
            "current_log_id": None,
            "current_repo_revision": None,
        }

    reasons: list[str] = []
    current_log_id = await latest_task_log_id(db, target.id)
    if (
        plan.plan_target_task_id is not None
        and current_log_id is not None
        and (
            plan.plan_context_log_id is None
            or current_log_id > plan.plan_context_log_id
        )
    ):
        reasons.append("conversation_changed")

    current_repo_revision = await capture_repo_revision(
        target.last_cwd or target.target_repo
    )
    if (
        plan.plan_repo_revision is not None
        and current_repo_revision != plan.plan_repo_revision
    ):
        reasons.append("repository_changed")

    return {
        "stale": bool(reasons),
        "reasons": reasons,
        "current_log_id": current_log_id,
        "current_repo_revision": current_repo_revision,
    }


async def approved_plans_for_message(
    db: AsyncSession,
    target: Task,
    plan_task_ids: list[int] | None,
    *,
    confirmed_stale_plan_task_ids: list[int] | None = None,
) -> list[Task]:
    """Validate explicit Plan attachments without mutating application state."""

    raw_ids = plan_task_ids or []
    if not raw_ids:
        return []
    if len(raw_ids) > 5:
        raise ValueError("At most 5 approved Plans can be attached to one message")
    ids: list[int] = []
    seen: set[int] = set()
    for value in raw_ids:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("plan_task_ids must contain positive integers")
        if value in seen:
            raise ValueError("plan_task_ids must not contain duplicates")
        seen.add(value)
        ids.append(value)

    rows = await db.execute(select(Task).where(Task.id.in_(ids)))
    by_id = {plan.id: plan for plan in rows.scalars().all()}
    confirmed = set(confirmed_stale_plan_task_ids or [])
    plans: list[Task] = []
    for plan_id in ids:
        plan = by_id.get(plan_id)
        if plan is None:
            raise ValueError(f"Plan Task #{plan_id} was not found")
        if plan.mode != "plan" or plan.plan_target_task_id != target.id:
            raise ValueError(
                f"Plan Task #{plan_id} is not associated with Task #{target.id}"
            )
        if (
            plan.plan_approved is not True
            or plan.status != "completed"
            or not plan.plan_content
        ):
            raise ValueError(f"Plan Task #{plan_id} is not approved and ready")
        if plan.plan_applied_at is not None:
            raise ValueError(f"Plan Task #{plan_id} has already been applied")
        stale = await plan_staleness(db, plan, current_target=target)
        if stale["stale"] and plan_id not in confirmed:
            error = ValueError(
                f"Plan Task #{plan_id} context changed; confirm stale application"
            )
            setattr(error, "staleness", stale)
            setattr(error, "plan_task_id", plan_id)
            raise error
        plans.append(plan)
    return plans


def applied_plan_snapshots(plans: list[Task]) -> list[dict[str, object]]:
    """Freeze the exact approved Plan content used by one user message."""

    return [
        {
            "id": plan.id,
            "title": plan.title or f"Plan #{plan.id}",
            "content": plan.plan_content or "",
        }
        for plan in plans
    ]


def build_approved_plan_prompt(plans: list[Task], user_prompt: str) -> str:
    if not plans:
        return user_prompt
    parts = [
        "[Approved Plans explicitly selected by the user for this turn]",
        (
            "The plans below are context for the user's current instruction. "
            "Do not treat approval alone as permission beyond that instruction."
        ),
    ]
    for plan in plans:
        parts.append(
            f'<approved_plan task_id="{plan.id}">\n'
            f"{plan.plan_content}\n"
            "</approved_plan>"
        )
    parts.extend(["[User instruction for this turn]", user_prompt])
    return "\n\n".join(parts)
