"""Task-scoped artifact downloads for links emitted in chat messages."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import require_task_access
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task


router = APIRouter(prefix="/api/tasks", tags=["task-artifacts"])

MAX_ARTIFACT_DOWNLOAD_SIZE = 100 * 1024 * 1024
MAX_ARTIFACT_REFERENCE_LENGTH = 4096
CONTAINER_WORKSPACE = Path("/workspace")


def _get_worker_proxy():
    from backend.main import worker_proxy

    return worker_proxy


def _decode_artifact_reference(reference: str) -> str:
    """Return the filesystem portion of one Markdown link target."""

    if (
        not reference
        or not reference.strip()
        or len(reference) > MAX_ARTIFACT_REFERENCE_LENGTH
        or "\x00" in reference
    ):
        raise HTTPException(400, "Invalid artifact path")

    decoded = unquote(reference.strip())
    parsed = urlsplit(decoded)
    if parsed.scheme:
        if parsed.scheme.lower() != "file" or parsed.netloc not in {"", "localhost"}:
            raise HTTPException(400, "Artifact link must reference a task file")
        decoded_path = unquote(parsed.path)
    else:
        if parsed.netloc:
            raise HTTPException(400, "Artifact link must reference a task file")
        decoded_path = parsed.path

    if not decoded_path or "\x00" in decoded_path or "\\" in decoded_path:
        raise HTTPException(400, "Invalid artifact path")
    return decoded_path


async def _task_workspace_root(task: Task, db: AsyncSession) -> Path:
    """Resolve the authoritative project root for a task on this node."""

    candidates: list[str] = []
    if task.target_repo:
        candidates.append(task.target_repo)
    if task.project_id:
        project = await db.get(Project, task.project_id)
        if project and project.local_path:
            candidates.append(project.local_path)

    for raw_root in candidates:
        try:
            root = Path(raw_root).expanduser().resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if root.is_dir():
            return root

    raise HTTPException(404, "Task workspace is unavailable")


def _task_execution_base(task: Task, root: Path) -> Path:
    """Resolve last_cwd while keeping it inside the task workspace."""

    raw_cwd = task.last_cwd
    if not raw_cwd:
        return root

    if raw_cwd == str(CONTAINER_WORKSPACE):
        return root
    container_prefix = f"{CONTAINER_WORKSPACE}{os.sep}"
    if raw_cwd.startswith(container_prefix):
        candidate = root / raw_cwd[len(container_prefix):]
    else:
        candidate = Path(raw_cwd).expanduser()

    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return root
    return resolved if resolved.is_dir() else root


async def resolve_task_artifact(
    task: Task,
    db: AsyncSession,
    reference: str,
) -> Path:
    """Resolve one user-controlled link target inside a task workspace."""

    artifact_path = _decode_artifact_reference(reference)
    root = await _task_workspace_root(task, db)

    container_prefix = f"{CONTAINER_WORKSPACE}{os.sep}"
    if artifact_path == str(CONTAINER_WORKSPACE):
        candidate = root
    elif artifact_path.startswith(container_prefix):
        candidate = root / artifact_path[len(container_prefix):]
    elif os.path.isabs(artifact_path):
        candidate = Path(artifact_path)
    else:
        candidate = _task_execution_base(task, root) / artifact_path

    try:
        resolved = candidate.resolve(strict=True)
    except (FileNotFoundError, NotADirectoryError):
        raise HTTPException(404, "Artifact file not found")
    except (OSError, RuntimeError) as exc:
        raise HTTPException(400, "Invalid artifact path") from exc

    try:
        resolved.relative_to(root)
    except ValueError:
        raise HTTPException(403, "Artifact path is outside the task workspace")

    if not resolved.is_file():
        raise HTTPException(400, "Artifact path is not a regular file")

    try:
        size = resolved.stat().st_size
    except OSError as exc:
        raise HTTPException(404, "Artifact file is unavailable") from exc
    if size > MAX_ARTIFACT_DOWNLOAD_SIZE:
        raise HTTPException(
            413,
            f"Artifact exceeds the {MAX_ARTIFACT_DOWNLOAD_SIZE // 1024 // 1024} MB limit",
        )
    return resolved


@router.get("/{task_id}/artifacts/download")
async def download_task_artifact(
    task_id: int,
    request: Request,
    path: str = Query(..., description="Markdown task artifact link target"),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """Download a file linked by an assistant inside this task's workspace."""

    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)

    if task.worker_id is not None:
        worker_proxy = _get_worker_proxy()
        if worker_proxy is None:
            raise HTTPException(503, "Worker functionality is unavailable")
        return await worker_proxy.stream_task_artifact(task, path)

    artifact = await resolve_task_artifact(task, db, path)
    return FileResponse(
        path=str(artifact),
        filename=artifact.name,
        media_type="application/octet-stream",
    )
