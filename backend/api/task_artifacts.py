"""Task-scoped artifact downloads for links emitted in chat messages."""

from __future__ import annotations

import errno
import os
import stat
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import quote, unquote, urlsplit

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTask

from backend.api.deps import require_task_access
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task


router = APIRouter(prefix="/api/tasks", tags=["task-artifacts"])

MAX_ARTIFACT_DOWNLOAD_SIZE = 100 * 1024 * 1024
MAX_ARTIFACT_REFERENCE_LENGTH = 4096
ARTIFACT_STREAM_CHUNK_SIZE = 64 * 1024
CONTAINER_WORKSPACE = Path("/workspace")


@dataclass
class OpenedTaskArtifact:
    """One validated artifact descriptor owned by a streaming response."""

    fd: int
    filename: str
    size: int
    _close_lock: Any = field(default_factory=threading.Lock, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def read(self, size: int) -> bytes:
        with self._close_lock:
            if self._closed:
                return b""
            return os.read(self.fd, size)

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            os.close(self.fd)


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


def _resolve_artifact_parts(
    task: Task,
    root: Path,
    reference: str,
) -> tuple[tuple[str, ...], str]:
    """Resolve a link to canonical workspace-relative components.

    The returned pathname is not trusted for opening.  Callers must traverse
    the components from an already-open workspace descriptor with no-follow
    semantics so a task process cannot swap in a symlink after this check.
    """

    artifact_path = _decode_artifact_reference(reference)

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

    parts = resolved.relative_to(root).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise HTTPException(400, "Invalid artifact path")
    return parts, resolved.name


def _raise_artifact_open_error(exc: OSError) -> NoReturn:
    if exc.errno in {errno.ENOENT, getattr(errno, "ESTALE", -1)}:
        raise HTTPException(404, "Artifact file not found") from exc
    if exc.errno in {
        errno.EACCES,
        errno.EPERM,
        errno.ENOTDIR,
        getattr(errno, "ELOOP", -1),
    }:
        raise HTTPException(403, "Artifact path changed or is unsafe") from exc
    raise HTTPException(400, "Unable to open artifact safely") from exc


def _secure_open_flags(*, directory: bool) -> int:
    if (
        not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_DIRECTORY")
        or os.open not in getattr(os, "supports_dir_fd", set())
    ):
        raise HTTPException(
            501,
            "Secure task artifact downloads are unsupported on this platform",
        )

    flags = os.O_RDONLY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    if directory:
        flags |= os.O_DIRECTORY
    else:
        # Avoid blocking if a raced pathname resolves to a FIFO or device.
        flags |= getattr(os, "O_NONBLOCK", 0)
    return flags


def _open_workspace_root(root: Path) -> int:
    root_fd: int | None = None
    try:
        root_fd = os.open(root, _secure_open_flags(directory=True))
        root_stat = os.fstat(root_fd)
    except OSError as exc:
        if root_fd is not None:
            os.close(root_fd)
        _raise_artifact_open_error(exc)

    assert root_fd is not None
    if not stat.S_ISDIR(root_stat.st_mode):
        os.close(root_fd)
        raise HTTPException(404, "Task workspace is unavailable")
    return root_fd


def _open_resolved_artifact(
    root_fd: int,
    parts: tuple[str, ...],
    filename: str,
) -> OpenedTaskArtifact:
    """Open canonical parts beneath root_fd without following raced symlinks."""

    try:
        current_fd = os.dup(root_fd)
    except OSError as exc:
        _raise_artifact_open_error(exc)
    artifact_fd: int | None = None
    try:
        for component in parts[:-1]:
            next_fd = os.open(
                component,
                _secure_open_flags(directory=True),
                dir_fd=current_fd,
            )
            os.close(current_fd)
            current_fd = next_fd

        artifact_fd = os.open(
            parts[-1],
            _secure_open_flags(directory=False),
            dir_fd=current_fd,
        )
        artifact_stat = os.fstat(artifact_fd)
        if not stat.S_ISREG(artifact_stat.st_mode):
            raise HTTPException(400, "Artifact path is not a regular file")
        if artifact_stat.st_size > MAX_ARTIFACT_DOWNLOAD_SIZE:
            raise HTTPException(
                413,
                (
                    "Artifact exceeds the "
                    f"{MAX_ARTIFACT_DOWNLOAD_SIZE // 1024 // 1024} MB limit"
                ),
            )
        opened = OpenedTaskArtifact(
            fd=artifact_fd,
            filename=filename,
            size=artifact_stat.st_size,
        )
        artifact_fd = None
        return opened
    except OSError as exc:
        _raise_artifact_open_error(exc)
    finally:
        os.close(current_fd)
        if artifact_fd is not None:
            os.close(artifact_fd)


def _open_task_artifact(
    task: Task,
    root: Path,
    reference: str,
) -> OpenedTaskArtifact:
    """Anchor the workspace, resolve the link, then open the exact descriptor."""

    root_fd = _open_workspace_root(root)
    try:
        parts, filename = _resolve_artifact_parts(task, root, reference)
        return _open_resolved_artifact(root_fd, parts, filename)
    finally:
        os.close(root_fd)


async def _artifact_body(artifact: OpenedTaskArtifact):
    # The fstat size is frozen into the response.  Never read beyond it, even
    # if a task process grows the already-open inode after validation.
    remaining = min(artifact.size, MAX_ARTIFACT_DOWNLOAD_SIZE)
    try:
        while remaining > 0:
            chunk = await anyio.to_thread.run_sync(
                artifact.read,
                min(ARTIFACT_STREAM_CHUNK_SIZE, remaining),
            )
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk
    finally:
        artifact.close()


def _artifact_response(artifact: OpenedTaskArtifact) -> StreamingResponse:
    encoded_filename = quote(artifact.filename)
    content_disposition = (
        f"attachment; filename*=utf-8''{encoded_filename}"
        if encoded_filename != artifact.filename
        else f'attachment; filename="{artifact.filename}"'
    )
    return StreamingResponse(
        _artifact_body(artifact),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": content_disposition,
            "Content-Length": str(artifact.size),
        },
        background=BackgroundTask(artifact.close),
    )


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

    root = await _task_workspace_root(task, db)
    artifact = _open_task_artifact(task, root, path)
    try:
        return _artifact_response(artifact)
    except Exception:
        artifact.close()
        raise
