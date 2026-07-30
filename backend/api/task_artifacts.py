"""Task-scoped artifact downloads for links emitted in chat messages."""

from __future__ import annotations

import errno
import os
import stat
import threading
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
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


def _configured_workspace_root(raw_root: str) -> Path:
    """Return a normalized absolute root without following any symlinks."""

    if not raw_root or "\x00" in raw_root:
        raise ValueError("invalid workspace root")
    try:
        root = Path(raw_root).expanduser()
    except RuntimeError as exc:
        raise ValueError("invalid workspace root") from exc
    if not root.is_absolute() or root.anchor != os.path.sep:
        raise ValueError("workspace root must be an absolute POSIX path")

    components = []
    for component in root.parts[1:]:
        if component in {"", "."}:
            continue
        if component == "..":
            raise ValueError("workspace root cannot contain parent traversal")
        components.append(component)
    if not components:
        raise ValueError("filesystem root cannot be a task workspace")
    return Path(os.path.sep, *components)


async def _task_workspace_root(task: Task, db: AsyncSession) -> Path:
    """Load the authoritative lexical project root for a task on this node."""

    raw_root = task.target_repo
    if not raw_root and task.project_id:
        project = await db.get(Project, task.project_id)
        if project and project.local_path:
            raw_root = project.local_path

    try:
        return _configured_workspace_root(raw_root or "")
    except ValueError as exc:
        raise HTTPException(404, "Task workspace is unavailable") from exc


def _normalize_relative_parts(
    base: tuple[str, ...],
    components: tuple[str, ...],
) -> tuple[str, ...]:
    """Apply relative components without allowing escape above the root."""

    normalized = list(base)
    for component in components:
        if component in {"", "."}:
            continue
        if component == os.path.sep:
            raise HTTPException(400, "Invalid artifact path")
        if component == "..":
            if not normalized:
                raise HTTPException(
                    403,
                    "Artifact path is outside the task workspace",
                )
            normalized.pop()
            continue
        normalized.append(component)
    return tuple(normalized)


def _container_relative_parts(value: str) -> tuple[str, ...] | None:
    path = PurePosixPath(value)
    if path.parts[:2] != (os.path.sep, CONTAINER_WORKSPACE.name):
        return None
    return _normalize_relative_parts((), path.parts[2:])


def _absolute_parts(value: str) -> tuple[str, ...]:
    path = PurePosixPath(value)
    if not path.is_absolute() or path.anchor != os.path.sep:
        raise HTTPException(400, "Invalid artifact path")
    return _normalize_relative_parts((), path.parts[1:])


def _parts_beneath_root(
    absolute_parts: tuple[str, ...],
    root: Path,
) -> tuple[str, ...]:
    root_parts = root.parts[1:]
    if absolute_parts[:len(root_parts)] != root_parts:
        raise HTTPException(403, "Artifact path is outside the task workspace")
    return absolute_parts[len(root_parts):]


def _task_execution_base_parts(
    task: Task,
    root: Path,
) -> tuple[str, ...]:
    """Map last_cwd to lexical components beneath the configured root."""

    raw_cwd = task.last_cwd
    if not raw_cwd:
        return ()

    container_parts = _container_relative_parts(raw_cwd)
    if container_parts is not None:
        return container_parts

    try:
        expanded = Path(raw_cwd).expanduser()
    except RuntimeError:
        return ()
    if not expanded.is_absolute():
        return ()
    try:
        return _parts_beneath_root(_absolute_parts(str(expanded)), root)
    except HTTPException:
        return ()


def _lexical_artifact_parts(
    task: Task,
    root: Path,
    reference: str,
) -> tuple[tuple[str, ...], str]:
    """Convert one link to lexical components beneath the anchored workspace."""

    artifact_path = _decode_artifact_reference(reference)
    container_parts = _container_relative_parts(artifact_path)
    if container_parts is not None:
        parts = container_parts
    elif PurePosixPath(artifact_path).is_absolute():
        parts = _parts_beneath_root(_absolute_parts(artifact_path), root)
    else:
        parts = _normalize_relative_parts(
            _task_execution_base_parts(task, root),
            PurePosixPath(artifact_path).parts,
        )

    if not parts:
        raise HTTPException(400, "Invalid artifact path")
    return parts, parts[-1]


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


def _open_directory_component(parent_fd: int, component: str) -> int:
    """Open one directory component relative to an already-anchored parent."""

    return os.open(
        component,
        _secure_open_flags(directory=True),
        dir_fd=parent_fd,
    )


def _open_workspace_root(root: Path) -> int:
    """Open every root component from the stable filesystem root descriptor."""

    current_fd: int | None = None
    try:
        current_fd = os.open(
            os.path.sep,
            _secure_open_flags(directory=True),
        )
        for component in root.parts[1:]:
            next_fd = _open_directory_component(current_fd, component)
            previous_fd = current_fd
            current_fd = next_fd
            os.close(previous_fd)
        root_stat = os.fstat(current_fd)
    except OSError as exc:
        if current_fd is not None:
            os.close(current_fd)
        _raise_artifact_open_error(exc)
    except Exception:
        if current_fd is not None:
            os.close(current_fd)
        raise

    assert current_fd is not None
    if not stat.S_ISDIR(root_stat.st_mode):
        os.close(current_fd)
        raise HTTPException(404, "Task workspace is unavailable")
    return current_fd


def _open_anchored_artifact(
    root_fd: int,
    parts: tuple[str, ...],
    filename: str,
) -> OpenedTaskArtifact:
    """Open validated lexical parts beneath root_fd without following symlinks."""

    try:
        current_fd = os.dup(root_fd)
    except OSError as exc:
        _raise_artifact_open_error(exc)
    artifact_fd: int | None = None
    try:
        for component in parts[:-1]:
            next_fd = _open_directory_component(current_fd, component)
            previous_fd = current_fd
            current_fd = next_fd
            os.close(previous_fd)

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
        parts, filename = _lexical_artifact_parts(task, root, reference)
        return _open_anchored_artifact(root_fd, parts, filename)
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
