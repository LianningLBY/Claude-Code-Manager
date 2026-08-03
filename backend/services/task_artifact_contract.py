"""Shared constants and lexical roots for the Task artifact contract."""

from __future__ import annotations

import os
import stat
from pathlib import Path


TASK_ARTIFACT_SCOPE_VERSION = 1
TASK_ARTIFACT_POLICY_TAG = "<ccm_task_artifact_policy>"
TASK_ARTIFACT_LINK_TITLE = "ccm-task-artifact"
MANAGED_ARTIFACT_ROOT = (".claude-manager", "artifacts")


def configured_workspace_root(raw_root: str) -> Path:
    """Return a normalized absolute POSIX root without resolving symlinks."""

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


def workspace_root_is_secure_directory(root: Path) -> bool:
    """Confirm every existing root component without following symlinks."""

    if (
        not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_DIRECTORY")
        or os.open not in getattr(os, "supports_dir_fd", set())
    ):
        return False
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
    flags |= getattr(os, "O_CLOEXEC", 0)
    current_fd: int | None = None
    try:
        current_fd = os.open(os.path.sep, flags)
        for component in root.parts[1:]:
            next_fd = os.open(component, flags, dir_fd=current_fd)
            previous_fd = current_fd
            current_fd = next_fd
            os.close(previous_fd)
        return stat.S_ISDIR(os.fstat(current_fd).st_mode)
    except OSError:
        return False
    finally:
        if current_fd is not None:
            os.close(current_fd)
