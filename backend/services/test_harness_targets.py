"""Resolve immutable local Git targets for the frontend test harness."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.models.project import Project
from backend.models.task import Task
from backend.services.workspace_review import WorkspaceReviewError, _task_workspace


_SHA_RE = re.compile(r"^[0-9a-fA-F]{40,64}$")
_REMOTE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,119}$")
_SAFE_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,299}$")
_MAX_GIT_OUTPUT = 4 * 1024 * 1024


class TestHarnessTargetError(WorkspaceReviewError):
    """A requested PR/ref cannot be prepared as an immutable local target."""


@dataclass(slots=True)
class PreparedHarnessTarget:
    kind: str
    workspace: Path
    worktree_root: Path
    repo_root: Path
    git_head: str
    public_spec: dict[str, Any]
    temp_root: Path | None = None
    temp_ref: str | None = None


class TestHarnessTargetManager:
    """Prepare detached worktrees without switching or mutating the developer tree."""

    async def prepare(
        self,
        *,
        run_id: str,
        task: Task,
        project: Project | None,
        kind: str,
        target: dict[str, Any],
    ) -> PreparedHarnessTarget:
        source = _task_workspace(task, project)
        repo_root_raw = await self._git(source, ["rev-parse", "--show-toplevel"])
        repo_root = self._safe_existing_directory(Path(repo_root_raw.strip()))
        if kind == "current_workspace":
            head = await self._resolve_commit(repo_root, "HEAD")
            return PreparedHarnessTarget(
                kind=kind,
                workspace=source,
                worktree_root=repo_root,
                repo_root=repo_root,
                git_head=head,
                public_spec={"kind": kind, "resolved_git_head": head},
            )
        if kind not in {"pull_request", "git_ref"}:
            raise TestHarnessTargetError(f"target kind {kind!r} does not use a Git worktree")

        remote = self._validate_remote(target.get("remote", "origin"))
        remotes = set((await self._git(repo_root, ["remote"])).splitlines())
        if remote not in remotes:
            raise TestHarnessTargetError(f"Git remote {remote!r} is not configured")

        temp_ref: str | None = None
        if kind == "pull_request":
            number = target.get("pr_number")
            if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
                raise TestHarnessTargetError("pull request number must be positive")
            temp_ref = f"refs/ccm/test-harness/{run_id}"
            await self._git(
                repo_root,
                [
                    "fetch",
                    "--no-tags",
                    remote,
                    f"+refs/pull/{number}/head:{temp_ref}",
                ],
                timeout=180,
            )
            head = await self._resolve_commit(repo_root, temp_ref)
            requested: dict[str, Any] = {
                "kind": kind,
                "remote": remote,
                "pr_number": number,
                "resolved_git_head": head,
            }
        else:
            ref = self._validate_ref(target.get("ref"))
            try:
                head = await self._resolve_commit(repo_root, ref)
            except TestHarnessTargetError:
                if not bool(target.get("fetch", False)):
                    raise
                temp_ref = f"refs/ccm/test-harness/{run_id}"
                source_ref = ref if ref.startswith("refs/") else f"refs/heads/{ref}"
                await self._git(
                    repo_root,
                    ["fetch", "--no-tags", remote, f"+{source_ref}:{temp_ref}"],
                    timeout=180,
                )
                head = await self._resolve_commit(repo_root, temp_ref)
            requested = {
                "kind": kind,
                "remote": remote,
                "ref": ref,
                "fetch": bool(target.get("fetch", False)),
                "resolved_git_head": head,
            }

        temp_root = Path(tempfile.mkdtemp(prefix=f"ccm-test-harness-target-{run_id[:8]}-"))
        temp_root.chmod(0o700)
        worktree_root = temp_root / "workspace"
        try:
            await self._git(
                repo_root,
                ["worktree", "add", "--detach", str(worktree_root), head],
                timeout=120,
            )
            resolved_worktree = self._safe_existing_directory(worktree_root)
            actual_head = await self._resolve_commit(resolved_worktree, "HEAD")
            if actual_head != head:
                raise TestHarnessTargetError("detached worktree did not resolve to the captured commit")
            source_relative = source.relative_to(repo_root)
            resolved_workspace = self._safe_existing_directory(
                resolved_worktree / source_relative
            )
        except BaseException:
            await asyncio.shield(
                self._cleanup_partial(
                    repo_root=repo_root,
                    worktree_root=worktree_root,
                    temp_root=temp_root,
                    temp_ref=temp_ref,
                )
            )
            raise
        return PreparedHarnessTarget(
            kind=kind,
            workspace=resolved_workspace,
            worktree_root=resolved_worktree,
            repo_root=repo_root,
            git_head=head,
            public_spec=requested,
            temp_root=temp_root,
            temp_ref=temp_ref,
        )

    async def cleanup(self, target: PreparedHarnessTarget) -> None:
        if target.temp_root is None:
            return
        await self._cleanup_partial(
            repo_root=target.repo_root,
            worktree_root=target.worktree_root,
            temp_root=target.temp_root,
            temp_ref=target.temp_ref,
        )

    async def _cleanup_partial(
        self,
        *,
        repo_root: Path,
        worktree_root: Path,
        temp_root: Path,
        temp_ref: str | None,
    ) -> None:
        if worktree_root.exists():
            try:
                await self._git(
                    repo_root,
                    ["worktree", "remove", "--force", str(worktree_root)],
                    timeout=60,
                )
            except TestHarnessTargetError:
                # Prune only repository-owned metadata after an exact remove
                # attempt. The filesystem target is still guarded below.
                try:
                    await self._git(repo_root, ["worktree", "prune"], timeout=30)
                except TestHarnessTargetError:
                    pass
        if temp_ref is not None:
            try:
                await self._git(repo_root, ["update-ref", "-d", temp_ref], timeout=30)
            except TestHarnessTargetError:
                pass
        self._remove_private_temp_root(temp_root)

    async def _resolve_commit(self, repo: Path, revision: str) -> str:
        try:
            raw = await self._git(repo, ["rev-parse", "--verify", f"{revision}^{{commit}}"])
        except TestHarnessTargetError as exc:
            raise TestHarnessTargetError(f"Git revision {revision!r} is not an available commit") from exc
        head = raw.strip().lower()
        if _SHA_RE.fullmatch(head) is None:
            raise TestHarnessTargetError("Git returned an invalid commit id")
        return head

    @staticmethod
    def _validate_remote(value: object) -> str:
        if not isinstance(value, str) or _REMOTE_RE.fullmatch(value) is None:
            raise TestHarnessTargetError("Git remote name is invalid")
        if value.startswith("-") or ".." in value or "@{" in value:
            raise TestHarnessTargetError("Git remote name is unsafe")
        return value

    @staticmethod
    def _validate_ref(value: object) -> str:
        if not isinstance(value, str) or _SAFE_REF_RE.fullmatch(value) is None:
            raise TestHarnessTargetError("Git ref is invalid")
        if value.startswith("-") or ".." in value or "@{" in value or value.endswith(("/", ".")):
            raise TestHarnessTargetError("Git ref is unsafe")
        return value

    @staticmethod
    def _safe_existing_directory(path: Path) -> Path:
        if not path.is_absolute():
            raise TestHarnessTargetError("Git workspace path must be absolute")
        candidate = Path(os.path.abspath(path))
        cursor = Path(candidate.anchor)
        try:
            for part in candidate.parts[1:]:
                cursor /= part
                info = cursor.lstat()
                if stat.S_ISLNK(info.st_mode):
                    raise TestHarnessTargetError("Git workspace contains a symbolic link")
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise TestHarnessTargetError("Git workspace cannot be safely resolved") from exc
        if not resolved.is_dir():
            raise TestHarnessTargetError("Git workspace is not a directory")
        return resolved

    @staticmethod
    def _remove_private_temp_root(temp_root: Path) -> None:
        try:
            actual = temp_root.resolve(strict=True)
            system_temp = Path(tempfile.gettempdir()).resolve(strict=True)
            actual.relative_to(system_temp)
            info = actual.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or actual.is_symlink()
                or not actual.name.startswith("ccm-test-harness-target-")
            ):
                raise TestHarnessTargetError("refusing to remove an unsafe harness target path")
            shutil.rmtree(actual)
        except FileNotFoundError:
            return

    @staticmethod
    async def _git(
        cwd: Path,
        args: list[str],
        *,
        timeout: float = 60,
    ) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                "git",
                *args,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=(os.name == "posix"),
            )
        except OSError as exc:
            raise TestHarnessTargetError("could not start Git") from exc
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except BaseException:
            if process.returncode is None:
                try:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:  # pragma: no cover - Windows fallback
                        process.kill()
                except ProcessLookupError:
                    pass
            await process.wait()
            raise
        if len(stdout) + len(stderr) > _MAX_GIT_OUTPUT:
            raise TestHarnessTargetError("Git output exceeded the harness safety limit")
        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace")[-2000:].strip()
            raise TestHarnessTargetError(
                "Git target operation failed" + (f": {detail}" if detail else "")
            )
        try:
            return stdout.decode("utf-8", errors="strict").strip()
        except UnicodeDecodeError as exc:
            raise TestHarnessTargetError("Git returned non-UTF-8 output") from exc


test_harness_target_manager = TestHarnessTargetManager()
