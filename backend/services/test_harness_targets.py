"""Fail-closed admission for untrusted Git Test Harness targets.

PR/ref preparation used to create a detached worktree and then execute its
Preview commands on the Manager host.  A worktree is version isolation, not an
execution sandbox, so that implementation is intentionally absent.  A future
sandbox runtime can replace this gate; it must never fall back to host execution.
"""

from __future__ import annotations

from typing import Any

from backend.models.project import Project
from backend.models.task import Task
from backend.services.workspace_review import WorkspaceReviewError


UNTRUSTED_GIT_TARGETS_AVAILABLE = False
UNTRUSTED_GIT_TARGETS_REASON = (
    "PR and Git ref browser tests require an isolated sandbox; host execution is disabled"
)


class TestHarnessTargetError(WorkspaceReviewError):
    """An untrusted Git target cannot be admitted safely."""


class TestHarnessTargetManager:
    """Reject untrusted Git targets until a real sandbox runtime is installed."""

    async def prepare(
        self,
        *,
        run_id: str,
        task: Task,
        project: Project | None,
        kind: str,
        target: dict[str, Any],
    ) -> None:
        _ = (run_id, task, project, target)
        if kind in {"pull_request", "git_ref"}:
            raise TestHarnessTargetError(UNTRUSTED_GIT_TARGETS_REASON)
        raise TestHarnessTargetError(
            f"target kind {kind!r} does not use the untrusted Git sandbox gate"
        )


test_harness_target_manager = TestHarnessTargetManager()
