"""Fail-closed admission for untrusted Git Test Harness targets.

PR/ref preparation used to create a detached worktree and then execute its
Preview commands on the Manager host.  A worktree is version isolation, not an
execution sandbox, so that implementation is intentionally absent.  A future
sandbox runtime can replace this gate; it must never fall back to host execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.models.project import Project
from backend.models.task import Task
from backend.services.workspace_review import WorkspaceReviewError
from backend.services.test_harness_sandbox import (
    SandboxCapability,
    TestHarnessSandboxRuntime,
    test_harness_sandbox_runtime,
)


_TARGET_PIPELINE_AVAILABLE = False
_TARGET_PIPELINE_REASON = (
    "PR/ref sandbox runtime is not connected to the Test Harness target pipeline"
)


@dataclass(frozen=True, slots=True)
class UntrustedGitTargetCapability:
    available: bool
    reason: str | None
    sandbox: SandboxCapability

    def as_dict(self) -> dict[str, object]:
        return {
            "available": self.available,
            "reason": self.reason,
            "sandbox": self.sandbox.as_dict(),
        }


async def untrusted_git_target_capability(
    runtime: TestHarnessSandboxRuntime | None = None,
    *,
    force: bool = False,
) -> UntrustedGitTargetCapability:
    sandbox = await (runtime or test_harness_sandbox_runtime).probe(force=force)
    if not sandbox.available:
        return UntrustedGitTargetCapability(False, sandbox.reason, sandbox)
    if not _TARGET_PIPELINE_AVAILABLE:
        return UntrustedGitTargetCapability(False, _TARGET_PIPELINE_REASON, sandbox)
    return UntrustedGitTargetCapability(True, None, sandbox)


class TestHarnessTargetError(WorkspaceReviewError):
    """An untrusted Git target cannot be admitted safely."""


class TestHarnessTargetManager:
    """Reject untrusted Git targets until a real sandbox runtime is installed."""

    def __init__(self, runtime: TestHarnessSandboxRuntime | None = None) -> None:
        self.runtime = runtime or test_harness_sandbox_runtime

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
            capability = await untrusted_git_target_capability(self.runtime)
            raise TestHarnessTargetError(
                capability.reason or "PR/ref sandbox target is unavailable"
            )
        raise TestHarnessTargetError(
            f"target kind {kind!r} does not use the untrusted Git sandbox gate"
        )


test_harness_target_manager = TestHarnessTargetManager()
