from __future__ import annotations

import pytest

from backend.models.task import Task
from backend.services.test_harness_sandbox import SandboxCapability
from backend.services.test_harness_targets import (
    TestHarnessTargetError as HarnessTargetError,
    TestHarnessTargetManager as HarnessTargetManager,
    untrusted_git_target_capability,
)


class _Runtime:
    def __init__(self, capability: SandboxCapability):
        self.capability = capability

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        _ = force
        return self.capability


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "target"),
    [
        ("pull_request", {"remote": "origin", "pr_number": 99}),
        ("git_ref", {"remote": "origin", "ref": "feature", "fetch": True}),
    ],
)
async def test_untrusted_git_targets_fail_before_workspace_or_git(
    kind,
    target,
):
    task = Task(
        id=17,
        title="Untrusted target",
        target_repo="/path/that/must/not/be/inspected",
        last_cwd="/path/that/must/not/be/inspected",
    )
    manager = HarnessTargetManager(
        _Runtime(
            SandboxCapability(
                available=False,
                backend="docker",
                reason="PR/ref isolated sandbox is unavailable",
            )
        )
    )

    with pytest.raises(HarnessTargetError, match="isolated sandbox") as exc:
        await manager.prepare(
            run_id="a" * 32,
            task=task,
            project=None,
            kind=kind,
            target=target,
        )

    assert str(exc.value) == "PR/ref isolated sandbox is unavailable"


@pytest.mark.asyncio
async def test_ready_runtime_does_not_open_target_before_pipeline_is_connected():
    capability = await untrusted_git_target_capability(
        _Runtime(
            SandboxCapability(
                available=True,
                backend="docker",
                reason=None,
                image="ccm-test-harness-sandbox:local",
                image_id="sha256:" + "a" * 64,
            )
        )
    )

    assert capability.available is False
    assert capability.sandbox.available is True
    assert "not connected" in (capability.reason or "")
