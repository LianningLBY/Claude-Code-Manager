from __future__ import annotations

import pytest

from backend.models.task import Task
from backend.services.test_harness_targets import (
    UNTRUSTED_GIT_TARGETS_REASON,
    TestHarnessTargetError as HarnessTargetError,
    TestHarnessTargetManager,
)


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
    manager = TestHarnessTargetManager()

    with pytest.raises(HarnessTargetError, match="isolated sandbox") as exc:
        await manager.prepare(
            run_id="a" * 32,
            task=task,
            project=None,
            kind=kind,
            target=target,
        )

    assert str(exc.value) == UNTRUSTED_GIT_TARGETS_REASON
