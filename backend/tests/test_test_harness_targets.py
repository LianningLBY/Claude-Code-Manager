from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from backend.models.project import Project
from backend.models.task import Task
from backend.services.test_harness_targets import TestHarnessTargetManager


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, Path, str, str]:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-b", "main")
    _git(source, "config", "user.email", "harness@example.invalid")
    _git(source, "config", "user.name", "Harness Test")
    (source / "app").mkdir()
    (source / "app" / "version.txt").write_text("main", encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "main")
    main_sha = _git(source, "rev-parse", "HEAD")
    _git(source, "switch", "-c", "feature")
    (source / "app" / "version.txt").write_text("feature", encoding="utf-8")
    _git(source, "commit", "-am", "feature")
    feature_sha = _git(source, "rev-parse", "HEAD")
    _git(source, "switch", "main")

    remote = tmp_path / "origin.git"
    subprocess.run(
        ["git", "clone", "--bare", str(source), str(remote)],
        check=True,
        capture_output=True,
    )
    _git(remote, "update-ref", "refs/pull/99/head", feature_sha)
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "clone", str(remote), str(checkout)],
        check=True,
        capture_output=True,
    )
    assert _git(checkout, "rev-parse", "HEAD") == main_sha
    return checkout, remote, main_sha, feature_sha


@pytest.mark.asyncio
async def test_pr_target_uses_exact_detached_worktree_and_preserves_checkout(tmp_path):
    checkout, _remote, main_sha, feature_sha = _repository(tmp_path)
    task = Task(
        id=17,
        title="Review PR",
        target_repo=str(checkout / "app"),
        last_cwd=str(checkout / "app"),
    )
    project = Project(id=8, name="demo", local_path=str(checkout))
    manager = TestHarnessTargetManager()

    prepared = await manager.prepare(
        run_id="a" * 32,
        task=task,
        project=project,
        kind="pull_request",
        target={"remote": "origin", "pr_number": 99},
    )
    temp_root = prepared.temp_root
    assert prepared.git_head == feature_sha
    assert prepared.workspace.joinpath("version.txt").read_text(encoding="utf-8") == "feature"
    assert _git(checkout, "rev-parse", "HEAD") == main_sha
    assert _git(checkout, "status", "--short") == ""
    assert "prepared_worktree" not in prepared.public_spec

    await manager.cleanup(prepared)

    assert temp_root is not None and not temp_root.exists()
    assert _git(checkout, "rev-parse", "HEAD") == main_sha
    missing_ref = subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", "refs/ccm/test-harness/" + "a" * 32],
        cwd=checkout,
        check=False,
    )
    assert missing_ref.returncode == 1


@pytest.mark.asyncio
async def test_git_ref_rejects_unsafe_revision_without_running_git(tmp_path):
    checkout, _remote, _main_sha, _feature_sha = _repository(tmp_path)
    task = Task(id=18, title="Unsafe ref", target_repo=str(checkout), last_cwd=str(checkout))
    manager = TestHarnessTargetManager()

    with pytest.raises(Exception, match="Git ref is invalid|Git ref is unsafe"):
        await manager.prepare(
            run_id="b" * 32,
            task=task,
            project=None,
            kind="git_ref",
            target={"remote": "origin", "ref": "--upload-pack=evil"},
        )
