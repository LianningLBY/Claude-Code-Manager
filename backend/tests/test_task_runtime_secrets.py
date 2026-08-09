import json
import os
import stat

import pytest

from backend.config import settings
from backend.services.task_runtime_secrets import (
    TaskRuntimeSecretError,
    create_private_output,
    remove_private_file,
    remove_private_scope,
    write_private_json,
)


def test_private_runtime_json_has_private_directory_and_file_modes(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime-secrets"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))

    target = write_private_json(
        "task",
        41,
        "mcp.json",
        {"token": "scoped"},
    )

    assert json.loads(target.read_text(encoding="utf-8")) == {
        "token": "scoped"
    }
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(target.stat().st_mode) == 0o600

    write_private_json("task", 41, "mcp.json", {"token": "rotated"})
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "token": "rotated"
    }
    assert not list(target.parent.glob("*.tmp"))


def test_private_runtime_root_rejects_symlink_ancestor(tmp_path, monkeypatch):
    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(linked / "runtime"),
    )

    with pytest.raises(TaskRuntimeSecretError, match="symlink ancestor"):
        write_private_json("task", 5, "mcp.json", {"ok": True})


def test_private_runtime_cleanup_refuses_unexpected_entries(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime-secrets"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    target = write_private_json("task", 7, "mcp.json", {"ok": True})
    unexpected = target.parent / "nested"
    unexpected.mkdir()

    with pytest.raises(TaskRuntimeSecretError, match="Unexpected entry"):
        remove_private_scope("task", 7)

    unexpected.rmdir()
    remove_private_scope("task", 7)
    assert not target.parent.exists()


def test_private_runtime_single_file_cleanup_preserves_siblings(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime-secrets"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    first = write_private_json("monitor", 8, "mcp-1.json", {"generation": 1})
    second = write_private_json("monitor", 8, "mcp-2.json", {"generation": 2})

    remove_private_file("monitor", 8, "mcp-1.json")

    assert not first.exists()
    assert second.exists()
    assert stat.S_IMODE(os.lstat(second).st_mode) == 0o600


def test_private_output_is_random_exclusive_private_and_inode_safe(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    output = create_private_output("monitor", 91, "agent-output")
    path = output.path

    assert path.parent == root / "monitor-91"
    assert path.name.startswith("agent-output-")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    output._stream.write(b"model output")
    output.close()
    assert not path.exists()


@pytest.mark.parametrize("collision_kind", ["symlink", "hardlink", "regular"])
def test_private_output_collision_never_opens_attacker_path(
    tmp_path,
    monkeypatch,
    collision_kind,
):
    root = tmp_path / "runtime"
    scope = root / "sub-agent-92"
    scope.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    scope.chmod(0o700)
    victim = tmp_path / "victim"
    victim.write_bytes(b"do-not-truncate")
    collision = scope / "agent-output-fixed.log"
    if collision_kind == "symlink":
        collision.symlink_to(victim)
    elif collision_kind == "hardlink":
        os.link(victim, collision)
    else:
        collision.write_bytes(b"preexisting")
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    monkeypatch.setattr(
        "backend.services.task_runtime_secrets.secrets.token_hex",
        lambda _size: "fixed",
    )

    with pytest.raises(FileExistsError):
        create_private_output("sub-agent", 92, "agent-output")

    assert victim.read_bytes() == b"do-not-truncate"


def test_private_output_cleanup_refuses_replaced_path(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    output = create_private_output("monitor", 93, "agent-output")
    path = output.path
    victim = tmp_path / "victim"
    victim.write_bytes(b"unchanged")
    path.unlink()
    path.symlink_to(victim)

    with pytest.raises(TaskRuntimeSecretError, match="changed before cleanup"):
        output.close()

    assert victim.read_bytes() == b"unchanged"
