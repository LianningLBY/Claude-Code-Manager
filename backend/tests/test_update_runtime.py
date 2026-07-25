import fcntl
import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

import backend.services.update_runtime as update_runtime_module
from backend.services.update_runtime import (
    TrustedUpdateRuntime,
    UpdateRuntimeError,
)


def _write_script(root: Path) -> Path:
    script = root / "scripts" / "update_migrate.sh"
    script.parent.mkdir(parents=True)
    script.write_text(
        "#!/bin/bash\nCCM_UPDATE_PROTOCOL_VERSION=2\nexit 0\n",
        encoding="utf-8",
    )
    script.chmod(0o700)
    return script


def _runtime(
    tmp_path: Path,
    *,
    root: Path | None = None,
    legacy_root: Path | None = None,
) -> TrustedUpdateRuntime:
    return TrustedUpdateRuntime(
        port=8003,
        running_commit="a" * 40,
        root=root or tmp_path / "runtime",
        legacy_root=(
            legacy_root
            if legacy_root is not None
            else tmp_path / "legacy"
        ),
    )


def _managed_directory(
    root: Path,
    *,
    pid: int,
    pid_start: int,
    boot_id: str,
    owner: bool = True,
    malformed_owner: bool = False,
    extra_file: bool = False,
) -> Path:
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    directory = (
        root
        / f"ccm-update-runtime-v1-8003-{pid}-{pid_start}-"
        f"{'1' * 32}"
    )
    directory.mkdir(mode=0o700)
    script = directory / "update_migrate.sh"
    script.write_bytes(b"stale")
    script.chmod(0o500)
    metadata = directory.lstat()
    if owner:
        record = (
            {"broken": True}
            if malformed_owner
            else {
                "version": 1,
                "pid": pid,
                "pid_start": pid_start,
                "boot_id": boot_id,
                "uid": os.getuid(),
                "port": 8003,
                "running_commit": "old",
                "script_sha256": hashlib.sha256(b"stale").hexdigest(),
                "directory_device": metadata.st_dev,
                "directory_inode": metadata.st_ino,
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        )
        marker = directory / "owner.json"
        marker.write_text(json.dumps(record), encoding="utf-8")
        marker.chmod(0o600)
    if extra_file:
        extra = directory / "keep-me"
        extra.write_bytes(b"unknown")
        extra.chmod(0o600)
    return directory


def _legacy_directory(
    root: Path,
    *,
    pid: int,
    extra_file: bool = False,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    directory = root / f"ccm-update-runtime-8003-{pid}-abcdefgh"
    directory.mkdir(mode=0o700)
    script = directory / "update_migrate.sh"
    script.write_bytes(b"legacy")
    script.chmod(0o500)
    if extra_file:
        extra = directory / "unknown"
        extra.write_bytes(b"keep")
        extra.chmod(0o600)
    return directory


def test_snapshot_uses_private_runtime_and_close_is_reentrant(tmp_path):
    source = _write_script(tmp_path)
    runtime = _runtime(tmp_path)

    first = runtime.capture(source)

    assert first.parent.parent == (tmp_path / "runtime").resolve()
    assert "/tmp/ccm-update-runtime-" not in str(first)
    assert first.read_bytes() == source.read_bytes()
    assert stat.S_IMODE(first.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(first.stat().st_mode) == 0o500
    assert stat.S_IMODE((first.parent / "owner.json").stat().st_mode) == 0o600

    runtime.close()
    assert not first.parent.exists()
    runtime.close()

    second = runtime.ensure_snapshot()
    assert second != first
    assert second.read_bytes() == source.read_bytes()
    runtime.close()


def test_startup_reaps_only_provably_dead_managed_owner(tmp_path):
    source = _write_script(tmp_path)
    root = tmp_path / "runtime"
    runtime = _runtime(tmp_path, root=root)
    dead = _managed_directory(
        root,
        pid=999_999_999,
        pid_start=1,
        boot_id=runtime.boot_id,
    )
    live = _managed_directory(
        root,
        pid=runtime.pid,
        pid_start=runtime.pid_start,
        boot_id=runtime.boot_id,
    )

    runtime.capture(source)

    assert not dead.exists()
    assert live.exists()
    runtime.close()


def test_pid_reuse_and_previous_boot_are_provably_dead(tmp_path):
    source = _write_script(tmp_path)
    root = tmp_path / "runtime"
    runtime = _runtime(tmp_path, root=root)
    reused = _managed_directory(
        root,
        pid=runtime.pid,
        pid_start=runtime.pid_start + 1,
        boot_id=runtime.boot_id,
        owner=False,
    )
    previous_boot = _managed_directory(
        root,
        pid=runtime.pid,
        pid_start=runtime.pid_start,
        boot_id="0" * 32,
    )
    # Avoid the deterministic nonce collision in this test.
    previous_boot.rename(previous_boot.with_name(previous_boot.name[:-1] + "2"))
    previous_boot = previous_boot.with_name(previous_boot.name[:-1] + "2")

    runtime.capture(source)

    assert not reused.exists()
    assert not previous_boot.exists()
    runtime.close()


def test_unknown_or_malformed_owner_is_preserved(tmp_path, monkeypatch):
    source = _write_script(tmp_path)
    root = tmp_path / "runtime"
    runtime = _runtime(tmp_path, root=root)
    unknown = _managed_directory(
        root,
        pid=999_999_998,
        pid_start=1,
        boot_id=runtime.boot_id,
        owner=False,
    )
    malformed = _managed_directory(
        root,
        pid=999_999_997,
        pid_start=1,
        boot_id=runtime.boot_id,
        malformed_owner=True,
    )
    malformed.rename(malformed.with_name(malformed.name[:-1] + "2"))
    malformed = malformed.with_name(malformed.name[:-1] + "2")
    real_reader = update_runtime_module._read_process_identity

    def unreadable(pid: int):
        if pid == 999_999_998:
            raise UpdateRuntimeError("proc unavailable")
        return real_reader(pid)

    monkeypatch.setattr(
        update_runtime_module, "_read_process_identity", unreadable
    )
    runtime.capture(source)

    assert unknown.exists()
    assert malformed.exists()
    runtime.close()


def test_unknown_file_or_symlink_prevents_recursive_cleanup(tmp_path):
    source = _write_script(tmp_path)
    root = tmp_path / "runtime"
    runtime = _runtime(tmp_path, root=root)
    extra = _managed_directory(
        root,
        pid=999_999_996,
        pid_start=1,
        boot_id=runtime.boot_id,
        extra_file=True,
    )
    outside = tmp_path / "outside"
    outside.write_bytes(b"important")
    symlink = (
        root
        / "ccm-update-runtime-v1-8003-999999995-1-"
        f"{'2' * 32}"
    )
    symlink.symlink_to(outside)

    runtime.capture(source)

    assert extra.exists()
    assert symlink.is_symlink()
    assert outside.read_bytes() == b"important"
    runtime.close()


def test_legacy_tmp_cleanup_keeps_live_and_unsafe_directories(tmp_path):
    source = _write_script(tmp_path)
    legacy = tmp_path / "legacy"
    dead = _legacy_directory(legacy, pid=999_999_994)
    live = _legacy_directory(legacy, pid=os.getpid())
    live.rename(live.with_name(live.name[:-1] + "i"))
    live = live.with_name(live.name[:-1] + "i")
    unsafe = _legacy_directory(
        legacy,
        pid=999_999_993,
        extra_file=True,
    )
    unsafe.rename(unsafe.with_name(unsafe.name[:-1] + "j"))
    unsafe = unsafe.with_name(unsafe.name[:-1] + "j")
    runtime = _runtime(tmp_path, legacy_root=legacy)

    runtime.capture(source)

    assert not dead.exists()
    assert live.exists()
    assert unsafe.exists()
    runtime.close()


def test_snapshot_tampering_fails_closed(tmp_path):
    source = _write_script(tmp_path)
    runtime = _runtime(tmp_path)
    snapshot = runtime.capture(source)
    snapshot.chmod(0o700)
    snapshot.write_bytes(b"x" * snapshot.stat().st_size)
    snapshot.chmod(0o500)

    with pytest.raises(UpdateRuntimeError, match="内容校验失败"):
        runtime.read_verified_snapshot()
    with pytest.raises(UpdateRuntimeError):
        runtime.close()
    assert snapshot.parent.exists()


def test_close_refuses_unknown_content(tmp_path):
    source = _write_script(tmp_path)
    runtime = _runtime(tmp_path)
    snapshot = runtime.capture(source)
    extra = snapshot.parent / "unknown"
    extra.write_bytes(b"keep")
    extra.chmod(0o600)

    with pytest.raises(UpdateRuntimeError, match="未知文件"):
        runtime.close()

    assert snapshot.exists()
    assert extra.exists()


def test_symlink_runtime_root_is_rejected(tmp_path):
    source = _write_script(tmp_path)
    outside = tmp_path / "outside-root"
    outside.mkdir()
    root = tmp_path / "runtime-link"
    root.symlink_to(outside, target_is_directory=True)
    runtime = _runtime(tmp_path, root=root)

    with pytest.raises(UpdateRuntimeError, match="符号链接"):
        runtime.capture(source)


def test_existing_runtime_root_permissions_are_not_mutated(tmp_path):
    source = _write_script(tmp_path)
    root = tmp_path / "shared-directory"
    root.mkdir(mode=0o755)
    runtime = _runtime(tmp_path, root=root)

    with pytest.raises(UpdateRuntimeError, match="权限必须为 0700"):
        runtime.capture(source)

    assert stat.S_IMODE(root.stat().st_mode) == 0o755


def test_runtime_root_lock_wait_is_bounded(tmp_path, monkeypatch):
    source = _write_script(tmp_path)
    root = tmp_path / "runtime"
    root.mkdir(mode=0o700)
    lock_path = root / ".lifecycle.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    runtime = _runtime(tmp_path, root=root)
    monkeypatch.setattr(update_runtime_module, "_LOCK_TIMEOUT_SECONDS", 0)
    try:
        with pytest.raises(UpdateRuntimeError, match="生命周期锁超时"):
            runtime.capture(source)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
