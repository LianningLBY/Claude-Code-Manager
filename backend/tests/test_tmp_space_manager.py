import asyncio
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.tmp_space_manager import TmpSpaceManager


class DiskUsageSequence:
    def __init__(self, *used_values: int):
        self._values = list(used_values)
        self.calls = 0

    def __call__(self, _path: str | os.PathLike[str]):
        index = min(self.calls, len(self._values) - 1)
        self.calls += 1
        used = self._values[index]
        return SimpleNamespace(total=100, used=used, free=100 - used)


def _make_old(path: Path, *, seconds: float = 3600) -> None:
    timestamp = time.time() - seconds
    os.utime(path, (timestamp, timestamp), follow_symlinks=False)


def _manager(
    tmp_path: Path,
    disk_usage,
    *,
    min_age_seconds: float = 60,
    lock_wait_seconds: float = 0,
) -> TmpSpaceManager:
    return TmpSpaceManager(
        root=tmp_path,
        enabled=True,
        trigger_ratio=0.80,
        min_age_seconds=min_age_seconds,
        interval_seconds=60,
        lock_wait_seconds=lock_wait_seconds,
        lock_path=tmp_path.parent / f".{tmp_path.name}.cleanup.lock",
        disk_usage_reader=disk_usage,
        inode_usage_reader=lambda _path: 0.1,
    )


@pytest.mark.asyncio
async def test_below_eighty_percent_does_not_scan_or_delete(tmp_path):
    candidate = tmp_path / "ccm_sub_agent_10.log"
    candidate.write_bytes(b"old")
    _make_old(candidate)

    report = await _manager(
        tmp_path,
        DiskUsageSequence(79),
    ).ensure_capacity(reason="test")

    assert report.triggered is False
    assert report.before_usage_ratio == pytest.approx(0.79)
    assert candidate.exists()


@pytest.mark.asyncio
async def test_exactly_eighty_percent_removes_all_stale_allowlisted_artifacts(
    tmp_path,
):
    first = tmp_path / "ccm-skills-10-stale.md"
    first.write_bytes(b"stale monitor output")
    _make_old(first)
    second = tmp_path / "ccm_sub_agent_11.log"
    second.write_bytes(b"more stale output")
    _make_old(second)
    # Even after the first deletion drops usage below the 80% trigger,
    # every other eligible artifact must still be removed.
    usage = DiskUsageSequence(80, 69, 55)

    report = await _manager(tmp_path, usage).ensure_capacity(reason="test")

    assert report.triggered is True
    assert report.removed_count == 2
    assert report.removed_bytes >= len(
        b"stale monitor outputmore stale output"
    )
    assert report.after_usage_ratio == pytest.approx(0.55)
    assert not first.exists()
    assert not second.exists()


@pytest.mark.asyncio
async def test_pressure_cleanup_is_allowlist_age_and_symlink_safe(tmp_path):
    stale = tmp_path / "ccm_sub_agent_20.log"
    stale.write_bytes(b"safe")
    _make_old(stale)

    unknown = tmp_path / "user-important.txt"
    unknown.write_bytes(b"keep")
    _make_old(unknown)

    fixed_mcp = tmp_path / "ccm_mcp_20.json"
    fixed_mcp.write_bytes(b"keep")
    _make_old(fixed_mcp)

    update_state = tmp_path / "ccm-update-status-8003.json"
    update_state.write_bytes(b"keep")
    _make_old(update_state)

    recent = tmp_path / "ccm_sub_agent_21.log"
    recent.write_bytes(b"keep")

    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.write_bytes(b"outside")
    symlink = tmp_path / "ccm_sub_agent_22.log"
    symlink.symlink_to(outside)
    _make_old(symlink)

    report = await _manager(
        tmp_path,
        DiskUsageSequence(80, 80),
    ).ensure_capacity(reason="test")

    assert report.removed_count == 1
    assert not stale.exists()
    assert unknown.exists()
    assert fixed_mcp.exists()
    assert update_state.exists()
    assert recent.exists()
    assert symlink.is_symlink()
    assert outside.read_bytes() == b"outside"


@pytest.mark.asyncio
async def test_host_cleanup_never_recursively_deletes_directories(tmp_path):
    candidate = tmp_path / ".ccm-reap-0123456789abcdef0123456789abcdef"
    candidate.mkdir()
    (candidate / "payload").write_bytes(b"x")
    outside = tmp_path.parent / f"{tmp_path.name}-directory-outside"
    outside.write_bytes(b"outside")
    (candidate / "link").symlink_to(outside)
    _make_old(candidate)

    report = await _manager(
        tmp_path,
        DiskUsageSequence(80, 80),
    ).ensure_capacity(reason="test")

    assert report.removed_count == 0
    assert candidate.is_dir()
    assert outside.read_bytes() == b"outside"


@pytest.mark.asyncio
async def test_candidate_refreshed_after_scan_is_revalidated(
    tmp_path,
    monkeypatch,
):
    candidate = tmp_path / "ccm_sub_agent_31.log"
    candidate.write_bytes(b"became active")
    _make_old(candidate)
    manager = _manager(tmp_path, DiskUsageSequence(80, 80))
    original_scan = manager._scan_candidates

    def scan_then_refresh(**kwargs):
        result = original_scan(**kwargs)
        os.utime(candidate, None)
        return result

    monkeypatch.setattr(manager, "_scan_candidates", scan_then_refresh)
    report = await manager.ensure_capacity(reason="test")

    assert report.removed_count == 0
    assert candidate.exists()


@pytest.mark.asyncio
async def test_stale_session_migration_directory_is_excluded(tmp_path):
    candidate = tmp_path / "ccm-codex-sess-stale"
    candidate.mkdir()
    rollout = candidate / "rollout.jsonl"
    rollout.write_bytes(b"x" * 16)
    _make_old(rollout)
    _make_old(candidate)

    report = await _manager(
        tmp_path,
        DiskUsageSequence(80, 80),
    ).ensure_capacity(reason="test")

    assert report.removed_count == 0
    assert candidate.exists()


@pytest.mark.asyncio
async def test_inode_pressure_also_triggers_cleanup(tmp_path):
    candidate = tmp_path / "ccm_sub_agent_42.log"
    candidate.write_bytes(b"stale")
    _make_old(candidate)
    manager = _manager(tmp_path, DiskUsageSequence(10, 10))
    manager._inode_usage_reader = lambda _path: 0.80

    report = await manager.ensure_capacity(reason="test")

    assert report.triggered is True
    assert report.before_inode_ratio == pytest.approx(0.80)
    assert not candidate.exists()


def test_default_disk_usage_uses_space_available_to_service_uid(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_frsize=1,
            f_bsize=1,
            f_blocks=100,
            f_bavail=15,
        ),
    )

    usage = TmpSpaceManager._read_available_disk_usage(tmp_path)

    assert usage.total == 100
    assert usage.used == 85
    assert usage.free == 15


@pytest.mark.asyncio
async def test_concurrent_checks_share_one_cleanup_pass(tmp_path, monkeypatch):
    manager = _manager(tmp_path, DiskUsageSequence(80))
    calls = 0
    original = manager._check_and_cleanup

    def counted(reason: str):
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return original(reason)

    monkeypatch.setattr(manager, "_check_and_cleanup", counted)

    first, second = await asyncio.gather(
        manager.ensure_capacity(reason="one"),
        manager.ensure_capacity(reason="two"),
    )

    assert calls == 1
    assert first.reason == "one"
    assert second.reason == "two"

    await manager.ensure_capacity(reason="later")
    assert calls == 2


@pytest.mark.asyncio
async def test_completed_below_threshold_result_is_not_cached(tmp_path):
    usage = DiskUsageSequence(79, 100)
    manager = _manager(tmp_path, usage)

    first = await manager.ensure_capacity(reason="first")
    second = await manager.ensure_capacity(reason="second")

    assert first.triggered is False
    assert second.triggered is True
    assert second.before_usage_ratio == pytest.approx(1.0)
    assert usage.calls == 2


@pytest.mark.asyncio
async def test_cross_process_lock_busy_skips_this_periodic_pass(tmp_path):
    holder = _manager(tmp_path, DiskUsageSequence(10))
    contender = _manager(
        tmp_path,
        DiskUsageSequence(10),
        lock_wait_seconds=0.05,
    )
    assert holder.lock_path == contender.lock_path

    with holder._cross_process_lock() as acquired:
        assert acquired is True
        report = await contender.ensure_capacity(reason="periodic-test")

    assert report.skipped_reason == "cross_process_cleanup_busy"


@pytest.mark.asyncio
async def test_cancellation_waits_for_inflight_cleanup_thread(
    tmp_path,
    monkeypatch,
):
    manager = _manager(tmp_path, DiskUsageSequence(10))
    started = threading.Event()
    release = threading.Event()
    original = manager._check_and_cleanup

    def blocking_check(reason: str):
        started.set()
        assert release.wait(timeout=2)
        return original(reason)

    monkeypatch.setattr(manager, "_check_and_cleanup", blocking_check)
    task = asyncio.create_task(manager.ensure_capacity(reason="test"))
    assert await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0.01)
    assert task.done() is False

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_periodic_loop_is_cancellation_safe(tmp_path, monkeypatch):
    manager = _manager(tmp_path, DiskUsageSequence(10))
    manager.interval_seconds = 0.01
    called = asyncio.Event()

    async def checked(*, reason: str):
        called.set()

    monkeypatch.setattr(manager, "ensure_capacity", checked)
    task = manager.start_periodic()
    await asyncio.wait_for(called.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_disabled_manager_does_not_start_periodic_task(tmp_path):
    manager = TmpSpaceManager(
        root=tmp_path,
        enabled=False,
        lock_path=tmp_path.parent / ".disabled-cleanup.lock",
    )

    assert manager.start_periodic() is None
