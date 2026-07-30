from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.services.deployment_start_guard import (
    DeploymentTaskStartBlocked,
    assess_deployment_start,
    deployment_task_start_fence,
)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    path.chmod(0o600)


def test_no_deployment_record_allows_normal_start(tmp_path):
    # The default legacy status path /tmp/ccm-update-status-{port}.json is
    # machine-global; point it into tmp so a real deployment's leftover on
    # the host cannot leak into this hermetic "no record" scenario.
    result = assess_deployment_start(
        tmp_path,
        port=8000,
        running_commit="a" * 40,
        status_file=tmp_path / "ccm-update-status.json",
    )
    assert result.action == "normal"


def test_matching_controlled_start_skips_duplicate_mutations(tmp_path):
    commit = "b" * 40
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "starting",
            "port": 8000,
            "handoff": True,
            "owner_token": "token",
            "expected_commit": commit,
            "terminal_intent": "completed",
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit=commit
    )

    assert result.action == "skip_mutations"
    assert "只允许加载应用" in result.reason


def test_incomplete_controlled_start_is_maintenance_only(tmp_path):
    commit = "b" * 40
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "starting",
            "port": 8000,
            "handoff": True,
            "owner_token": "token",
            "expected_commit": commit,
            "terminal_intent": "rolled_back",
            "deployment_incomplete": True,
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit=commit
    )

    assert result.action == "skip_mutations"
    assert result.maintenance_only is True
    assert "修复入口" in result.reason


def test_controlled_start_rejects_wrong_commit(tmp_path):
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "starting",
            "port": 8000,
            "handoff": True,
            "owner_token": "token",
            "expected_commit": "b" * 40,
            "terminal_intent": "completed",
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="a" * 40
    )

    assert result.action == "block"
    assert "安全启动点" in result.reason


def test_authoritative_start_requires_owner_token(tmp_path):
    commit = "b" * 40
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "starting",
            "port": 8000,
            "handoff": True,
            "expected_commit": commit,
            "terminal_intent": "completed",
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit=commit
    )

    assert result.action == "block"


def test_active_deployment_for_other_port_blocks_shared_checkout(tmp_path):
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "migrating",
            "port": 8002,
            "handoff": True,
            "owner_token": "token",
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="a" * 40
    )

    assert result.action == "block"
    assert "owner_port=8002" in result.reason


def test_incomplete_terminal_allows_repair_start_without_mutations(tmp_path):
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "failed",
            "port": 8000,
            "deployment_incomplete": True,
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="b" * 40
    )

    assert result.action == "skip_mutations"
    assert "禁止" in result.reason


def test_corrupt_lease_fails_closed(tmp_path):
    lease = tmp_path / "backups" / "deployment-lease.json"
    lease.parent.mkdir(parents=True)
    lease.write_text("{not-json")
    lease.chmod(0o600)

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="a" * 40
    )

    assert result.action == "block"


def test_legacy_starting_status_can_boot_new_guarded_release(tmp_path):
    commit = "b" * 40
    status = tmp_path / "legacy-status.json"
    _write(
        status,
        {
            "status": "starting",
            "port": 8000,
            "expected_commit": commit,
            "terminal_intent": "completed",
        },
    )

    result = assess_deployment_start(
        tmp_path,
        port=8000,
        running_commit=commit,
        status_file=status,
    )

    assert result.action == "skip_mutations"


def test_legacy_restarting_status_enters_maintenance_only(tmp_path):
    """An old updater may pull the new guard before its direct restart."""

    commit = "b" * 40
    status = tmp_path / "legacy-status.json"
    _write(
        status,
        {
            "status": "restarting",
            "port": 8000,
            "old_commit": "a" * 40,
            "new_commit": commit,
            "deployment_incomplete": True,
        },
    )

    result = assess_deployment_start(
        tmp_path,
        port=8000,
        running_commit=commit,
        status_file=status,
    )

    assert result.action == "skip_mutations"
    assert result.maintenance_only is True
    assert "旧版更新器" in result.reason


def test_legacy_restarting_status_rejects_wrong_commit(tmp_path):
    status = tmp_path / "legacy-status.json"
    _write(
        status,
        {
            "status": "restarting",
            "port": 8000,
            "old_commit": "a" * 40,
            "new_commit": "b" * 40,
        },
    )

    result = assess_deployment_start(
        tmp_path,
        port=8000,
        running_commit="c" * 40,
        status_file=status,
    )

    assert result.action == "block"


def test_tokened_restarting_status_cannot_use_legacy_recovery(tmp_path):
    commit = "b" * 40
    status = tmp_path / "status.json"
    _write(
        status,
        {
            "status": "restarting",
            "port": 8000,
            "new_commit": commit,
            "owner_token": "unverified-worker",
        },
    )

    result = assess_deployment_start(
        tmp_path,
        port=8000,
        running_commit=commit,
        status_file=status,
    )

    assert result.action == "block"


def test_complete_terminal_lease_ignores_stale_active_tmp_status(tmp_path):
    commit = "b" * 40
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "completed",
            "port": 8000,
            "expected_commit": commit,
            "deployment_incomplete": False,
        },
    )
    status = tmp_path / "stale.json"
    _write(status, {"status": "migrating", "port": 8000})

    result = assess_deployment_start(
        tmp_path,
        port=8000,
        running_commit=commit,
        status_file=status,
    )

    assert result.action == "normal"


def test_dead_active_lease_enters_maintenance_only(tmp_path):
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "backing_up",
            "port": 8000,
            "owner_token": "token",
            "owner_pid": 2_147_483_646,
            "owner_pid_start": "123",
            "handoff": False,
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="a" * 40
    )

    assert result.action == "skip_mutations"
    assert result.maintenance_only is True
    assert "异常退出" in result.reason


def test_active_lease_with_unknown_owner_identity_stays_blocked(tmp_path):
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "running",
            "port": 8000,
            "owner_token": "token",
            "owner_pid": 2_147_483_646,
            "handoff": False,
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="a" * 40
    )

    assert result.action == "block"


def test_dead_owner_with_unexpired_provisional_handoff_stays_blocked(
    tmp_path,
):
    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "running",
            "port": 8000,
            "owner_token": "token",
            "owner_pid": 2_147_483_646,
            "owner_pid_start": "123",
            "handoff": True,
            "handoff_provisional": True,
            "handoff_ack_deadline": (
                datetime.now(timezone.utc) + timedelta(minutes=1)
            ).isoformat(),
        },
    )

    result = assess_deployment_start(
        tmp_path, port=8000, running_commit="a" * 40
    )

    assert result.action == "block"


def test_task_start_fence_blocks_active_or_incomplete_deployment(tmp_path):
    lease = tmp_path / "backups" / "deployment-lease.json"
    _write(
        lease,
        {
            "status": "running",
            "port": 8000,
            "owner_token": "token",
        },
    )

    with pytest.raises(DeploymentTaskStartBlocked):
        with deployment_task_start_fence(tmp_path):
            pass

    _write(
        lease,
        {
            "status": "failed",
            "deployment_incomplete": True,
        },
    )
    with pytest.raises(DeploymentTaskStartBlocked):
        with deployment_task_start_fence(tmp_path):
            pass


def test_task_start_fence_allows_missing_or_clean_terminal_lease(tmp_path):
    with deployment_task_start_fence(tmp_path):
        pass

    _write(
        tmp_path / "backups" / "deployment-lease.json",
        {
            "status": "completed",
            "deployment_incomplete": False,
        },
    )
    with deployment_task_start_fence(tmp_path):
        pass
