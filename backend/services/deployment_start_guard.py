"""Fail-closed startup decisions for CCM's self-deployment handoff.

This module intentionally uses only the Python standard library. systemd's
``ExecStartPre`` runs it before ``uv sync``, so importing application settings
or third-party packages here would defeat the guard.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal


StartAction = Literal["normal", "skip_mutations", "block"]

ACTIVE_DEPLOYMENT_STATUSES = {
    "claimed",
    "running",
    "backing_up",
    "restarting",
    "starting",
    "stopping",
    "migrating",
    "rolling_back",
}
TERMINAL_DEPLOYMENT_STATUSES = {
    "completed",
    "rolled_back",
    "failed",
    "rollback_failed",
}


class DeploymentTaskStartBlocked(RuntimeError):
    """A repository deployment fence rejected a new task claim."""


@dataclass(frozen=True)
class StartDecision:
    action: StartAction
    reason: str
    source: str = ""
    maintenance_only: bool = False

    @property
    def skip_mutations(self) -> bool:
        return self.action == "skip_mutations"

    @property
    def blocked(self) -> bool:
        return self.action == "block"


def _read_record(path: Path) -> tuple[dict[str, Any] | None, str]:
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None, ""
    except OSError as exc:
        return None, f"无法读取部署状态 {path}: {exc}"
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            return None, f"部署状态不是安全的普通文件: {path}"
        with os.fdopen(descriptor, encoding="utf-8") as stream:
            descriptor = -1
            raw = stream.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            return None, f"部署状态文件异常过大: {path}"
    except OSError as exc:
        return None, f"无法读取部署状态 {path}: {exc}"
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"部署状态不是有效 JSON ({path}): {exc}"
    if not isinstance(payload, dict):
        return None, f"部署状态必须是 JSON object: {path}"
    return payload, ""


def _record_port(record: dict[str, Any]) -> str:
    value = record.get("port")
    return "" if value is None else str(value)


def _expected_commit(record: dict[str, Any]) -> str:
    explicit = str(record.get("expected_commit") or "")
    if explicit:
        return explicit
    is_rollback = (
        record.get("terminal_intent") == "rolled_back"
        or record.get("handoff_mode") in {"rollback", "rollback_code"}
        or record.get("status") == "rolled_back"
    )
    if is_rollback:
        return str(record.get("old_commit") or "")
    return str(
        record.get("new_commit")
        or record.get("target_commit")
        or ""
    )


def _terminal_is_incomplete(record: dict[str, Any]) -> bool:
    return bool(
        record.get("deployment_incomplete")
        or record.get("rollback_incomplete")
        or record.get("status") == "rollback_failed"
    )


def _pid_identity_state(
    record: dict[str, Any],
    *,
    pid_field: str,
    identity_field: str,
) -> Literal["live", "dead", "unknown"]:
    """Prove a lease process live/dead without treating ambiguity as death."""

    try:
        pid = int(record.get(pid_field, 0))
    except (TypeError, ValueError):
        return "unknown"
    identity = str(record.get(identity_field) or "")
    if pid <= 0 or not identity:
        return "unknown"
    try:
        suffix = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1]
        current_identity = suffix.split()[19]
    except FileNotFoundError:
        return "dead"
    except (OSError, IndexError, ValueError):
        return "unknown"
    return "live" if current_identity == identity else "dead"


def _provisional_handoff_expired(
    record: dict[str, Any],
) -> bool | None:
    if not record.get("handoff_provisional"):
        return None
    try:
        deadline = datetime.fromisoformat(
            str(record.get("handoff_ack_deadline") or "")
        )
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None
    return datetime.now(timezone.utc) > deadline


def _active_lease_is_provably_abandoned(
    record: dict[str, Any],
) -> bool:
    if _pid_identity_state(
        record,
        pid_field="owner_pid",
        identity_field="owner_pid_start",
    ) != "dead":
        return False
    if not record.get("handoff"):
        return True
    if record.get("handoff_provisional"):
        return _provisional_handoff_expired(record) is True
    return (
        _pid_identity_state(
            record,
            pid_field="handoff_pid",
            identity_field="handoff_pid_start",
        )
        == "dead"
    )


def _evaluate_record(
    record: dict[str, Any],
    *,
    source: Path,
    port: int,
    running_commit: str,
    authoritative_lease: bool,
) -> StartDecision:
    status = str(record.get("status") or "")
    record_port = _record_port(record)
    if status in ACTIVE_DEPLOYMENT_STATUSES:
        if record_port and record_port != str(port):
            return StartDecision(
                "block",
                "同一代码目录正由另一个 CCM 端口执行部署 "
                f"(owner_port={record_port}, status={status})",
                str(source),
            )

        expected = _expected_commit(record)
        controlled_start = (
            status == "starting"
            and not bool(record.get("rollback_incomplete"))
            and str(record.get("terminal_intent") or "")
            in {"completed", "rolled_back"}
            and bool(expected)
            and bool(running_commit)
            and expected == running_commit
        )
        if authoritative_lease:
            controlled_start = controlled_start and bool(
                record.get("handoff")
                and record.get("owner_token")
            )
        if controlled_start:
            maintenance_only = bool(record.get("deployment_incomplete"))
            return StartDecision(
                "skip_mutations",
                (
                    "受控部署正在验证未完整恢复的版本；本次启动仅提供"
                    "健康检查和部署修复入口"
                    if maintenance_only
                    else
                    "受控部署已完成所有代码、依赖和数据库变更；"
                    "本次启动只允许加载应用并由外部脚本验证健康状态"
                ),
                str(source),
                maintenance_only=maintenance_only,
            )
        if (
            authoritative_lease
            and _active_lease_is_provably_abandoned(record)
        ):
            return StartDecision(
                "skip_mutations",
                "检测到部署进程已异常退出；以 maintenance-only 模式"
                "启动，等待管理员检查并执行修复",
                str(source),
                maintenance_only=True,
            )
        return StartDecision(
            "block",
            "检测到尚未到达安全启动点的部署操作 "
            f"(status={status}, expected={expected or 'unknown'}, "
            f"running={running_commit or 'unknown'})",
            str(source),
        )

    if status in TERMINAL_DEPLOYMENT_STATUSES:
        if _terminal_is_incomplete(record):
            return StartDecision(
                "skip_mutations",
                "上一次部署未完整结束；允许应用尝试启动以提供修复入口，"
                "但禁止 pre-start/init_db 自动修改依赖或数据库",
                str(source),
                maintenance_only=True,
            )
        return StartDecision("normal", "没有活动中的部署操作", str(source))

    if status:
        return StartDecision(
            "block",
            f"无法识别部署状态 {status!r}，为避免绕过备份而拒绝启动",
            str(source),
        )
    return StartDecision("normal", "部署状态为空", str(source))


def assess_deployment_start(
    project_dir: str | os.PathLike[str],
    *,
    port: int,
    running_commit: str,
    status_file: str | os.PathLike[str] | None = None,
) -> StartDecision:
    """Return whether startup may mutate artifacts, must skip, or must block."""

    project = Path(project_dir).resolve()
    lease_path = project / "backups" / "deployment-lease.json"
    lease, lease_error = _read_record(lease_path)
    if lease_error:
        return StartDecision("block", lease_error, str(lease_path))
    if lease is not None:
        # The durable repository lease is authoritative over per-port /tmp
        # status. A stale temporary file must never overturn its terminal state.
        return _evaluate_record(
            lease,
            source=lease_path,
            port=port,
            running_commit=running_commit,
            authoritative_lease=True,
        )

    status_path = (
        Path(status_file)
        if status_file is not None
        else Path(f"/tmp/ccm-update-status-{port}.json")
    )
    journal_path = (
        project / "backups" / f"deployment-status-{port}.json"
    )
    candidates: list[tuple[int, Path, dict[str, Any]]] = []
    errors: list[str] = []
    for path in (status_path, journal_path):
        record, error = _read_record(path)
        if error:
            errors.append(error)
            continue
        if record is None:
            continue
        try:
            modified = path.stat().st_mtime_ns
        except OSError:
            modified = 0
        candidates.append((modified, path, record))
    if errors:
        return StartDecision("block", "; ".join(errors))
    if not candidates:
        return StartDecision("normal", "没有部署状态记录")
    _, source, record = max(candidates, key=lambda item: item[0])
    return _evaluate_record(
        record,
        source=source,
        port=port,
        running_commit=running_commit,
        authoritative_lease=False,
    )


@contextmanager
def deployment_task_start_fence(
    project_dir: str | os.PathLike[str],
):
    """Serialize a task's persisted claim with repo-wide update admission.

    The shared lock remains held until the caller has committed the Task's
    active state. An updater takes the same file exclusively before publishing
    its active lease, so either the task wins and is visible to the updater's
    blocker query, or the lease wins and this task is rejected.
    """

    project = Path(project_dir).resolve()
    backups = project / "backups"
    backups.mkdir(parents=True, exist_ok=True)
    if backups.resolve() != backups.absolute():
        raise DeploymentTaskStartBlocked(
            "部署锁目录包含符号链接，拒绝启动新任务"
        )
    lock_path = backups / "deployment-lease.lock"
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise DeploymentTaskStartBlocked(
            f"无法打开部署锁，拒绝启动新任务: {exc}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            raise DeploymentTaskStartBlocked(
                "部署锁文件不安全，拒绝启动新任务"
            )
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_SH)

        lease_path = backups / "deployment-lease.json"
        lease, error = _read_record(lease_path)
        if error:
            raise DeploymentTaskStartBlocked(error)
        if lease:
            status = str(lease.get("status") or "")
            incomplete = _terminal_is_incomplete(lease)
            if status in ACTIVE_DEPLOYMENT_STATUSES or incomplete:
                raise DeploymentTaskStartBlocked(
                    "仓库正在部署或等待修复，暂停启动新任务"
                )
            if status not in TERMINAL_DEPLOYMENT_STATUSES:
                raise DeploymentTaskStartBlocked(
                    f"无法识别部署状态 {status!r}，暂停启动新任务"
                )
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--status-file")
    args = parser.parse_args()

    decision = assess_deployment_start(
        args.project,
        port=args.port,
        running_commit=args.commit,
        status_file=args.status_file,
    )
    print(f"{decision.action}: {decision.reason}")
    if decision.action == "normal":
        return 0
    if decision.action == "skip_mutations":
        return 10
    return 20


if __name__ == "__main__":
    raise SystemExit(main())
