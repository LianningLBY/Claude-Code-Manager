"""Lifecycle management for the process-bound trusted update helper.

The trusted helper must outlive mutations of the Git checkout, but it is not a
generic temporary artifact.  Keep it in a private CCM runtime root, identify
its exact owning process, and reap only owners that are provably dead.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

_RUNTIME_VERSION = 1
_OWNER_FILE = "owner.json"
_SCRIPT_FILE = "update_migrate.sh"
_TEMP_SCRIPT_FILE = ".update_migrate.sh.tmp"
_LOCK_FILE = ".lifecycle.lock"
_LOCK_TIMEOUT_SECONDS = 10.0
_MANAGED_FILES = frozenset({_OWNER_FILE, _SCRIPT_FILE, _TEMP_SCRIPT_FILE})
_RUNTIME_NAME_RE = re.compile(
    r"^ccm-update-runtime-v1-"
    r"(?P<port>[0-9]+)-"
    r"(?P<pid>[1-9][0-9]*)-"
    r"(?P<start>[1-9][0-9]*)-"
    r"(?P<nonce>[0-9a-f]{32})$"
)
_LEGACY_NAME_RE = re.compile(
    r"^ccm-update-runtime-"
    r"(?P<port>[0-9]+)-"
    r"(?P<pid>[1-9][0-9]*)-"
    r"(?P<nonce>[a-z0-9_]{8})$"
)
_BOOT_ID_RE = re.compile(r"^[0-9a-f-]{32,64}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class UpdateRuntimeError(RuntimeError):
    """The trusted update runtime could not be verified or managed safely."""


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    digest: str


@dataclass(frozen=True)
class _DirectoryIdentity:
    device: int
    inode: int


def _read_process_identity(pid: int) -> tuple[str, int] | None:
    """Return Linux process state/start ticks, or None when PID is absent."""

    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise UpdateRuntimeError(
            f"无法确认更新快照所属进程 {pid}: {exc}"
        ) from exc

    separator = raw.rfind(") ")
    if separator < 0:
        raise UpdateRuntimeError(f"无法解析更新快照所属进程 {pid}")
    fields = raw[separator + 2 :].split()
    if len(fields) <= 19:
        raise UpdateRuntimeError(f"更新快照所属进程 {pid} 信息不完整")
    try:
        return fields[0], int(fields[19])
    except ValueError as exc:
        raise UpdateRuntimeError(
            f"更新快照所属进程 {pid} 身份无效"
        ) from exc


def _read_boot_id() -> str:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="utf-8"
        ).strip()
    except OSError as exc:
        raise UpdateRuntimeError(f"无法读取系统 boot ID: {exc}") from exc
    if not re.fullmatch(r"[0-9a-fA-F-]{32,64}", value):
        raise UpdateRuntimeError("系统 boot ID 格式无效")
    return value.lower()


class TrustedUpdateRuntime:
    """Own one immutable helper snapshot for the current Python process."""

    def __init__(
        self,
        *,
        port: int,
        running_commit: str,
        root: str | os.PathLike[str] | None = None,
        legacy_root: str | os.PathLike[str] | None = "/tmp",
    ) -> None:
        if int(port) < 0:
            raise ValueError("port must be non-negative")
        configured_root = (
            Path(root)
            if root is not None
            else Path(
                os.environ.get(
                    "CCM_UPDATE_RUNTIME_DIR",
                    str(Path.home() / ".cache" / "ccm" / "update-runtime"),
                )
            )
        )
        configured_root = configured_root.expanduser()
        if not configured_root.is_absolute():
            raise UpdateRuntimeError("CCM 更新运行目录必须是绝对路径")

        self.port = int(port)
        self.running_commit = str(running_commit or "")
        self.root = configured_root
        self.legacy_root = (
            Path(legacy_root).expanduser() if legacy_root is not None else None
        )
        if self.legacy_root is not None and not self.legacy_root.is_absolute():
            raise UpdateRuntimeError("旧版更新快照根目录必须是绝对路径")
        self.pid = os.getpid()
        self.uid = os.getuid()
        process_identity = _read_process_identity(self.pid)
        if process_identity is None or process_identity[0] == "Z":
            raise UpdateRuntimeError("当前 CCM 进程身份不可用")
        self.pid_start = process_identity[1]
        self.boot_id = _read_boot_id()
        self._lock = threading.RLock()
        self._script_bytes: bytes | None = None
        self._script_digest = ""
        self._runtime_dir: Path | None = None
        self._runtime_identity: _DirectoryIdentity | None = None
        self._snapshot_identity: _FileIdentity | None = None

    @property
    def snapshot_path(self) -> Path | None:
        with self._lock:
            if self._runtime_dir is None:
                return None
            return self._runtime_dir / _SCRIPT_FILE

    @property
    def has_captured_script(self) -> bool:
        with self._lock:
            return self._script_bytes is not None

    def capture(self, source: Path) -> Path:
        """Capture stable source bytes once, then materialize the snapshot."""

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        source_fd = os.open(source, flags)
        try:
            before = os.fstat(source_fd)
            if not stat.S_ISREG(before.st_mode):
                raise UpdateRuntimeError(f"更新脚本不是普通文件: {source}")
            chunks: list[bytes] = []
            digest = hashlib.sha256()
            while True:
                chunk = os.read(source_fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
                digest.update(chunk)
            after = os.fstat(source_fd)
            try:
                path_after = source.lstat()
            except OSError as exc:
                raise UpdateRuntimeError(
                    f"更新脚本在创建可信快照时不可用: {exc}"
                ) from exc
            identity_before = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            )
            identity_after = (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            )
            path_identity = (
                path_after.st_dev,
                path_after.st_ino,
                path_after.st_size,
                path_after.st_mtime_ns,
            )
            if (
                identity_before != identity_after
                or identity_after != path_identity
            ):
                raise UpdateRuntimeError("更新脚本在创建可信快照时发生变化")
            payload = b"".join(chunks)
            if len(payload) != before.st_size:
                raise UpdateRuntimeError("更新脚本可信快照长度不一致")
        finally:
            os.close(source_fd)

        with self._lock:
            if self._script_bytes is not None:
                if digest.hexdigest() != self._script_digest:
                    raise UpdateRuntimeError("拒绝替换当前进程已捕获的更新脚本")
            else:
                self._script_bytes = payload
                self._script_digest = digest.hexdigest()
            return self._ensure_snapshot_locked()

    def ensure_snapshot(self) -> Path:
        """Recreate a deleted runtime directory from immutable captured bytes."""

        with self._lock:
            if self._script_bytes is None:
                raise UpdateRuntimeError("尚未捕获当前进程的更新脚本")
            return self._ensure_snapshot_locked()

    def read_verified_snapshot(self) -> bytes:
        """Read through O_NOFOLLOW and verify identity plus SHA-256."""

        with self._lock:
            self._ensure_snapshot_locked()
            return self._read_verified_snapshot_locked()

    def copy_snapshot_to(self, destination: Path, *, mode: int = 0o700) -> None:
        """Copy verified bytes to an exclusive per-operation destination."""

        with self._lock:
            payload = self._read_verified_snapshot_locked()
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(destination, flags, 0o600)
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(fd, view)
                    if written <= 0:
                        raise UpdateRuntimeError(
                            "无法完整写入更新 worker 脚本副本"
                    )
                    view = view[written:]
                os.fchmod(fd, mode)
                os.fsync(fd)
            except BaseException:
                try:
                    os.close(fd)
                finally:
                    destination.unlink(missing_ok=True)
                raise
            else:
                os.close(fd)

    def close(self) -> None:
        """Remove this process's exact snapshot; safe and idempotent."""

        with self._lock:
            runtime_dir = self._runtime_dir
            if runtime_dir is None:
                return
            try:
                with self._root_lock():
                    try:
                        metadata = runtime_dir.lstat()
                    except FileNotFoundError:
                        self._forget_materialized_snapshot()
                        return
                    expected = self._runtime_identity
                    if (
                        expected is None
                        or not stat.S_ISDIR(metadata.st_mode)
                        or stat.S_ISLNK(metadata.st_mode)
                        or metadata.st_uid != self.uid
                        or (metadata.st_dev, metadata.st_ino)
                        != (expected.device, expected.inode)
                    ):
                        raise UpdateRuntimeError(
                            "当前更新快照目录身份已变化，拒绝清理"
                        )
                    record = self._read_owner_record(runtime_dir)
                    if not self._record_matches(
                        record, runtime_dir.name, metadata
                    ):
                        raise UpdateRuntimeError(
                            "当前更新快照 owner 标记异常，拒绝清理"
                        )
                    self._read_verified_snapshot_locked()
                    self._remove_flat_runtime_dir(
                        runtime_dir,
                        expected_directory=expected,
                        expected_snapshot=self._snapshot_identity,
                    )
            finally:
                if not runtime_dir.exists():
                    self._forget_materialized_snapshot()

    def _ensure_snapshot_locked(self) -> Path:
        if self._script_bytes is None:
            raise UpdateRuntimeError("尚未捕获当前进程的更新脚本")
        if self._runtime_dir is not None:
            try:
                self._read_verified_snapshot_locked()
            except FileNotFoundError:
                if self._runtime_dir.exists():
                    raise UpdateRuntimeError("更新脚本快照不完整")
                self._forget_materialized_snapshot()
            else:
                return self._runtime_dir / _SCRIPT_FILE

        with self._root_lock():
            self._cleanup_stale_locked()
            self._cleanup_legacy_stale_locked()
            runtime_dir = self._create_runtime_dir_locked()
            try:
                directory_metadata = runtime_dir.lstat()
                directory_identity = _DirectoryIdentity(
                    directory_metadata.st_dev,
                    directory_metadata.st_ino,
                )
                record = {
                    "version": _RUNTIME_VERSION,
                    "pid": self.pid,
                    "pid_start": self.pid_start,
                    "boot_id": self.boot_id,
                    "uid": self.uid,
                    "port": self.port,
                    "running_commit": self.running_commit,
                    "script_sha256": self._script_digest,
                    "directory_device": directory_identity.device,
                    "directory_inode": directory_identity.inode,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                self._write_owner_record(runtime_dir, record)
                temporary = runtime_dir / _TEMP_SCRIPT_FILE
                self._write_script(temporary, self._script_bytes, mode=0o500)
                snapshot = runtime_dir / _SCRIPT_FILE
                os.replace(temporary, snapshot)
                snapshot_metadata = snapshot.lstat()
                snapshot_identity = _FileIdentity(
                    snapshot_metadata.st_dev,
                    snapshot_metadata.st_ino,
                    snapshot_metadata.st_size,
                    self._script_digest,
                )
                self._fsync_directory(runtime_dir)
            except BaseException:
                self._remove_incomplete_runtime_dir(runtime_dir)
                raise

            self._runtime_dir = runtime_dir
            self._runtime_identity = directory_identity
            self._snapshot_identity = snapshot_identity
            return snapshot

    def _read_verified_snapshot_locked(self) -> bytes:
        runtime_dir = self._runtime_dir
        expected_dir = self._runtime_identity
        expected_file = self._snapshot_identity
        if runtime_dir is None or expected_dir is None or expected_file is None:
            raise FileNotFoundError("更新脚本快照尚未创建")
        root = self._ensure_private_root()
        if runtime_dir.parent != root:
            raise UpdateRuntimeError("更新脚本快照脱离专用运行目录")
        directory_metadata = runtime_dir.lstat()
        if (
            not stat.S_ISDIR(directory_metadata.st_mode)
            or stat.S_ISLNK(directory_metadata.st_mode)
            or directory_metadata.st_uid != self.uid
            or (directory_metadata.st_dev, directory_metadata.st_ino)
            != (expected_dir.device, expected_dir.inode)
        ):
            raise UpdateRuntimeError("更新脚本快照目录身份异常")
        record = self._read_owner_record(runtime_dir)
        if (
            not self._record_matches(
                record, runtime_dir.name, directory_metadata
            )
            or int(record.get("pid")) != self.pid
            or int(record.get("pid_start")) != self.pid_start
            or str(record.get("boot_id") or "") != self.boot_id
            or str(record.get("script_sha256") or "")
            != expected_file.digest
        ):
            raise UpdateRuntimeError("更新脚本快照 owner 身份异常")
        snapshot = runtime_dir / _SCRIPT_FILE
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(snapshot, flags)
        try:
            before = os.fstat(fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != self.uid
                or before.st_nlink != 1
                or stat.S_IMODE(before.st_mode) != 0o500
                or (before.st_dev, before.st_ino, before.st_size)
                != (expected_file.device, expected_file.inode, expected_file.size)
            ):
                raise UpdateRuntimeError("更新脚本快照文件身份异常")
            chunks: list[bytes] = []
            digest = hashlib.sha256()
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
                digest.update(chunk)
            after = os.fstat(fd)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise UpdateRuntimeError("更新脚本快照读取期间发生变化")
            if digest.hexdigest() != expected_file.digest:
                raise UpdateRuntimeError("更新脚本快照内容校验失败")
            return b"".join(chunks)
        finally:
            os.close(fd)

    def _ensure_private_root(self) -> Path:
        path = self.root
        if path.is_symlink():
            raise UpdateRuntimeError(f"拒绝符号链接更新运行目录: {path}")
        created = False
        try:
            path.mkdir(parents=True, mode=0o700)
            created = True
        except FileExistsError:
            pass
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.uid
        ):
            raise UpdateRuntimeError(f"更新运行目录身份异常: {path}")
        if stat.S_IMODE(metadata.st_mode) != 0o700:
            if not created:
                raise UpdateRuntimeError(
                    f"既有更新运行目录权限必须为 0700: {path}"
                )
            os.chmod(path, 0o700)
            metadata_after = path.lstat()
            if (
                metadata_after.st_dev,
                metadata_after.st_ino,
                stat.S_IMODE(metadata_after.st_mode),
            ) != (metadata.st_dev, metadata.st_ino, 0o700):
                raise UpdateRuntimeError(f"无法保护更新运行目录: {path}")
        resolved = path.resolve(strict=True)
        self.root = resolved
        return resolved

    @contextmanager
    def _root_lock(self) -> Iterator[None]:
        root = self._ensure_private_root()
        lock_path = root / _LOCK_FILE
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(lock_path, flags, 0o600)
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.uid
                or metadata.st_nlink != 1
                or metadata.st_mode & 0o077
            ):
                raise UpdateRuntimeError("更新运行目录生命周期锁不安全")
            deadline = time.monotonic() + _LOCK_TIMEOUT_SECONDS
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise UpdateRuntimeError(
                            "等待更新运行目录生命周期锁超时"
                        ) from exc
                    time.sleep(0.05)
            yield
        finally:
            os.close(fd)

    def _create_runtime_dir_locked(self) -> Path:
        root = self._ensure_private_root()
        for _ in range(10):
            name = (
                f"ccm-update-runtime-v1-{self.port}-{self.pid}-"
                f"{self.pid_start}-{uuid.uuid4().hex}"
            )
            runtime_dir = root / name
            try:
                os.mkdir(runtime_dir, 0o700)
            except FileExistsError:
                continue
            metadata = runtime_dir.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.uid
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise UpdateRuntimeError("新建更新快照目录身份异常")
            return runtime_dir
        raise UpdateRuntimeError("无法分配唯一更新快照目录")

    def _write_owner_record(self, runtime_dir: Path, record: dict) -> None:
        destination = runtime_dir / _OWNER_FILE
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(destination, flags, 0o600)
        try:
            payload = (
                json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
            ).encode("utf-8")
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise UpdateRuntimeError("无法写入更新快照 owner 标记")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _write_script(path: Path, payload: bytes, *, mode: int) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise UpdateRuntimeError("无法写入更新脚本可信快照")
                view = view[written:]
            os.fchmod(fd, mode)
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _read_owner_record(self, runtime_dir: Path) -> dict | None:
        path = runtime_dir / _OWNER_FILE
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except FileNotFoundError:
            return None
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.uid
                or metadata.st_nlink != 1
                or metadata.st_mode & 0o077
                or metadata.st_size > 64 * 1024
            ):
                raise UpdateRuntimeError("更新快照 owner 标记身份异常")
            payload = os.read(fd, 64 * 1024 + 1)
        finally:
            os.close(fd)
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UpdateRuntimeError("更新快照 owner 标记损坏") from exc
        if not isinstance(value, dict):
            raise UpdateRuntimeError("更新快照 owner 标记不是 JSON object")
        return value

    def _record_matches(
        self,
        record: dict | None,
        directory_name: str,
        directory_metadata: os.stat_result,
    ) -> bool:
        match = _RUNTIME_NAME_RE.fullmatch(directory_name)
        if record is None or match is None:
            return False
        try:
            boot_id = str(record.get("boot_id") or "")
            script_digest = str(record.get("script_sha256") or "")
            return (
                record.get("version") == _RUNTIME_VERSION
                and int(record.get("pid")) == int(match.group("pid"))
                and int(record.get("pid_start")) == int(match.group("start"))
                and int(record.get("uid")) == self.uid
                and int(record.get("port")) == int(match.group("port"))
                and _BOOT_ID_RE.fullmatch(boot_id) is not None
                and _SHA256_RE.fullmatch(script_digest) is not None
                and int(record.get("directory_device"))
                == directory_metadata.st_dev
                and int(record.get("directory_inode"))
                == directory_metadata.st_ino
            )
        except (TypeError, ValueError):
            return False

    def _owner_state(
        self,
        *,
        pid: int,
        pid_start: int,
        boot_id: str | None,
    ) -> str:
        if boot_id and boot_id != self.boot_id:
            return "dead"
        try:
            process = _read_process_identity(pid)
        except UpdateRuntimeError:
            return "unknown"
        if process is None:
            return "dead"
        state, current_start = process
        if state == "Z" or current_start != pid_start:
            return "dead"
        return "live"

    def _cleanup_stale_locked(self) -> None:
        root = self._ensure_private_root()
        for candidate in root.iterdir():
            match = _RUNTIME_NAME_RE.fullmatch(candidate.name)
            if match is None:
                continue
            try:
                metadata = candidate.lstat()
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or stat.S_ISLNK(metadata.st_mode)
                    or metadata.st_uid != self.uid
                    or metadata.st_mode & 0o077
                ):
                    logger.warning(
                        "保留身份异常的更新快照目录: %s", candidate
                    )
                    continue
                try:
                    record = self._read_owner_record(candidate)
                except UpdateRuntimeError:
                    logger.warning(
                        "保留 owner 标记异常的更新快照目录: %s",
                        candidate,
                    )
                    continue
                if record is not None and not self._record_matches(
                    record, candidate.name, metadata
                ):
                    logger.warning(
                        "保留 owner 身份不匹配的更新快照目录: %s",
                        candidate,
                    )
                    continue
                pid = int(match.group("pid"))
                pid_start = int(match.group("start"))
                boot_id = (
                    str(record.get("boot_id") or "") if record else None
                )
                if self._owner_state(
                    pid=pid,
                    pid_start=pid_start,
                    boot_id=boot_id,
                ) != "dead":
                    continue
                expected = _DirectoryIdentity(
                    metadata.st_dev, metadata.st_ino
                )
                self._remove_flat_runtime_dir(
                    candidate,
                    expected_directory=expected,
                    expected_snapshot=None,
                )
                logger.info("已清理死亡进程的更新快照: %s", candidate)
            except FileNotFoundError:
                continue
            except (OSError, UpdateRuntimeError):
                logger.warning(
                    "无法安全清理旧更新快照，已保留: %s",
                    candidate,
                    exc_info=True,
                )

    def _cleanup_legacy_stale_locked(self) -> None:
        root = self.legacy_root
        if root is None:
            return
        try:
            root_metadata = root.lstat()
        except FileNotFoundError:
            return
        except OSError:
            logger.warning("无法检查旧版更新快照根目录: %s", root)
            return
        if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(
            root_metadata.st_mode
        ):
            logger.warning("旧版更新快照根目录身份异常: %s", root)
            return
        for candidate in root.iterdir():
            match = _LEGACY_NAME_RE.fullmatch(candidate.name)
            if match is None:
                continue
            try:
                metadata = candidate.lstat()
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or stat.S_ISLNK(metadata.st_mode)
                    or metadata.st_uid != self.uid
                    or metadata.st_mode & 0o077
                ):
                    continue
                try:
                    process = _read_process_identity(int(match.group("pid")))
                except UpdateRuntimeError:
                    continue
                # Legacy names have no start tick, so an existing PID can never
                # be proven to belong to a dead owner.
                if process is not None:
                    continue
                self._remove_flat_runtime_dir(
                    candidate,
                    expected_directory=_DirectoryIdentity(
                        metadata.st_dev, metadata.st_ino
                    ),
                    expected_snapshot=None,
                    allow_owner_record=False,
                )
                logger.info("已清理死亡进程的旧版更新快照: %s", candidate)
            except FileNotFoundError:
                continue
            except (OSError, UpdateRuntimeError):
                logger.warning(
                    "无法安全清理旧版更新快照，已保留: %s",
                    candidate,
                    exc_info=True,
                )

    def _remove_flat_runtime_dir(
        self,
        runtime_dir: Path,
        *,
        expected_directory: _DirectoryIdentity,
        expected_snapshot: _FileIdentity | None,
        allow_owner_record: bool = True,
    ) -> None:
        metadata = runtime_dir.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.uid
            or metadata.st_mode & 0o077
            or (metadata.st_dev, metadata.st_ino)
            != (expected_directory.device, expected_directory.inode)
        ):
            raise UpdateRuntimeError("更新快照目录在清理前发生变化")

        children = list(runtime_dir.iterdir())
        allowed = (
            _MANAGED_FILES
            if allow_owner_record
            else frozenset({_SCRIPT_FILE, _TEMP_SCRIPT_FILE})
        )
        if any(child.name not in allowed for child in children):
            raise UpdateRuntimeError("更新快照目录包含未知文件")
        identities: list[tuple[Path, os.stat_result]] = []
        for child in children:
            child_metadata = child.lstat()
            if (
                not stat.S_ISREG(child_metadata.st_mode)
                or stat.S_ISLNK(child_metadata.st_mode)
                or child_metadata.st_uid != self.uid
                or child_metadata.st_dev != metadata.st_dev
                or child_metadata.st_nlink != 1
                or child_metadata.st_mode & 0o022
            ):
                raise UpdateRuntimeError("更新快照目录包含不安全文件")
            if (
                expected_snapshot is not None
                and child.name == _SCRIPT_FILE
                and (
                    child_metadata.st_dev,
                    child_metadata.st_ino,
                    child_metadata.st_size,
                )
                != (
                    expected_snapshot.device,
                    expected_snapshot.inode,
                    expected_snapshot.size,
                )
            ):
                raise UpdateRuntimeError("更新脚本快照在清理前发生变化")
            identities.append((child, child_metadata))

        for child, before in identities:
            current = child.lstat()
            if (
                current.st_dev,
                current.st_ino,
                current.st_mode,
                current.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_mode,
                before.st_nlink,
            ):
                raise UpdateRuntimeError("更新快照文件在清理前被替换")
            child.unlink()
        runtime_dir.rmdir()

    def _remove_incomplete_runtime_dir(self, runtime_dir: Path) -> None:
        try:
            metadata = runtime_dir.lstat()
            self._remove_flat_runtime_dir(
                runtime_dir,
                expected_directory=_DirectoryIdentity(
                    metadata.st_dev, metadata.st_ino
                ),
                expected_snapshot=None,
            )
        except FileNotFoundError:
            return
        except (OSError, UpdateRuntimeError):
            logger.warning(
                "无法安全清理未完成的更新快照目录: %s",
                runtime_dir,
                exc_info=True,
            )

    def _forget_materialized_snapshot(self) -> None:
        self._runtime_dir = None
        self._runtime_identity = None
        self._snapshot_identity = None
