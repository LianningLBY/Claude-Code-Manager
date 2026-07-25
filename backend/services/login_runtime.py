"""Shared browser runtime for Claude and Codex account login flows.

The two pool APIs run in the same CCM process, so they must share one lock and
one Xvfb owner.  Separate CCM deployments can use different displays/ports via
environment variables while a filesystem lock prevents two processes from
starting the same display concurrently.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import secrets
import shutil
import stat
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

logger = logging.getLogger(__name__)

_DISPLAY_RE = re.compile(r"^:(\d+)$")


class LoginRuntimeError(RuntimeError):
    """The headed-browser runtime could not be prepared safely."""


class LoginResourceError(LoginRuntimeError):
    """The host lacks enough memory or disk space to launch another browser."""


@dataclass(frozen=True)
class LoginRuntime:
    display: str
    xauthority: Path
    temp_dir: Path

    def child_environment(
        self,
        *,
        extra: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        env = {
            **os.environ,
            "DISPLAY": self.display,
            "XAUTHORITY": str(self.xauthority),
            "TMPDIR": str(self.temp_dir),
            "TMP": str(self.temp_dir),
            "TEMP": str(self.temp_dir),
            "CCM_LOGIN_TMPDIR": str(self.temp_dir),
        }
        if extra:
            env.update(extra)
        # Fixed CDP ports can be captured by an orphan Chrome.  Login scripts
        # now bind through their unique profile's DevToolsActivePort instead.
        env.pop("CCM_LOGIN_CDP_PORT", None)
        return env


# Claude and Codex pool routes import this exact object.  This closes the
# previous gap where each module serialized only its own login operations.
login_lock = asyncio.Lock()


def _private_directory(path: Path) -> Path:
    path = path.expanduser()
    if path.is_symlink():
        raise LoginRuntimeError(f"Refusing symlink login runtime directory: {path}")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.is_dir():
        raise LoginRuntimeError(f"Login runtime path is not a directory: {path}")
    os.chmod(path, 0o700)
    return path


def _configured_display() -> tuple[str, int]:
    display = os.environ.get("CCM_XVFB_DISPLAY", ":99").strip()
    match = _DISPLAY_RE.fullmatch(display)
    if not match:
        raise LoginRuntimeError(
            f"Invalid CCM_XVFB_DISPLAY={display!r}; expected :<number>",
        )
    return display, int(match.group(1))


def _configured_nonnegative_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise LoginRuntimeError(f"Invalid {name}={raw!r}; expected an integer") from exc
    if value < 0:
        raise LoginRuntimeError(f"Invalid {name}={value}; expected >= 0")
    return value


def _runtime_root() -> Path:
    configured = os.environ.get("CCM_LOGIN_RUNTIME_DIR")
    if configured:
        return _private_directory(Path(configured))
    return _private_directory(Path.home() / ".cache" / "ccm" / "login-runtime")


def login_temp_directory() -> Path:
    configured = os.environ.get("CCM_LOGIN_TMPDIR")
    if configured:
        return _private_directory(Path(configured))
    # Keep browser profiles on the normal disk instead of /tmp, which is often
    # a RAM-backed tmpfs on small cloud instances.
    return _private_directory(Path.home() / ".cache" / "ccm" / "login-tmp")


def _mem_available_bytes(meminfo_path: Path = Path("/proc/meminfo")) -> int | None:
    try:
        for line in meminfo_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def ensure_login_capacity(
    *,
    temp_dir: Path | None = None,
    mem_available_bytes: int | None = None,
) -> None:
    """Fail before Chrome launch when the host is already resource-starved."""

    temp_dir = temp_dir or login_temp_directory()
    min_memory_mb = _configured_nonnegative_int(
        "CCM_LOGIN_MIN_AVAILABLE_MB",
        512,
    )
    min_temp_mb = _configured_nonnegative_int(
        "CCM_LOGIN_MIN_TEMP_FREE_MB",
        512,
    )
    available_memory = (
        _mem_available_bytes()
        if mem_available_bytes is None
        else mem_available_bytes
    )
    temp_free = shutil.disk_usage(temp_dir).free
    load_one = os.getloadavg()[0] if hasattr(os, "getloadavg") else 0.0

    failures: list[str] = []
    if (
        available_memory is not None
        and available_memory < min_memory_mb * 1024 * 1024
    ):
        failures.append(
            f"available memory {available_memory // (1024 * 1024)} MiB "
            f"is below {min_memory_mb} MiB",
        )
    if temp_free < min_temp_mb * 1024 * 1024:
        failures.append(
            f"login temp free space {temp_free // (1024 * 1024)} MiB "
            f"is below {min_temp_mb} MiB",
        )
    if failures:
        raise LoginResourceError(
            "Login browser not started: "
            + "; ".join(failures)
            + f" (load1={load_one:.2f}, temp={temp_dir})",
        )


class XvfbManager:
    """Own or safely reuse one configured Xvfb display."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._proc: subprocess.Popen | None = None
        self._stderr_handle = None

    async def ensure(self) -> LoginRuntime:
        async with self._lock:
            return await asyncio.to_thread(self._ensure_sync)

    def _paths(self, display_number: int) -> tuple[Path, Path, Path, Path]:
        root = _runtime_root()
        stem = f"display-{display_number}"
        return (
            root / f"{stem}.lock",
            root / f"{stem}.auth",
            root / f"{stem}.stderr.log",
            Path(f"/tmp/.X11-unix/X{display_number}"),
        )

    @staticmethod
    def _owner_path(lock_path: Path) -> Path:
        return lock_path.with_name(f"{lock_path.stem}.owner.json")

    @staticmethod
    def _x_lock_path(socket_path: Path, display_number: int) -> Path:
        if socket_path.parent == Path("/tmp/.X11-unix"):
            return Path(f"/tmp/.X{display_number}-lock")
        # Tests and private runtimes may redirect the socket away from /tmp.
        return socket_path.parent / f".X{display_number}-lock"

    @staticmethod
    def _read_process_identity(pid: int) -> tuple[str, int] | None:
        """Return Linux process state/start ticks, or None when PID is gone."""

        try:
            raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise LoginRuntimeError(
                f"Unable to verify Xvfb owner process {pid}: {exc}",
            ) from exc

        # comm is parenthesized and may itself contain spaces or parentheses.
        separator = raw.rfind(") ")
        if separator < 0:
            raise LoginRuntimeError(
                f"Unable to parse Xvfb owner process identity for PID {pid}",
            )
        fields = raw[separator + 2 :].split()
        if len(fields) <= 19:
            raise LoginRuntimeError(
                f"Incomplete Xvfb owner process identity for PID {pid}",
            )
        try:
            return fields[0], int(fields[19])
        except ValueError as exc:
            raise LoginRuntimeError(
                f"Invalid Xvfb owner process identity for PID {pid}",
            ) from exc

    @staticmethod
    def _path_identity(path: Path) -> dict[str, int] | None:
        try:
            info = path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise LoginRuntimeError(
                f"Unable to inspect Xvfb runtime artifact {path}: {exc}",
            ) from exc
        if stat.S_ISLNK(info.st_mode):
            raise LoginRuntimeError(
                f"Refusing symlink Xvfb runtime artifact: {path}",
            )
        return {
            "device": int(info.st_dev),
            "inode": int(info.st_ino),
            "uid": int(info.st_uid),
            "file_type": int(stat.S_IFMT(info.st_mode)),
        }

    @staticmethod
    def _same_path_identity(
        current: dict[str, int] | None,
        recorded: object,
    ) -> bool:
        if current is None or not isinstance(recorded, dict):
            return False
        required = {"device", "inode", "uid", "file_type"}
        if set(recorded) != required:
            return False
        return all(current[key] == recorded.get(key) for key in required)

    @staticmethod
    def _write_owner_record(path: Path, record: dict[str, object]) -> None:
        if path.is_symlink():
            raise LoginRuntimeError(f"Refusing symlink Xvfb owner file: {path}")
        temp_path = path.with_name(
            f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp",
        )
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temp_path, flags, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = -1
                json.dump(record, handle, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            os.chmod(path, 0o600)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _read_owner_record(path: Path) -> dict[str, object] | None:
        try:
            info = path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise LoginRuntimeError(
                f"Unable to inspect Xvfb owner file {path}: {exc}",
            ) from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise LoginRuntimeError(f"Invalid Xvfb owner file: {path}")
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise LoginRuntimeError(
                f"Unsafe Xvfb owner file permissions: {path}",
            )

        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                raise LoginRuntimeError(
                    f"Xvfb owner file changed while opening: {path}",
                )
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                descriptor = -1
                record = json.load(handle)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise LoginRuntimeError(f"Invalid Xvfb owner file {path}: {exc}") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

        if not isinstance(record, dict):
            raise LoginRuntimeError(f"Invalid Xvfb owner record: {path}")
        return record

    def _record_owned_display(
        self,
        *,
        display: str,
        proc: subprocess.Popen,
        owner_path: Path,
        socket_path: Path,
        x_lock_path: Path,
    ) -> None:
        process_identity = self._read_process_identity(proc.pid)
        if process_identity is None:
            raise LoginRuntimeError(
                f"Xvfb {display} exited before ownership could be recorded",
            )
        state, start_time_ticks = process_identity
        if state == "Z":
            raise LoginRuntimeError(
                f"Xvfb {display} became a zombie before ownership was recorded",
            )
        socket_identity = self._path_identity(socket_path)
        if socket_identity is None or socket_identity["file_type"] != stat.S_IFSOCK:
            raise LoginRuntimeError(
                f"Xvfb {display} became ready without its Unix socket",
            )
        self._write_owner_record(
            owner_path,
            {
                "version": 1,
                "display": display,
                "pid": int(proc.pid),
                "start_time_ticks": start_time_ticks,
                "socket": socket_identity,
                "x_lock": self._path_identity(x_lock_path),
            },
        )

    def _recover_stale_owned_display(
        self,
        *,
        display: str,
        owner_path: Path,
        socket_path: Path,
        x_lock_path: Path,
    ) -> bool:
        """Remove artifacts only when they still belong to a dead CCM Xvfb."""

        record = self._read_owner_record(owner_path)
        socket_identity = self._path_identity(socket_path)
        x_lock_identity = self._path_identity(x_lock_path)
        if record is None:
            return False
        if (
            record.get("version") != 1
            or record.get("display") != display
            or not isinstance(record.get("pid"), int)
            or not isinstance(record.get("start_time_ticks"), int)
        ):
            raise LoginRuntimeError(
                f"Invalid Xvfb owner record for display {display}",
            )

        pid = int(record["pid"])
        process_identity = self._read_process_identity(pid)
        if process_identity is not None:
            state, start_time_ticks = process_identity
            if state != "Z" and start_time_ticks == record["start_time_ticks"]:
                raise LoginRuntimeError(
                    f"X display {display} is owned by live CCM Xvfb PID {pid} "
                    "but cannot be authenticated",
                )

        if socket_identity is not None:
            if (
                socket_identity["file_type"] != stat.S_IFSOCK
                or not self._same_path_identity(socket_identity, record.get("socket"))
            ):
                raise LoginRuntimeError(
                    f"X display {display} socket no longer matches its CCM owner "
                    "record; refusing to remove it",
                )
        if x_lock_identity is not None and not self._same_path_identity(
            x_lock_identity,
            record.get("x_lock"),
        ):
            raise LoginRuntimeError(
                f"X display {display} lock no longer matches its CCM owner "
                "record; refusing to remove it",
            )

        if socket_identity is not None:
            socket_path.unlink()
        if x_lock_identity is not None:
            x_lock_path.unlink()
        try:
            owner_path.unlink()
        except FileNotFoundError:
            pass
        logger.warning(
            "Recovered stale CCM-owned Xvfb artifacts display=%s pid=%s",
            display,
            pid,
        )
        return True

    @staticmethod
    def _display_ready(display: str, auth_path: Path) -> bool:
        if not auth_path.is_file() or auth_path.is_symlink():
            return False
        env = {
            **os.environ,
            "DISPLAY": display,
            "XAUTHORITY": str(auth_path),
        }
        try:
            result = subprocess.run(
                ["xdpyinfo"],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=2,
                check=False,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
        return result.returncode == 0

    @staticmethod
    def _write_xauthority(display: str, auth_path: Path) -> None:
        if auth_path.is_symlink():
            raise LoginRuntimeError(f"Refusing symlink Xauthority file: {auth_path}")
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(auth_path, flags, 0o600)
        os.close(descriptor)
        os.chmod(auth_path, 0o600)
        subprocess.run(
            ["xauth", "-f", str(auth_path)],
            input=(
                f"add {display} MIT-MAGIC-COOKIE-1 "
                f"{secrets.token_hex(16)}\n"
            ),
            text=True,
            check=True,
            capture_output=True,
        )

    def _stop_owned_process(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        proc.poll()
        if proc.returncode is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if self._stderr_handle is not None:
            self._stderr_handle.close()
            self._stderr_handle = None

    @staticmethod
    def _stderr_tail(path: Path, limit: int = 2000) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="replace")[-limit:]
        except OSError:
            return ""

    def _ensure_sync(self) -> LoginRuntime:
        display, display_number = _configured_display()
        temp_dir = login_temp_directory()
        ensure_login_capacity(temp_dir=temp_dir)
        lock_path, auth_path, stderr_path, socket_path = self._paths(display_number)
        owner_path = self._owner_path(lock_path)
        x_lock_path = self._x_lock_path(socket_path, display_number)

        lock_flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            lock_flags |= os.O_NOFOLLOW
        lock_fd = os.open(lock_path, lock_flags, 0o600)
        try:
            os.fchmod(lock_fd, 0o600)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            if self._proc is not None:
                # Popen.returncode is cached until poll()/wait() refreshes it.
                self._proc.poll()
                if (
                    self._proc.returncode is None
                    and self._display_ready(display, auth_path)
                ):
                    return self._activate(
                        display, auth_path, temp_dir,
                    )
                self._stop_owned_process()

            # A sibling CCM process may already own this display.  Reuse it
            # only when its shared private cookie proves the display is ready.
            if self._display_ready(display, auth_path):
                return self._activate(display, auth_path, temp_dir)
            socket_exists = self._path_identity(socket_path) is not None
            x_lock_exists = self._path_identity(x_lock_path) is not None
            owner_exists = self._path_identity(owner_path) is not None
            if socket_exists or x_lock_exists or owner_exists:
                if self._recover_stale_owned_display(
                    display=display,
                    owner_path=owner_path,
                    socket_path=socket_path,
                    x_lock_path=x_lock_path,
                ):
                    socket_exists = self._path_identity(socket_path) is not None
                    x_lock_exists = self._path_identity(x_lock_path) is not None
            if socket_exists or x_lock_exists:
                raise LoginRuntimeError(
                    f"X display {display} exists but cannot be authenticated; "
                    "configure a distinct CCM_XVFB_DISPLAY instead of killing it",
                )

            self._write_xauthority(display, auth_path)
            if stderr_path.is_symlink():
                raise LoginRuntimeError(
                    f"Refusing symlink Xvfb stderr log: {stderr_path}",
                )
            stderr_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
            if hasattr(os, "O_NOFOLLOW"):
                stderr_flags |= os.O_NOFOLLOW
            stderr_fd = os.open(stderr_path, stderr_flags, 0o600)
            os.fchmod(stderr_fd, 0o600)
            self._stderr_handle = os.fdopen(stderr_fd, "wb", buffering=0)
            try:
                self._proc = subprocess.Popen(
                    [
                        "Xvfb",
                        display,
                        "-screen",
                        "0",
                        "1920x1080x24",
                        "-nolisten",
                        "tcp",
                        "-auth",
                        str(auth_path),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=self._stderr_handle,
                )
            except Exception:
                self._stderr_handle.close()
                self._stderr_handle = None
                raise

            deadline = time.monotonic() + float(
                os.environ.get("CCM_XVFB_READY_TIMEOUT_SECONDS", "10"),
            )
            while time.monotonic() < deadline:
                self._proc.poll()
                if self._proc.returncode is not None:
                    break
                if self._display_ready(display, auth_path):
                    self._record_owned_display(
                        display=display,
                        proc=self._proc,
                        owner_path=owner_path,
                        socket_path=socket_path,
                        x_lock_path=x_lock_path,
                    )
                    logger.info(
                        "Xvfb ready display=%s pid=%s",
                        display,
                        self._proc.pid,
                    )
                    return self._activate(
                        display, auth_path, temp_dir,
                    )
                time.sleep(0.1)

            return_code = self._proc.returncode
            self._stop_owned_process()
            diagnostic = self._stderr_tail(stderr_path)
            raise LoginRuntimeError(
                f"Xvfb {display} did not become ready"
                f" (returncode={return_code}, stderr={diagnostic!r})",
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise LoginRuntimeError(f"Unable to prepare Xvfb {display}: {exc}") from exc
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

    @staticmethod
    def _activate(
        display: str,
        auth_path: Path,
        temp_dir: Path,
    ) -> LoginRuntime:
        # Preserve compatibility with scripts that inherit the API process
        # environment while every explicit child receives the same values.
        os.environ["DISPLAY"] = display
        os.environ["XAUTHORITY"] = str(auth_path)
        os.environ["TMPDIR"] = str(temp_dir)
        os.environ["TMP"] = str(temp_dir)
        os.environ["TEMP"] = str(temp_dir)
        os.environ["CCM_LOGIN_TMPDIR"] = str(temp_dir)
        os.environ.pop("CCM_LOGIN_CDP_PORT", None)
        return LoginRuntime(display, auth_path, temp_dir)


xvfb_manager = XvfbManager()


async def ensure_login_runtime() -> LoginRuntime:
    return await xvfb_manager.ensure()


def login_child_environment(
    *,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    display, display_number = _configured_display()
    root = _runtime_root()
    runtime = LoginRuntime(
        display=display,
        xauthority=root / f"display-{display_number}.auth",
        temp_dir=login_temp_directory(),
    )
    return runtime.child_environment(extra=extra)
