"""Bind a login flow to the exact Chrome process/profile it launched."""

from __future__ import annotations

import asyncio
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse


class ChromeCdpError(RuntimeError):
    """Chrome's private DevTools endpoint could not be verified."""


class ChromeExitedBeforeCdp(ChromeCdpError):
    """The owned Chrome process exited before its endpoint became ready."""


@dataclass(frozen=True)
class OwnedChromeCdp:
    port: int
    browser_websocket_path: str
    tabs: list[dict]


def dynamic_debugging_arguments(profile_dir: Path) -> list[str]:
    """Ask Chrome for a private port recorded inside this unique profile."""

    return [
        "--remote-debugging-port=0",
        f"--user-data-dir={profile_dir}",
    ]


def _read_devtools_active_port(profile_dir: Path) -> tuple[int, str] | None:
    try:
        profile_info = profile_dir.lstat()
    except OSError as exc:
        raise ChromeCdpError(f"Cannot inspect Chrome profile {profile_dir}: {exc}") from exc
    if (
        stat.S_ISLNK(profile_info.st_mode)
        or not stat.S_ISDIR(profile_info.st_mode)
        or profile_info.st_uid != os.geteuid()
        or profile_info.st_mode & 0o077
    ):
        raise ChromeCdpError(f"Unsafe Chrome profile directory: {profile_dir}")

    path = profile_dir / "DevToolsActivePort"
    try:
        before = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ChromeCdpError(f"Cannot inspect {path}: {exc}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ChromeCdpError(f"Unsafe DevToolsActivePort file: {path}")
    if before.st_uid != os.geteuid():
        raise ChromeCdpError(f"Unsafe DevToolsActivePort permissions: {path}")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ChromeCdpError(f"DevToolsActivePort changed while opening: {path}")
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            contents = handle.read(4096)
    except (UnicodeDecodeError, OSError) as exc:
        raise ChromeCdpError(f"Cannot read {path}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    lines = contents.splitlines()
    if len(lines) < 2:
        raise ChromeCdpError(f"Incomplete DevToolsActivePort file: {path}")
    try:
        port = int(lines[0])
    except ValueError as exc:
        raise ChromeCdpError(f"Invalid Chrome DevTools port in {path}") from exc
    browser_path = lines[1].strip()
    if not 1 <= port <= 65535:
        raise ChromeCdpError(f"Invalid Chrome DevTools port {port}")
    if not browser_path.startswith("/devtools/browser/"):
        raise ChromeCdpError(
            f"Invalid Chrome DevTools browser path in {path}",
        )
    return port, browser_path


def _version_matches_owner(
    *,
    port: int,
    browser_path: str,
    payload: object,
) -> bool:
    if not isinstance(payload, dict):
        return False
    websocket_url = payload.get("webSocketDebuggerUrl")
    if not isinstance(websocket_url, str):
        return False
    parsed = urlparse(websocket_url)
    return (
        parsed.scheme == "ws"
        and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        and parsed.port == port
        and parsed.path == browser_path
    )


async def wait_for_owned_chrome_cdp(
    process,
    profile_dir: Path,
    *,
    client_factory,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
    attempts: int = 15,
    delay_seconds: float = 2,
) -> OwnedChromeCdp:
    """Discover and verify only the DevTools endpoint written by this profile."""

    last_error = ""
    for _attempt in range(attempts):
        await sleep(delay_seconds)
        returncode = process.poll()
        if returncode is not None:
            raise ChromeExitedBeforeCdp(
                f"Chrome exited before CDP was ready (code={returncode})",
            )

        try:
            endpoint = _read_devtools_active_port(profile_dir)
        except ChromeCdpError:
            # A present but unsafe identity file is not a startup race.
            raise
        if endpoint is None:
            continue
        port, browser_path = endpoint

        try:
            async with client_factory() as client:
                version_response = await client.get(
                    f"http://127.0.0.1:{port}/json/version",
                    timeout=3,
                )
                version_payload = version_response.json()
                if not _version_matches_owner(
                    port=port,
                    browser_path=browser_path,
                    payload=version_payload,
                ):
                    raise ChromeCdpError(
                        "Chrome DevTools endpoint does not match this login profile",
                    )
                tabs_response = await client.get(
                    f"http://127.0.0.1:{port}/json",
                    timeout=3,
                )
                tabs = tabs_response.json()
        except ChromeCdpError:
            raise
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            continue

        if process.poll() is not None:
            raise ChromeExitedBeforeCdp(
                "Chrome exited while its CDP endpoint was being verified",
            )
        if not isinstance(tabs, list):
            raise ChromeCdpError("Chrome CDP tabs response is not a list")
        return OwnedChromeCdp(
            port=port,
            browser_websocket_path=browser_path,
            tabs=tabs,
        )

    suffix = f" (last error: {last_error})" if last_error else ""
    raise ChromeCdpError(
        f"Owned Chrome CDP not ready after {attempts} attempts{suffix}",
    )
