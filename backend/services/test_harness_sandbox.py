"""Fail-closed runtime contract for untrusted Test Harness environments.

The existing shared-project ``ContainerManager`` is deliberately not reused as
the security boundary for PR/ref reviews: it bind-mounts a host checkout and
may mount account or Git credentials.  Harness sandboxes are ephemeral,
credential-free resources whose availability must be proven before an
untrusted target can be admitted.
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
from dataclasses import asdict, dataclass
from typing import Awaitable, Callable

from backend.config import settings


_DOCKER_IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SandboxCommandRunner = Callable[[list[str], float], Awaitable[tuple[int, str]]]


@dataclass(frozen=True, slots=True)
class SandboxCapability:
    """One immutable capability observation returned to admission callers."""

    available: bool
    backend: str
    reason: str | None
    image: str | None = None
    image_id: str | None = None
    runtime_version: str | None = None
    public_repositories_only: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class TestHarnessSandboxError(RuntimeError):
    """The sandbox runtime could not safely satisfy an operation."""


class TestHarnessSandboxRuntime:
    """Provider-neutral interface used by PR/ref target admission."""

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        raise NotImplementedError


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        await process.wait()
        return
    try:
        if os.name == "posix" and type(process.pid) is int and process.pid > 1:
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(process.wait(), timeout=2)
        return
    except asyncio.TimeoutError:
        pass
    try:
        if os.name == "posix" and type(process.pid) is int and process.pid > 1:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        pass
    await process.wait()


async def _run_command(argv: list[str], timeout: float) -> tuple[int, str]:
    """Run one read-only runtime probe with cancellation-safe process cleanup."""

    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=(os.name == "posix"),
    )
    communicate = asyncio.create_task(process.communicate())
    try:
        stdout, _ = await asyncio.wait_for(asyncio.shield(communicate), timeout)
    except BaseException:
        await asyncio.shield(_terminate_process(process))
        if not communicate.done():
            communicate.cancel()
        await asyncio.gather(communicate, return_exceptions=True)
        raise
    return process.returncode or 0, (stdout or b"").decode(
        "utf-8", errors="replace"
    )


class DockerTestHarnessSandboxRuntime(TestHarnessSandboxRuntime):
    """Probe the administrator-owned Docker runtime and pinned local image."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        docker_binary: str | None = None,
        image: str | None = None,
        runner: SandboxCommandRunner | None = None,
        probe_ttl_seconds: float = 15.0,
    ) -> None:
        self.enabled = (
            settings.test_harness_sandbox_enabled if enabled is None else enabled
        )
        self.docker_binary = (
            docker_binary or settings.test_harness_sandbox_docker_binary
        )
        self.image = image or settings.test_harness_sandbox_image
        self._runner = runner or _run_command
        self._probe_ttl_seconds = max(0.0, probe_ttl_seconds)
        self._probe_lock = asyncio.Lock()
        self._cached: tuple[float, SandboxCapability] | None = None

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        loop = asyncio.get_running_loop()
        now = loop.time()
        cached = self._cached
        if (
            not force
            and cached is not None
            and now - cached[0] < self._probe_ttl_seconds
        ):
            return cached[1]
        async with self._probe_lock:
            now = loop.time()
            cached = self._cached
            if (
                not force
                and cached is not None
                and now - cached[0] < self._probe_ttl_seconds
            ):
                return cached[1]
            capability = await self._probe_uncached()
            self._cached = (loop.time(), capability)
            return capability

    async def _probe_uncached(self) -> SandboxCapability:
        if not self.enabled:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox runtime is disabled by "
                    "administrator configuration"
                ),
                image=self.image,
            )
        binary = shutil.which(self.docker_binary)
        if binary is None:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason="PR/ref isolated sandbox Docker client is unavailable",
                image=self.image,
            )
        try:
            version_code, version_output = await self._runner(
                [binary, "version", "--format", "{{.Server.Version}}"],
                5.0,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox Docker daemon probe failed: "
                    f"{type(exc).__name__}"
                ),
                image=self.image,
            )
        version = version_output.strip()
        if version_code != 0 or not version or len(version) > 100:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason="PR/ref isolated sandbox Docker daemon is unavailable",
                image=self.image,
            )
        try:
            image_code, image_output = await self._runner(
                [binary, "image", "inspect", "--format", "{{.Id}}", self.image],
                5.0,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox image probe failed: "
                    f"{type(exc).__name__}"
                ),
                image=self.image,
                runtime_version=version,
            )
        image_id = image_output.strip().lower()
        if image_code != 0 or _DOCKER_IMAGE_ID_RE.fullmatch(image_id) is None:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=(
                    "PR/ref isolated sandbox image is missing or has an "
                    "invalid identity"
                ),
                image=self.image,
                runtime_version=version,
            )
        return SandboxCapability(
            available=True,
            backend="docker",
            reason=None,
            image=self.image,
            image_id=image_id,
            runtime_version=version,
        )


test_harness_sandbox_runtime = DockerTestHarnessSandboxRuntime()
