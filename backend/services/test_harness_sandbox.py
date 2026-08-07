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
import secrets
import shutil
import signal
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Awaitable, Callable

from sqlalchemy import select

from backend.database import async_session
from backend.models.test_harness import TestHarnessRun, TestHarnessSandboxLease

from backend.config import settings


_DOCKER_IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_HEX_ID_RE = re.compile(r"[0-9a-f]{32}\Z")
_LEASE_NONCE_RE = re.compile(r"[0-9a-f]{48}\Z")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{12,64}\Z")
_MEMORY_RE = re.compile(r"[1-9][0-9]*(?:[kKmMgG])?\Z")
_OWNER_LABEL = "com.ccm.owner"
_RUN_LABEL = "com.ccm.harness.run-id"
_LEASE_LABEL = "com.ccm.harness.lease-id"
_NONCE_LABEL = "com.ccm.harness.lease-nonce"
_OWNER_VALUE = "test-harness"
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

    async def provision(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> "SandboxResource":
        raise NotImplementedError

    async def cleanup_identity(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> int:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class SandboxResource:
    backend: str
    resource_id: str
    resource_name: str
    image_ref: str
    image_digest: str
    metadata: dict[str, object]


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
        memory: str | None = None,
        cpus: float | None = None,
        pids_limit: int | None = None,
        workspace_bytes: int | None = None,
        tmp_bytes: int | None = None,
    ) -> None:
        self.enabled = (
            settings.test_harness_sandbox_enabled if enabled is None else enabled
        )
        self.docker_binary = (
            docker_binary or settings.test_harness_sandbox_docker_binary
        )
        self.image = image or settings.test_harness_sandbox_image
        self._runner = runner or _run_command
        self.memory = memory or settings.test_harness_sandbox_memory
        self.cpus = float(
            settings.test_harness_sandbox_cpus if cpus is None else cpus
        )
        self.pids_limit = int(
            settings.test_harness_sandbox_pids_limit
            if pids_limit is None
            else pids_limit
        )
        self.workspace_bytes = int(
            settings.test_harness_sandbox_workspace_bytes
            if workspace_bytes is None
            else workspace_bytes
        )
        self.tmp_bytes = int(
            settings.test_harness_sandbox_tmp_bytes
            if tmp_bytes is None
            else tmp_bytes
        )
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
        try:
            self._validate_security_limits()
        except TestHarnessSandboxError as exc:
            return SandboxCapability(
                available=False,
                backend="docker",
                reason=f"PR/ref isolated sandbox configuration is invalid: {exc}",
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

    def _validate_security_limits(self) -> None:
        if _MEMORY_RE.fullmatch(self.memory) is None:
            raise TestHarnessSandboxError("sandbox memory limit is invalid")
        if not 0.1 <= self.cpus <= 32:
            raise TestHarnessSandboxError("sandbox CPU limit is invalid")
        if not 16 <= self.pids_limit <= 4096:
            raise TestHarnessSandboxError("sandbox PID limit is invalid")
        if not 128 * 1024 * 1024 <= self.workspace_bytes <= 16 * 1024**3:
            raise TestHarnessSandboxError("sandbox workspace limit is invalid")
        if not 64 * 1024 * 1024 <= self.tmp_bytes <= 4 * 1024**3:
            raise TestHarnessSandboxError("sandbox tmpfs limit is invalid")

    @staticmethod
    def _validate_identity(run_id: str, lease_id: str, lease_nonce: str) -> None:
        if _HEX_ID_RE.fullmatch(run_id) is None:
            raise TestHarnessSandboxError("sandbox run identity is invalid")
        if _HEX_ID_RE.fullmatch(lease_id) is None:
            raise TestHarnessSandboxError("sandbox lease identity is invalid")
        if _LEASE_NONCE_RE.fullmatch(lease_nonce) is None:
            raise TestHarnessSandboxError("sandbox lease nonce is invalid")

    @staticmethod
    def _resource_name(run_id: str, lease_nonce: str) -> str:
        return f"ccm-harness-{run_id[:16]}-{lease_nonce[:8]}"

    def _labels(self, run_id: str, lease_id: str, lease_nonce: str) -> list[str]:
        return [
            "--label",
            f"{_OWNER_LABEL}={_OWNER_VALUE}",
            "--label",
            f"{_RUN_LABEL}={run_id}",
            "--label",
            f"{_LEASE_LABEL}={lease_id}",
            "--label",
            f"{_NONCE_LABEL}={lease_nonce}",
        ]

    async def provision(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> SandboxResource:
        self._validate_identity(run_id, lease_id, lease_nonce)
        self._validate_security_limits()
        capability = await self.probe()
        if not capability.available or capability.image_id is None:
            raise TestHarnessSandboxError(
                capability.reason or "isolated sandbox runtime is unavailable"
            )
        binary = shutil.which(self.docker_binary)
        if binary is None:  # pragma: no cover - probe is the authority.
            raise TestHarnessSandboxError("isolated sandbox Docker client disappeared")
        name = self._resource_name(run_id, lease_nonce)
        create_args = [
            binary,
            "create",
            "--name",
            name,
            *self._labels(run_id, lease_id, lease_nonce),
            "--init",
            "--user",
            "10001:10001",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--read-only",
            "--network",
            "none",
            "--pids-limit",
            str(self.pids_limit),
            "--memory",
            self.memory,
            "--cpus",
            str(self.cpus),
            "--stop-timeout",
            "5",
            "--tmpfs",
            f"/workspace:rw,nosuid,nodev,size={self.workspace_bytes}",
            "--tmpfs",
            f"/tmp:rw,noexec,nosuid,nodev,size={self.tmp_bytes}",
            "--tmpfs",
            f"/home/sandbox:rw,nosuid,nodev,size={self.tmp_bytes}",
            "--tmpfs",
            "/run:rw,noexec,nosuid,nodev,size=67108864",
            "--workdir",
            "/workspace",
            "--entrypoint",
            "/usr/bin/tail",
            capability.image_id,
            "-f",
            "/dev/null",
        ]
        try:
            code, output = await self._runner(create_args, 30.0)
            container_id = output.strip().lower()
            if code != 0 or _CONTAINER_ID_RE.fullmatch(container_id) is None:
                raise TestHarnessSandboxError(
                    "isolated sandbox container creation failed"
                )
            start_code, _ = await self._runner([binary, "start", container_id], 30.0)
            if start_code != 0:
                raise TestHarnessSandboxError("isolated sandbox container start failed")
            await self._verify_resource(
                binary=binary,
                resource_id=container_id,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                require_running=True,
            )
            return SandboxResource(
                backend="docker",
                resource_id=container_id,
                resource_name=name,
                image_ref=self.image,
                image_digest=capability.image_id,
                metadata={
                    "network_mode": "none",
                    "read_only_root": True,
                    "host_mounts": 0,
                    "credential_mounts": 0,
                    "workspace_bytes": self.workspace_bytes,
                    "tmp_bytes": self.tmp_bytes,
                    "memory": self.memory,
                    "cpus": self.cpus,
                    "pids_limit": self.pids_limit,
                },
            )
        except BaseException:
            try:
                await asyncio.shield(
                    self.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                raise TestHarnessSandboxError(
                    "sandbox provisioning failed and cleanup could not be proven"
                ) from cleanup_exc
            raise

    async def _verify_resource(
        self,
        *,
        binary: str,
        resource_id: str,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        require_running: bool,
    ) -> None:
        if _CONTAINER_ID_RE.fullmatch(resource_id) is None:
            raise TestHarnessSandboxError("sandbox container identity is invalid")
        template = (
            '{{.Id}}\t{{index .Config.Labels "' + _OWNER_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _RUN_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _LEASE_LABEL + '"}}\t'
            '{{index .Config.Labels "' + _NONCE_LABEL + '"}}\t'
            "{{.State.Running}}\t{{.HostConfig.ReadonlyRootfs}}\t"
            "{{.HostConfig.NetworkMode}}"
        )
        code, output = await self._runner(
            [binary, "inspect", "--format", template, resource_id],
            10.0,
        )
        fields = output.strip().split("\t")
        expected = [
            resource_id,
            _OWNER_VALUE,
            run_id,
            lease_id,
            lease_nonce,
            "true" if require_running else fields[5] if len(fields) > 5 else "",
            "true",
            "none",
        ]
        if code != 0 or len(fields) != len(expected) or fields != expected:
            raise TestHarnessSandboxError(
                "sandbox container identity or security profile could not be proven"
            )

    async def cleanup_identity(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> int:
        self._validate_identity(run_id, lease_id, lease_nonce)
        binary = shutil.which(self.docker_binary)
        if binary is None:
            raise TestHarnessSandboxError(
                "Docker client unavailable; sandbox cleanup cannot be proven"
            )
        code, output = await self._runner(
            [
                binary,
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
            ],
            10.0,
        )
        if code != 0:
            raise TestHarnessSandboxError("sandbox cleanup discovery failed")
        resources = [item.strip().lower() for item in output.splitlines() if item.strip()]
        if any(_CONTAINER_ID_RE.fullmatch(item) is None for item in resources):
            raise TestHarnessSandboxError("sandbox cleanup returned an invalid resource")
        for resource_id in resources:
            await self._verify_resource(
                binary=binary,
                resource_id=resource_id,
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
                require_running=False,
            )
            remove_code, _ = await self._runner(
                [binary, "rm", "-f", resource_id],
                30.0,
            )
            if remove_code != 0:
                raise TestHarnessSandboxError("sandbox container removal failed")
        verify_code, verify_output = await self._runner(
            [
                binary,
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                f"label={_OWNER_LABEL}={_OWNER_VALUE}",
                "--filter",
                f"label={_RUN_LABEL}={run_id}",
                "--filter",
                f"label={_LEASE_LABEL}={lease_id}",
                "--filter",
                f"label={_NONCE_LABEL}={lease_nonce}",
            ],
            10.0,
        )
        if verify_code != 0 or verify_output.strip():
            raise TestHarnessSandboxError("sandbox cleanup could not be proven")
        return len(resources)


class TestHarnessSandboxManager:
    """Persist each external resource identity before and after Docker calls."""

    def __init__(
        self,
        *,
        runtime: TestHarnessSandboxRuntime | None = None,
        db_factory=async_session,
    ) -> None:
        self.runtime = runtime or test_harness_sandbox_runtime
        self.db_factory = db_factory
        self._lock = asyncio.Lock()

    async def provision(self, run_id: str) -> TestHarnessSandboxLease:
        if _HEX_ID_RE.fullmatch(run_id) is None:
            raise TestHarnessSandboxError("sandbox run identity is invalid")
        async with self._lock:
            capability = await self.runtime.probe()
            if not capability.available or not capability.image:
                raise TestHarnessSandboxError(
                    capability.reason or "isolated sandbox runtime is unavailable"
                )
            async with self.db_factory() as db:
                if await db.get(TestHarnessRun, run_id) is None:
                    raise TestHarnessSandboxError("Harness run not found")
                existing = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
                if existing is not None:
                    raise TestHarnessSandboxError(
                        "Harness run already owns a sandbox lease"
                    )
                lease = TestHarnessSandboxLease(
                    id=uuid.uuid4().hex,
                    run_id=run_id,
                    backend=capability.backend,
                    lease_nonce=secrets.token_hex(24),
                    image_ref=capability.image,
                    image_digest=capability.image_id,
                    status="provisioning",
                    phase="creating_container",
                    runtime_metadata={},
                    cleanup_status="pending",
                )
                db.add(lease)
                await db.commit()
                await db.refresh(lease)
                lease_id = lease.id
                lease_nonce = lease.lease_nonce
        try:
            resource = await self.runtime.provision(
                run_id=run_id,
                lease_id=lease_id,
                lease_nonce=lease_nonce,
            )
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            await asyncio.shield(
                self._mark_failed(
                    lease_id,
                    error=(str(exc) or type(exc).__name__)[:4000],
                    cleanup_error=cleanup_error,
                )
            )
            raise
        try:
            async with self.db_factory() as db:
                lease = await db.get(TestHarnessSandboxLease, lease_id)
                if lease is None:
                    raise TestHarnessSandboxError("sandbox lease disappeared")
                lease.resource_id = resource.resource_id
                lease.resource_name = resource.resource_name
                lease.image_digest = resource.image_digest
                lease.runtime_metadata = resource.metadata
                lease.status = "ready"
                lease.phase = "isolated_idle"
                lease.started_at = datetime.utcnow()
                await db.commit()
                await db.refresh(lease)
                return lease
        except BaseException as exc:
            cleanup_error: str | None = None
            try:
                await asyncio.shield(
                    self.runtime.cleanup_identity(
                        run_id=run_id,
                        lease_id=lease_id,
                        lease_nonce=lease_nonce,
                    )
                )
            except BaseException as cleanup_exc:
                cleanup_error = str(cleanup_exc)[:4000]
            try:
                await asyncio.shield(
                    self._mark_failed(
                        lease_id,
                        error=(str(exc) or type(exc).__name__)[:4000],
                        cleanup_error=cleanup_error,
                    )
                )
            finally:
                if cleanup_error:
                    raise TestHarnessSandboxError(
                        "sandbox persistence failed and cleanup could not be proven"
                    ) from exc
            raise

    async def _mark_failed(
        self,
        lease_id: str,
        *,
        error: str,
        cleanup_error: str | None,
    ) -> None:
        async with self.db_factory() as db:
            lease = await db.get(TestHarnessSandboxLease, lease_id)
            if lease is None:
                return
            lease.status = "failed"
            lease.phase = "failed"
            lease.error = error
            lease.cleanup_status = "failed" if cleanup_error else "completed"
            lease.cleanup_error = cleanup_error
            lease.completed_at = datetime.utcnow()
            await db.commit()

    async def cleanup(self, run_id: str) -> TestHarnessSandboxLease | None:
        async with self._lock:
            async with self.db_factory() as db:
                lease = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
                if lease is None:
                    return None
                if lease.cleanup_status == "completed":
                    return lease
                lease.status = "cleaning"
                lease.phase = "cleaning"
                lease.cleanup_status = "cleaning"
                await db.commit()
                lease_id = lease.id
                lease_nonce = lease.lease_nonce
            try:
                await self.runtime.cleanup_identity(
                    run_id=run_id,
                    lease_id=lease_id,
                    lease_nonce=lease_nonce,
                )
            except BaseException as exc:
                async with self.db_factory() as db:
                    lease = await db.get(TestHarnessSandboxLease, lease_id)
                    if lease is not None:
                        lease.status = "cleanup_failed"
                        lease.phase = "cleanup_failed"
                        lease.cleanup_status = "failed"
                        lease.cleanup_error = (str(exc) or type(exc).__name__)[:4000]
                        await db.commit()
                raise
            async with self.db_factory() as db:
                lease = await db.get(TestHarnessSandboxLease, lease_id)
                if lease is None:
                    return None
                lease.status = "cleaned"
                lease.phase = "cleaned"
                lease.cleanup_status = "completed"
                lease.cleanup_error = None
                lease.completed_at = datetime.utcnow()
                await db.commit()
                await db.refresh(lease)
                return lease

    async def recover_interrupted(self) -> int:
        async with self.db_factory() as db:
            run_ids = list(
                await db.scalars(
                    select(TestHarnessSandboxLease.run_id).where(
                        TestHarnessSandboxLease.cleanup_status != "completed"
                    )
                )
            )
        recovered = 0
        for run_id in run_ids:
            try:
                await self.cleanup(run_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            recovered += 1
        return recovered


test_harness_sandbox_runtime = DockerTestHarnessSandboxRuntime()
test_harness_sandbox_manager = TestHarnessSandboxManager(
    runtime=test_harness_sandbox_runtime
)
