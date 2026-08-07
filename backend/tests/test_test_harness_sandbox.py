from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from backend.models.test_harness import (
    TestHarnessSandboxLease as SandboxLeaseModel,
)
from backend.services.test_harness_sandbox import (
    DockerTestHarnessSandboxRuntime,
    SandboxCapability,
    SandboxResource,
    TestHarnessSandboxError as SandboxError,
    TestHarnessSandboxManager as SandboxManager,
)
from backend.services.test_harness import TestHarnessService as HarnessService
from backend.services.test_harness_contracts import TestHarnessSpec as HarnessSpec


@pytest.mark.asyncio
async def test_disabled_sandbox_never_invokes_docker():
    calls: list[list[str]] = []

    async def runner(argv: list[str], timeout: float) -> tuple[int, str]:
        calls.append(argv)
        return 0, "unexpected"

    runtime = DockerTestHarnessSandboxRuntime(
        enabled=False,
        docker_binary="docker",
        runner=runner,
        probe_ttl_seconds=0,
    )

    capability = await runtime.probe()

    assert capability.available is False
    assert "disabled" in (capability.reason or "")
    assert calls == []


@pytest.mark.asyncio
async def test_sandbox_probe_requires_daemon_and_valid_local_image(monkeypatch):
    calls: list[list[str]] = []

    async def runner(argv: list[str], timeout: float) -> tuple[int, str]:
        assert timeout == 5.0
        calls.append(argv)
        if argv[1] == "version":
            return 0, "27.5.1\n"
        return 0, "sha256:" + "b" * 64 + "\n"

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        docker_binary="docker",
        image="ccm-test-harness-sandbox:test",
        runner=runner,
        probe_ttl_seconds=60,
    )

    first, second = await asyncio.gather(runtime.probe(), runtime.probe())

    assert first == second
    assert first.available is True
    assert first.image_id == "sha256:" + "b" * 64
    assert first.runtime_version == "27.5.1"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_sandbox_probe_rejects_unverifiable_image_identity(monkeypatch):
    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        if argv[1] == "version":
            return 0, "27.5.1"
        return 0, "ccm-test-harness-sandbox:latest"

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )

    capability = await runtime.probe()

    assert capability.available is False
    assert "invalid identity" in (capability.reason or "")


@pytest.mark.asyncio
async def test_docker_sandbox_provision_has_no_host_mount_or_network(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    container_id = "d" * 64
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        action = argv[1]
        if action == "version":
            return 0, "27.5.1"
        if action == "image":
            return 0, "sha256:" + "e" * 64
        if action == "create":
            return 0, container_id
        if action == "start":
            return 0, container_id
        if action == "inspect":
            return 0, "\t".join(
                [
                    container_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    "true",
                    "true",
                    "none",
                ]
            )
        raise AssertionError(argv)

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
        memory="2g",
        cpus=1.5,
        pids_limit=128,
        workspace_bytes=512 * 1024 * 1024,
        tmp_bytes=128 * 1024 * 1024,
    )

    resource = await runtime.provision(
        run_id=run_id,
        lease_id=lease_id,
        lease_nonce=nonce,
    )

    create = next(call for call in calls if call[1] == "create")
    assert resource.resource_id == container_id
    assert "--read-only" in create
    assert create[create.index("--network") + 1] == "none"
    assert create[create.index("--cap-drop") + 1] == "ALL"
    assert create[create.index("--user") + 1] == "10001:10001"
    assert "--mount" not in create
    assert "-v" not in create
    assert "/var/run/docker.sock" not in " ".join(create)
    assert all(".claude" not in value and ".codex" not in value for value in create)


@pytest.mark.asyncio
async def test_docker_cleanup_requires_exact_labels_before_removal(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    container_id = "d" * 64
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "ps":
            return 0, container_id
        if argv[1] == "inspect":
            return 0, "\t".join(
                [
                    container_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    "f" * 48,
                    "true",
                    "true",
                    "none",
                ]
            )
        raise AssertionError(argv)

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )

    with pytest.raises(SandboxError, match="could not be proven"):
        await runtime.cleanup_identity(
            run_id=run_id,
            lease_id=lease_id,
            lease_nonce=nonce,
        )

    assert all(call[1] != "rm" for call in calls)


class _ManagedRuntime:
    def __init__(self, *, fail: BaseException | None = None):
        self.fail = fail
        self.cleaned: list[tuple[str, str, str]] = []

    async def probe(self, *, force: bool = False) -> SandboxCapability:
        _ = force
        return SandboxCapability(
            available=True,
            backend="docker",
            reason=None,
            image="ccm-test-harness-sandbox:test",
            image_id="sha256:" + "e" * 64,
        )

    async def provision(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> SandboxResource:
        if self.fail is not None:
            raise self.fail
        return SandboxResource(
            backend="docker",
            resource_id="d" * 64,
            resource_name=f"ccm-harness-{run_id[:16]}-{lease_nonce[:8]}",
            image_ref="ccm-test-harness-sandbox:test",
            image_digest="sha256:" + "e" * 64,
            metadata={"host_mounts": 0},
        )

    async def cleanup_identity(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
    ) -> int:
        self.cleaned.append((run_id, lease_id, lease_nonce))
        return 1


async def _fixed_url_run(db_factory):
    from backend.models.task import Task

    async with db_factory() as db:
        task = Task(title="Sandbox owner", status="completed")
        db.add(task)
        await db.commit()
        task_id = task.id
    return await HarnessService(db_factory=db_factory).start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Create a durable sandbox owner",
        ),
    )


@pytest.mark.asyncio
async def test_sandbox_manager_persists_identity_before_cleanup(db_factory):
    run = await _fixed_url_run(db_factory)
    runtime = _ManagedRuntime()
    manager = SandboxManager(runtime=runtime, db_factory=db_factory)

    lease = await manager.provision(run.id)

    assert lease.status == "ready"
    assert lease.resource_id == "d" * 64
    assert lease.runtime_metadata == {"host_mounts": 0}
    cleaned = await manager.cleanup(run.id)
    assert cleaned is not None
    assert cleaned.status == "cleaned"
    assert cleaned.cleanup_status == "completed"
    assert runtime.cleaned == [(run.id, lease.id, lease.lease_nonce)]


@pytest.mark.asyncio
async def test_sandbox_manager_cancellation_cleans_reserved_identity(db_factory):
    run = await _fixed_url_run(db_factory)
    runtime = _ManagedRuntime(fail=asyncio.CancelledError())
    manager = SandboxManager(runtime=runtime, db_factory=db_factory)

    with pytest.raises(asyncio.CancelledError):
        await manager.provision(run.id)

    async with db_factory() as db:
        lease = await db.scalar(
            select(SandboxLeaseModel).where(
                SandboxLeaseModel.run_id == run.id
            )
        )
    assert lease is not None
    assert lease.status == "failed"
    assert lease.cleanup_status == "completed"
    assert runtime.cleaned == [(run.id, lease.id, lease.lease_nonce)]
