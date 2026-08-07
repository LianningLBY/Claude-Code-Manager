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
    SandboxSourceSnapshot,
    TestHarnessSandboxError as SandboxError,
    TestHarnessSandboxManager as SandboxManager,
)
from backend.services.test_harness_git_targets import ResolvedGitTarget
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
                    "source",
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
                    "source",
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

    async def acquire_source(
        self,
        *,
        run_id: str,
        lease_id: str,
        lease_nonce: str,
        resource_id: str,
        resource_name: str,
        target: ResolvedGitTarget,
        additional_allowed_hosts: tuple[str, ...] = (),
    ) -> SandboxSourceSnapshot:
        _ = (
            run_id,
            lease_id,
            lease_nonce,
            resource_id,
            resource_name,
            additional_allowed_hosts,
        )
        return SandboxSourceSnapshot(
            repository_path="/workspace/repo",
            head_sha=target.head_sha,
            internal_network_id="e" * 64,
            egress_network_id="f" * 64,
            proxy_container_id="1" * 64,
            allowed_hosts=("github.com",),
        )


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
async def test_sandbox_manager_freezes_resolved_target_before_source_ready(db_factory):
    run = await _fixed_url_run(db_factory)
    runtime = _ManagedRuntime()
    manager = SandboxManager(runtime=runtime, db_factory=db_factory)
    lease = await manager.provision(run.id)
    target = ResolvedGitTarget(
        kind="pull_request",
        repository="zjw49246/CC-Manager",
        clone_url="https://github.com/zjw49246/CC-Manager.git",
        base_sha="a" * 40,
        head_sha="b" * 40,
        fetch_ref="refs/pull/99/head",
        source_repository="fork/CC-Manager",
        source_ref="feature",
        pr_number=99,
        changed_files=(),
        fingerprint="c" * 64,
    )

    snapshot = await manager.acquire_source(run.id, target)

    assert snapshot.head_sha == "b" * 40
    async with db_factory() as db:
        stored_run = await db.get(type(run), run.id)
        stored_lease = await db.get(SandboxLeaseModel, lease.id)
    assert stored_run is not None
    assert stored_run.resolved_target == target.as_dict()
    assert stored_run.source_git_head == "b" * 40
    assert stored_run.source_fingerprint == "c" * 64
    assert stored_lease is not None
    assert stored_lease.status == "source_ready"
    assert stored_lease.runtime_metadata["repository_path"] == "/workspace/repo"


@pytest.mark.asyncio
async def test_source_acquisition_uses_internal_network_and_exact_sha(monkeypatch):
    run_id = "a" * 32
    lease_id = "b" * 32
    nonce = "c" * 48
    source_id = "d" * 64
    internal_id = "e" * 64
    egress_id = "f" * 64
    proxy_id = "1" * 64
    source_name = f"ccm-harness-{run_id[:16]}-{nonce[:8]}"
    head_sha = "2" * 40
    calls: list[list[str]] = []

    async def runner(argv: list[str], _timeout: float) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "version":
            return 0, "27.5.1"
        if argv[1:3] == ["image", "inspect"]:
            return 0, "sha256:" + "3" * 64
        if argv[1:3] == ["network", "create"]:
            role = next(value for value in argv if "harness.role=" in value)
            return 0, internal_id if role.endswith("internal-network") else egress_id
        if argv[1:3] == ["network", "inspect"]:
            network_id = argv[-1]
            if network_id == internal_id:
                role, internal = "internal-network", "true"
            else:
                role, internal = "egress-network", "false"
            return 0, "\t".join(
                [
                    network_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    role,
                    internal,
                ]
            )
        if argv[1:3] == ["network", "connect"]:
            return 0, ""
        if argv[1] == "create":
            assert any(value.endswith("egress-proxy") for value in argv)
            return 0, proxy_id
        if argv[1] == "start":
            return 0, proxy_id
        if argv[1] == "inspect":
            resource_id = argv[-1]
            role = "source" if resource_id == source_id else "egress-proxy"
            network = "none" if resource_id == source_id else internal_id
            return 0, "\t".join(
                [
                    resource_id,
                    "test-harness",
                    run_id,
                    lease_id,
                    nonce,
                    role,
                    "true",
                    "true",
                    network,
                ]
            )
        if argv[1] == "exec":
            if "rev-parse" in argv:
                return 0, head_sha
            return 0, "ok"
        raise AssertionError(argv)

    monkeypatch.setattr("shutil.which", lambda _value: "/usr/bin/docker")
    runtime = DockerTestHarnessSandboxRuntime(
        enabled=True,
        runner=runner,
        probe_ttl_seconds=0,
    )
    target = ResolvedGitTarget(
        kind="pull_request",
        repository="zjw49246/CC-Manager",
        clone_url="https://github.com/zjw49246/CC-Manager.git",
        base_sha="4" * 40,
        head_sha=head_sha,
        fetch_ref="refs/pull/99/head",
        source_repository="fork/CC-Manager",
        source_ref="feature",
        pr_number=99,
        changed_files=(),
        fingerprint="5" * 64,
    )

    snapshot = await runtime.acquire_source(
        run_id=run_id,
        lease_id=lease_id,
        lease_nonce=nonce,
        resource_id=source_id,
        resource_name=source_name,
        target=target,
        additional_allowed_hosts=("registry.npmjs.org",),
    )

    assert isinstance(snapshot, SandboxSourceSnapshot)
    assert snapshot.head_sha == head_sha
    assert snapshot.repository_path == "/workspace/repo"
    assert snapshot.internal_network_id == internal_id
    assert snapshot.egress_network_id == egress_id
    assert snapshot.proxy_container_id == proxy_id
    network_creates = [call for call in calls if call[1:3] == ["network", "create"]]
    assert "--internal" in network_creates[0]
    assert "--internal" not in network_creates[1]
    proxy_create = next(call for call in calls if call[1] == "create")
    assert "--read-only" in proxy_create
    assert "--mount" not in proxy_create and "-v" not in proxy_create
    assert any(
        value == "CCM_ALLOWED_HOSTS=api.github.com,codeload.github.com,github.com,objects.githubusercontent.com,registry.npmjs.org"
        for value in proxy_create
    )
    fetch = next(call for call in calls if "fetch" in call)
    assert "HTTPS_PROXY=http://egress-proxy:3128" in fetch
    assert "refs/pull/99/head" in fetch
    assert not any("token" in value.lower() for value in fetch)


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
