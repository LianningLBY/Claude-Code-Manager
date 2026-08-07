from __future__ import annotations

import asyncio

import pytest

from backend.services.test_harness_sandbox import (
    DockerTestHarnessSandboxRuntime,
)


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
