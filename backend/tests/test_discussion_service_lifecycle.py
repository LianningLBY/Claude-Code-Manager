"""Regression tests for Discussion subprocess ownership and shutdown."""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from backend.models.discussion import Discussion, DiscussionAgent, DiscussionEvent
from backend.services import discussion_service
from backend.services.discussion_service import (
    DiscussionProcessCleanupError,
    DiscussionService,
)


class _FakeDb:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def execute(self, *_args, **_kwargs):
        return None

    async def commit(self):
        return None


class _Broadcaster:
    def __init__(self):
        self.broadcast = AsyncMock()


async def _wait_for_process(
    service: DiscussionService,
    agent_id: int,
) -> asyncio.subprocess.Process:
    for _ in range(100):
        process = service._processes.get(agent_id)
        if process is not None:
            return process
        await asyncio.sleep(0.01)
    raise AssertionError("discussion subprocess was not registered")


@pytest.mark.asyncio
async def test_cancelling_consumer_reaps_live_process():
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            41,
            7,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            {},
        )
    )
    service._consumers[41] = consumer
    process = await _wait_for_process(service, 41)

    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(consumer, timeout=5)

    assert process.returncode is not None
    assert 41 not in service._processes
    assert 41 not in service._consumers


@pytest.mark.asyncio
async def test_stderr_is_drained_while_process_is_running():
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            42,
            8,
            [
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "sys.stderr.write('x' * 2_000_000); "
                    "sys.stderr.flush(); "
                    "sys.exit(3)"
                ),
            ],
            {},
        )
    )
    service._consumers[42] = consumer

    await asyncio.wait_for(consumer, timeout=5)
    assert 42 not in service._processes
    assert 42 not in service._consumers


@pytest.mark.asyncio
async def test_shutdown_reaps_registered_process_without_consumer():
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        start_new_session=True,
    )
    service._processes[43] = process

    await asyncio.wait_for(service.shutdown(), timeout=5)

    assert process.returncode is not None
    assert service._processes == {}
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_cancellation_during_spawn_still_reaps_created_process(
    monkeypatch,
):
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    real_spawn = asyncio.create_subprocess_exec
    spawn_started = asyncio.Event()
    release_spawn = asyncio.Event()
    created: list[asyncio.subprocess.Process] = []

    async def delayed_spawn(*args, **kwargs):
        spawn_started.set()
        await release_spawn.wait()
        process = await real_spawn(*args, **kwargs)
        created.append(process)
        return process

    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        delayed_spawn,
    )
    consumer = asyncio.create_task(
        service._run_and_consume(
            44,
            9,
            [sys.executable, "-c", "import time; time.sleep(60)"],
            {},
        )
    )
    service._consumers[44] = consumer
    await spawn_started.wait()

    consumer.cancel()
    release_spawn.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(consumer, timeout=5)

    assert len(created) == 1
    assert created[0].returncode is not None
    assert service._processes == {}
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_shutdown_is_bounded_and_retains_unreaped_process(
    monkeypatch,
):
    class _StubbornProcess:
        pid = 12345
        returncode = None

        async def wait(self):
            await asyncio.Future()

    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    process = _StubbornProcess()
    service._processes[45] = process
    monkeypatch.setattr(
        discussion_service,
        "_PROCESS_SIGNAL_TIMEOUTS",
        (0.01, 0.01, 0.01),
    )
    monkeypatch.setattr(service, "_process_tree_alive", lambda _process: True)
    monkeypatch.setattr(
        service,
        "_send_process_signal",
        lambda _process, _signal: None,
    )

    with pytest.raises(DiscussionProcessCleanupError):
        await asyncio.wait_for(service.shutdown(), timeout=1)

    assert service._processes[45] is process


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="process-group regression")
async def test_leader_exit_does_not_leave_descendant_process_group(tmp_path):
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    child_pid_file = tmp_path / "child.pid"
    script = (
        "import os,time; "
        "pid=os.fork(); "
        "\nif pid == 0:\n"
        " d=os.open(os.devnull, os.O_RDWR); os.dup2(d,1); os.dup2(d,2); "
        "time.sleep(60); os._exit(0)\n"
        f"open({str(child_pid_file)!r},'w').write(str(pid))"
    )
    consumer = asyncio.create_task(
        service._run_and_consume(
            46,
            10,
            [sys.executable, "-c", script],
            {},
        )
    )
    service._consumers[46] = consumer

    await asyncio.wait_for(consumer, timeout=5)

    assert child_pid_file.exists()
    assert service._processes == {}
    assert service._consumers == {}


@pytest.mark.asyncio
async def test_shutdown_cancels_and_reaps_facilitator(monkeypatch):
    service = DiscussionService(lambda: _FakeDb(), _Broadcaster())
    real_spawn = asyncio.create_subprocess_exec

    async def spawn_sleeper(*_args, **kwargs):
        return await real_spawn(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdout=kwargs["stdout"],
            stderr=kwargs["stderr"],
            env=kwargs["env"],
            cwd=kwargs["cwd"],
            limit=kwargs["limit"],
            start_new_session=kwargs["start_new_session"],
        )

    monkeypatch.setattr(
        discussion_service.asyncio,
        "create_subprocess_exec",
        spawn_sleeper,
    )
    facilitator = asyncio.create_task(
        service._run_facilitator_process(
            SimpleNamespace(
                id=11,
                facilitator_model="model",
                facilitator_session_id=None,
            ),
            "prompt",
        )
    )
    for _ in range(100):
        if service._facilitator_processes:
            break
        await asyncio.sleep(0.01)
    assert service._facilitator_processes
    process = next(iter(service._facilitator_processes.values()))

    await asyncio.wait_for(service.shutdown(), timeout=5)

    assert process.returncode is not None
    assert facilitator.done()
    assert service._facilitator_processes == {}
    assert service._facilitator_tasks == set()


@pytest.mark.asyncio
async def test_concurrent_agent_start_has_single_winner(db_factory):
    async with db_factory() as db:
        discussion = Discussion(title="atomic start")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            session_id="session",
            status="idle",
        )
        db.add(agent)
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    service = DiscussionService(db_factory, _Broadcaster())
    launches: list[str] = []
    service._launch_agent_resume = (
        lambda _agent, _disc, message, cwd=None: launches.append(message)
    )

    async def send(message):
        async with db_factory() as db:
            await service.send_to_agent(db, agent_id, message)

    results = await asyncio.gather(
        send("first"),
        send("second"),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1
    assert len(launches) == 1
    async with db_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
        event_count = await db.scalar(
            select(func.count(DiscussionEvent.id)).where(
                DiscussionEvent.discussion_id == discussion_id,
                DiscussionEvent.agent_id == agent_id,
                DiscussionEvent.event_type == "user_message",
            )
        )
    assert current.status == "running"
    assert event_count == 1


@pytest.mark.asyncio
async def test_spawn_failure_rolls_back_running_claim(db_factory):
    async with db_factory() as db:
        discussion = Discussion(title="spawn failure")
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
            status="running",
        )
        db.add(agent)
        await db.commit()
        agent_id = agent.id
        discussion_id = discussion.id

    service = DiscussionService(db_factory, _Broadcaster())
    consumer = asyncio.create_task(
        service._run_and_consume(
            agent_id,
            discussion_id,
            ["/definitely/missing/ccm-discussion-binary"],
            {},
        )
    )
    service._consumers[agent_id] = consumer

    with pytest.raises(FileNotFoundError):
        await consumer

    async with db_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
    assert current.status == "error"
    assert current.pid is None
    assert service._consumers == {}
