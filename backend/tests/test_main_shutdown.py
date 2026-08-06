import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_execution_runtimes_start_in_dependency_order(monkeypatch):
    import backend.main as main

    calls: list[str] = []

    async def record(name: str) -> None:
        calls.append(name)

    async def start_dispatcher() -> None:
        await record("dispatcher")

    async def start_capability() -> None:
        await record("capability")

    async def start_delivery() -> None:
        await record("delivery")

    monkeypatch.setattr(main.settings, "auto_start_dispatcher", True)
    monkeypatch.setattr(
        main.dispatcher,
        "start",
        AsyncMock(side_effect=start_dispatcher),
    )
    monkeypatch.setattr(
        main.capability_coordinator,
        "start",
        AsyncMock(side_effect=start_capability),
    )
    monkeypatch.setattr(
        main.delivery_controller,
        "start",
        AsyncMock(side_effect=start_delivery),
    )

    await main._start_execution_runtimes()

    assert calls == ["dispatcher", "capability", "delivery"]


@pytest.mark.asyncio
async def test_execution_runtime_shutdown_is_reverse_order_and_best_effort(
    monkeypatch,
):
    import backend.main as main

    calls: list[str] = []

    async def stop_delivery() -> None:
        calls.append("delivery")
        raise RuntimeError("delivery shutdown failed")

    async def record(name: str) -> None:
        calls.append(name)

    async def stop_capability() -> None:
        await record("capability")

    async def stop_dispatcher() -> None:
        await record("dispatcher")

    monkeypatch.setattr(main.ralph_loop, "shutdown", AsyncMock())
    monkeypatch.setattr(
        main.delivery_controller,
        "shutdown",
        AsyncMock(side_effect=stop_delivery),
    )
    monkeypatch.setattr(
        main.capability_coordinator,
        "shutdown",
        AsyncMock(side_effect=stop_capability),
    )
    monkeypatch.setattr(
        main.dispatcher,
        "shutdown",
        AsyncMock(side_effect=stop_dispatcher),
    )
    monkeypatch.setattr(main.instance_manager, "_pty_backend", None)
    monkeypatch.setattr(
        main.instance_manager,
        "shutdown_codex_app_server",
        AsyncMock(),
    )
    monkeypatch.setattr(main.sub_agent_watcher, "shutdown", AsyncMock())

    with pytest.raises(RuntimeError, match="delivery shutdown failed"):
        await main._shutdown_runtime_services(
            heartbeat_task=None,
            worker_health_task=None,
            upload_cleanup_task=None,
            tmp_cleanup_task=None,
            backup_svc=None,
        )

    assert calls == ["delivery", "capability", "dispatcher"]


@pytest.mark.asyncio
async def test_dispatcher_shutdown_error_propagates_after_other_cleanup(
    monkeypatch,
):
    import backend.main as main

    dispatcher_error = RuntimeError("dispatcher generation survived")
    dispatcher = MagicMock()
    dispatcher.shutdown = AsyncMock(side_effect=dispatcher_error)

    pty_backend = MagicMock()
    pty_backend.shutdown = AsyncMock()
    instance_manager = MagicMock()
    instance_manager._pty_backend = pty_backend
    instance_manager.shutdown_codex_app_server = AsyncMock()

    watcher = MagicMock(shutdown=AsyncMock())
    heartbeat = MagicMock()
    worker_health = MagicMock()
    upload_cleanup = MagicMock()
    tmp_cleanup = MagicMock()
    backup = MagicMock()

    monkeypatch.setattr(main, "dispatcher", dispatcher)
    monkeypatch.setattr(main, "instance_manager", instance_manager)
    monkeypatch.setattr(main, "sub_agent_watcher", watcher)

    with pytest.raises(RuntimeError, match="generation survived"):
        await main._shutdown_runtime_services(
            heartbeat_task=heartbeat,
            worker_health_task=worker_health,
            upload_cleanup_task=upload_cleanup,
            tmp_cleanup_task=tmp_cleanup,
            backup_svc=backup,
        )

    heartbeat.cancel.assert_called_once_with()
    worker_health.cancel.assert_called_once_with()
    upload_cleanup.cancel.assert_called_once_with()
    tmp_cleanup.cancel.assert_called_once_with()
    pty_backend.shutdown.assert_awaited_once_with()
    instance_manager.shutdown_codex_app_server.assert_awaited_once_with()
    watcher.shutdown.assert_awaited_once_with()
    backup.stop.assert_called_once_with()
    assert dispatcher.shutdown.await_count == 2


@pytest.mark.asyncio
async def test_dispatcher_shutdown_retry_can_recover_after_transport_cleanup(
    monkeypatch,
):
    import backend.main as main

    dispatcher = MagicMock()
    dispatcher.shutdown = AsyncMock(
        side_effect=[RuntimeError("spawn still settling"), None]
    )
    pty_backend = MagicMock(shutdown=AsyncMock())
    instance_manager = MagicMock()
    instance_manager._pty_backend = pty_backend
    instance_manager.shutdown_codex_app_server = AsyncMock()
    watcher = MagicMock(shutdown=AsyncMock())

    monkeypatch.setattr(main, "dispatcher", dispatcher)
    monkeypatch.setattr(main, "instance_manager", instance_manager)
    monkeypatch.setattr(main, "sub_agent_watcher", watcher)

    upload_cleanup = MagicMock()
    tmp_cleanup = MagicMock()
    await main._shutdown_runtime_services(
        heartbeat_task=None,
        worker_health_task=None,
        upload_cleanup_task=upload_cleanup,
        tmp_cleanup_task=tmp_cleanup,
        backup_svc=None,
    )

    assert dispatcher.shutdown.await_count == 2
    pty_backend.shutdown.assert_awaited_once_with()
    instance_manager.shutdown_codex_app_server.assert_awaited_once_with()
    upload_cleanup.cancel.assert_called_once_with()
    tmp_cleanup.cancel.assert_called_once_with()
    watcher.shutdown.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_shutdown_awaits_cancelled_background_tasks(monkeypatch):
    import backend.main as main

    dispatcher = MagicMock(shutdown=AsyncMock())
    instance_manager = MagicMock()
    instance_manager._pty_backend = None
    instance_manager.shutdown_codex_app_server = AsyncMock()
    watcher = MagicMock(shutdown=AsyncMock())
    monkeypatch.setattr(main, "dispatcher", dispatcher)
    monkeypatch.setattr(main, "instance_manager", instance_manager)
    monkeypatch.setattr(main, "sub_agent_watcher", watcher)

    finalized = [asyncio.Event() for _ in range(4)]

    async def background(done):
        try:
            await asyncio.Event().wait()
        finally:
            done.set()

    tasks = [
        asyncio.create_task(background(done))
        for done in finalized
    ]
    await asyncio.sleep(0)

    await main._shutdown_runtime_services(
        heartbeat_task=tasks[0],
        worker_health_task=tasks[1],
        upload_cleanup_task=tasks[2],
        tmp_cleanup_task=tasks[3],
        backup_svc=None,
    )

    assert all(task.done() for task in tasks)
    assert all(done.is_set() for done in finalized)
    watcher.shutdown.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_lifespan_always_closes_trusted_update_runtime(monkeypatch):
    import backend.main as main

    update_service = MagicMock()

    @asynccontextmanager
    async def failing_runtime(_app):
        yield
        raise RuntimeError("runtime teardown failed")

    monkeypatch.setattr(main, "_runtime_lifespan", failing_runtime)
    monkeypatch.setattr(main, "update_service", update_service)

    with pytest.raises(RuntimeError, match="runtime teardown failed"):
        async with main.lifespan(MagicMock()):
            pass

    update_service.close_runtime_snapshot.assert_called_once_with()
