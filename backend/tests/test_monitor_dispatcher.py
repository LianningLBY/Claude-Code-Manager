"""Tests for the durable scheduled-turn Monitor lifecycle.

``start_monitor_session`` owns one recoverable scheduler. Each due check claims
an exact DB generation, launches one short provider turn, and requires that
turn to consume its generation through a callback before another check can run.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import os
import signal
import sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task
from backend.models.monitor_session import MonitorSession, MonitorCheck
from backend.services.dispatcher import GlobalDispatcher


@pytest.fixture
def mock_broadcaster():
    b = MagicMock()
    b.broadcast = AsyncMock()
    return b


@pytest.fixture
def dispatcher(db_factory, mock_broadcaster):
    d = GlobalDispatcher.__new__(GlobalDispatcher)
    d.db_factory = db_factory
    d.broadcaster = mock_broadcaster
    d.instance_manager = MagicMock()
    d._running_tasks = {}
    d._monitor_tasks = {}
    d._monitor_processes = {}
    d._monitor_config_dirs = {}
    d._monitor_log_fhs = {}
    d._monitor_active_turns = set()
    d._sub_agent_tasks = {}
    d._sub_agent_processes = {}
    d._sub_agent_log_fhs = {}
    d._sub_agent_codex_processes = {}
    d._sub_agent_codex_homes = {}
    d._sub_agent_codex_threads = {}
    d.codex_pool = None

    @asynccontextmanager
    async def runtime_admission(
        _provider,
        _home,
        _model,
        *,
        service_tier="default",
    ):
        assert service_tier == "default"
        yield None

    d.instance_manager._cloudrouter_runtime_admission = runtime_admission
    return d


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["monitor", "sub-agent"])
async def test_api_account_aux_home_survives_cancelled_unreaped_spawn(
    dispatcher, tmp_path, kind,
):
    home = str(tmp_path / "api-account" / "claude")
    dispatcher.pool = MagicMock()
    dispatcher._pool_select = AsyncMock(return_value=home)
    dispatcher._sanitize_cloudrouter_claude_env = MagicMock()
    process = _fake_proc(returncode=None)
    session_id = 91 if kind == "monitor" else 92

    async def cancelled_spawn(**kwargs):
        kwargs["process_map"][session_id] = process
        raise asyncio.CancelledError()

    dispatcher._launch_registered_aux_process = cancelled_spawn
    with pytest.raises(asyncio.CancelledError):
        if kind == "monitor":
            await dispatcher._launch_monitor_agent(
                prompt="monitor",
                cwd=str(tmp_path),
                model="claude-opus-4-8",
                monitor_session_id=session_id,
                mcp_config_path=tmp_path / "monitor.json",
            )
        else:
            await dispatcher._launch_sub_agent(
                prompt="child",
                cwd=str(tmp_path),
                model="claude-opus-4-8",
                session_id=session_id,
                mcp_config_path=tmp_path / "child.json",
            )

    home_map = (
        dispatcher._monitor_config_dirs
        if kind == "monitor"
        else dispatcher._sub_agent_config_dirs
    )
    assert home_map[session_id] == home

    dispatcher._aux_process_reaped = MagicMock(return_value=False)
    account = MagicMock(claude_config_dir=home)
    blockers = dispatcher.api_account_aux_runtime_users(account)
    assert blockers == [f"{kind} {session_id}"]


async def _seed_task_and_monitor(
    db_factory, status="in_progress", max_checks=50, interval=1, context=None
):
    async with db_factory() as db:
        task = Task(
            title="t", description="d", status=status,
            enabled_skills={"monitor": True}, target_repo="/tmp",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        ms = MonitorSession(
            task_id=task.id, description="test monitor",
            interval=interval, max_checks=max_checks, monitor_context=context,
        )
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        return task.id, ms.id


async def _seed_codex_sub_agent(
    dispatcher,
    *,
    task_id: int,
    session_id: int,
) -> None:
    async with dispatcher.db_factory() as db:
        task = Task(
            id=task_id,
            title="codex parent",
            description="d",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
        )
        session = MonitorSession(
            id=session_id,
            task_id=task_id,
            agent_type="sub_agent",
            source="ccm",
            description="child",
            status="running",
        )
        db.add_all([task, session])
        await db.commit()


def _fake_proc(returncode=0):
    proc = MagicMock()
    proc.returncode = returncode
    proc.pid = 12345
    proc.wait = AsyncMock(return_value=returncode)
    proc.kill = MagicMock()
    return proc


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat_path = f"/proc/{pid}/stat"
    try:
        with open(stat_path, encoding="utf-8") as stat_file:
            stat = stat_file.read()
    except (FileNotFoundError, PermissionError):
        return True
    close_paren = stat.rfind(")")
    state = stat[close_paren + 2:].split()[0] if close_paren >= 0 else ""
    return state != "Z"


async def _wait_for_pid_file(path, timeout: float = 2.0) -> int:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if path.exists():
            return int(path.read_text(encoding="utf-8"))
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out waiting for auxiliary child PID")


async def _wait_until_not_running(pid: int, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if not _pid_is_running(pid):
            return
        await asyncio.sleep(0.02)
    assert not _pid_is_running(pid), f"Process {pid} is still running"


# === Prompt building ===


def test_build_monitor_agent_prompt(dispatcher):
    prompt = dispatcher._build_monitor_agent_prompt("watch build", "tail -f /tmp/log")
    assert "监控目标" in prompt
    assert "watch build" in prompt
    assert "上下文" in prompt
    assert "tail -f /tmp/log" in prompt
    # The sub-agent must be told about its MCP callback tools
    assert "report_status" in prompt
    assert "mark_complete" in prompt
    assert "只执行一次状态检查" in prompt
    assert "不要 sleep" in prompt


def test_build_monitor_agent_prompt_no_context(dispatcher):
    prompt = dispatcher._build_monitor_agent_prompt("test", None)
    assert "test" in prompt
    assert "上下文" not in prompt


@pytest.mark.asyncio
async def test_launch_codex_sub_agent_uses_required_thread_mcp(dispatcher):
    await _seed_codex_sub_agent(
        dispatcher,
        task_id=7,
        session_id=41,
    )
    process = _fake_proc(returncode=None)
    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "thread-child"))
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = registry

    @asynccontextmanager
    async def admit(home):
        yield home or "/tmp/default-codex-home"

    dispatcher.instance_manager.codex_home_app_server_guard = admit

    from backend.services.mcp_config import build_sub_agent_mcp_server_specs

    specs = build_sub_agent_mcp_server_specs(41, 7)
    launched = await dispatcher._launch_codex_sub_agent(
        prompt="review",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort_level="high",
        session_id=41,
        task_id=7,
        task_metadata={},
        mcp_specs=specs,
    )

    assert launched is process
    kwargs = registry.start_turn.await_args.kwargs
    assert kwargs["resume_session_id"] is None
    assert "ephemeral" not in kwargs
    assert kwargs["mcp_specs"] == specs
    assert kwargs["mcp_specs"][0].required is True
    assert kwargs["disable_project_config"] is False
    assert set(kwargs["mcp_specs"][0].enabled_tools) == {
        "get_context",
        "report_progress",
        "submit_result",
    }
    assert dispatcher._sub_agent_codex_processes[41] is process
    assert dispatcher._sub_agent_codex_threads[41] == "thread-child"


@pytest.mark.asyncio
async def test_launch_api_codex_sub_agent_disables_project_config(dispatcher):
    await _seed_codex_sub_agent(
        dispatcher,
        task_id=7,
        session_id=61,
    )
    process = _fake_proc(returncode=None)
    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "thread-api-child"))
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = (
        registry
    )
    api_home = "/tmp/apex-1/codex"
    account = object()
    dispatcher.cloudrouter_store = MagicMock()
    dispatcher.cloudrouter_store.account_for_codex_home.return_value = account
    dispatcher.codex_pool = MagicMock(enabled=True)
    dispatcher.codex_pool.select.return_value = api_home
    dispatcher.codex_pool.canonical_home.side_effect = lambda home: home

    admission_calls = []

    @asynccontextmanager
    async def runtime_admission(
        provider,
        home,
        model,
        *,
        service_tier="default",
    ):
        admission_calls.append((provider, home, model))
        assert service_tier == "default"
        yield account

    @asynccontextmanager
    async def home_admission(home):
        yield home

    dispatcher.instance_manager._cloudrouter_runtime_admission = (
        runtime_admission
    )
    dispatcher.instance_manager.codex_home_app_server_guard = home_admission

    await dispatcher._launch_codex_sub_agent(
        prompt="review",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort_level="high",
        session_id=61,
        task_id=7,
        task_metadata={},
        mcp_specs=(),
    )

    assert admission_calls == [("codex", api_home, "gpt-5.6-sol")]
    assert (
        registry.start_turn.await_args.kwargs["disable_project_config"]
        is True
    )


@pytest.mark.asyncio
async def test_codex_sub_agent_final_gate_rejects_pending_task_routing(
    dispatcher,
):
    await _seed_codex_sub_agent(
        dispatcher,
        task_id=7,
        session_id=71,
    )
    async with dispatcher.db_factory() as db:
        task = await db.get(Task, 7)
        task.metadata_ = {
            "worker_routing_config_pending": {
                "op_id": "sub-agent-stage",
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "priority",
            }
        }
        await db.commit()

    registry = MagicMock()
    registry.start_turn = AsyncMock()
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = (
        registry
    )

    @asynccontextmanager
    async def admit(home):
        yield home or "/tmp/default-codex-home"

    dispatcher.instance_manager.codex_home_app_server_guard = admit

    with pytest.raises(RuntimeError, match="routing synchronization"):
        await dispatcher._launch_codex_sub_agent(
            prompt="must not start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort_level="high",
            session_id=71,
            task_id=7,
            task_metadata={},
            mcp_specs=(),
            expected_task_routing=(
                "codex",
                "gpt-5.6-sol",
                "default",
            ),
        )
    registry.start_turn.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_sub_agent_final_gate_rejects_stopped_generation(
    dispatcher,
):
    await _seed_codex_sub_agent(
        dispatcher,
        task_id=7,
        session_id=72,
    )
    async with dispatcher.db_factory() as db:
        session = await db.get(MonitorSession, 72)
        session.status = "stopped"
        await db.commit()

    registry = MagicMock()
    registry.start_turn = AsyncMock()
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = (
        registry
    )

    @asynccontextmanager
    async def admit(home):
        yield home or "/tmp/default-codex-home"

    dispatcher.instance_manager.codex_home_app_server_guard = admit

    with pytest.raises(RuntimeError, match="launch admission changed"):
        await dispatcher._launch_codex_sub_agent(
            prompt="must not start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort_level="high",
            session_id=72,
            task_id=7,
            task_metadata={},
            mcp_specs=(),
            expected_task_routing=(
                "codex",
                "gpt-5.6-sol",
                "default",
            ),
        )
    registry.start_turn.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_sub_agent_commit_failure_aborts_exact_started_turn(
    dispatcher,
    monkeypatch,
):
    await _seed_codex_sub_agent(
        dispatcher,
        task_id=7,
        session_id=73,
    )
    process = _fake_proc(returncode=None)
    registry = MagicMock()
    registry.start_turn = AsyncMock(
        return_value=(process, "thread-uncommitted")
    )

    async def abort(_home, candidate, *, reason):
        assert candidate is process
        assert "did not commit" in reason
        candidate.returncode = 130
        return False

    registry.abort_unclaimed_turn = AsyncMock(side_effect=abort)
    registry.delete_thread = AsyncMock()
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = (
        registry
    )

    @asynccontextmanager
    async def admit(home):
        yield home or "/tmp/default-codex-home"

    dispatcher.instance_manager.codex_home_app_server_guard = admit

    async def fail_commit(_session):
        raise RuntimeError("commit failed")

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)

    with pytest.raises(RuntimeError, match="commit failed"):
        await dispatcher._launch_codex_sub_agent(
            prompt="must be cleaned",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort_level="high",
            session_id=73,
            task_id=7,
            task_metadata={},
            mcp_specs=(),
        )

    registry.abort_unclaimed_turn.assert_awaited_once_with(
        "/tmp/default-codex-home",
        process,
        reason="Codex sub-agent launch admission did not commit",
    )
    registry.delete_thread.assert_awaited_once_with(
        "/tmp/default-codex-home",
        "thread-uncommitted",
    )
    assert 73 not in dispatcher._sub_agent_codex_processes
    assert 73 not in dispatcher._sub_agent_codex_homes
    assert 73 not in dispatcher._sub_agent_codex_threads


@pytest.mark.asyncio
async def test_codex_sub_agent_commit_cancellation_waits_for_exact_abort(
    dispatcher,
    monkeypatch,
):
    await _seed_codex_sub_agent(
        dispatcher,
        task_id=7,
        session_id=74,
    )
    process = _fake_proc(returncode=None)
    registry = MagicMock()
    registry.start_turn = AsyncMock(
        return_value=(process, "thread-cancelled-commit")
    )
    commit_started = asyncio.Event()
    abort_started = asyncio.Event()
    release_abort = asyncio.Event()

    async def blocked_commit(_session):
        commit_started.set()
        await asyncio.Future()

    async def abort(_home, candidate, *, reason):
        assert candidate is process
        assert "did not commit" in reason
        abort_started.set()
        await release_abort.wait()
        candidate.returncode = 130
        return False

    registry.abort_unclaimed_turn = AsyncMock(side_effect=abort)
    registry.delete_thread = AsyncMock()
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = (
        registry
    )

    @asynccontextmanager
    async def admit(home):
        yield home or "/tmp/default-codex-home"

    dispatcher.instance_manager.codex_home_app_server_guard = admit
    monkeypatch.setattr(AsyncSession, "commit", blocked_commit)

    launch = asyncio.create_task(
        dispatcher._launch_codex_sub_agent(
            prompt="must be cleaned after cancellation",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort_level="high",
            session_id=74,
            task_id=7,
            task_metadata={},
            mcp_specs=(),
        )
    )
    await asyncio.wait_for(commit_started.wait(), timeout=1)
    launch.cancel()
    await asyncio.wait_for(abort_started.wait(), timeout=1)
    assert not launch.done()

    release_abort.set()
    with pytest.raises(asyncio.CancelledError):
        await launch

    registry.abort_unclaimed_turn.assert_awaited_once_with(
        "/tmp/default-codex-home",
        process,
        reason="Codex sub-agent launch admission did not commit",
    )
    registry.delete_thread.assert_awaited_once_with(
        "/tmp/default-codex-home",
        "thread-cancelled-commit",
    )
    assert 74 not in dispatcher._sub_agent_codex_processes
    assert 74 not in dispatcher._sub_agent_codex_homes
    assert 74 not in dispatcher._sub_agent_codex_threads


@pytest.mark.asyncio
async def test_codex_sub_agent_rejects_home_owned_by_exec_generation(
    dispatcher, monkeypatch, tmp_path,
):
    from backend.services.codex_app_server import CodexAppServerBusyError
    from backend.services.instance_manager import InstanceManager
    from backend.services.mcp_config import build_sub_agent_mcp_server_specs

    home = str((tmp_path / "codex-exec-owner").resolve())
    monkeypatch.setenv("CODEX_HOME", home)
    manager = InstanceManager(MagicMock(), MagicMock())
    registry = MagicMock()
    registry.start_turn = AsyncMock()
    manager._codex_app_server = registry
    manager._codex_exec_homes[81] = home
    dispatcher.instance_manager = manager

    with pytest.raises(
        CodexAppServerBusyError,
        match="still has an exec generation",
    ):
        await dispatcher._launch_codex_sub_agent(
            prompt="must not overlap",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort_level="high",
            session_id=51,
            task_id=7,
            task_metadata={},
            mcp_specs=build_sub_agent_mcp_server_specs(51, 7),
        )

    registry.start_turn.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_sub_agent_rejects_active_ephemeral_exec(
    dispatcher, monkeypatch, tmp_path,
):
    from backend.services.codex_app_server import CodexAppServerBusyError
    from backend.services.instance_manager import InstanceManager
    from backend.services.mcp_config import build_sub_agent_mcp_server_specs

    home = str((tmp_path / "codex-ephemeral-owner").resolve())
    monkeypatch.setenv("CODEX_HOME", home)
    manager = InstanceManager(MagicMock(), MagicMock())
    registry = MagicMock()
    registry.shutdown_home = AsyncMock(return_value=True)
    registry.start_turn = AsyncMock()
    manager._codex_app_server = registry
    dispatcher.instance_manager = manager

    async with manager.codex_home_exec_guard(home):
        with pytest.raises(
            CodexAppServerBusyError,
            match="active ephemeral exec",
        ):
            await dispatcher._launch_codex_sub_agent(
                prompt="must not overlap",
                cwd="/tmp",
                model="gpt-5.6-sol",
                effort_level="high",
                session_id=52,
                task_id=7,
                task_metadata={},
                mcp_specs=build_sub_agent_mcp_server_specs(52, 7),
            )

    registry.start_turn.assert_not_awaited()


@pytest.mark.asyncio
async def test_finalize_codex_sub_agent_interrupts_turn_not_process_group(
    dispatcher,
):
    process = _fake_proc(returncode=None)
    registry = MagicMock()

    async def abort(_home, candidate, *, reason):
        assert reason == "stop"
        candidate.returncode = 130

    registry.abort_unclaimed_turn = AsyncMock(side_effect=abort)
    registry.delete_thread = AsyncMock()
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = registry
    dispatcher._sub_agent_codex_processes[42] = process
    dispatcher._sub_agent_codex_homes[42] = "/tmp/codex-home"
    dispatcher._sub_agent_codex_threads[42] = "thread-child"

    with patch.object(
        dispatcher,
        "_terminate_aux_process",
        new_callable=AsyncMock,
    ) as terminate_group:
        await dispatcher._finalize_codex_sub_agent_turn(
            42,
            process,
            reason="stop",
        )

    registry.abort_unclaimed_turn.assert_awaited_once_with(
        "/tmp/codex-home",
        process,
        reason="stop",
    )
    registry.delete_thread.assert_awaited_once_with(
        "/tmp/codex-home",
        "thread-child",
    )
    terminate_group.assert_not_awaited()
    assert 42 not in dispatcher._sub_agent_codex_processes
    assert 42 not in dispatcher._sub_agent_codex_homes
    assert 42 not in dispatcher._sub_agent_codex_threads


@pytest.mark.asyncio
async def test_finalize_completed_codex_sub_agent_deletes_thread_without_interrupt(
    dispatcher,
):
    process = _fake_proc(returncode=0)
    registry = MagicMock()
    registry.abort_unclaimed_turn = AsyncMock()
    registry.delete_thread = AsyncMock()
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = registry
    dispatcher._sub_agent_codex_processes[43] = process
    dispatcher._sub_agent_codex_homes[43] = "/tmp/codex-home"
    dispatcher._sub_agent_codex_threads[43] = "thread-completed"

    await dispatcher._finalize_codex_sub_agent_turn(
        43,
        process,
        reason="completed",
    )

    registry.abort_unclaimed_turn.assert_not_awaited()
    registry.delete_thread.assert_awaited_once_with(
        "/tmp/codex-home",
        "thread-completed",
    )
    assert 43 not in dispatcher._sub_agent_codex_processes
    assert 43 not in dispatcher._sub_agent_codex_homes
    assert 43 not in dispatcher._sub_agent_codex_threads


@pytest.mark.asyncio
async def test_failed_codex_sub_agent_thread_delete_retains_cleanup_evidence(
    dispatcher,
):
    process = _fake_proc(returncode=0)
    registry = MagicMock()
    registry.delete_thread = AsyncMock(side_effect=RuntimeError("delete failed"))
    dispatcher.instance_manager._ensure_codex_app_server_registry.return_value = registry
    dispatcher._sub_agent_codex_processes[44] = process
    dispatcher._sub_agent_codex_homes[44] = "/tmp/codex-home"
    dispatcher._sub_agent_codex_threads[44] = "thread-retained"

    with pytest.raises(RuntimeError, match="delete failed"):
        await dispatcher._finalize_codex_sub_agent_turn(
            44,
            process,
            reason="completed",
        )

    assert dispatcher._sub_agent_codex_processes[44] is process
    assert dispatcher._sub_agent_codex_homes[44] == "/tmp/codex-home"
    assert dispatcher._sub_agent_codex_threads[44] == "thread-retained"


def test_build_monitor_agent_prompt_interval_guidance(dispatcher):
    """The model never owns the interval in scheduled-turn mode."""
    prompt = dispatcher._build_monitor_agent_prompt("watch", None, interval=1800)
    assert "不要 sleep" in prompt
    assert "等待 1800 秒" in prompt
    assert "time.sleep" not in prompt
    assert "ScheduleWakeup" in prompt


@pytest.mark.asyncio
async def test_launch_monitor_agent_raises_bash_max_timeout(dispatcher, tmp_path):
    """A one-check turn no longer scales shell timeout with interval."""
    dispatcher.pool = None
    captured = {}

    async def fake_exec(*cmd, **kwargs):
        captured["env"] = kwargs["env"]
        return _fake_proc()

    with patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
        await dispatcher._launch_monitor_agent(
            prompt="p", cwd="/tmp", model=None,
            monitor_session_id=990001,
            mcp_config_path=tmp_path / "mcp.json",
            interval_seconds=3600,
        )

    assert captured["env"]["BASH_MAX_TIMEOUT_MS"] == "600000"
    dispatcher._monitor_log_fhs[990001].close()


@pytest.mark.asyncio
async def test_launch_monitor_agent_keeps_larger_env_timeout(
    dispatcher, tmp_path, monkeypatch
):
    """环境里已有更大的 BASH_MAX_TIMEOUT_MS 时只抬不降。"""
    dispatcher.pool = None
    monkeypatch.setenv("BASH_MAX_TIMEOUT_MS", "99999000")
    captured = {}

    async def fake_exec(*cmd, **kwargs):
        captured["env"] = kwargs["env"]
        return _fake_proc()

    with patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
        await dispatcher._launch_monitor_agent(
            prompt="p", cwd="/tmp", model=None,
            monitor_session_id=990002,
            mcp_config_path=tmp_path / "mcp.json",
            interval_seconds=300,
        )

    assert captured["env"]["BASH_MAX_TIMEOUT_MS"] == "99999000"
    dispatcher._monitor_log_fhs[990002].close()


# === start_monitor_session ===


@pytest.mark.asyncio
async def test_start_monitor_session(dispatcher):
    ms = MagicMock()
    ms.id = 1
    with patch.object(dispatcher, "_monitor_session_lifecycle", new_callable=AsyncMock):
        dispatcher.start_monitor_session(ms)
    assert 1 in dispatcher._monitor_tasks
    dispatcher._monitor_tasks[1].cancel()
    try:
        await dispatcher._monitor_tasks[1]
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_start_monitor_session_is_idempotent(dispatcher):
    ms = MagicMock(id=11)
    release = asyncio.Event()

    async def lifecycle(_session_id):
        await release.wait()

    with patch.object(
        dispatcher,
        "_monitor_session_lifecycle",
        side_effect=lifecycle,
    ) as run:
        dispatcher.start_monitor_session(ms)
        first = dispatcher._monitor_tasks[11]
        dispatcher.start_monitor_session(ms)
        assert dispatcher._monitor_tasks[11] is first
        assert run.call_count == 1
        release.set()
        await first


@pytest.mark.asyncio
async def test_recover_monitor_sessions_rehydrates_only_safe_schedules(
    dispatcher,
    db_factory,
):
    task_id, scheduled_id = await _seed_task_and_monitor(db_factory)
    async with db_factory() as db:
        active = MonitorSession(
            task_id=task_id,
            description="uncertain",
            status="running",
            turn_generation=2,
            active_turn_generation=2,
        )
        remote = MonitorSession(
            task_id=task_id,
            description="remote mirror",
            status="running",
            remote_id=91,
        )
        db.add_all([active, remote])
        await db.commit()

    with patch.object(dispatcher, "start_monitor_session") as start:
        await dispatcher._recover_monitor_sessions()

    assert [call.args[0].id for call in start.call_args_list] == [
        scheduled_id
    ]


def test_scheduled_monitor_without_active_turn_is_not_restart_blocker(
    dispatcher,
):
    dispatcher._monitor_tasks[1] = MagicMock(done=MagicMock(return_value=False))
    assert dispatcher.active_auxiliary_blockers() == []

    dispatcher._monitor_active_turns.add(1)
    assert dispatcher.active_auxiliary_blockers() == [
        {
            "id": 1,
            "title": "监控子 Agent #1",
            "status": "running_auxiliary",
            "kind": "monitor",
        }
    ]


@pytest.mark.asyncio
async def test_auxiliary_admission_is_closed_before_shutdown_snapshot(dispatcher):
    dispatcher._shutting_down = True
    dispatcher._sub_agent_tasks = {}

    with pytest.raises(RuntimeError, match="monitor admission is closed"):
        dispatcher.start_monitor_session(MagicMock(id=91))
    with pytest.raises(RuntimeError, match="sub-agent admission is closed"):
        dispatcher.start_sub_agent_session(MagicMock(id=92))

    assert 91 not in dispatcher._monitor_tasks
    assert 92 not in dispatcher._sub_agent_tasks


@pytest.mark.asyncio
async def test_stop_aux_refreshes_process_registered_during_spawn_cancel(
    dispatcher,
):
    process = _fake_proc(returncode=None)
    process_map = {}
    task_map = {}

    async def spawn_window():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # `_settle_aux_process_spawn` returns the exact handle only after
            # the caller cancellation; registration therefore happens here.
            process_map[93] = process
            raise

    lifecycle = asyncio.create_task(spawn_window())
    task_map[93] = lifecycle
    await asyncio.sleep(0)

    async def terminate(candidate):
        assert candidate is process
        candidate.returncode = -9

    dispatcher._terminate_aux_process = AsyncMock(side_effect=terminate)
    with patch.object(
        GlobalDispatcher, "_aux_process_group_alive", return_value=False
    ):
        await dispatcher._stop_aux_session(93, task_map, process_map)

    dispatcher._terminate_aux_process.assert_awaited_once_with(process)
    assert 93 not in process_map


@pytest.mark.asyncio
async def test_stop_aux_timeout_retains_lifecycle_evidence(dispatcher):
    release = asyncio.Event()

    async def ignores_first_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    lifecycle = asyncio.create_task(ignores_first_cancellation())
    task_map = {94: lifecycle}
    process_map = {}
    await asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="did not stop"):
            await dispatcher._stop_aux_session(
                94,
                task_map,
                process_map,
                lifecycle_timeout=0.01,
            )
        assert task_map[94] is lifecycle
        assert not lifecycle.done()
    finally:
        release.set()
        await asyncio.wait_for(lifecycle, timeout=1)


# === Lifecycle: durable scheduled-turn state transitions ===


@pytest.mark.asyncio
async def test_scheduler_runs_two_generation_fenced_checks(
    dispatcher,
    db_factory,
    client,
):
    task_id, ms_id = await _seed_task_and_monitor(
        db_factory,
        max_checks=2,
        interval=0,
    )
    dispatcher.enqueue_message = AsyncMock()
    launched_generations = []

    async def launch(_session_id, snapshot):
        generation = int(snapshot["generation"])
        launched_generations.append(generation)
        process = _fake_proc(returncode=0)

        async def report():
            response = await client.post(
                f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/checks",
                json={
                    "summary": f"check-{generation}",
                    "turn_generation": generation,
                },
            )
            assert response.status_code == 200, response.text
            return 0

        process.wait = AsyncMock(side_effect=report)
        return process, MagicMock()

    with (
        patch("backend.main.dispatcher", dispatcher),
        patch.object(
            dispatcher,
            "_launch_scheduled_monitor_turn",
            side_effect=launch,
        ),
        patch.object(
            dispatcher,
            "_finalize_aux_lifecycle_process",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        await dispatcher._monitor_session_lifecycle(ms_id)

    assert launched_generations == [1, 2]
    async with db_factory() as db:
        session = await db.get(MonitorSession, ms_id)
        reports = list(
            (
                await db.execute(
                    select(MonitorCheck)
                    .where(MonitorCheck.monitor_session_id == ms_id)
                    .order_by(MonitorCheck.check_number)
                )
            )
            .scalars()
            .all()
        )
    assert session.status == "completed"
    assert session.checks_done == 2
    assert session.active_turn_generation is None
    assert [report.summary for report in reports] == [
        "check-1",
        "check-2",
    ]


@pytest.mark.asyncio
async def test_lifecycle_completed_by_subagent(dispatcher, db_factory, mock_broadcaster):
    """Sub-agent calls mark_complete (via API) then exits → session stays completed."""
    task_id, ms_id = await _seed_task_and_monitor(db_factory)

    proc = _fake_proc(returncode=0)

    async def wait_and_complete():
        # Simulate the sub-agent's mark_complete MCP call before exiting
        async with db_factory() as db:
            await db.execute(
                update(MonitorSession).where(MonitorSession.id == ms_id)
                .values(
                    status="completed",
                    active_turn_generation=None,
                    turn_started_at=None,
                    next_check_at=None,
                )
            )
            await db.commit()
        return 0

    proc.wait = AsyncMock(side_effect=wait_and_complete)

    with patch.object(dispatcher, "_launch_monitor_agent", new_callable=AsyncMock, return_value=proc) as mock_launch, \
         patch("backend.services.mcp_config.cleanup_monitor_agent_mcp_config") as mock_cleanup:
        await dispatcher._monitor_session_lifecycle(ms_id)

    # Launched once with the monitor prompt and the session's cwd
    mock_launch.assert_awaited_once()
    launch_kwargs = mock_launch.call_args.kwargs
    assert "test monitor" in launch_kwargs["prompt"]
    assert launch_kwargs["monitor_session_id"] == ms_id

    async with db_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "completed"

    # No "failed" broadcast for a clean completion
    failed_events = [
        c for c in mock_broadcaster.broadcast.call_args_list
        if c[0][1].get("event") == "monitor_session_status" and c[0][1].get("status") == "failed"
    ]
    assert failed_events == []

    # MCP config cleaned up, bookkeeping dicts emptied
    mock_cleanup.assert_called_once_with(ms_id, 1)
    assert ms_id not in dispatcher._monitor_tasks
    assert ms_id not in dispatcher._monitor_processes


@pytest.mark.asyncio
async def test_normal_parent_exit_kills_residual_monitor_group(
    dispatcher, db_factory
):
    """A clean CLI parent exit cannot leave its tool child running."""
    _, ms_id = await _seed_task_and_monitor(db_factory)
    proc = _fake_proc(returncode=0)
    group_alive = True
    signals = []

    async def wait_and_complete():
        async with db_factory() as db:
            await db.execute(
                update(MonitorSession)
                .where(MonitorSession.id == ms_id)
                .values(
                    status="completed",
                    active_turn_generation=None,
                    turn_started_at=None,
                    next_check_at=None,
                )
            )
            await db.commit()
        return 0

    proc.wait = AsyncMock(side_effect=wait_and_complete)

    async def launch_and_register(**kwargs):
        dispatcher._monitor_processes[ms_id] = proc
        return proc

    def kill_group(pid, sig):
        nonlocal group_alive
        assert pid == proc.pid
        if sig == 0:
            if group_alive:
                return None
            raise ProcessLookupError
        assert sig == signal.SIGKILL
        signals.append(sig)
        group_alive = False

    with (
        patch.object(
            dispatcher,
            "_launch_monitor_agent",
            side_effect=launch_and_register,
        ),
        patch("backend.services.dispatcher.os.killpg", side_effect=kill_group),
    ):
        await dispatcher._monitor_session_lifecycle(ms_id)

    assert signals == [signal.SIGKILL]
    assert ms_id not in dispatcher._monitor_processes


@pytest.mark.asyncio
async def test_failed_group_proof_retains_aux_process_evidence(dispatcher):
    proc = _fake_proc(returncode=0)
    dispatcher._monitor_processes[71] = proc

    with (
        patch.object(
            dispatcher,
            "_terminate_aux_process",
            new_callable=AsyncMock,
            side_effect=RuntimeError("cannot prove"),
        ),
        patch.object(
            GlobalDispatcher,
            "_aux_process_group_alive",
            return_value=True,
        ),
    ):
        delayed = await dispatcher._finalize_aux_lifecycle_process(
            session_id=71,
            process=proc,
            process_map=dispatcher._monitor_processes,
        )

    assert delayed is None
    assert dispatcher._monitor_processes[71] is proc


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups only")
@pytest.mark.parametrize("map_kind", ["monitor", "sub-agent"])
async def test_aux_spawn_cancellation_settles_registers_and_reaps(
    dispatcher, tmp_path, map_kind
):
    """Cancellation inside spawn cannot lose the exact child group handle."""
    pid_file = tmp_path / f"{map_kind}-child.pid"
    log_path = tmp_path / f"{map_kind}.log"
    process_map = {}
    log_map = {}
    captured = {}
    spawned = asyncio.Event()
    release_spawn = asyncio.Event()
    real_create_subprocess_exec = asyncio.create_subprocess_exec

    script = """
import pathlib
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(30)"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
time.sleep(30)
"""
    cmd = [sys.executable, "-c", script, str(pid_file)]

    async def delayed_spawn(*args, **kwargs):
        process = await real_create_subprocess_exec(*args, **kwargs)
        captured["process"] = process
        spawned.set()
        await release_spawn.wait()
        return process

    launch = None
    child_pid = None
    try:
        with patch(
            "backend.services.dispatcher.asyncio.create_subprocess_exec",
            side_effect=delayed_spawn,
        ):
            launch = asyncio.create_task(
                dispatcher._launch_registered_aux_process(
                    cmd=cmd,
                    cwd=str(tmp_path),
                    env=dict(os.environ),
                    log_path=log_path,
                    session_id=81,
                    process_map=process_map,
                    log_map=log_map,
                )
            )
            await spawned.wait()
            child_pid = await _wait_for_pid_file(pid_file)
            launch.cancel()
            await asyncio.sleep(0)
            assert not launch.done()
            release_spawn.set()
            with pytest.raises(asyncio.CancelledError):
                await launch

        assert captured["process"].returncode is not None
        await _wait_until_not_running(child_pid)
        assert process_map == {}
        assert log_map == {}
    finally:
        release_spawn.set()
        if launch is not None and not launch.done():
            launch.cancel()
            await asyncio.gather(launch, return_exceptions=True)
        process = captured.get("process")
        if process is not None and process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()
        if child_pid is not None and _pid_is_running(child_pid):
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.asyncio
async def test_failed_turn_releases_with_backoff(
    dispatcher,
    db_factory,
):
    """A non-terminal turn failure releases its claim and schedules a retry."""
    _, ms_id = await _seed_task_and_monitor(db_factory)
    started_at = datetime.utcnow()
    async with db_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        ms.turn_generation = 7
        ms.active_turn_generation = 7
        ms.turn_started_at = started_at
        ms.next_check_at = None
        await db.commit()

    await dispatcher._record_monitor_turn_failure(
        ms_id,
        7,
        "provider exited before callback",
    )

    async with db_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "running"
        assert ms.active_turn_generation is None
        assert ms.turn_started_at is None
        assert ms.consecutive_failures == 1
        assert ms.last_error == "provider exited before callback"
        assert ms.next_check_at is not None
        delay = (ms.next_check_at - datetime.utcnow()).total_seconds()
        assert 3.0 <= delay <= 5.0


@pytest.mark.asyncio
async def test_lifecycle_abnormal_exit_marks_failed(
    dispatcher,
    db_factory,
    mock_broadcaster,
    monkeypatch,
):
    """Three failed scheduled turns end the session after bounded retries."""
    task_id, ms_id = await _seed_task_and_monitor(db_factory)
    monkeypatch.setattr(
        "backend.services.dispatcher.MONITOR_FAILURE_BACKOFF_BASE",
        0,
    )
    monkeypatch.setattr(
        "backend.services.dispatcher.MONITOR_FAILURE_BACKOFF_MAX",
        0,
    )

    proc = _fake_proc(returncode=1)
    with patch.object(
        dispatcher,
        "_launch_monitor_agent",
        new_callable=AsyncMock,
        return_value=proc,
    ) as launch:
        await dispatcher._monitor_session_lifecycle(ms_id)
    assert launch.await_count == 3

    async with db_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "failed"
        assert ms.completed_at is not None

    failed_events = [
        c for c in mock_broadcaster.broadcast.call_args_list
        if c[0][1].get("event") == "monitor_session_status" and c[0][1].get("status") == "failed"
    ]
    assert len(failed_events) == 1
    assert failed_events[0][0][0] == f"task:{task_id}"
    assert failed_events[0][0][1]["monitor_session_id"] == ms_id


@pytest.mark.asyncio
async def test_lifecycle_timeout_kills_process(
    dispatcher,
    db_factory,
    mock_broadcaster,
    monkeypatch,
):
    """Repeated one-turn timeouts are reaped and eventually fail."""
    task_id, ms_id = await _seed_task_and_monitor(db_factory)
    monkeypatch.setattr(
        "backend.services.dispatcher.MONITOR_FAILURE_BACKOFF_BASE",
        0,
    )
    monkeypatch.setattr(
        "backend.services.dispatcher.MONITOR_FAILURE_BACKOFF_MAX",
        0,
    )

    proc = _fake_proc(returncode=None)

    async def time_out():
        raise asyncio.TimeoutError

    proc.wait = AsyncMock(side_effect=time_out)
    group_alive = True
    signals = []

    def kill_group(pid, sig):
        nonlocal group_alive
        assert pid == proc.pid
        if sig == 0:
            if group_alive:
                return None
            raise ProcessLookupError
        signals.append(sig)
        group_alive = False
        proc.returncode = -9

    with (
        patch.object(
            dispatcher,
            "_launch_monitor_agent",
            new_callable=AsyncMock,
            return_value=proc,
        ),
        patch("backend.services.dispatcher.os.killpg", side_effect=kill_group),
    ):
        await dispatcher._monitor_session_lifecycle(ms_id)

    assert signals, "process group should be killed on timeout"

    async with db_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "failed"


@pytest.mark.asyncio
async def test_lifecycle_cancelled(dispatcher, db_factory, mock_broadcaster):
    """Shutdown cancellation reaps the turn and leaves it recoverable."""
    task_id, ms_id = await _seed_task_and_monitor(db_factory)

    proc = _fake_proc(returncode=None)

    async def hang():
        await asyncio.sleep(9999)

    proc.wait = AsyncMock(side_effect=hang)
    group_alive = True
    signals = []

    def kill_group(pid, sig):
        nonlocal group_alive
        assert pid == proc.pid
        if sig == 0:
            if group_alive:
                return None
            raise ProcessLookupError
        signals.append(sig)
        group_alive = False
        proc.returncode = -9

    async def launch_and_register(**kwargs):
        dispatcher._monitor_processes[ms_id] = proc
        return proc

    with (
        patch.object(
            dispatcher,
            "_launch_monitor_agent",
            side_effect=launch_and_register,
        ),
        patch("backend.services.dispatcher.os.killpg", side_effect=kill_group),
    ):
        lifecycle_task = asyncio.create_task(dispatcher._monitor_session_lifecycle(ms_id))
        await asyncio.sleep(0.1)
        lifecycle_task.cancel()
        try:
            await lifecycle_task
        except asyncio.CancelledError:
            pass

    assert signals, "subprocess group should be killed on cancellation"
    assert ms_id not in dispatcher._monitor_tasks
    assert ms_id not in dispatcher._monitor_processes
    async with db_factory() as db:
        session = await db.get(MonitorSession, ms_id)
        assert session.status == "running"
        assert session.active_turn_generation is None
        assert session.next_check_at is not None


@pytest.mark.asyncio
async def test_lifecycle_launch_failure_marks_failed(
    dispatcher,
    db_factory,
    mock_broadcaster,
    monkeypatch,
):
    """Repeated pre-spawn failures follow the same terminal threshold."""
    task_id, ms_id = await _seed_task_and_monitor(db_factory)
    monkeypatch.setattr(
        "backend.services.dispatcher.MONITOR_FAILURE_BACKOFF_BASE",
        0,
    )
    monkeypatch.setattr(
        "backend.services.dispatcher.MONITOR_FAILURE_BACKOFF_MAX",
        0,
    )

    with patch.object(
        dispatcher, "_launch_monitor_agent",
        new_callable=AsyncMock, side_effect=RuntimeError("spawn failed"),
    ):
        await dispatcher._monitor_session_lifecycle(ms_id)

    async with db_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "failed"
        assert ms.completed_at is not None


# === API callbacks: MonitorCheck records + broadcasts ===
# In the new design the sub-agent reports via MCP tools that hit these endpoints,
# so the per-check persistence/broadcast coverage moved here.


async def _seed_via_api(client, session_factory, max_checks=50):
    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "enabled_skills": {"monitor": True}, "provider": "claude",
    })
    task_id = resp.json()["id"]
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(status="in_progress"))
        ms = MonitorSession(task_id=task_id, description="api monitor", max_checks=max_checks)
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        return task_id, ms.id


def _mock_main_dispatcher():
    d = MagicMock()
    d.broadcaster = MagicMock()
    d.broadcaster.broadcast = AsyncMock()
    d.enqueue_message = AsyncMock()
    d.stop_monitor_session_process = AsyncMock()
    d._monitor_processes = {}
    return d


@pytest.mark.asyncio
async def test_report_check_writes_record_and_broadcasts(client, session_factory):
    task_id, ms_id = await _seed_via_api(client, session_factory)
    mock_d = _mock_main_dispatcher()

    with patch("backend.main.dispatcher", mock_d):
        resp = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/checks",
            json={"summary": "Process running at 45% CPU", "status": "success"},
        )
    assert resp.status_code == 200

    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.checks_done == 1
        assert ms.last_summary == "Process running at 45% CPU"
        assert ms.status == "running"

        result = await db.execute(
            select(MonitorCheck).where(MonitorCheck.monitor_session_id == ms_id)
        )
        check = result.scalars().one()
        assert check.check_number == 1
        assert check.status == "success"
        assert check.summary == "Process running at 45% CPU"

    events = [
        c for c in mock_d.broadcaster.broadcast.call_args_list
        if c[0][1].get("event") == "monitor_check"
    ]
    assert len(events) == 1
    assert events[0][0][0] == f"task:{task_id}"
    assert events[0][0][1]["summary"] == "Process running at 45% CPU"
    # Non-important routine check does not interrupt the main agent
    mock_d.enqueue_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_report_check_important_enqueues_to_main_agent(client, session_factory):
    task_id, ms_id = await _seed_via_api(client, session_factory)
    mock_d = _mock_main_dispatcher()

    with patch("backend.main.dispatcher", mock_d):
        resp = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/checks",
            json={"summary": "Build FAILED", "status": "success", "is_important": True},
        )
    assert resp.status_code == 200

    mock_d.enqueue_message.assert_awaited_once()
    kwargs = mock_d.enqueue_message.call_args.kwargs
    assert kwargs["task_id"] == task_id
    assert kwargs["source"] == "monitor:report"
    assert "Build FAILED" in kwargs["prompt"]


@pytest.mark.asyncio
async def test_report_check_max_checks_auto_completes(client, session_factory):
    task_id, ms_id = await _seed_via_api(client, session_factory, max_checks=1)
    mock_d = _mock_main_dispatcher()

    with patch("backend.main.dispatcher", mock_d):
        resp = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/checks",
            json={"summary": "still running", "status": "success"},
        )
    assert resp.status_code == 200

    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "completed"
        assert ms.checks_done == 1
        assert ms.completed_at is not None

    status_events = [
        c for c in mock_d.broadcaster.broadcast.call_args_list
        if c[0][1].get("event") == "monitor_session_status"
    ]
    assert len(status_events) == 1
    assert status_events[0][0][1]["status"] == "completed"

    mock_d.enqueue_message.assert_awaited_once()
    assert mock_d.enqueue_message.call_args.kwargs["source"] == "monitor:complete"


@pytest.mark.asyncio
async def test_mark_complete_endpoint(client, session_factory):
    task_id, ms_id = await _seed_via_api(client, session_factory)
    mock_d = _mock_main_dispatcher()

    with patch("backend.main.dispatcher", mock_d):
        resp = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/complete",
            json={"reason": "Build finished successfully"},
        )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "completed"
        assert ms.completed_at is not None
        assert ms.last_summary == "Build finished successfully"

        result = await db.execute(
            select(MonitorCheck).where(MonitorCheck.monitor_session_id == ms_id)
        )
        check = result.scalars().one()
        assert check.status == "completed"
        assert check.summary == "Build finished successfully"

    events = {c[0][1].get("event") for c in mock_d.broadcaster.broadcast.call_args_list}
    assert "monitor_check" in events
    assert "monitor_session_status" in events

    # Completion is relayed to the main agent
    mock_d.enqueue_message.assert_awaited_once()
    assert mock_d.enqueue_message.call_args.kwargs["source"] == "monitor:complete"


@pytest.mark.asyncio
async def test_report_check_session_not_running(client, session_factory):
    task_id, ms_id = await _seed_via_api(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(MonitorSession).where(MonitorSession.id == ms_id)
            .values(status="completed")
        )
        await db.commit()

    resp = await client.post(
        f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/checks",
        json={"summary": "late report", "status": "success"},
    )
    assert resp.status_code == 400
