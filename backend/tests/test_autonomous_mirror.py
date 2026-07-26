"""PTY autonomous-turn 全量镜像测试。

背景（2026-07-13 task 27 实录）：后台监视器正点回调、session 自主醒来写出
完整报告，但 adapter 在 chat turn 结束时把 on_autonomous_event 降级成
_subagent_only_callback，报告只存在于 JSONL、聊天永久不可见。

修复两半：
- FullMirrorCCMBackend.on_exit 在 super() 降级后原位换回全量转发；
- _process_event 对 autonomous user-role 事件消毒（<task-notification> 压成
  一行 system_event，其余丢弃），承担历史上"重放旧 prompt"的防线。
"""
import asyncio
from datetime import datetime, timedelta

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select

from backend.services.instance_manager import (
    InstanceManager,
    LaunchSupersededError,
)
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.log_entry import LogEntry


async def _make_inst_task(db_factory):
    async with db_factory() as db:
        inst = Instance(name="t-mirror")
        task = Task(title="t", description="d")
        db.add(inst)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        return inst.id, task.id


def _make_im(db_factory):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return InstanceManager(db_factory, broadcaster), broadcaster


async def _entries(db_factory, task_id):
    async with db_factory() as db:
        result = await db.execute(
            select(LogEntry).where(LogEntry.task_id == task_id).order_by(LogEntry.id)
        )
        return result.scalars().all()


class TestAutonomousUserSanitization:
    """_process_event：autonomous user-role 事件绝不入库为用户消息。"""

    async def test_task_notification_becomes_system_event(self, db_factory):
        inst_id, task_id = await _make_inst_task(db_factory)
        im, broadcaster = _make_im(db_factory)

        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "user",
            "content": (
                "<task-notification>\n<task-id>bjv0gacf8</task-id>\n"
                "<tool-use-id>toolu_x</tool-use-id>\n"
                "<status>completed</status>\n</task-notification>"
            ),
            "autonomous": True,
        })

        entries = await _entries(db_factory, task_id)
        assert len(entries) == 1
        assert entries[0].event_type == "system_event"
        assert entries[0].role == "system"
        assert "bjv0gacf8" in entries[0].content
        assert "completed" in entries[0].content
        # 广播的也是消毒后的 system_event
        broadcast_events = [
            c.args[1] for c in broadcaster.broadcast.await_args_list
            if c.args[0] == f"task:{task_id}"
        ]
        assert any(e.get("event_type") == "system_event" for e in broadcast_events)
        assert not any(e.get("role") == "user" for e in broadcast_events)

    async def test_channel_echo_dropped(self, db_factory):
        """channel 注入回显（发送时已入库过）直接丢弃，不重复。"""
        inst_id, task_id = await _make_inst_task(db_factory)
        im, broadcaster = _make_im(db_factory)

        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "user",
            "content": '<channel source="pty-bridge">\n看下进度\n</channel>',
            "autonomous": True,
        })

        assert await _entries(db_factory, task_id) == []
        broadcaster.broadcast.assert_not_awaited()

    async def test_non_autonomous_user_event_unchanged(self, db_factory):
        """非 autonomous 的 user 事件维持原行为（turn 内 orphan 回填依赖它）。"""
        inst_id, task_id = await _make_inst_task(db_factory)
        im, _ = _make_im(db_factory)

        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "user",
            "content": '<channel source="pty-bridge">\n看下进度\n</channel>',
        })

        entries = await _entries(db_factory, task_id)
        assert len(entries) == 1
        assert entries[0].role == "user"

    async def test_autonomous_assistant_message_logged_and_unread(self, db_factory):
        """autonomous assistant 产出正常入库 + 亮未读 + 广播（修复的主目标）。"""
        inst_id, task_id = await _make_inst_task(db_factory)
        im, broadcaster = _make_im(db_factory)

        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "assistant",
            "content": "# 第 5 轮结果：持平 20.78，没有再提高",
            "autonomous": True,
        })

        entries = await _entries(db_factory, task_id)
        assert len(entries) == 1
        assert entries[0].event_type == "message"
        assert "20.78" in entries[0].content
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.has_unread is True
        channels = [c.args[0] for c in broadcaster.broadcast.await_args_list]
        assert f"task:{task_id}" in channels

    async def test_detached_autonomous_event_cannot_touch_reused_instance(
        self,
        db_factory,
    ):
        """An idle PTY callback remains task-scoped after its slot is reused."""

        heartbeat = datetime.utcnow() - timedelta(minutes=5)
        async with db_factory() as db:
            inst = Instance(
                name="reused-autonomous-slot",
                status="running",
                pid=8831,
                last_heartbeat=heartbeat,
            )
            old_task = Task(
                title="old",
                description="old",
                status="completed",
                session_id="session-old",
            )
            new_task = Task(
                title="new",
                description="new",
                status="executing",
                session_id="session-new",
            )
            db.add_all([inst, old_task, new_task])
            await db.flush()
            inst.current_task_id = new_task.id
            new_task.instance_id = inst.id
            await db.commit()
            inst_id = inst.id
            old_task_id = old_task.id

        im, broadcaster = _make_im(db_factory)
        await im._process_event(
            inst_id,
            old_task_id,
            {
                "event_type": "message",
                "role": "assistant",
                "content": "late autonomous report",
                "autonomous": True,
                "context_usage": {
                    "input_tokens": 30,
                    "total_input_tokens": 30,
                },
            },
            detached_autonomous=True,
            expected_session_id="session-old",
        )

        async with db_factory() as db:
            current_instance = await db.get(Instance, inst_id)
            current_old_task = await db.get(Task, old_task_id)
            assert current_instance.last_heartbeat == heartbeat
            assert current_instance.current_task_id == new_task.id
            assert current_old_task.has_unread is True
            assert current_old_task.context_window_usage is None

        channels = [c.args[0] for c in broadcaster.broadcast.await_args_list]
        assert f"task:{old_task_id}" in channels
        assert f"instance:{inst_id}" not in channels


class TestFullMirrorBackend:
    """on_exit 后把降级的 subagent-only 回调换回全量转发。"""

    def _bare_backend(self, im=None):
        from backend.services.pty_full_mirror import FullMirrorCCMBackend
        backend = object.__new__(FullMirrorCCMBackend)  # 跳过 BridgeHub 启动
        backend._im = im or MagicMock()
        backend._sessions = {}
        backend._consumers = {}
        backend._proxies = {}
        return backend

    async def test_foreground_event_forwards_immutable_consumer_record(self):
        im = MagicMock()
        im._process_event = AsyncMock()
        im.wait_for_pty_launch_metadata = AsyncMock()
        backend = self._bare_backend(im)
        consumer = asyncio.current_task()
        record = MagicMock()
        previous = getattr(
            consumer, "_ccm_output_consumer_record", None
        )
        setattr(consumer, "_ccm_output_consumer_record", record)
        try:
            event = {
                "event_type": "message",
                "role": "assistant",
                "content": "foreground",
            }
            await backend.on_event(
                7,
                event,
                task_id=27,
                loop_iteration=3,
            )
        finally:
            if previous is None:
                delattr(consumer, "_ccm_output_consumer_record")
            else:
                setattr(
                    consumer,
                    "_ccm_output_consumer_record",
                    previous,
                )

        im._process_event.assert_awaited_once_with(
            7,
            27,
            event,
            3,
            consumer_record=record,
        )
        im.wait_for_pty_launch_metadata.assert_awaited_once_with(7)

    @pytest.mark.parametrize(
        (
            "status",
            "expected_exit",
            "expects_answer",
            "quota_before_echo",
            "expected_event_error",
        ),
        [
            ("allowed_warning", 0, True, False, False),
            ("rejected", 1, False, False, True),
            ("rejected", 0, True, True, True),
        ],
    )
    async def test_structured_quota_status_only_ends_hard_limit(
        self,
        status,
        expected_exit,
        expects_answer,
        quota_before_echo,
        expected_event_error,
    ):
        """Exercise the pinned Session generator through FullMirror._consume."""

        from claude_pty.config import PTYConfig
        from claude_pty.jsonl_reader import JsonlReader
        from claude_pty.session import Session

        im = MagicMock()
        im.wait_for_pty_launch_metadata = AsyncMock()
        im._process_event = AsyncMock()
        backend = self._bare_backend(im)
        backend.on_exit = AsyncMock()

        prompt = "hello"
        quota = {
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": status,
                "rateLimitType": "five_hour",
                "utilization": 0.95,
            },
        }
        current_turn_start = [
            {
                "type": "user",
                "message": {"content": prompt},
            },
        ]
        if not quota_before_echo:
            current_turn_start.append(quota)
        batches = [[], [quota]] if quota_before_echo else [[]]
        batches.append(current_turn_start)
        if expects_answer:
            batches.append(
                [
                {
                    "type": "assistant",
                    "message": {
                        "content": [
                            {"type": "text", "text": "real answer"},
                        ],
                    },
                },
                {"type": "system", "subtype": "turn_duration"},
                ]
            )

        delegate = JsonlReader("/nonexistent")

        class FakeReader:
            def read_new_messages(self):
                return batches.pop(0) if batches else []

            def normalize(self, *args, **kwargs):
                return delegate.normalize(*args, **kwargs)

            def is_prompt_echo(self, *args, **kwargs):
                return delegate.is_prompt_echo(*args, **kwargs)

            def is_response_complete(self, *args, **kwargs):
                return delegate.is_response_complete(*args, **kwargs)

        class FakeProcess:
            is_alive = True
            exit_code = 0
            session_id = "quota-session"
            rate_limited = False

            def send_prompt(self, _text):
                pass

            def clear_rate_limited(self):
                self.rate_limited = False

        session = Session(
            cwd="/repo",
            session_id="quota-session",
            config=PTYConfig(
                response_timeout=1,
                jsonl_poll_interval=0,
                post_response_wait=0,
                subagent_check_interval=float("inf"),
            ),
        )
        session._started = True
        session._process = FakeProcess()
        session._reader = FakeReader()

        consumer = asyncio.create_task(
            backend._consume(
                7,
                session,
                prompt,
                task_id=27,
                chat_initiated=True,
            )
        )
        proxy = MagicMock(session=session)
        record = MagicMock(process=proxy)
        setattr(consumer, "_ccm_output_consumer_record", record)
        backend._consumers[7] = consumer
        backend._sessions[7] = session
        await consumer

        forwarded = [
            call.args[2]
            for call in im._process_event.await_args_list
        ]
        quota_event = next(
            event
            for event in forwarded
            if event.get("event_type") == "rate_limit_event"
        )
        assert quota_event["rate_limit_info"]["status"] == status
        assert quota_event["is_error"] is expected_event_error
        assert bool(quota_event.get("orphan")) is quota_before_echo
        assert any(
            event.get("content") == "real answer"
            for event in forwarded
        ) is expects_answer
        assert session.rate_limited is (expected_exit != 0)
        assert backend.on_exit.await_args.args[1] == expected_exit

    def test_restore_replaces_subagent_only(self):
        backend = self._bare_backend()
        session = MagicMock()

        async def _subagent_only_callback(event, **ctx):
            pass

        session.on_autonomous_event = _subagent_only_callback
        backend._restore_full_autonomous_mirror(session, 7, 27, None)
        assert session.on_autonomous_event is not _subagent_only_callback
        assert session.on_autonomous_event.__name__ == "_full_autonomous_mirror"

    async def test_on_exit_waits_for_initial_running_metadata_barrier(self):
        release_metadata = asyncio.Event()
        wait_entered = asyncio.Event()
        im = MagicMock()

        async def wait_for_metadata(instance_id):
            assert instance_id == 7
            wait_entered.set()
            await release_metadata.wait()

        im.wait_for_pty_launch_metadata = AsyncMock(
            side_effect=wait_for_metadata
        )
        im.finalize_pty_container_exec = AsyncMock()
        backend = self._bare_backend(im)
        session = MagicMock()
        session._reader._tracker.has_pending = False

        with patch(
            "backend.services.pty_full_mirror.CCMBackend.on_exit",
            new_callable=AsyncMock,
        ) as base_on_exit:
            exiting = asyncio.create_task(backend.on_exit(
                7,
                0,
                session=session,
                task_id=27,
            ))
            await wait_entered.wait()
            base_on_exit.assert_not_awaited()
            release_metadata.set()
            await exiting
            im.finalize_pty_container_exec.assert_awaited_once_with(
                7, expected_process=None
            )
            # FullMirror owns exact terminal bookkeeping locally; delegating
            # would reintroduce the dependency's id-only stale writes.
            base_on_exit.assert_not_awaited()

    async def test_exact_pty_generation_finalizes_task_and_instance(
        self, db_factory
    ):
        im, broadcaster = _make_im(db_factory)
        backend = self._bare_backend(im)
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 4
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 321
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 321
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        backend._sessions[instance_id] = session
        backend._proxies[instance_id] = proxy
        im._launch_params[instance_id] = {"_retried": True}

        async def exit_turn():
            consumer = asyncio.current_task()
            backend._consumers[instance_id] = consumer
            im.processes[instance_id] = proxy
            im._track_output_consumer(
                instance_id,
                proxy,
                consumer,
                chat_initiated=True,
                provider="claude",
                task_id=task_id,
                task_retry_count=4,
                instance_started_at=started_at,
            )
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        await exit_turn()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "completed"
            assert task.retry_count == 4
            assert inst.status == "idle"
            assert inst.pid is None
            assert inst.current_task_id is None
        assert proxy.returncode == 0
        status_events = [
            call.args[1]
            for call in broadcaster.broadcast.await_args_list
            if call.args[0] == "tasks"
        ]
        assert any(
            event.get("new_status") == "completed"
            for event in status_events
        )

    async def test_stop_owned_pty_exit_does_not_reenter_lifecycle_lock(
        self, db_factory
    ):
        """Task 257: stop awaiting on_exit must not deadlock on its own lock."""

        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        im._pty_backend = backend
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            # stop-session terminalizes the Task before stopping its process.
            task.status = "completed"
            task.retry_count = 3
            task.instance_id = instance_id
            task.completed_at = started_at
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 25_701
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 25_701
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        begin = asyncio.Event()

        async def consume_until_stopped():
            await begin.wait()
            try:
                await asyncio.Event().wait()
            finally:
                await backend.on_exit(
                    instance_id,
                    130,
                    session=session,
                    task_id=task_id,
                    chat_initiated=True,
                )

        consumer = asyncio.create_task(consume_until_stopped())
        backend._sessions[instance_id] = session
        backend._consumers[instance_id] = consumer
        backend._proxies[instance_id] = proxy
        im.processes[instance_id] = proxy
        record = im._track_output_consumer(
            instance_id,
            proxy,
            consumer,
            chat_initiated=True,
            provider="claude",
            task_id=task_id,
            task_retry_count=3,
            instance_started_at=started_at,
        )

        async def stop_backend(key):
            assert key == instance_id
            assert record.pty_terminal_owner == "stop"
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

        backend.stop = stop_backend
        im._wait_process_tree = AsyncMock()
        begin.set()
        await asyncio.sleep(0)

        stopping = asyncio.create_task(
            im.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=proxy.pid,
                expected_started_at=started_at,
                task_status="completed",
                consumer_cancel_timeout=0.2,
            )
        )
        done, _ = await asyncio.wait({stopping}, timeout=1)
        if not done:
            # Keep a future regression from wedging the whole test process:
            # a second cancellation interrupts an on_exit blocked on the lock.
            consumer.cancel()
            await asyncio.wait({stopping}, timeout=1)
            pytest.fail("PTY stop deadlocked while awaiting consumer on_exit")
        assert await stopping is True

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "completed"
            assert inst.status == "idle"
            assert inst.pid is None
            assert inst.current_task_id is None
        assert record.pty_terminal_owner == "stop"
        assert instance_id not in im.processes
        assert instance_id not in im._tasks
        assert instance_id not in im._consumer_records
        assert instance_id not in im._stopping

    async def test_consumer_owned_pty_exit_makes_stop_wait_outside_lock(
        self, db_factory
    ):
        """A naturally exiting consumer can win without racing stop cleanup."""

        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        im._pty_backend = backend
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 5
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 25_702
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 25_702
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        begin_exit = asyncio.Event()
        container_finalize_entered = asyncio.Event()
        release_container_finalize = asyncio.Event()

        async def gated_container_finalize(*args, **kwargs):
            container_finalize_entered.set()
            await release_container_finalize.wait()

        im.finalize_pty_container_exec = gated_container_finalize

        async def exit_naturally():
            await begin_exit.wait()
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        consumer = asyncio.create_task(exit_naturally())
        backend._sessions[instance_id] = session
        backend._consumers[instance_id] = consumer
        backend._proxies[instance_id] = proxy
        im.processes[instance_id] = proxy
        record = im._track_output_consumer(
            instance_id,
            proxy,
            consumer,
            chat_initiated=True,
            provider="claude",
            task_id=task_id,
            task_retry_count=5,
            instance_started_at=started_at,
        )
        backend.stop = AsyncMock(
            side_effect=AssertionError(
                "consumer-owned terminal path must not call backend.stop"
            )
        )

        begin_exit.set()
        await container_finalize_entered.wait()
        assert record.pty_terminal_owner == "consumer"

        stopping = asyncio.create_task(
            im.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=proxy.pid,
                expected_started_at=started_at,
                task_status="completed",
                terminal_consumer_timeout=1,
                consumer_cancel_timeout=0.2,
            )
        )
        for _ in range(100):
            if instance_id in im._stopping:
                break
            await asyncio.sleep(0.01)
        assert instance_id in im._stopping
        assert not im._instance_lifecycle_lock(instance_id).locked()
        with pytest.raises(
            RuntimeError, match="being stopped"
        ):
            await im.launch(instance_id, "must not race stop")

        release_container_finalize.set()
        assert await asyncio.wait_for(stopping, timeout=1) is True
        backend.stop.assert_not_awaited()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "completed"
            assert inst.status == "idle"
            assert inst.pid is None
            assert inst.current_task_id is None
        assert record.pty_terminal_owner == "consumer"
        assert instance_id not in im.processes
        assert instance_id not in im._tasks
        assert instance_id not in im._consumer_records
        assert instance_id not in im._stopping

    async def test_stop_takes_over_failed_completed_consumer(
        self, db_factory
    ):
        """A failed consumer cannot strand its terminal-owner claim."""

        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        im._pty_backend = backend
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 6
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 25_703
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 25_703
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        begin_exit = asyncio.Event()

        async def fail_during_exit():
            await begin_exit.wait()
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        consumer = asyncio.create_task(fail_during_exit())
        backend._sessions[instance_id] = session
        backend._consumers[instance_id] = consumer
        backend._proxies[instance_id] = proxy
        im.processes[instance_id] = proxy
        record = im._track_output_consumer(
            instance_id,
            proxy,
            consumer,
            chat_initiated=True,
            provider="claude",
            task_id=task_id,
            task_retry_count=6,
            instance_started_at=started_at,
        )
        im.finalize_pty_container_exec = AsyncMock(
            side_effect=RuntimeError("container finalization failed")
        )

        begin_exit.set()
        result = await asyncio.gather(consumer, return_exceptions=True)
        assert isinstance(result[0], RuntimeError)
        assert record.pty_terminal_owner == "consumer"

        async def stop_backend(key):
            assert key == instance_id
            assert record.pty_terminal_owner == "stop"
            proxy.complete(130)

        backend.stop = stop_backend
        im._wait_process_tree = AsyncMock()

        assert await asyncio.wait_for(
            im.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=proxy.pid,
                expected_started_at=started_at,
                task_status="completed",
                consumer_cancel_timeout=0.2,
            ),
            timeout=1,
        ) is True

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "completed"
            assert inst.status == "idle"
            assert inst.pid is None
            assert inst.current_task_id is None
        assert record.pty_terminal_owner == "stop"
        assert instance_id not in im.processes
        assert instance_id not in im._tasks
        assert instance_id not in im._consumer_records
        assert instance_id not in im._stopping

    async def test_stop_cancels_stalled_consumer_before_owner_takeover(
        self, db_factory
    ):
        """A timed-out live consumer is reaped before stop takes ownership."""

        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        im._pty_backend = backend
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 7
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 25_705
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 25_705
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        begin_exit = asyncio.Event()
        finalizer_entered = asyncio.Event()

        async def never_finish_container_finalizer(*args, **kwargs):
            finalizer_entered.set()
            await asyncio.Event().wait()

        im.finalize_pty_container_exec = never_finish_container_finalizer

        async def stall_during_exit():
            await begin_exit.wait()
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        consumer = asyncio.create_task(stall_during_exit())
        backend._sessions[instance_id] = session
        backend._consumers[instance_id] = consumer
        backend._proxies[instance_id] = proxy
        im.processes[instance_id] = proxy
        record = im._track_output_consumer(
            instance_id,
            proxy,
            consumer,
            chat_initiated=True,
            provider="claude",
            task_id=task_id,
            task_retry_count=7,
            instance_started_at=started_at,
        )

        begin_exit.set()
        await finalizer_entered.wait()
        assert record.pty_terminal_owner == "consumer"

        async def stop_backend(key):
            assert key == instance_id
            assert consumer.done()
            assert record.pty_terminal_owner == "stop"
            proxy.complete(130)

        backend.stop = stop_backend
        im._wait_process_tree = AsyncMock()

        assert await asyncio.wait_for(
            im.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=proxy.pid,
                expected_started_at=started_at,
                task_status="completed",
                terminal_consumer_timeout=0.02,
                consumer_cancel_timeout=0.2,
            ),
            timeout=1,
        ) is True

        assert consumer.cancelled()
        assert record.pty_terminal_owner == "stop"
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "completed"
            assert inst.status == "idle"
            assert inst.pid is None
            assert inst.current_task_id is None
        assert instance_id not in im.processes
        assert instance_id not in im._tasks
        assert instance_id not in im._consumer_records
        assert instance_id not in im._stopping

    async def test_aborted_pty_launch_claims_stop_before_consumer_exit(
        self, db_factory
    ):
        """Launch rollback must not await on_exit while holding its lock."""

        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        backend._pool = MagicMock()
        backend._pool._sessions = {}
        im._pty_backend = backend
        instance_id, task_id = await _make_inst_task(db_factory)

        class Session:
            is_alive = True

            def __init__(self):
                self._reader = MagicMock()
                self._reader._tracker.has_pending = False

        class Proxy:
            pid = 25_704
            returncode = None

            def __init__(self, session):
                self.session = session

            def complete(self, code=0):
                self.returncode = code

        session = Session()
        proxy = Proxy(session)
        consumer = None
        stop_owner_seen = []

        async def consume_until_stopped():
            try:
                await asyncio.Event().wait()
            finally:
                await backend.on_exit(
                    instance_id,
                    130,
                    session=session,
                    task_id=task_id,
                    chat_initiated=True,
                )

        async def launch_for_ccm(**kwargs):
            nonlocal consumer
            consumer = asyncio.create_task(consume_until_stopped())
            backend._sessions[instance_id] = session
            backend._consumers[instance_id] = consumer
            backend._proxies[instance_id] = proxy
            im.processes[instance_id] = proxy
            im._tasks[instance_id] = consumer
            return "aborted-session"

        async def stop_backend(key):
            assert key == instance_id
            record = im._consumer_records[instance_id]
            stop_owner_seen.append(record.pty_terminal_owner)
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

        backend.launch_for_ccm = launch_for_ccm
        backend.stop = stop_backend

        async def launch_while_holding_lifecycle():
            async with im._instance_lifecycle_lock(instance_id):
                return await im._launch_pty(
                    instance_id=instance_id,
                    prompt="must roll back",
                    task_id=task_id,
                    cwd="/tmp",
                    model=None,
                    resume_session_id=None,
                    loop_iteration=None,
                    git_env=None,
                    thinking_budget=None,
                    effort_level=None,
                    chat_initiated=True,
                    config_dir=None,
                    enable_workflows=False,
                    enabled_skills=None,
                    mcp_config_path=None,
                    task_retry_count=0,
                )

        launching = asyncio.create_task(launch_while_holding_lifecycle())
        done, _ = await asyncio.wait({launching}, timeout=1)
        if not done:
            # A second cancellation releases an on_exit that regressed into
            # waiting for the lifecycle lock held by this launch rollback.
            consumer.cancel()
            await asyncio.wait({launching}, timeout=1)
            pytest.fail(
                "PTY launch rollback deadlocked while awaiting consumer on_exit"
            )
        with pytest.raises(LaunchSupersededError):
            await launching

        assert consumer is not None and consumer.done()
        assert stop_owner_seen == ["stop"]
        assert proxy.returncode == 130
        assert instance_id not in im.processes
        assert instance_id not in im._tasks
        assert instance_id not in im._consumer_records
        assert not im._instance_lifecycle_lock(instance_id).locked()
        async with db_factory() as db:
            inst = await db.get(Instance, instance_id)
            assert inst.status == "idle"
            assert inst.pid is None
            assert inst.current_task_id is None

    async def test_failed_pty_generation_records_terminal_timestamp(
        self, db_factory
    ):
        im, _ = _make_im(db_factory)
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        class Proxy:
            pid = 777
            returncode = 9

        proxy = Proxy()
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 2
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = proxy.pid
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        consumer = asyncio.current_task()
        im.processes[instance_id] = proxy
        im._track_output_consumer(
            instance_id,
            proxy,
            consumer,
            chat_initiated=True,
            provider="claude",
            task_id=task_id,
            task_retry_count=2,
            instance_started_at=started_at,
        )
        status = await im.finalize_pty_chat_generation(
            instance_id,
            task_id,
            9,
            im._consumer_records[instance_id],
        )
        assert status == "failed"
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "failed"
            assert task.completed_at is not None
            assert "code 9" in task.error_message
            assert inst.status == "error"

    async def test_pty_api_error_overrides_zero_process_exit(
        self, db_factory
    ):
        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()
        error_text = "API Error: invalid_request_error: unsupported beta"

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 3
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 778
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 778
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        backend._sessions[instance_id] = session
        backend._proxies[instance_id] = proxy
        im._try_chat_transient_retry = AsyncMock(return_value=False)
        im._try_chat_pool_rotation = AsyncMock(return_value=False)

        async def exit_turn():
            consumer = asyncio.current_task()
            backend._consumers[instance_id] = consumer
            im.processes[instance_id] = proxy
            record = im._track_output_consumer(
                instance_id,
                proxy,
                consumer,
                chat_initiated=True,
                provider="claude",
                task_id=task_id,
                task_retry_count=3,
                instance_started_at=started_at,
            )
            await im._process_event(
                instance_id,
                task_id,
                {
                    "event_type": "message",
                    "role": "assistant",
                    "content": error_text,
                    "is_error": True,
                    "raw_json": (
                        '{"type":"assistant","isApiErrorMessage":true}'
                    ),
                },
                consumer_record=record,
            )
            assert record.fatal_provider_error == error_text
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        await exit_turn()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            persisted = (
                await db.execute(
                    select(LogEntry).where(
                        LogEntry.task_id == task_id,
                        LogEntry.content == error_text,
                        LogEntry.is_error.is_(True),
                    )
                )
            ).scalar_one()
            assert persisted.event_type == "message"
            assert task.status == "failed"
            assert task.error_message == error_text
            assert inst.status == "error"
        assert proxy.returncode == 1
        im._try_chat_transient_retry.assert_awaited_once()
        im._try_chat_pool_rotation.assert_awaited_once()

    async def test_soft_quota_warning_keeps_successful_pty_turn_completed(
        self, db_factory
    ):
        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        backend._maybe_retry_empty_reply = AsyncMock()
        instance_id, task_id = await _make_inst_task(db_factory)
        started_at = datetime.utcnow()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = 2
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 779
            inst.current_task_id = task_id
            inst.started_at = started_at
            await db.commit()

        class Proxy:
            pid = 779
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        backend._sessions[instance_id] = session
        backend._proxies[instance_id] = proxy
        im._pty_rate_limit_seen.add(instance_id)
        im._pty_rate_limit_info[instance_id] = {
            "status": "allowed_warning",
            "rateLimitType": "five_hour",
            "utilization": 0.95,
        }
        im._try_chat_transient_retry = AsyncMock(return_value=False)
        im._try_chat_pool_rotation = AsyncMock(return_value=False)

        async def exit_turn():
            consumer = asyncio.current_task()
            backend._consumers[instance_id] = consumer
            im.processes[instance_id] = proxy
            im._track_output_consumer(
                instance_id,
                proxy,
                consumer,
                chat_initiated=True,
                provider="claude",
                task_id=task_id,
                task_retry_count=2,
                instance_started_at=started_at,
            )
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        await exit_turn()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)

        assert task.status == "completed"
        assert inst.status == "idle"
        assert proxy.returncode == 0
        im._try_chat_transient_retry.assert_not_awaited()
        im._try_chat_pool_rotation.assert_not_awaited()

    @pytest.mark.parametrize("changed_field", ["retry", "started_at"])
    async def test_old_pty_exit_cannot_finalize_new_same_task_generation(
        self, db_factory, changed_field
    ):
        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        instance_id, task_id = await _make_inst_task(db_factory)
        old_started_at = datetime.utcnow()
        durable_started_at = (
            old_started_at + timedelta(seconds=1)
            if changed_field == "started_at"
            else old_started_at
        )
        durable_retry = 8 if changed_field == "retry" else 7

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.status = "executing"
            task.retry_count = durable_retry
            task.instance_id = instance_id
            inst = await db.get(Instance, instance_id)
            inst.status = "running"
            inst.pid = 654
            inst.current_task_id = task_id
            inst.started_at = durable_started_at
            await db.commit()

        class Proxy:
            pid = 654
            returncode = None

            def complete(self, code=0):
                self.returncode = code

        proxy = Proxy()
        session = MagicMock()
        session._reader._tracker.has_pending = False
        backend._sessions[instance_id] = session
        backend._proxies[instance_id] = proxy

        async def exit_old_turn():
            consumer = asyncio.current_task()
            backend._consumers[instance_id] = consumer
            im.processes[instance_id] = proxy
            im._track_output_consumer(
                instance_id,
                proxy,
                consumer,
                chat_initiated=True,
                provider="claude",
                task_id=task_id,
                task_retry_count=7,
                instance_started_at=old_started_at,
            )
            await backend.on_exit(
                instance_id,
                0,
                session=session,
                task_id=task_id,
                chat_initiated=True,
            )

        await exit_old_turn()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            inst = await db.get(Instance, instance_id)
            assert task.status == "executing"
            assert task.retry_count == durable_retry
            assert inst.status == "running"
            assert inst.pid == 654
            assert inst.current_task_id == task_id
            assert inst.started_at == durable_started_at

    async def test_stale_pty_callback_keeps_replacement_maps(self, db_factory):
        im, _ = _make_im(db_factory)
        backend = self._bare_backend(im)
        instance_id, task_id = await _make_inst_task(db_factory)

        class Proxy:
            def __init__(self, pid):
                self.pid = pid
                self.returncode = None

            def complete(self, code=0):
                self.returncode = code

        old_proxy = Proxy(111)
        new_proxy = Proxy(222)
        old_session = MagicMock()
        old_session._reader._tracker.has_pending = False
        new_session = MagicMock()
        backend._sessions[instance_id] = new_session
        backend._proxies[instance_id] = new_proxy

        replacement_ready = asyncio.Event()
        release_old = asyncio.Event()

        async def old_exit():
            consumer = asyncio.current_task()
            from backend.services.instance_manager import _OutputConsumerRecord

            old_record = _OutputConsumerRecord(
                old_proxy,
                consumer,
                True,
                "claude",
                task_id,
                0,
                datetime.utcnow(),
            )
            setattr(
                consumer, "_ccm_output_consumer_record", old_record
            )
            replacement_ready.set()
            await release_old.wait()
            await backend.on_exit(
                instance_id,
                0,
                session=old_session,
                task_id=task_id,
                chat_initiated=True,
            )

        old_task = asyncio.create_task(old_exit())
        await replacement_ready.wait()
        new_consumer = asyncio.create_task(asyncio.sleep(60))
        try:
            from backend.services.instance_manager import _OutputConsumerRecord

            new_record = _OutputConsumerRecord(
                new_proxy,
                new_consumer,
                True,
                "claude",
                task_id,
                0,
                datetime.utcnow(),
            )
            backend._consumers[instance_id] = new_consumer
            im._tasks[instance_id] = new_consumer
            im._consumer_records[instance_id] = new_record
            im.processes[instance_id] = new_proxy
            release_old.set()
            await old_task
            assert backend._proxies[instance_id] is new_proxy
            assert backend._sessions[instance_id] is new_session
            assert backend._consumers[instance_id] is new_consumer
            assert im.processes[instance_id] is new_proxy
            assert im._tasks[instance_id] is new_consumer
            assert im._consumer_records[instance_id] is new_record
            assert old_proxy.returncode == 0
            assert new_proxy.returncode is None
        finally:
            new_consumer.cancel()
            await asyncio.gather(new_consumer, return_exceptions=True)

    async def test_mirror_forwards_to_process_event(self):
        im = MagicMock()
        im._process_event = AsyncMock()
        backend = self._bare_backend(im)
        session = MagicMock()

        async def _subagent_only_callback(event, **ctx):
            pass

        session.on_autonomous_event = _subagent_only_callback
        session.session_id = "session-27"
        backend._restore_full_autonomous_mirror(session, 7, 27, 3)

        event = MagicMock()
        event.to_dict.return_value = {
            "event_type": "message", "role": "assistant",
            "content": "hi", "autonomous": True,
        }
        await session.on_autonomous_event(event)
        im._process_event.assert_awaited_once_with(
            7,
            27,
            event.to_dict.return_value,
            3,
            detached_autonomous=True,
            expected_session_id="session-27",
        )

    async def test_mirror_swallows_process_event_errors(self):
        """镜像回调绝不向 idle watcher 抛异常。"""
        im = MagicMock()
        im._process_event = AsyncMock(side_effect=RuntimeError("db down"))
        backend = self._bare_backend(im)
        session = MagicMock()

        async def _subagent_only_callback(event, **ctx):
            pass

        session.on_autonomous_event = _subagent_only_callback
        backend._restore_full_autonomous_mirror(session, 7, 27, None)
        event = MagicMock()
        event.to_dict.return_value = {"event_type": "message"}
        await session.on_autonomous_event(event)  # 不抛

    def test_restore_skips_fresh_binding(self):
        """launch 重新绑定的 _on_autonomous（轮换 relaunch）不得被覆盖。"""
        backend = self._bare_backend()
        session = MagicMock()

        async def _on_autonomous(event):
            pass

        session.on_autonomous_event = _on_autonomous
        backend._restore_full_autonomous_mirror(session, 7, 27, None)
        assert session.on_autonomous_event is _on_autonomous

    def test_restore_skips_none_session(self):
        backend = self._bare_backend()
        backend._restore_full_autonomous_mirror(None, 7, 27, None)  # 不抛

    def test_init_wires_full_mirror_backend(self, db_factory):
        """use_pty_mode 默认开：IM 构造时就应接上 FullMirrorCCMBackend。"""
        fake_cls = MagicMock()
        with patch(
            "backend.services.pty_full_mirror.FullMirrorCCMBackend", fake_cls
        ):
            im, _ = _make_im(db_factory)
        fake_cls.assert_called_once_with(im)
        assert im._pty_enabled is True
