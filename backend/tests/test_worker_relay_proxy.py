"""Phase 2 测试：WorkerRelay 事件处理 / Dispatcher 双路径 / Chat 与操作代理。"""
import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select, update

import backend.main as main_module
import backend.api.tasks as tasks_api_module
import backend.services.task_events as task_events_module
import backend.services.worker_proxy as worker_proxy_module
import backend.services.worker_relay as worker_relay_module
from backend.models.log_entry import LogEntry
from backend.models.instance import Instance
from backend.models.monitor_session import MonitorCheck, MonitorSession
from backend.models.project import Project
from backend.models.task import Task
from backend.models.user_skill import UserSkill
from backend.models.worker import Worker
from backend.schemas.task import TaskCreate
from backend.services.worker_proxy import (
    WorkerEndpointNotFoundError,
    WorkerProxy,
)
from backend.services.worker_relay import WorkerRelay
from backend.services.worker_routing_config import (
    WORKER_ROUTING_PENDING_KEY,
)


class FakeBroadcaster:
    def __init__(self):
        self.sent: list[tuple[str, dict]] = []

    async def broadcast(self, channel, data):
        self.sent.append((channel, data))


@pytest.fixture
def broadcaster():
    return FakeBroadcaster()


@pytest.fixture
def relay(db_factory, broadcaster):
    r = WorkerRelay(db_factory=db_factory, broadcaster=broadcaster)
    return r


async def test_concurrent_worker_connection_admission_creates_one_transport(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    entered = asyncio.Event()
    release = asyncio.Event()
    sockets = []

    class FakeWebSocket:
        def __init__(self):
            self.sent = []
            self.closed = False

        async def send(self, payload):
            self.sent.append(json.loads(payload))

        async def close(self):
            self.closed = True

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()

    async def connect(*_args, **_kwargs):
        socket = FakeWebSocket()
        sockets.append(socket)
        entered.set()
        await release.wait()
        return socket

    monkeypatch.setattr(
        worker_relay_module.websockets,
        "connect",
        connect,
    )
    first = asyncio.create_task(relay.ensure_connection(worker))
    await entered.wait()
    second = asyncio.create_task(relay.ensure_connection(worker))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert len(sockets) == 1
    assert relay._ws[worker.id] is sockets[0]
    assert len(relay._loops) == 1
    await relay.stop_worker(worker.id)


async def _mk_worker(session_factory, **fields) -> Worker:
    fields.setdefault("status", "ready")
    fields.setdefault("private_ip", "10.0.0.9")
    fields.setdefault("auth_token", "wtoken")
    async with session_factory() as db:
        w = Worker(name="w1", **fields)
        db.add(w)
        await db.commit()
        await db.refresh(w)
        return w


async def _mk_task(session_factory, **fields) -> Task:
    fields.setdefault("status", "in_progress")
    fields.setdefault("description", "d")
    async with session_factory() as db:
        t = Task(title="t", **fields)
        db.add(t)
        await db.commit()
        await db.refresh(t)
        return t


def _remote_task(task: Task, **overrides) -> dict:
    payload = {
        "id": task.id,
        "status": task.status,
        "retry_count": task.retry_count,
        "session_id": task.session_id,
        "started_at": (
            task.started_at.isoformat() if task.started_at else None
        ),
        "completed_at": (
            task.completed_at.isoformat() if task.completed_at else None
        ),
        "error_message": task.error_message,
    }
    payload.update(overrides)
    return payload


def _routing_snapshot(
    task: Task,
    *,
    status: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    codex_service_tier: str | None = None,
    pending: dict | None = None,
) -> dict:
    return {
        "id": task.id,
        "status": status or task.status,
        "worker_id": None,
        "shared_from_id": None,
        "provider": provider or task.provider,
        "model": task.model if model is None else model,
        "codex_service_tier": (
            codex_service_tier or task.codex_service_tier
        ),
        "pending": pending,
    }


async def test_authoritative_worker_apply_preserves_supersede_marker(
    session_factory,
):
    """Normal relay/proxy convergence cannot drop a lost-response gate."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        metadata_={"pr_review_id": 37},
    )
    observed = worker_relay_module.worker_task_generation(task)
    assert observed is not None

    async with session_factory() as db:
        resulting = await (
            worker_relay_module.apply_authoritative_worker_task(
                db,
                observed,
                _remote_task(
                    task,
                    status="completed",
                    metadata_={"pr_review_superseded": True},
                ),
            )
        )

    assert resulting is not None
    assert resulting.status == "completed"
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.metadata_ == {
            "pr_review_id": 37,
            "pr_review_superseded": True,
        }


def test_worker_proxy_ssh_is_scoped_to_cloud_instance(monkeypatch):
    ssh_factory = Mock()
    monkeypatch.setattr(worker_proxy_module, "SSHExecutor", ssh_factory)
    monkeypatch.setattr(
        worker_proxy_module,
        "worker_known_hosts_path",
        Mock(return_value="/tmp/known-hosts/i-worker-proxy"),
    )
    proxy = WorkerProxy(None, relay=AsyncMock())
    worker = Worker(
        name="scoped-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        ssh_key_path="/tmp/worker-key",
        cloud_instance_id="i-worker-proxy",
    )

    proxy._ssh(worker)

    worker_proxy_module.worker_known_hosts_path.assert_called_once_with(
        "i-worker-proxy"
    )
    ssh_factory.assert_called_once_with(
        host="10.0.0.9",
        user="ubuntu",
        key_path="/tmp/worker-key",
        known_hosts_path="/tmp/known-hosts/i-worker-proxy",
    )


async def test_worker_task_operation_lock_blocks_concurrent_reforward():
    proxy = WorkerProxy(None, relay=AsyncMock())
    task = Task(id=321, title="remote", worker_id=7)
    forwarded = asyncio.Event()

    async def record_forward(_task):
        forwarded.set()

    proxy._forward_task_to_worker_locked = AsyncMock(
        side_effect=record_forward
    )
    operation_lock = proxy.task_operation_lock(task.id)
    await operation_lock.acquire()
    forward_task = asyncio.create_task(proxy.forward_task_to_worker(task))
    await asyncio.sleep(0)
    assert not forwarded.is_set()

    operation_lock.release()
    await forward_task
    assert forwarded.is_set()


async def test_worker_forward_preserves_pr_review_tag_through_task_create(
    monkeypatch,
):
    """The Worker copy retains the internal endpoint's routing fallback tag."""

    captured_payload = {}

    class Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response({
                "default_codex_model": "gpt-5.6-sol",
                "codex_model_service_tiers": {
                    "gpt-5.6-sol": ["default", "priority"],
                },
            })

        async def post(self, _url, *, headers, json):
            captured_payload.update(json)
            return Response(json)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = AsyncMock()
    proxy = WorkerProxy(None, relay)
    worker = Worker(
        id=77,
        name="worker",
        status="ready",
        private_ip="10.0.0.77",
        auth_token="token",
    )
    task = Task(
        id=901,
        title="PR Review: owner/repo#1",
        description="review",
        worker_id=worker.id,
        project_id=12,
        priority=0,
        max_retries=2,
        mode="auto",
        max_iterations=50,
        must_complete=False,
        goal_max_turns=30,
        provider="codex",
        codex_service_tier="priority",
        enable_workflows=False,
        selected_user_skills=[5],
        tags=["pr-review"],
        metadata_={"pr_review_id": 123},
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)
    proxy._user_skill_snapshots = AsyncMock(return_value=[{
        "id": 5,
        "name": "Manager skill",
        "description": "copied",
        "content": "body",
    }])

    await proxy._forward_task_to_worker_locked(task)

    parsed_on_worker = TaskCreate.model_validate(captured_payload)
    assert captured_payload["tags"] == ["pr-review"]
    assert captured_payload["selected_user_skills"] == [5]
    assert captured_payload["user_skill_snapshots"] == [{
        "id": 5,
        "name": "Manager skill",
        "description": "copied",
        "content": "body",
    }]
    assert captured_payload["codex_service_tier"] == "priority"
    assert parsed_on_worker.tags == ["pr-review"]
    assert parsed_on_worker.codex_service_tier == "priority"
    # metadata_ is intentionally not a public TaskCreate field; the hidden
    # termination endpoint accepts the forwarded tag only for Worker copies.
    assert not hasattr(parsed_on_worker, "metadata_")


async def test_worker_skill_selection_syncs_before_follow_up(monkeypatch):
    captured_payload = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": task.id,
                "status": task.status,
                "retry_count": task.retry_count,
                # Worker instance ids belong to a different database and are
                # intentionally not comparable with the Manager mirror.
                "instance_id": None,
                "enabled_skills": captured_payload["enabled_skills"],
                "selected_user_skills": captured_payload[
                    "selected_user_skills"
                ],
                "metadata_": {
                    "ccm_user_skill_snapshots": captured_payload[
                        "user_skill_snapshots"
                    ],
                },
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def put(self, _url, *, headers, json):
            captured_payload.update(json)
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    proxy._user_skill_snapshots = AsyncMock(return_value=[{
        "id": 6,
        "name": "Updated skill",
        "description": "latest selection",
        "content": "body",
    }])
    worker = Worker(
        id=78,
        name="worker",
        private_ip="10.0.0.78",
        auth_token="token",
    )
    task = Task(
        id=902,
        title="remote",
        description="continue",
        worker_id=worker.id,
        enabled_skills={"code-review": True},
        selected_user_skills=[6],
    )

    await proxy.sync_task_skill_selection(worker, task)

    assert captured_payload == {
        "enabled_skills": {"code-review": True},
        "selected_user_skills": [6],
        "user_skill_snapshots": [{
            "id": 6,
            "name": "Updated skill",
            "description": "latest selection",
            "content": "body",
        }],
    }


async def test_worker_skill_selection_sync_fails_closed_on_stale_confirmation(
    monkeypatch,
):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": task.id,
                "status": task.status,
                "retry_count": task.retry_count,
                "instance_id": task.instance_id,
                "enabled_skills": {},
                "selected_user_skills": [],
                "metadata_": {"ccm_user_skill_snapshots": []},
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def put(self, _url, *, headers, json):
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, relay=AsyncMock())
    proxy._user_skill_snapshots = AsyncMock(return_value=[{
        "id": 9,
        "name": "Authoritative",
        "description": "Manager copy",
        "content": "body",
    }])
    worker = Worker(
        id=79,
        name="worker",
        private_ip="10.0.0.79",
        auth_token="token",
    )
    task = Task(
        id=903,
        title="remote",
        description="continue",
        worker_id=worker.id,
        instance_id=444,
        enabled_skills={"code-review": True},
        selected_user_skills=[9],
    )

    with pytest.raises(HTTPException) as exc:
        await proxy.sync_task_skill_selection(worker, task)

    assert exc.value.status_code == 409
    assert "execution was blocked" in exc.value.detail


@pytest.mark.parametrize(
    "local_collision",
    [False, True],
    ids=["missing-local-row", "colliding-local-row"],
)
async def test_worker_proxy_uses_authoritative_user_skill_snapshots(
    session_factory,
    local_collision,
):
    from backend.models.user_skill import UserSkill

    if local_collision:
        async with session_factory() as db:
            db.add(UserSkill(
                id=86,
                name="Wrong Worker-local skill",
                description="must not replace Manager snapshot",
                content="wrong local body",
            ))
            await db.commit()

    authoritative = [{
        "id": 86,
        "name": "Manager snapshot",
        "description": "authoritative",
        "content": "correct Manager body",
    }]
    task = Task(
        id=986,
        title="snapshot forwarding",
        selected_user_skills=[86],
        metadata_={"ccm_user_skill_snapshots": authoritative},
    )
    proxy = WorkerProxy(session_factory, relay=AsyncMock())

    assert await proxy._user_skill_snapshots(task) == authoritative


async def test_worker_fast_fails_before_forward_when_capability_is_unproven(
    monkeypatch,
):
    post = AsyncMock()

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            # Shape returned by an older Worker that predates service tiers.
            return {
                "default_codex_model": "gpt-5.6-sol",
            }

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response()

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    relay = AsyncMock()
    proxy = WorkerProxy(None, relay)
    worker = Worker(
        id=78,
        name="old-worker",
        status="ready",
        private_ip="10.0.0.78",
        auth_token="token",
    )
    task = Task(
        id=902,
        title="Fast remote task",
        description="run fast",
        worker_id=worker.id,
        project_id=12,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    proxy.get_worker = AsyncMock(return_value=worker)
    proxy.ensure_worker_project = AsyncMock(return_value=34)

    with pytest.raises(RuntimeError, match="未声明.*支持 Codex Fast"):
        await proxy._forward_task_to_worker_locked(task)

    proxy.ensure_worker_project.assert_not_awaited()
    relay.subscribe_task.assert_not_awaited()
    post.assert_not_awaited()


async def test_worker_fast_preflight_resolves_default_model_alias(
    monkeypatch,
):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "default_codex_model": "gpt-5.6-sol",
                "codex_model_service_tiers": {
                    "gpt-5.6-sol": ["default", "priority"],
                },
            }

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, _url, *, headers):
            return Response()

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    proxy = WorkerProxy(None, AsyncMock())
    worker = Worker(
        id=79,
        name="worker",
        status="ready",
        private_ip="10.0.0.79",
        auth_token="token",
    )
    task = Task(
        id=903,
        title="Fast default-model task",
        description="run fast",
        worker_id=worker.id,
        provider="codex",
        model="default",
        codex_service_tier="priority",
    )

    await proxy.require_worker_fast_support(worker, task)


# === WorkerRelay._handle ===


async def test_relay_chat_event_stored_and_forwarded(relay, broadcaster, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}

    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event_type": "message", "role": "assistant", "content": "hi",
                 "instance_id": 7},
    }, w)

    async with session_factory() as db:
        logs = (await db.execute(select(LogEntry).where(LogEntry.task_id == t.id))).scalars().all()
        task = await db.get(Task, t.id)
    assert len(logs) == 1
    assert logs[0].instance_id is None
    assert logs[0].content == "hi"
    assert task.has_unread is True
    # 镜像广播到同名 channel，且剥掉 worker 的 instance_id
    assert broadcaster.sent == [(f"task:{t.id}", {"event_type": "message", "role": "assistant", "content": "hi"})]


async def test_relay_skips_user_message_and_unsubscribed(relay, broadcaster, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}

    await relay._handle({"channel": f"task:{t.id}",
                         "data": {"event_type": "user_message", "content": "x"}}, w)
    await relay._handle({"channel": "task:99999",
                         "data": {"event_type": "message", "content": "x"}}, w)

    async with session_factory() as db:
        count = len((await db.execute(select(LogEntry))).scalars().all())
    assert count == 0
    assert broadcaster.sent == []


async def test_relay_status_change_syncs_task(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            t,
            status="completed",
            completed_at=None,
        )
    )

    await relay._handle({
        "channel": "tasks",
        "data": {"event": "status_change", "task_id": t.id,
                 "old_status": "in_progress", "new_status": "completed"},
    }, w)
    async with session_factory() as db:
        current = await db.get(Task, t.id)
        assert current.status == "completed"
        assert current.completed_at is not None
        assert current.error_message is None


async def test_relay_conflict_is_terminal_with_timestamp_and_error(
    relay,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="conflict",
            completed_at=None,
            error_message=None,
        )
    )

    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "conflict",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "conflict"
    assert current.completed_at is not None
    assert "conflict" in current.error_message


async def test_relay_plan_ready_fetches_content(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, mode="plan")
    relay._tasks[w.id] = {t.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            t,
            status="plan_review",
            plan_content="THE PLAN",
        )
    )

    await relay._handle({
        "channel": "tasks",
        "data": {"event": "plan_ready", "task_id": t.id},
    }, w)
    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.plan_content == "THE PLAN"
    assert task.status == "plan_review"


async def test_relay_monitor_events_with_remote_id(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}

    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event": "monitor_session_created", "monitor_session_id": 5,
                 "description": "watch"},
    }, w)
    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event": "monitor_check", "monitor_session_id": 5,
                 "check_number": 1, "status": "ok", "summary": "fine"},
    }, w)
    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event": "monitor_session_status", "monitor_session_id": 5,
                 "status": "completed"},
    }, w)

    async with session_factory() as db:
        ms = (await db.execute(select(MonitorSession))).scalars().one()
        checks = (await db.execute(select(MonitorCheck))).scalars().all()
    assert ms.remote_id == 5
    assert ms.task_id == t.id
    assert ms.status == "completed"
    assert ms.last_summary == "fine"
    assert len(checks) == 1
    assert checks[0].monitor_session_id == ms.id  # 本地 id，不是 remote 的 5


async def test_relay_context_usage_syncs(relay, session_factory):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    relay._tasks[w.id] = {t.id}
    await relay._handle({
        "channel": f"task:{t.id}",
        "data": {"event_type": "context_usage", "input_tokens": 100, "context_window": 200000},
    }, w)
    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.context_window_usage == {"input_tokens": 100, "context_window": 200000}


async def test_delayed_worker_message_cannot_mark_reassigned_local_task_unread(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        has_unread=False,
    )
    relay._tasks[worker.id] = {task.id}
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                worker_id=None,
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="local-session",
            )
        )
        await db.commit()

    await relay._handle(
        {
            "channel": f"task:{task.id}",
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "late Worker output",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.has_unread is False
    assert current.session_id == "local-session"
    assert logs == []
    assert broadcaster.sent == []


async def test_delayed_worker_status_cannot_overwrite_reassigned_local_generation(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def delayed_snapshot(*_args, **_kwargs):
        fetch_started.set()
        await release_fetch.wait()
        return _remote_task(
            task,
            status="completed",
            session_id="old-worker-session",
            completed_at=None,
        )

    relay._fetch_task_snapshot = AsyncMock(side_effect=delayed_snapshot)
    handling = asyncio.create_task(
        relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "status_change",
                    "task_id": task.id,
                    "new_status": "completed",
                },
            },
            worker,
        )
    )
    await fetch_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                worker_id=None,
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="local-session",
            )
        )
        await db.commit()
    release_fetch.set()
    await handling

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "local-session"
    assert current.completed_at is None
    assert broadcaster.sent == []


async def test_delayed_worker_status_cannot_overwrite_same_worker_retry_aba(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def delayed_snapshot(*_args, **_kwargs):
        fetch_started.set()
        await release_fetch.wait()
        return _remote_task(
            task,
            status="completed",
            session_id="old-session",
            completed_at=None,
        )

    relay._fetch_task_snapshot = AsyncMock(side_effect=delayed_snapshot)
    handling = asyncio.create_task(
        relay._handle(
            {
                "channel": "tasks",
                "data": {
                    "event": "status_change",
                    "task_id": task.id,
                    "new_status": "completed",
                },
            },
            worker,
        )
    )
    await fetch_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="new-session",
            )
        )
        await db.commit()
    release_fetch.set()
    await handling

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == worker.id
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert current.completed_at is None
    assert broadcaster.sent == []


async def test_worker_status_publication_fence_drops_superseded_result(
    relay,
    broadcaster,
    session_factory,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    relay._tasks[worker.id] = {task.id}
    relay._fetch_task_snapshot = AsyncMock(
        return_value=_remote_task(
            task,
            status="completed",
            completed_at=None,
        )
    )
    real_publish = relay._publish_status_generation

    async def retry_before_publication(generation, payload=None):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                )
            )
            await db.commit()
        return await real_publish(generation, payload)

    relay._publish_status_generation = AsyncMock(
        side_effect=retry_before_publication
    )
    await relay._handle(
        {
            "channel": "tasks",
            "data": {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "completed",
            },
        },
        worker,
    )

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.completed_at is None
    assert broadcaster.sent == []


async def test_reconnect_backfill_cannot_write_after_task_moves_local(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    history_started = asyncio.Event()
    release_history = asyncio.Event()

    class Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **_kwargs):
            if "/chat/history" in url:
                history_started.set()
                await release_history.wait()
                return Response(
                    [
                        {
                            "event_type": "message",
                            "role": "assistant",
                            "content": "late history",
                        }
                    ]
                )
            return Response(
                _remote_task(
                    task,
                    status="completed",
                    session_id="old-worker-session",
                    completed_at=None,
                )
            )

    monkeypatch.setattr(worker_relay_module.httpx, "AsyncClient", Client)
    backfill = asyncio.create_task(
        relay._backfill_missing_logs(worker, {task.id})
    )
    await history_started.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                worker_id=None,
                status="executing",
                retry_count=Task.retry_count + 1,
                session_id="local-session",
            )
        )
        await db.commit()
    release_history.set()
    await backfill

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        logs = (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.session_id == "local-session"
    assert logs == []
    assert broadcaster.sent == []


async def test_reconnect_exhaustion_cannot_fail_same_worker_retry(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    relay._tasks[worker.id] = {task.id}
    retried = False

    async def fail_after_retry(_worker):
        nonlocal retried
        if not retried:
            retried = True
            async with session_factory() as db:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        status="executing",
                        retry_count=Task.retry_count + 1,
                        session_id="new-session",
                    )
                )
                await db.commit()
        raise OSError("still disconnected")

    relay.ensure_connection = AsyncMock(side_effect=fail_after_retry)
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )
    await relay._reconnect(worker)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert relay.ensure_connection.await_count == 10
    assert current.worker_id == worker.id
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert current.completed_at is None
    assert current.error_message is None
    assert broadcaster.sent == []


async def test_reconnect_exhaustion_fails_only_exact_generation_and_publishes(
    relay,
    broadcaster,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    relay._tasks[worker.id] = {task.id}
    relay.ensure_connection = AsyncMock(
        side_effect=OSError("still disconnected")
    )
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    await relay._reconnect(worker)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "failed"
    assert current.completed_at is not None
    assert "无法重连" in current.error_message
    assert broadcaster.sent == [
        (
            "tasks",
            {
                "event": "status_change",
                "task_id": task.id,
                "new_status": "failed",
            },
        )
    ]


async def test_reconnect_snapshot_does_not_pop_new_connection_tasks(
    relay,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    old_task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="executing",
    )
    new_task_ids = {999_001}
    relay._tasks[worker.id] = new_task_ids
    relay._ws[worker.id] = object()
    relay.ensure_connection = AsyncMock()
    relay.subscribe_task = AsyncMock()
    relay._backfill_missing_logs = AsyncMock()
    monkeypatch.setattr(
        worker_relay_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    await relay._reconnect(worker, {old_task.id})

    assert relay._tasks[worker.id] is new_task_ids
    relay.subscribe_task.assert_awaited_once()


# === Dispatcher 双路径 ===


async def test_dispatch_worker_tasks_forwards(db_factory, session_factory, broadcaster, monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, status="pending")

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    disp = GlobalDispatcher.__new__(GlobalDispatcher)
    disp.db_factory = db_factory
    disp.broadcaster = broadcaster
    disp._running_tasks = {}

    await disp._dispatch_worker_tasks()
    # 等 fire-and-forget 的 forward 跑完
    for _ in range(10):
        await asyncio.sleep(0)

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "in_progress"
    proxy.forward_task_to_worker.assert_called_once()
    assert any(c == "tasks" and d.get("new_status") == "in_progress" for c, d in broadcaster.sent)


async def test_dispatch_worker_tasks_skips_unready_worker(db_factory, session_factory, broadcaster, monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher
    w = await _mk_worker(session_factory, status="stopped")
    t = await _mk_task(session_factory, worker_id=w.id, status="pending")
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())

    disp = GlobalDispatcher.__new__(GlobalDispatcher)
    disp.db_factory = db_factory
    disp.broadcaster = broadcaster
    disp._running_tasks = {}
    await disp._dispatch_worker_tasks()

    async with session_factory() as db:
        assert (await db.get(Task, t.id)).status == "pending"  # 留队等 worker 就绪


async def test_dispatch_worker_claim_rejects_same_worker_pending_retry_aba(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        retry_count=Task.retry_count + 1,
                        title="new retry generation",
                    )
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.retry_count == task.retry_count + 1
    assert current.title == "new retry generation"
    proxy.forward_task_to_worker.assert_not_awaited()
    assert broadcaster.sent == []


async def test_dispatch_worker_claim_rejects_new_shared_shadow(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(shared_from_id=987654)
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.shared_from_id == 987654
    proxy.forward_task_to_worker.assert_not_awaited()
    assert broadcaster.sent == []


async def test_dispatch_worker_forwards_refreshed_claimed_task(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(title="current title")
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    forward = dispatcher._running_tasks.get(f"worker-{task.id}")
    assert forward is not None
    await forward

    forwarded_task = proxy.forward_task_to_worker.await_args.args[0]
    assert forwarded_task.title == "current title"


async def test_dispatch_worker_target_repo_fill_preserves_concurrent_project_edit(
    session_factory,
    broadcaster,
    monkeypatch,
):
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    async with session_factory() as db:
        original_project = Project(
            name="worker-original-project",
            local_path="/workspace/original",
            status="ready",
        )
        replacement_project = Project(
            name="worker-replacement-project",
            local_path="/workspace/replacement",
            status="ready",
        )
        db.add_all([original_project, replacement_project])
        await db.commit()
        await db.refresh(original_project)
        await db.refresh(replacement_project)
        original_project_id = original_project.id
        replacement_project_id = replacement_project.id
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="pending",
        project_id=original_project_id,
        target_repo="",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    factory_calls = 0

    @asynccontextmanager
    async def racing_db_factory():
        nonlocal factory_calls
        factory_calls += 1
        async with session_factory() as db:
            if factory_calls == 4:
                await db.execute(
                    update(Task)
                    .where(Task.id == task.id)
                    .values(
                        project_id=replacement_project_id,
                        target_repo="/workspace/user-choice",
                    )
                )
                await db.commit()
            yield db

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = racing_db_factory
    dispatcher.broadcaster = broadcaster
    dispatcher._running_tasks = {}

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "pending"
    assert current.project_id == replacement_project_id
    assert current.target_repo == "/workspace/user-choice"
    proxy.forward_task_to_worker.assert_not_awaited()
    assert broadcaster.sent == []


async def test_dispatch_forward_failure_marks_failed(db_factory, session_factory, broadcaster, monkeypatch):
    from backend.services.dispatcher import GlobalDispatcher
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id, status="pending")

    proxy = AsyncMock()
    proxy.forward_task_to_worker.side_effect = RuntimeError("boom")
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    disp = GlobalDispatcher.__new__(GlobalDispatcher)
    disp.db_factory = db_factory
    disp.broadcaster = broadcaster
    disp._running_tasks = {}

    await disp._dispatch_worker_tasks()
    # _safe_forward_to_worker 带 3 次指数退避重试（1s+2s），直接 await 转发
    # 任务跑完全部重试（done_callback 会 pop，所以先取引用）
    fwd = disp._running_tasks.get(f"worker-{t.id}")
    assert fwd is not None
    await fwd

    async with session_factory() as db:
        task = await db.get(Task, t.id)
    assert task.status == "failed"
    assert "转发到 Worker 失败" in task.error_message


async def test_old_worker_forward_failure_cannot_fail_reclaimed_generation(
    db_factory,
    session_factory,
    broadcaster,
    monkeypatch,
):
    """The async forwarder is fenced to the claim created for that request."""

    import backend.services.dispatcher as dispatcher_module
    from backend.services.dispatcher import GlobalDispatcher

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    proxy = AsyncMock()
    proxy.forward_task_to_worker.side_effect = RuntimeError("boom")
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(
        dispatcher_module.asyncio,
        "sleep",
        AsyncMock(),
    )

    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    dispatcher.db_factory = db_factory
    dispatcher.broadcaster = broadcaster
    async with db_factory() as db:
        claimed = await db.get(Task, task.id)
        old_generation = dispatcher._task_status_generation(claimed)
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(retry_count=Task.retry_count + 1)
        )
        await db.commit()

    await dispatcher._safe_forward_to_worker(task, old_generation)

    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "in_progress"
        assert current.retry_count == old_generation.retry_count + 1
    assert not any(
        channel == "tasks" and payload.get("new_status") == "failed"
        for channel, payload in broadcaster.sent
    )


# === API 代理 ===


class _ProxyResponse:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class _InvalidJSONProxyResponse(_ProxyResponse):
    def json(self):
        raise ValueError("invalid JSON")


def _install_proxy_transport(monkeypatch, outcome):
    requests = []

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.timeout = kwargs.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def request(self, method, url, **kwargs):
            requests.append((method, url, kwargs))
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", FakeAsyncClient)
    return requests


@pytest.mark.parametrize("remote_status", [401, 403])
async def test_generic_worker_proxy_hides_internal_auth_failures(
    session_factory, monkeypatch, remote_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    requests = _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(remote_status, {"detail": "secret Worker auth diagnostic"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(task, "POST", f"/api/tasks/{task.id}/retry")

    assert caught.value.status_code == 502
    assert "内部 Worker 认证失败" in caught.value.detail
    assert str(remote_status) in caught.value.detail
    assert "secret Worker auth diagnostic" not in caught.value.detail
    assert requests[0][2]["headers"] == {"Authorization": "Bearer wtoken"}


async def test_generic_worker_proxy_can_require_json_confirmation(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _InvalidJSONProxyResponse(200, "not-json"),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(
            task,
            "DELETE",
            f"/api/tasks/{task.id}",
            require_json=True,
        )

    assert caught.value.status_code == 502
    assert "invalid confirmation" in caught.value.detail


async def test_generic_worker_proxy_can_confirm_task_already_absent(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(404, {"detail": "Task not found"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    result = await proxy.proxy_to_worker(
        task,
        "DELETE",
        f"/api/tasks/{task.id}",
        require_json=True,
        allow_task_absent=True,
    )

    assert result == {"ok": True, "already_deleted": True}


async def test_generic_worker_proxy_can_surface_exact_endpoint_404(
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(404, {"detail": "Not Found"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(WorkerEndpointNotFoundError):
        await proxy.proxy_to_worker(
            task,
            "GET",
            f"/api/tasks/{task.id}/routing-config/status",
            require_json=True,
            surface_endpoint_not_found=True,
        )


@pytest.mark.parametrize(
    ("transport_error", "expected_status", "expected_detail"),
    [
        (httpx.ConnectError("private address unreachable"), 502, "Worker 网关连接失败"),
        (httpx.ReadTimeout("Worker stalled"), 503, "Worker w1 请求超时"),
    ],
)
async def test_generic_worker_proxy_maps_transport_failures(
    session_factory,
    monkeypatch,
    transport_error,
    expected_status,
    expected_detail,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(monkeypatch, transport_error)
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(task, "POST", f"/api/tasks/{task.id}/retry")

    assert caught.value.status_code == expected_status
    assert expected_detail in caught.value.detail
    assert str(transport_error) not in caught.value.detail


@pytest.mark.parametrize("remote_status", [302, 400, 404, 429, 500, 503])
async def test_generic_worker_proxy_hides_other_upstream_error_bodies(
    session_factory, monkeypatch, remote_status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(session_factory, worker_id=worker.id)
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(remote_status, {"detail": "sensitive Worker traceback"}),
    )
    proxy = WorkerProxy(session_factory, AsyncMock())

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(task, "POST", f"/api/tasks/{task.id}/retry")

    assert caught.value.status_code == 502
    assert f"远端 HTTP {remote_status}" in caught.value.detail
    assert "sensitive Worker traceback" not in caught.value.detail


async def test_create_task_with_worker_id_and_explicit_id(client, session_factory):
    await _mk_worker(session_factory, id=3)
    resp = await client.post("/api/tasks", json={
        "id": 4242, "worker_id": 3, "title": "x", "description": "remote",
    })
    assert resp.status_code in (200, 201), resp.text
    data = resp.json()
    assert data["id"] == 4242
    assert data["worker_id"] == 3


async def test_worker_routing_config_update_confirms_remote_before_manager(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        if path.endswith("/stage"):
            return _routing_snapshot(task, pending=dict(body))
        assert path.endswith("/ack")
        return _routing_snapshot(
            task,
            codex_service_tier="priority",
        )

    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "priority"
    assert proxy.proxy_to_worker.await_count == 3
    stage = proxy.proxy_to_worker.await_args_list[1]
    assert stage.args[1:3] == (
        "POST",
        f"/api/tasks/{task.id}/routing-config/stage",
    )
    assert stage.args[3] | {"op_id": "<ignored>"} == {
        "op_id": "<ignored>",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    assert stage.kwargs["require_json"] is True
    assert stage.kwargs["operation_lock_held"] is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.codex_service_tier == "priority"


async def test_worker_routing_config_updates_model_and_disables_fast_atomically(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    proxy = AsyncMock()

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(task)
        if path.endswith("/stage"):
            return _routing_snapshot(task, pending=dict(body))
        assert path.endswith("/ack")
        return _routing_snapshot(
            task,
            model="gpt-5.4-mini",
            codex_service_tier="default",
        )

    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={
            "model": "gpt-5.4-mini",
            "codex_service_tier": "default",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["model"] == "gpt-5.4-mini"
    assert response.json()["codex_service_tier"] == "default"
    assert proxy.proxy_to_worker.await_count == 3
    stage_payload = proxy.proxy_to_worker.await_args_list[1].args[3]
    assert {
        key: value
        for key, value in stage_payload.items()
        if key != "op_id"
    } == {
        "model": "gpt-5.4-mini",
        "codex_service_tier": "default",
        "provider": "codex",
    }


async def test_worker_routing_config_rejects_mixed_routing_and_other_fields(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={
            "title": "must-not-change",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    assert "save other fields separately" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.title == "t"
    assert current.codex_service_tier == "default"


@pytest.mark.parametrize(
    ("field", "remote_value"),
    (
        ("id", -1),
        ("provider", "claude"),
        ("model", "gpt-5.6-terra"),
        ("codex_service_tier", "default"),
    ),
)
async def test_worker_routing_config_rejects_unconfirmed_remote_snapshot(
    client,
    session_factory,
    monkeypatch,
    field,
    remote_value,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    remote = {
        "id": task.id,
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    remote[field] = remote_value
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = remote
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 502
    assert "invalid routing synchronization snapshot" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.model == "gpt-5.6-sol"
    assert current.codex_service_tier == "default"


async def test_worker_routing_config_remote_failure_keeps_manager_standard(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = HTTPException(
        502,
        "Worker rejected config",
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 502
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.codex_service_tier == "default"


async def test_worker_routing_config_relay_active_change_keeps_worker_blocked(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )

    remote_pending = None

    async def stage_after_relay_event(
        _task,
        method,
        path,
        body=None,
        **_kwargs,
    ):
        nonlocal remote_pending
        if method == "GET":
            return _routing_snapshot(task)
        assert path.endswith("/stage")
        remote_pending = dict(body)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(status="executing")
            )
            await db.commit()
        return _routing_snapshot(task, pending=remote_pending)

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = stage_after_relay_event
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409, response.text
    assert "remains safely blocked" in response.json()["detail"]
    assert remote_pending is not None
    assert proxy.proxy_to_worker.await_count == 2
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.codex_service_tier == "default"


@pytest.mark.parametrize("status", ("pending", "in_progress", "executing"))
async def test_worker_routing_config_rejects_forwarding_or_active_generation(
    client,
    session_factory,
    monkeypatch,
    status,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status=status,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "pending or active" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.codex_service_tier == "default"


async def test_worker_routing_config_rereads_assignment_after_operation_barrier(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    class MigrationGate:
        async def __aenter__(self):
            entered.set()
            await release.wait()

        async def __aexit__(self, *_args):
            return False

    monkeypatch.setattr(
        tasks_api_module,
        "get_task_operation_lock",
        lambda _task_id: MigrationGate(),
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    request = asyncio.create_task(
        client.put(
            f"/api/tasks/{task.id}",
            json={"codex_service_tier": "priority"},
        )
    )
    await entered.wait()
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(worker_id=None)
        )
        await db.commit()
    release.set()
    response = await request

    assert response.status_code == 409
    assert "assignment changed" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.codex_service_tier == "default"


async def test_worker_local_stage_readback_and_ack_are_atomic(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        worker_id=None,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
        metadata_={"keep": "yes"},
    )
    payload = {
        "op_id": "stage-standard-to-fast",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }

    staged = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json=payload,
    )

    assert staged.status_code == 200, staged.text
    assert staged.json()["codex_service_tier"] == "default"
    assert staged.json()["pending"] == payload
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert current.metadata_["keep"] == "yes"
        assert (
            current.metadata_[WORKER_ROUTING_PENDING_KEY]["op_id"]
            == payload["op_id"]
        )

    readback = await client.get(
        f"/api/tasks/{task.id}/routing-config/status"
    )
    assert readback.status_code == 200
    assert readback.json() == staged.json()

    acked = await client.post(
        f"/api/tasks/{task.id}/routing-config/ack",
        json=payload,
    )
    assert acked.status_code == 200, acked.text
    assert acked.json()["codex_service_tier"] == "priority"
    assert acked.json()["pending"] is None

    # A lost ack response may be retried exactly.
    duplicate = await client.post(
        f"/api/tasks/{task.id}/routing-config/ack",
        json=payload,
    )
    assert duplicate.status_code == 200
    assert duplicate.json() == acked.json()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "priority"
        assert current.metadata_ == {"keep": "yes"}


async def test_worker_stage_holds_codex_thread_guard_through_marker_commit(
    client,
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        worker_id=None,
        status="completed",
        session_id="native-thread-guarded",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    guard_exited = False

    @asynccontextmanager
    async def routing_guard(_home, thread_id):
        nonlocal guard_exited
        assert thread_id == "native-thread-guarded"
        async with session_factory() as db:
            before = await db.get(Task, task.id)
            assert WORKER_ROUTING_PENDING_KEY not in (before.metadata_ or {})
        yield {"thread": {"status": {"type": "idle"}}, "goal": None}
        async with session_factory() as db:
            staged = await db.get(Task, task.id)
            assert staged.codex_service_tier == "default"
            assert WORKER_ROUTING_PENDING_KEY in staged.metadata_
        guard_exited = True

    monkeypatch.setattr(
        main_module.instance_manager,
        "codex_thread_routing_guard",
        routing_guard,
    )
    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "guarded-standard-to-fast",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "default"
    assert response.json()["pending"]["codex_service_tier"] == "priority"
    assert guard_exited


@pytest.mark.parametrize("status", ("pending", "in_progress", "executing"))
async def test_worker_local_stage_atomically_rejects_active_status(
    client,
    session_factory,
    status,
):
    task = await _mk_task(
        session_factory,
        status=status,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": f"active-{status}",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_pending_marker_blocks_direct_retry_chat_and_plan_approve(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    payload = {
        "op_id": "block-direct-turns",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    staged = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json=payload,
    )
    assert staged.status_code == 200

    retry = await client.post(f"/api/tasks/{task.id}/retry")
    chat = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "must remain queued nowhere"},
    )
    assert retry.status_code == 409
    assert chat.status_code == 409
    async with session_factory() as db:
        logs = list(
            (
                await db.execute(
                    select(LogEntry).where(LogEntry.task_id == task.id)
                )
            ).scalars()
        )
    assert logs == []

    plan = await _mk_task(
        session_factory,
        status="plan_review",
        mode="plan",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    staged_plan = await client.post(
        f"/api/tasks/{plan.id}/routing-config/stage",
        json={**payload, "op_id": "block-plan-approve"},
    )
    assert staged_plan.status_code == 200
    approve = await client.post(f"/api/tasks/{plan.id}/plan/approve")
    assert approve.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        assert current.status == "plan_review"
        assert not current.plan_approved


async def test_worker_stage_rejects_running_ccm_sub_agent(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        db.add(
            MonitorSession(
                task_id=task.id,
                agent_type="sub_agent",
                source="ccm",
                description="delayed child",
                status="running",
            )
        )
        await db.commit()

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "child-running",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    assert "sub-agent is running" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_stage_rejects_unsettled_main_instance_generation(
    client,
    session_factory,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        instance = Instance(
            name="still-running-old-standard",
            status="running",
            pid=987654,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        current = await db.get(Task, task.id)
        current.instance_id = instance.id
        await db.commit()

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "must-wait-for-old-turn",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    assert "Instance generation" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_stage_rejects_unsettled_preowner_launch_reservation(
    client,
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    async with session_factory() as db:
        instance = Instance(name="preowner-reservation", status="idle")
        db.add(instance)
        await db.flush()
        current = await db.get(Task, task.id)
        current.instance_id = instance.id
        await db.commit()
        instance_id = instance.id

    barrier = AsyncMock(
        side_effect=HTTPException(
            409,
            "pre-owner process launch could not be proven stopped",
        )
    )
    monkeypatch.setattr(
        tasks_api_module,
        "_settle_task_launch_barrier",
        barrier,
    )

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "hidden-preowner",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409
    barrier.assert_awaited_once_with(task.id, instance_id)
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"
        assert WORKER_ROUTING_PENDING_KEY not in (current.metadata_ or {})


async def test_worker_routing_stage_timeout_leaves_remote_blocked_and_manager_old(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    remote_pending = None

    async def lose_stage_response(
        _task,
        method,
        path,
        body=None,
        **_kwargs,
    ):
        nonlocal remote_pending
        if method == "GET":
            return _routing_snapshot(task)
        assert path.endswith("/stage")
        remote_pending = dict(body)
        raise HTTPException(503, "stage response lost")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = lose_stage_response
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 503
    assert remote_pending is not None
    assert proxy.proxy_to_worker.await_count == 2
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"


async def test_worker_routing_lost_ack_response_converges_by_readback(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    state = {
        "tier": "default",
        "pending": None,
        "ack_lost": False,
    }

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(
                task,
                codex_service_tier=state["tier"],
                pending=state["pending"],
            )
        if path.endswith("/stage"):
            state["pending"] = dict(body)
            return _routing_snapshot(task, pending=state["pending"])
        assert path.endswith("/ack")
        state["tier"] = body["codex_service_tier"]
        state["pending"] = None
        state["ack_lost"] = True
        raise HTTPException(503, "ack response lost")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "priority"
    assert state == {
        "tier": "priority",
        "pending": None,
        "ack_lost": True,
    }
    assert proxy.proxy_to_worker.await_count == 4


async def test_worker_fast_to_standard_returns_authoritative_after_unreadable_ack(
    client,
    session_factory,
    monkeypatch,
):
    """Post-commit uncertainty must not leave the UI showing stale Fast."""

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    remote_pending = None
    get_count = 0

    async def protocol(_task, method, path, body=None, **_kwargs):
        nonlocal remote_pending, get_count
        if method == "GET":
            get_count += 1
            if get_count == 1:
                return _routing_snapshot(
                    task,
                    codex_service_tier="priority",
                )
            raise HTTPException(503, "ack readback unavailable")
        if path.endswith("/stage"):
            remote_pending = dict(body)
            return _routing_snapshot(
                task,
                codex_service_tier="priority",
                pending=remote_pending,
            )
        assert path.endswith("/ack")
        raise HTTPException(503, "ack unavailable")

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "default"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "default"
    assert remote_pending is not None
    assert remote_pending["codex_service_tier"] == "default"
    assert proxy.proxy_to_worker.await_count == 4
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "default"


async def test_worker_execution_reconciles_orphan_stage_to_manager_tuple(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    pending = {
        "op_id": "orphan-fast-stage",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }

    async def protocol(_task, method, path, body=None, **_kwargs):
        nonlocal pending
        if method == "GET":
            return _routing_snapshot(task, pending=pending)
        if path.endswith("/reconcile"):
            assert body["op_id"] == "orphan-fast-stage"
            assert body["codex_service_tier"] == "default"
            pending = None
            return _routing_snapshot(task)
        assert path.endswith("/retry")
        return _remote_task(
            task,
            status="pending",
            retry_count=task.retry_count + 1,
            completed_at=None,
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert pending is None
    assert proxy.proxy_to_worker.await_count == 3


@pytest.mark.parametrize(
    ("status", "mode", "action_path", "retry_delta"),
    [
        pytest.param(
            "completed",
            "auto",
            "retry",
            1,
            id="retry",
        ),
        pytest.param(
            "plan_review",
            "plan",
            "plan/approve",
            0,
            id="plan-approve",
        ),
    ],
)
async def test_worker_execution_admission_syncs_latest_manager_skills(
    client,
    session_factory,
    monkeypatch,
    status,
    mode,
    action_path,
    retry_delta,
):
    worker = await _mk_worker(session_factory)
    async with session_factory() as db:
        user_skill = UserSkill(
            name=f"Final {action_path} skill",
            description="Manager-authoritative description",
            content="Manager-authoritative content",
        )
        db.add(user_skill)
        await db.commit()
        await db.refresh(user_skill)
        user_skill_id = user_skill.id

    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status=status,
        mode=mode,
        plan_content="approved plan" if mode == "plan" else None,
        enabled_skills={"code-review": False},
        selected_user_skills=[],
    )
    admission_order = []
    worker_skill_payloads = []

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": task.id,
                "status": task.status,
                "retry_count": task.retry_count,
                "instance_id": task.instance_id,
                "enabled_skills": self.payload["enabled_skills"],
                "selected_user_skills": self.payload[
                    "selected_user_skills"
                ],
                "metadata_": {
                    "ccm_user_skill_snapshots": self.payload[
                        "user_skill_snapshots"
                    ],
                },
            }

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def put(self, _url, *, headers, json):
            admission_order.append("skills")
            worker_skill_payloads.append(json)
            return Response(json)

    async def protocol(current, method, path, body=None, **_kwargs):
        if method == "GET":
            assert path.endswith("/routing-config/status")
            return _routing_snapshot(current)
        assert method == "POST"
        assert path.endswith(f"/{action_path}")
        assert admission_order[-1] == "skills"
        admission_order.append(action_path)
        return _remote_task(
            current,
            status="pending",
            retry_count=current.retry_count + retry_delta,
            completed_at=None,
            plan_approved=True if mode == "plan" else current.plan_approved,
        )

    proxy = WorkerProxy(session_factory, relay=AsyncMock())
    proxy.proxy_to_worker = AsyncMock(side_effect=protocol)
    monkeypatch.setattr(
        worker_proxy_module.httpx,
        "AsyncClient",
        Client,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    updated = await client.put(
        f"/api/tasks/{task.id}",
        json={
            "enabled_skills": {"code-review": True},
            "selected_user_skills": [user_skill_id],
        },
    )
    assert updated.status_code == 200, updated.text

    response = await client.post(f"/api/tasks/{task.id}/{action_path}")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert admission_order == ["skills", action_path]
    assert len(worker_skill_payloads) == 1
    assert worker_skill_payloads[0]["enabled_skills"] == {
        "code-review": True,
    }
    assert worker_skill_payloads[0]["selected_user_skills"] == [
        user_skill_id,
    ]
    assert worker_skill_payloads[0]["user_skill_snapshots"] == [{
        "id": user_skill_id,
        "name": f"Final {action_path} skill",
        "description": "Manager-authoritative description",
        "content": "Manager-authoritative content",
    }]


@pytest.mark.parametrize(
    ("source_status", "mode", "action_path"),
    [
        pytest.param("completed", "auto", "retry", id="retry"),
        pytest.param("completed", "auto", "chat", id="chat"),
        pytest.param(
            "plan_review",
            "plan",
            "plan/approve",
            id="plan-approve",
        ),
    ],
)
async def test_migrated_inert_task_can_start_its_next_worker_turn(
    client,
    db_factory,
    session_factory,
    monkeypatch,
    source_status,
    mode,
    action_path,
):
    """Migration and the next admission agree on one remote generation."""

    from backend.services.task_migrator import TaskMigrator

    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        status=source_status,
        mode=mode,
        plan_content="ready plan" if mode == "plan" else None,
        instance_id=444,
        enabled_skills={"code-review": True},
    )
    relay = AsyncMock()
    remote: dict = {}
    imported_statuses = []
    skill_payloads = []

    class Response:
        text = ""

        def __init__(self, payload, status_code=200):
            self.payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return dict(self.payload)

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, *, headers, json):
            assert url.endswith("/api/tasks/migration-import")
            imported_statuses.append(json["source_status"])
            remote.update({
                "id": json["id"],
                "status": json["source_status"],
                "retry_count": json["retry_count"],
                "instance_id": None,
                "enabled_skills": json["enabled_skills"],
                "selected_user_skills": json["selected_user_skills"],
                "metadata_": {
                    "ccm_user_skill_snapshots": json[
                        "user_skill_snapshots"
                    ],
                },
                "provider": json["provider"],
                "model": json["model"],
                "codex_service_tier": json["codex_service_tier"],
            })
            return Response(remote, status_code=201)

        async def put(self, url, *, headers, json):
            assert url.endswith(f"/api/tasks/{task.id}")
            skill_payloads.append(json)
            remote["enabled_skills"] = json["enabled_skills"]
            remote["selected_user_skills"] = json["selected_user_skills"]
            remote["metadata_"]["ccm_user_skill_snapshots"] = json[
                "user_skill_snapshots"
            ]
            return Response(remote)

    async def worker_protocol(current, method, path, body=None, **_kwargs):
        if method == "GET":
            assert path.endswith("/routing-config/status")
            return _routing_snapshot(
                current,
                status=remote["status"],
            )
        assert method == "POST"
        if action_path == "retry":
            assert path.endswith("/retry")
            assert remote["status"] == "completed"
            remote.update(
                status="pending",
                retry_count=remote["retry_count"] + 1,
                instance_id=None,
            )
            return _remote_task(
                current,
                status="pending",
                retry_count=remote["retry_count"],
                completed_at=None,
            )
        if action_path == "plan/approve":
            assert path.endswith("/plan/approve")
            assert remote["status"] == "plan_review"
            remote.update(status="pending", instance_id=None)
            return _remote_task(
                current,
                status="pending",
                retry_count=remote["retry_count"],
                completed_at=None,
                plan_approved=True,
            )
        assert path.endswith("/chat")
        assert remote["status"] == "completed"
        return {
            "ok": True,
            "queued": True,
            "session_id": "migrated-session",
        }

    monkeypatch.setattr(
        worker_proxy_module.httpx,
        "AsyncClient",
        Client,
    )
    proxy = WorkerProxy(db_factory, relay=relay)
    proxy.ensure_worker_project = AsyncMock(return_value=17)
    proxy.proxy_to_worker = AsyncMock(side_effect=worker_protocol)
    migrator = TaskMigrator(
        db_factory=db_factory,
        relay=relay,
        broadcaster=None,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    migrated = await client.put(
        f"/api/tasks/{task.id}",
        json={"worker_id": worker.id},
    )

    assert migrated.status_code == 200, migrated.text
    assert migrated.json()["status"] == source_status
    assert migrated.json()["worker_id"] == worker.id
    assert imported_statuses == [source_status]
    assert remote["instance_id"] is None

    if action_path == "chat":
        response = await client.post(
            f"/api/tasks/{task.id}/chat",
            json={"message": "continue after migration"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["queued"] is True
    else:
        response = await client.post(
            f"/api/tasks/{task.id}/{action_path}",
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "pending"

    assert len(skill_payloads) == 1
    assert skill_payloads[0]["enabled_skills"] == {
        "code-review": True,
    }


async def test_worker_skill_update_shares_execution_admission_lock(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        enabled_skills={"code-review": False},
    )
    update_entered = asyncio.Event()
    original_update = tasks_api_module.TaskQueue.update_task

    async def observed_update(self, task_id, **updates):
        update_entered.set()
        return await original_update(self, task_id, **updates)

    monkeypatch.setattr(
        tasks_api_module.TaskQueue,
        "update_task",
        observed_update,
    )
    lock = worker_proxy_module.get_task_operation_lock(task.id)
    async with lock:
        pending = asyncio.create_task(client.put(
            f"/api/tasks/{task.id}",
            json={"enabled_skills": {"code-review": True}},
        ))
        for _ in range(10):
            await asyncio.sleep(0)
        assert not update_entered.is_set()

    response = await pending

    assert response.status_code == 200, response.text
    assert update_entered.is_set()
    assert response.json()["enabled_skills"]["code-review"] is True


async def test_worker_standard_execution_accepts_matching_legacy_routing(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    paths = []

    async def legacy_protocol(_task, method, path, _body=None, **kwargs):
        paths.append(path)
        assert method == "GET"
        if path.endswith("/routing-config/status"):
            assert kwargs["surface_endpoint_not_found"] is True
            raise WorkerEndpointNotFoundError(path)
        assert path == f"/api/tasks/{task.id}"
        return {
            "id": task.id,
            "status": task.status,
            "provider": "codex",
            "model": "gpt-5.6-sol",
            # A pre-Fast Worker has no codex_service_tier response field.
        }

    monkeypatch.setattr(tasks_api_module, "_proxy", legacy_protocol)

    snapshot = await tasks_api_module._ensure_worker_routing_ready(
        task,
        operation_lock_held=True,
    )

    assert snapshot.provider == "codex"
    assert snapshot.model == "gpt-5.6-sol"
    assert snapshot.codex_service_tier == "default"
    assert snapshot.pending is None
    assert paths == [
        f"/api/tasks/{task.id}/routing-config/status",
        f"/api/tasks/{task.id}",
    ]


async def test_worker_fast_execution_rejects_legacy_routing(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    paths = []

    async def legacy_protocol(_task, _method, path, _body=None, **_kwargs):
        paths.append(path)
        if path.endswith("/routing-config/status"):
            raise WorkerEndpointNotFoundError(path)
        return {
            "id": task.id,
            "status": task.status,
            "provider": "codex",
            "model": "gpt-5.6-sol",
        }

    monkeypatch.setattr(tasks_api_module, "_proxy", legacy_protocol)

    with pytest.raises(HTTPException) as caught:
        await tasks_api_module._ensure_worker_routing_ready(
            task,
            operation_lock_held=True,
        )

    assert caught.value.status_code == 409
    assert "cannot confirm Codex Fast" in caught.value.detail
    assert paths == [
        f"/api/tasks/{task.id}/routing-config/status",
        f"/api/tasks/{task.id}",
    ]


async def test_worker_standard_execution_rejects_mismatched_legacy_routing(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )

    async def legacy_protocol(_task, _method, path, _body=None, **_kwargs):
        if path.endswith("/routing-config/status"):
            raise WorkerEndpointNotFoundError(path)
        return {
            "id": task.id,
            "status": task.status,
            "provider": "codex",
            "model": "gpt-5.6-terra",
        }

    monkeypatch.setattr(tasks_api_module, "_proxy", legacy_protocol)

    with pytest.raises(HTTPException) as caught:
        await tasks_api_module._ensure_worker_routing_ready(
            task,
            operation_lock_held=True,
        )

    assert caught.value.status_code == 409
    assert "does not exactly match" in caught.value.detail


async def test_worker_execution_does_not_downgrade_non_404_routing_failure(
    session_factory,
    monkeypatch,
):
    task = await _mk_task(
        session_factory,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock(
        side_effect=HTTPException(
            502,
            "Worker 上游请求失败（远端 HTTP 500）",
        )
    )
    monkeypatch.setattr(tasks_api_module, "_proxy", proxy)

    with pytest.raises(HTTPException) as caught:
        await tasks_api_module._ensure_worker_routing_ready(
            task,
            operation_lock_held=True,
        )

    assert caught.value.status_code == 502
    assert "远端 HTTP 500" in caught.value.detail
    proxy.assert_awaited_once()


async def test_worker_routing_update_does_not_use_legacy_protocol(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    paths = []

    async def old_worker(_task, _method, path, _body=None, **kwargs):
        paths.append(path)
        assert kwargs.get("surface_endpoint_not_found", False) is False
        raise HTTPException(
            502,
            "Worker 上游请求失败（远端 HTTP 404）",
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = old_worker
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 502
    assert paths == [f"/api/tasks/{task.id}/routing-config/status"]


async def test_worker_routing_update_finishes_ack_before_propagating_cancel(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    ack_started = asyncio.Event()
    release_ack = asyncio.Event()
    state = {"tier": "default", "pending": None}

    async def protocol(_task, method, path, body=None, **_kwargs):
        if method == "GET":
            return _routing_snapshot(
                task,
                codex_service_tier=state["tier"],
                pending=state["pending"],
            )
        if path.endswith("/stage"):
            state["pending"] = dict(body)
            return _routing_snapshot(task, pending=state["pending"])
        assert path.endswith("/ack")
        ack_started.set()
        await release_ack.wait()
        state["tier"] = "priority"
        state["pending"] = None
        return _routing_snapshot(
            task,
            codex_service_tier="priority",
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = protocol
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    request = asyncio.create_task(
        client.put(
            f"/api/tasks/{task.id}",
            json={"codex_service_tier": "priority"},
        )
    )
    await asyncio.wait_for(ack_started.wait(), timeout=1)
    request.cancel()
    await asyncio.sleep(0)
    assert state["pending"] is not None
    release_ack.set()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert state == {"tier": "priority", "pending": None}
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current.codex_service_tier == "priority"


async def test_worker_execution_fails_closed_on_unmarked_tuple_divergence(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = _routing_snapshot(
        task,
        codex_service_tier="priority",
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 409
    assert "execution was blocked" in response.json()["detail"]
    proxy.proxy_to_worker.assert_awaited_once()


async def test_proxy_terminal_response_commits_normalized_generation_then_publishes(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        error_message="stale error",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = _remote_task(
        task,
        status="completed",
        completed_at=None,
        error_message=None,
    )
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "completed"
    assert response.json()["completed_at"] is not None
    assert response.json()["error_message"] is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.completed_at is not None
    assert current.error_message is None
    status_broadcast.assert_awaited_once_with(task.id, "completed")


async def test_proxy_response_cannot_overwrite_task_reassigned_during_request(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )

    async def move_local_before_response(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    worker_id=None,
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    session_id="local-session",
                )
            )
            await db.commit()
        return _remote_task(
            task,
            status="cancelled",
            session_id="old-worker-session",
            completed_at=None,
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = move_local_before_response
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id is None
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "local-session"
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_proxy_response_cannot_overwrite_same_worker_retry_aba(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
        error_message="old failure",
    )

    async def retry_completes_before_old_response(
        _task,
        method,
        _path,
        *_args,
        **_kwargs,
    ):
        if method == "GET":
            return _routing_snapshot(task)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    session_id="new-session",
                    error_message=None,
                    completed_at=None,
                )
            )
            await db.commit()
        return _remote_task(
            task,
            status="pending",
            retry_count=task.retry_count + 1,
            session_id=None,
            error_message=None,
            completed_at=None,
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = retry_completes_before_old_response
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.worker_id == worker.id
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert current.error_message is None
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_proxy_status_publication_fence_miss_returns_conflict(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = _remote_task(
        task,
        status="completed",
        completed_at=None,
    )
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    real_apply = worker_relay_module.apply_authoritative_worker_task

    async def replace_after_authoritative_commit(db, observed, result):
        resulting = await real_apply(db, observed, result)
        assert resulting is not None
        async with session_factory() as replacement_db:
            await replacement_db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="executing",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                )
            )
            await replacement_db.commit()
        return resulting

    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        "backend.api.tasks.apply_authoritative_worker_task",
        replace_after_authoritative_commit,
    )
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "executing"
    assert current.retry_count == task.retry_count + 1
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_proxy_response_without_remote_generation_fails_closed(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {
        "id": task.id,
        "status": "cancelled",
        # retry_count intentionally absent: this cannot identify a remote
        # generation on the same Worker.
    }
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "in_progress"
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


@pytest.mark.parametrize(
    "remote_overrides",
    [
        {"status": "not-a-task-status", "retry_count": 2},
        {"status": "cancelled", "retry_count": 1},
    ],
)
async def test_proxy_response_rejects_malformed_or_regressed_generation(
    client,
    session_factory,
    monkeypatch,
    remote_overrides,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
        retry_count=2,
    )
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = _remote_task(
        task,
        completed_at=None,
        **remote_overrides,
    )
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "in_progress"
    assert current.retry_count == 2
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()


async def test_proxy_response_cannot_overwrite_new_shared_shadow_authority(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="in_progress",
    )

    async def become_shared_before_response(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(shared_from_id=987654)
            )
            await db.commit()
        return _remote_task(
            task,
            status="cancelled",
            completed_at=None,
        )

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = become_shared_before_response
    local_broadcaster = FakeBroadcaster()
    status_broadcast = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", local_broadcaster)
    monkeypatch.setattr(
        task_events_module,
        "broadcast_status_change",
        status_broadcast,
    )

    response = await client.post(f"/api/tasks/{task.id}/cancel")

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.shared_from_id == 987654
    assert current.status == "in_progress"
    assert current.completed_at is None
    status_broadcast.assert_not_awaited()
    assert local_broadcaster.sent == []


async def test_local_dequeue_skips_worker_tasks(session_factory):
    from backend.services.task_queue import TaskQueue
    await _mk_task(session_factory, status="pending", worker_id=1)
    local = await _mk_task(session_factory, status="pending")
    async with session_factory() as db:
        q = TaskQueue(db)
        got = await q.dequeue()
    assert got is not None and got.id == local.id


async def test_chat_proxy_for_worker_task(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = w
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(t)
        return {"ok": True, "queued": True, "session_id": "sess-1"}

    proxy.proxy_to_worker.side_effect = route_then_chat
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    resp = await client.post(f"/api/tasks/{t.id}/chat", json={"message": "hello worker"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["session_id"] == "sess-1"
    assert body["instance_id"] is None

    async with session_factory() as db:
        logs = (await db.execute(
            select(LogEntry).where(LogEntry.task_id == t.id,
                                   LogEntry.event_type == "user_message")
        )).scalars().all()
        task = await db.get(Task, t.id)
    assert len(logs) == 1 and logs[0].instance_id is None
    assert task.session_id == "sess-1"
    assert proxy.proxy_to_worker.await_count == 2
    assert (
        proxy.proxy_to_worker.call_args.kwargs["operation_lock_held"]
        is True
    )


async def test_worker_chat_response_cannot_overwrite_retry_aba(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
        session_id="old-session",
    )

    async def replace_generation_before_response(
        _task,
        method,
        _path,
        *_args,
        **_kwargs,
    ):
        if method == "GET":
            return _routing_snapshot(task)
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    retry_count=Task.retry_count + 1,
                    session_id="new-session",
                )
            )
            await db.commit()
        return {
            "ok": True,
            "queued": True,
            "session_id": "stale-worker-session",
        }

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = worker
    proxy.relay = AsyncMock()
    proxy.proxy_to_worker.side_effect = replace_generation_before_response
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", FakeBroadcaster())

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "old generation chat"},
    )

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.retry_count == task.retry_count + 1
    assert current.session_id == "new-session"
    assert (
        proxy.proxy_to_worker.call_args.kwargs["operation_lock_held"]
        is True
    )


async def test_worker_chat_sender_prefix_is_display_only(session_factory, monkeypatch):
    """Manager displays the sender, while the Worker receives raw model text."""
    import json
    from types import SimpleNamespace

    from backend.api.chat import ChatMessage, _send_worker_chat
    from backend.models.user import User

    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    async with session_factory() as db:
        sender = User(
            email="worker-prefix@test.local",
            name="Worker Alice",
            password_hash="unused",
            role="super_admin",
        )
        db.add(sender)
        await db.commit()
        await db.refresh(sender)

    proxy = AsyncMock()
    proxy.require_ready_worker.return_value = w
    proxy.relay = AsyncMock()

    async def route_then_chat(_task, method, _path, *_args, **_kwargs):
        if method == "GET":
            return _routing_snapshot(t)
        return {
            "ok": True,
            "queued": True,
            "session_id": "worker-prefix-session",
        }

    proxy.proxy_to_worker.side_effect = route_then_chat
    broadcaster = FakeBroadcaster()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)

    request = SimpleNamespace(
        state=SimpleNamespace(user_id=sender.id, user_role="super_admin")
    )
    async with session_factory() as db:
        task = await db.get(Task, t.id)
        await _send_worker_chat(
            task,
            ChatMessage(message="[FIX] preserve this tag"),
            db,
            request,
        )

    forwarded = proxy.proxy_to_worker.call_args.kwargs["body"]
    assert forwarded["message"] == "[FIX] preserve this tag"
    async with session_factory() as db:
        stored = (await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == t.id,
                LogEntry.event_type == "user_message",
            )
        )).scalar_one()
    assert stored.content == "[Worker Alice] [FIX] preserve this tag"
    assert json.loads(stored.raw_json)["raw_content"] == "[FIX] preserve this tag"
    assert broadcaster.sent[0][1]["content"] == stored.content


async def test_chat_proxy_rejects_secrets(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())
    resp = await client.post(f"/api/tasks/{t.id}/chat",
                             json={"message": "x", "secret_ids": [1]})
    assert resp.status_code == 400


async def test_worker_retry_rejects_migrating_without_remote_mutation(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="migrating",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{task.id}/retry")

    assert response.status_code == 409
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current.status == "migrating"
    assert current.retry_count == task.retry_count


async def test_stop_session_proxies_for_worker_task(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {"ok": True, "stopped": True, "cleared_messages": 0}
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    resp = await client.post(f"/api/tasks/{t.id}/stop-session")
    assert resp.status_code == 200
    assert resp.json()["stopped"] is True
    proxy.proxy_to_worker.assert_called_once()
    method, path = proxy.proxy_to_worker.call_args.args[1:3]
    assert method == "POST" and path == f"/api/tasks/{t.id}/stop-session"


async def test_delete_worker_task_remote_first_then_cleans_exact_manager_mirror(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    async with session_factory() as db:
        log = LogEntry(
            task_id=task.id,
            event_type="message",
            content="remote result",
        )
        monitor = MonitorSession(
            task_id=task.id,
            remote_id=17,
            description="stale mirror",
            status="running",
        )
        db.add_all([log, monitor])
        await db.flush()
        db.add(
            MonitorCheck(
                monitor_session_id=monitor.id,
                check_number=1,
                status="ok",
            )
        )
        await db.commit()

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {"ok": True}
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    proxy.proxy_to_worker.assert_awaited_once()
    method, path = proxy.proxy_to_worker.call_args.args[1:3]
    assert method == "DELETE"
    assert path == f"/api/tasks/{task.id}"
    assert proxy.proxy_to_worker.call_args.kwargs == {
        "require_json": True,
        "allow_task_absent": True,
        "operation_lock_held": True,
    }
    proxy.relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None
        assert not (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().all()
        assert not (
            await db.execute(
                select(MonitorSession).where(
                    MonitorSession.task_id == task.id
                )
            )
        ).scalars().all()
        assert not (await db.execute(select(MonitorCheck))).scalars().all()


async def test_delete_worker_task_retry_converges_when_remote_is_already_absent(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    requests = _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(404, {"detail": "Task not found"}),
    )
    relay = Mock()
    relay.subscribe_task = AsyncMock()
    proxy = WorkerProxy(session_factory, relay)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    assert requests[0][0] == "DELETE"
    relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None


async def test_delete_worker_task_does_not_treat_unrelated_404_as_confirmation(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )
    _install_proxy_transport(
        monkeypatch,
        _ProxyResponse(404, {"detail": "Route not found"}),
    )
    relay = Mock()
    relay.subscribe_task = AsyncMock()
    proxy = WorkerProxy(session_factory, relay)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 502
    relay.unsubscribe_task.assert_not_called()
    async with session_factory() as db:
        assert await db.get(Task, task.id) is not None


@pytest.mark.parametrize(
    "remote_outcome",
    [
        HTTPException(502, "Worker unreachable"),
        {"ok": False},
        {"deleted": True},
    ],
)
async def test_delete_worker_task_preserves_manager_mirror_without_confirmation(
    client,
    session_factory,
    monkeypatch,
    remote_outcome,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="failed",
    )
    async with session_factory() as db:
        db.add(
            LogEntry(
                task_id=task.id,
                event_type="system_event",
                content="retain me",
            )
        )
        await db.commit()

    proxy = AsyncMock()
    if isinstance(remote_outcome, BaseException):
        proxy.proxy_to_worker.side_effect = remote_outcome
    else:
        proxy.proxy_to_worker.return_value = remote_outcome
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 502
    proxy.relay.unsubscribe_task.assert_not_called()
    async with session_factory() as db:
        assert await db.get(Task, task.id) is not None
        assert (
            await db.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars().one().content == "retain me"


async def test_delete_worker_task_converges_after_stale_relay_generation_update(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _mk_worker(session_factory)
    task = await _mk_task(
        session_factory,
        worker_id=worker.id,
        status="completed",
    )

    async def remote_delete_then_relay_new_generation(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(
                    status="in_progress",
                    retry_count=Task.retry_count + 1,
                )
            )
            await db.commit()
        return {"ok": True}

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = remote_delete_then_relay_new_generation
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 200, response.text
    proxy.relay.unsubscribe_task.assert_called_once_with(worker.id, task.id)
    async with session_factory() as db:
        assert await db.get(Task, task.id) is None


async def test_delete_worker_task_preserves_mirror_moved_to_another_worker(
    client,
    session_factory,
    monkeypatch,
):
    source = await _mk_worker(session_factory)
    destination = await _mk_worker(
        session_factory,
        private_ip="10.0.0.10",
    )
    task = await _mk_task(
        session_factory,
        worker_id=source.id,
        status="completed",
    )

    async def remote_delete_then_move_mirror(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task.id)
                .values(worker_id=destination.id)
            )
            await db.commit()
        return {"ok": True}

    proxy = AsyncMock()
    proxy.proxy_to_worker.side_effect = remote_delete_then_move_mirror
    proxy.relay = Mock()
    proxy.task_operation_lock = Mock(return_value=asyncio.Lock())
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    response = await client.delete(f"/api/tasks/{task.id}")

    assert response.status_code == 409, response.text
    proxy.relay.unsubscribe_task.assert_called_once_with(source.id, task.id)
    async with session_factory() as db:
        preserved = await db.get(Task, task.id)
        assert preserved is not None
        assert preserved.worker_id == destination.id


async def test_monitor_delete_translates_remote_id(client, session_factory, monkeypatch):
    w = await _mk_worker(session_factory)
    t = await _mk_task(session_factory, worker_id=w.id)
    async with session_factory() as db:
        ms = MonitorSession(task_id=t.id, remote_id=5, description="m", status="running")
        db.add(ms)
        await db.commit()
        await db.refresh(ms)

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = {"ok": True}
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    resp = await client.delete(f"/api/tasks/{t.id}/monitor-sessions/{ms.id}")
    assert resp.status_code == 200
    path = proxy.proxy_to_worker.call_args.args[2]
    assert path.endswith("/monitor-sessions/5")  # 用 remote_id，不是本地 id
    async with session_factory() as db:
        assert (await db.get(MonitorSession, ms.id)).status == "cancelled"


# === WS 认证 ===


async def test_ws_token_check():
    from backend.api.ws import _ws_token_ok
    from backend.config import settings

    class FakeWS:
        def __init__(self, headers=None, qp=None):
            self.headers = headers or {}
            self.query_params = qp or {}

    old = settings.auth_token
    try:
        settings.auth_token = ""
        assert _ws_token_ok(FakeWS()) is True  # 未配置 token 放行
        settings.auth_token = "secret"
        assert _ws_token_ok(FakeWS()) is False
        assert _ws_token_ok(FakeWS(headers={"authorization": "Bearer secret"})) is True
        assert _ws_token_ok(FakeWS(qp={"token": "secret"})) is True
        assert _ws_token_ok(FakeWS(qp={"token": "wrong"})) is False
    finally:
        settings.auth_token = old


# ---------------------------------------------------------------------------
# Backfill dedup — duplicate-message-on-reconnect regression
# ---------------------------------------------------------------------------

def _entry(et="message", role="assistant", content=None, tool_name=None,
           tool_input=None, tool_output=None, loop_iteration=None):
    return {
        "event_type": et, "role": role, "content": content,
        "tool_name": tool_name, "tool_input": tool_input,
        "tool_output": tool_output, "loop_iteration": loop_iteration,
    }


class TestBackfillDedup:
    """`_missing_by_fingerprint` must not re-insert already-present entries —
    the count-based `remote[local_count:]` slicing did exactly that whenever a
    gap was mid-stream (not tail-only) or the live relay raced the backfill."""

    def test_tail_only_missing(self):
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content=str(i)) for i in range(5)]
        local = remote[:3]
        missing = _missing_by_fingerprint(local, remote)
        assert [m["content"] for m in missing] == ["3", "4"]

    def test_mid_stream_gap_does_not_duplicate(self):
        # local missed entry "2" in the middle but has "3"; count-based slicing
        # (remote[local_count=3:]) would re-insert "3" AND drop "2".
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content=str(i)) for i in range(5)]  # 0,1,2,3,4
        local = [remote[0], remote[1], remote[3]]            # missing "2"
        missing = _missing_by_fingerprint(local, remote)
        assert [m["content"] for m in missing] == ["2", "4"]  # "3" NOT re-inserted

    def test_fully_synced_inserts_nothing(self):
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content=str(i)) for i in range(4)]
        assert _missing_by_fingerprint(list(remote), remote) == []

    def test_truncated_tool_output_still_matches(self):
        # remote tool_output is truncated by the history endpoint; the local copy
        # is full. Prefix-capped fingerprint must still treat them as identical.
        from backend.services.worker_relay import _missing_by_fingerprint
        full = "x" * 50_000
        truncated = ("x" * 20_000) + "\n…(truncated)"
        local = [_entry(et="tool_result", tool_name="bash", tool_output=full)]
        remote = [_entry(et="tool_result", tool_name="bash", tool_output=truncated)]
        assert _missing_by_fingerprint(local, remote) == []

    def test_duplicate_fingerprints_preserve_multiplicity(self):
        from backend.services.worker_relay import _missing_by_fingerprint
        remote = [_entry(content="same") for _ in range(3)]
        local = [_entry(content="same")]  # only one present
        missing = _missing_by_fingerprint(local, remote)
        assert len(missing) == 2  # insert the two still-missing copies
