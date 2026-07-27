"""Tests for Discussion API endpoints."""
import pytest
from unittest.mock import AsyncMock, MagicMock, call, patch
from datetime import datetime

from backend.models.discussion import Discussion, DiscussionAgent, DiscussionMessage


@pytest.mark.asyncio
async def test_create_discussion(client):
    resp = await client.post("/api/discussions", json={
        "title": "Test Discussion",
        "facilitator_model": "claude-opus-4-6",
        "agent_model": "claude-opus-4-6",
        "max_agents": 3,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "Test Discussion"
    assert data["status"] == "active"
    assert data["max_agents"] == 3
    assert data["agent_count"] == 0
    assert data["message_count"] == 0


@pytest.mark.asyncio
async def test_list_discussions(client):
    await client.post("/api/discussions", json={"title": "A"})
    await client.post("/api/discussions", json={"title": "B"})
    resp = await client.get("/api/discussions")
    assert resp.status_code == 200
    assert len(resp.json()) >= 2


@pytest.mark.asyncio
async def test_get_discussion(client):
    create = await client.post("/api/discussions", json={"title": "Detail Test"})
    did = create.json()["id"]
    resp = await client.get(f"/api/discussions/{did}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["title"] == "Detail Test"
    assert isinstance(data["messages"], list)
    assert isinstance(data["agents"], list)


@pytest.mark.asyncio
async def test_get_discussion_not_found(client):
    resp = await client.get("/api/discussions/99999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_discussion(client):
    create = await client.post("/api/discussions", json={"title": "Delete Me"})
    did = create.json()["id"]
    resp = await client.delete(f"/api/discussions/{did}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    resp2 = await client.get(f"/api/discussions/{did}")
    assert resp2.status_code == 404


@pytest.mark.asyncio
async def test_delete_discussion_not_found(client):
    resp = await client.delete("/api/discussions/99999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Stop agent tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_agent_not_found(client):
    create = await client.post("/api/discussions", json={"title": "Stop Test"})
    did = create.json()["id"]
    resp = await client.post(f"/api/discussions/{did}/agents/99999/stop")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stop_agent_wrong_discussion(client, session_factory):
    d1 = await client.post("/api/discussions", json={"title": "D1"})
    d2 = await client.post("/api/discussions", json={"title": "D2"})
    d1_id = d1.json()["id"]
    d2_id = d2.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=d1_id,
            role_name="Tester",
            system_prompt="test",
            status="running",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(f"/api/discussions/{d2_id}/agents/{agent_id}/stop")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stop_idle_agent_succeeds(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Stop Idle"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Idle Agent",
            system_prompt="test",
            status="idle",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    with patch("backend.api.discussions._get_service") as mock_svc:
        mock_svc.return_value.stop_agent = AsyncMock()
        resp = await client.post(f"/api/discussions/{did}/agents/{agent_id}/stop")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_svc.return_value.stop_agent.assert_awaited_once_with(agent_id)


@pytest.mark.asyncio
async def test_stop_running_agent_calls_service(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Stop Running"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Runner",
            system_prompt="test",
            status="running",
            pid=12345,
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    with patch("backend.api.discussions._get_service") as mock_svc:
        mock_svc.return_value.stop_agent = AsyncMock()
        resp = await client.post(f"/api/discussions/{did}/agents/{agent_id}/stop")
        assert resp.status_code == 200
        mock_svc.return_value.stop_agent.assert_awaited_once_with(agent_id)


# ---------------------------------------------------------------------------
# Stop agent unit tests (service layer)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_agent_service_sigint_then_wait():
    """stop_agent signals the exact process group and proves it exited."""
    from backend.services.discussion_service import DiscussionService
    import signal

    mock_broadcaster = MagicMock()
    svc = DiscussionService(db_factory=AsyncMock(), broadcaster=mock_broadcaster)

    mock_proc = AsyncMock()
    mock_proc.returncode = None
    mock_proc.pid = 4242
    mock_proc.send_signal = MagicMock()

    async def wait_for_exit():
        mock_proc.returncode = 0
        return 0

    mock_proc.wait = AsyncMock(side_effect=wait_for_exit)

    svc._processes[42] = mock_proc
    process_group_signalled = False

    def fake_killpg(pgid, sig):
        nonlocal process_group_signalled
        assert pgid == 4242
        if sig == 0:
            if process_group_signalled:
                raise ProcessLookupError
            return
        assert sig == signal.SIGINT
        process_group_signalled = True

    with patch(
        "backend.services.discussion_service.os.killpg",
        side_effect=fake_killpg,
    ) as killpg:
        await svc.stop_agent(42)

    killpg.assert_any_call(4242, signal.SIGINT)
    assert [
        call
        for call in killpg.call_args_list
        if call.args[1] != 0
    ] == [call(4242, signal.SIGINT)]
    mock_proc.send_signal.assert_not_called()


@pytest.mark.asyncio
async def test_stop_agent_service_no_process():
    """stop_agent with no tracked process is a no-op."""
    from backend.services.discussion_service import DiscussionService

    svc = DiscussionService(db_factory=AsyncMock(), broadcaster=MagicMock())
    await svc.stop_agent(999)


@pytest.mark.asyncio
async def test_stop_agent_service_already_exited():
    """A reaped leader with no live descendants is a no-op."""
    from backend.services.discussion_service import DiscussionService

    svc = DiscussionService(db_factory=AsyncMock(), broadcaster=MagicMock())

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.pid = 4242
    svc._processes[42] = mock_proc

    with patch(
        "backend.services.discussion_service.os.killpg",
        side_effect=ProcessLookupError,
    ) as killpg:
        await svc.stop_agent(42)

    killpg.assert_called_once_with(4242, 0)
    mock_proc.send_signal.assert_not_called()


# ---------------------------------------------------------------------------
# Trigger / Chat guards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_agent_not_found(client):
    create = await client.post("/api/discussions", json={"title": "Trigger"})
    did = create.json()["id"]
    resp = await client.post(f"/api/discussions/{did}/agents/99999/trigger")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_running_agent_409(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Trigger Running"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Busy",
            system_prompt="test",
            status="running",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(f"/api/discussions/{did}/agents/{agent_id}/trigger")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_chat_agent_not_found(client):
    create = await client.post("/api/discussions", json={"title": "Chat"})
    did = create.json()["id"]
    resp = await client.post(
        f"/api/discussions/{did}/agents/99999/chat",
        json={"message": "hello"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_chat_agent_running_409(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Chat Running"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Busy",
            system_prompt="test",
            status="running",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(
        f"/api/discussions/{did}/agents/{agent_id}/chat",
        json={"message": "hello"},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_chat_agent_no_session_400(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Chat No Session"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="New",
            system_prompt="test",
            status="idle",
            session_id=None,
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(
        f"/api/discussions/{did}/agents/{agent_id}/chat",
        json={"message": "hello"},
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Add agent endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_agent_not_found(client):
    resp = await client.post("/api/discussions/99999/add-agent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_add_agent_calls_service(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Add Agent"})
    did = create.json()["id"]

    mock_agent = DiscussionAgent(
        id=1,
        discussion_id=did,
        role_name="新角色",
        system_prompt="test",
        status="running",
        created_at=datetime.utcnow(),
    )

    with patch("backend.api.discussions._get_service") as mock_svc:
        mock_svc.return_value.add_agent = AsyncMock(return_value=mock_agent)
        resp = await client.post(f"/api/discussions/{did}/add-agent")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_svc.return_value.add_agent.assert_awaited_once()


# ---------------------------------------------------------------------------
# Resume-all endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_all_no_agents(client):
    create = await client.post("/api/discussions", json={"title": "Resume Empty"})
    did = create.json()["id"]
    resp = await client.post(f"/api/discussions/{did}/resume-all")
    assert resp.status_code == 200
    assert resp.json()["resumed"] == 0


@pytest.mark.asyncio
async def test_resume_all_not_found(client):
    resp = await client.post("/api/discussions/99999/resume-all")
    assert resp.status_code == 404
