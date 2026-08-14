"""Security regressions for HTTP authentication and WebSocket channel ACLs."""

import asyncio
import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.api.auth import create_jwt, decode_jwt
from backend.config import settings
from backend.models.discussion import Discussion
from backend.models.pr_monitor import MonitoredRepo
from backend.models.log_entry import LogEntry
from backend.models.plan import Plan
from backend.models.project import Project
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare, TeamTaskShare  # noqa: F401
from backend.models.user import User
from backend.models.user_group import UserGroupMember  # noqa: F401
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.delivery_service import DeliveryCreateSpec, create_delivery_run


@pytest_asyncio.fixture
async def secured_client(db_engine, monkeypatch):
    """Run the real app with auth enabled and one shared in-memory database."""

    session_factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    from backend import database
    from backend.api import ask_user as ask_user_api
    from backend.database import get_db
    from backend.main import app
    from backend.middleware.auth import TokenAuthMiddleware

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr(database, "async_session", session_factory)
    monkeypatch.setattr(ask_user_api, "async_session", session_factory)

    original_token = settings.auth_token
    settings.auth_token = "security-service-token"
    TokenAuthMiddleware._admin_user_id = None
    TokenAuthMiddleware._admin_resolved = False
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            yield client, session_factory
    finally:
        settings.auth_token = original_token
        TokenAuthMiddleware._admin_user_id = None
        TokenAuthMiddleware._admin_resolved = False
        app.dependency_overrides.clear()


async def _create_user(
    session_factory,
    *,
    email: str,
    role: str,
    active: bool = True,
) -> tuple[int, str]:
    async with session_factory() as db:
        user = User(
            email=email,
            name=email.split("@", 1)[0],
            password_hash="not-used",
            role=role,
            is_active=active,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        return user.id, create_jwt(user)


@pytest.mark.asyncio
async def test_http_uses_current_database_role_for_admin_routes(secured_client):
    client, session_factory = secured_client
    user_id, stale_admin_token = await _create_user(
        session_factory,
        email="demoted@example.com",
        role="admin",
    )

    response = await client.get(
        "/api/instances",
        headers={"Authorization": f"Bearer {stale_admin_token}"},
    )
    assert response.status_code == 200

    async with session_factory() as db:
        user = await db.get(User, user_id)
        user.role = "member"
        await db.commit()

    response = await client.get(
        "/api/instances",
        headers={"Authorization": f"Bearer {stale_admin_token}"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_user", [False, True])
async def test_http_rejects_disabled_or_deleted_jwt_user(
    secured_client,
    remove_user,
):
    client, session_factory = secured_client
    user_id, token = await _create_user(
        session_factory,
        email=f"revoked-{remove_user}@example.com",
        role="admin",
    )

    async with session_factory() as db:
        user = await db.get(User, user_id)
        if remove_user:
            await db.delete(user)
        else:
            user.is_active = False
        await db.commit()

    response = await client.get(
        "/api/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_service_token_does_not_bind_disabled_seeded_admin(
    secured_client,
):
    client, session_factory = secured_client
    async with session_factory() as db:
        seeded = User(
            email="admin@apexin.ai",
            name="Disabled seeded admin",
            password_hash="rotated",
            role="super_admin",
            is_active=False,
        )
        db.add(seeded)
        await db.commit()
        await db.refresh(seeded)
        seeded_id = seeded.id

    headers = {"Authorization": "Bearer security-service-token"}
    me = await client.get("/api/auth/me", headers=headers)
    assert me.status_code == 200
    assert me.json() == {
        "ok": True,
        "auth_type": "token",
        "role": "super_admin",
    }

    created = await client.post(
        "/api/tasks",
        headers=headers,
        json={"title": "token-owned task", "description": "work"},
    )
    assert created.status_code == 201
    async with session_factory() as db:
        task = await db.get(Task, created.json()["id"])
        assert task is not None
        assert task.created_by is None
        assert task.created_by != seeded_id


@pytest.mark.asyncio
async def test_member_cannot_control_process_wide_system_operations(
    secured_client,
):
    client, session_factory = secured_client
    _, token = await _create_user(
        session_factory,
        email="system-member@example.com",
        role="member",
    )
    headers = {"Authorization": f"Bearer {token}"}

    responses = [
        await client.post(
            "/api/system/update",
            headers=headers,
            json={"dry_run": True},
        ),
        await client.get("/api/system/update/status", headers=headers),
        await client.post("/api/system/update/rollback", headers=headers),
        await client.post("/api/system/skills/curator", headers=headers),
        await client.post("/api/system/skills/distill", headers=headers),
    ]

    assert [response.status_code for response in responses] == [
        403,
        403,
        403,
        403,
        403,
    ]


@pytest.mark.asyncio
async def test_shared_project_plan_uses_project_location_boundary(
    secured_client,
    tmp_path,
):
    client, session_factory = secured_client
    admin_id, _ = await _create_user(
        session_factory,
        email="plan-project-admin@example.com",
        role="admin",
    )
    member_id, member_token = await _create_user(
        session_factory,
        email="plan-project-member@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(
            name="shared-plan-project",
            local_path=str(tmp_path),
            status="ready",
        )
        owned_worker = Worker(
            name="member-plan-worker",
            owner_user_id=member_id,
            status="ready",
        )
        db.add_all([project, owned_worker])
        await db.flush()
        db.add(TeamProjectShare(
            project_id=project.id,
            target_type="user",
            target_id=member_id,
            shared_by=admin_id,
        ))
        await db.commit()
        project_id = project.id
        worker_id = owned_worker.id

    headers = {"Authorization": f"Bearer {member_token}"}
    created = await client.post(
        "/api/plans",
        headers=headers,
        json={
            "input": "Plan inside the shared checkout",
            "project_id": project_id,
            "target_repo": "/etc",
        },
    )
    assert created.status_code == 201, created.text
    assert created.json()["target_repo"] == str(tmp_path)

    mismatch = await client.post(
        "/api/plans",
        headers=headers,
        json={
            "input": "Do not move the Project to another Worker",
            "project_id": project_id,
            "worker_id": worker_id,
        },
    )
    assert mismatch.status_code == 400
    assert "must match" in mismatch.text


@pytest.mark.asyncio
async def test_plan_catalog_sql_acl_matches_detail_access(secured_client, tmp_path):
    client, session_factory = secured_client
    admin_id, _ = await _create_user(
        session_factory,
        email="plan-list-admin@example.com",
        role="admin",
    )
    member_id, member_token = await _create_user(
        session_factory,
        email="plan-list-member@example.com",
        role="member",
    )
    other_id, _ = await _create_user(
        session_factory,
        email="plan-list-other@example.com",
        role="member",
    )
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        worker = Worker(
            name="plan-list-worker",
            owner_user_id=member_id,
            status="ready",
        )
        project = Project(
            name="plan-list-project",
            local_path=str(tmp_path),
            status="ready",
        )
        owner_task = Task(
            title="member task",
            description="member task",
            created_by=member_id,
        )
        shared_task = Task(
            title="shared task",
            description="shared task",
            created_by=other_id,
        )
        hidden_task = Task(
            title="hidden task",
            description="hidden task",
            created_by=other_id,
        )
        db.add_all([worker, project, owner_task, shared_task, hidden_task])
        await db.flush()
        db.add_all([
            TeamProjectShare(
                project_id=project.id,
                target_type="user",
                target_id=member_id,
                shared_by=admin_id,
            ),
            TeamTaskShare(
                task_id=shared_task.id,
                target_type="user",
                target_id=member_id,
                permission="chat",
                shared_by=admin_id,
            ),
        ])
        visible = [
            Plan(
                title="owned standalone",
                initial_request="visible",
                created_by=member_id,
                pipeline_config=pipeline,
            ),
            Plan(
                title="worker standalone",
                initial_request="visible",
                worker_id=worker.id,
                pipeline_config=pipeline,
            ),
            Plan(
                title="project standalone",
                initial_request="visible",
                project_id=project.id,
                pipeline_config=pipeline,
            ),
            Plan(
                title="owned related",
                initial_request="visible",
                target_task_id=owner_task.id,
                pipeline_config=pipeline,
            ),
            Plan(
                title="chat-shared related",
                initial_request="visible",
                target_task_id=shared_task.id,
                pipeline_config=pipeline,
            ),
        ]
        hidden = [
            Plan(
                title="hidden standalone",
                initial_request="hidden",
                created_by=other_id,
                pipeline_config=pipeline,
            ),
            Plan(
                title="hidden related",
                initial_request="hidden",
                target_task_id=hidden_task.id,
                pipeline_config=pipeline,
            ),
        ]
        db.add_all([*visible, *hidden])
        await db.commit()
        visible_ids = {plan.id for plan in visible}

    headers = {"Authorization": f"Bearer {member_token}"}
    listed = await client.get("/api/plans?limit=200", headers=headers)
    counted = await client.get("/api/plans/count", headers=headers)

    assert listed.status_code == 200, listed.text
    assert {item["id"] for item in listed.json()} == visible_ids
    assert counted.status_code == 200, counted.text
    assert counted.json() == {"total": len(visible_ids)}


@pytest.mark.asyncio
async def test_plan_delivery_resolution_requires_admin(secured_client):
    client, session_factory = secured_client
    _member_id, member_token = await _create_user(
        session_factory,
        email="plan-delivery-member@example.com",
        role="member",
    )

    response = await client.post(
        "/api/plans/1/application-deliveries/example-receipt/resolve",
        headers={"Authorization": f"Bearer {member_token}"},
        json={
            "action": "release_for_retry",
            "note": "Member must not resolve ambiguous execution",
        },
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_internal_ask_user_wait_rejects_member_jwt(secured_client):
    client, session_factory = secured_client
    _, token = await _create_user(
        session_factory,
        email="member@example.com",
        role="member",
    )
    payload = {
        "session_id": "visible-session-id",
        "questions": [],
    }

    denied = await client.post(
        "/api/ask-user/wait",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert denied.status_code == 403

    service = await client.post(
        "/api/ask-user/wait",
        json=payload,
        headers={"Authorization": "Bearer security-service-token"},
    )
    assert service.status_code == 200
    assert service.json() == {"answered": False, "reason": "no questions"}


@pytest.mark.asyncio
async def test_ask_user_pending_submit_follow_task_acl_and_validate_answers(
    secured_client,
):
    from backend.services.ask_user import ask_user_registry

    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="ask-owner@example.com",
        role="member",
    )
    other_id, _ = await _create_user(
        session_factory,
        email="ask-other@example.com",
        role="member",
    )
    async with session_factory() as db:
        owned = Task(
            title="owned",
            description="d",
            created_by=owner_id,
            session_id="owned-session",
            incarnation_id="1" * 32,
            status="executing",
        )
        other = Task(
            title="other",
            description="d",
            created_by=other_id,
            session_id="other-session",
            incarnation_id="2" * 32,
            status="executing",
        )
        db.add_all([owned, other])
        await db.commit()
        await db.refresh(owned)
        await db.refresh(other)
        owned_id = owned.id
        other_id = other.id

    question = [{
        "header": "Choice",
        "question": "Continue?",
        "options": [{"label": "Yes"}],
    }]
    owned_pending = ask_user_registry.create(
        task_id=owned_id,
        task_incarnation_id="1" * 32,
        task_retry_count=0,
        task_turn_generation=0,
        task_status="executing",
        session_id="owned-session",
        questions=question,
    )
    other_pending = ask_user_registry.create(
        task_id=other_id,
        task_incarnation_id="2" * 32,
        task_retry_count=0,
        task_turn_generation=0,
        task_status="executing",
        session_id="other-session",
        questions=question,
    )
    headers = {"Authorization": f"Bearer {owner_token}"}
    try:
        own = await client.get(
            f"/api/tasks/{owned_id}/ask-user/pending",
            headers=headers,
        )
        assert own.status_code == 200
        assert own.json()["pending"][0]["request_id"] == owned_pending.request_id

        denied = await client.get(
            f"/api/tasks/{other_id}/ask-user/pending",
            headers=headers,
        )
        assert denied.status_code == 403

        global_pending = await client.get(
            "/api/ask-user/pending",
            headers=headers,
        )
        assert global_pending.status_code == 200
        assert {
            item["request_id"]
            for item in global_pending.json()["pending"]
        } == {owned_pending.request_id}

        malformed = await client.post(
            f"/api/tasks/{owned_id}/ask-user/{owned_pending.request_id}",
            headers=headers,
            json={"answers": [{"labels": [123]}]},
        )
        assert malformed.status_code == 422
        assert ask_user_registry.get(owned_pending.request_id) is not None

        answered = await client.post(
            f"/api/tasks/{owned_id}/ask-user/{owned_pending.request_id}",
            headers=headers,
            json={"answers": [{"labels": ["Yes"]}]},
        )
        assert answered.status_code == 200
    finally:
        ask_user_registry.discard(owned_pending.request_id)
        ask_user_registry.discard(other_pending.request_id)


@pytest.mark.asyncio
async def test_concurrent_ask_user_answers_commit_exactly_one_audit(
    secured_client,
):
    from backend.services.ask_user import AskUserRevocation, ask_user_registry

    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="ask-race-owner@example.com",
        role="member",
    )
    incarnation = "a" * 32
    async with session_factory() as db:
        task = Task(
            title="ask-race",
            description="d",
            created_by=owner_id,
            session_id="ask-race-session",
            incarnation_id=incarnation,
            status="executing",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    pending = ask_user_registry.create(
        task_id=task_id,
        task_incarnation_id=incarnation,
        task_retry_count=0,
        task_turn_generation=0,
        task_status="executing",
        session_id="ask-race-session",
        questions=[{"header": "Choice", "question": "A or B?"}],
    )
    headers = {"Authorization": f"Bearer {owner_token}"}
    url = f"/api/tasks/{task_id}/ask-user/{pending.request_id}"
    try:
        first, second = await asyncio.gather(
            client.post(
                url,
                headers=headers,
                json={"answers": [{"labels": ["A"]}]},
            ),
            client.post(
                url,
                headers=headers,
                json={"answers": [{"labels": ["B"]}]},
            ),
        )
        assert sorted((first.status_code, second.status_code)) == [200, 410]
        winning_answers = await pending.future
        assert winning_answers in ([{"labels": ["A"]}], [{"labels": ["B"]}])

        async with session_factory() as db:
            audits = list((await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "system_event",
                )
            )).scalars())
        assert len(audits) == 1
        assert audits[0].content in {"已回答: Choice → A", "已回答: Choice → B"}
    finally:
        ask_user_registry.discard(pending.request_id)


@pytest.mark.asyncio
async def test_ask_user_transition_after_audit_commit_revokes_old_future(
    secured_client,
    monkeypatch,
):
    from backend.api import ask_user as ask_user_api
    from backend.services.ask_user import AskUserRevocation, ask_user_registry

    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="ask-final-fence-owner@example.com",
        role="member",
    )
    incarnation = "f" * 32
    async with session_factory() as db:
        task = Task(
            title="ask-final-fence",
            description="d",
            created_by=owner_id,
            session_id="ask-final-fence-session",
            incarnation_id=incarnation,
            retry_count=3,
            turn_generation=8,
            status="executing",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    pending = ask_user_registry.create(
        task_id=task_id,
        task_incarnation_id=incarnation,
        task_retry_count=3,
        task_turn_generation=8,
        task_status="executing",
        session_id="ask-final-fence-session",
        questions=[{"header": "Fence", "question": "old turn?"}],
    )
    original_settle = ask_user_api._settle_despite_cancellation
    settle_calls = 0

    async def transition_after_first_commit(awaitable):
        nonlocal settle_calls
        settle_calls += 1
        operation, cancellation = await original_settle(awaitable)
        if settle_calls == 1:
            operation.result()
            # Win exactly after the durable answer audit releases its first
            # writer fence and before submit acquires the final wake fence.
            async with session_factory() as transition_db:
                transitioned = await transition_db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.incarnation_id == incarnation,
                        Task.retry_count == 3,
                        Task.turn_generation == 8,
                        Task.status == "executing",
                    )
                    .values(turn_generation=9)
                )
                assert transitioned.rowcount == 1
                await transition_db.commit()
        return operation, cancellation

    monkeypatch.setattr(
        ask_user_api,
        "_settle_despite_cancellation",
        transition_after_first_commit,
    )
    try:
        response = await client.post(
            f"/api/tasks/{task_id}/ask-user/{pending.request_id}",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={"answers": [{"labels": ["stale"]}]},
        )
        assert response.status_code == 410
        assert settle_calls == 2
        assert ask_user_registry.get(pending.request_id) is None
        assert isinstance(await pending.future, AskUserRevocation)

        async with session_factory() as db:
            current_task = await db.get(Task, task_id)
            audits = list((await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "system_event",
                )
            )).scalars())
        assert current_task is not None
        assert current_task.turn_generation == 9
        assert len(audits) == 1
        assert json.loads(audits[0].raw_json)["task_turn_generation"] == 8
    finally:
        ask_user_registry.discard(pending.request_id)


@pytest.mark.asyncio
async def test_ask_user_old_turn_and_terminal_pending_are_revoked(
    secured_client,
):
    from backend.services.ask_user import AskUserRevocation, ask_user_registry

    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="ask-generation-owner@example.com",
        role="member",
    )
    incarnation = "b" * 32
    async with session_factory() as db:
        task = Task(
            title="ask-generation",
            description="d",
            created_by=owner_id,
            session_id="ask-generation-session",
            incarnation_id=incarnation,
            retry_count=1,
            turn_generation=2,
            status="executing",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    old_turn = ask_user_registry.create(
        task_id=task_id,
        task_incarnation_id=incarnation,
        task_retry_count=1,
        task_turn_generation=2,
        task_status="executing",
        session_id="ask-generation-session",
        questions=[{"header": "Old", "question": "old turn?"}],
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(turn_generation=3)
        )
        await db.commit()

    headers = {"Authorization": f"Bearer {owner_token}"}
    listed = await client.get(
        f"/api/tasks/{task_id}/ask-user/pending",
        headers=headers,
    )
    assert listed.status_code == 200
    assert listed.json() == {"pending": []}
    assert ask_user_registry.get(old_turn.request_id) is None
    assert isinstance(await old_turn.future, AskUserRevocation)

    terminal = ask_user_registry.create(
        task_id=task_id,
        task_incarnation_id=incarnation,
        task_retry_count=1,
        task_turn_generation=3,
        task_status="executing",
        session_id="ask-generation-session",
        questions=[{"header": "Terminal", "question": "too late?"}],
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="completed")
        )
        await db.commit()

    submitted = await client.post(
        f"/api/tasks/{task_id}/ask-user/{terminal.request_id}",
        headers=headers,
        json={"answers": [{"labels": ["Yes"]}]},
    )
    assert submitted.status_code == 410
    assert ask_user_registry.get(terminal.request_id) is None
    assert isinstance(await terminal.future, AskUserRevocation)


class _IdentityWebSocket:
    def __init__(self, token: str):
        self.headers = {}
        self.query_params = {"token": token}


@pytest.mark.asyncio
async def test_ws_identity_uses_current_role_and_active_state(db_factory):
    from backend.api.ws import _current_ws_identity, _revalidate_ws_identity
    from backend.config import settings

    # An empty AUTH_TOKEN short-circuits _ws_identity into a super_admin
    # "none" identity before the JWT path; pin a token so the test exercises
    # JWT revalidation regardless of the host environment's .env.
    original_token = settings.auth_token
    settings.auth_token = "ws-identity-service-token"
    try:
        await _run_ws_identity_current_role_checks(db_factory)
    finally:
        settings.auth_token = original_token


async def _run_ws_identity_current_role_checks(db_factory):
    from backend.api.ws import _current_ws_identity, _revalidate_ws_identity

    async with db_factory() as db:
        user = User(
            email="ws-admin@example.com",
            name="ws-admin",
            password_hash="not-used",
            role="admin",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        token = create_jwt(user)
        identity = {**decode_jwt(token), "auth_type": "jwt"}
        user.role = "member"
        await db.commit()

    ws = _IdentityWebSocket(token)
    async with db_factory() as db:
        # A stale admin claim must reconnect as the current member rather than
        # being locked out until the JWT expires.
        current = await _current_ws_identity(ws, db)
        assert current is not None
        assert current["role"] == "member"

        refreshed = await _revalidate_ws_identity(ws, identity, db)
        assert refreshed is not None
        assert refreshed["role"] == "member"
        user = await db.get(User, identity["user_id"])
        user.is_active = False
        await db.commit()

    async with db_factory() as db:
        assert await _revalidate_ws_identity(ws, identity, db) is None


@pytest.mark.asyncio
async def test_ws_channels_apply_resource_acl_and_default_deny(db_factory):
    from backend.api.ws import _ws_channel_allowed

    async with db_factory() as db:
        owner = User(
            email="owner@example.com",
            name="owner",
            password_hash="not-used",
            role="member",
        )
        other = User(
            email="other@example.com",
            name="other",
            password_hash="not-used",
            role="member",
        )
        db.add_all([owner, other])
        await db.flush()
        task = Task(title="owned", description="d", created_by=owner.id)
        worker = Worker(name="owned-worker", owner_user_id=owner.id)
        discussion = Discussion(title="owned", creator_user_id=owner.id)
        db.add_all([task, worker, discussion])
        await db.flush()
        plan = Plan(
            title="owned Plan",
            initial_request="plan it",
            target_task_id=task.id,
            created_by=owner.id,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
        )
        standalone_plan = Plan(
            title="owned standalone Plan",
            initial_request="plan it",
            created_by=owner.id,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
        )
        db.add_all([plan, standalone_plan])
        await db.commit()
        await db.refresh(task)
        await db.refresh(worker)
        await db.refresh(discussion)
        await db.refresh(plan)
        await db.refresh(standalone_plan)

        owner_identity = {
            "user_id": owner.id,
            "role": "member",
            "auth_type": "jwt",
        }
        other_identity = {
            "user_id": other.id,
            "role": "member",
            "auth_type": "jwt",
        }

        assert await _ws_channel_allowed(
            f"task:{task.id}",
            owner_identity,
            db,
        )
        assert await _ws_channel_allowed(
            f"worker:{worker.id}",
            owner_identity,
            db,
        )
        assert await _ws_channel_allowed(
            f"discussion:{discussion.id}:agent:9",
            owner_identity,
            db,
        )
        assert await _ws_channel_allowed(f"plan:{plan.id}", owner_identity, db)
        assert await _ws_channel_allowed(
            f"plan:{standalone_plan.id}", owner_identity, db
        )
        assert not await _ws_channel_allowed(
            f"task:{task.id}",
            other_identity,
            db,
        )
        assert not await _ws_channel_allowed(
            f"worker:{worker.id}",
            other_identity,
            db,
        )
        assert not await _ws_channel_allowed(
            f"plan:{plan.id}", other_identity, db
        )
        assert not await _ws_channel_allowed("plans", owner_identity, db)
        assert not await _ws_channel_allowed(
            "instance:1",
            owner_identity,
            db,
        )
        assert not await _ws_channel_allowed("workers", owner_identity, db)
        assert not await _ws_channel_allowed(
            "capabilities", owner_identity, db
        )
        assert not await _ws_channel_allowed(
            "task:1:spoofed",
            owner_identity,
            db,
        )

        admin_identity = {
            "user_id": owner.id,
            "role": "admin",
            "auth_type": "jwt",
        }
        assert await _ws_channel_allowed("instance:1", admin_identity, db)
        assert await _ws_channel_allowed("workers", admin_identity, db)
        assert await _ws_channel_allowed("plans", admin_identity, db)
        assert await _ws_channel_allowed(
            "capabilities", admin_identity, db
        )


@pytest.mark.asyncio
async def test_ws_delivery_channel_uses_project_acl(db_factory):
    from backend.api.ws import _ws_channel_allowed

    async with db_factory() as db:
        owner = User(
            email="delivery-ws-owner@example.com",
            name="delivery owner",
            password_hash="not-used",
            role="member",
        )
        other = User(
            email="delivery-ws-other@example.com",
            name="delivery other",
            password_hash="not-used",
            role="member",
        )
        project = Project(
            name="delivery-ws-project",
            local_path="/tmp/delivery-ws-project",
            git_url="git@github.com:acme/delivery-ws-project.git",
            has_remote=True,
            default_branch="main",
            status="ready",
        )
        db.add_all([owner, other, project])
        await db.flush()
        db.add(
            TeamProjectShare(
                project_id=project.id,
                target_type="user",
                target_id=owner.id,
                shared_by=owner.id,
            )
        )
        repo = MonitoredRepo(
            repo_full_name="acme/delivery-ws-project",
            project_id=project.id,
            webhook_secret="secret",
            enabled=True,
            auto_merge=False,
            auto_repair=True,
            review_mode="panel",
            wait_for_ci=False,
            required_checks=[],
            merge_queue_mode="manual",
            default_branch="main",
        )
        db.add(repo)
        await db.flush()
        run = await create_delivery_run(
            db,
            DeliveryCreateSpec(
                idempotency_key="delivery-ws-run",
                project_id=project.id,
                monitored_repo_id=repo.id,
                title="Delivery WS ACL",
                requirements="Verify scoped updates.",
                created_by=owner.id,
            ),
        )
        owner_identity = {
            "user_id": owner.id,
            "role": "member",
            "auth_type": "jwt",
        }
        other_identity = {
            "user_id": other.id,
            "role": "member",
            "auth_type": "jwt",
        }

        assert await _ws_channel_allowed(
            f"delivery:{run.id}", owner_identity, db
        )
        assert not await _ws_channel_allowed(
            f"delivery:{run.id}", other_identity, db
        )
        assert not await _ws_channel_allowed("deliveries", owner_identity, db)
        assert await _ws_channel_allowed(
            "deliveries",
            {**owner_identity, "role": "admin"},
            db,
        )
