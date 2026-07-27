"""Security regression tests for per-user Feishu OAuth binding."""

from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.api import feishu
from backend.config import settings
from backend.database import get_db
from backend.models.user import User


@pytest_asyncio.fixture
async def feishu_api(db_engine, monkeypatch):
    session_factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    app = FastAPI()
    app.state.current_user_id = None

    @app.middleware("http")
    async def set_test_identity(request: Request, call_next):
        request.state.user_id = request.app.state.current_user_id
        request.state.user_role = "member"
        return await call_next(request)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    app.include_router(feishu.router)

    monkeypatch.setattr(settings, "feishu_app_id", "cli_test")
    monkeypatch.setattr(settings, "feishu_app_secret", "a" * 32)
    monkeypatch.setattr(settings, "feishu_oauth_state_secret", "b" * 32)
    monkeypatch.setattr(settings, "feishu_oauth_state_ttl_seconds", 600)
    monkeypatch.setattr(settings, "public_base_url", "https://ccm.example")

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client, app, session_factory


async def _create_user(
    session_factory,
    *,
    email: str,
    role: str = "member",
    active: bool = True,
) -> int:
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
        return user.id


@pytest.mark.asyncio
async def test_auth_url_issues_signed_state_for_exact_active_user(
    feishu_api,
    monkeypatch,
):
    client, app, session_factory = feishu_api
    user_id = await _create_user(
        session_factory,
        email="active@example.com",
    )
    app.state.current_user_id = user_id
    captured = {}

    async def fake_get_auth_url(redirect_uri: str, state: str = "") -> str:
        captured.update(redirect_uri=redirect_uri, state=state)
        return "https://feishu.example/authorize"

    monkeypatch.setattr(feishu.feishu_auth, "get_auth_url", fake_get_auth_url)

    response = await client.get("/api/feishu/auth-url")

    assert response.status_code == 200
    assert response.json() == {"url": "https://feishu.example/authorize"}
    assert captured["redirect_uri"] == (
        "https://ccm.example/api/feishu/callback"
    )
    assert not captured["state"].startswith("uid:")
    assert feishu._verify_oauth_state(captured["state"]) == user_id


@pytest.mark.asyncio
async def test_auth_url_rejects_inactive_user(feishu_api, monkeypatch):
    client, app, session_factory = feishu_api
    user_id = await _create_user(
        session_factory,
        email="disabled@example.com",
        active=False,
    )
    app.state.current_user_id = user_id
    get_auth_url = AsyncMock()
    monkeypatch.setattr(feishu.feishu_auth, "get_auth_url", get_auth_url)

    response = await client.get("/api/feishu/auth-url")

    assert response.status_code == 401
    get_auth_url.assert_not_awaited()


@pytest.mark.asyncio
async def test_callback_binds_only_signed_active_user(
    feishu_api,
    monkeypatch,
):
    client, _, session_factory = feishu_api
    target_id = await _create_user(
        session_factory,
        email="target@example.com",
    )
    other_id = await _create_user(
        session_factory,
        email="other@example.com",
        role="super_admin",
    )
    monkeypatch.setattr(
        feishu.feishu_auth,
        "exchange_code",
        AsyncMock(return_value={"access_token": "access"}),
    )
    monkeypatch.setattr(
        feishu.feishu_auth,
        "get_user_info",
        AsyncMock(
            return_value={
                "open_id": "ou_exact",
                "name": "Exact User",
                "avatar_url": "https://avatar.example/exact.png",
            }
        ),
    )

    response = await client.get(
        "/api/feishu/callback",
        params={
            "code": "valid-code",
            "state": feishu._create_oauth_state(target_id),
        },
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert response.headers["location"] == "/#/team?feishu_bound=1"
    async with session_factory() as db:
        target = await db.get(User, target_id)
        other = await db.get(User, other_id)
        assert target.feishu_open_id == "ou_exact"
        assert target.feishu_name == "Exact User"
        assert target.avatar_url == "https://avatar.example/exact.png"
        assert other.feishu_open_id == ""


@pytest.mark.asyncio
async def test_callback_rejects_tampered_state_before_upstream(
    feishu_api,
    monkeypatch,
):
    client, _, session_factory = feishu_api
    user_id = await _create_user(
        session_factory,
        email="tamper@example.com",
    )
    state = feishu._create_oauth_state(user_id)
    payload, signature = state.split(".")
    replacement = "A" if signature[0] != "A" else "B"
    tampered = f"{payload}.{replacement}{signature[1:]}"
    exchange_code = AsyncMock()
    monkeypatch.setattr(feishu.feishu_auth, "exchange_code", exchange_code)

    response = await client.get(
        "/api/feishu/callback",
        params={"code": "attacker-code", "state": tampered},
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert response.headers["location"] == "/#/team?feishu_error=1"
    exchange_code.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(User, user_id)).feishu_open_id == ""


@pytest.mark.asyncio
async def test_callback_rejects_expired_state_before_upstream(
    feishu_api,
    monkeypatch,
):
    client, _, session_factory = feishu_api
    user_id = await _create_user(
        session_factory,
        email="expired@example.com",
    )
    expired = feishu._create_oauth_state(user_id, now=1)
    exchange_code = AsyncMock()
    monkeypatch.setattr(feishu.feishu_auth, "exchange_code", exchange_code)

    response = await client.get(
        "/api/feishu/callback",
        params={"code": "stale-code", "state": expired},
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert response.headers["location"] == "/#/team?feishu_error=1"
    exchange_code.assert_not_awaited()


@pytest.mark.asyncio
async def test_callback_rejects_inactive_user_and_legacy_admin_fallback(
    feishu_api,
    monkeypatch,
):
    client, _, session_factory = feishu_api
    disabled_admin_id = await _create_user(
        session_factory,
        email="old-admin@example.com",
        role="super_admin",
        active=False,
    )
    active_admin_id = await _create_user(
        session_factory,
        email="new-admin@example.com",
        role="super_admin",
    )
    exchange_code = AsyncMock()
    monkeypatch.setattr(feishu.feishu_auth, "exchange_code", exchange_code)

    for state in (
        feishu._create_oauth_state(disabled_admin_id),
        f"uid:{active_admin_id}",
        "",
    ):
        response = await client.get(
            "/api/feishu/callback",
            params={"code": "attacker-code", "state": state},
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert response.headers["location"] == "/#/team?feishu_error=1"

    exchange_code.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(User, disabled_admin_id)).feishu_open_id == ""
        assert (await db.get(User, active_admin_id)).feishu_open_id == ""
