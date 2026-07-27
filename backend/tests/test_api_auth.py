"""Tests for Auth API endpoints."""
import asyncio
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from backend.api.auth import _hash_password
from backend.config import settings
from backend.models.user import User


@pytest_asyncio.fixture
async def auth_app(db_engine):
    """App fixture that does NOT disable auth (unlike the shared one)."""
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

    from backend.main import app as real_app
    from backend.database import get_db

    async def override_get_db():
        async with session_factory() as session:
            yield session

    real_app.dependency_overrides[get_db] = override_get_db
    yield real_app
    real_app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def auth_client(auth_app):
    transport = ASGITransport(app=auth_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_login_no_auth_configured(auth_client):
    """When auth_token is empty, login always succeeds."""
    original = settings.auth_token
    settings.auth_token = ""
    try:
        resp = await auth_client.post("/api/auth/login", json={"token": "anything"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert "No auth configured" in resp.json().get("message", "")
    finally:
        settings.auth_token = original


@pytest.mark.asyncio
async def test_login_valid_token(auth_client):
    """Valid token returns ok."""
    original = settings.auth_token
    settings.auth_token = "test-secret-123"
    try:
        resp = await auth_client.post("/api/auth/login", json={"token": "test-secret-123"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
    finally:
        settings.auth_token = original


@pytest.mark.asyncio
async def test_login_invalid_token(auth_client):
    """Invalid token returns 401."""
    original = settings.auth_token
    settings.auth_token = "test-secret-123"
    try:
        resp = await auth_client.post("/api/auth/login", json={"token": "wrong"})
        assert resp.status_code == 401
    finally:
        settings.auth_token = original


@pytest.mark.asyncio
async def test_login_missing_token_field(auth_client):
    """token 字段已可选（支持 email+password 登录），空 body 走业务校验返回 400。"""
    resp = await auth_client.post("/api/auth/login", json={})
    assert resp.status_code == 400
    assert "Email and password required" in resp.text


@pytest.mark.asyncio
async def test_no_auth_mode_grants_full_access(client):
    """无鉴权模式（AUTH_TOKEN 为空）回归测试。

    RBAC 上线后中间件在无 token 分支曾直接放行而不设置身份，导致
    require_task_access / require_admin 全线 403、无鉴权部署不可用。
    修复后该模式所有请求视为 super_admin（等价于历史「无鉴权 = 全开放」语义）。
    """
    # require_task_access 路径：创建后读取（修复前 GET 返回 403）
    created = await client.post("/api/tasks", json={"title": "t", "description": "d"})
    assert created.status_code == 201
    task_id = created.json()["id"]
    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200

    # require_admin 路径：admin-only 端点（修复前 403 "Admin only"）
    resp = await client.post("/api/instances", json={"name": "no-auth-inst"})
    assert resp.status_code == 201

    # The frontend probes this endpoint. No-auth deployments must remain
    # usable after Instance GET endpoints become administrator-only.
    resp = await client.get("/api/auth/me")
    assert resp.status_code == 200
    assert resp.json() == {
        "ok": True,
        "auth_type": "none",
        "role": "super_admin",
    }


@pytest.mark.asyncio
async def test_first_registered_user_is_only_super_admin(auth_client):
    original = settings.auth_token
    settings.auth_token = "deployment-bootstrap-token"
    try:
        with patch(
            "backend.services.email_service.verify_code",
            return_value=True,
        ):
            first, second = await asyncio.gather(
                auth_client.post(
                    "/api/auth/register",
                    json={
                        "email": "first@example.com",
                        "name": "First",
                        "password": "safe-password-1",
                        "code": "123456",
                        "bootstrap_token": "deployment-bootstrap-token",
                    },
                ),
                auth_client.post(
                    "/api/auth/register",
                    json={
                        "email": "second@example.com",
                        "name": "Second",
                        "password": "safe-password-2",
                        "code": "123456",
                        "bootstrap_token": "deployment-bootstrap-token",
                    },
                ),
            )
    finally:
        settings.auth_token = original

    assert first.status_code == 200
    assert second.status_code == 200
    assert sorted(
        [first.json()["user"]["role"], second.json()["user"]["role"]]
    ) == ["member", "super_admin"]


@pytest.mark.asyncio
@pytest.mark.parametrize("bootstrap_token", ["", "wrong-token"])
async def test_first_admin_requires_configured_bootstrap_token(
    auth_client,
    bootstrap_token,
):
    original = settings.auth_token
    settings.auth_token = "deployment-bootstrap-token"
    try:
        with patch(
            "backend.services.email_service.verify_code",
            return_value=True,
        ) as verify:
            response = await auth_client.post(
                "/api/auth/register",
                json={
                    "email": "first@example.com",
                    "name": "First",
                    "password": "safe-password-1",
                    "code": "123456",
                    "bootstrap_token": bootstrap_token,
                },
            )
    finally:
        settings.auth_token = original

    assert response.status_code == 403
    # A wrong deployment token must not consume a valid one-time email code.
    verify.assert_not_called()


@pytest.mark.asyncio
async def test_first_admin_accepts_configured_bootstrap_token(auth_client):
    original = settings.auth_token
    settings.auth_token = "deployment-bootstrap-token"
    try:
        with patch(
            "backend.services.email_service.verify_code",
            return_value=True,
        ):
            response = await auth_client.post(
                "/api/auth/register",
                json={
                    "email": "owner@example.com",
                    "name": "Owner",
                    "password": "safe-password-1",
                    "code": "123456",
                    "bootstrap_token": "deployment-bootstrap-token",
                },
            )
    finally:
        settings.auth_token = original

    assert response.status_code == 200
    assert response.json()["user"]["role"] == "super_admin"


@pytest.mark.asyncio
async def test_first_admin_needs_no_bootstrap_token_without_auth_token(
    auth_client,
):
    original = settings.auth_token
    settings.auth_token = ""
    try:
        with patch(
            "backend.services.email_service.verify_code",
            return_value=True,
        ):
            response = await auth_client.post(
                "/api/auth/register",
                json={
                    "email": "owner@example.com",
                    "name": "Owner",
                    "password": "safe-password-1",
                    "code": "123456",
                },
            )
    finally:
        settings.auth_token = original

    assert response.status_code == 200
    assert response.json()["user"]["role"] == "super_admin"


@pytest.mark.asyncio
async def test_disabled_users_do_not_bypass_first_active_user_bootstrap(
    auth_client,
    db_engine,
):
    session_factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with session_factory() as session:
        session.add(User(
            email="disabled@example.com",
            name="Disabled",
            password_hash=_hash_password("disabled-password"),
            role="super_admin",
            is_active=False,
        ))
        await session.commit()

    original = settings.auth_token
    settings.auth_token = "deployment-bootstrap-token"
    try:
        with patch(
            "backend.services.email_service.verify_code",
            return_value=True,
        ) as verify:
            denied = await auth_client.post(
                "/api/auth/register",
                json={
                    "email": "owner@example.com",
                    "name": "Owner",
                    "password": "safe-password-1",
                    "code": "123456",
                },
            )
            allowed = await auth_client.post(
                "/api/auth/register",
                json={
                    "email": "owner@example.com",
                    "name": "Owner",
                    "password": "safe-password-1",
                    "code": "123456",
                    "bootstrap_token": "deployment-bootstrap-token",
                },
            )
    finally:
        settings.auth_token = original

    assert denied.status_code == 403
    assert allowed.status_code == 200
    assert allowed.json()["user"]["role"] == "super_admin"
    verify.assert_called_once_with("owner@example.com", "123456")


@pytest.mark.asyncio
async def test_send_code_passes_request_client_ip(auth_client):
    with patch(
        "backend.services.email_service.send_verification_code",
        return_value=True,
    ) as send:
        response = await auth_client.post(
            "/api/auth/send-code",
            json={"email": "user@example.com"},
        )

    assert response.status_code == 200
    send.assert_called_once_with("user@example.com", "127.0.0.1")


@pytest.mark.asyncio
async def test_send_code_returns_429_with_retry_after(auth_client):
    from backend.services.email_service import (
        VerificationCodeRateLimitError,
    )

    with patch(
        "backend.services.email_service.send_verification_code",
        side_effect=VerificationCodeRateLimitError(17),
    ):
        response = await auth_client.post(
            "/api/auth/send-code",
            json={"email": "user@example.com"},
        )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "17"


@pytest.mark.asyncio
async def test_send_code_returns_503_at_bounded_capacity(auth_client):
    from backend.services.email_service import (
        VerificationCodeCapacityError,
    )

    with patch(
        "backend.services.email_service.send_verification_code",
        side_effect=VerificationCodeCapacityError(),
    ):
        response = await auth_client.post(
            "/api/auth/send-code",
            json={"email": "user@example.com"},
        )

    assert response.status_code == 503


@pytest.mark.asyncio
async def test_send_code_rejects_unbounded_email_keys(auth_client):
    with patch(
        "backend.services.email_service.send_verification_code",
        return_value=True,
    ) as send:
        response = await auth_client.post(
            "/api/auth/send-code",
            json={"email": f"user@{'x' * 400}.example"},
        )

    assert response.status_code == 422
    send.assert_not_called()
