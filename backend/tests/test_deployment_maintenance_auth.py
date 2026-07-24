from types import SimpleNamespace

import pytest
from fastapi import Depends, FastAPI, Request
from httpx import ASGITransport, AsyncClient

from backend.api.auth import create_jwt, router as auth_router
from backend.api.deps import require_admin
from backend.config import settings
from backend.database import get_db
from backend.middleware.auth import TokenAuthMiddleware


def _maintenance_app() -> FastAPI:
    app = FastAPI()
    app.state.deployment_maintenance_only = True
    app.add_middleware(TokenAuthMiddleware)
    app.include_router(auth_router)

    class ExplodingDatabase:
        async def execute(self, *args, **kwargs):
            raise AssertionError("maintenance authentication touched DB")

    async def database_override():
        yield ExplodingDatabase()

    app.dependency_overrides[get_db] = database_override

    @app.get(
        "/api/system/update/status",
        dependencies=[Depends(require_admin)],
    )
    async def status(request: Request):
        return {"auth_type": request.state.auth_type}

    @app.get("/api/system/health")
    async def health():
        return {"status": "ok", "commit": "controlled"}

    @app.get("/api/tasks")
    async def tasks():
        return []

    return app


def _token(*, role: str) -> str:
    return create_jwt(
        SimpleNamespace(id=123, email="admin@example.com", role=role)
    )


@pytest.mark.asyncio
async def test_maintenance_health_remains_available_to_handoff_worker(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/api/system/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "commit": "controlled",
    }


@pytest.mark.asyncio
async def test_maintenance_update_accepts_signed_admin_without_db(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/system/update/status",
            headers={"Authorization": f"Bearer {_token(role='admin')}"},
        )

    assert response.status_code == 200
    assert response.json()["auth_type"] == "jwt-maintenance"


@pytest.mark.asyncio
async def test_maintenance_update_rejects_non_admin_snapshot(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/system/update/status",
            headers={"Authorization": f"Bearer {_token(role='member')}"},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_maintenance_blocks_unrelated_api_before_database_access(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/tasks",
            headers={"Authorization": f"Bearer {_token(role='admin')}"},
        )

    assert response.status_code == 503
    assert response.json()["deployment_maintenance_only"] is True


@pytest.mark.asyncio
async def test_maintenance_update_accepts_legacy_admin_token_without_db(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/system/update/status",
            headers={"Authorization": "Bearer legacy-secret"},
        )

    assert response.status_code == 200
    assert response.json()["auth_type"] == "token"


@pytest.mark.asyncio
async def test_maintenance_identity_probe_uses_admin_jwt_snapshot_without_db(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {_token(role='admin')}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["deployment_maintenance_only"] is True
    assert payload["user"]["role"] == "admin"


@pytest.mark.asyncio
async def test_maintenance_identity_probe_rejects_member_jwt(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {_token(role='member')}"},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_maintenance_login_allows_legacy_token_but_not_password(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "legacy-secret")
    app = _maintenance_app()
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        legacy = await client.post(
            "/api/auth/login",
            json={"token": "legacy-secret"},
        )
        password = await client.post(
            "/api/auth/login",
            json={
                "email": "admin@example.com",
                "password": "password",
            },
        )

    assert legacy.status_code == 200
    assert legacy.json()["auth_type"] == "token"
    assert password.status_code == 503
