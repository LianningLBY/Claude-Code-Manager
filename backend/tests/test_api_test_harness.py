from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from backend.api import test_harness as test_harness_api
from backend.database import get_db
from backend.models.task import Task
from backend.services.test_harness import TestHarnessService as HarnessService
from backend.services.test_harness_contracts import TestHarnessSpec as HarnessSpec


@pytest.mark.asyncio
async def test_task_test_run_api_persists_lists_and_cancels_fixed_url(
    monkeypatch,
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="API Harness",
            status="completed",
            provider="claude",
            model="claude-opus-4-6",
            effort_level="high",
        )
        db.add(task)
        other_task = Task(
            title="Other API Harness",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(other_task)
        await db.commit()
        task_id = task.id
        other_task_id = other_task.id

    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    async def _attach_marker(*, run_id: str, inline: bool):
        assert inline is False
        await service._update_run(
            run_id,
            values={"browser_review_job_id": "b" * 32},
            event_type="lifecycle",
            title="Browser reserved",
            source_key="test:browser-reserved",
        )
        return object()

    start_browser = AsyncMock(side_effect=_attach_marker)
    monkeypatch.setattr(service, "start_fixed_url_browser", start_browser)
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)

    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        started = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Verify the settings screen",
                "allow_actions": False,
                "max_actions": 0,
                "idempotency_key": "api-fixed-url-v1",
            },
        )
        assert started.status_code == 202, started.text
        payload = started.json()
        run_id = payload["id"]
        assert payload["target_kind"] == "fixed_url"
        assert payload["runtime"]["provider"] == "claude"
        assert payload["runtime"]["context_policy"] == "isolated_black_box_v1"
        assert payload["stage"] == "waiting_for_browser"
        start_browser.assert_awaited_once_with(run_id=run_id, inline=False)

        listed = await client.get(f"/api/tasks/{task_id}/test-runs")
        assert listed.status_code == 200
        assert [item["id"] for item in listed.json()] == [run_id]

        duplicate = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Verify the settings screen",
                "allow_actions": False,
                "max_actions": 0,
                "idempotency_key": "api-fixed-url-v1",
            },
        )
        assert duplicate.status_code == 202
        assert duplicate.json()["id"] == run_id
        assert start_browser.await_count == 1

        cancelled = await client.post(
            f"/api/tasks/{task_id}/test-runs/{run_id}/cancel"
        )
        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json()["status"] == "cancelled"

        repeated = await client.post(
            f"/api/tasks/{task_id}/test-runs/{run_id}/repeat"
        )
        assert repeated.status_code == 202, repeated.text
        repeated_payload = repeated.json()
        assert repeated_payload["id"] != run_id
        assert repeated_payload["parent_run_id"] == run_id
        assert repeated_payload["browser_review_job_id"] == "b" * 32
        assert start_browser.await_count == 2
        assert start_browser.await_args.kwargs == {
            "run_id": repeated_payload["id"],
            "inline": False,
        }

        foreign = await service.start_task_run(
            task_id=other_task_id,
            spec=HarnessSpec(
                target_kind="fixed_url",
                target={"url": "http://127.0.0.1:5174"},
                goal="Foreign Task evidence must remain private",
            ),
        )
        compared = await client.get(
            f"/api/tasks/{task_id}/test-runs/{run_id}/compare/{foreign.id}"
        )
        assert compared.status_code == 404


@pytest.mark.asyncio
async def test_public_test_run_waits_for_parent_task_terminal(db_factory):
    async with db_factory() as db:
        task = Task(title="Running", status="executing")
        db.add(task)
        await db.commit()
        task_id = task.id

    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Do not race the active turn",
            },
        )

    assert response.status_code == 409
    assert "Agent 可直接调用测试工具" in response.json()["detail"]
