"""Public Capability API contract and ACL tests."""

from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest

from backend.config import settings
from backend.models.task import Task
from backend.services.capability_registry import (
    CapabilityDefinition,
    register_capability,
    unregister_capability,
)


@pytest.fixture(autouse=True)
def capability_runtime():
    previous = settings.capability_core_enabled
    settings.capability_core_enabled = True
    unregister_capability("plan")
    register_capability(
        CapabilityDefinition(
            capability_key="plan",
            executor_kind="fake_plan",
            executor_config={"secret_route": "must-not-leak"},
            policy_snapshot={"gate": "server-owned"},
            max_attempts=2,
        )
    )
    yield
    unregister_capability("plan")
    settings.capability_core_enabled = previous


async def _task(session_factory, **values) -> Task:
    async with session_factory() as db:
        task = Task(title="API capability target", **values)
        db.add(task)
        await db.commit()
        return task


def _body(**overrides):
    return {
        "capability": "plan",
        "request": {"prompt": "propose a safe plan"},
        "idempotency_key": "api-request-1",
        **overrides,
    }


@pytest.mark.asyncio
async def test_public_create_freezes_server_owned_contract(
    client,
    session_factory,
):
    task = await _task(session_factory)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["created"] is True
    invocation = payload["invocation"]
    assert invocation["source"] == "human_request"
    assert invocation["purpose"] == "advisory"
    assert invocation["resume_policy"] == "attach_only"
    assert invocation["executor_kind"] == "fake_plan"
    assert "executor_config" not in invocation
    assert "policy_snapshot" not in invocation
    assert invocation["active_execution"]["status"] == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forged_field,forged_value",
    [
        ("source", "delivery_controller"),
        ("purpose", "required_gate"),
        ("resume_policy", "controller"),
        ("executor_kind", "attacker"),
        ("executor_config", {"command": "unsafe"}),
        ("policy_snapshot", {"bypass": True}),
        ("subject_ref", {"task_id": 999}),
        ("result_hash", "0" * 64),
    ],
)
async def test_public_create_rejects_server_owned_fields(
    client,
    session_factory,
    forged_field,
    forged_value,
):
    task = await _task(session_factory)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(**{forged_field: forged_value}),
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_api_idempotent_replay_and_conflict(client, session_factory):
    task = await _task(session_factory)
    first = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert first.status_code == 201

    replay = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert replay.status_code == 200
    assert replay.json()["created"] is False
    assert (
        replay.json()["invocation"]["id"]
        == first.json()["invocation"]["id"]
    )

    conflict = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(request={"prompt": "different"}),
    )
    assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_api_rejects_invalid_key_and_oversized_request(
    client,
    session_factory,
):
    task = await _task(session_factory)
    invalid_key = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(capability="BAD key"),
    )
    oversized = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(
            request={"text": "界" * 20_000},
            idempotency_key="oversized",
        ),
    )
    assert invalid_key.status_code == 422
    assert oversized.status_code == 422


@pytest.mark.asyncio
async def test_flag_off_keeps_read_cancel_and_replay_available(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation = created.json()["invocation"]
    settings.capability_core_enabled = False

    replay = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert replay.status_code == 200
    blocked = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="new-disabled-request"),
    )
    assert blocked.status_code == 503

    listed = await client.get(
        f"/api/tasks/{task.id}/capability-invocations"
    )
    assert listed.status_code == 200
    assert [item["id"] for item in listed.json()] == [invocation["id"]]
    read = await client.get(
        f"/api/capability-invocations/{invocation['id']}"
    )
    assert read.status_code == 200

    cancelled = await client.post(
        f"/api/capability-invocations/{invocation['id']}/cancel",
        json={"expected_state_version": invocation["state_version"]},
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    repeated = await client.post(
        f"/api/capability-invocations/{invocation['id']}/cancel",
        json={
            "expected_state_version": cancelled.json()["state_version"],
        },
    )
    assert repeated.status_code == 200
    assert repeated.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_remote_and_shared_tasks_are_rejected(client, session_factory):
    remote = await _task(session_factory, worker_id=17)
    shared = await _task(session_factory, shared_from_id=18)

    remote_response = await client.post(
        f"/api/tasks/{remote.id}/capability-invocations",
        json=_body(idempotency_key="remote"),
    )
    shared_response = await client.post(
        f"/api/tasks/{shared.id}/capability-invocations",
        json=_body(idempotency_key="shared"),
    )

    assert remote_response.status_code == 409
    assert shared_response.status_code == 409


@pytest.mark.asyncio
async def test_create_and_cancel_require_task_control(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    denied = AsyncMock(side_effect=HTTPException(403, "denied"))
    monkeypatch.setattr(api, "require_task_control", denied)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert response.status_code == 403
    denied.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_and_list_require_task_access(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation_id = created.json()["invocation"]["id"]
    denied = AsyncMock(side_effect=HTTPException(403, "denied"))
    monkeypatch.setattr(api, "require_task_access", denied)

    listed = await client.get(
        f"/api/tasks/{task.id}/capability-invocations"
    )
    read = await client.get(
        f"/api/capability-invocations/{invocation_id}"
    )
    assert listed.status_code == 403
    assert read.status_code == 403
    assert denied.await_count == 2
