"""HTTP contracts for Delivery Loop admission and operator controls."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import func, select

from backend.api import delivery_runs as delivery_api
from backend.config import settings
from backend.models.delivery import (
    DeliveryAction,
    DeliveryCycle,
    DeliveryRun,
    DeliveryTransition,
)
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare
from backend.services import delivery_service
from backend.services.delivery_service import (
    DeliveryCreateSpec,
    DeliveryValidationError,
    create_delivery_run,
)
from backend.tests.test_auth_ws_security import _create_user, secured_client


def _payload(project: Project, repo: MonitoredRepo, **overrides: object) -> dict:
    payload: dict[str, object] = {
        "idempotency_key": f"delivery-api-{uuid4()}",
        "project_id": project.id,
        "monitored_repo_id": repo.id,
        "title": "Fix the delivery race",
        "requirements": "Fix the race and add focused regression coverage.",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "effort_level": "high",
        "max_cycles": 7,
        "max_no_progress": 2,
    }
    payload.update(overrides)
    return payload


async def _scope(
    session_factory,
    *,
    suffix: str,
) -> tuple[Project, MonitoredRepo]:
    async with session_factory() as db:
        project = Project(
            name=f"delivery-api-{suffix}",
            local_path=f"/srv/repos/delivery-api-{suffix}",
            git_url=f"git@github.com:acme/delivery-api-{suffix}.git",
            has_remote=True,
            default_branch="main",
            status="ready",
        )
        db.add(project)
        await db.flush()
        repo = MonitoredRepo(
            repo_full_name=f"acme/delivery-api-{suffix}",
            project_id=project.id,
            webhook_secret="test-secret",
            enabled=True,
            auto_merge=False,
            review_mode="panel",
            wait_for_ci=True,
            required_checks=[
                {
                    "kind": "check_run",
                    "name": "tests",
                    "app_slug": "github-actions",
                }
            ],
            merge_queue_mode="manual",
            default_branch="main",
        )
        db.add(repo)
        await db.commit()
        await db.refresh(project)
        await db.refresh(repo)
        return project, repo


@pytest.fixture
def delivery_enabled(monkeypatch):
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    # API tests verify the durable wake boundary, not a background controller
    # racing the assertions against the in-memory database.
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delivery_flag", "capability_flag", "detail"),
    [
        (False, True, "Delivery Loop mode is disabled"),
        (True, False, "requires Capability Core"),
    ],
)
async def test_create_is_fail_closed_by_both_feature_flags(
    client,
    session_factory,
    monkeypatch,
    delivery_flag,
    capability_flag,
    detail,
):
    project, repo = await _scope(session_factory, suffix=detail[:4])
    monkeypatch.setattr(settings, "delivery_loop_enabled", delivery_flag)
    monkeypatch.setattr(settings, "capability_core_enabled", capability_flag)
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)

    response = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )

    assert response.status_code == 503
    assert detail in response.json()["detail"]
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_delivery_admission_contract_rejects_non_codex_provider(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="runtime-validation")

    response = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            provider="claude",
            model="claude-opus-4-6",
            codex_service_tier="priority",
        ),
    )

    assert response.status_code == 422, response.text
    assert "codex" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_delivery_admission_requires_caller_idempotency_key(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="missing-idempotency")
    payload = _payload(project, repo)
    payload.pop("idempotency_key")

    response = await client.post("/api/delivery-runs", json=payload)

    assert response.status_code == 422, response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_delivery_admission_replays_same_request_and_conflicts_on_rebind(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="idempotent-replay")
    payload = _payload(
        project,
        repo,
        idempotency_key="api-stable-admission-key",
    )

    first = await client.post("/api/delivery-runs", json=payload)
    replay = await client.post("/api/delivery-runs", json=payload)
    conflict = await client.post(
        "/api/delivery-runs",
        json={**payload, "requirements": "A different request."},
    )

    assert first.status_code == 201, first.text
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == first.json()["id"]
    assert replay.json()["developer_task_id"] == first.json()["developer_task_id"]
    assert conflict.status_code == 409, conflict.text
    assert "different Delivery request" in conflict.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert await db.scalar(select(func.count(Task.id))) == 1
        run = await db.get(DeliveryRun, first.json()["id"])
        assert run is not None
        assert run.admission_scope == "system"
        assert run.idempotency_key == "api-stable-admission-key"
        assert len(run.request_hash) == 64


@pytest.mark.asyncio
async def test_feature_flags_stop_admission_but_keep_existing_run_controls(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="flag-recovery")
    original_payload = _payload(project, repo)
    created = await client.post(
        "/api/delivery-runs",
        json=original_payload,
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]

    monkeypatch.setattr(settings, "delivery_loop_enabled", False)
    monkeypatch.setattr(settings, "capability_core_enabled", False)
    replay = await client.post(
        "/api/delivery-runs",
        json=original_payload,
    )
    rejected_new = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, title="Not admitted"),
    )
    listed = await client.get("/api/delivery-runs")
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "dark-launch rollback"},
    )

    assert rejected_new.status_code == 503
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == run_id
    assert [item["id"] for item in listed.json()] == [run_id]
    assert readback.status_code == 200
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["outcome"] == "cancelled"


@pytest.mark.asyncio
async def test_create_is_atomic_and_detail_exposes_durable_evidence(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="atomic")

    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["phase"] == "planning"
    assert body["activity"] == "ready"
    assert body["allowed_actions"] == ["pause", "cancel"]
    assert body["delivery_branch"] == (
        f"ccm/delivery/{body['id']}-fix-the-delivery-race"
    )

    detail = await client.get(f"/api/delivery-runs/{body['id']}")
    assert detail.status_code == 200, detail.text
    detail_body = detail.json()
    assert detail_body["policy_snapshot"]["terminal"] == "ready_to_merge"
    assert detail_body["policy_snapshot"]["auto_merge"] is False
    assert len(detail_body["cycles"]) == 1
    assert detail_body["cycles"][0]["trigger_kind"] == "initial_request"
    assert detail_body["turns"] == []
    assert [item["cause"] for item in detail_body["transitions"]] == [
        "created"
    ]

    task_response = await client.get(f"/api/tasks/{body['developer_task_id']}")
    assert task_response.status_code == 200, task_response.text
    task_body = task_response.json()
    assert task_body["mode"] == "delivery_loop"
    assert task_body["status"] == "delivery_waiting"
    assert task_body["delivery_run_id"] == body["id"]
    assert task_body["delivery_role"] == "developer"
    assert task_body["delivery_phase"] == "planning"
    assert task_body["delivery_activity"] == "ready"
    assert task_body["delivery_outcome"] is None

    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert await db.scalar(select(func.count(DeliveryCycle.id))) == 1
        assert await db.scalar(select(func.count(DeliveryTransition.id))) == 1
        assert await db.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
async def test_active_run_freezes_project_identity_and_destructive_actions(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="project-freeze")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text

    responses = [
        await client.put(
            f"/api/projects/{project.id}",
            json={"git_url": "git@github.com:acme/other.git"},
        ),
        await client.put(
            f"/api/projects/{project.id}",
            json={"default_branch": "develop"},
        ),
        await client.put(
            f"/api/projects/{project.id}",
            json={"has_remote": False},
        ),
        await client.post(f"/api/projects/{project.id}/reclone"),
        await client.delete(f"/api/projects/{project.id}"),
    ]

    assert [response.status_code for response in responses] == [409] * 5
    assert all("Delivery Run" in response.text for response in responses)
    cosmetic = await client.put(
        f"/api/projects/{project.id}",
        json={"badge_color": "blue"},
    )
    assert cosmetic.status_code == 200, cosmetic.text
    async with session_factory() as db:
        persisted = await db.get(Project, project.id)
        assert persisted is not None
        assert persisted.git_url == project.git_url
        assert persisted.default_branch == "main"
        assert persisted.has_remote is True


@pytest.mark.asyncio
async def test_active_run_freezes_monitor_policy_disable_secret_and_delete(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="monitor-freeze")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text

    responses = [
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"project_id": None},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"default_branch": "develop"},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"merge_queue_mode": "auto"},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"auto_merge": True},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"enabled": False},
        ),
        await client.post(f"/api/pr-monitor/repos/{repo.id}/toggle"),
        await client.post(
            f"/api/pr-monitor/repos/{repo.id}/regenerate-secret"
        ),
        await client.delete(f"/api/pr-monitor/repos/{repo.id}"),
    ]

    assert [response.status_code for response in responses] == [409] * 8
    assert all("Delivery Run" in response.text for response in responses)
    no_op = await client.put(
        f"/api/pr-monitor/repos/{repo.id}",
        json={"default_branch": "main"},
    )
    assert no_op.status_code == 200, no_op.text
    async with session_factory() as db:
        persisted = await db.get(MonitoredRepo, repo.id)
        assert persisted is not None
        assert persisted.project_id == project.id
        assert persisted.enabled is True
        assert persisted.auto_merge is False
        assert persisted.merge_queue_mode == "manual"
        assert persisted.default_branch == "main"


@pytest.mark.asyncio
async def test_terminal_run_allows_identity_updates_but_preserves_scope_history(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="terminal-scope")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{created.json()['id']}/cancel",
        json={"reason": "scope guard test"},
    )
    assert cancelled.status_code == 200, cancelled.text

    project_update = await client.put(
        f"/api/projects/{project.id}",
        json={"default_branch": "develop"},
    )
    repo_update = await client.put(
        f"/api/pr-monitor/repos/{repo.id}",
        json={"default_branch": "develop"},
    )
    repo_delete = await client.delete(f"/api/pr-monitor/repos/{repo.id}")
    project_delete = await client.delete(f"/api/projects/{project.id}")

    assert project_update.status_code == 200, project_update.text
    assert repo_update.status_code == 200, repo_update.text
    assert repo_delete.status_code == 409
    assert project_delete.status_code == 409
    assert "referenced by Delivery Run" in repo_delete.text
    assert "referenced by Delivery Run" in project_delete.text


@pytest.mark.asyncio
async def test_create_rolls_back_run_task_cycle_and_todo_on_late_failure(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="rollback")
    async with session_factory() as db:
        todo = ProjectTodo(
            project_id=project.id,
            title="Atomic source",
            prompt="Keep this open if admission fails.",
            status="open",
        )
        db.add(todo)
        await db.commit()
        await db.refresh(todo)
        todo_id = todo.id

    async def fail_after_task_stage(*args, **kwargs):
        raise DeliveryValidationError("injected cycle failure")

    monkeypatch.setattr(
        delivery_service,
        "start_next_cycle",
        fail_after_task_stage,
    )
    response = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, source_todo_id=todo_id),
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "injected cycle failure"
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(DeliveryCycle.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0
        todo = await db.get(ProjectTodo, todo_id)
        assert todo is not None
        assert todo.status == "open"
        assert todo.created_task_id is None


@pytest.mark.asyncio
async def test_source_todo_provenance_is_atomic_and_project_scoped(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="todo")
    other_project, _ = await _scope(session_factory, suffix="todo-other")
    async with session_factory() as db:
        source = ProjectTodo(
            project_id=project.id,
            title="Todo source",
            prompt="Implement it through Delivery Loop.",
            status="open",
        )
        foreign = ProjectTodo(
            project_id=other_project.id,
            title="Foreign source",
            prompt="Must not be attached to another project.",
            status="open",
        )
        db.add_all([source, foreign])
        await db.commit()
        await db.refresh(source)
        await db.refresh(foreign)
        source_id = source.id
        foreign_id = foreign.id

    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, source_todo_id=source_id),
    )
    rejected = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            source_todo_id=foreign_id,
            title="Do not create",
        ),
    )

    assert created.status_code == 201, created.text
    assert rejected.status_code == 400
    assert "does not belong" in rejected.json()["detail"]
    async with session_factory() as db:
        source = await db.get(ProjectTodo, source_id)
        foreign = await db.get(ProjectTodo, foreign_id)
        assert source is not None
        assert source.status == "done"
        assert source.created_task_id == created.json()["developer_task_id"]
        assert foreign is not None
        assert foreign.status == "open"
        assert foreign.created_task_id is None
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1


@pytest.mark.asyncio
async def test_source_todo_conditional_claim_rejects_duplicate_run(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="todo-claim")
    async with session_factory() as db:
        todo = ProjectTodo(
            project_id=project.id,
            title="Claim exactly once",
            prompt="A retry must recover the first result, not fork provenance.",
            status="open",
        )
        db.add(todo)
        await db.commit()
        await db.refresh(todo)
        todo_id = todo.id

    first = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, source_todo_id=todo_id),
    )
    duplicate = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            source_todo_id=todo_id,
            title="Duplicate request",
        ),
    )

    assert first.status_code == 201, first.text
    assert duplicate.status_code == 409, duplicate.text
    assert "already owned by Delivery Run" in duplicate.json()["detail"]
    async with session_factory() as db:
        todo = await db.get(ProjectTodo, todo_id)
        assert todo is not None
        assert todo.status == "done"
        assert todo.created_task_id == first.json()["developer_task_id"]
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert await db.scalar(select(func.count(Task.id))) == 1
        assert await db.scalar(select(func.count(DeliveryCycle.id))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forged",
    [
        {"mode": "delivery_loop"},
        {"delivery_run_id": 1},
        {"delivery_run_id": 0},
        {"delivery_role": "developer"},
    ],
)
async def test_public_task_create_rejects_delivery_controller_forgery(
    client,
    session_factory,
    forged,
):
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Forged Delivery Task",
            "description": "Do not admit through the ordinary Task API.",
            **forged,
        },
    )

    assert response.status_code == 422, response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_public_task_create_tolerates_null_delivery_readback_fields(
    client,
    session_factory,
):
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Ordinary Task",
            "description": "Null readback fields carry no ownership claim.",
            "delivery_run_id": None,
            "delivery_role": None,
        },
    )

    assert response.status_code == 201, response.text
    async with session_factory() as db:
        task = await db.get(Task, response.json()["id"])
        assert task is not None
        assert task.mode == "auto"
        assert task.delivery_run_id is None
        assert task.delivery_role is None


@pytest.mark.asyncio
async def test_pause_resume_cancel_state_contract(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="commands")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]

    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "operator maintenance"},
    )
    duplicate_pause = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "again"},
    )
    paused_cancel = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "paused cancellation is unsafe"},
    )
    resumed = await client.post(
        f"/api/delivery-runs/{run_id}/resume",
        json={"reason": "maintenance complete"},
    )
    duplicate_resume = await client.post(
        f"/api/delivery-runs/{run_id}/resume",
        json={},
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "request withdrawn"},
    )
    terminal_cancel = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "again"},
    )

    assert paused.status_code == 200, paused.text
    assert paused.json()["activity"] == "paused"
    assert paused.json()["pause_reason"] == "operator maintenance"
    assert paused.json()["allowed_actions"] == ["resume"]
    assert duplicate_pause.status_code == 409
    assert paused_cancel.status_code == 409
    assert "only be resumed" in paused_cancel.text
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["activity"] == "ready"
    assert resumed.json()["allowed_actions"] == ["pause", "cancel"]
    assert duplicate_resume.status_code == 409
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["phase"] == "done"
    assert cancelled.json()["activity"] == "terminal"
    assert cancelled.json()["outcome"] == "cancelled"
    assert cancelled.json()["allowed_actions"] == []
    assert terminal_cancel.status_code == 409

    detail = await client.get(f"/api/delivery-runs/{run_id}")
    transitions = detail.json()["transitions"]
    assert [item["cause"] for item in transitions] == [
        "created",
        "pause",
        "resume",
        "cancel",
    ]
    assert [item["metadata"] for item in transitions[1:]] == [
        {"reason": "operator maintenance"},
        {"reason": "maintenance complete"},
        {"reason": "request withdrawn"},
    ]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        task = await db.get(Task, run.developer_task_id)
        assert cycle.status == "cancelled"
        assert cycle.active_run_id is None
        assert task.status == "cancelled"
        assert task.completed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "activity", "wait_reason"),
    [
        ("coding", "running", None),
        ("planning", "waiting", "plan_capability"),
        ("pre_review", "waiting", "code_review_capability"),
    ],
)
async def test_commands_reject_active_exact_generation_effects(
    client,
    session_factory,
    delivery_enabled,
    phase,
    activity,
    wait_reason,
):
    project, repo = await _scope(
        session_factory,
        suffix=f"active-{phase}-{activity}",
    )
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.phase = phase
        run.activity = activity
        run.wait_reason = wait_reason
        await db.commit()

    responses = [
        await client.post(
            f"/api/delivery-runs/{run_id}/pause",
            json={"reason": "unsafe"},
        ),
        await client.post(
            f"/api/delivery-runs/{run_id}/cancel",
            json={"reason": "unsafe"},
        ),
    ]

    assert [response.status_code for response in responses] == [409, 409]
    assert all("exact-generation" in response.text for response in responses)
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    assert readback.json()["allowed_actions"] == []
    assert readback.json()["phase"] == phase
    assert readback.json()["activity"] == activity


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "paused_from_activity"),
    [
        ("coding", "running"),
        ("publishing", "running"),
        ("planning", "waiting"),
        ("pre_review", "waiting"),
    ],
)
async def test_cancel_rejects_paused_exact_generation_effects(
    client,
    session_factory,
    delivery_enabled,
    phase,
    paused_from_activity,
):
    project, repo = await _scope(
        session_factory,
        suffix=f"paused-{phase}-{paused_from_activity}",
    )
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.phase = phase
        run.activity = "paused"
        run.wait_reason = None
        run.paused_from_activity = paused_from_activity
        run.pause_reason = "controller preserved an exact active generation"
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "do not orphan the active generation"},
    )

    assert before.status_code == 200, before.text
    assert before.json()["allowed_actions"] == ["resume"]
    assert cancelled.status_code == 409, cancelled.text
    assert "exact-generation" in cancelled.text
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    assert readback.json()["phase"] == phase
    assert readback.json()["activity"] == "paused"
    assert readback.json()["outcome"] is None
    assert readback.json()["allowed_actions"] == ["resume"]

    async with session_factory() as db:
        causes = list(
            (
                await db.execute(
                    select(DeliveryTransition.cause).where(
                        DeliveryTransition.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert causes == ["created"]


@pytest.mark.asyncio
async def test_monitor_wait_rejects_pause_and_cancel_without_effect_fence(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="monitor-wait")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.phase = "monitoring"
        run.activity = "waiting"
        run.wait_reason = "pr_monitor"
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "hold publication result"},
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "no longer needed"},
    )

    assert before.json()["allowed_actions"] == []
    assert paused.status_code == 409, paused.text
    assert cancelled.status_code == 409, cancelled.text
    assert "exact-generation" in paused.text
    assert "exact-generation" in cancelled.text
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    assert readback.json()["phase"] == "monitoring"
    assert readback.json()["activity"] == "waiting"
    assert readback.json()["wait_reason"] == "pr_monitor"
    assert readback.json()["outcome"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_kind", ["pr_number", "active_action"])
async def test_ready_run_rejects_commands_after_publication_side_effect(
    client,
    session_factory,
    delivery_enabled,
    fence_kind,
):
    project, repo = await _scope(
        session_factory,
        suffix=f"publication-fence-{fence_kind}",
    )
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        if fence_kind == "pr_number":
            run.pr_number = 73
        else:
            db.add(
                DeliveryAction(
                    run_id=run.id,
                    cycle_id=run.current_cycle_id,
                    active_run_id=run.id,
                    action_type="publish_pr",
                    idempotency_key=f"api-publication-fence-{run.id}",
                    desired_version=run.state_version,
                    payload={},
                    payload_hash="e" * 64,
                    status="pending",
                )
            )
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "unsafe after publication"},
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "unsafe after publication"},
    )

    assert before.json()["allowed_actions"] == []
    assert paused.status_code == 409, paused.text
    assert cancelled.status_code == 409, cancelled.text
    expected = "side-effect fence" if fence_kind == "pr_number" else "publication action"
    assert expected in paused.text
    assert expected in cancelled.text


@pytest.mark.asyncio
async def test_locked_state_is_rechecked_after_acl_snapshot(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="stale-command")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    real_lock_run = delivery_api.lock_run

    async def controller_won_race(db, requested_run_id):
        run = await real_lock_run(db, requested_run_id)
        # Emulate the controller transition becoming visible only at the
        # locked read.  The command must recheck this fresh generation before
        # applying pause/cancel.
        run.phase = "coding"
        run.activity = "running"
        run.wait_reason = None
        return run

    monkeypatch.setattr(delivery_api, "lock_run", controller_won_race)
    response = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "stale operator click"},
    )

    assert response.status_code == 409
    assert "exact-generation" in response.text
    async with session_factory() as db:
        # The emulated concurrent state was in the rejected command's
        # transaction and is rolled back; importantly, no pause transition was
        # committed from the stale ready snapshot.
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        assert run.activity == "ready"
        causes = list(
            (
                await db.execute(
                    select(DeliveryTransition.cause).where(
                        DeliveryTransition.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert causes == ["created"]


@pytest.mark.asyncio
async def test_commands_cannot_cross_active_controller_lease(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="active-lease")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.lease_owner = "controller-between-effect-and-state"
        run.controller_generation += 1
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    responses = [
        await client.post(
            f"/api/delivery-runs/{run_id}/{command}",
            json={"reason": "must serialize with controller"},
        )
        for command in ("pause", "cancel")
    ]

    assert before.status_code == 200, before.text
    assert before.json()["allowed_actions"] == []
    assert [response.status_code for response in responses] == [409, 409]
    assert all("lease" in response.text for response in responses)
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        assert (run.phase, run.activity, run.outcome) == (
            "planning",
            "ready",
            None,
        )
        causes = list(
            (
                await db.execute(
                    select(DeliveryTransition.cause).where(
                        DeliveryTransition.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert causes == ["created"]


@pytest.mark.asyncio
async def test_delivery_acl_and_deep_filtered_pagination(
    secured_client,
    monkeypatch,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="delivery-alice@example.com",
        role="member",
    )
    bob_id, _ = await _create_user(
        session_factory,
        email="delivery-bob@example.com",
        role="member",
    )
    visible_project, visible_repo = await _scope(
        session_factory,
        suffix="acl-visible",
    )
    hidden_project, hidden_repo = await _scope(
        session_factory,
        suffix="acl-hidden",
    )
    async with session_factory() as db:
        db.add(
            TeamProjectShare(
                project_id=visible_project.id,
                target_type="user",
                target_id=alice_id,
                shared_by=bob_id,
            )
        )
        await db.commit()

    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)
    headers = {"Authorization": f"Bearer {alice_token}"}

    unsigned = await client.get("/api/delivery-runs")
    visible = await client.post(
        "/api/delivery-runs",
        headers=headers,
        json=_payload(visible_project, visible_repo, title="Visible older run"),
    )
    assert visible.status_code == 201, visible.text
    visible_id = visible.json()["id"]

    # More than the old ``limit * 4`` candidate window are newer but hidden.
    # The ACL-filtered list must keep scanning and still return the visible Run.
    async with session_factory() as db:
        hidden_ids = []
        for index in range(5):
            run = await create_delivery_run(
                db,
                DeliveryCreateSpec(
                    idempotency_key=f"hidden-listing-{index}",
                    project_id=hidden_project.id,
                    monitored_repo_id=hidden_repo.id,
                    title=f"Hidden newer run {index}",
                    requirements="Private Delivery evidence.",
                    created_by=bob_id,
                ),
            )
            hidden_ids.append(run.id)

    all_visible = await client.get(
        "/api/delivery-runs?limit=1",
        headers=headers,
    )
    visible_offset = await client.get(
        "/api/delivery-runs?limit=1&offset=1",
        headers=headers,
    )
    visible_project_list = await client.get(
        f"/api/delivery-runs?project_id={visible_project.id}",
        headers=headers,
    )
    forbidden_project_list = await client.get(
        f"/api/delivery-runs?project_id={hidden_project.id}",
        headers=headers,
    )
    forbidden_detail = await client.get(
        f"/api/delivery-runs/{hidden_ids[-1]}",
        headers=headers,
    )
    allowed_detail = await client.get(
        f"/api/delivery-runs/{visible_id}",
        headers=headers,
    )

    assert unsigned.status_code == 401
    assert all_visible.status_code == 200, all_visible.text
    assert [item["id"] for item in all_visible.json()] == [visible_id]
    assert visible_offset.status_code == 200
    assert visible_offset.json() == []
    assert [item["id"] for item in visible_project_list.json()] == [visible_id]
    assert forbidden_project_list.status_code == 403
    assert forbidden_detail.status_code == 403
    assert allowed_detail.status_code == 200
    assert allowed_detail.json()["created_by"] == alice_id
