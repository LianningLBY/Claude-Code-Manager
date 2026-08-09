"""Sharing lifecycle fences that are easy to regress across API families."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from backend.api import sharing, team_sharing
from backend.models.delivery import DeliveryRun
from backend.models.log_entry import LogEntry
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_share import ProjectShare, SharedTaskReceived
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user import User
from backend.models.user_group import UserGroup, UserGroupMember
from backend.services.project_share_admission import ProjectShareAdmissionError
from backend.services.shared_relay import SharedRelay


def _request(*, user_id: int = 7, role: str = "member"):
    return SimpleNamespace(state=SimpleNamespace(
        user_id=user_id,
        user_role=role,
    ))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status", "expected_detail"),
    [
        (
            ProjectShareAdmissionError(
                "Could not establish the Project sharing fence; retry"
            ),
            409,
            "retry",
        ),
        (ValueError("Project 404 not found"), 404, "Project not found"),
    ],
)
async def test_team_project_share_lock_preserves_admission_error_semantics(
    db_session,
    monkeypatch,
    error,
    expected_status,
    expected_detail,
):
    monkeypatch.setattr(
        team_sharing,
        "lock_project_share_authority",
        AsyncMock(side_effect=error),
    )

    with pytest.raises(HTTPException) as rejected:
        await team_sharing._lock_project_share_authority(404, db_session)

    assert rejected.value.status_code == expected_status
    assert expected_detail in rejected.value.detail


@pytest.mark.asyncio
async def test_feishu_project_share_admission_error_maps_to_conflict(
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(
        sharing.task_sharing,
        "share_project",
        AsyncMock(side_effect=ProjectShareAdmissionError("local Agent active")),
    )

    with pytest.raises(HTTPException) as rejected:
        await sharing.share_project(
            7,
            sharing.ShareRequest(targets=[]),
            db_session,
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail == "local Agent active"


@pytest.mark.asyncio
async def test_team_task_share_reauthorizes_after_taking_task_lock(
    db_session,
    monkeypatch,
):
    task = Task(
        title="authority changes while waiting",
        description="acl fence",
        created_by=7,
    )
    db_session.add(task)
    await db_session.commit()

    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_task", changing_authority)
    with pytest.raises(HTTPException) as denied:
        await team_sharing.share_task(
            task.id,
            team_sharing.ShareBody(target_type="user", target_id=99),
            _request(),
            db_session,
        )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.scalar(
        select(func.count()).select_from(TeamTaskShare)
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["unshare", "list"])
async def test_team_task_share_reads_and_deletes_reauthorize_inside_lock(
    db_session,
    monkeypatch,
    operation,
):
    task = Task(
        title=f"task {operation} authority race",
        description="acl fence",
        created_by=7,
    )
    db_session.add(task)
    await db_session.flush()
    grant = TeamTaskShare(
        task_id=task.id,
        target_type="user",
        target_id=99,
        permission="chat",
        shared_by=7,
    )
    db_session.add(grant)
    await db_session.commit()
    grant_id = grant.id
    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_task", changing_authority)
    with pytest.raises(HTTPException) as denied:
        if operation == "unshare":
            await team_sharing.unshare_task(
                task.id,
                team_sharing.UnshareBody(target_type="user", target_id=99),
                _request(),
                db_session,
            )
        else:
            await team_sharing.list_task_shares(
                task.id,
                _request(),
                db_session,
            )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.get(TeamTaskShare, grant_id) is not None


@pytest.mark.asyncio
async def test_team_project_share_reauthorizes_after_project_lock(
    db_session,
    monkeypatch,
):
    project = Project(name="project-authority-race", status="ready")
    db_session.add(project)
    await db_session.commit()
    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_project", changing_authority)
    with pytest.raises(HTTPException) as denied:
        await team_sharing.share_project(
            project.id,
            team_sharing.ShareBody(target_type="user", target_id=99),
            _request(),
            db_session,
        )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.scalar(
        select(func.count()).select_from(TeamProjectShare)
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["unshare", "list"])
async def test_team_project_share_reads_and_deletes_reauthorize_inside_lock(
    db_session,
    monkeypatch,
    operation,
):
    project = Project(name=f"project-{operation}-authority-race", status="ready")
    db_session.add(project)
    await db_session.flush()
    grant = TeamProjectShare(
        project_id=project.id,
        target_type="user",
        target_id=99,
        shared_by=7,
    )
    db_session.add(grant)
    await db_session.commit()
    grant_id = grant.id
    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_project", changing_authority)
    with pytest.raises(HTTPException) as denied:
        if operation == "unshare":
            await team_sharing.unshare_project(
                project.id,
                team_sharing.UnshareBody(target_type="user", target_id=99),
                _request(),
                db_session,
            )
        else:
            await team_sharing.list_project_shares(
                project.id,
                _request(),
                db_session,
            )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.get(TeamProjectShare, grant_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_fields", "detail_fragment"),
    [
        ({"mode": "delivery_loop"}, "Delivery-owned Tasks"),
        ({"tags": {"pr-review": True}}, "Automated PR workflow Tasks"),
    ],
)
async def test_team_writable_share_rejects_controller_owned_tasks(
    client,
    session_factory,
    task_fields,
    detail_fragment,
):
    async with session_factory() as db:
        target = User(
            email=f"target-{detail_fragment[:3]}@example.test",
            name="Target",
            password_hash="unused",
            is_active=True,
        )
        db.add(target)
        if task_fields.get("mode") == "delivery_loop":
            project = Project(name="delivery-share-policy", status="ready")
            db.add(project)
            await db.flush()
            run = DeliveryRun(
                admission_scope="test",
                idempotency_key="delivery-share-policy",
                request_hash="r" * 64,
                project_id=project.id,
                title="Delivery share policy",
                requirements="Test the sharing boundary",
                requirements_hash="q" * 64,
                policy_snapshot={},
                policy_hash="p" * 64,
                base_branch="main",
                delivery_branch="delivery/share-policy",
            )
            db.add(run)
            await db.flush()
            task_fields = {
                "mode": "delivery_loop",
                "delivery_run_id": run.id,
                "delivery_role": "developer",
            }
        task = Task(
            title="controller owned",
            description="must not become writable",
            **task_fields,
        )
        db.add(task)
        await db.commit()
        target_id = target.id
        task_id = task.id

    response = await client.post(
        f"/api/team/tasks/{task_id}/share",
        json={"target_type": "user", "target_id": target_id},
    )

    assert response.status_code == 409
    assert detail_fragment in response.json()["detail"]
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(TeamTaskShare.task_id == task_id)
        ) == 0


@pytest.mark.asyncio
async def test_delete_group_purges_group_target_grants_only(
    client,
    session_factory,
):
    async with session_factory() as db:
        group = UserGroup(name="disposable-group")
        db.add(group)
        await db.flush()
        group_id = group.id
        db.add_all([
            UserGroupMember(group_id=group_id, user_id=88),
            TeamTaskShare(
                task_id=101,
                target_type="group",
                target_id=group_id,
                permission="chat",
                shared_by=1,
            ),
            TeamProjectShare(
                project_id=202,
                target_type="group",
                target_id=group_id,
                shared_by=1,
            ),
            TeamTaskShare(
                task_id=303,
                target_type="user",
                target_id=group_id,
                permission="chat",
                shared_by=1,
            ),
            TeamProjectShare(
                project_id=404,
                target_type="user",
                target_id=group_id,
                shared_by=1,
            ),
        ])
        await db.commit()

    response = await client.delete(f"/api/team/groups/{group_id}")
    assert response.status_code == 200

    async with session_factory() as db:
        assert await db.get(UserGroup, group_id) is None
        assert await db.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(
                TeamTaskShare.target_type == "group",
                TeamTaskShare.target_id == group_id,
            )
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(
                TeamProjectShare.target_type == "group",
                TeamProjectShare.target_id == group_id,
            )
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(
                TeamTaskShare.target_type == "user",
                TeamTaskShare.target_id == group_id,
            )
        ) == 1
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(
                TeamProjectShare.target_type == "user",
                TeamProjectShare.target_id == group_id,
            )
        ) == 1


@pytest.mark.asyncio
async def test_delete_project_purges_both_project_share_families(
    client,
    session_factory,
):
    async with session_factory() as db:
        project = Project(name="shared-project-to-delete", status="ready")
        db.add(project)
        await db.flush()
        project_id = project.id
        db.add_all([
            ProjectShare(
                project_id=project_id,
                shared_to_open_id="ou-recipient",
                shared_to_name="Recipient",
                shared_to_ccm_url="https://receiver.example.test",
                status="active",
            ),
            TeamProjectShare(
                project_id=project_id,
                target_type="user",
                target_id=77,
                shared_by=1,
            ),
        ])
        await db.commit()

    response = await client.delete(f"/api/projects/{project_id}")
    assert response.status_code == 200

    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(ProjectShare)
            .where(ProjectShare.project_id == project_id)
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(TeamProjectShare.project_id == project_id)
        ) == 0


async def _mismatched_shared_shadow(session_factory):
    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://owner.example.test",
            remote_task_id=42,
            share_token="exact-token",
            status="active",
        )
        replacement = Task(
            title="unrelated replacement",
            description="must not receive relay writes",
            status="pending",
        )
        db.add_all([shared, replacement])
        await db.flush()
        shared.local_task_id = replacement.id
        await db.commit()
        return shared, replacement.id


@pytest.mark.asyncio
async def test_stale_relay_cannot_write_to_unowned_local_task(
    session_factory,
):
    shared, replacement_id = await _mismatched_shared_shadow(session_factory)
    broadcaster = SimpleNamespace(broadcast=AsyncMock())
    relay = SharedRelay(session_factory, broadcaster)

    await relay._handle(
        {
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "stale remote write",
            }
        },
        shared,
    )

    async with session_factory() as db:
        replacement = await db.get(Task, replacement_id)
        assert replacement.status == "pending"
        assert replacement.has_unread is False
        assert await db.scalar(
            select(func.count())
            .select_from(LogEntry)
            .where(LogEntry.task_id == replacement_id)
        ) == 0
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_cleanup_does_not_cancel_unowned_local_task(
    session_factory,
):
    from backend.api.shared import _cleanup_shared

    shared, replacement_id = await _mismatched_shared_shadow(session_factory)
    async with session_factory() as db:
        current = await db.get(SharedTaskReceived, shared.id)
        with patch("backend.main.shared_relay", None):
            await _cleanup_shared(current, db)

    async with session_factory() as db:
        replacement = await db.get(Task, replacement_id)
        assert replacement.status == "pending"
        assert replacement.error_message is None
        assert await db.get(SharedTaskReceived, shared.id) is None


@pytest.mark.asyncio
async def test_directly_deleted_shadow_id_cannot_receive_stale_relay_write(
    session_factory,
):
    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://original-owner.example.test",
            remote_task_id=51,
            share_token="original-token",
            status="active",
        )
        db.add(shared)
        await db.flush()
        shadow = Task(
            title="original shadow",
            description="deleted out of band",
            status="pending",
            shared_from_id=shared.id,
        )
        db.add(shadow)
        await db.flush()
        shadow_id = shadow.id
        shared.local_task_id = shadow_id
        await db.commit()
        await db.delete(shadow)
        await db.commit()
        replacement = Task(
            id=shadow_id,
            title="explicit id replacement",
            description="must remain untouched",
            status="pending",
        )
        db.add(replacement)
        await db.commit()

    broadcaster = SimpleNamespace(broadcast=AsyncMock())
    relay = SharedRelay(session_factory, broadcaster)
    await relay._handle(
        {
            "data": {
                "event_type": "status_change",
                "new_status": "completed",
            }
        },
        shared,
    )

    async with session_factory() as db:
        replacement = await db.get(Task, shadow_id)
        assert replacement.status == "pending"
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_shadow_cannot_proxy_after_received_share_id_reuse(
    session_factory,
):
    from backend.api.chat import ChatMessage, _send_shared_chat
    from backend.api.shared import _cleanup_shared

    async with session_factory() as db:
        old_share = SharedTaskReceived(
            owner_ccm_url="https://old-owner.example.test",
            remote_task_id=61,
            share_token="old-token",
            status="active",
        )
        db.add(old_share)
        await db.flush()
        old_shadow = Task(
            title="old shadow",
            description="survives revoke as cancelled",
            status="pending",
            shared_from_id=old_share.id,
        )
        db.add(old_shadow)
        await db.flush()
        old_share.local_task_id = old_shadow.id
        await db.commit()
        old_share_id = old_share.id
        old_shadow_id = old_shadow.id
        with patch("backend.main.shared_relay", None):
            await _cleanup_shared(old_share, db)

        new_shadow = Task(
            title="new shadow",
            description="different remote owner",
            status="pending",
            shared_from_id=old_share_id,
        )
        db.add(new_shadow)
        await db.flush()
        new_share = SharedTaskReceived(
            id=old_share_id,
            owner_ccm_url="https://new-owner.example.test",
            remote_task_id=62,
            share_token="new-token",
            local_task_id=new_shadow.id,
            status="active",
        )
        db.add(new_share)
        await db.commit()

    async with session_factory() as db:
        stale_shadow = await db.get(Task, old_shadow_id)
        proxy = AsyncMock()
        broadcaster = SimpleNamespace(broadcast=AsyncMock())
        with patch("backend.services.shared_proxy.proxy_chat", proxy), patch(
            "backend.main.broadcaster",
            broadcaster,
        ), pytest.raises(HTTPException) as rejected:
            await _send_shared_chat(
                stale_shadow,
                ChatMessage(message="must not reach the new owner"),
                db,
            )

    assert rejected.value.status_code == 400
    proxy.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()
