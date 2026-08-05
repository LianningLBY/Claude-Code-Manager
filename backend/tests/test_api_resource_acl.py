"""Cross-resource HTTP ACL regressions.

These tests use the real authentication middleware.  They intentionally keep
the identities, Workers, and Projects distinct so an "owns any resource"
check cannot accidentally satisfy an exact-target authorization decision.
"""

import pytest

from backend.models.discussion import Discussion, DiscussionAgent, DiscussionEvent
from backend.config import settings
from backend.models.monitor_session import MonitorSession
from backend.models.pr_monitor import MonitoredRepo, PRReview
from backend.models.project import Project
from backend.models.secret import Secret
from backend.models.sub_agent import SubAgentSession
from backend.models.tag import Tag
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.worker import Worker
from backend.tests.test_auth_ws_security import _create_user, secured_client


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _add_worker(db, *, name: str, owner_user_id: int) -> Worker:
    worker = Worker(
        name=name,
        status="ready",
        owner_user_id=owner_user_id,
        auth_token=f"{name}-token",
    )
    db.add(worker)
    await db.flush()
    return worker


@pytest.mark.asyncio
async def test_org_registry_mutations_are_not_unsigned_public_writes(
    secured_client,
    monkeypatch,
):
    client, session_factory = secured_client
    _, member_token = await _create_user(
        session_factory,
        email="org-member@example.com",
        role="member",
    )
    monkeypatch.setattr(settings, "org_registry_enabled", True)
    registration = {
        "open_id": "ou_attacker",
        "name": "Forged member",
        "ccm_url": "http://127.0.0.1:9",
    }

    unsigned = await client.post("/api/org/register", json=registration)
    member_register = await client.post(
        "/api/org/register",
        headers=_headers(member_token),
        json=registration,
    )
    member_import = await client.post(
        "/api/org/import",
        headers=_headers(member_token),
        json={"members": [], "teams": [], "team_members": []},
    )
    member_registry_change = await client.post(
        "/api/org/registry-changed",
        headers=_headers(member_token),
        json={"new_registry_url": "http://127.0.0.1:9"},
    )

    assert unsigned.status_code == 401
    assert member_register.status_code == 403
    assert member_import.status_code == 403
    assert member_registry_change.status_code == 403


@pytest.mark.asyncio
async def test_task_and_project_targets_require_exact_resource_access(
    secured_client,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="target-alice@example.com",
        role="member",
    )
    bob_id, _ = await _create_user(
        session_factory,
        email="target-bob@example.com",
        role="member",
    )

    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="target-alice-worker",
            owner_user_id=alice_id,
        )
        bob_worker = await _add_worker(
            db,
            name="target-bob-worker",
            owner_user_id=bob_id,
        )
        shared_project = Project(
            name="target-shared-project",
            worker_id=bob_worker.id,
            local_path="/tmp/target-shared-project",
            status="ready",
        )
        victim_project = Project(
            name="target-victim-project",
            worker_id=bob_worker.id,
            local_path="/tmp/target-victim-project",
            status="ready",
        )
        db.add_all([shared_project, victim_project])
        await db.flush()
        db.add(
            TeamProjectShare(
                project_id=shared_project.id,
                target_type="user",
                target_id=alice_id,
                shared_by=bob_id,
            )
        )
        clone_source = Task(
            title="private clone source",
            description="private",
            worker_id=bob_worker.id,
            created_by=bob_id,
        )
        alice_task = Task(
            title="alice task",
            description="owned",
            worker_id=alice_worker.id,
            created_by=alice_id,
        )
        shared_task_by_bob = Task(
            title="shared project task",
            description="shared",
            worker_id=bob_worker.id,
            project_id=shared_project.id,
            created_by=bob_id,
        )
        db.add_all([clone_source, alice_task, shared_task_by_bob])
        await db.commit()
        ids = {
            "alice_worker": alice_worker.id,
            "bob_worker": bob_worker.id,
            "shared_project": shared_project.id,
            "victim_project": victim_project.id,
            "clone_source": clone_source.id,
            "alice_task": alice_task.id,
            "shared_task_by_bob": shared_task_by_bob.id,
        }

    headers = _headers(alice_token)

    local_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={"title": "local", "description": "local"},
    )
    other_worker_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "other worker",
            "description": "other worker",
            "worker_id": ids["bob_worker"],
        },
    )
    own_worker_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "own worker",
            "description": "own worker",
            "worker_id": ids["alice_worker"],
        },
    )
    assert local_task.status_code == 403
    assert other_worker_task.status_code == 403
    assert own_worker_task.status_code == 201

    # A Project share grants work on that exact Project, including inheriting
    # its Worker.  It does not grant the Worker as a free-standing target.
    shared_project_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "shared project",
            "description": "shared project",
            "project_id": ids["shared_project"],
        },
    )
    mismatched_project_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "mismatched project",
            "description": "mismatched project",
            "project_id": ids["shared_project"],
            "worker_id": ids["alice_worker"],
        },
    )
    assert shared_project_task.status_code == 201, shared_project_task.text
    assert shared_project_task.json()["worker_id"] == ids["bob_worker"]
    assert mismatched_project_task.status_code == 400

    inaccessible_clone = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "clone",
            "description": "clone",
            "worker_id": ids["alice_worker"],
            "clone_from_task_id": ids["clone_source"],
        },
    )
    assert inaccessible_clone.status_code == 403

    inaccessible_project_update = await client.put(
        f"/api/tasks/{ids['alice_task']}",
        headers=headers,
        json={"project_id": ids["victim_project"]},
    )
    inaccessible_worker_update = await client.put(
        f"/api/tasks/{ids['alice_task']}",
        headers=headers,
        json={"worker_id": ids["bob_worker"]},
    )
    assert inaccessible_project_update.status_code == 403
    assert inaccessible_worker_update.status_code == 403

    shared_project_update = await client.put(
        f"/api/tasks/{ids['shared_task_by_bob']}",
        headers=headers,
        json={"title": "updated by project collaborator"},
    )
    assert shared_project_update.status_code == 200
    assert shared_project_update.json()["title"] == (
        "updated by project collaborator"
    )

    own_project = await client.post(
        "/api/projects",
        headers=headers,
        json={"name": "alice-project", "worker_id": ids["alice_worker"]},
    )
    other_project = await client.post(
        "/api/projects",
        headers=headers,
        json={"name": "bob-project-from-alice", "worker_id": ids["bob_worker"]},
    )
    local_project = await client.post(
        "/api/projects",
        headers=headers,
        json={"name": "local-project-from-alice"},
    )
    assert own_project.status_code == 201
    assert other_project.status_code == 403
    assert local_project.status_code == 403


@pytest.mark.asyncio
async def test_chat_share_is_read_and_chat_only(secured_client):
    client, session_factory = secured_client
    owner_id, _owner_token = await _create_user(
        session_factory,
        email="chat-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        worker = await _add_worker(
            db,
            name="chat-owner-worker",
            owner_user_id=owner_id,
        )
        task = Task(
            title="chat shared",
            description="shared",
            worker_id=worker.id,
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        db.add(
            TeamTaskShare(
                task_id=task.id,
                target_type="user",
                target_id=recipient_id,
                permission="chat",
                shared_by=owner_id,
            )
        )
        monitor = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="private monitor",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        task_id = task.id

    headers = _headers(recipient_token)
    detail = await client.get(f"/api/tasks/{task_id}", headers=headers)
    history = await client.get(
        f"/api/tasks/{task_id}/chat/history",
        headers=headers,
    )
    delete = await client.delete(f"/api/tasks/{task_id}", headers=headers)
    archive = await client.post(f"/api/tasks/{task_id}/archive", headers=headers)
    create_monitor = await client.post(
        f"/api/tasks/{task_id}/monitor-sessions",
        headers=headers,
        json={"description": "not allowed"},
    )
    assert detail.status_code == 200
    assert history.status_code == 200
    assert delete.status_code == 403
    assert archive.status_code == 403
    assert create_monitor.status_code == 403

    async with session_factory() as db:
        assert await db.get(Task, task_id) is not None


@pytest.mark.asyncio
async def test_project_children_and_reorder_follow_project_acl(
    secured_client,
    tmp_path,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="project-alice@example.com",
        role="member",
    )
    bob_id, bob_token = await _create_user(
        session_factory,
        email="project-bob@example.com",
        role="member",
    )
    project_root = tmp_path / "victim-project"
    project_root.mkdir()
    (project_root / ".env").write_text("SECRET=value\n")

    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="project-alice-worker",
            owner_user_id=alice_id,
        )
        bob_worker = await _add_worker(
            db,
            name="project-bob-worker",
            owner_user_id=bob_id,
        )
        own_project = Project(
            name="project-alice-visible",
            worker_id=alice_worker.id,
            local_path="/tmp/project-alice-visible",
            status="ready",
            tags=["alice-tag"],
        )
        victim_project = Project(
            name="project-bob-private",
            worker_id=bob_worker.id,
            local_path=str(project_root),
            status="ready",
            env_files=[".env"],
            git_credential_type="https",
            git_https_username="victim",
            git_https_token="victim-secret-token",
            tags=["victim-tag"],
        )
        local_project = Project(
            name="project-local-admin-only",
            worker_id=None,
            local_path="/tmp/project-local-admin-only",
            status="ready",
        )
        db.add_all([own_project, victim_project, local_project])
        await db.flush()
        # A stale/inconsistent Task reference must not grant access to the
        # Project's tags.  Project.worker_id is the authoritative location.
        db.add(
            Task(
                title="stale cross-worker reference",
                description="stale",
                worker_id=alice_worker.id,
                project_id=victim_project.id,
                created_by=bob_id,
            )
        )
        await db.commit()
        ids = {
            "own": own_project.id,
            "victim": victim_project.id,
            "local": local_project.id,
        }

    headers = _headers(alice_token)
    todos = await client.get(
        f"/api/projects/{ids['victim']}/todos",
        headers=headers,
    )
    env_file = await client.get(
        f"/api/projects/{ids['victim']}/env-files/.env",
        headers=headers,
    )
    reorder_empty = await client.put(
        "/api/projects/reorder",
        headers=headers,
        json=[],
    )
    reorder_victim = await client.put(
        "/api/projects/reorder",
        headers=headers,
        json=[{"id": ids["victim"], "sort_order": 99}],
    )
    tags = await client.get("/api/projects/tags", headers=headers)
    victim_shares = await client.get(
        f"/api/team/projects/{ids['victim']}/shares",
        headers=headers,
    )
    forge_victim_share = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=headers,
        json={"target_type": "user", "target_id": alice_id},
    )
    assert todos.status_code == 403
    assert env_file.status_code == 403
    assert reorder_empty.status_code == 200
    assert [row["id"] for row in reorder_empty.json()] == [ids["own"]]
    assert "victim-secret-token" not in reorder_empty.text
    assert reorder_victim.status_code == 403
    assert tags.status_code == 200
    assert tags.json() == ["alice-tag"]
    assert victim_shares.status_code == 403
    assert forge_victim_share.status_code == 403

    bob_headers = _headers(bob_token)
    invalid_target_type = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=bob_headers,
        json={"target_type": "invalid", "target_id": alice_id},
    )
    missing_target = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=bob_headers,
        json={"target_type": "user", "target_id": 999_999},
    )
    valid_share = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=bob_headers,
        json={"target_type": "user", "target_id": alice_id},
    )
    assert invalid_target_type.status_code == 422
    assert missing_target.status_code == 404
    assert valid_share.status_code == 200


@pytest.mark.asyncio
async def test_queue_monitor_and_sub_agent_routes_enforce_task_acl_and_service_auth(
    secured_client,
):
    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="agent-owner@example.com",
        role="member",
    )
    outsider_id, outsider_token = await _create_user(
        session_factory,
        email="agent-outsider@example.com",
        role="member",
    )
    async with session_factory() as db:
        owner_worker = await _add_worker(
            db,
            name="agent-owner-worker",
            owner_user_id=owner_id,
        )
        outsider_worker = await _add_worker(
            db,
            name="agent-outsider-worker",
            owner_user_id=outsider_id,
        )
        owner_task = Task(
            title="owner pending",
            description="owner",
            worker_id=owner_worker.id,
            created_by=owner_id,
            status="pending",
        )
        outsider_task = Task(
            title="outsider pending",
            description="outsider",
            worker_id=outsider_worker.id,
            created_by=outsider_id,
            status="pending",
        )
        db.add_all([owner_task, outsider_task])
        await db.flush()
        monitor = MonitorSession(
            task_id=owner_task.id,
            agent_type="monitor",
            source="ccm",
            description="owner monitor",
            status="running",
            checks_done=0,
        )
        sub_agent = SubAgentSession(
            task_id=owner_task.id,
            agent_type="sub_agent",
            source="ccm",
            description="owner sub agent",
            status="running",
            checks_done=0,
        )
        db.add_all([monitor, sub_agent])
        await db.commit()
        ids = {
            "owner_task": owner_task.id,
            "outsider_task": outsider_task.id,
            "monitor": monitor.id,
            "sub_agent": sub_agent.id,
        }

    outsider_headers = _headers(outsider_token)
    owner_headers = _headers(owner_token)
    queue = await client.get("/api/tasks/queue/next", headers=outsider_headers)
    monitor_list = await client.get(
        f"/api/tasks/{ids['owner_task']}/monitor-sessions",
        headers=outsider_headers,
    )
    monitor_spoof = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/monitor-sessions/"
            f"{ids['monitor']}/checks"
        ),
        headers=owner_headers,
        json={"summary": "forged"},
    )
    sub_agent_spoof = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/sub-agent-sessions/"
            f"{ids['sub_agent']}/progress"
        ),
        headers=owner_headers,
        json={"summary": "forged"},
    )
    sub_agent_summary = await client.get(
        f"/api/tasks/{ids['owner_task']}/sub-agents/summary",
        headers=outsider_headers,
    )

    assert queue.status_code == 200
    assert [row["id"] for row in queue.json()] == [ids["outsider_task"]]
    assert monitor_list.status_code == 403
    assert monitor_spoof.status_code == 403
    assert sub_agent_spoof.status_code == 403
    assert sub_agent_summary.status_code == 403

    service_headers = {"Authorization": "Bearer security-service-token"}
    monitor_service = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/monitor-sessions/"
            f"{ids['monitor']}/checks"
        ),
        headers=service_headers,
        json={"summary": "real service callback"},
    )
    sub_agent_service = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/sub-agent-sessions/"
            f"{ids['sub_agent']}/progress"
        ),
        headers=service_headers,
        json={"summary": "real service callback"},
    )
    assert monitor_service.status_code == 200, monitor_service.text
    assert sub_agent_service.status_code == 200, sub_agent_service.text


@pytest.mark.asyncio
async def test_pr_monitor_detail_reviews_and_mutations_require_exact_worker_owner(
    secured_client,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="pr-alice@example.com",
        role="member",
    )
    bob_id, bob_token = await _create_user(
        session_factory,
        email="pr-bob@example.com",
        role="member",
    )
    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="pr-alice-worker",
            owner_user_id=alice_id,
        )
        bob_worker = await _add_worker(
            db,
            name="pr-bob-worker",
            owner_user_id=bob_id,
        )
        repo = MonitoredRepo(
            repo_full_name="private/repository",
            worker_id=bob_worker.id,
            webhook_secret="full-private-secret",
        )
        db.add(repo)
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=7,
            head_sha="a" * 40,
            pr_title="Private PR",
            pr_author="private-author",
            pr_url="https://example.invalid/private/repository/pull/7",
            status="pending",
        )
        db.add(review)
        await db.commit()
        ids = {
            "alice_worker": alice_worker.id,
            "bob_worker": bob_worker.id,
            "repo": repo.id,
            "review": review.id,
        }

    alice_headers = _headers(alice_token)
    denied = [
        await client.get(
            f"/api/pr-monitor/repos/{ids['repo']}",
            headers=alice_headers,
        ),
        await client.put(
            f"/api/pr-monitor/repos/{ids['repo']}",
            headers=alice_headers,
            json={"enabled": False},
        ),
        await client.get(
            f"/api/pr-monitor/repos/{ids['repo']}/reviews",
            headers=alice_headers,
        ),
        await client.get(
            f"/api/pr-monitor/reviews/{ids['review']}",
            headers=alice_headers,
        ),
        await client.post(
            f"/api/pr-monitor/repos/{ids['repo']}/toggle",
            headers=alice_headers,
        ),
        await client.post(
            f"/api/pr-monitor/repos/{ids['repo']}/regenerate-secret",
            headers=alice_headers,
        ),
        await client.post(
            "/api/pr-monitor/repos",
            headers=alice_headers,
            json={
                "repo_full_name": "private/new-repository",
                "worker_id": ids["bob_worker"],
            },
        ),
    ]
    assert [response.status_code for response in denied] == [403] * len(denied)

    bob_headers = _headers(bob_token)
    detail = await client.get(
        f"/api/pr-monitor/repos/{ids['repo']}",
        headers=bob_headers,
    )
    reviews = await client.get(
        f"/api/pr-monitor/repos/{ids['repo']}/reviews",
        headers=bob_headers,
    )
    assert detail.status_code == 200
    assert detail.json()["webhook_secret"] == "full***"
    assert "full-private-secret" not in detail.text
    assert reviews.status_code == 200
    assert [row["id"] for row in reviews.json()] == [ids["review"]]


@pytest.mark.asyncio
async def test_discussion_events_require_discussion_owner(secured_client):
    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="discussion-owner@example.com",
        role="member",
    )
    _, outsider_token = await _create_user(
        session_factory,
        email="discussion-outsider@example.com",
        role="member",
    )
    async with session_factory() as db:
        discussion = Discussion(
            title="private discussion",
            creator_user_id=owner_id,
        )
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
        )
        db.add(agent)
        await db.flush()
        db.add(
            DiscussionEvent(
                discussion_id=discussion.id,
                agent_id=agent.id,
                event_type="assistant",
                content="private event",
            )
        )
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    path = f"/api/discussions/{discussion_id}/agents/{agent_id}/events"
    outsider = await client.get(path, headers=_headers(outsider_token))
    owner = await client.get(path, headers=_headers(owner_token))
    assert outsider.status_code == 403
    assert owner.status_code == 200
    assert owner.json()[0]["content"] == "private event"


@pytest.mark.asyncio
async def test_host_files_and_global_git_credentials_are_admin_only(
    secured_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = secured_client
    _, member_token = await _create_user(
        session_factory,
        email="host-files-member@example.com",
        role="member",
    )
    member_headers = _headers(member_token)

    member_files = await client.get(
        "/api/files/list",
        params={"path": str(tmp_path)},
        headers=member_headers,
    )
    member_git = await client.get("/api/settings/git", headers=member_headers)
    assert member_files.status_code == 403
    assert member_git.status_code == 403

    service_headers = {"Authorization": "Bearer security-service-token"}
    admin_files = await client.get(
        "/api/files/list",
        params={"path": str(tmp_path)},
        headers=service_headers,
    )
    admin_git = await client.get("/api/settings/git", headers=service_headers)
    assert admin_files.status_code == 200
    assert admin_git.status_code == 200

    target = tmp_path / "uploads"
    target.mkdir()
    traversal = await client.post(
        "/api/files/upload",
        headers=service_headers,
        data={"target_dir": str(target)},
        files=[
            ("files", ("prefix.txt", b"must roll back")),
            ("files", ("../escape.txt", b"escape")),
        ],
    )
    assert traversal.status_code == 400
    assert not (tmp_path / "escape.txt").exists()
    assert not (target / "prefix.txt").exists()

    import backend.api.files as files_api

    monkeypatch.setattr(files_api, "MAX_UPLOAD_TOTAL_SIZE", 5)
    over_total = await client.post(
        "/api/files/upload",
        headers=service_headers,
        data={"target_dir": str(target)},
        files=[
            ("files", ("first.txt", b"abc")),
            ("files", ("second.txt", b"def")),
        ],
    )
    assert over_total.status_code == 400
    assert "combined" in over_total.json()["detail"].lower()
    assert not (target / "first.txt").exists()
    assert not (target / "second.txt").exists()

    normal = await client.post(
        "/api/files/upload",
        headers=service_headers,
        data={"target_dir": str(target)},
        files={"files": ("safe.txt", b"safe")},
    )
    assert normal.status_code == 200, normal.text
    assert (target / "safe.txt").read_bytes() == b"safe"


@pytest.mark.asyncio
async def test_global_secrets_and_cross_project_tag_cascades_are_admin_only(
    secured_client,
):
    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email="global-data-member@example.com",
        role="member",
    )
    other_id, _ = await _create_user(
        session_factory,
        email="global-data-other@example.com",
        role="member",
    )
    async with session_factory() as db:
        member_worker = await _add_worker(
            db,
            name="global-data-member-worker",
            owner_user_id=member_id,
        )
        other_worker = await _add_worker(
            db,
            name="global-data-other-worker",
            owner_user_id=other_id,
        )
        secret = Secret(name="global-token", content="plaintext-global-secret")
        tag = Tag(name="global-label", color="rose", created_by=member_id)
        victim_project = Project(
            name="global-data-victim-project",
            worker_id=other_worker.id,
            local_path="/tmp/global-data-victim-project",
            status="ready",
            tags=["global-label"],
        )
        local_task = Task(
            title="legacy local task",
            description="local",
            worker_id=None,
            created_by=member_id,
            session_id="private-session",
        )
        db.add_all([secret, tag, victim_project, local_task])
        await db.commit()
        ids = {
            "member_worker": member_worker.id,
            "secret": secret.id,
            "tag": tag.id,
            "victim_project": victim_project.id,
            "local_task": local_task.id,
        }

    headers = _headers(member_token)
    secret_list = await client.get("/api/secrets", headers=headers)
    secret_detail = await client.get(
        f"/api/secrets/{ids['secret']}",
        headers=headers,
    )
    task_with_secret = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "guess secret",
            "description": "guess secret",
            "worker_id": ids["member_worker"],
            "secret_ids": [ids["secret"]],
        },
    )
    ordinary_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "ordinary",
            "description": "ordinary",
            "worker_id": ids["member_worker"],
        },
    )
    chat_with_secret = await client.post(
        f"/api/tasks/{ids['local_task']}/chat",
        headers=headers,
        json={"message": "echo it", "secret_ids": [ids["secret"]]},
    )
    rename_tag = await client.put(
        f"/api/tags/{ids['tag']}",
        headers=headers,
        json={"name": "stolen-global-label"},
    )
    delete_tag = await client.delete(
        f"/api/tags/{ids['tag']}",
        headers=headers,
    )

    assert secret_list.status_code == 403
    assert secret_detail.status_code == 403
    assert task_with_secret.status_code == 403
    assert ordinary_task.status_code == 201
    assert chat_with_secret.status_code == 403
    assert rename_tag.status_code == 403
    assert delete_tag.status_code == 403

    async with session_factory() as db:
        victim = await db.get(Project, ids["victim_project"])
        assert victim.tags == ["global-label"]
        assert await db.get(Tag, ids["tag"]) is not None

    service_headers = {"Authorization": "Bearer security-service-token"}
    admin_secret = await client.get(
        f"/api/secrets/{ids['secret']}",
        headers=service_headers,
    )
    assert admin_secret.status_code == 200
    assert admin_secret.json()["content"] == "plaintext-global-secret"
