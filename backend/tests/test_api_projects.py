"""Tests for Project API endpoints."""
from pathlib import Path
import subprocess

import pytest
from unittest.mock import patch, AsyncMock

from backend.models.discussion import Discussion
from backend.models.project import Project


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()


@pytest.fixture
def mock_bg_tasks():
    """Patch background git tasks to prevent real git operations."""
    with patch("backend.api.projects._clone_repo", new_callable=AsyncMock) as mock_clone, \
         patch("backend.api.projects._init_local_repo", new_callable=AsyncMock) as mock_init:
        yield mock_clone, mock_init


@pytest.mark.asyncio
async def test_list_projects_empty(client):
    resp = await client.get("/api/projects")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_project_with_git_url(client, mock_bg_tasks):
    mock_clone, mock_init = mock_bg_tasks
    resp = await client.post("/api/projects", json={
        "name": "my-remote-proj",
        "git_url": "https://github.com/user/repo.git",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "my-remote-proj"
    assert data["has_remote"] is True
    assert data["git_url"] == "https://github.com/user/repo.git"
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_create_project_local_no_git_url(client, mock_bg_tasks):
    mock_clone, mock_init = mock_bg_tasks
    resp = await client.post("/api/projects", json={"name": "local-proj"})
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "local-proj"
    assert data["has_remote"] is False
    assert data["git_url"] is None


@pytest.mark.asyncio
async def test_create_project_duplicate_name(client, mock_bg_tasks):
    await client.post("/api/projects", json={"name": "dup-proj"})
    resp = await client.post("/api/projects", json={"name": "dup-proj"})
    assert resp.status_code == 400
    assert "already exists" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_get_project(client, mock_bg_tasks):
    create_resp = await client.post("/api/projects", json={"name": "proj-get"})
    project_id = create_resp.json()["id"]
    resp = await client.get(f"/api/projects/{project_id}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "proj-get"


@pytest.mark.asyncio
async def test_get_project_not_found(client):
    resp = await client.get("/api/projects/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_project(client, mock_bg_tasks):
    create_resp = await client.post("/api/projects", json={"name": "proj-update"})
    project_id = create_resp.json()["id"]
    resp = await client.put(f"/api/projects/{project_id}", json={"name": "proj-renamed"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "proj-renamed"


@pytest.mark.asyncio
async def test_update_project_git_url_sets_has_remote(client, mock_bg_tasks):
    """Setting git_url via update auto-sets has_remote=True."""
    create_resp = await client.post("/api/projects", json={"name": "local-2-remote"})
    project_id = create_resp.json()["id"]
    assert create_resp.json()["has_remote"] is False

    resp = await client.put(f"/api/projects/{project_id}", json={
        "git_url": "https://github.com/user/repo.git"
    })
    assert resp.status_code == 200
    assert resp.json()["has_remote"] is True


@pytest.mark.asyncio
async def test_update_project_not_found(client):
    resp = await client.put("/api/projects/9999", json={"name": "X"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_project(client, mock_bg_tasks):
    create_resp = await client.post("/api/projects", json={"name": "proj-del"})
    project_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/projects/{project_id}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    resp = await client.get(f"/api/projects/{project_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_project_not_found(client):
    resp = await client.delete("/api/projects/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("discussion_status", ["active", "closing"])
async def test_delete_project_rejects_provider_capable_discussion_lease(
    client,
    mock_bg_tasks,
    session_factory,
    discussion_status,
):
    created = await client.post(
        "/api/projects",
        json={"name": f"project-delete-{discussion_status}-discussion"},
    )
    project_id = created.json()["id"]
    async with session_factory() as db:
        discussion = Discussion(
            title=f"{discussion_status} deletion lease",
            project_id=project_id,
            status=discussion_status,
        )
        db.add(discussion)
        await db.commit()

    response = await client.delete(f"/api/projects/{project_id}")

    assert response.status_code == 409
    assert "active or closing Discussion" in response.json()["detail"]
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None


@pytest.mark.asyncio
async def test_delete_project_requires_closed_discussion_to_be_deleted_first(
    client,
    mock_bg_tasks,
    session_factory,
):
    created = await client.post(
        "/api/projects",
        json={"name": "project-delete-closed-discussion"},
    )
    project_id = created.json()["id"]
    async with session_factory() as db:
        discussion = Discussion(
            title="closed deletion lease",
            project_id=project_id,
            status="closed",
        )
        db.add(discussion)
        await db.commit()
        discussion_id = discussion.id

    rejected = await client.delete(f"/api/projects/{project_id}")
    assert rejected.status_code == 409
    assert "Delete Discussion" in rejected.json()["detail"]
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None
        assert await db.get(Discussion, discussion_id) is not None

    cleaned = await client.delete(f"/api/discussions/{discussion_id}")
    assert cleaned.status_code == 200
    deleted = await client.delete(f"/api/projects/{project_id}")
    assert deleted.status_code == 200
    async with session_factory() as db:
        assert await db.get(Discussion, discussion_id) is None
        assert await db.get(Project, project_id) is None


@pytest.mark.asyncio
async def test_reclone_success(client, mock_bg_tasks, session_factory):
    """Reclone on a remote project resets status and triggers background clone."""
    mock_clone, mock_init = mock_bg_tasks
    create_resp = await client.post("/api/projects", json={
        "name": "proj-reclone",
        "git_url": "https://github.com/user/repo.git",
    })
    project_id = create_resp.json()["id"]

    resp = await client.post(f"/api/projects/{project_id}/reclone")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


@pytest.mark.asyncio
async def test_reclone_local_project_rejected(client, mock_bg_tasks):
    """Cannot reclone a local project (has_remote=False)."""
    create_resp = await client.post("/api/projects", json={"name": "proj-local-reclone"})
    project_id = create_resp.json()["id"]
    resp = await client.post(f"/api/projects/{project_id}/reclone")
    assert resp.status_code == 400
    assert "local project" in resp.json()["detail"].lower()


# === AGENTS.md injection (Codex instruction file) ===


def test_inject_agents_md_creates_symlink(tmp_path):
    from backend.api.projects import _inject_agents_md
    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    assert _inject_agents_md(str(tmp_path)) is True
    agents = tmp_path / "AGENTS.md"
    assert agents.exists()
    # Symlink (or fallback pointer file) must surface CLAUDE.md's guidance
    if agents.is_symlink():
        assert agents.read_text() == "# guide\n"
    else:
        assert "CLAUDE.md" in agents.read_text()


def test_inject_agents_md_noop_without_claude_md(tmp_path):
    from backend.api.projects import _inject_agents_md
    assert _inject_agents_md(str(tmp_path)) is False
    assert not (tmp_path / "AGENTS.md").exists()


def test_inject_agents_md_noop_when_exists(tmp_path):
    from backend.api.projects import _inject_agents_md
    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    (tmp_path / "AGENTS.md").write_text("custom\n")
    assert _inject_agents_md(str(tmp_path)) is False
    assert (tmp_path / "AGENTS.md").read_text() == "custom\n"


@pytest.mark.asyncio
async def test_init_local_repo_preserves_existing_claude_md(db_factory, tmp_path, monkeypatch):
    """存量目录（有文件但未 git init）里已有的 CLAUDE.md 不被模板覆盖。"""
    from backend.api import projects as projects_mod
    monkeypatch.setattr(projects_mod, "async_session", db_factory)

    async with db_factory() as db:
        p = Project(name="pre", local_path=str(tmp_path), status="pending")
        db.add(p)
        await db.commit()
        await db.refresh(p)
        pid = p.id

    (tmp_path / "CLAUDE.md").write_text("# my existing guide\n")

    await projects_mod._init_local_repo(
        pid, str(tmp_path), "pre", "main",
        git_config={"git_user_name": "t", "git_user_email": "t@t.co"},
    )

    assert (tmp_path / "CLAUDE.md").read_text() == "# my existing guide\n"
    # AGENTS.md 补上了（指向未被覆盖的原 CLAUDE.md）
    assert (tmp_path / "AGENTS.md").exists()
    async with db_factory() as db:
        p2 = await db.get(Project, pid)
        assert p2.status == "ready"


@pytest.mark.asyncio
async def test_init_local_repo_preserves_both_existing_docs(db_factory, tmp_path, monkeypatch):
    """两个文件都已存在时全部原样保留，且不因无事可提交而报错。"""
    from backend.api import projects as projects_mod
    monkeypatch.setattr(projects_mod, "async_session", db_factory)

    async with db_factory() as db:
        p = Project(name="pre2", local_path=str(tmp_path), status="pending")
        db.add(p)
        await db.commit()
        await db.refresh(p)
        pid = p.id

    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    (tmp_path / "AGENTS.md").write_text("# my own agents doc\n")

    await projects_mod._init_local_repo(
        pid, str(tmp_path), "pre2", "main",
        git_config={"git_user_name": "t", "git_user_email": "t@t.co"},
    )

    assert (tmp_path / "CLAUDE.md").read_text() == "# guide\n"
    assert (tmp_path / "AGENTS.md").read_text() == "# my own agents doc\n"
    async with db_factory() as db:
        p2 = await db.get(Project, pid)
        assert p2.status == "ready"


@pytest.mark.asyncio
async def test_existing_remote_project_adds_missing_origin_before_ready(
    db_factory,
    tmp_path,
    monkeypatch,
):
    from backend.api import projects as projects_mod
    from backend.services import delivery_setup

    remote = tmp_path / "remote.git"
    subprocess.run(
        ["git", "init", "--bare", str(remote)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    local = tmp_path / "existing"
    local.mkdir()
    _git(local, "init", "-b", "main")
    _git(local, "config", "user.name", "CCM Test")
    _git(local, "config", "user.email", "ccm@example.invalid")
    (local / "seed.txt").write_text("seed\n", encoding="utf-8")
    _git(local, "add", "seed.txt")
    _git(local, "commit", "-m", "seed")

    monkeypatch.setattr(projects_mod, "async_session", db_factory)
    monitor_setup = AsyncMock(return_value=None)
    monkeypatch.setattr(
        delivery_setup,
        "try_auto_configure_delivery_monitor",
        monitor_setup,
    )
    async with db_factory() as db:
        project = Project(
            name="existing-remote",
            local_path=str(local),
            git_url=str(remote),
            has_remote=True,
            default_branch="main",
            status="pending",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id

    await projects_mod._clone_repo(
        project_id,
        str(remote),
        str(local),
        "existing-remote",
        "main",
        {
            "git_author_name": "CCM Test",
            "git_author_email": "ccm@example.invalid",
        },
    )

    assert _git(local, "remote", "get-url", "origin") == str(remote)
    async with db_factory() as db:
        stored = await db.get(Project, project_id)
        assert stored is not None
        assert stored.status == "ready"
        assert stored.error_message is None
    monitor_setup.assert_awaited_once_with(project_id)


@pytest.mark.asyncio
async def test_existing_remote_project_rejects_ambiguous_origin(tmp_path):
    from backend.api import projects as projects_mod

    local = tmp_path / "ambiguous"
    local.mkdir()
    _git(local, "init", "-b", "main")
    first = "https://github.com/acme/first.git"
    second = "https://github.com/acme/second.git"
    _git(local, "remote", "add", "origin", first)
    _git(local, "config", "--add", "remote.origin.url", second)

    with pytest.raises(RuntimeError, match="at most one fetch"):
        await projects_mod._prepare_existing_project_remote(
            str(local),
            first,
            env=None,
        )


@pytest.mark.asyncio
async def test_existing_remote_project_repairs_push_only_origin(tmp_path):
    from backend.api import projects as projects_mod

    remote = tmp_path / "push-only-remote.git"
    subprocess.run(
        ["git", "init", "--bare", "--initial-branch=main", str(remote)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    local = tmp_path / "push-only"
    local.mkdir()
    _git(local, "init", "-b", "main")
    _git(local, "config", "remote.origin.pushurl", str(remote))
    _git(local, "config", "user.name", "CCM Test")
    _git(local, "config", "user.email", "ccm@example.invalid")
    (local / "seed.txt").write_text("seed\n", encoding="utf-8")
    _git(local, "add", "seed.txt")
    _git(local, "commit", "-m", "seed")
    _git(local, "push", "origin", "main")

    await projects_mod._prepare_existing_project_remote(
        str(local),
        str(remote),
        env=None,
    )

    assert _git(local, "remote", "get-url", "origin") == str(remote)
    assert _git(local, "remote", "get-url", "--push", "origin") == str(remote)
