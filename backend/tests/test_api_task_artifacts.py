"""Task-scoped artifact download and path-boundary regressions."""

from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, Request
from fastapi.responses import Response
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.api import task_artifacts
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task
import backend.models.team_share  # noqa: F401
import backend.models.user_group  # noqa: F401
from backend.services.worker_proxy import WorkerProxy


@pytest_asyncio.fixture
async def artifact_client(db_engine):
    session_factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    app = FastAPI()

    @app.middleware("http")
    async def test_identity(request: Request, call_next):
        raw_user_id = request.headers.get("X-Test-User")
        request.state.user_id = int(raw_user_id) if raw_user_id else None
        request.state.user_role = request.headers.get(
            "X-Test-Role",
            "super_admin",
        )
        return await call_next(request)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.include_router(task_artifacts.router)
    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, session_factory


async def _create_local_task(
    session_factory,
    root: Path,
    *,
    created_by: int | None = None,
    last_cwd: str | None = None,
) -> int:
    async with session_factory() as db:
        project = Project(
            name=f"artifact-project-{root.name}-{created_by}",
            local_path=str(root),
            status="ready",
        )
        db.add(project)
        await db.flush()
        task = Task(
            title="artifact task",
            description="create an artifact",
            created_by=created_by,
            project_id=project.id,
            target_repo=str(root),
            last_cwd=last_cwd,
        )
        db.add(task)
        await db.commit()
        return task.id


@pytest.mark.asyncio
async def test_downloads_unicode_relative_to_last_cwd(
    artifact_client,
    tmp_path,
):
    client, session_factory = artifact_client
    report_dir = tmp_path / "输出"
    report_dir.mkdir()
    report = report_dir / "汇报稿.md"
    report.write_text("完整汇报内容", encoding="utf-8")
    task_id = await _create_local_task(
        session_factory,
        tmp_path,
        last_cwd=str(report_dir),
    )

    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "%E6%B1%87%E6%8A%A5%E7%A8%BF.md#result"},
    )

    assert response.status_code == 200
    assert response.content == report.read_bytes()
    assert response.headers["content-type"] == "application/octet-stream"
    assert "attachment" in response.headers["content-disposition"]
    assert "%E6%B1%87%E6%8A%A5%E7%A8%BF.md" in response.headers["content-disposition"]


@pytest.mark.asyncio
async def test_supports_container_workspace_links(
    artifact_client,
    tmp_path,
):
    client, session_factory = artifact_client
    artifact = tmp_path / "dist" / "report.pdf"
    artifact.parent.mkdir()
    artifact.write_bytes(b"%PDF-test")
    task_id = await _create_local_task(
        session_factory,
        tmp_path,
        last_cwd="/workspace/dist",
    )

    relative = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "report.pdf"},
    )
    absolute = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "/workspace/dist/report.pdf"},
    )
    host_absolute = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": str(artifact)},
    )

    assert relative.status_code == 200
    assert relative.content == b"%PDF-test"
    assert absolute.status_code == 200
    assert absolute.content == b"%PDF-test"
    assert host_absolute.status_code == 200
    assert host_absolute.content == b"%PDF-test"


@pytest.mark.asyncio
async def test_relative_parent_segments_can_remain_inside_workspace(
    artifact_client,
    tmp_path,
):
    client, session_factory = artifact_client
    drafts = tmp_path / "reports" / "drafts"
    final = tmp_path / "reports" / "final"
    drafts.mkdir(parents=True)
    final.mkdir()
    artifact = final / "report.md"
    artifact.write_text("final report", encoding="utf-8")
    task_id = await _create_local_task(
        session_factory,
        tmp_path,
        last_cwd=str(drafts),
    )

    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "../final/report.md"},
    )

    assert response.status_code == 200
    assert response.content == b"final report"


@pytest.mark.asyncio
async def test_rejects_external_links_traversal_and_outside_symlinks(
    artifact_client,
    tmp_path,
):
    client, session_factory = artifact_client
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "private.txt"
    outside.write_text("private", encoding="utf-8")
    (workspace / "escape.txt").symlink_to(outside)
    task_id = await _create_local_task(session_factory, workspace)
    endpoint = f"/api/tasks/{task_id}/artifacts/download"

    external = await client.get(endpoint, params={"path": "https://example.com/file"})
    traversal = await client.get(endpoint, params={"path": "../private.txt"})
    symlink = await client.get(endpoint, params={"path": "escape.txt"})

    assert external.status_code == 400
    assert traversal.status_code == 403
    assert symlink.status_code == 403


@pytest.mark.asyncio
async def test_rejects_symlinked_workspace_root(
    artifact_client,
    tmp_path,
):
    client, session_factory = artifact_client
    actual_workspace = tmp_path / "actual-workspace"
    actual_workspace.mkdir()
    (actual_workspace / "report.md").write_text(
        "outside-by-alias",
        encoding="utf-8",
    )
    workspace_alias = tmp_path / "workspace-alias"
    workspace_alias.symlink_to(actual_workspace, target_is_directory=True)
    task_id = await _create_local_task(session_factory, workspace_alias)

    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "report.md"},
    )

    assert response.status_code == 403
    assert b"outside-by-alias" not in response.content


@pytest.mark.asyncio
async def test_rejects_workspace_parent_symlink_swap_during_root_open(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    workspace_parent = tmp_path / "artifact-anchor-parent"
    workspace = workspace_parent / "workspace"
    workspace.mkdir(parents=True)
    (workspace / "report.md").write_text("safe report", encoding="utf-8")
    outside_parent = tmp_path / "outside-parent"
    outside_workspace = outside_parent / "workspace"
    outside_workspace.mkdir(parents=True)
    (outside_workspace / "report.md").write_text(
        "outside secret",
        encoding="utf-8",
    )
    saved_parent = tmp_path / "artifact-anchor-parent-safe"
    task_id = await _create_local_task(session_factory, workspace)

    original_open = task_artifacts._open_directory_component
    attacked = False

    def swap_parent_while_opening(parent_fd, component):
        nonlocal attacked
        if component != workspace_parent.name or attacked:
            return original_open(parent_fd, component)
        attacked = True
        workspace_parent.rename(saved_parent)
        workspace_parent.symlink_to(outside_parent, target_is_directory=True)
        try:
            return original_open(parent_fd, component)
        finally:
            workspace_parent.unlink()
            saved_parent.rename(workspace_parent)

    monkeypatch.setattr(
        task_artifacts,
        "_open_directory_component",
        swap_parent_while_opening,
    )
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "report.md"},
    )

    assert attacked is True
    assert response.status_code == 403
    assert b"outside secret" not in response.content


@pytest.mark.asyncio
async def test_workspace_parent_fd_stays_anchored_after_path_swap(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    workspace_parent = tmp_path / "artifact-open-parent"
    workspace = workspace_parent / "workspace"
    workspace.mkdir(parents=True)
    (workspace / "report.md").write_text("safe report", encoding="utf-8")
    outside_parent = tmp_path / "outside-open-parent"
    outside_workspace = outside_parent / "workspace"
    outside_workspace.mkdir(parents=True)
    (outside_workspace / "report.md").write_text(
        "outside secret",
        encoding="utf-8",
    )
    saved_parent = tmp_path / "artifact-open-parent-safe"
    task_id = await _create_local_task(session_factory, workspace)

    original_open = task_artifacts._open_directory_component
    attacked = False

    def swap_parent_after_open(parent_fd, component):
        nonlocal attacked
        opened_fd = original_open(parent_fd, component)
        if component == workspace_parent.name and not attacked:
            attacked = True
            workspace_parent.rename(saved_parent)
            workspace_parent.symlink_to(
                outside_parent,
                target_is_directory=True,
            )
        return opened_fd

    monkeypatch.setattr(
        task_artifacts,
        "_open_directory_component",
        swap_parent_after_open,
    )
    try:
        response = await client.get(
            f"/api/tasks/{task_id}/artifacts/download",
            params={"path": "report.md"},
        )
    finally:
        if workspace_parent.is_symlink():
            workspace_parent.unlink()
        if saved_parent.exists():
            saved_parent.rename(workspace_parent)

    assert attacked is True
    assert response.status_code == 200
    assert response.content == b"safe report"
    assert b"outside secret" not in response.content


@pytest.mark.asyncio
async def test_rejects_final_file_swap_after_resolution(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    artifact = workspace / "report.md"
    artifact.write_text("safe report", encoding="utf-8")
    outside = tmp_path / "private.txt"
    outside.write_text("outside secret", encoding="utf-8")
    task_id = await _create_local_task(session_factory, workspace)

    original_resolver = task_artifacts._lexical_artifact_parts

    def resolve_then_swap(task, root, reference):
        resolved = original_resolver(task, root, reference)
        artifact.unlink()
        artifact.symlink_to(outside)
        return resolved

    monkeypatch.setattr(
        task_artifacts,
        "_lexical_artifact_parts",
        resolve_then_swap,
    )
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "report.md"},
    )

    assert response.status_code == 403
    assert b"outside secret" not in response.content


@pytest.mark.asyncio
async def test_rejects_intermediate_directory_swap_after_resolution(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    workspace = tmp_path / "workspace"
    reports = workspace / "reports"
    reports.mkdir(parents=True)
    (reports / "report.md").write_text("safe report", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "report.md").write_text("outside secret", encoding="utf-8")
    task_id = await _create_local_task(session_factory, workspace)

    original_resolver = task_artifacts._lexical_artifact_parts

    def resolve_then_swap(task, root, reference):
        resolved = original_resolver(task, root, reference)
        reports.rename(workspace / "reports-original")
        reports.symlink_to(outside, target_is_directory=True)
        return resolved

    monkeypatch.setattr(
        task_artifacts,
        "_lexical_artifact_parts",
        resolve_then_swap,
    )
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "reports/report.md"},
    )

    assert response.status_code == 403
    assert b"outside secret" not in response.content


@pytest.mark.asyncio
async def test_enforces_artifact_size_limit(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    artifact = tmp_path / "large.bin"
    artifact.write_bytes(b"12345")
    task_id = await _create_local_task(session_factory, tmp_path)
    from backend.api import task_artifacts

    monkeypatch.setattr(task_artifacts, "MAX_ARTIFACT_DOWNLOAD_SIZE", 4)
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "large.bin"},
    )

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_caps_stream_when_file_grows_after_descriptor_validation(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    artifact = tmp_path / "growing.bin"
    artifact.write_bytes(b"safe")
    task_id = await _create_local_task(session_factory, tmp_path)
    monkeypatch.setattr(task_artifacts, "MAX_ARTIFACT_DOWNLOAD_SIZE", 4)

    original_response = task_artifacts._artifact_response

    def grow_then_respond(opened):
        with artifact.open("ab") as handle:
            handle.write(b"outside-limit")
        return original_response(opened)

    monkeypatch.setattr(
        task_artifacts,
        "_artifact_response",
        grow_then_respond,
    )
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "growing.bin"},
    )

    assert response.status_code == 200
    assert "content-length" not in response.headers
    assert response.content == b"safe"
    assert b"outside-limit" not in response.content


@pytest.mark.asyncio
async def test_stream_remains_valid_when_file_shrinks_after_validation(
    artifact_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = artifact_client
    artifact = tmp_path / "shrinking.bin"
    artifact.write_bytes(b"original")
    task_id = await _create_local_task(session_factory, tmp_path)

    original_response = task_artifacts._artifact_response

    def shrink_then_respond(opened):
        artifact.write_bytes(b"new")
        return original_response(opened)

    monkeypatch.setattr(
        task_artifacts,
        "_artifact_response",
        shrink_then_respond,
    )
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "shrinking.bin"},
    )

    assert response.status_code == 200
    assert "content-length" not in response.headers
    assert response.content == b"new"


@pytest.mark.asyncio
async def test_task_acl_applies_to_artifact_download(
    artifact_client,
    tmp_path,
):
    client, session_factory = artifact_client
    owner_id = 17
    artifact = tmp_path / "result.txt"
    artifact.write_text("owner result", encoding="utf-8")
    task_id = await _create_local_task(
        session_factory,
        tmp_path,
        created_by=owner_id,
    )
    endpoint = f"/api/tasks/{task_id}/artifacts/download"

    owner = await client.get(
        endpoint,
        params={"path": "result.txt"},
        headers={"X-Test-User": str(owner_id), "X-Test-Role": "member"},
    )
    outsider = await client.get(
        endpoint,
        params={"path": "result.txt"},
        headers={"X-Test-User": "18", "X-Test-Role": "member"},
    )

    assert owner.status_code == 200
    assert owner.content == b"owner result"
    assert outsider.status_code == 403


@pytest.mark.asyncio
async def test_manager_delegates_worker_artifacts(
    artifact_client,
    monkeypatch,
):
    client, session_factory = artifact_client
    async with session_factory() as db:
        task = Task(
            title="remote artifact",
            description="remote",
            worker_id=7,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    calls: list[tuple[int, str]] = []

    class FakeWorkerProxy:
        async def stream_task_artifact(self, task, artifact_path):
            calls.append((task.id, artifact_path))
            return Response(
                b"remote file",
                media_type="application/octet-stream",
                headers={"Content-Disposition": 'attachment; filename="remote.txt"'},
            )

    monkeypatch.setattr(
        task_artifacts,
        "_get_worker_proxy",
        lambda: FakeWorkerProxy(),
    )
    response = await client.get(
        f"/api/tasks/{task_id}/artifacts/download",
        params={"path": "remote.txt"},
    )

    assert response.status_code == 200
    assert response.content == b"remote file"
    assert calls == [(task_id, "remote.txt")]


@pytest.mark.asyncio
async def test_worker_proxy_streams_content_and_download_headers(monkeypatch):
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=b"worker bytes",
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": "12",
                "Content-Disposition": 'attachment; filename="worker.txt"',
            },
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def client_factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_async_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    proxy = WorkerProxy(db_factory=None, relay=None)
    worker = SimpleNamespace(
        id=7,
        name="worker-seven",
        status="ready",
        private_ip="worker.internal",
        ccm_port=8000,
        auth_token="internal-token",
    )

    async def ready_worker(_worker_id):
        return worker

    monkeypatch.setattr(proxy, "require_ready_worker", ready_worker)
    task = SimpleNamespace(id=91, worker_id=7)
    response = await proxy.stream_task_artifact(task, "输出/worker.txt")
    body = b"".join([chunk async for chunk in response.body_iterator])

    assert body == b"worker bytes"
    assert response.headers["content-disposition"] == 'attachment; filename="worker.txt"'
    assert captured[0].headers["authorization"] == "Bearer internal-token"
    assert captured[0].url.params["path"] == "输出/worker.txt"
