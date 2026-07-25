"""Regression tests for temporary files created by the file API."""

import tempfile
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

import backend.api.files as files_module


async def test_ssh_download_removes_temporary_file_after_response(
    tmp_path,
    monkeypatch,
):
    payload = b"downloaded over ssh"
    created_paths: list[Path] = []
    real_named_temporary_file = tempfile.NamedTemporaryFile

    def isolated_named_temporary_file(*args, **kwargs):
        kwargs["dir"] = tmp_path
        temporary_file = real_named_temporary_file(*args, **kwargs)
        created_paths.append(Path(temporary_file.name))
        return temporary_file

    class FakeSFTP:
        closed = False

        def stat(self, remote_path):
            assert remote_path == "/remote/report.txt"
            return SimpleNamespace(st_size=len(payload))

        def getfo(self, remote_path, destination):
            assert remote_path == "/remote/report.txt"
            destination.write(payload)

        def close(self):
            self.closed = True

    class FakeSSHClient:
        closed = False

        def __init__(self):
            self.sftp = FakeSFTP()

        def open_sftp(self):
            return self.sftp

        def close(self):
            self.closed = True

    ssh_client = FakeSSHClient()
    monkeypatch.setattr(
        files_module.tempfile,
        "NamedTemporaryFile",
        isolated_named_temporary_file,
    )
    monkeypatch.setattr(
        files_module,
        "_make_ssh_client",
        lambda _credentials: ssh_client,
    )

    app = FastAPI()
    app.include_router(files_module.router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/files/ssh/download",
            json={
                "host": "worker.internal",
                "username": "ubuntu",
                "path": "/remote/report.txt",
            },
        )

    assert response.status_code == 200
    assert response.content == payload
    assert len(created_paths) == 1
    assert created_paths[0].name.startswith("ccm-ssh-download-")
    assert not created_paths[0].exists()
    assert ssh_client.sftp.closed is True
    assert ssh_client.closed is True
