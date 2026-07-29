"""Tests for image upload API endpoints."""
import io
import pytest

# ── helpers ────────────────────────────────────────────────────────────────

def _png_bytes(size: int = 64) -> bytes:
    """Return a minimal valid 1×1 PNG (89 bytes) repeated to reach ~size bytes."""
    # Minimal 1×1 white pixel PNG
    return (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00\x90wS\xde"
        b"\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd8N"
        b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )


def _file_tuple(name: str, data: bytes, content_type: str = "image/png"):
    return (name, io.BytesIO(data), content_type)


# ── live-injection attachment validation ────────────────────────────────────

def test_validate_upload_attachment_uses_server_authoritative_metadata(
    tmp_path,
    monkeypatch,
):
    import backend.api.uploads as uploads_mod

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    uploaded = tmp_path / "11111111-1111-4111-8111-111111111111.png"
    uploaded.write_bytes(_png_bytes())

    result = uploads_mod.validate_upload_attachments(
        file_paths=[str(uploaded)],
        image_paths=[str(uploaded)],
        attachments=[{
            "url": (
                "/api/uploads/"
                "11111111-1111-4111-8111-111111111111.png"
            ),
            "name": "original-name.png",
            "is_image": True,
        }],
    )

    assert result == [uploads_mod.ValidatedUploadAttachment(
        path=str(uploaded),
        url=(
            "/api/uploads/"
            "11111111-1111-4111-8111-111111111111.png"
        ),
        name="original-name.png",
        is_image=True,
    )]


def test_validate_upload_attachment_rejects_symlink(
    tmp_path,
    monkeypatch,
):
    import backend.api.uploads as uploads_mod

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    target = tmp_path / "target.txt"
    target.write_text("secret", encoding="utf-8")
    link = tmp_path / "22222222-2222-4222-8222-222222222222.txt"
    link.symlink_to(target)

    with pytest.raises(
        uploads_mod.UploadAttachmentValidationError,
        match="missing or unsafe",
    ):
        uploads_mod.validate_upload_attachments(
            file_paths=[str(link)],
        )


def test_validate_upload_attachment_rejects_forged_metadata(
    tmp_path,
    monkeypatch,
):
    import backend.api.uploads as uploads_mod

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    uploaded = tmp_path / "33333333-3333-4333-8333-333333333333.txt"
    uploaded.write_text("notes", encoding="utf-8")

    with pytest.raises(
        uploads_mod.UploadAttachmentValidationError,
        match="does not match",
    ):
        uploads_mod.validate_upload_attachments(
            file_paths=[str(uploaded)],
            attachments=[{
                "url": "/api/uploads/different.txt",
                "name": "../escape.txt",
                "is_image": True,
            }],
        )


def test_validate_upload_attachment_refreshes_cleanup_ttl(
    tmp_path,
    monkeypatch,
):
    import os
    import time
    import backend.api.uploads as uploads_mod

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    uploaded = tmp_path / "55555555-5555-4555-8555-555555555555.txt"
    uploaded.write_text("old fork attachment", encoding="utf-8")
    expired = time.time() - 16 * 86400
    os.utime(uploaded, (expired, expired))

    uploads_mod.validate_upload_attachments(
        file_paths=[str(uploaded)],
    )

    assert uploads_mod.cleanup_expired_uploads() == 0
    assert uploaded.exists()


# ── upload endpoint ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upload_single_image(client, tmp_path, monkeypatch):
    """Upload a single valid PNG → 200, returns id/filename/path/url."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    data = _png_bytes()
    resp = await client.post(
        "/api/uploads",
        files=[("files", _file_tuple("test.png", data))],
    )
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) == 1
    r = results[0]
    assert r["filename"] == "test.png"
    assert r["path"].endswith(".png")
    assert r["url"].startswith("/api/uploads/")
    assert "id" in r


@pytest.mark.asyncio
async def test_upload_multiple_images(client, tmp_path, monkeypatch):
    """Upload 3 images at once."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    data = _png_bytes()
    resp = await client.post(
        "/api/uploads",
        files=[
            ("files", _file_tuple("a.png", data)),
            ("files", _file_tuple("b.png", data)),
            ("files", _file_tuple("c.png", data)),
        ],
    )
    assert resp.status_code == 200
    assert len(resp.json()) == 3


@pytest.mark.asyncio
async def test_upload_too_many_files(client, tmp_path, monkeypatch):
    """Uploading more than _MAX_FILES files returns 400."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    data = _png_bytes()
    count = uploads_mod._MAX_FILES + 1
    resp = await client.post(
        "/api/uploads",
        files=[("files", _file_tuple(f"img{i}.png", data)) for i in range(count)],
    )
    assert resp.status_code == 400
    assert "maximum" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_upload_invalid_type(client, tmp_path, monkeypatch):
    """Uploading a non-image file type returns 400."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    resp = await client.post(
        "/api/uploads",
        files=[("files", ("malware.exe", io.BytesIO(b"MZ..."), "application/octet-stream"))],
    )
    assert resp.status_code == 400
    assert "not allowed" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_upload_rejects_control_characters_in_filename(
    client,
    tmp_path,
    monkeypatch,
):
    import backend.api.uploads as uploads_mod

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    boundary = "ccm-upload-boundary"
    body = (
        f"--{boundary}\r\n"
        "Content-Disposition: form-data; name=\"files\"; "
        "filename*=UTF-8''proof.%0Awhoami%0A\r\n"
        "Content-Type: application/octet-stream\r\n"
        "\r\n"
        "payload\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    response = await client.post(
        "/api/uploads",
        content=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
    )

    assert response.status_code == 400
    assert "filename" in response.json()["detail"].lower()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_upload_discards_shell_unsafe_extension_from_saved_path(
    client,
    tmp_path,
    monkeypatch,
):
    import backend.api.uploads as uploads_mod

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    response = await client.post(
        "/api/uploads",
        files=[(
            "files",
            (
                "proof.$(touch injected)",
                io.BytesIO(b"payload"),
                "application/octet-stream",
            ),
        )],
    )

    assert response.status_code == 200
    saved = response.json()[0]
    assert uploads_mod.is_managed_upload_basename(
        saved["path"].rsplit("/", 1)[-1],
    )
    assert saved["path"].rsplit("/", 1)[-1] == saved["id"]


@pytest.mark.asyncio
async def test_multi_upload_validation_is_atomic(client, tmp_path, monkeypatch):
    """A later invalid file must not leave an earlier file on disk."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    resp = await client.post(
        "/api/uploads",
        files=[
            ("files", _file_tuple("kept.txt", b"must-not-remain", "text/plain")),
            (
                "files",
                ("blocked.exe", io.BytesIO(b"MZ..."), "application/octet-stream"),
            ),
        ],
    )

    assert resp.status_code == 400
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_multi_upload_total_limit_is_atomic(client, tmp_path, monkeypatch):
    """The request-wide cap bounds memory and leaves no promoted prefix."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(uploads_mod, "_MAX_TOTAL_SIZE_BYTES", 5)

    resp = await client.post(
        "/api/uploads",
        files=[
            ("files", _file_tuple("first.txt", b"abc", "text/plain")),
            ("files", _file_tuple("second.txt", b"def", "text/plain")),
        ],
    )

    assert resp.status_code == 400
    assert "combined" in resp.json()["detail"].lower()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_upload_saves_file_to_disk(client, tmp_path, monkeypatch):
    """Uploaded file actually exists on disk after upload."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    data = _png_bytes()
    resp = await client.post(
        "/api/uploads",
        files=[("files", _file_tuple("disk_check.png", data))],
    )
    assert resp.status_code == 200
    path = resp.json()[0]["path"]
    from pathlib import Path
    assert Path(path).exists()
    assert Path(path).read_bytes() == data


@pytest.mark.asyncio
async def test_upload_unique_ids(client, tmp_path, monkeypatch):
    """Each upload gets a unique id (no collision)."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    data = _png_bytes()
    r1 = (await client.post("/api/uploads", files=[("files", _file_tuple("x.png", data))])).json()
    r2 = (await client.post("/api/uploads", files=[("files", _file_tuple("x.png", data))])).json()
    assert r1[0]["id"] != r2[0]["id"]
    assert r1[0]["path"] != r2[0]["path"]


# ── serve uploaded image ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_uploaded_image(client, tmp_path, monkeypatch):
    """GET /api/uploads/{filename} serves the file that was uploaded."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    data = _png_bytes()
    upload_resp = await client.post(
        "/api/uploads",
        files=[("files", _file_tuple("serve_me.png", data))],
    )
    assert upload_resp.status_code == 200
    url = upload_resp.json()[0]["url"]  # e.g. /api/uploads/uuid.png
    filename = url.split("/")[-1]

    serve_resp = await client.get(f"/api/uploads/{filename}")
    assert serve_resp.status_code == 200
    assert serve_resp.content == data


@pytest.mark.asyncio
async def test_get_nonexistent_image(client, tmp_path, monkeypatch):
    """GET /api/uploads/nonexistent.png returns 404."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    resp = await client.get("/api/uploads/does_not_exist.png")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_path_traversal_rejected(client, tmp_path, monkeypatch):
    """Filenames with '..' components are rejected with 4xx."""
    import backend.api.uploads as uploads_mod
    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)

    # Direct call to the endpoint handler — httpx normalizes encoded slashes
    # before they reach FastAPI, so we test the handler's own guard directly.
    from backend.api.uploads import get_file
    from fastapi import HTTPException as _HTTPException
    with pytest.raises(_HTTPException) as exc_info:
        await get_file("../secret.txt")
    assert exc_info.value.status_code in (400, 404)


@pytest.mark.asyncio
async def test_get_sibling_prefix_path_and_symlink_are_rejected(
    tmp_path,
    monkeypatch,
):
    import backend.api.uploads as uploads_mod
    from fastapi import HTTPException as _HTTPException

    monkeypatch.setattr(uploads_mod, "UPLOAD_DIR", tmp_path)
    sibling = tmp_path.parent / f"{tmp_path.name}-sibling"
    sibling.mkdir()
    secret = sibling / "secret.txt"
    secret.write_text("outside")

    with pytest.raises(_HTTPException) as traversal_error:
        await uploads_mod.get_file(f"../{sibling.name}/secret.txt")
    assert traversal_error.value.status_code == 400

    link = tmp_path / "link.txt"
    link.symlink_to(secret)
    with pytest.raises(_HTTPException) as symlink_error:
        await uploads_mod.get_file(link.name)
    assert symlink_error.value.status_code == 400
