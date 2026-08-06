from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from backend.services.ssh_executor import (
    SSHProbeResult,
    derive_openssh_public_key,
)


def _private_key_file(tmp_path: Path) -> Path:
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = tmp_path / "managed-ssh-key"
    path.write_bytes(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    path.chmod(0o600)
    return path


@pytest.mark.asyncio
async def test_managed_profile_crud_masks_key_and_revisions_identity(
    client, tmp_path, monkeypatch,
):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)

    create = await client.post("/api/ssh-profiles", json={
        "name": "staging",
        "host": "ssh.staging.internal",
        "port": 2222,
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    })
    assert create.status_code == 201, create.text
    profile = create.json()
    assert profile["revision"] == 1
    assert profile["key_path_hint"] == "…/managed-ssh-key"
    assert "key_path" not in profile
    assert profile["public_key_fingerprint"].startswith("SHA256:")
    original_key_fingerprint = profile["public_key_fingerprint"]
    assert profile["host_key_fingerprint"].startswith("SHA256:")

    rename = await client.put(
        f"/api/ssh-profiles/{profile['id']}", json={"name": "staging-a"},
    )
    assert rename.status_code == 200
    assert rename.json()["revision"] == 1

    identity = await client.put(
        f"/api/ssh-profiles/{profile['id']}", json={"username": "release"},
    )
    assert identity.status_code == 200
    assert identity.json()["revision"] == 2
    assert identity.json()["last_test_ok"] is None

    replacement = ed25519.Ed25519PrivateKey.generate()
    key_path.write_bytes(replacement.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    key_path.chmod(0o600)
    rotated = await client.put(
        f"/api/ssh-profiles/{profile['id']}",
        json={"key_path": str(key_path)},
    )
    assert rotated.status_code == 200
    assert rotated.json()["revision"] == 3
    assert rotated.json()["public_key_fingerprint"] != original_key_fingerprint

    monkeypatch.setattr(
        "backend.services.ssh_profiles.SSHExecutor.probe",
        lambda *_args, **_kwargs: _async_result(SSHProbeResult(True)),
    )
    tested = await client.post(f"/api/ssh-profiles/{profile['id']}/test")
    assert tested.status_code == 200
    assert tested.json() == {"ok": True, "error_code": None, "detail": None}

    listing = await client.get("/api/ssh-profiles")
    assert listing.status_code == 200
    assert [item["name"] for item in listing.json()] == ["staging-a"]

    deleted = await client.delete(f"/api/ssh-profiles/{profile['id']}")
    assert deleted.status_code == 200
    assert (await client.get("/api/ssh-profiles")).json() == []
    assert (await client.get(f"/api/ssh-profiles/{profile['id']}")).status_code == 404


async def _async_result(value):
    return value


@pytest.mark.asyncio
async def test_profile_endpoint_change_requires_new_host_key(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    created = await client.post("/api/ssh-profiles", json={
        "name": "production",
        "host": "old.example.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    })
    profile_id = created.json()["id"]

    rejected = await client.put(
        f"/api/ssh-profiles/{profile_id}",
        json={"host": "new.example.internal"},
    )

    assert rejected.status_code == 400
    assert "newly confirmed host key" in rejected.text


@pytest.mark.asyncio
async def test_profile_rejects_unsafe_key_and_duplicate_name(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    payload = {
        "name": "duplicate",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    }
    assert (await client.post("/api/ssh-profiles", json=payload)).status_code == 201
    assert (await client.post("/api/ssh-profiles", json=payload)).status_code == 409

    key_path.chmod(0o644)
    unsafe = await client.post("/api/ssh-profiles", json={**payload, "name": "unsafe"})
    assert unsafe.status_code == 400
    assert unsafe.json()["detail"]["code"] == "key_permissions"


@pytest.mark.asyncio
async def test_probe_host_key_returns_confirmable_identity(client, monkeypatch):
    monkeypatch.setattr(
        "backend.api.ssh_profiles.probe_ssh_host_key",
        lambda host, *, port, timeout: type("HostKey", (), {
            "key_type": "ssh-ed25519",
            "openssh_public_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJNQCBTQso2itH2uBoMKDWX3zZZS0tI4WZJ1bnFmM8oQ",
            "sha256_fingerprint": "SHA256:test",
        })(),
    )

    response = await client.post("/api/ssh-profiles/probe-host-key", json={
        "host": "ssh.example.internal",
        "port": 2200,
    })

    assert response.status_code == 200
    assert response.json()["key_type"] == "ssh-ed25519"
    assert response.json()["fingerprint"] == "SHA256:test"
