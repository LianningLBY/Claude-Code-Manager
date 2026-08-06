import asyncio
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import get_current_user_id, require_admin
from backend.database import get_db
from backend.models.ssh_profile import SSHProfile
from backend.schemas.ssh_profile import (
    SSHHostKeyProbeRequest,
    SSHHostKeyProbeResponse,
    SSHProfileCreate,
    SSHProfileResponse,
    SSHProfileTestResponse,
    SSHProfileUpdate,
)
from backend.services.ssh_executor import SSHKeyPreflightError, probe_ssh_host_key
from backend.services.ssh_profiles import (
    CONNECTION_IDENTITY_FIELDS,
    test_profile,
    validated_profile_material,
)


router = APIRouter(
    prefix="/api/ssh-profiles",
    tags=["ssh-profiles"],
    dependencies=[Depends(require_admin)],
)


def _profile_error(exc: Exception) -> HTTPException:
    if isinstance(exc, SSHKeyPreflightError):
        return HTTPException(400, {"code": exc.code, "message": exc.detail})
    return HTTPException(400, str(exc))


async def _live_profile(db: AsyncSession, profile_id: int) -> SSHProfile:
    profile = await db.get(SSHProfile, profile_id)
    if profile is None or profile.deleted_at is not None:
        raise HTTPException(404, "SSH profile not found")
    return profile


@router.get("", response_model=list[SSHProfileResponse])
async def list_ssh_profiles(
    include_disabled: bool = True,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(SSHProfile).where(SSHProfile.deleted_at.is_(None))
    if not include_disabled:
        stmt = stmt.where(SSHProfile.enabled.is_(True))
    result = await db.execute(stmt.order_by(SSHProfile.name.asc()))
    return list(result.scalars().all())


@router.post("/probe-host-key", response_model=SSHHostKeyProbeResponse)
async def probe_host_key(body: SSHHostKeyProbeRequest):
    try:
        info = await asyncio.to_thread(
            probe_ssh_host_key,
            body.host,
            port=body.port,
            timeout=body.timeout_seconds,
        )
    except Exception as exc:
        raise _profile_error(exc) from exc
    return SSHHostKeyProbeResponse(
        key_type=info.key_type,
        host_key_value=info.openssh_public_key,
        fingerprint=info.sha256_fingerprint,
    )


@router.post("", response_model=SSHProfileResponse, status_code=201)
async def create_ssh_profile(
    body: SSHProfileCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        material = validated_profile_material(
            key_path=body.key_path,
            host_key_value=body.host_key_value,
        )
    except Exception as exc:
        raise _profile_error(exc) from exc
    profile = SSHProfile(
        name=body.name,
        host=body.host,
        port=body.port,
        username=body.username,
        enabled=body.enabled,
        created_by=get_current_user_id(request),
        **material,
    )
    db.add(profile)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "SSH profile name already exists") from exc
    await db.refresh(profile)
    return profile


@router.get("/{profile_id}", response_model=SSHProfileResponse)
async def get_ssh_profile(
    profile_id: int, db: AsyncSession = Depends(get_db),
):
    return await _live_profile(db, profile_id)


@router.put("/{profile_id}", response_model=SSHProfileResponse)
async def update_ssh_profile(
    profile_id: int,
    body: SSHProfileUpdate,
    db: AsyncSession = Depends(get_db),
):
    profile = await _live_profile(db, profile_id)
    values = body.model_dump(exclude_unset=True)
    if values.get("key_path") is None:
        values.pop("key_path", None)
    if (
        any(field in values and values[field] != getattr(profile, field) for field in ("host", "port"))
        and "host_key_value" not in values
    ):
        raise HTTPException(
            400,
            "Changing the SSH endpoint requires a newly confirmed host key",
        )
    # An explicitly re-entered key or host key is a re-authorization request,
    # even when its path/value string is unchanged. The file at a stable path
    # may have been rotated outside CCM and must receive a new revision.
    identity_changed = bool({"key_path", "host_key_value"} & values.keys()) or any(
        field in values and values[field] != getattr(profile, field)
        for field in CONNECTION_IDENTITY_FIELDS - {"key_path", "host_key_value"}
    )
    if identity_changed:
        try:
            material = validated_profile_material(
                key_path=values.get("key_path", profile.key_path),
                host_key_value=values.get(
                    "host_key_value", profile.host_key_value,
                ),
            )
        except Exception as exc:
            raise _profile_error(exc) from exc
        values.update(material)
        profile.revision += 1
        profile.last_tested_at = None
        profile.last_test_ok = None
        profile.last_error_code = None
        profile.last_error_detail = None
    for field, value in values.items():
        setattr(profile, field, value)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "SSH profile name already exists") from exc
    await db.refresh(profile)
    return profile


@router.post("/{profile_id}/test", response_model=SSHProfileTestResponse)
async def test_ssh_profile(
    profile_id: int, db: AsyncSession = Depends(get_db),
):
    profile = await _live_profile(db, profile_id)
    try:
        result = await test_profile(profile)
    except Exception as exc:
        result_error = _profile_error(exc)
        profile.last_tested_at = datetime.utcnow()
        profile.last_test_ok = False
        detail = result_error.detail
        profile.last_error_code = (
            detail.get("code") if isinstance(detail, dict) else "connection_failed"
        )
        profile.last_error_detail = (
            detail.get("message") if isinstance(detail, dict) else str(detail)
        )[:500]
        await db.commit()
        return SSHProfileTestResponse(
            ok=False,
            error_code=profile.last_error_code,
            detail=profile.last_error_detail,
        )
    await db.commit()
    return SSHProfileTestResponse(
        ok=result.ok,
        error_code=result.error_code,
        detail=result.detail,
    )


@router.delete("/{profile_id}")
async def delete_ssh_profile(
    profile_id: int, db: AsyncSession = Depends(get_db),
):
    profile = await _live_profile(db, profile_id)
    profile.enabled = False
    profile.revision += 1
    profile.deleted_at = datetime.utcnow()
    await db.commit()
    return {"ok": True}
