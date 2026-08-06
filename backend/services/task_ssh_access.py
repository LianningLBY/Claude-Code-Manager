from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.schemas.task_ssh_grant import TaskSSHGrantInput
from backend.services.skill_context import is_worker_managed_task_metadata


class TaskSSHAccessError(ValueError):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class PreparedTaskSSHGrant:
    profile_id: int
    profile_revision: int
    capabilities: tuple[str, ...]


def task_ssh_scope_invalid_reason(
    *,
    worker_id: int | None,
    shared_from_id: int | None,
    metadata: Mapping[str, Any] | None,
) -> str | None:
    if worker_id is not None:
        return "task_on_worker"
    if shared_from_id is not None:
        return "task_shared"
    if is_worker_managed_task_metadata(metadata):
        return "task_worker_managed"
    return None


async def prepare_task_ssh_grants(
    db: AsyncSession,
    inputs: Iterable[TaskSSHGrantInput | dict],
    *,
    worker_id: int | None,
    shared_from_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> list[PreparedTaskSSHGrant]:
    parsed = [
        value if isinstance(value, TaskSSHGrantInput) else TaskSSHGrantInput.model_validate(value)
        for value in inputs
    ]
    if not parsed:
        return []
    if task_ssh_scope_invalid_reason(
        worker_id=worker_id,
        shared_from_id=shared_from_id,
        metadata=metadata,
    ) is not None:
        raise TaskSSHAccessError(
            409,
            "Managed SSH grants are available only to local, unshared Manager Tasks",
        )
    profile_ids = [value.profile_id for value in parsed]
    if len(profile_ids) != len(set(profile_ids)):
        raise TaskSSHAccessError(422, "Each SSH profile may be granted only once")
    profiles = list((await db.execute(
        select(SSHProfile).where(SSHProfile.id.in_(profile_ids))
    )).scalars().all())
    by_id = {profile.id: profile for profile in profiles}
    prepared: list[PreparedTaskSSHGrant] = []
    for value in parsed:
        profile = by_id.get(value.profile_id)
        if profile is None or profile.deleted_at is not None:
            raise TaskSSHAccessError(404, f"SSH profile {value.profile_id} not found")
        if not profile.enabled:
            raise TaskSSHAccessError(409, f"SSH profile {value.profile_id} is disabled")
        prepared.append(PreparedTaskSSHGrant(
            profile_id=profile.id,
            profile_revision=profile.revision,
            capabilities=tuple(value.capabilities),
        ))
    return prepared


def task_ssh_grant_rows(
    task_id: int,
    prepared: Iterable[PreparedTaskSSHGrant],
    *,
    created_by: int | None,
) -> list[TaskSSHGrant]:
    return [
        TaskSSHGrant(
            task_id=task_id,
            ssh_profile_id=value.profile_id,
            profile_revision=value.profile_revision,
            capabilities=list(value.capabilities),
            created_by=created_by,
        )
        for value in prepared
    ]


def _invalid_reason(task: Task, grant: TaskSSHGrant, profile: SSHProfile) -> str | None:
    scope_reason = task_ssh_scope_invalid_reason(
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    )
    if scope_reason is not None:
        return scope_reason
    if profile.deleted_at is not None:
        return "profile_deleted"
    if not profile.enabled:
        return "profile_disabled"
    if profile.revision != grant.profile_revision:
        return "profile_revision_changed"
    return None


async def task_ssh_grant_snapshots(
    db: AsyncSession,
    task: Task,
) -> list[dict]:
    rows = (await db.execute(
        select(TaskSSHGrant, SSHProfile)
        .join(SSHProfile, SSHProfile.id == TaskSSHGrant.ssh_profile_id)
        .where(TaskSSHGrant.task_id == task.id)
        .order_by(SSHProfile.name.asc(), TaskSSHGrant.id.asc())
    )).all()
    snapshots = []
    for grant, profile in rows:
        invalid_reason = _invalid_reason(task, grant, profile)
        snapshots.append({
            "id": grant.id,
            "task_id": grant.task_id,
            "profile_id": profile.id,
            "profile_name": profile.name,
            "host": profile.host,
            "port": profile.port,
            "username": profile.username,
            "host_key_fingerprint": profile.host_key_fingerprint,
            "profile_revision": grant.profile_revision,
            "current_profile_revision": profile.revision,
            "capabilities": grant.capabilities,
            "valid": invalid_reason is None,
            "invalid_reason": invalid_reason,
            "created_by": grant.created_by,
            "created_at": grant.created_at,
            "updated_at": grant.updated_at,
        })
    return snapshots


async def replace_task_ssh_grants(
    db: AsyncSession,
    task: Task,
    inputs: Iterable[TaskSSHGrantInput | dict],
    *,
    created_by: int | None,
) -> list[dict]:
    task_id = task.id
    prepared = await prepare_task_ssh_grants(
        db,
        inputs,
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    )
    await db.execute(
        delete(TaskSSHGrant).where(TaskSSHGrant.task_id == task_id)
    )
    db.add_all(task_ssh_grant_rows(
        task_id,
        prepared,
        created_by=created_by,
    ))
    await db.commit()
    db.expire_all()
    refreshed = await db.get(Task, task_id)
    if refreshed is None:
        raise TaskSSHAccessError(409, "Task disappeared while saving SSH grants")
    return await task_ssh_grant_snapshots(db, refreshed)


async def resolve_task_ssh_profile(
    db: AsyncSession,
    *,
    task_id: int,
    profile_id: int,
    required_capability: str,
) -> SSHProfile:
    task = await db.get(Task, task_id)
    if task is None:
        raise TaskSSHAccessError(404, "Task not found")
    scope_reason = task_ssh_scope_invalid_reason(
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    )
    if scope_reason is not None:
        raise TaskSSHAccessError(
            409,
            f"Task SSH access is not available in this scope: {scope_reason}",
        )
    row = (await db.execute(
        select(TaskSSHGrant, SSHProfile)
        .join(SSHProfile, SSHProfile.id == TaskSSHGrant.ssh_profile_id)
        .where(
            TaskSSHGrant.task_id == task_id,
            TaskSSHGrant.ssh_profile_id == profile_id,
        )
    )).one_or_none()
    if row is None:
        raise TaskSSHAccessError(403, "SSH profile is not granted to this Task")
    grant, profile = row
    if required_capability not in (grant.capabilities or []):
        raise TaskSSHAccessError(
            403,
            f"Task SSH grant does not allow {required_capability}",
        )
    invalid_reason = _invalid_reason(task, grant, profile)
    if invalid_reason is not None:
        raise TaskSSHAccessError(
            409,
            f"Task SSH grant is no longer valid: {invalid_reason}",
        )
    return profile
