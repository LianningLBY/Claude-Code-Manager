"""Manager→Worker 任务转发与操作代理（elastic-worker 设计 §5.3/§6.3/§6.4/§8）。

- forward_task_to_worker：确保 worker 有项目 → 先订阅 relay → 用 Manager 分配的
  同一 task ID 在 worker 上创建 task（ID 全局统一，见设计 §2）
- proxy_to_worker：通用操作代理（stop/cancel/retry/plan/monitor），转发前确保
  relay 已订阅（幂等；retry 场景 Manager 重启后 relay 未订阅，不补订阅则全丢）
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import stat
from dataclasses import dataclass, field
from datetime import datetime
from weakref import WeakKeyDictionary

import httpx
from fastapi import HTTPException
from sqlalchemy import select, update
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from backend.config import settings
from backend.models.project import Project
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.legacy_plan_execution import (
    LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION,
    LegacyPlanExecutionCarrierProof,
    parse_legacy_plan_execution_carrier_proof,
)
from backend.services.pr_review_runtime import (
    PR_REVIEW_SNAPSHOT_CONTEXT_VERSION,
    PR_REVIEW_TERMINAL_CHAT_HEADER,
    PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE,
    PR_REVIEW_TERMINAL_CHAT_VERSION,
    is_pr_review_task,
    is_pr_sandbox_task,
)
from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path
from backend.services.task_artifact_contract import (
    TASK_ARTIFACT_SCOPE_VERSION,
)
from backend.services.worker_relay import worker_task_generation
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)

logger = logging.getLogger(__name__)


_WORKER_DESTROY_CLAIM_SEAL = object()


@dataclass(frozen=True)
class WorkerDestroyLifecycleClaim:
    """Opaque authority for one already-claimed Worker destroy lifecycle.

    The public Worker proxy remains ready-only.  This token is created only
    from the row returned by the ``ready|stopped|error -> destroying`` CAS and
    lets the destroy coordinator perform the narrow stop/readback handshake
    while that exact Worker endpoint still owns the Task.
    """

    _seal: object = field(repr=False, compare=False)
    worker_id: int
    created_at: datetime | None
    updated_at: datetime | None
    cloud_instance_id: str | None
    private_ip: str | None
    ccm_port: int
    auth_token: str | None = field(repr=False)


def capture_worker_destroy_lifecycle_claim(
    worker: Worker,
) -> WorkerDestroyLifecycleClaim:
    """Freeze the stable identity behind one successful destroy CAS."""

    if worker.status != "destroying":
        raise ValueError("Worker destroy claim requires destroying status")
    return WorkerDestroyLifecycleClaim(
        _seal=_WORKER_DESTROY_CLAIM_SEAL,
        worker_id=worker.id,
        created_at=worker.created_at,
        updated_at=worker.updated_at,
        cloud_instance_id=worker.cloud_instance_id,
        private_ip=worker.private_ip,
        ccm_port=worker.ccm_port,
        auth_token=worker.auth_token,
    )


def _worker_destroy_lifecycle_predicates(
    claim: WorkerDestroyLifecycleClaim,
) -> tuple:
    """Return the durable CAS fence for one opaque in-process destroy claim."""

    if (
        not isinstance(claim, WorkerDestroyLifecycleClaim)
        or claim._seal is not _WORKER_DESTROY_CLAIM_SEAL
    ):
        raise ValueError("invalid Worker destroy lifecycle claim")
    return (
        Worker.id == claim.worker_id,
        Worker.status == "destroying",
        (
            Worker.created_at.is_(None)
            if claim.created_at is None
            else Worker.created_at == claim.created_at
        ),
        (
            Worker.updated_at.is_(None)
            if claim.updated_at is None
            else Worker.updated_at == claim.updated_at
        ),
        (
            Worker.cloud_instance_id.is_(None)
            if claim.cloud_instance_id is None
            else Worker.cloud_instance_id == claim.cloud_instance_id
        ),
        (
            Worker.private_ip.is_(None)
            if claim.private_ip is None
            else Worker.private_ip == claim.private_ip
        ),
        Worker.ccm_port == claim.ccm_port,
    )

# (worker_id, manager_project_id) -> Lock，防并发 task 重复建项目
_project_locks: dict[tuple[int, int], asyncio.Lock] = {}
_task_operation_locks: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = WeakKeyDictionary()


class WorkerEndpointNotFoundError(Exception):
    """A caller-requested signal that the Worker returned an exact HTTP 404."""


class WorkerTaskForwardOutcomeUncertainError(RuntimeError):
    """The initial create request may already have committed on the Worker.

    Retrying that POST without an idempotent remote receipt can create a
    second execution or make the Manager declare failure while the Worker is
    still running.  ``cancellation`` preserves an outer shutdown request after
    the Manager has durably quarantined the ambiguous claim.
    """

    def __init__(
        self,
        message: str,
        *,
        cancellation: asyncio.CancelledError | None = None,
    ) -> None:
        super().__init__(message)
        self.cancellation = cancellation


class WorkerTaskForwardAdmissionBlockedError(RuntimeError):
    """A durable termination receipt won before initial Worker creation."""


class WorkerTaskMutationOutcomeUncertainError(RuntimeError):
    """A Worker mutation may have committed without a readable response.

    Callers which opt into this contract must durably quarantine the exact
    Manager-side generation before releasing the per-Task operation lock.  In
    particular, blindly replaying a cancel/stop POST is unsafe: the first
    request may already have terminated the only remote execution.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        cancellation: asyncio.CancelledError | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.cancellation = cancellation


def get_task_operation_lock(task_id: int) -> asyncio.Lock:
    """Return the process-wide operation lock for one Task on this event loop.

    Task migration and every Manager→Worker mutation must use the same lock.
    Keeping the registry at module scope avoids two independently constructed
    service objects accidentally creating different locks.  The event-loop key
    keeps async test loops isolated and lets completed loops be collected.
    """

    loop = asyncio.get_running_loop()
    locks = _task_operation_locks.setdefault(loop, {})
    return locks.setdefault(task_id, asyncio.Lock())


class WorkerProxy:
    def __init__(self, db_factory, relay):
        self.db_factory = db_factory
        self.relay = relay

    def task_operation_lock(self, task_id: int) -> asyncio.Lock:
        """Serialize remote operations that can create/mutate one Worker task."""

        return get_task_operation_lock(task_id)

    @staticmethod
    def _api(worker: Worker, path: str) -> str:
        return f"http://{worker.private_ip}:{worker.ccm_port}{path}"

    @staticmethod
    def _headers(worker: Worker) -> dict:
        return {"Authorization": f"Bearer {worker.auth_token}"}

    @staticmethod
    def _ssh(worker: Worker) -> SSHExecutor:
        """Build every WorkerProxy SSH path with per-instance host trust."""
        return SSHExecutor(
            host=worker.private_ip,
            user=worker.ssh_user,
            key_path=worker.ssh_key_path or settings.worker_ssh_key_path,
            known_hosts_path=(
                worker_known_hosts_path(worker.cloud_instance_id)
                if worker.cloud_instance_id else None
            ),
        )

    async def get_worker(self, worker_id: int) -> Worker | None:
        async with self.db_factory() as db:
            return await db.get(Worker, worker_id)

    async def require_ready_worker(self, worker_id: int) -> Worker:
        worker = await self.get_worker(worker_id)
        if not worker:
            raise HTTPException(404, f"Worker {worker_id} 不存在")
        if worker.status != "ready":
            raise HTTPException(
                503,
                f"Worker {worker.name} 当前状态 {worker.status}，无法执行操作。"
                "请等待 Worker 恢复或将 task 切回本机执行。",
            )
        return worker

    async def _require_destroy_lifecycle_claim(
        self,
        claim: WorkerDestroyLifecycleClaim,
    ) -> Worker:
        """Resolve one opaque destroy claim without widening ready admission."""

        async with self.db_factory() as db:
            worker = (
                await db.execute(
                    select(Worker).where(
                        *_worker_destroy_lifecycle_predicates(claim)
                    )
                )
            ).scalar_one_or_none()
        # Keep the internal credential out of SQL parameters: driver errors are
        # routinely logged and may render bound values. ``updated_at`` already
        # fences every supported credential mutation; compare the token again
        # in memory before it can authorize a request.
        if worker is None or worker.auth_token != claim.auth_token:
            raise HTTPException(
                409,
                "Worker destroy lifecycle or endpoint identity changed; "
                "remote Task mutation was refused",
            )
        return worker

    async def _require_versioned_plan_protocol(self, worker: Worker) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("versioned_plan_worker_protocol") != 3
        ):
            raise RuntimeError(
                f"Worker {worker.name} does not support versioned Plan protocol 3"
            )

    async def _require_legacy_plan_execution_carrier_protocol(
        self,
        worker: Worker,
    ) -> None:
        """Require exact readback before trusting an existing Plan carrier."""

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("legacy_plan_execution_carrier_protocol")
            != LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
        ):
            raise RuntimeError(
                f"Worker {worker.name} does not support legacy Plan execution "
                f"carrier protocol "
                f"{LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION}"
            )

    async def get_legacy_plan_execution_carrier_proof(
        self,
        worker: Worker,
        task_id: int,
    ) -> LegacyPlanExecutionCarrierProof | None:
        """Read one existing Worker's semantic carrier proof, never create it."""

        if type(task_id) is not int or task_id <= 0:
            raise ValueError("legacy Plan carrier task_id must be positive")
        await self._require_legacy_plan_execution_carrier_protocol(worker)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/legacy-plan-execution-carrier-proof",
                ),
                headers=self._headers(worker),
            )
        if response.status_code in {404, 409}:
            # Both outcomes prove that the assigned Worker cannot supply the
            # exact migrated carrier.  Recovery must durably quarantine the
            # Manager mirror instead of retrying a permanent 409 forever.
            return None
        response.raise_for_status()
        try:
            proof = parse_legacy_plan_execution_carrier_proof(response.json())
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Worker {worker.name} returned an invalid legacy Plan "
                "execution carrier proof"
            ) from exc
        if proof.task_id != task_id:
            raise RuntimeError(
                f"Worker {worker.name} returned a legacy Plan carrier proof "
                "for another Task"
            )
        return proof

    async def get_plan_repo_revision(
        self,
        *,
        worker: Worker,
        manager_project_id: int | None,
        target_task_id: int | None,
    ) -> dict | None:
        """Read the execution node's repository fingerprint for staleness."""

        await self._require_versioned_plan_protocol(worker)
        worker_project_id = None
        if manager_project_id is not None:
            async with self.db_factory() as db:
                current = await db.get(Worker, worker.id)
                mapping = dict(current.project_mapping or {}) if current else {}
            worker_project_id = mapping.get(str(manager_project_id))
            if worker_project_id is None:
                raise RuntimeError("Worker Project mapping is missing")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(worker, "/api/plans/worker-repo-revision"),
                headers=self._headers(worker),
                json={
                    "project_id": worker_project_id,
                    "target_task_id": target_task_id,
                },
            )
            response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Worker returned an invalid repository receipt")
        revision = payload.get("repo_revision")
        if revision is not None and not isinstance(revision, dict):
            raise RuntimeError("Worker returned an invalid repository fingerprint")
        return revision

    async def get_plan_application_receipt(
        self, worker: Worker, receipt_key: str
    ) -> dict | None:
        await self._require_versioned_plan_protocol(worker)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/plans/worker-application-receipts/{receipt_key}",
                ),
                headers=self._headers(worker),
            )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("receipt_key") != receipt_key:
            raise RuntimeError("Worker returned an invalid Plan application receipt")
        return payload

    async def get_worker_turn_handoff_receipt(
        self,
        worker: Worker,
        task_id: int,
        handoff_id: str,
    ) -> dict | None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/worker-turn-handoffs/{handoff_id}",
                ),
                headers=self._headers(worker),
            )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("handoff_id") != handoff_id
            or payload.get("task_id") != task_id
        ):
            raise RuntimeError(
                "Worker returned an invalid turn handoff receipt"
            )
        return payload

    async def resume_worker_turn_handoff(
        self,
        worker: Worker,
        task_id: int,
        handoff_id: str,
    ) -> dict:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/worker-turn-handoffs/"
                    f"{handoff_id}/resume",
                ),
                headers=self._headers(worker),
            )
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("handoff_id") != handoff_id
            or payload.get("task_id") != task_id
        ):
            raise RuntimeError(
                "Worker returned an invalid turn handoff resume receipt"
            )
        return payload

    async def resolve_plan_application_receipt(
        self,
        worker: Worker,
        receipt_key: str,
        *,
        action: str,
        note: str,
    ) -> dict:
        await self._require_versioned_plan_protocol(worker)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(
                    worker,
                    f"/api/plans/worker-application-receipts/{receipt_key}/resolve",
                ),
                headers=self._headers(worker),
                json={"action": action, "note": note},
            )
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("receipt_key") != receipt_key
            or payload.get("action") != action
        ):
            raise RuntimeError(
                "Worker returned an invalid Plan delivery resolution"
            )
        return payload

    @staticmethod
    def _attachment_manifest(paths: list[str]) -> list[dict]:
        manifest = []
        for path in paths:
            absolute = os.path.abspath(path)
            if path != absolute:
                raise RuntimeError("Plan attachment path must be absolute")
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(absolute, flags)
            digest = hashlib.sha256()
            size = 0
            with os.fdopen(fd, "rb") as handle:
                metadata = os.fstat(handle.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise RuntimeError("Plan attachment must be a regular file")
                if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
                    raise RuntimeError("Plan attachment owner does not match CCM")
                while chunk := handle.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
            manifest.append({
                "path": absolute,
                "size": size,
                "sha256": digest.hexdigest(),
            })
        return manifest

    @staticmethod
    def _plan_attachment_payload(
        items: list[dict] | None,
    ) -> tuple[list[str], list[str], list[dict]]:
        rows = [item for item in (items or []) if isinstance(item, dict)]
        paths = [item["path"] for item in rows if isinstance(item.get("path"), str)]
        if len(paths) != len(rows):
            raise RuntimeError("Plan attachment mirror is missing a validated path")
        images = [
            item["path"]
            for item in rows
            if item.get("is_image") is True
        ]
        public = [
            {key: item[key] for key in ("url", "name", "is_image")}
            for item in rows
        ]
        return paths, images, public

    @staticmethod
    def _version_seed(version: PlanVersion) -> dict:
        return {
            "source_version_id": version.id,
            "version_number": version.version_number,
            "content": version.content,
            "context_session_id": version.context_session_id,
            "context_log_id": version.context_log_id,
            "context_snapshot": version.context_snapshot,
            "repo_revision": version.repo_revision,
            "reviewer_repo_revision": version.reviewer_repo_revision,
            "review_verdict": version.review_verdict,
            "review_feedback": version.review_feedback,
            "review_exhausted": version.review_exhausted,
            "reviewed_at": (
                version.reviewed_at.isoformat()
                if version.reviewed_at is not None
                else None
            ),
            "human_decision": version.human_decision,
        }

    async def run_versioned_plan_until_pause(
        self,
        plan: Plan,
        run: PlanAgentRun,
    ) -> dict:
        """Mirror/resume one Manager PlanRun and return an authoritative pause."""

        if plan.worker_id is None or run.worker_id != plan.worker_id:
            raise RuntimeError("Plan Run Worker assignment changed before forwarding")
        worker = await self.require_ready_worker(plan.worker_id)
        await self._require_versioned_plan_protocol(worker)
        worker_project_id = (
            await self.ensure_worker_project(worker, plan)
            if plan.project_id is not None
            else None
        )
        plan_paths, plan_images, plan_attachments = self._plan_attachment_payload(
            plan.initial_attachments
        )
        run_paths, run_images, run_attachments = self._plan_attachment_payload(
            run.attachments
        )
        paths = list(dict.fromkeys([*plan_paths, *run_paths]))
        image_paths = [
            path
            for path in paths
            if path in {*plan_images, *run_images}
        ]
        attachment_by_path = {
            path: attachment
            for path, attachment in [
                *zip(plan_paths, plan_attachments, strict=True),
                *zip(run_paths, run_attachments, strict=True),
            ]
        }
        attachments = [attachment_by_path[path] for path in paths]
        attachment_manifest = self._attachment_manifest(paths)
        if paths:
            await self.push_files(worker, paths)

        base_version = None
        if run.base_version_id is not None:
            async with self.db_factory() as db:
                base_version = await db.get(PlanVersion, run.base_version_id)
            if base_version is None:
                raise RuntimeError("Plan Run base Version disappeared before forwarding")
        request_text = run.request_text or plan.initial_request
        base_seed = self._version_seed(base_version) if base_version is not None else None
        if run.run_type == "fork" and base_version is not None:
            request_text = (
                f"{request_text}\n\n[Base Version selected for this fork]\n"
                f"{base_version.content}"
            )
            # A fork starts a fresh Version sequence; materializing its source
            # inside the new Plan would incorrectly make the first output vN+1.
            base_seed = None

        payload = {
            "protocol": 3,
            "plan_id": plan.id,
            "run_id": run.id,
            # This fences the Manager lifecycle only. The imported Worker Run
            # has an independent local generation used for its own retries and
            # input answers.
            "manager_claim_generation": run.generation,
            "title": plan.title,
            "initial_request": plan.initial_request,
            "target_task_id": plan.target_task_id,
            "project_id": worker_project_id,
            "target_branch": plan.target_branch,
            "priority": plan.priority,
            "timeout_hours": plan.timeout_hours,
            "pipeline_config": run.pipeline_config or plan.pipeline_config,
            "run_type": run.run_type,
            "source_run_id": run.source_run_id,
            "request_text": request_text,
            "context_session_id": run.context_session_id,
            "context_log_id": run.context_log_id,
            "context_snapshot": run.context_snapshot,
            "repo_revision": run.repo_revision,
            "max_interactions": run.max_interactions,
            "base_version": base_seed,
            "file_paths": paths or None,
            "image_paths": image_paths or None,
            "attachments": attachments or None,
            "attachment_manifest": attachment_manifest or None,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(worker, "/api/plans/worker-import"),
                headers=self._headers(worker),
                json=payload,
            )
            response.raise_for_status()
            imported = response.json()
        remote_run = imported.get("run") if isinstance(imported, dict) else None
        base_worker_version_id = (
            imported.get("base_worker_version_id")
            if isinstance(imported, dict)
            else None
        )
        if not isinstance(remote_run, dict) or remote_run.get("id") != run.id:
            raise RuntimeError("Worker returned an invalid Plan Run import receipt")
        if imported.get("attachment_receipt") != attachment_manifest:
            raise RuntimeError("Worker Plan attachment receipt does not match the manifest")

        if remote_run.get("status") == "waiting_user":
            remote_input_id = remote_run.get("open_input_request_id")
            async with self.db_factory() as db:
                answer = (
                    await db.execute(
                        select(PlanInputRequest)
                        .where(
                            PlanInputRequest.run_id == run.id,
                            PlanInputRequest.worker_id == worker.id,
                            PlanInputRequest.worker_input_request_id == remote_input_id,
                            PlanInputRequest.status == "answered",
                        )
                        .order_by(PlanInputRequest.id.desc())
                        .limit(1)
                    )
                ).scalar_one_or_none()
            if answer is not None:
                answer_paths, answer_images, answer_attachments = (
                    self._plan_attachment_payload(answer.attachments)
                )
                if answer_paths:
                    await self.push_files(worker, answer_paths)
                answer_manifest = self._attachment_manifest(answer_paths)
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.post(
                        self._api(
                            worker,
                            f"/api/plan-runs/{run.id}/input-requests/{remote_input_id}/answer",
                        ),
                        headers=self._headers(worker),
                        json={
                            "expected_run_generation": remote_run["generation"],
                            "idempotency_key": answer.answer_idempotency_key,
                            "answers": answer.answers or [],
                            "response_text": answer.response_text,
                            "file_paths": answer_paths or None,
                            "image_paths": answer_images or None,
                            "attachments": answer_attachments or None,
                            "attachment_manifest": answer_manifest or None,
                        },
                    )
                    response.raise_for_status()
                remote_run["status"] = "queued"

        timeout_seconds = (
            plan.timeout_hours * 3600
            if plan.timeout_hours is not None and plan.timeout_hours > 0
            else (
                None
                if plan.timeout_hours == 0
                else settings.task_timeout_seconds
            )
        )
        deadline = (
            asyncio.get_running_loop().time() + max(300.0, timeout_seconds + 300)
            if timeout_seconds is not None
            else None
        )
        while remote_run.get("status") in {"queued", "running"}:
            if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError("Worker Plan Run outcome polling timed out")
            await asyncio.sleep(1)
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, f"/api/plan-runs/{run.id}"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
                remote_run = response.json()
            if not isinstance(remote_run, dict) or remote_run.get("id") != run.id:
                raise RuntimeError("Worker returned an invalid Plan Run snapshot")

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self._api(worker, f"/api/plans/{plan.id}/versions"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
            versions = response.json()
        if not isinstance(versions, list):
            raise RuntimeError("Worker returned an invalid Plan Version list")
        versions = [
            version
            for version in versions
            if isinstance(version, dict)
            and version.get("produced_by_run_id") == run.id
        ]
        return {
            "protocol": 3,
            "base_worker_version_id": base_worker_version_id,
            "run": remote_run,
            "versions": versions,
        }

    async def materialize_plan_version(
        self,
        *,
        worker: Worker,
        plan: Plan,
        version: PlanVersion,
    ) -> int:
        """Ensure an exact immutable Version exists on the target Worker."""

        await self._require_versioned_plan_protocol(worker)
        worker_project_id = (
            await self.ensure_worker_project(worker, plan)
            if plan.project_id is not None
            else None
        )
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self._api(worker, "/api/plans/worker-materialize-version"),
                headers=self._headers(worker),
                json={
                    "protocol": 3,
                    "plan_id": plan.id,
                    "title": plan.title,
                    "initial_request": plan.initial_request,
                    "target_task_id": plan.target_task_id,
                    "project_id": worker_project_id,
                    "target_branch": plan.target_branch,
                    "priority": plan.priority,
                    "timeout_hours": plan.timeout_hours,
                    "pipeline_config": plan.pipeline_config,
                    "version": self._version_seed(version),
                },
            )
            response.raise_for_status()
            receipt = response.json()
        remote_id = receipt.get("id") if isinstance(receipt, dict) else None
        if isinstance(remote_id, bool) or not isinstance(remote_id, int):
            raise RuntimeError("Worker returned an invalid Version materialization receipt")
        return remote_id

    async def cancel_versioned_plan_run(self, worker_id: int, run_id: int) -> None:
        worker = await self.require_ready_worker(worker_id)
        await self._require_versioned_plan_protocol(worker)
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                self._api(worker, f"/api/plan-runs/{run_id}/cancel"),
                headers=self._headers(worker),
            )
        response.raise_for_status()

    # ------------------------------------------------------------------
    # 项目映射（设计 §8）
    # ------------------------------------------------------------------

    async def ensure_worker_project(self, worker: Worker, task: Task) -> int:
        """确保 worker 上有 task 对应的项目，返回 worker 侧 project_id。

        Phase 2 仅支持有 git remote 的项目（worker 自己 clone）；
        纯本地项目走 Phase 3 的播种方案，这里直接报错。
        """
        if not task.project_id:
            raise RuntimeError("worker task 必须关联项目（需要 git 信息）")

        key = (worker.id, task.project_id)
        lock = _project_locks.setdefault(key, asyncio.Lock())
        async with lock:
            async with self.db_factory() as db:
                w = await db.get(Worker, worker.id)
                mapping = dict(w.project_mapping or {})
            if str(task.project_id) in mapping:
                return mapping[str(task.project_id)]

            async with self.db_factory() as db:
                project = await db.get(Project, task.project_id)
            if not project:
                raise RuntimeError(f"项目 {task.project_id} 不存在")
            if not project.git_url:
                # 纯本地项目：先把整个项目目录（含 .git 和未提交改动）rsync 到
                # worker 同路径，worker 的 _init_local_repo 见 .git 存在即跳过 init
                import os as _os
                path = _os.path.expanduser(project.local_path).rstrip("/")
                if not _os.path.isdir(path):
                    raise RuntimeError(f"项目目录不存在: {path}")
                ssh = self._ssh(worker)
                await ssh.run(f"mkdir -p {path}")
                await ssh.rsync_to(path + "/", path + "/", excludes=[], timeout=1200)

            async with httpx.AsyncClient(timeout=30) as c:
                # 同名项目可能已存在（之前转发过/手工建过）
                r = await c.get(self._api(worker, "/api/projects"), headers=self._headers(worker))
                r.raise_for_status()
                items = r.json()
                if isinstance(items, dict):
                    items = items.get("projects", [])
                remote = next((p for p in items if p.get("name") == project.name), None)
                if remote is None:
                    r = await c.post(
                        self._api(worker, "/api/projects"),
                        headers=self._headers(worker),
                        json={
                            "name": project.name,
                            "git_url": project.git_url,
                            "default_branch": project.default_branch or "main",
                            "git_author_name": project.git_author_name,
                            "git_author_email": project.git_author_email,
                            "git_credential_type": project.git_credential_type,
                            "git_https_username": project.git_https_username,
                            "git_https_token": project.git_https_token,
                        },
                    )
                    r.raise_for_status()
                    remote = r.json()
                remote_id = remote["id"]

                # clone 是后台任务，等 status=ready（worker dispatch 需要 local_path 就绪）
                deadline = asyncio.get_event_loop().time() + 300
                while remote.get("status") != "ready":
                    if asyncio.get_event_loop().time() > deadline:
                        raise RuntimeError(f"worker 项目 {project.name} clone 超时")
                    await asyncio.sleep(3)
                    r = await c.get(
                        self._api(worker, f"/api/projects/{remote_id}"),
                        headers=self._headers(worker),
                    )
                    r.raise_for_status()
                    remote = r.json()

            async with self.db_factory() as db:
                w = await db.get(Worker, worker.id)
                mapping = dict(w.project_mapping or {})
                mapping[str(task.project_id)] = remote_id
                w.project_mapping = mapping
                await db.commit()
            return remote_id

    # ------------------------------------------------------------------
    # 任务转发（设计 §5.3）
    # ------------------------------------------------------------------

    async def require_worker_fast_support(
        self,
        worker: Worker,
        task: Task,
    ) -> None:
        """Fail before creation when a Worker cannot prove required features.

        Older Workers ignore unknown Task fields, which would otherwise let a
        Manager display Fast while the remote turn runs as Standard, or run a
        PR review from the Worker's CCM checkout without snapshot isolation.
        """

        needs_fast = (
            (task.provider or "claude").lower() == "codex"
            and (task.codex_service_tier or "default") == "priority"
        )
        needs_pr_snapshot_context = is_pr_sandbox_task(task)
        if not needs_fast and not needs_pr_snapshot_context:
            return

        async with httpx.AsyncClient(timeout=30) as c:
            response = await c.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            response.raise_for_status()
        try:
            config = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"Worker {worker.name} 无法确认任务所需能力，任务未转发"
            ) from exc
        if not isinstance(config, dict):
            raise RuntimeError(
                f"Worker {worker.name} 无法确认任务所需能力，任务未转发"
            )

        if (
            needs_pr_snapshot_context
            and config.get("pr_review_snapshot_context_version")
            != PR_REVIEW_SNAPSHOT_CONTEXT_VERSION
        ):
            raise RuntimeError(
                f"Worker {worker.name} 未声明 PR 审核快照隔离能力 v"
                f"{PR_REVIEW_SNAPSHOT_CONTEXT_VERSION}，任务未转发"
            )

        if not needs_fast:
            return

        model = task.model
        if not model or model == "default":
            model = config.get("default_codex_model")
        tiers_by_model = config.get("codex_model_service_tiers")
        supported = (
            tiers_by_model.get(model)
            if isinstance(tiers_by_model, dict) and isinstance(model, str)
            else None
        )
        if not isinstance(supported, list) or "priority" not in supported:
            raise RuntimeError(
                f"Worker {worker.name} 未声明模型 {model or 'default'} "
                "支持 Codex Fast，任务未转发"
            )

    async def require_terminal_pr_review_chat_support(
        self,
        worker: Worker,
    ) -> None:
        """Reject mixed-version PR follow-ups before Manager-side logging."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                f"无法确认 Worker {worker.name} 的 PR 审核续聊能力",
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("pr_review_terminal_chat_version")
            != PR_REVIEW_TERMINAL_CHAT_VERSION
        ):
            raise HTTPException(
                409,
                f"Worker {worker.name} 版本过旧，升级后才能继续 PR 审核对话",
            )

    async def forward_task_to_worker(
        self,
        task: Task,
        *,
        operation_lock_held: bool = False,
    ):
        if operation_lock_held:
            current = await self._authoritative_forward_task(task)
            return await self._forward_task_to_worker_locked(current)
        async with self.task_operation_lock(task.id):
            current = await self._authoritative_forward_task(task)
            return await self._forward_task_to_worker_locked(current)

    async def _authoritative_forward_task(self, task: Task) -> Task:
        """Fence one claimed generation immediately before Worker effects."""

        if self.db_factory is None:
            return task
        expected = worker_task_generation(task)
        if expected is None:
            raise RuntimeError(
                "Task is no longer assigned to a Worker before forwarding"
            )
        async with self.db_factory() as db:
            # This is the portable Task-side writer fence shared with
            # termination admission. ``forward_task_to_worker`` holds the
            # per-Task operation lock across this check and every following
            # Worker effect, so a receipt either wins first and blocks the
            # POST, or waits until this exact forwarding attempt settles.
            admitted = await db.execute(
                update(Task)
                .where(
                    Task.id == task.id,
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
            )
            if admitted.rowcount != 1:
                current = await db.get(Task, task.id, populate_existing=True)
                current_generation = (
                    worker_task_generation(current)
                    if current is not None
                    else None
                )
                await db.rollback()
                if current_generation == expected:
                    raise WorkerTaskForwardAdmissionBlockedError(
                        "Task termination owns the claimed Worker generation"
                    )
                raise RuntimeError(
                    "Task Worker generation changed before initial forwarding"
                )
            current = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == task.id,
                        no_active_worker_task_termination_predicate(),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if current is None or worker_task_generation(current) != expected:
                await db.rollback()
                raise RuntimeError(
                    "Task Worker generation changed before initial forwarding"
                )
            await db.commit()
            return current

    async def _forward_task_to_worker_locked(self, task: Task):
        worker = await self.get_worker(task.worker_id)
        if not worker or worker.status != "ready":
            raise RuntimeError(
                f"Worker {worker.name if worker else task.worker_id} 不可用"
                f"（{worker.status if worker else 'not found'}）"
            )

        await self.require_worker_fast_support(worker, task)
        # PR reviews use only the remote GitHub snapshot named in their prompt.
        # Mapping the Manager's synthetic PR-Monitor project would either fail
        # (it has no repository) or make the Worker load unrelated local agent
        # docs.  Tags survive Manager→Worker forwarding, unlike metadata.
        worker_project_id = (
            None
            if is_pr_sandbox_task(task)
            else await self.ensure_worker_project(worker, task)
        )

        metadata = task.metadata_ or {}
        # Related-Plan uploads are validated and marked by the Manager API.
        # Do not copy arbitrary legacy metadata paths to another machine.
        has_related_plan_uploads = (
            task.mode == "plan"
            and task.plan_target_task_id is not None
            and metadata.get("created_from_plan_target_task_id")
            == task.plan_target_task_id
        )
        attachment_paths = (
            metadata.get("file_paths") or metadata.get("image_paths") or []
            if has_related_plan_uploads
            else []
        )
        attachment_records = (
            metadata.get("attachments") or []
            if has_related_plan_uploads
            else []
        )
        if attachment_paths:
            await self.push_files(worker, attachment_paths)
        image_paths = [
            path
            for index, path in enumerate(attachment_paths)
            if (
                index < len(attachment_records)
                and isinstance(attachment_records[index], dict)
                and attachment_records[index].get("is_image") is True
            )
        ]

        # 先订阅 relay 再创建：worker Dispatcher 可能创建后立即执行，后订阅丢初始事件
        await self.relay.subscribe_task(worker, task.id)
        user_skill_snapshots = await self._user_skill_snapshots(task)

        payload = {
            "id": task.id,  # 关键：Manager 分配的全局 ID
            "title": task.title,
            "description": task.description or "",
            "project_id": worker_project_id,
            "target_branch": task.target_branch or "main",
            "priority": task.priority,
            "max_retries": task.max_retries,
            "mode": task.mode,
            "todo_file_path": task.todo_file_path,
            "max_iterations": task.max_iterations,
            "must_complete": task.must_complete,
            "goal_condition": task.goal_condition,
            "goal_max_turns": task.goal_max_turns,
            "goal_evaluator_model": task.goal_evaluator_model,
            "plan_target_task_id": task.plan_target_task_id,
            "plan_context_session_id": task.plan_context_session_id,
            "plan_context_log_id": task.plan_context_log_id,
            "plan_context_snapshot": task.plan_context_snapshot,
            "plan_repo_revision": task.plan_repo_revision,
            "supersedes_plan_task_id": task.supersedes_plan_task_id,
            "plan_pipeline_config": task.plan_pipeline_config,
            "provider": task.provider,
            "model": task.model,
            "codex_service_tier": task.codex_service_tier,
            "effort_level": task.effort_level,
            "thinking_budget": task.thinking_budget,
            "timeout_hours": task.timeout_hours,
            "enable_workflows": task.enable_workflows,
            "enabled_skills": task.enabled_skills,
            "selected_user_skills": task.selected_user_skills,
            "user_skill_snapshots": user_skill_snapshots,
            "tags": list(task.tags) if task.tags else None,
            "file_paths": attachment_paths or None,
            "image_paths": image_paths or None,
            "attachments": attachment_records or None,
            "attention_tag": task.attention_tag,
        }
        post_started = False
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                post_started = True
                r = await c.post(
                    self._api(worker, "/api/tasks"),
                    headers=self._headers(worker),
                    json=payload,
                )
                # Once POST has started, even an HTTP error or malformed ACK
                # cannot prove that the Worker did not commit and wake its
                # dispatcher.  Surface a distinct uncertainty contract so the
                # Manager never blindly resends the create request.
                r.raise_for_status()
                if (task.codex_service_tier or "default") == "priority":
                    try:
                        created = r.json()
                    except Exception as exc:
                        raise RuntimeError(
                            f"Worker {worker.name} 未确认 Codex Fast 任务配置"
                        ) from exc
                    if (
                        not isinstance(created, dict)
                        or created.get("codex_service_tier") != "priority"
                    ):
                        raise RuntimeError(
                            f"Worker {worker.name} 未确认 Codex Fast 任务配置"
                        )
        except asyncio.CancelledError as exc:
            if not post_started:
                raise
            raise WorkerTaskForwardOutcomeUncertainError(
                f"Worker {worker.name} initial Task POST was cancelled after "
                "its outcome became uncertain",
                cancellation=exc,
            ) from exc
        except Exception as exc:
            if not post_started:
                raise
            raise WorkerTaskForwardOutcomeUncertainError(
                f"Worker {worker.name} initial Task POST outcome is uncertain: {exc}"
            ) from exc
        logger.info("task %s forwarded to worker %s", task.id, worker.id)

    async def _user_skill_snapshots(self, task: Task) -> list[dict]:
        from backend.services.skill_context import (
            build_user_skill_snapshot_payload,
            normalize_user_skill_ids,
        )

        if not normalize_user_skill_ids(task.selected_user_skills):
            return []
        async with self.db_factory() as db:
            return await build_user_skill_snapshot_payload(
                db,
                task.selected_user_skills,
                metadata=task.metadata_,
            )

    async def sync_task_skill_selection(
        self,
        worker: Worker,
        task: Task,
    ) -> None:
        """Refresh and confirm a remote task's Skills before a new turn."""

        from backend.services.command_registry import ensure_default_skills
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
            normalize_user_skill_ids,
        )

        user_skill_snapshots = await self._user_skill_snapshots(task)
        payload = {
            "enabled_skills": ensure_default_skills(task.enabled_skills),
            "selected_user_skills": normalize_user_skill_ids(
                task.selected_user_skills
            ),
            "user_skill_snapshots": user_skill_snapshots,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.put(
                self._api(worker, f"/api/tasks/{task.id}"),
                headers=self._headers(worker),
                json=payload,
            )
            response.raise_for_status()
            try:
                confirmed = response.json()
            except Exception as exc:
                raise HTTPException(
                    502,
                    "Worker Skill selection synchronization returned an "
                    "invalid confirmation",
                ) from exc

        confirmed_metadata = (
            confirmed.get("metadata_")
            if isinstance(confirmed, dict)
            else None
        )
        confirmed_snapshots = (
            confirmed_metadata.get(USER_SKILL_SNAPSHOTS_METADATA_KEY)
            if isinstance(confirmed_metadata, dict)
            else None
        )
        # ``instance_id`` is node-local execution ownership. A task migrated
        # from Manager to Worker deliberately keeps the old Manager instance
        # id while the imported Worker row has no corresponding local
        # instance. The globally assigned task id plus the monotonic retry
        # generation and coordinated inert status identify the remote copy;
        # comparing unrelated database ids would reject every such migration.
        if (
            not isinstance(confirmed, dict)
            or confirmed.get("id") != task.id
            or confirmed.get("status") != task.status
            or confirmed.get("retry_count") != task.retry_count
            or confirmed.get("enabled_skills") != payload["enabled_skills"]
            or confirmed.get("selected_user_skills")
            != payload["selected_user_skills"]
            or confirmed_snapshots != user_skill_snapshots
        ):
            raise HTTPException(
                409,
                "Worker Skill selection does not exactly match the Manager; "
                "execution was blocked",
            )

    async def push_files(self, worker: Worker, paths: list[str]):
        """chat 附件推到 worker 同一绝对路径（worker 上 Claude 用 Read 读）。"""
        ssh = self._ssh(worker)
        for path in paths:
            await ssh.copy_file(path, path)

    async def require_task_artifact_scope_support(
        self,
        worker: Worker,
    ) -> None:
        """Fail closed when a Worker cannot enforce the managed namespace."""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self._api(worker, "/api/system/config"),
                    headers=self._headers(worker),
                )
                response.raise_for_status()
            config = response.json()
        except Exception as exc:
            raise HTTPException(
                503,
                f"无法确认 Worker {worker.name} 的 Task 产物隔离能力",
            ) from exc
        if (
            not isinstance(config, dict)
            or config.get("task_artifact_scope_version")
            != TASK_ARTIFACT_SCOPE_VERSION
        ):
            raise HTTPException(
                409,
                f"Worker {worker.name} 版本过旧，升级后才能下载 Task 产物",
            )

    async def stream_task_artifact(
        self,
        task: Task,
        artifact_path: str,
    ) -> StreamingResponse:
        """Stream a task-scoped file from its Worker without buffering it."""

        worker = await self.require_ready_worker(task.worker_id)
        await self.require_task_artifact_scope_support(worker)
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=None, write=30, pool=10),
        )
        try:
            request = client.build_request(
                "GET",
                self._api(
                    worker,
                    f"/api/tasks/{task.id}/artifacts/download",
                ),
                headers=self._headers(worker),
                params={"path": artifact_path},
            )
            response = await client.send(request, stream=True)
        except (httpx.TimeoutException, TimeoutError) as exc:
            await client.aclose()
            raise HTTPException(
                503,
                f"Worker {worker.name} artifact request timed out",
            ) from exc
        except (httpx.RequestError, OSError) as exc:
            await client.aclose()
            raise HTTPException(
                502,
                f"Unable to reach Worker {worker.name}",
            ) from exc

        if not 200 <= response.status_code < 300:
            try:
                payload = await response.aread()
            finally:
                await response.aclose()
                await client.aclose()
            if response.status_code == 401:
                raise HTTPException(
                    502,
                    f"Worker {worker.name} rejected its internal credential",
                )
            status_code = (
                response.status_code
                if response.status_code in {400, 403, 404, 413}
                else 502
            )
            detail = "Worker artifact download failed"
            try:
                decoded = response.json()
                if isinstance(decoded, dict) and isinstance(decoded.get("detail"), str):
                    detail = decoded["detail"]
            except Exception:
                if payload:
                    detail = payload[:300].decode(errors="replace")
            raise HTTPException(status_code, detail)

        forwarded_headers = {}
        for header in ("content-disposition", "content-length", "content-type"):
            value = response.headers.get(header)
            if value:
                forwarded_headers[header] = value

        async def close_upstream() -> None:
            await response.aclose()
            await client.aclose()

        async def body():
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            finally:
                await close_upstream()

        return StreamingResponse(
            body(),
            status_code=response.status_code,
            headers=forwarded_headers,
            background=BackgroundTask(close_upstream),
        )

    # ------------------------------------------------------------------
    # 通用操作代理（设计 §6.4）
    # ------------------------------------------------------------------

    async def proxy_to_worker(
        self,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        require_json: bool = False,
        allow_task_absent: bool = False,
        surface_endpoint_not_found: bool = False,
        operation_lock_held: bool = False,
        pr_review_terminal_chat: bool = False,
        quarantine_on_transport_uncertainty: bool = False,
    ):
        if pr_review_terminal_chat and not is_pr_review_task(task):
            raise ValueError(
                "Terminal PR review chat authorization requires a PR review Task"
            )
        if operation_lock_held:
            return await self._proxy_to_worker_locked(
                task,
                method,
                path,
                body,
                require_json=require_json,
                allow_task_absent=allow_task_absent,
                surface_endpoint_not_found=surface_endpoint_not_found,
                pr_review_terminal_chat=pr_review_terminal_chat,
                quarantine_on_transport_uncertainty=(
                    quarantine_on_transport_uncertainty
                ),
            )
        async with self.task_operation_lock(task.id):
            return await self._proxy_to_worker_locked(
                task,
                method,
                path,
                body,
                require_json=require_json,
                allow_task_absent=allow_task_absent,
                surface_endpoint_not_found=surface_endpoint_not_found,
                pr_review_terminal_chat=pr_review_terminal_chat,
                quarantine_on_transport_uncertainty=(
                    quarantine_on_transport_uncertainty
                ),
            )

    async def _proxy_to_worker_locked(
        self,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        require_json: bool,
        allow_task_absent: bool,
        surface_endpoint_not_found: bool,
        pr_review_terminal_chat: bool,
        quarantine_on_transport_uncertainty: bool,
    ):
        # A durable Manager receipt owns every remote mutation until its exact
        # result is ACKed. The reconciliation loop itself traverses this common
        # proxy, so admit only that receipt's identity-bound GET/PUT/ACK paths;
        # all ordinary Plan/Monitor/Sub-Agent/config requests must wait.
        if self.db_factory is not None:
            async with self.db_factory() as db:
                active_receipt = await active_worker_task_termination_receipt(
                    db,
                    task.id,
                )
            if active_receipt is not None:
                receipt_path = (
                    f"/api/tasks/{task.id}/termination-receipts/"
                    f"{active_receipt.operation_id}"
                )
                exact_receipt_request = bool(
                    active_receipt.side == "manager"
                    and active_receipt.worker_id == task.worker_id
                    and (
                        (
                            method == "GET"
                            and path == receipt_path
                            and body is None
                        )
                        or (
                            method == "PUT"
                            and path == receipt_path
                            and isinstance(body, dict)
                        )
                        or (
                            method == "POST"
                            and path == f"{receipt_path}/ack"
                            and isinstance(body, dict)
                        )
                    )
                )
                if not exact_receipt_request:
                    raise HTTPException(
                        409,
                        "Task has an active Worker termination receipt",
                    )
        worker = await self.require_ready_worker(task.worker_id)
        return await self._proxy_to_authorized_worker_locked(
            worker,
            task,
            method,
            path,
            body,
            require_json=require_json,
            allow_task_absent=allow_task_absent,
            surface_endpoint_not_found=surface_endpoint_not_found,
            pr_review_terminal_chat=pr_review_terminal_chat,
            quarantine_on_transport_uncertainty=(
                quarantine_on_transport_uncertainty
            ),
        )

    async def _proxy_to_claimed_destroying_worker(
        self,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        destroy_claim: WorkerDestroyLifecycleClaim,
        require_json: bool = False,
        allow_task_absent: bool = False,
        surface_endpoint_not_found: bool = False,
        operation_lock_held: bool = False,
        quarantine_on_transport_uncertainty: bool = False,
    ):
        """Proxy only for exact terminal reconciliation during Worker destroy."""

        if not operation_lock_held:
            raise ValueError(
                "claimed Worker destroy proxy requires the Task operation lock"
            )
        receipt_prefix = f"/api/tasks/{task.id}/termination-receipts/"
        receipt_suffix = path[len(receipt_prefix):] if path.startswith(receipt_prefix) else ""
        receipt_operation_id = (
            receipt_suffix[:-4] if receipt_suffix.endswith("/ack") else receipt_suffix
        )
        valid_receipt_id = bool(
            len(receipt_operation_id) == 32
            and all(char in "0123456789abcdef" for char in receipt_operation_id)
        )
        receipt_get = method == "GET" and valid_receipt_id and not receipt_suffix.endswith("/ack")
        receipt_put = method == "PUT" and valid_receipt_id and not receipt_suffix.endswith("/ack")
        receipt_ack = method == "POST" and valid_receipt_id and receipt_suffix.endswith("/ack")
        allowed_request = bool(
            (receipt_get and body is None)
            or ((receipt_put or receipt_ack) and isinstance(body, dict))
        )
        if (
            not allowed_request
            or require_json is not True
            or allow_task_absent
            or surface_endpoint_not_found
            or quarantine_on_transport_uncertainty
        ):
            raise ValueError(
                "Worker destroy claim authorizes only exact termination receipt "
                "GET/PUT/ACK requests"
            )
        if task.worker_id != destroy_claim.worker_id:
            raise HTTPException(
                409,
                "Task moved away from the claimed destroying Worker",
            )
        worker = await self._require_destroy_lifecycle_claim(destroy_claim)
        return await self._proxy_to_authorized_worker_locked(
            worker,
            task,
            method,
            path,
            body,
            require_json=require_json,
            allow_task_absent=allow_task_absent,
            surface_endpoint_not_found=surface_endpoint_not_found,
            pr_review_terminal_chat=False,
            quarantine_on_transport_uncertainty=(
                quarantine_on_transport_uncertainty
            ),
        )

    async def _proxy_to_authorized_worker_locked(
        self,
        worker: Worker,
        task: Task,
        method: str,
        path: str,
        body=None,
        *,
        require_json: bool,
        allow_task_absent: bool,
        surface_endpoint_not_found: bool,
        pr_review_terminal_chat: bool,
        quarantine_on_transport_uncertainty: bool,
    ):
        await self.relay.subscribe_task(worker, task.id)
        headers = self._headers(worker)
        if pr_review_terminal_chat:
            headers[PR_REVIEW_TERMINAL_CHAT_HEADER] = (
                PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE
            )
        request_started = False
        try:
            async with httpx.AsyncClient(timeout=60) as c:
                request_started = True
                r = await c.request(
                    method, self._api(worker, path),
                    headers=headers, json=body,
                )
        except asyncio.CancelledError as exc:
            if quarantine_on_transport_uncertainty and request_started:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} request was cancelled after "
                    "the mutation boundary",
                    status_code=503,
                    cancellation=exc,
                ) from exc
            raise
        except (httpx.TimeoutException, TimeoutError) as exc:
            if quarantine_on_transport_uncertainty and request_started:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} request timed out after the "
                    "mutation boundary",
                    status_code=503,
                ) from exc
            raise HTTPException(
                503,
                f"Worker {worker.name} 请求超时，请稍后重试",
            ) from exc
        except (httpx.RequestError, OSError) as exc:
            if quarantine_on_transport_uncertainty and request_started:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} connection was lost after the "
                    "mutation boundary",
                    status_code=502,
                ) from exc
            raise HTTPException(
                502,
                f"Worker 网关连接失败，无法连接到 Worker {worker.name}",
            ) from exc

        # Worker token is an internal Manager→Worker credential.  Never
        # propagate a remote 401/403: doing so makes the frontend treat the
        # Manager login as expired.  Other upstream failures are gateway
        # errors too, and their response bodies may contain Worker internals.
        if r.status_code in (401, 403):
            raise HTTPException(
                502,
                f"内部 Worker 认证失败（远端 HTTP {r.status_code}），"
                "请重试 Worker 引导以同步认证凭据",
            )
        if surface_endpoint_not_found and r.status_code == 404:
            raise WorkerEndpointNotFoundError(path)
        if allow_task_absent and r.status_code == 404:
            try:
                missing = r.json()
            except Exception:
                missing = None
            if (
                isinstance(missing, dict)
                and missing.get("detail") == "Task not found"
            ):
                return {"ok": True, "already_deleted": True}
        if not 200 <= r.status_code < 300:
            if quarantine_on_transport_uncertainty:
                raise WorkerTaskMutationOutcomeUncertainError(
                    f"Worker {worker.name} returned HTTP {r.status_code} "
                    "after the mutation boundary",
                    status_code=502,
                )
            raise HTTPException(
                502,
                f"Worker 上游请求失败（远端 HTTP {r.status_code}）",
            )
        try:
            return r.json()
        except Exception as exc:
            if require_json:
                if quarantine_on_transport_uncertainty:
                    raise WorkerTaskMutationOutcomeUncertainError(
                        f"Worker {worker.name} returned an unreadable "
                        "confirmation after the mutation boundary",
                        status_code=502,
                    ) from exc
                raise HTTPException(
                    502,
                    f"Worker {worker.name} returned an invalid confirmation",
                ) from exc
            return {"ok": True}
