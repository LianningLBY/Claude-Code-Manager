"""Manager→Worker 任务转发与操作代理（elastic-worker 设计 §5.3/§6.3/§6.4/§8）。

- forward_task_to_worker：确保 worker 有项目 → 先订阅 relay → 用 Manager 分配的
  同一 task ID 在 worker 上创建 task（ID 全局统一，见设计 §2）
- proxy_to_worker：通用操作代理（stop/cancel/retry/plan/monitor），转发前确保
  relay 已订阅（幂等；retry 场景 Manager 重启后 relay 未订阅，不补订阅则全丢）
"""

from __future__ import annotations

import asyncio
import logging
from weakref import WeakKeyDictionary

import httpx
from fastapi import HTTPException
from sqlalchemy import select

from backend.config import settings
from backend.models.project import Project
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path
from backend.services.worker_relay import worker_task_generation

logger = logging.getLogger(__name__)

# (worker_id, manager_project_id) -> Lock，防并发 task 重复建项目
_project_locks: dict[tuple[int, int], asyncio.Lock] = {}
_task_operation_locks: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = WeakKeyDictionary()


class WorkerEndpointNotFoundError(Exception):
    """A caller-requested signal that the Worker returned an exact HTTP 404."""


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
            or payload.get("versioned_plan_worker_protocol") != 1
        ):
            raise RuntimeError(
                f"Worker {worker.name} does not support versioned Plan protocol 1"
            )

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
            "protocol": 1,
            "plan_id": plan.id,
            "run_id": run.id,
            "run_generation": run.generation,
            "title": plan.title,
            "initial_request": plan.initial_request,
            "target_task_id": plan.target_task_id,
            "project_id": worker_project_id,
            "target_branch": plan.target_branch,
            "priority": plan.priority,
            "timeout_hours": plan.timeout_hours,
            "pipeline_config": run.pipeline_config or plan.pipeline_config,
            "run_type": run.run_type,
            "request_text": request_text,
            "context_session_id": run.context_session_id,
            "context_log_id": run.context_log_id,
            "context_snapshot": run.context_snapshot,
            "repo_revision": run.repo_revision,
            "max_interactions": run.max_interactions,
            "base_version": base_seed,
            "file_paths": run_paths or plan_paths or None,
            "image_paths": run_images or plan_images or None,
            "attachments": run_attachments or plan_attachments or None,
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
            "protocol": 1,
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
                    "protocol": 1,
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
        """Fail before remote task creation when a Worker cannot prove Fast.

        Older Workers ignore unknown Task fields, which would otherwise let a
        Manager display Fast while the remote turn runs as Standard.
        """

        if (
            (task.provider or "claude").lower() != "codex"
            or (task.codex_service_tier or "default") != "priority"
        ):
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
                f"Worker {worker.name} 无法确认 Codex Fast 能力，任务未转发"
            ) from exc
        if not isinstance(config, dict):
            raise RuntimeError(
                f"Worker {worker.name} 无法确认 Codex Fast 能力，任务未转发"
            )

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
        """Reload one claimed generation before serializing Worker create."""

        if self.db_factory is None:
            return task
        expected = worker_task_generation(task)
        if expected is None:
            raise RuntimeError(
                "Task is no longer assigned to a Worker before forwarding"
            )
        async with self.db_factory() as db:
            current = await db.get(Task, task.id)
        if current is None or worker_task_generation(current) != expected:
            raise RuntimeError(
                "Task Worker generation changed before initial forwarding"
            )
        return current

    async def _forward_task_to_worker_locked(self, task: Task):
        worker = await self.get_worker(task.worker_id)
        if not worker or worker.status != "ready":
            raise RuntimeError(
                f"Worker {worker.name if worker else task.worker_id} 不可用"
                f"（{worker.status if worker else 'not found'}）"
            )

        await self.require_worker_fast_support(worker, task)
        worker_project_id = await self.ensure_worker_project(worker, task)

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
        }
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                self._api(worker, "/api/tasks"),
                headers=self._headers(worker),
                json=payload,
            )
            # 不检查会卡死在 in_progress：422 字段校验失败 / 500 都要立刻暴露
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
    ):
        if operation_lock_held:
            return await self._proxy_to_worker_locked(
                task,
                method,
                path,
                body,
                require_json=require_json,
                allow_task_absent=allow_task_absent,
                surface_endpoint_not_found=surface_endpoint_not_found,
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
    ):
        worker = await self.require_ready_worker(task.worker_id)
        await self.relay.subscribe_task(worker, task.id)
        async with httpx.AsyncClient(timeout=60) as c:
            try:
                r = await c.request(
                    method, self._api(worker, path),
                    headers=self._headers(worker), json=body,
                )
            except (httpx.TimeoutException, TimeoutError) as exc:
                raise HTTPException(
                    503,
                    f"Worker {worker.name} 请求超时，请稍后重试",
                ) from exc
            except (httpx.RequestError, OSError) as exc:
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
            raise HTTPException(
                502,
                f"Worker 上游请求失败（远端 HTTP {r.status_code}）",
            )
        try:
            return r.json()
        except Exception as exc:
            if require_json:
                raise HTTPException(
                    502,
                    f"Worker {worker.name} returned an invalid confirmation",
                ) from exc
            return {"ok": True}
