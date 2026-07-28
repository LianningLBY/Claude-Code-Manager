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

from backend.config import settings
from backend.models.project import Project
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path

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

    async def forward_task_to_worker(self, task: Task):
        async with self.task_operation_lock(task.id):
            return await self._forward_task_to_worker_locked(task)

    async def _forward_task_to_worker_locked(self, task: Task):
        worker = await self.get_worker(task.worker_id)
        if not worker or worker.status != "ready":
            raise RuntimeError(
                f"Worker {worker.name if worker else task.worker_id} 不可用"
                f"（{worker.status if worker else 'not found'}）"
            )

        await self.require_worker_fast_support(worker, task)
        worker_project_id = await self.ensure_worker_project(worker, task)

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
        if (
            not isinstance(confirmed, dict)
            or confirmed.get("id") != task.id
            or confirmed.get("status") != task.status
            or confirmed.get("retry_count") != task.retry_count
            or confirmed.get("instance_id") != task.instance_id
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
