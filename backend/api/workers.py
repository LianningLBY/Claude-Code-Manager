"""Worker 管理 API（elastic-worker 设计 §18）。

长流程（创建/开关机/销毁）全部 fire-and-forget 后台执行，
进度经 "workers" WS channel 实时广播，API 立即返回当前记录。
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import shlex
import socket
import time
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import desc, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.worker import Worker
from backend.schemas.worker import WorkerCreate, WorkerLogsResponse, WorkerResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/workers", tags=["workers"])

# 后台任务强引用：event loop 只持弱引用，长耗时 bootstrap 任务可能被 GC
# 掐死在半路（asyncio 文档明确的坑）
_background_tasks: set[asyncio.Task] = set()
# Ready-Worker account logins outlive their initiating HTTP request.  Keep only
# challenge metadata here; passwords, mailbox tokens and OTP codes never enter
# this process-wide status store.
_worker_login_state: dict[str, dict] = {}
_worker_login_admission_lock = asyncio.Lock()
# Background logins for different accounts can finish at the same time.  The
# accounts column is one JSON value, so serialize its read/modify/write cycle
# to prevent one successful login from overwriting another.
_worker_account_store_lock = asyncio.Lock()
# Lifecycle endpoints perform a durable compare-and-set before spawning their
# background operation.  Keep same-process transitions for one Worker inside a
# single transaction boundary.  Besides preventing duplicate coordinators,
# this is required for SQLite's single-connection configurations where a
# concurrent losing rollback could otherwise undo the winning request's
# uncommitted CAS while it is still checking destroy blockers.


class _WorkerLifecycleTransitionLock:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


_worker_lifecycle_transition_locks: dict[
    tuple[asyncio.AbstractEventLoop, int], _WorkerLifecycleTransitionLock
] = {}

_LOGIN_METHODS = frozenset({"", "171mail", "mailcom", "onet", "gazeta"})
_CODEX_LOGIN_METHODS = _LOGIN_METHODS | {"mailcatcher"}
_WORKER_ACCOUNT_PROVIDERS = frozenset({"claude", "codex"})
_WORKER_DESTROYABLE_STATUSES = frozenset({"ready", "stopped", "error"})
_WORKER_AUTH_FAILURE_STATUSES = frozenset({401, 403})
_WORKER_ACTIVE_LOGIN_STATUSES = frozenset({
    "running", "awaiting_otp", "verifying_otp", "finalizing", "cancelling",
})
_NO_WORKER_JSON = object()


def _normalize_login_method(value: str | None) -> str:
    if value is not None and not isinstance(value, str):
        raise HTTPException(400, "login_method 必须是字符串")
    method = (value or "").strip().lower()
    if method not in _LOGIN_METHODS:
        raise HTTPException(400, f"不支持的登录方式: {method}")
    return method


def _normalize_worker_account_provider(value: str | None) -> str:
    if not isinstance(value, str):
        raise HTTPException(400, "provider 必须是字符串")
    provider = value.strip().lower()
    if provider not in _WORKER_ACCOUNT_PROVIDERS:
        raise HTTPException(400, f"不支持的 Worker 账号 provider: {provider}")
    return provider


def _normalize_worker_login_method(value: str | None, provider: str) -> str:
    if value is not None and not isinstance(value, str):
        raise HTTPException(400, "login_method 必须是字符串")
    method = (value or "").strip().lower()
    allowed = _CODEX_LOGIN_METHODS if provider == "codex" else _LOGIN_METHODS
    if method not in allowed:
        raise HTTPException(400, f"不支持的 {provider} 登录方式: {method}")
    return method


def _normalize_worker_account(
    *,
    email: str,
    provider: str,
    token: str | None,
    password: str | None,
    login_method: str | None,
    require_unattended: bool = False,
) -> dict:
    normalized_email = email.strip()
    if not normalized_email:
        raise HTTPException(400, "账号 email 必填")

    normalized_provider = _normalize_worker_account_provider(provider)
    normalized_token = (token or "").strip()
    # OpenAI passwords are opaque. In particular, never trim leading/trailing
    # characters while moving them through Manager storage into the Worker.
    normalized_password = password or ""
    if normalized_provider == "claude":
        if not normalized_token:
            raise HTTPException(400, f"Claude 账号 {normalized_email} 缺少 token")
    elif not normalized_token and not normalized_password:
        raise HTTPException(
            400,
            f"Codex 账号 {normalized_email} 的 token 和 password 至少填写一项",
        )
    elif normalized_provider == "codex" and require_unattended and not normalized_token:
        raise HTTPException(
            400,
            f"Codex 账号 {normalized_email} 的 Worker 自动 bootstrap 必须提供邮箱 token",
        )

    return {
        "email": normalized_email,
        "provider": normalized_provider,
        "token": normalized_token,
        "password": normalized_password,
        "login_method": _normalize_worker_login_method(
            login_method, normalized_provider
        ),
    }


def _reject_duplicate_worker_accounts(accounts: list[dict]) -> None:
    """Reject identities that would resolve to the same remote pool slot."""
    seen: set[tuple[str, str]] = set()
    seen_slots: set[tuple[str, str]] = set()
    for account in accounts:
        provider = str(account.get("provider") or "claude").lower()
        identity = (
            provider,
            str(account.get("email") or "").strip().casefold(),
        )
        if identity in seen:
            raise HTTPException(
                400,
                f"重复的 Worker 账号: {account.get('email')} ({identity[0]})",
            )
        seen.add(identity)
        account_id = str(account.get("account_id") or "").strip()
        if account_id:
            slot = (provider, account_id)
            if slot in seen_slots:
                raise HTTPException(
                    400,
                    f"重复的 Worker 账号槽位: {account_id} ({provider})",
                )
            seen_slots.add(slot)


def _build_add_account_command(
    remote_dir: str,
    *,
    email: str,
    token: str,
    slot: str,
    login_method: str,
) -> str:
    """Build the remote login command with every dynamic argv shell-quoted."""
    argv = [
        "xvfb-run",
        "--auto-servernum",
        "--server-args=-screen 0 1920x1080x24",
        "uv",
        "run",
        "python",
        "scripts/auto_login.py",
        "--email",
        email,
        "--token",
        token,
        "--add-to-pool",
        slot,
        "--save-token",
    ]
    if login_method:
        argv.extend(["--login-method", login_method])
    return (
        f"cd {shlex.quote(remote_dir)} && "
        'export PATH="$HOME/.local/bin:$PATH" && '
        f"{shlex.join(argv)}"
    )


def _remove_persisted_worker_account(
    accounts: list | None,
    *,
    provider: str,
    account_id: str,
) -> tuple[list, bool]:
    """Remove a remotely deleted account from bootstrap retry credentials.

    New records persist ``account_id``.  Historical Claude-only records did
    not, so reconstruct their deterministic legacy slots as a compatibility
    fallback.  Codex never had historical provider-less Worker records.
    """
    kept: list = []
    removed = False
    provider_index = 0
    for account in accounts or []:
        if not isinstance(account, dict):
            kept.append(account)
            continue
        account_provider = str(account.get("provider") or "claude").lower()
        inferred_id = None
        if account_provider == provider:
            provider_index += 1
            if provider == "claude":
                inferred_id = (
                    "default" if provider_index == 1
                    else f"account-{provider_index}"
                )
        persisted_id = account.get("account_id") or inferred_id
        if (
            not removed
            and account_provider == provider
            and persisted_id == account_id
        ):
            removed = True
            continue
        if inferred_id and not account.get("account_id"):
            # Freeze legacy positional slots while the full original ordering
            # is still available.  Otherwise deleting ``default`` makes the
            # old ``account-2`` look like default on the next request.
            kept.append({**account, "account_id": inferred_id})
        else:
            kept.append(account)
    return kept, removed


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def _worker_http_request(
    worker: Worker,
    method: str,
    path: str,
    *,
    timeout: float,
    payload: object = _NO_WORKER_JSON,
    allow_statuses: frozenset[int] = frozenset(),
    client: httpx.AsyncClient | None = None,
):
    """Call a Worker without leaking its auth/upstream errors to the client.

    A Worker bearer token is an internal Manager-to-Worker credential.  In
    particular, forwarding an upstream 401 would make the frontend treat the
    *Manager* session as expired and clear the user's Manager token.
    """
    if not worker.private_ip:
        raise HTTPException(502, "Worker 网关缺少目标地址")
    url = f"http://{worker.private_ip}:{worker.ccm_port}{path}"
    kwargs: dict = {
        "headers": {"Authorization": f"Bearer {worker.auth_token}"},
    }
    if payload is not _NO_WORKER_JSON:
        kwargs["json"] = payload

    async def _send(active_client):
        sender = getattr(active_client, method.lower())
        return await sender(url, **kwargs)

    try:
        if client is None:
            async with httpx.AsyncClient(timeout=timeout) as active_client:
                response = await _send(active_client)
        else:
            response = await _send(client)
    except (httpx.RequestError, OSError, TimeoutError) as exc:
        raise HTTPException(
            502,
            f"Worker 网关连接失败: {type(exc).__name__}: {str(exc)[:200]}",
        ) from exc

    status_code = response.status_code
    if status_code in _WORKER_AUTH_FAILURE_STATUSES:
        raise HTTPException(
            502,
            f"Worker 认证失败（远端 HTTP {status_code}），请重试 Worker 引导以同步认证凭据",
        )
    if not 200 <= status_code < 300 and status_code not in allow_statuses:
        raise HTTPException(
            502,
            f"Worker 上游请求失败（远端 HTTP {status_code}）",
        )
    return response


def _worker_response_json(response) -> object:
    """Decode a Worker response or surface malformed upstream data as 502."""
    try:
        return response.json()
    except (TypeError, ValueError) as exc:
        raise HTTPException(502, "Worker 上游返回了无效 JSON") from exc


async def _persist_worker_account_state(
    provisioner,
    worker_id: int,
    account: dict,
    *,
    status: str,
    account_id: str | None = None,
) -> None:
    """Upsert login intent/result so process restarts cannot lose credentials."""
    async with _worker_account_store_lock:
        async with provisioner.db_factory() as db:
            worker = await db.get(Worker, worker_id)
            if worker is None:
                raise RuntimeError("Worker record disappeared after account login")
            if worker.status in {"destroying", "terminated"}:
                # A late browser callback must never repopulate credentials
                # after destroy has scrubbed them.  This also closes the race
                # where /pool/add read ready immediately before destroy CAS.
                raise RuntimeError(
                    f"Worker account persistence rejected while {worker.status}"
                )
            provider = account["provider"]
            updated_accounts = [
                item for item in (worker.accounts or [])
                if not (
                    isinstance(item, dict)
                    and str(item.get("provider") or "claude").lower() == provider
                    and (
                        (account_id and item.get("account_id") == account_id)
                        or (
                            str(item.get("email") or "").strip().casefold()
                            == account["email"].casefold()
                        )
                    )
                )
            ]
            updated_accounts.append({
                **account,
                **({"account_id": account_id} if account_id else {}),
                "status": status,
            })
            # End the snapshot read transaction, then make status gating and
            # the JSON write one SQL statement.  A destroy CAS/credential
            # scrub that wins between the read and write must make rowcount 0;
            # a stale login callback can never update a terminated row.
            await db.rollback()
            persisted = await db.execute(
                update(Worker)
                .where(
                    Worker.id == worker_id,
                    Worker.status.not_in(("destroying", "terminated")),
                )
                .values(accounts=updated_accounts)
            )
            if persisted.rowcount != 1:
                await db.rollback()
                current_status = await db.scalar(
                    select(Worker.status).where(Worker.id == worker_id)
                )
                raise RuntimeError(
                    "Worker account persistence rejected while "
                    f"{current_status or 'missing'}"
                )
            await db.commit()


def _provisioner():
    from backend.main import worker_provisioner

    if worker_provisioner is None:
        raise HTTPException(503, "Worker 功能未启用（WORKER_ENABLED=false 或缺少 boto3）")
    return worker_provisioner


@router.get("", response_model=list[WorkerResponse])
async def list_workers(request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import get_current_user_id, get_current_user_role
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    stmt = select(Worker).where(Worker.status != "terminated").order_by(desc(Worker.created_at))
    if user_role not in ("admin", "super_admin"):
        stmt = stmt.where(Worker.owner_user_id == user_id)
    result = await db.execute(stmt)
    return result.scalars().all()


@router.post("", response_model=WorkerResponse)
async def create_worker(body: WorkerCreate, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_admin
    require_admin(request)
    prov = _provisioner()
    if not body.name or not body.name.strip():
        raise HTTPException(400, "请填写 Worker 名称")
    # Fail before creating a DB job or a billable EC2 instance.  The same
    # preflight is repeated inside the background provisioner to close races
    # where a key is replaced between request validation and instance launch.
    from backend.services.ssh_executor import SSHKeyPreflightError
    try:
        prov.preflight_ssh_key()
    except SSHKeyPreflightError as exc:
        raise HTTPException(
            503,
            f"Worker SSH 密钥配置无效（{exc.code}）：{exc.detail}",
        ) from exc
    accounts = []
    for account in body.accounts:
        accounts.append(_normalize_worker_account(
            email=account.email,
            provider=account.provider,
            token=account.token,
            password=account.password,
            login_method=account.login_method,
            require_unattended=True,
        ))
    _reject_duplicate_worker_accounts(accounts)
    worker = Worker(
        name=body.name.strip(),
        status="creating",
        auth_token=secrets.token_hex(24),
        ssh_user=settings.worker_ssh_user,
        ssh_key_path=settings.worker_ssh_key_path,
        accounts=[{**account, "status": "pending"} for account in accounts],
    )
    db.add(worker)
    await db.commit()
    await db.refresh(worker)

    _spawn(
        prov.create_worker(worker.id, accounts=accounts)
    )
    return worker


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    return worker


@router.get("/{worker_id}/logs", response_model=WorkerLogsResponse)
async def get_worker_logs(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    return WorkerLogsResponse(id=worker.id, bootstrap_log=worker.bootstrap_log)


async def _transition_worker_status(
    db: AsyncSession,
    worker_id: int,
    *,
    allowed_statuses: tuple[str, ...] | frozenset[str],
    target_status: str,
    block_active_task_terminations: bool = False,
    destroy_recovery: bool = False,
) -> Worker:
    loop = asyncio.get_running_loop()
    lock_key = (loop, worker_id)
    entry = _worker_lifecycle_transition_locks.setdefault(
        lock_key, _WorkerLifecycleTransitionLock(),
    )
    entry.users += 1
    try:
        async with entry.lock:
            return await _transition_worker_status_locked(
                db,
                worker_id,
                allowed_statuses=allowed_statuses,
                target_status=target_status,
                block_active_task_terminations=block_active_task_terminations,
                destroy_recovery=destroy_recovery,
            )
    finally:
        entry.users -= 1
        if (
            entry.users == 0
            and _worker_lifecycle_transition_locks.get(lock_key) is entry
        ):
            _worker_lifecycle_transition_locks.pop(lock_key, None)


async def _transition_worker_status_locked(
    db: AsyncSession,
    worker_id: int,
    *,
    allowed_statuses: tuple[str, ...] | frozenset[str],
    target_status: str,
    block_active_task_terminations: bool = False,
    destroy_recovery: bool = False,
) -> Worker:
    """Atomically claim a Worker lifecycle transition.

    Routes perform authorization from a read first.  End that read transaction
    before the compare-and-set so concurrent SQLite requests do not both try to
    upgrade a shared read lock.  Only the UPDATE winner may spawn background
    lifecycle work.
    """
    await db.rollback()
    task_ids: list[int] = []
    if block_active_task_terminations:
        from backend.models.task import Task
        from backend.services.worker_task_termination import (
            active_worker_task_termination_receipt,
        )

        # Receipt admission and this destroy claim share the Task write lock.
        # SELECT FOR UPDATE covers PostgreSQL/MySQL; the exact no-op UPDATE is
        # the corresponding SQLite/MySQL CAS barrier.  Check only after those
        # locks so a concurrently committed receipt cannot be crossed by the
        # Worker lifecycle transition.
        task_ids = list(
            (
                await db.execute(
                    select(Task.id)
                    .where(Task.worker_id == worker_id)
                    .order_by(Task.id)
                    .with_for_update()
                )
            ).scalars()
        )
        await db.execute(
            update(Task)
            .where(Task.worker_id == worker_id)
            .values(status=Task.status)
        )
    worker_predicates = [
        Worker.id == worker_id,
        Worker.status.in_(tuple(allowed_statuses)),
    ]
    if block_active_task_terminations:
        # A restart recovery is a narrower lifecycle than an ordinary destroy:
        # it may resume the exact Manager stop receipt admitted by the previous
        # claim, but only while the durable restart marker still identifies an
        # interrupted destroy.  Conversely, an ordinary destroy must not race
        # across a row which became a recovery lifecycle after its initial read.
        if destroy_recovery:
            worker_predicates.extend(
                (Worker.status == "error", Worker.bootstrap_step == "destroy")
            )
        else:
            worker_predicates.append(
                or_(
                    Worker.bootstrap_step.is_(None),
                    Worker.bootstrap_step != "destroy",
                )
            )
    result = await db.execute(
        update(Worker)
        .where(*worker_predicates)
        .values(status=target_status)
    )
    if result.rowcount != 1:
        await db.rollback()
        current_status = await db.scalar(
            select(Worker.status).where(Worker.id == worker_id)
        )
        if current_status is None:
            raise HTTPException(404, "Worker not found")
        raise HTTPException(
            409,
            f"Worker 当前状态 {current_status}，不允许该操作",
        )
    if block_active_task_terminations:
        # Plan/Run writers take this same Worker row as their admission fence.
        # Once the CAS above owns ``destroying``, no new Worker Plan generation
        # can commit. Historical terminal rows are audit, not runtime owners;
        # only active/unarchived/native-runtime evidence blocks destruction.
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
        if plan_rows or run_rows:
            await db.rollback()
            raise HTTPException(
                409,
                _worker_plan_ownership_block_detail(plan_rows, run_rows),
            )

        # Global cross-process order is Task -> Worker -> receipt.  The Worker
        # status change remains uncommitted until every receipt has been
        # checked; a blocker rolls the lifecycle claim back atomically.
        blocked_task_id = None
        for task_id in task_ids:
            receipt = await active_worker_task_termination_receipt(
                db,
                task_id,
                for_update=True,
            )
            if receipt is None:
                continue
            if destroy_recovery and (
                receipt.side == "manager"
                and receipt.worker_id == worker_id
                and receipt.operation == "stop_session"
                and receipt.status in {"pending_remote", "awaiting_ack"}
            ):
                continue
            blocked_task_id = task_id
            break
        if blocked_task_id is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker destroy is blocked by active Task termination "
                f"receipt on Task {blocked_task_id}",
            )
    await db.commit()
    worker = await db.get(Worker, worker_id)
    if worker is None:  # Defensive: the row cannot normally disappear here.
        raise HTTPException(404, "Worker not found")
    await db.refresh(worker)
    return worker


async def _worker_plan_runtime_blockers(
    db: AsyncSession,
    worker_id: int,
) -> tuple[
    list[tuple[int, int | None, int | None]],
    list[tuple[int, int | None, str, int | None]],
]:
    """Return active or unclean first-class Plan evidence for one Worker."""

    from backend.main import dispatcher
    from backend.models.instance import Instance
    from backend.models.plan import Plan
    from backend.models.plan_agent import (
        PlanAgentRun,
        PlanAgentWorkerDispatchReceipt,
    )
    from backend.services.plan_agent_runner import active_plan_run_ids
    from backend.services.worker_plan_dispatch import (
        WorkerPlanDispatchConflict,
        snapshot_worker_dispatch_receipt,
        worker_mirror_run_is_clean,
    )

    worker_runs = list(
        (
            await db.execute(
                select(
                    PlanAgentRun.id,
                    PlanAgentRun.plan_id,
                    PlanAgentRun.status,
                    PlanAgentRun.instance_id,
                )
                .where(PlanAgentRun.worker_id == worker_id)
                .order_by(PlanAgentRun.id)
            )
        ).all()
    )
    worker_run_ids = {int(row.id) for row in worker_runs}
    unclean_run_ids: set[int] = set()
    for run_id in sorted(worker_run_ids):
        # A status string is not cleanup proof. Manager-side Worker Runs own
        # remote Step mirrors, so validate their exact dispatch history and
        # reject any contradictory local provider-runtime receipt.
        if not await worker_mirror_run_is_clean(db, run_id=run_id):
            unclean_run_ids.add(run_id)

    # Dispatch ownership is frozen on the receipt.  Query it directly instead
    # of reaching it only through the Run's current worker_id: a Run may have
    # drifted, been detached, or been lost while the old Worker still owns an
    # uncertain remote boundary.
    dispatch_receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.worker_id == worker_id)
                .order_by(PlanAgentWorkerDispatchReceipt.id)
            )
        ).scalars()
    )
    dispatch_run_ids = {int(receipt.run_id) for receipt in dispatch_receipts}
    dispatch_runs = (
        {
            int(row.id): row
            for row in (
                await db.execute(
                    select(
                        PlanAgentRun.id,
                        PlanAgentRun.plan_id,
                        PlanAgentRun.status,
                        PlanAgentRun.instance_id,
                        PlanAgentRun.worker_id,
                        PlanAgentRun.generation,
                    ).where(PlanAgentRun.id.in_(sorted(dispatch_run_ids)))
                )
            ).all()
        }
        if dispatch_run_ids
        else {}
    )
    dispatch_plan_ids = {int(receipt.plan_id) for receipt in dispatch_receipts}
    dispatch_plans = (
        {
            int(row.id): row
            for row in (
                await db.execute(
                    select(
                        Plan.id,
                        Plan.target_task_id,
                        Plan.worker_id,
                    ).where(Plan.id.in_(sorted(dispatch_plan_ids)))
                )
            ).all()
        }
        if dispatch_plan_ids
        else {}
    )
    detached_dispatch_blockers: list[
        tuple[int, int | None, str, int | None]
    ] = []
    for receipt in dispatch_receipts:
        # Complete receipt history for Runs still owned by this Worker was
        # validated above, including historical settled generations.  This
        # second pass exists to catch frozen receipts whose Run/Plan drifted
        # away from the Worker and must remain fail-closed.
        if receipt.run_id in worker_run_ids:
            continue
        run = dispatch_runs.get(int(receipt.run_id))
        plan = dispatch_plans.get(int(receipt.plan_id))
        try:
            snapshot = snapshot_worker_dispatch_receipt(receipt)
            valid_shape = True
        except WorkerPlanDispatchConflict:
            snapshot = None
            valid_shape = False
        exact_identity = bool(
            run is not None
            and plan is not None
            and run.plan_id == receipt.plan_id
            and run.worker_id == receipt.worker_id
            and run.generation == receipt.run_generation
            and plan.worker_id == receipt.worker_id
            and plan.target_task_id == receipt.target_task_id
        )
        if (
            valid_shape
            and snapshot is not None
            and snapshot.status == "settled"
            and exact_identity
        ):
            continue
        blocker_status = (
            f"dispatch:{receipt.status}"
            if valid_shape
            else f"dispatch:malformed-{receipt.status}"
        )
        detached_dispatch_blockers.append(
            (
                int(receipt.run_id),
                int(receipt.plan_id),
                blocker_status,
                run.instance_id if run is not None else None,
            )
        )
    reverse_owner_run_ids = (
        set(
            (
                await db.execute(
                    select(Instance.current_plan_run_id).where(
                        Instance.current_plan_run_id.in_(worker_run_ids)
                    )
                )
            ).scalars()
        )
        if worker_run_ids
        else set()
    )
    live_run_ids = set(active_plan_run_ids())
    if dispatcher is not None:
        for lifecycle in getattr(dispatcher, "_running_tasks", {}).values():
            if lifecycle.done():
                continue
            for attribute in ("_ccm_plan_run_id", "_ccm_worker_plan_run_id"):
                run_id = getattr(lifecycle, attribute, None)
                if type(run_id) is int:
                    live_run_ids.add(run_id)

    terminal_statuses = {"completed", "failed", "cancelled"}
    run_rows = [
        tuple(row)
        for row in worker_runs
        if row.status not in terminal_statuses
        or row.instance_id is not None
        or row.id in unclean_run_ids
        or row.id in reverse_owner_run_ids
        or row.id in live_run_ids
    ]
    run_rows.extend(detached_dispatch_blockers)
    run_rows.sort(key=lambda row: (row[0], row[2]))
    plan_rows = [
        tuple(row)
        for row in (
            await db.execute(
                select(Plan.id, Plan.target_task_id, Plan.active_run_id)
                .where(
                    Plan.worker_id == worker_id,
                    or_(
                        Plan.archived_at.is_(None),
                        Plan.active_run_id.is_not(None),
                    ),
                )
                .order_by(Plan.id)
            )
        ).all()
    ]
    return plan_rows, run_rows


def _worker_plan_ownership_block_detail(
    plan_rows: list[tuple[int, int | None, int | None]],
    run_rows: list[tuple[int, int | None, str, int | None]],
) -> str:
    parts: list[str] = []
    if plan_rows:
        parts.append(
            "Plans "
            + ", ".join(
                f"{plan_id}(task={target_task_id}, active_run={active_run_id})"
                for plan_id, target_task_id, active_run_id in plan_rows[:20]
            )
        )
    if run_rows:
        parts.append(
            "Plan Runs "
            + ", ".join(
                f"{run_id}(plan={plan_id}, status={status}, instance={instance_id})"
                for run_id, plan_id, status, instance_id in run_rows[:20]
            )
        )
    return (
        "Worker destroy is blocked by active first-class Plan runtime: "
        + "; ".join(parts)
        + ". Cancel active Runs and archive inactive Plans before retrying."
    )


@router.post("/{worker_id}/stop", response_model=WorkerResponse)
async def stop_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    if worker.bootstrap_step == "destroy":
        raise HTTPException(409, "Worker 有未完成的销毁操作，只能重试销毁")
    prov = _provisioner()
    worker = await _transition_worker_status(
        db,
        worker_id,
        allowed_statuses=("ready", "error"),
        target_status="stopping",
    )
    _spawn(prov.stop_worker(worker.id))
    return worker


@router.post("/{worker_id}/start", response_model=WorkerResponse)
async def start_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    if worker.bootstrap_step == "destroy":
        raise HTTPException(409, "Worker 有未完成的销毁操作，只能重试销毁")
    prov = _provisioner()
    worker = await _transition_worker_status(
        db,
        worker_id,
        allowed_statuses=("stopped", "error"),
        target_status="starting",
    )
    _spawn(prov.start_worker(worker.id))
    return worker


@router.post("/{worker_id}/destroy", response_model=WorkerResponse)
async def destroy_worker(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_admin
    from backend.services.worker_proxy import (
        capture_worker_destroy_lifecycle_claim,
    )

    require_admin(request)
    prov = _provisioner()
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    destroy_recovery = (
        worker.status == "error" and worker.bootstrap_step == "destroy"
    )
    worker = await _transition_worker_status(
        db,
        worker_id,
        allowed_statuses=_WORKER_DESTROYABLE_STATUSES,
        target_status="destroying",
        block_active_task_terminations=True,
        destroy_recovery=destroy_recovery,
    )
    destroy_claim = capture_worker_destroy_lifecycle_claim(worker)
    # 先把该 worker 的 task 全部迁回本机（执行态无损），再销毁实例
    _spawn(_migrate_back_then_destroy(prov, worker.id, destroy_claim))
    return worker


async def _migrate_back_then_destroy(
    prov,
    worker_id: int,
    destroy_claim,
    db_factory=None,
):
    """销毁 = 批量 migrate(task, 本机) + terminate（设计 §10.3）。

    单个 inert task 迁移失败时仍保留旧的可损脱钩降级（并写入
    task.error_message）。任何非 inert 状态、未收敛的 Worker turn
    handoff 或 durable execution quarantine 都必须保留 Worker 路由；该
    Worker 恢复 ready 以继续对账，云实例不得销毁。"""
    from backend.main import task_migrator, worker_relay
    from backend.api.tasks import _stop_worker_task_for_destroy
    from backend.models.task import Task
    from backend.services.worker_proxy import WorkerProxy
    from sqlalchemy import select

    if db_factory is None:
        from backend.database import async_session as db_factory

    if destroy_claim.worker_id != worker_id:
        raise ValueError("Worker destroy claim does not match its coordinator")
    destroy_proxy = WorkerProxy(db_factory, worker_relay)
    try:
        await destroy_proxy._require_destroy_lifecycle_claim(destroy_claim)
    except Exception as e:
        detail = f"Worker 销毁已拒绝：destroy lifecycle claim 已失效（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # Admission and background coordination are separate transactions. A Plan
    # may have committed just after the HTTP transition's snapshot, so repeat
    # the fail-closed ownership check before mutating or migrating any Task.
    async with db_factory() as db:
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
    if plan_rows or run_rows:
        detail = _worker_plan_ownership_block_detail(plan_rows, run_rows)
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return

    # TaskMigrator 已接受 destroying 状态作为迁移源，无需临时改 ready
    async with db_factory() as db:
        result = await db.execute(select(Task).where(Task.worker_id == worker_id))
        tasks = result.scalars().all()
    # Resume the exact stop receipt for every Task before migration.  The helper
    # is a no-op for an inert Task without a receipt, while terminal Tasks with
    # an awaiting ACK still need this call to finish durable reconciliation.
    for task in tasks:
        try:
            async with db_factory() as db:
                await _stop_worker_task_for_destroy(
                    task.id,
                    destroy_claim,
                    destroy_proxy,
                    db,
                )
            logger.info("destroy: settled task %s before migration", task.id)
        except Exception as e:
            logger.warning("destroy: failed to settle task %s: %s", task.id, e)
    # Refresh task statuses after stopping
    async with db_factory() as db:
        result = await db.execute(select(Task).where(Task.worker_id == worker_id))
        tasks = result.scalars().all()
    for task in tasks:
        try:
            if task_migrator is None:
                raise RuntimeError("Task migrator is unavailable")
            await task_migrator.migrate(task.id, None)

            # A successful return must have cut the source pointer.  Never let
            # a buggy/mixed-version migrator response become permission to
            # terminate the only Worker still named by the durable Task row.
            async with db_factory() as db:
                remaining_worker_id = await db.scalar(
                    select(Task.worker_id).where(Task.id == task.id)
                )
            if remaining_worker_id == worker_id:
                raise RuntimeError(
                    "Task migration returned without changing Worker ownership"
                )
        except Exception as e:
            logger.warning("destroy: migrate task %s back failed: %s", task.id, e)
            detached, block_reason = (
                await _fallback_detach_after_destroy_migration_failure(
                    db_factory,
                    task_id=task.id,
                    worker_id=worker_id,
                    error=e,
                )
            )
            if not detached:
                reason = block_reason or (
                    f"Task {task.id} 仍保留远程执行证据"
                )
                detail = f"Worker 销毁已拒绝：{reason}"
                logger.error("destroy: worker %s blocked: %s", worker_id, detail)
                await _mark_worker_destroy_blocked(
                    db_factory,
                    destroy_claim=destroy_claim,
                    detail=detail,
                )
                return

    # Final durable gate: normal assignment rejects a ``destroying`` target,
    # but an older process or a failed fallback must still not strand a Task on
    # an instance we are about to terminate.
    async with db_factory() as db:
        remaining = list(
            (
                await db.execute(
                    select(Task.id, Task.status).where(
                        Task.worker_id == worker_id
                    )
                )
            ).all()
        )
        plan_rows, run_rows = await _worker_plan_runtime_blockers(db, worker_id)
    if remaining or plan_rows or run_rows:
        blockers: list[str] = []
        if remaining:
            blockers.append(
                "Tasks "
                + ", ".join(
                    f"{task_id}:{status}" for task_id, status in remaining[:20]
                )
            )
        if plan_rows or run_rows:
            blockers.append(
                _worker_plan_ownership_block_detail(plan_rows, run_rows)
            )
        detail = "Worker 销毁已拒绝：仍有持久所有权指向该 Worker（" + "; ".join(blockers) + "）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    try:
        await destroy_proxy._require_destroy_lifecycle_claim(destroy_claim)
    except Exception as e:
        detail = f"Worker 销毁已拒绝：destroy lifecycle claim 已失效（{e}）"
        logger.error("destroy: worker %s blocked: %s", worker_id, detail)
        await _mark_worker_destroy_blocked(
            db_factory,
            destroy_claim=destroy_claim,
            detail=detail,
        )
        return
    if worker_relay is not None:
        try:
            await worker_relay.stop_worker(worker_id)
        except Exception as e:
            # Relay is Manager-local cleanup.  A stale relay must not prevent
            # the cloud termination attempt or strand the row in destroying.
            logger.warning("destroy: stop worker relay %s failed: %s", worker_id, e)
    await prov.destroy_worker(worker_id)


async def _fallback_detach_after_destroy_migration_failure(
    db_factory,
    *,
    task_id: int,
    worker_id: int,
    error: Exception,
) -> tuple[bool, str | None]:
    """Apply the legacy lossy fallback only to a proven inert mirror.

    ``TaskMigrator`` and Manager→Worker mutations share this operation lock.
    Re-reading the row under that lock lets an exact relay reconciliation which
    already cleared the marker win, while preventing a new handoff from being
    installed between this decision and the detach write.  Active/queued
    generations and uncertain remote termination outcomes retain ``worker_id``
    so cloud destruction remains fail-closed.
    """
    from backend.models.task import Task
    from backend.services.worker_proxy import get_task_operation_lock
    from backend.services.worker_relay import (
        has_worker_execution_quarantine,
    )
    from backend.services.worker_task_termination import (
        active_worker_task_termination_receipt,
        no_active_worker_task_termination_predicate,
    )
    from backend.services.worker_routing_config import (
        WORKER_ROUTING_SAFE_STATUSES,
    )

    async with get_task_operation_lock(task_id):
        async with db_factory() as db:
            current = (
                await db.execute(
                    select(Task)
                    .where(Task.id == task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current is None or current.worker_id != worker_id:
                await db.rollback()
                return True, None
            if current.status not in WORKER_ROUTING_SAFE_STATUSES:
                status = current.status
                await db.rollback()
                return False, (
                    f"Task {task_id} 仍处于非 inert 状态 {status}；"
                    "远程执行结果尚未可证明"
                )
            if current.worker_turn_handoff_id is not None:
                handoff_id = current.worker_turn_handoff_id
                await db.rollback()
                return False, (
                    f"Task {task_id} 仍有未收敛的 turn handoff "
                    f"{handoff_id}；请等待 exact remote outcome 同步后重试"
                )
            if has_worker_execution_quarantine(current.metadata_):
                await db.rollback()
                return False, (
                    f"Task {task_id} 的 Worker execution 仍处于 quarantine；"
                    "必须先对账 exact remote generation"
                )

            if await active_worker_task_termination_receipt(db, task_id):
                await db.rollback()
                return False, (
                    f"Task {task_id} 仍有 active Worker termination receipt；"
                    "必须先完成 durable termination reconciliation"
                )

            detached = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.worker_id == worker_id,
                    Task.status == current.status,
                    Task.retry_count == current.retry_count,
                    Task.turn_generation == current.turn_generation,
                    Task.worker_turn_handoff_id.is_(None),
                    no_active_worker_task_termination_predicate(),
                )
                .values(
                    worker_id=None,
                    error_message=(
                        (current.error_message or "")
                        + f"\n[销毁迁移失败: {error}]"
                    ),
                )
            )
            if detached.rowcount != 1:
                await db.rollback()
                return False, (
                    f"Task {task_id} 在销毁 fallback gate 期间取得新的 "
                    "termination/turn generation；保留远程 Worker 路由"
                )
            await db.commit()
            return True, None


async def _mark_worker_destroy_blocked(
    db_factory,
    *,
    destroy_claim,
    detail: str,
) -> None:
    """Restore relay eligibility without overwriting a newer lifecycle state.

    Handoff recovery deliberately runs only for ``ready`` Workers.  Leaving a
    blocked destroy in ``error`` (especially with ``bootstrap_step=destroy``)
    would therefore make the marker impossible to settle and every retry would
    fail on the same marker.  Keep the visible error text, but return the live
    Worker to the normal recovery state so a later destroy can succeed.
    """
    from backend.services.worker_proxy import (
        _worker_destroy_lifecycle_predicates,
    )

    async with db_factory() as db:
        result = await db.execute(
            update(Worker)
            .where(*_worker_destroy_lifecycle_predicates(destroy_claim))
            .values(
                status="ready",
                bootstrap_step=None,
                bootstrap_error=detail[:2000],
            )
        )
        if result.rowcount == 1:
            await db.commit()
        else:
            await db.rollback()


@router.post("/{worker_id}/retry", response_model=WorkerResponse)
async def retry_bootstrap(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    """error 状态下重跑创建/bootstrap 流程。"""
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if worker:
        await require_worker_access(request, worker)
    prov = _provisioner()
    if worker is None:
        raise HTTPException(404, "Worker not found")
    if worker.status != "error":
        raise HTTPException(
            409,
            f"Worker 当前状态 {worker.status}，不允许该操作",
        )
    if worker.bootstrap_step == "destroy":
        raise HTTPException(409, "Worker 有未完成的销毁操作，只能重试销毁")
    # 从 DB 读已有账号信息，retry 时重新登录。历史记录没有
    # provider，它们均由旧 Claude-only Worker 链路创建。
    saved_accounts = worker.accounts or []
    accounts = []
    for account in saved_accounts:
        email = str(account.get("email", "")).strip()
        if not email:
            raise HTTPException(409, "Worker 保存的账号缺少 email，无法重试")
        try:
            provider = _normalize_worker_account_provider(
                account.get("provider") or "claude"
            )
            token = account.get("token") or ""
            password = account.get("password") or ""
            if not isinstance(token, str) or not isinstance(password, str):
                raise HTTPException(400, "保存的账号凭据格式无效")
            normalized = _normalize_worker_account(
                email=email,
                provider=provider,
                token=token,
                password=password,
                login_method=account.get("login_method"),
                require_unattended=True,
            )
            account_id = account.get("account_id") or ""
            if not isinstance(account_id, str):
                raise HTTPException(400, "保存的账号 account_id 格式无效")
            if account_id.strip():
                normalized["account_id"] = account_id.strip()
        except HTTPException as exc:
            raise HTTPException(
                409,
                f"账号 {email} 的保存登录信息无效，无法重试：{exc.detail}",
            ) from exc
        accounts.append(normalized)
    try:
        _reject_duplicate_worker_accounts(accounts)
    except HTTPException as exc:
        raise HTTPException(409, f"Worker 保存了重复账号，无法重试：{exc.detail}") from exc
    worker = await _transition_worker_status(
        db,
        worker_id,
        allowed_statuses=("error",),
        target_status="creating",
    )
    _spawn(
        prov.create_worker(worker.id, accounts=accounts)
    )
    return worker


@router.get("/{worker_id}/pool")
async def get_worker_pool(
    worker_id: int,
    request: Request,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    """实时拉取 Worker 上指定 provider 的账号池状态。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    provider = _normalize_worker_account_provider(provider)
    status_path = (
        "/api/codex-pool/status" if provider == "codex" else "/api/pool/status"
    )
    r = await _worker_http_request(
        worker,
        "GET",
        status_path,
        timeout=10,
        allow_statuses=frozenset({404}) if provider == "claude" else frozenset(),
    )
    if provider == "claude" and r.status_code == 404:
        # worker 端 POOL_ENABLED=false：单账号模式。
        # 老版 worker 没有账号查询端点，经 SSH 读 ~/.claude.json
        # 的 oauthAccount.emailAddress 兜底，让用户知道用的是哪个号
        email = None
        try:
            from backend.services.ssh_executor import (
                SSHExecutor,
                worker_known_hosts_path,
            )
            ssh = SSHExecutor(
                host=worker.private_ip,
                user=worker.ssh_user,
                key_path=(worker.ssh_key_path or settings.worker_ssh_key_path),
                known_hosts_path=(
                    worker_known_hosts_path(worker.cloud_instance_id)
                    if worker.cloud_instance_id else None
                ),
            )
            code, out = await ssh.run(
                "python3 -c \"import json;"
                "print(json.load(open('/home/'+__import__('getpass').getuser()+'/.claude.json'))"
                ".get('oauthAccount',{}).get('emailAddress',''))\"",
                timeout=15,
            )
            if code == 0 and out.strip():
                email = out.strip().splitlines()[-1]
        except Exception:
            email = None
        accounts = (
            [{"id": "default", "email": email, "enabled": True,
              "available": True, "cooldown_remaining": 0}]
            if email else []
        )
        return {"enabled": True, "total": len(accounts),
                "available": len(accounts), "accounts": accounts}
    return _worker_response_json(r)


@router.post("/{worker_id}/pool/add")
async def add_worker_account(worker_id: int, request: Request, body: dict, db: AsyncSession = Depends(get_db)):
    """在 Worker 上添加 Codex（默认）或兼容 Claude 账号。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")

    raw_email = body.get("email", "")
    raw_token = body.get("token", "")
    raw_password = body.get("password", "")
    raw_provider = body.get("provider", "codex")
    if not all(
        isinstance(value, str)
        for value in (raw_email, raw_token, raw_password, raw_provider)
    ):
        raise HTTPException(400, "email/provider/token/password 必须是字符串")
    account = _normalize_worker_account(
        email=raw_email,
        provider=raw_provider,
        token=raw_token,
        password=raw_password,
        login_method=body.get("login_method"),
        require_unattended=True,
    )
    email = account["email"]
    provider = account["provider"]

    # Email identity is case-insensitive.  Normalize the in-memory admission
    # key as well as the persisted lookup so differently-cased concurrent
    # requests cannot start two browser logins for the same account.
    state_key = f"{worker_id}:{provider}:{email.casefold()}"

    if provider == "codex":
        prov = _provisioner()
        async with _worker_login_admission_lock:
            existing_state = _worker_login_state.get(state_key, {})
            if existing_state.get("status") in _WORKER_ACTIVE_LOGIN_STATUSES:
                return {
                    "ok": True,
                    "provider": provider,
                    **{
                        key: existing_state[key]
                        for key in (
                            "status", "attempt_id", "challenge_id",
                            "expires_at", "account_id",
                        )
                        if existing_state.get(key) is not None
                    },
                }
            async with prov.db_factory() as account_db:
                current_worker = await account_db.get(Worker, worker_id)
            if current_worker is None:
                raise HTTPException(404, "Worker not found")
            persisted_matches = [
                item for item in (current_worker.accounts or [])
                if isinstance(item, dict)
                and str(item.get("provider") or "claude").lower() == provider
                and str(item.get("email") or "").strip().casefold()
                == email.casefold()
            ]
            if len(persisted_matches) > 1:
                raise HTTPException(409, "Manager 中存在重复的 Worker 账号记录，请先清理")
            if persisted_matches:
                persisted = persisted_matches[0]
                persisted_status = str(persisted.get("status") or "")
                if persisted_status == "logged_in":
                    raise HTTPException(409, "该 Codex 邮箱已在 Worker 号池中")
                if persisted_status == "pending":
                    # Resume an intent that survived Manager restart without
                    # replacing its known-good credentials from an add form.
                    account = dict(persisted)
                elif persisted.get("account_id"):
                    # A failed slot is an explicit retry: retain its identity
                    # while allowing corrected credentials from this request.
                    account["account_id"] = persisted["account_id"]
            _worker_login_state[state_key] = {
                "status": "running",
                "provider": provider,
                "started_at": time.time(),
            }
            # Persist the intent before starting the long remote browser flow.
            # A Manager restart can then reclaim the active/committed slot.
            try:
                await _persist_worker_account_state(
                    prov,
                    worker_id,
                    account,
                    status="pending",
                )
            except Exception:
                _worker_login_state.pop(state_key, None)
                raise

        async def _publish_codex_status(remote_state: dict) -> None:
            current = _worker_login_state.get(state_key, {})
            safe = {
                key: remote_state[key]
                for key in (
                    "status", "detail", "attempt_id", "challenge_id",
                    "expires_at", "account_id",
                )
                if remote_state.get(key) is not None
            }
            # No remote terminal status is the Manager transaction boundary:
            # credentials/account_id or retryable failure still need to commit
            # to Worker.accounts.  Keep DELETE/retry blocked until _run_codex
            # performs the final DB write and publishes the sole terminal
            # state.  This includes unexpected/idle remote states because
            # ensure_codex_account raises only after this callback returns.
            remote_status = safe.get("status")
            if (
                remote_status is not None
                and remote_status not in _WORKER_ACTIVE_LOGIN_STATUSES
            ):
                safe["status"] = (
                    "cancelling" if remote_status == "cancelled" else "finalizing"
                )
            _worker_login_state[state_key] = {
                **current,
                **safe,
                "provider": provider,
            }
            remote_account_id = str(remote_state.get("account_id") or "").strip()
            if remote_account_id and account.get("account_id") != remote_account_id:
                account["account_id"] = remote_account_id
                await _persist_worker_account_state(
                    prov,
                    worker_id,
                    account,
                    status="pending",
                    account_id=remote_account_id,
                )

        async def _run_codex():
            try:
                account_id = await prov.ensure_codex_account(
                    worker,
                    account,
                    allow_manual_otp=True,
                    on_status=_publish_codex_status,
                )
                if not account_id:
                    raise RuntimeError("Worker Codex login returned no account id")
                await _persist_worker_account_state(
                    prov,
                    worker_id,
                    account,
                    status="logged_in",
                    account_id=account_id,
                )
                _worker_login_state[state_key] = {
                    "status": "success",
                    "provider": provider,
                    "account_id": account_id,
                }
            except Exception as exc:
                logger.warning(
                    "Worker %s Codex account login failed for %s: %s",
                    worker_id,
                    email,
                    exc,
                )
                failed_state = {
                    **_worker_login_state.get(state_key, {}),
                    "status": "failed",
                    "provider": provider,
                    "detail": str(exc)[-1000:],
                }
                try:
                    await _persist_worker_account_state(
                        prov,
                        worker_id,
                        account,
                        status="failed",
                        account_id=(
                            str(account.get("account_id") or "").strip() or None
                        ),
                    )
                except Exception:
                    logger.exception(
                        "Failed to persist Worker %s Codex login failure for %s",
                        worker_id,
                        email,
                    )
                finally:
                    # A terminal state is also the promise that no later DB
                    # write from this login remains.  DELETE relies on that
                    # ordering to prevent removed credentials being revived.
                    _worker_login_state[state_key] = failed_state

        _spawn(_run_codex())
        return {"ok": True, "status": "running", "provider": provider}

    _worker_login_state[state_key] = {
        "status": "running",
        "provider": provider,
        "started_at": time.time(),
    }

    from backend.config import settings
    from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path
    ssh = SSHExecutor(host=worker.private_ip, user=worker.ssh_user,
                      key_path=worker.ssh_key_path or settings.worker_ssh_key_path,
                      known_hosts_path=(
                          worker_known_hosts_path(worker.cloud_instance_id)
                          if worker.cloud_instance_id else None
                      ))

    # 算 slot 名：查 worker 现有账号数
    try:
        r = await _worker_http_request(
            worker,
            "GET",
            "/api/pool/status",
            timeout=10,
            allow_statuses=frozenset({404}),
        )
    except HTTPException as exc:
        _worker_login_state[state_key] = {
            "status": "failed",
            "provider": provider,
            "detail": str(exc.detail),
        }
        raise
    if r.status_code == 404:
        # Explicit legacy POOL_ENABLED=false is the only safe empty-pool
        # fallback.  Auth/5xx/connectivity failures must stop before choosing
        # ``default`` and potentially colliding with an existing account.
        existing = 0
    else:
        pool_status = _worker_response_json(r)
        if not isinstance(pool_status, dict) or not isinstance(
            pool_status.get("accounts"), list
        ):
            raise HTTPException(502, "Worker Claude 号池返回了无效状态")
        existing = len(pool_status["accounts"])

    slot = f"account-{existing + 1}" if existing > 0 else "default"
    remote_dir = settings.worker_remote_dir

    # 后台跑 auto_login（xvfb-run 包装）
    cmd = _build_add_account_command(
        remote_dir,
        email=email,
        token=account["token"],
        slot=slot,
        login_method=account["login_method"],
    )

    # 这个任务可能跑 1-2 分钟，用 fire-and-forget
    async def _run():
        code, out = await ssh.run(cmd, timeout=600, sensitive=True)
        _worker_login_state[state_key] = {
            "status": "success" if code == 0 else "failed",
            "provider": provider,
            "detail": out[-1000:],
        }

    _spawn(_run())
    return {"ok": True, "status": "running", "provider": provider, "slot": slot}


@router.get("/{worker_id}/pool/add/{email}")
async def worker_add_status(
    worker_id: int,
    email: str,
    request: Request,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    provider = _normalize_worker_account_provider(provider)
    return _worker_login_state.get(
        f"{worker_id}:{provider}:{email.casefold()}"
    ) or {"status": "idle", "provider": provider}


def _worker_login_attempt_state(worker_id: int, attempt_id: str) -> dict | None:
    prefix = f"{worker_id}:codex:"
    matches = [
        state for key, state in _worker_login_state.items()
        if key.startswith(prefix) and state.get("attempt_id") == attempt_id
    ]
    return matches[0] if len(matches) == 1 else None


@router.post("/{worker_id}/pool/login-attempts/{attempt_id}/otp")
async def submit_worker_login_otp(
    worker_id: int,
    attempt_id: str,
    request: Request,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Relay a one-time code over the Worker's SSH loopback API channel."""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    state = _worker_login_attempt_state(worker_id, attempt_id)
    if not state:
        raise HTTPException(404, "Worker 登录流程已结束或不存在")
    challenge_id = body.get("challenge_id")
    code = body.get("code")
    if not isinstance(challenge_id, str) or challenge_id != state.get("challenge_id"):
        raise HTTPException(409, "验证码挑战已更新")
    if not isinstance(code, str) or not code.strip().isdigit() or len(code.strip()) != 6:
        raise HTTPException(422, "请输入 6 位数字验证码")
    response = await _provisioner().worker_local_api(
        worker,
        "POST",
        f"/api/codex-pool/login-attempts/{quote(attempt_id, safe='')}/otp",
        payload={"challenge_id": challenge_id, "code": code.strip()},
        timeout=30,
    )
    state.update({"status": "verifying_otp"})
    return response


@router.delete("/{worker_id}/pool/login-attempts/{attempt_id}")
async def cancel_worker_login(
    worker_id: int,
    attempt_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    state = _worker_login_attempt_state(worker_id, attempt_id)
    if not state:
        raise HTTPException(404, "Worker 登录流程已结束或不存在")
    response = await _provisioner().worker_local_api(
        worker,
        "DELETE",
        f"/api/codex-pool/login-attempts/{quote(attempt_id, safe='')}",
        timeout=45,
    )
    # The background poller may have replaced the state dict while the remote
    # cancellation request was in flight.  Re-resolve it before mutating so we
    # never update an orphaned object or overwrite a completed terminal state.
    current_state = _worker_login_attempt_state(worker_id, attempt_id)
    if (
        current_state is not None
        and current_state.get("status") in _WORKER_ACTIVE_LOGIN_STATUSES
    ):
        # The background poller still has to observe cancellation and persist
        # its retryable failure record.  Keep deletion blocked until then.
        current_state.update({"status": "cancelling", "detail": "正在取消登录"})
    return {
        "ok": bool(response.get("ok", True)) if isinstance(response, dict) else True,
        "status": (
            current_state.get("status", "cancelling")
            if current_state is not None else "cancelling"
        ),
    }


@router.delete("/{worker_id}/pool/{account_id}")
async def delete_worker_account(
    worker_id: int,
    request: Request,
    account_id: str,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    """从 worker 的号池中删除账号。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    provider = _normalize_worker_account_provider(provider)
    remote_path = (
        f"/api/codex-pool/accounts/{quote(account_id, safe='')}"
        if provider == "codex"
        else f"/api/pool/accounts/{quote(account_id, safe='')}"
    )

    # Commit deletion intent locally first. If the Manager exits after the
    # remote call, stale bootstrap credentials must never resurrect the slot.
    async with _worker_login_admission_lock:
        prefix = f"{worker_id}:{provider}:"
        if any(
            key.startswith(prefix)
            and state.get("status") in _WORKER_ACTIVE_LOGIN_STATUSES
            for key, state in _worker_login_state.items()
        ):
            raise HTTPException(
                409,
                "Worker 账号登录仍在进行中，请先取消并等待登录结束后再删除",
            )
        async with _worker_account_store_lock:
            # The row was loaded before waiting for the mutation locks. Refresh
            # it so a concurrently completed login is not overwritten.
            await db.refresh(worker)
            remaining_accounts, removed = _remove_persisted_worker_account(
                worker.accounts,
                provider=provider,
                account_id=account_id,
            )
            if removed:
                # Release the snapshot and make lifecycle gating + JSON write
                # atomic.  A concurrent destroy that already scrubbed secrets
                # must make this update fail instead of restoring the stale
                # credentials of accounts that were not deleted.
                await db.rollback()
                deleted = await db.execute(
                    update(Worker)
                    .where(Worker.id == worker_id, Worker.status == "ready")
                    .values(accounts=remaining_accounts)
                )
                if deleted.rowcount != 1:
                    await db.rollback()
                    current_status = await db.scalar(
                        select(Worker.status).where(Worker.id == worker_id)
                    )
                    raise HTTPException(
                        409,
                        f"Worker 状态已变为 {current_status or 'missing'}，账号删除已取消",
                    )
                await db.commit()
                # rollback() expired the route's ORM snapshot. Reload the
                # connection/auth fields before the remote idempotent delete.
                await db.refresh(worker)
        # Keep admission closed until the remote slot is gone.  Otherwise a
        # same-email add can adopt/live-verify the still-present slot after the
        # local delete commits, only for this request to delete it remotely a
        # moment later and strand a false logged_in Manager record.
        r = await _worker_http_request(
            worker,
            "DELETE",
            remote_path,
            timeout=10,
            allow_statuses=frozenset({404}),
        )
        if r.status_code == 404:
            return {"ok": True, "already_absent": True}
        return _worker_response_json(r)


@router.get("/{worker_id}/pool/usage")
async def get_worker_pool_usage(
    worker_id: int,
    request: Request,
    provider: str = "codex",
    db: AsyncSession = Depends(get_db),
):
    """拉取 Worker 指定 provider 的账号额度。"""
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    provider = _normalize_worker_account_provider(provider)
    usage_path = (
        "/api/codex-pool/usage?force=true"
        if provider == "codex"
        else "/api/pool/usage"
    )
    status_path = (
        "/api/codex-pool/status"
        if provider == "codex"
        else "/api/pool/status"
    )
    timeout = 60 if provider == "codex" else 15
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await _worker_http_request(
            worker,
            "GET",
            usage_path,
            timeout=timeout,
            allow_statuses=frozenset({404}),
            client=client,
        )
        if r.status_code != 404:
            return _worker_response_json(r)

        # Compatibility only: an old Worker can expose pool status but have
        # no usage endpoint, while a disabled legacy pool returns 404 for
        # both.  Auth, quota and 5xx failures never enter this fallback.
        r2 = await _worker_http_request(
            worker,
            "GET",
            status_path,
            timeout=timeout,
            allow_statuses=frozenset({404}),
            client=client,
        )
        if r2.status_code == 404:
            return {"enabled": False, "total": 0, "available": 0, "accounts": []}
        return _worker_response_json(r2)


@router.get("/{worker_id}/settings/runtime")
async def get_worker_runtime_settings(worker_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    r = await _worker_http_request(
        worker, "GET", "/api/settings/runtime", timeout=10,
    )
    return _worker_response_json(r)


@router.put("/{worker_id}/settings/runtime")
async def update_worker_runtime_settings(worker_id: int, request: Request, body: dict, db: AsyncSession = Depends(get_db)):
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    from backend.api.deps import require_worker_access as _rwa
    await _rwa(request, worker)
    if worker.status != "ready" or not worker.private_ip:
        raise HTTPException(409, f"Worker 未就绪（{worker.status}）")
    r = await _worker_http_request(
        worker,
        "PUT",
        "/api/settings/runtime",
        timeout=10,
        payload=body,
    )
    return _worker_response_json(r)


# --- Team CCM: Worker rename ---

from pydantic import BaseModel as _BaseModel


class RenameWorkerBody(_BaseModel):
    name: str


@router.patch("/{worker_id}/rename", response_model=WorkerResponse)
async def rename_worker(worker_id: int, body: RenameWorkerBody, request: Request, db: AsyncSession = Depends(get_db)):
    """Rename a worker (DB + AWS Name tag if cloud_instance_id exists)."""
    from backend.api.deps import require_worker_access
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    await require_worker_access(request, worker)
    new_name = body.name.strip()
    if not new_name:
        raise HTTPException(400, "Worker 名称不能为空")
    # Rename is a lifecycle mutation too.  In particular, changing the Name
    # tag parameter after a lost RunInstances response makes AWS reject the
    # stable ClientToken with IdempotentParameterMismatch.  Requiring a known
    # instance id also closes the rename-vs-retry race via this SQL CAS.
    await db.rollback()
    renamed = await db.execute(
        update(Worker)
        .where(
            Worker.id == worker_id,
            Worker.status.in_(tuple(_WORKER_DESTROYABLE_STATUSES)),
            Worker.cloud_instance_id.is_not(None),
            or_(
                Worker.bootstrap_step.is_(None),
                Worker.bootstrap_step != "destroy",
            ),
        )
        .values(name=new_name)
    )
    if renamed.rowcount != 1:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker 正在执行生命周期操作或 EC2 创建结果尚未认领，暂不能重命名",
        )
    await db.commit()
    worker = await db.get(Worker, worker_id)
    if worker is None:
        raise HTTPException(404, "Worker not found")
    await db.refresh(worker)
    # Update AWS Name tag (best-effort)
    if worker.cloud_instance_id:
        try:
            from backend.services.cloud_provider import AWSProvider
            cloud = AWSProvider()
            await cloud.update_instance_tags(worker.cloud_instance_id, {"Name": new_name})
        except Exception:
            logger.warning("Failed to update AWS Name tag for %s", worker.cloud_instance_id, exc_info=True)
    # Broadcast
    from backend.main import broadcaster
    if broadcaster:
        await broadcaster.broadcast("workers", {
            "event_type": "worker_update",
            "worker_id": worker.id,
            "status": worker.status,
        })
    return worker


# --- Team CCM: Worker assignment ---


class AssignWorkerBody(_BaseModel):
    owner_user_id: int | None = None


@router.put("/{worker_id}/assign", response_model=WorkerResponse)
async def assign_worker(worker_id: int, body: AssignWorkerBody, request: Request, db: AsyncSession = Depends(get_db)):
    """Assign a worker to a user (admin only). Set owner_user_id=null for public pool."""
    from backend.api.deps import require_admin
    require_admin(request)
    worker = await db.get(Worker, worker_id)
    if not worker:
        raise HTTPException(404, "Worker not found")
    prev_owner = worker.owner_user_id
    worker.owner_user_id = body.owner_user_id
    await db.commit()
    await db.refresh(worker)
    from backend.api.deps import get_current_user_id
    from backend.models.user import User
    admin_id = get_current_user_id(request)
    # Notify new owner
    if body.owner_user_id:
        try:
            from backend.services.feishu_notify import notify_worker_assigned
            admin = await db.get(User, admin_id) if admin_id else None
            import asyncio
            asyncio.create_task(notify_worker_assigned(
                admin.name if admin else "Admin",
                worker.name,
                body.owner_user_id,
            ))
        except Exception:
            pass
    # Notify previous owner (if changed and not self-revoke)
    if prev_owner and prev_owner != body.owner_user_id and prev_owner != admin_id:
        try:
            from backend.services.feishu_notify import notify_worker_unassigned
            admin = await db.get(User, admin_id) if admin_id else None
            import asyncio
            asyncio.create_task(notify_worker_unassigned(
                admin.name if admin else "Admin",
                worker.name,
                prev_owner,
            ))
        except Exception:
            pass
    return worker
