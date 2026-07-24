"""Administrative API for CloudRouter-backed Claude/Codex accounts."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, SecretStr

from backend.api.deps import require_admin
from backend.services.cloudrouter_accounts import (
    CloudRouterAccountNotFound,
    CloudRouterAccountStore,
    CloudRouterUnsafePathError,
    CloudRouterUpstreamError,
)

router = APIRouter(
    prefix="/api/cloudrouter/accounts",
    tags=["cloudrouter-accounts"],
)


class CloudRouterAccountCreate(BaseModel):
    name: str
    api_key: SecretStr


def _get_store() -> CloudRouterAccountStore:
    from backend.main import cloudrouter_store

    return cloudrouter_store


def _runtime_pools():
    import backend.main as runtime
    from backend.config import settings

    accounts = runtime.cloudrouter_store.all_accounts()
    has_claude = any(
        account.enabled
        and not account.retired
        and account.supports_model("claude", None)
        for account in accounts
    )
    has_codex = any(
        account.enabled
        and not account.retired
        and account.supports_model("codex", None)
        for account in accounts
    )

    if has_claude and runtime.dispatcher.pool is None:
        from backend.services.claude_pool import ClaudePool

        runtime.dispatcher.pool = ClaudePool(
            config_path=settings.pool_config_path,
            cooldown_seconds=settings.pool_cooldown_seconds,
            cloudrouter_store=runtime.cloudrouter_store,
            bootstrap_default=settings.pool_enabled,
            include_native=settings.pool_enabled,
        )

    if has_codex and runtime.codex_pool is None:
        from backend.services.codex_pool import CodexPool

        runtime.codex_pool = CodexPool(
            config_path=settings.codex_pool_config_path,
            cooldown_seconds=settings.codex_pool_cooldown_seconds,
            quota_reader=runtime.instance_manager.read_codex_rate_limits,
            cloudrouter_store=runtime.cloudrouter_store,
            bootstrap_default=settings.codex_pool_enabled,
            include_native=settings.codex_pool_enabled,
        )
        runtime.dispatcher.codex_pool = runtime.codex_pool

    return runtime.dispatcher.pool, runtime.codex_pool


def _reload_runtime_pools() -> None:
    """Project persisted API accounts into both already-running pools."""

    reloaded: set[int] = set()
    for pool in _runtime_pools():
        if pool is None or id(pool) in reloaded:
            continue
        pool.reload()
        reloaded.add(id(pool))


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, CloudRouterAccountNotFound):
        return HTTPException(404, "CloudRouter account not found")
    if isinstance(exc, CloudRouterUnsafePathError):
        return HTTPException(409, "CloudRouter account storage is unsafe")
    if isinstance(exc, CloudRouterUpstreamError):
        if exc.status_code in {401, 403}:
            return HTTPException(400, "CloudRouter API key is invalid or unauthorized")
        if exc.code in {
            "timeout", "network_error", "upstream_unavailable", "rate_limited",
        }:
            return HTTPException(503, "CloudRouter is temporarily unavailable")
        return HTTPException(400, f"CloudRouter validation failed: {exc.code}")
    if isinstance(exc, ValueError):
        return HTTPException(422, str(exc))
    return HTTPException(500, "CloudRouter account operation failed")


@router.get("")
async def list_accounts(request: Request, force: bool = False):
    require_admin(request)
    store = _get_store()
    accounts = store.all_accounts()
    usage = await asyncio.gather(
        *(store.fetch_usage(account.id, force=force) for account in accounts),
    )
    return [
        {**account.public_dict(), "api_quota": snapshot}
        for account, snapshot in zip(accounts, usage, strict=True)
    ]


@router.post("", status_code=201)
async def create_account(request: Request, body: CloudRouterAccountCreate):
    require_admin(request)
    try:
        store = _get_store()
        account = await store.add_account(
            body.name, body.api_key.get_secret_value(),
        )
        quota = await store.fetch_usage(account.id, force=True)
        _reload_runtime_pools()
    except Exception as exc:
        raise _http_error(exc) from exc
    return {**account.public_dict(), "api_quota": quota}


@router.post("/{account_id}/refresh")
async def refresh_account(request: Request, account_id: str):
    require_admin(request)
    store = _get_store()
    try:
        account = await store.refresh_account(account_id)
        quota = await store.fetch_usage(account_id, force=True)
        _reload_runtime_pools()
    except Exception as exc:
        raise _http_error(exc) from exc
    return {**account.public_dict(), "api_quota": quota}


@router.delete("/{account_id}")
async def retire_account(request: Request, account_id: str):
    """Refuse retirement until every API credential consumer shares one fence.

    Main turns, PTY/container launches, goal evaluators, distillation, monitors,
    and sub-agents do not yet have a single cross-lifecycle admission barrier.
    A best-effort process snapshot leaves a select→spawn race where retirement
    can remove the key/config beneath a newly admitted process.  Keep the store
    retirement primitive for offline maintenance/tests, but do not expose an
    unsafe runtime delete operation.
    """

    require_admin(request)
    store = _get_store()
    try:
        account = store.account(account_id)
    except Exception as exc:
        raise _http_error(exc) from exc
    if account is None:
        raise HTTPException(404, "CloudRouter account not found")
    if account.retired and not account.cleanup_pending:
        return {"ok": True, **account.public_dict()}
    raise HTTPException(
        409,
        "CloudRouter API account deletion is temporarily disabled while "
        "runtime credential-use fencing is unavailable",
    )
