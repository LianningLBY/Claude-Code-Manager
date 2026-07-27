"""Feishu OAuth endpoints — per-user Feishu binding for Team CCM."""

import base64
import binascii
import hashlib
import hmac
import logging
import secrets
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse

from backend.api.deps import get_current_user_id
from backend.config import settings
from backend.database import get_db
from backend.models.user import User
from backend.services import feishu_auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/feishu", tags=["feishu"])

_STATE_VERSION = "v1"
_STATE_CONTEXT = b"ccm:feishu-oauth-state:v1\x00"
_STATE_MAX_LENGTH = 1024
_STATE_NONCE_BYTES = 16
_STATE_MAX_TTL_SECONDS = 3600


class _InvalidOAuthState(ValueError):
    """The callback state is malformed, unauthenticated, or expired."""


def _state_signing_key() -> bytes:
    """Return a stable, deployment-owned key with domain separation."""
    secret = (
        settings.feishu_oauth_state_secret
        or settings.feishu_app_secret
    )
    if not secret:
        raise _InvalidOAuthState("Feishu OAuth state secret is not configured")
    return hashlib.sha256(_STATE_CONTEXT + secret.encode("utf-8")).digest()


def _state_ttl_seconds() -> int:
    ttl = settings.feishu_oauth_state_ttl_seconds
    if not isinstance(ttl, int) or isinstance(ttl, bool):
        raise _InvalidOAuthState("Invalid Feishu OAuth state TTL")
    if ttl <= 0 or ttl > _STATE_MAX_TTL_SECONDS:
        raise _InvalidOAuthState("Invalid Feishu OAuth state TTL")
    return ttl


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    if not value:
        raise _InvalidOAuthState("Invalid Feishu OAuth state encoding")
    padding = "=" * (-len(value) % 4)
    try:
        return base64.b64decode(
            value + padding,
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, binascii.Error) as exc:
        raise _InvalidOAuthState(
            "Invalid Feishu OAuth state encoding"
        ) from exc


def _create_oauth_state(user_id: int, *, now: int | None = None) -> str:
    """Create a stateless, expiring state bound to one exact user."""
    if (
        not isinstance(user_id, int)
        or isinstance(user_id, bool)
        or user_id <= 0
    ):
        raise _InvalidOAuthState("Invalid Feishu OAuth state user")

    issued_at = int(time.time()) if now is None else int(now)
    expires_at = issued_at + _state_ttl_seconds()
    nonce = _b64encode(secrets.token_bytes(_STATE_NONCE_BYTES))
    payload = (
        f"{_STATE_VERSION}:{user_id}:{expires_at}:{nonce}"
    ).encode("ascii")
    signature = hmac.new(
        _state_signing_key(),
        _STATE_CONTEXT + payload,
        hashlib.sha256,
    ).digest()
    return f"{_b64encode(payload)}.{_b64encode(signature)}"


def _verify_oauth_state(state: str, *, now: int | None = None) -> int:
    """Verify a state in constant time and return its bound user id."""
    if not state or len(state) > _STATE_MAX_LENGTH:
        raise _InvalidOAuthState("Missing or oversized Feishu OAuth state")
    parts = state.split(".")
    if len(parts) != 2:
        raise _InvalidOAuthState("Invalid Feishu OAuth state")

    payload = _b64decode(parts[0])
    supplied_signature = _b64decode(parts[1])
    expected_signature = hmac.new(
        _state_signing_key(),
        _STATE_CONTEXT + payload,
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(supplied_signature, expected_signature):
        raise _InvalidOAuthState("Invalid Feishu OAuth state signature")

    try:
        version, raw_user_id, raw_expires_at, nonce = (
            payload.decode("ascii").split(":")
        )
        user_id = int(raw_user_id)
        expires_at = int(raw_expires_at)
    except (UnicodeDecodeError, ValueError) as exc:
        raise _InvalidOAuthState("Invalid Feishu OAuth state payload") from exc

    if (
        version != _STATE_VERSION
        or user_id <= 0
        or raw_user_id != str(user_id)
        or raw_expires_at != str(expires_at)
        or len(nonce) != 22
    ):
        raise _InvalidOAuthState("Invalid Feishu OAuth state payload")

    current_time = int(time.time()) if now is None else int(now)
    if expires_at <= current_time:
        raise _InvalidOAuthState("Expired Feishu OAuth state")
    return user_id


@router.get("/auth-url")
async def get_feishu_auth_url(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return a Feishu OAuth URL bound to the active current user."""
    if not settings.feishu_app_id or not settings.feishu_app_secret:
        raise HTTPException(400, "Feishu app not configured")
    user_id = get_current_user_id(request)
    if not user_id:
        raise HTTPException(401, "Authenticated user required")

    user = await db.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(401, "Authenticated user is not active")

    redirect_uri = settings.public_base_url + "/api/feishu/callback"
    try:
        state = _create_oauth_state(user.id)
    except _InvalidOAuthState as exc:
        logger.error("Feishu OAuth state configuration is invalid: %s", exc)
        raise HTTPException(
            503,
            "Feishu OAuth state signing is not configured",
        ) from exc
    url = await feishu_auth.get_auth_url(redirect_uri, state=state)
    return {"url": url}


@router.get("/callback")
async def feishu_callback(code: str, state: str = "", db: AsyncSession = Depends(get_db)):
    """Handle Feishu OAuth callback for the exact active signed-state user."""
    try:
        # Authenticate the callback and its target before making any external
        # request.  Bare legacy ``uid:<id>`` states are intentionally rejected:
        # accepting them would restore the account-binding vulnerability.
        user_id = _verify_oauth_state(state)
        user = await db.get(User, user_id)
        if not user or not user.is_active:
            raise _InvalidOAuthState(
                "Feishu OAuth state user is missing or inactive"
            )

        token_data = await feishu_auth.exchange_code(code)
        access_token = token_data["access_token"]

        user_info = await feishu_auth.get_user_info(access_token)
        open_id = user_info["open_id"]
        name = user_info.get("name", "")
        avatar_url = user_info.get("avatar_url", "")

        values = {
            "feishu_open_id": open_id,
            "feishu_name": name,
        }
        if avatar_url:
            values["avatar_url"] = avatar_url
        result = await db.execute(
            update(User)
            .where(User.id == user_id, User.is_active.is_(True))
            .values(**values)
        )
        if result.rowcount != 1:
            raise _InvalidOAuthState(
                "Feishu OAuth state user became inactive"
            )
        await db.commit()
        logger.info(
            "Feishu bound for user %s: %s (%s)",
            user_id,
            name,
            open_id,
        )
    except Exception:
        await db.rollback()
        logger.exception("Feishu callback failed")
        return RedirectResponse("/#/team?feishu_error=1")

    return RedirectResponse("/#/team?feishu_bound=1")


@router.get("/status")
async def get_feishu_status(request: Request, db: AsyncSession = Depends(get_db)):
    """Return current user's Feishu binding status."""
    user_id = get_current_user_id(request)
    if not user_id:
        return {"bound": False}

    user = await db.get(User, user_id)
    if not user or not user.feishu_open_id:
        return {"bound": False, "name": None, "open_id": None, "avatar_url": None}

    return {
        "bound": True,
        "name": user.feishu_name,
        "open_id": user.feishu_open_id,
        "avatar_url": user.avatar_url,
    }


@router.delete("/unbind")
async def unbind_feishu(request: Request, db: AsyncSession = Depends(get_db)):
    """Unbind current user's Feishu account."""
    user_id = get_current_user_id(request)
    if not user_id:
        raise HTTPException(401, "Not authenticated")

    user = await db.get(User, user_id)
    if not user or not user.feishu_open_id:
        raise HTTPException(404, "No Feishu binding found")

    user.feishu_open_id = ""
    user.feishu_name = ""
    await db.commit()
    return {"ok": True}
