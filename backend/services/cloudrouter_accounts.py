"""Private on-disk CloudRouter API accounts.

An API account deliberately looks like an ordinary Claude/Codex pool account:
it owns one ``CLAUDE_CONFIG_DIR`` and one ``CODEX_HOME``.  The API key is kept
outside both CLI configuration files and is only exposed through a small
credential helper.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shlex
import shutil
import stat
import tempfile
import time
import tomllib
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

CLAUDE_BASE_URL = "https://console.cloudrouter.online"
CODEX_BASE_URL = "https://console.cloudrouter.online/v1"
MODELS_URL = f"{CODEX_BASE_URL}/models"
USAGE_URL = f"{CODEX_BASE_URL}/usage"
ENDPOINTS = {
    "claude_base_url": CLAUDE_BASE_URL,
    "codex_base_url": CODEX_BASE_URL,
    "models_url": MODELS_URL,
    "usage_url": USAGE_URL,
}

ACCOUNT_ID_RE = re.compile(r"^cloudrouter-([1-9][0-9]*)$")
MAX_METADATA_BYTES = 256 * 1024
MAX_API_RESPONSE_BYTES = 1024 * 1024
MAX_API_KEY_BYTES = 16 * 1024
DEFAULT_HTTP_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
DEFAULT_QUOTA_CACHE_TTL = 60.0
CLAUDE_SKIP_DANGEROUS_PROMPT = "skipDangerousModePermissionPrompt"


class CloudRouterAccountError(RuntimeError):
    """Base error for account storage and upstream validation."""


class CloudRouterAccountNotFound(CloudRouterAccountError):
    """The requested local API account does not exist."""


class CloudRouterUnsafePathError(CloudRouterAccountError):
    """A managed path failed a no-symlink/type/containment check."""


class CloudRouterUpstreamError(CloudRouterAccountError):
    """CloudRouter rejected or could not complete a request."""

    def __init__(self, code: str, *, status_code: int | None = None):
        self.code = code
        self.status_code = status_code
        super().__init__(code)


def _now() -> float:
    return time.time()


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _ensure_private_directory(path: Path, *, create: bool = True) -> None:
    """Create/check one managed directory without accepting a symlink."""

    for ancestor in path.parents:
        if ancestor.is_symlink():
            raise CloudRouterUnsafePathError(
                f"Managed directory has a symlink ancestor: {path}",
            )
    if not path.exists() and not path.is_symlink():
        if not create:
            raise CloudRouterUnsafePathError(f"Missing managed directory: {path}")
        path.mkdir(parents=True, mode=0o700)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise CloudRouterUnsafePathError(f"Unsafe managed directory: {path}")
    if metadata.st_uid != os.getuid():
        raise CloudRouterUnsafePathError(f"Managed directory has another owner: {path}")
    if _mode(path) != 0o700:
        os.chmod(path, 0o700, follow_symlinks=False)


def _open_regular_nofollow(path: Path, *, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise CloudRouterUnsafePathError(f"Not a regular file: {path}")
        if metadata.st_uid != os.getuid():
            raise CloudRouterUnsafePathError(f"Managed file has another owner: {path}")
        if metadata.st_size > maximum:
            raise CloudRouterUnsafePathError(f"Managed file is too large: {path}")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > maximum:
            raise CloudRouterUnsafePathError(f"Managed file is too large: {path}")
        return payload
    finally:
        os.close(descriptor)


def _require_owned_regular(path: Path, expected_mode: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise CloudRouterUnsafePathError(f"Missing managed file: {path}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise CloudRouterUnsafePathError(f"Unsafe managed file: {path}")


def _converge_cli_mutable_private_file(path: Path) -> None:
    """Safely restore 0600 on an owned regular file a CLI may rewrite."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CloudRouterUnsafePathError(f"Missing managed file: {path}") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.getuid():
            raise CloudRouterUnsafePathError(f"Unsafe managed file: {path}")
        if stat.S_IMODE(opened.st_mode) != 0o600:
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
        converged = os.fstat(descriptor)
        current = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(converged.st_mode)
            or converged.st_uid != os.getuid()
            or stat.S_IMODE(converged.st_mode) != 0o600
            or not stat.S_ISREG(current.st_mode)
            or current.st_uid != os.getuid()
            or stat.S_IMODE(current.st_mode) != 0o600
            or current.st_dev != converged.st_dev
            or current.st_ino != converged.st_ino
        ):
            raise CloudRouterUnsafePathError(f"Unsafe managed file: {path}")
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_private_write(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    _ensure_private_directory(path.parent)
    if path.is_symlink():
        raise CloudRouterUnsafePathError(f"Refusing symlink target: {path}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode, follow_symlinks=False)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _atomic_private_json(path: Path, value: dict[str, Any]) -> None:
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    _atomic_private_write(path, payload)


def _validate_account_id(account_id: str) -> str:
    if not isinstance(account_id, str) or not ACCOUNT_ID_RE.fullmatch(account_id):
        raise CloudRouterAccountNotFound("Unknown CloudRouter account")
    return account_id


def _key_hint(api_key: str) -> str:
    if len(api_key) <= 8:
        return f"…{api_key[-2:]}"
    return f"{api_key[:3]}…{api_key[-4:]}"


def _claude_helper_command(account_root: Path) -> str:
    container_helper = "/home/sandbox/.ccm-api-account/key-helper"
    runtime_helper = account_root / "key-helper"
    return (
        f"if [ -x {shlex.quote(container_helper)} ]; then "
        f"{shlex.quote(container_helper)}; else "
        f"{shlex.quote(str(runtime_helper))}; fi"
    )


def _normalise_model(model: str) -> str:
    value = str(model or "").strip()
    if value.endswith("[1m]"):
        value = value[:-4]
    return value


def _provider_for_model(model: str) -> str | None:
    value = _normalise_model(model).lower()
    if value.startswith("claude-"):
        return "claude"
    if value.startswith(("gpt-", "o1", "o3", "o4", "codex-")):
        return "codex"
    return None


def _normalise_models(payload: Any) -> dict[str, list[str]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise CloudRouterUpstreamError("invalid_models_response")
    result: dict[str, list[str]] = {"claude": [], "codex": []}
    seen: set[str] = set()
    for item in payload["data"]:
        model_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(model_id, str):
            continue
        model_id = model_id.strip()
        provider = _provider_for_model(model_id)
        if not provider or model_id in seen:
            continue
        seen.add(model_id)
        result[provider].append(model_id)
    for values in result.values():
        values.sort()
    if not any(result.values()):
        raise CloudRouterUpstreamError("no_supported_models")
    return result


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _json_number(value: Decimal | None) -> float | int | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _number(value: Any) -> float | int | None:
    return _json_number(_decimal(value))


def _window_numbers(
    raw_used: Any, raw_limit: Any, raw_remaining: Any = None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    used = _decimal(raw_used)
    limit = _decimal(raw_limit)
    remaining = _decimal(raw_remaining)
    if remaining is None and used is not None and limit is not None:
        remaining = limit - used
    return used, limit, remaining


def _window(
    *,
    window_id: str,
    label: str,
    currency: str,
    raw_used: Any,
    raw_limit: Any,
    raw_remaining: Any = None,
    reset_at: Any = None,
) -> tuple[dict[str, Any], bool]:
    unlimited = _decimal(raw_remaining) == Decimal("-1")
    used, limit, remaining = _window_numbers(
        raw_used, raw_limit, raw_remaining,
    )
    item: dict[str, Any] = {
        "id": window_id,
        "label": label,
        "currency": currency,
    }
    for key, value in (("used", used), ("limit", limit), ("remaining", remaining)):
        if (parsed := _json_number(value)) is not None:
            item[key] = parsed
    if limit is not None and limit > 0 and used is not None:
        item["utilization"] = float((used / limit) * Decimal(100))
    if isinstance(reset_at, (str, int, float)):
        item["reset_at"] = reset_at
    if unlimited:
        item["unlimited"] = True
    exhausted = bool(
        not unlimited
        and
        limit is not None
        and limit > 0
        and (
            (remaining is not None and remaining <= 0)
            or (used is not None and used >= limit)
        )
    )
    return item, exhausted


def _usage_metrics(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    keys = (
        "requests", "input_tokens", "output_tokens", "total_tokens",
        "cache_creation_tokens", "cache_read_tokens", "actual_cost",
        "rpm", "tpm", "average_duration_ms",
    )
    result = {key: parsed for key in keys if (parsed := _number(value.get(key))) is not None}
    for key in ("model_stats", "daily_usage"):
        if isinstance(value.get(key), (dict, list)):
            result[key] = value[key]
    return result or None


def _normalise_usage(account_id: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise CloudRouterUpstreamError("invalid_usage_response")

    upstream_status = str(payload.get("status") or "active").lower()
    mode = str(payload.get("mode") or "").lower()
    if mode not in {"quota_limited", "subscription", "wallet"}:
        mode = "subscription" if isinstance(payload.get("subscription"), dict) else "wallet"
    expired = upstream_status == "expired"
    exhausted = upstream_status in {"quota_exhausted", "exhausted"}
    invalid = payload.get("isValid") is False
    if payload.get("isValid") is False:
        if upstream_status == "active":
            upstream_status = "invalid"

    subscription = payload.get("subscription")
    subscription_uses_usd = isinstance(subscription, dict) and any(
        key.endswith("_usd") for key in subscription
    )
    currency = (
        "USD"
        if mode in {"quota_limited", "wallet"} or subscription_uses_usd
        else "credits"
    )
    quota_value = payload.get("quota")
    quota: dict[str, Any] | None = None
    if isinstance(quota_value, dict):
        quota_unlimited = _decimal(quota_value.get("remaining")) == Decimal("-1")
        quota_used, quota_limit, quota_remaining = _window_numbers(
            quota_value.get("used"),
            quota_value.get("limit"),
            quota_value.get("remaining"),
        )
        quota = {}
        for key, value in (
            ("limit", quota_limit),
            ("used", quota_used),
            ("remaining", quota_remaining),
        ):
            if (parsed := _json_number(value)) is not None:
                quota[key] = parsed
        quota = quota or None
        if quota is not None:
            quota["currency"] = currency
            if quota_unlimited:
                quota["unlimited"] = True
        if (
            not quota_unlimited
            and
            quota_remaining is not None
            and quota_remaining <= 0
            and quota_limit is not None
            and quota_limit > 0
        ):
            exhausted = True
        elif (
            not quota_unlimited
            and
            quota_used is not None
            and quota_limit is not None
            and quota_limit > 0
            and quota_used >= quota_limit
        ):
            exhausted = True

    windows: list[dict[str, Any]] = []
    rate_limits = payload.get("rate_limits")
    if isinstance(rate_limits, list):
        for raw in rate_limits:
            if not isinstance(raw, dict):
                continue
            window = str(raw.get("window") or "").strip()
            if not window:
                continue
            item, window_exhausted = _window(
                window_id=window,
                label=window,
                currency=currency,
                raw_used=raw.get("used"),
                raw_limit=raw.get("limit"),
                raw_remaining=raw.get("remaining"),
                reset_at=raw.get("reset_at"),
            )
            windows.append(item)
            exhausted = exhausted or window_exhausted

    if isinstance(subscription, dict):
        suffix = "usd" if subscription_uses_usd else "credits"
        for prefix, label in (("daily", "1d"), ("weekly", "7d"), ("monthly", "30d")):
            raw_used = subscription.get(f"{prefix}_usage_{suffix}")
            raw_limit = subscription.get(f"{prefix}_limit_{suffix}")
            if _decimal(raw_used) is None and _decimal(raw_limit) is None:
                continue
            item, window_exhausted = _window(
                window_id=prefix,
                label=label,
                currency=currency,
                raw_used=raw_used,
                raw_limit=raw_limit,
            )
            windows.append(item)
            exhausted = exhausted or window_exhausted

    usage_value = payload.get("usage")
    usage: dict[str, Any] | None = None
    if isinstance(usage_value, dict):
        usage = {}
        for key in ("today", "total"):
            if metrics := _usage_metrics(usage_value.get(key)):
                usage[key] = metrics
        for key in ("rpm", "tpm", "average_duration_ms"):
            if (parsed := _number(usage_value.get(key))) is not None:
                usage[key] = parsed
        for key in ("model_stats", "daily_usage"):
            if isinstance(usage_value.get(key), (dict, list)):
                usage[key] = usage_value[key]
        usage = usage or None

    balance_decimal = _decimal(payload.get("balance"))
    remaining_decimal = _decimal(payload.get("remaining"))
    def _wallet_depleted(value: Decimal | None) -> bool:
        # CloudRouter uses exactly -1 as the unlimited sentinel. Any other
        # non-positive finite balance is exhausted, including an overdrawn
        # wallet reported as a negative number.
        return value is not None and value != Decimal("-1") and value <= 0

    if mode == "wallet" and (
        _wallet_depleted(balance_decimal)
        or _wallet_depleted(remaining_decimal)
    ):
        exhausted = True

    if invalid:
        state = "error"
    elif expired:
        state = "expired"
    elif exhausted:
        state = "exhausted"
    else:
        state = "active"
    unavailable = state != "active"
    reason = upstream_status if unavailable and upstream_status != "active" else state
    snapshot: dict[str, Any] = {
        "account_id": account_id,
        "fetched_at": _now(),
        "stale": False,
        "state": state,
        "status": state,
        "mode": mode,
        "currency": currency,
        "unit": currency,
        "quota": quota,
        "windows": windows,
        "usage": usage,
        "available": not unavailable,
        "known": True,
        "reason": reason,
    }
    aliases = {
        "balance": ("balance",),
        "remaining": ("remaining",),
        "expires_at": ("expires_at", "expiry", "expiresAt"),
        "days_until_expiry": ("days_until_expiry", "daysUntilExpiry"),
    }
    for key, source_keys in aliases.items():
        raw = next(
            (payload.get(source) for source in source_keys if payload.get(source) is not None),
            None,
        )
        if raw is None and isinstance(subscription, dict):
            raw = next(
                (
                    subscription.get(source)
                    for source in source_keys
                    if subscription.get(source) is not None
                ),
                None,
            )
        if key in {"expires_at"} and isinstance(raw, (str, int, float)):
            snapshot[key] = raw
        elif key == "days_until_expiry" and (parsed := _number(raw)) is not None:
            snapshot[key] = parsed
        elif key in {"balance", "remaining"} and (parsed := _number(raw)) is not None:
            snapshot[key] = parsed
    plan_name = payload.get("planName", payload.get("plan_name"))
    if plan_name is None and isinstance(subscription, dict):
        plan_name = subscription.get(
            "planName", subscription.get("plan_name"),
        )
    if isinstance(plan_name, str):
        snapshot["plan_name"] = plan_name
    return snapshot


def _unknown_snapshot(
    account_id: str,
    reason: str,
    *,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = dict(previous or {})
    if previous:
        result["last_known_available"] = previous.get(
            "last_known_available", previous.get("available"),
        )
        result["last_known_reason"] = previous.get(
            "last_known_reason", previous.get("reason"),
        )
    result.update({
        "account_id": account_id,
        "fetched_at": _now(),
        "stale": bool(previous),
        "state": "unknown",
        "status": "unknown",
        "available": True,
        "known": False,
        "reason": reason,
    })
    return result


def _unavailable_snapshot(account_id: str, reason: str) -> dict[str, Any]:
    return {
        "account_id": account_id,
        "fetched_at": _now(),
        "stale": False,
        "state": "error",
        "status": "unavailable",
        "mode": None,
        "currency": None,
        "unit": None,
        "quota": None,
        "windows": [],
        "usage": None,
        "available": False,
        "known": True,
        "reason": reason,
    }


@dataclass(frozen=True, slots=True)
class CloudRouterAccount:
    id: str
    name: str
    enabled: bool
    retired: bool
    cleanup_pending: bool
    models: dict[str, list[str]]
    key_hint: str
    root: Path

    @property
    def claude_config_dir(self) -> str:
        return str(self.root / "claude")

    @property
    def codex_home(self) -> str:
        return str(self.root / "codex")

    @property
    def providers(self) -> list[str]:
        return [provider for provider in ("claude", "codex") if self.models.get(provider)]

    def supports_model(self, provider: str, model: str | None) -> bool:
        provider = str(provider or "").lower()
        if provider not in {"claude", "codex"}:
            return False
        requested = _normalise_model(model)
        if not requested or requested == "default":
            return bool(self.models.get(provider))
        available = {
            _normalise_model(item) for item in self.models.get(provider, [])
        }
        if requested in available:
            return True
        # Anthropic publishes immutable dated model IDs while CCM exposes the
        # corresponding stable short alias.  Accept only the exact alias plus
        # one YYYYMMDD suffix; a generic prefix match would incorrectly route
        # similarly named but distinct models.
        if provider == "claude":
            dated = re.compile(rf"^{re.escape(requested)}-[0-9]{{8}}$")
            return any(dated.fullmatch(candidate) for candidate in available)
        return False

    def public_dict(self) -> dict[str, Any]:
        supported_models = sorted({
            model for values in self.models.values() for model in values
        })
        return {
            "id": self.id,
            "name": self.name,
            "enabled": self.enabled,
            "retired": self.retired,
            "cleanup_pending": self.cleanup_pending,
            "models": self.models,
            "providers": self.providers,
            "key_hint": self.key_hint,
            "account_dir": str(self.root),
            "claude_config_dir": self.claude_config_dir,
            "codex_home": self.codex_home,
            "supported_models": supported_models,
            "endpoints": dict(ENDPOINTS),
        }


KEY_HELPER = r"""#!/usr/bin/env python3
import os
import stat
import sys
from pathlib import Path

path = Path(__file__).with_name("api.key")
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
try:
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise RuntimeError("API key file must be a private regular file")
    payload = os.read(descriptor, 16385)
finally:
    if "descriptor" in locals():
        os.close(descriptor)
if not payload or len(payload) > 16384 or b"\n" in payload or b"\r" in payload:
    raise RuntimeError("Invalid API key file")
sys.stdout.write(payload.decode("utf-8"))
"""


class CloudRouterAccountStore:
    """Manage CloudRouter accounts rooted under one caller-selected directory."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        quota_cache_ttl: float = DEFAULT_QUOTA_CACHE_TTL,
        http_timeout: httpx.Timeout | float = DEFAULT_HTTP_TIMEOUT,
    ):
        raw_root = Path(os.path.expandvars(os.path.expanduser(os.fspath(root))))
        self.root = raw_root.absolute()
        _ensure_private_directory(self.root)
        self._quota_cache_ttl = max(0.0, float(quota_cache_ttl))
        self._http_timeout = http_timeout
        self._accounts: dict[str, CloudRouterAccount] = {}
        self._quota_cache: dict[str, dict[str, Any]] = {}
        self._quota_cached_at: dict[str, float] = {}
        self._mutation_lock = asyncio.Lock()
        self.reload()

    def _account_root(self, account_id: str) -> Path:
        valid = _validate_account_id(account_id)
        candidate = self.root / valid
        if candidate.parent != self.root:
            raise CloudRouterUnsafePathError("Account path escaped its store root")
        return candidate

    def _load_account(self, path: Path) -> CloudRouterAccount:
        _ensure_private_directory(path, create=False)
        account_id = _validate_account_id(path.name)
        metadata_path = path / "account.json"
        _require_owned_regular(metadata_path, 0o600)
        try:
            data = json.loads(
                _open_regular_nofollow(
                    metadata_path, maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"),
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid account metadata: {account_id}",
            ) from exc
        if not isinstance(data, dict) or data.get("id") != account_id:
            raise CloudRouterUnsafePathError(f"Mismatched account metadata: {account_id}")
        if data.get("endpoints") != ENDPOINTS:
            raise CloudRouterUnsafePathError(f"Modified fixed endpoints: {account_id}")
        name = data.get("name")
        models = data.get("models")
        if not isinstance(name, str) or not name.strip() or not isinstance(models, dict):
            raise CloudRouterUnsafePathError(f"Invalid account metadata: {account_id}")
        normalised_models = {
            provider: sorted({
                str(model) for model in models.get(provider, [])
                if isinstance(model, str) and _provider_for_model(model) == provider
            })
            for provider in ("claude", "codex")
        }
        account = CloudRouterAccount(
            id=account_id,
            name=name,
            enabled=bool(data.get("enabled", True)) and not bool(data.get("retired", False)),
            retired=bool(data.get("retired", False)),
            cleanup_pending=bool(data.get("cleanup_pending", False)),
            models=normalised_models,
            key_hint=str(data.get("key_hint") or ""),
            root=path,
        )
        for directory in (path / "claude", path / "codex"):
            _ensure_private_directory(directory, create=False)
        if account.retired:
            for preserved in (
                path / "claude" / "projects",
                path / "codex" / "sessions",
            ):
                if preserved.exists() or preserved.is_symlink():
                    _ensure_private_directory(preserved, create=False)
        else:
            for file_name, expected_mode in (
                ("account.json", 0o600), ("api.key", 0o600), ("key-helper", 0o700),
            ):
                _require_owned_regular(path / file_name, expected_mode)
            _require_owned_regular(path / "claude" / "settings.json", 0o600)
            self._converge_claude_runtime_settings(account)
            _converge_cli_mutable_private_file(
                path / "claude" / ".claude.json",
            )
            _require_owned_regular(path / "codex" / "config.toml", 0o600)
            self._validate_runtime_configuration(account)
        return account

    @staticmethod
    def _converge_claude_runtime_settings(
        account: CloudRouterAccount,
    ) -> None:
        """Upgrade managed Claude settings needed for unattended launches.

        Claude Code exits when ``--dangerously-skip-permissions`` is used for
        the first time unless this acknowledgement is already present.  API
        accounts are intentionally non-interactive, so migrate older account
        folders while preserving CCM-owned hooks and other harmless CLI state.
        Routing and credential-helper fields are verified before any rewrite.
        """

        settings_path = account.root / "claude" / "settings.json"
        try:
            settings = json.loads(_open_regular_nofollow(
                settings_path,
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid Claude settings: {account.id}",
            ) from exc
        if (
            not isinstance(settings, dict)
            or settings.get("env") != {"ANTHROPIC_BASE_URL": CLAUDE_BASE_URL}
            or settings.get("apiKeyHelper")
            != _claude_helper_command(account.root)
        ):
            raise CloudRouterUnsafePathError(
                f"Modified Claude API routing: {account.id}",
            )
        if settings.get(CLAUDE_SKIP_DANGEROUS_PROMPT) is not True:
            settings[CLAUDE_SKIP_DANGEROUS_PROMPT] = True
            _atomic_private_json(settings_path, settings)

    @staticmethod
    def _validate_runtime_configuration(account: CloudRouterAccount) -> None:
        """Fail closed if a CLI config could redirect or replace API auth."""

        helper_path = account.root / "key-helper"
        try:
            helper_payload = _open_regular_nofollow(
                helper_path, maximum=len(KEY_HELPER.encode("utf-8")),
            )
        except CloudRouterUnsafePathError as exc:
            raise CloudRouterUnsafePathError(
                f"Modified CloudRouter credential helper: {account.id}",
            ) from exc
        if helper_payload != KEY_HELPER.encode("utf-8"):
            raise CloudRouterUnsafePathError(
                f"Modified CloudRouter credential helper: {account.id}",
            )

        try:
            settings = json.loads(_open_regular_nofollow(
                account.root / "claude" / "settings.json",
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid Claude settings: {account.id}",
            ) from exc
        if (
            not isinstance(settings, dict)
            or settings.get("env") != {"ANTHROPIC_BASE_URL": CLAUDE_BASE_URL}
            or settings.get("apiKeyHelper") != _claude_helper_command(account.root)
            or settings.get(CLAUDE_SKIP_DANGEROUS_PROMPT) is not True
        ):
            raise CloudRouterUnsafePathError(
                f"Modified Claude API routing: {account.id}",
            )

        try:
            onboarding = json.loads(_open_regular_nofollow(
                account.root / "claude" / ".claude.json",
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid Claude onboarding state: {account.id}",
            ) from exc
        if (
            not isinstance(onboarding, dict)
            or onboarding.get("hasCompletedOnboarding") is not True
        ):
            raise CloudRouterUnsafePathError(
                f"Invalid Claude onboarding state: {account.id}",
            )

        try:
            codex = tomllib.loads(_open_regular_nofollow(
                account.root / "codex" / "config.toml",
                maximum=MAX_METADATA_BYTES,
            ).decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise CloudRouterUnsafePathError(
                f"Invalid Codex configuration: {account.id}",
            ) from exc
        providers = codex.get("model_providers")
        provider = providers.get("cloudrouter") if isinstance(providers, dict) else None
        expected_provider = {
            "name": "CloudRouter",
            "base_url": CODEX_BASE_URL,
            "wire_api": "responses",
            "supports_websockets": False,
            "auth": {
                "command": str(account.root / "key-helper"),
                "timeout_ms": 5000,
                "refresh_interval_ms": 0,
            },
        }
        if (
            codex.get("model_provider") != "cloudrouter"
            or provider != expected_provider
        ):
            raise CloudRouterUnsafePathError(
                f"Modified Codex API routing: {account.id}",
            )

    def reload(self) -> list[CloudRouterAccount]:
        _ensure_private_directory(self.root)
        loaded: dict[str, CloudRouterAccount] = {}
        for child in self.root.iterdir():
            if not ACCOUNT_ID_RE.fullmatch(child.name):
                continue
            account = self._load_account(child)
            loaded[account.id] = account
        self._accounts = loaded
        self._quota_cache = {
            key: value for key, value in self._quota_cache.items() if key in loaded
        }
        self._quota_cached_at = {
            key: value for key, value in self._quota_cached_at.items() if key in loaded
        }
        return self.all_accounts(include_retired=True)

    def all_accounts(self, include_retired: bool = False) -> list[CloudRouterAccount]:
        accounts = sorted(
            self._accounts.values(),
            key=lambda account: int(ACCOUNT_ID_RE.fullmatch(account.id).group(1)),  # type: ignore[union-attr]
        )
        if not include_retired:
            accounts = [account for account in accounts if not account.retired]
        return accounts

    def account(self, account_id: str) -> CloudRouterAccount | None:
        _validate_account_id(account_id)
        return self._accounts.get(account_id)

    @staticmethod
    def _canonical_runtime_path(path: str | os.PathLike[str]) -> str:
        raw = os.path.expandvars(os.path.expanduser(os.fspath(path)))
        if not raw:
            return ""
        return os.path.realpath(os.path.abspath(raw))

    def account_for_claude_config_dir(
        self, path: str | os.PathLike[str],
    ) -> CloudRouterAccount | None:
        """Find an active or retired API account by exact runtime directory."""

        candidate = self._canonical_runtime_path(path)
        return next((
            account for account in self._accounts.values()
            if self._canonical_runtime_path(account.claude_config_dir) == candidate
        ), None)

    def account_for_codex_home(
        self, path: str | os.PathLike[str],
    ) -> CloudRouterAccount | None:
        """Find an active or retired API account by exact CODEX_HOME."""

        candidate = self._canonical_runtime_path(path)
        return next((
            account for account in self._accounts.values()
            if self._canonical_runtime_path(account.codex_home) == candidate
        ), None)

    def account_for_runtime_home(
        self, path: str | os.PathLike[str],
    ) -> CloudRouterAccount | None:
        return (
            self.account_for_claude_config_dir(path)
            or self.account_for_codex_home(path)
        )

    @asynccontextmanager
    async def runtime_admission(
        self,
        provider: str,
        runtime_home: str | os.PathLike[str],
        model: str | None,
    ):
        """Serialize model/quota revalidation with metadata mutation.

        The lock is intentionally held only until the caller has spawned and
        registered its process. Refresh can then update future routing without
        invalidating the admission decision between selection and spawn.
        """

        provider = str(provider or "").lower()
        if provider not in {"claude", "codex"}:
            raise CloudRouterAccountError("Unknown provider")
        async with self._mutation_lock:
            try:
                self.reload()
                finder = (
                    self.account_for_codex_home
                    if provider == "codex"
                    else self.account_for_claude_config_dir
                )
                account = finder(runtime_home)
            except CloudRouterAccountError:
                raise
            except OSError as exc:
                # Filesystem races/read-only mounts are permanent for this
                # admission attempt. Convert them to the same sanitized,
                # non-requeued safety failure as an invalid managed path.
                raise CloudRouterUnsafePathError(
                    "CloudRouter account storage is unavailable"
                ) from exc
            if account is None or account.retired or not account.enabled:
                raise CloudRouterAccountError(
                    "CloudRouter API account is disabled or missing"
                )
            if not account.supports_model(provider, model):
                raise CloudRouterAccountError(
                    f"CloudRouter API account does not support model {model!r}"
                )
            decision = self.cached_quota_decision(account.id)
            if (
                bool(decision.get("known"))
                and decision.get("available") is False
            ):
                raise CloudRouterAccountError(
                    "CloudRouter API account is unavailable: "
                    f"{decision.get('reason') or 'quota'}"
                )
            yield account

    def _require_account(
        self, account_id: str, *, allow_retired: bool = False,
    ) -> CloudRouterAccount:
        account = self.account(account_id)
        if account is None or (account.retired and not allow_retired):
            raise CloudRouterAccountNotFound("Unknown CloudRouter account")
        return account

    def _next_account_id(self) -> str:
        used = {
            int(match.group(1))
            for child in self.root.iterdir()
            if (match := ACCOUNT_ID_RE.fullmatch(child.name))
        }
        number = 1
        while number in used:
            number += 1
        return f"cloudrouter-{number}"

    async def _request_json(self, url: str, api_key: str) -> Any:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }
        try:
            async with httpx.AsyncClient(
                timeout=self._http_timeout,
                follow_redirects=False,
            ) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    status_code = response.status_code
                    if 300 <= status_code < 400:
                        raise CloudRouterUpstreamError(
                            "unexpected_redirect", status_code=status_code,
                        )
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_API_RESPONSE_BYTES:
                            raise CloudRouterUpstreamError("response_too_large")
                        chunks.append(chunk)
        except CloudRouterUpstreamError:
            raise
        except (httpx.TimeoutException, TimeoutError) as exc:
            raise CloudRouterUpstreamError("timeout") from exc
        except (httpx.RequestError, OSError) as exc:
            raise CloudRouterUpstreamError("network_error") from exc

        if status_code == 401:
            raise CloudRouterUpstreamError("invalid_api_key", status_code=401)
        if status_code == 403:
            raise CloudRouterUpstreamError("forbidden", status_code=403)
        if status_code == 429:
            raise CloudRouterUpstreamError("rate_limited", status_code=429)
        if status_code >= 500:
            raise CloudRouterUpstreamError("upstream_unavailable", status_code=status_code)
        if not 200 <= status_code < 300:
            raise CloudRouterUpstreamError("upstream_rejected", status_code=status_code)
        try:
            return json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudRouterUpstreamError("invalid_json") from exc

    async def probe_models(self, api_key: str) -> dict[str, list[str]]:
        return _normalise_models(await self._request_json(MODELS_URL, api_key))

    def _read_api_key(self, account: CloudRouterAccount) -> str:
        path = account.root / "api.key"
        try:
            payload = _open_regular_nofollow(path, maximum=MAX_API_KEY_BYTES)
            metadata = path.lstat()
        except OSError as exc:
            raise CloudRouterUnsafePathError("API key is unavailable") from exc
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise CloudRouterUnsafePathError("API key permissions are unsafe")
        try:
            value = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CloudRouterUnsafePathError("API key is invalid") from exc
        if not value or value.strip() != value or "\r" in value or "\n" in value:
            raise CloudRouterUnsafePathError("API key is invalid")
        return value

    @staticmethod
    def _metadata(
        account_id: str,
        name: str,
        models: dict[str, list[str]],
        key_hint: str,
        *,
        enabled: bool = True,
        retired: bool = False,
        created_at: float | None = None,
    ) -> dict[str, Any]:
        current = _now()
        return {
            "version": 1,
            "id": account_id,
            "name": name,
            "enabled": enabled,
            "retired": retired,
            "cleanup_pending": False,
            "models": models,
            "key_hint": key_hint,
            "endpoints": dict(ENDPOINTS),
            "created_at": created_at or current,
            "updated_at": current,
        }

    def _write_account_files(
        self,
        root: Path,
        *,
        runtime_root: Path | None = None,
        account_id: str,
        name: str,
        api_key: str,
        models: dict[str, list[str]],
    ) -> None:
        _ensure_private_directory(root)
        claude_dir = root / "claude"
        codex_dir = root / "codex"
        _ensure_private_directory(claude_dir)
        _ensure_private_directory(codex_dir)

        helper = root / "key-helper"
        runtime_helper = (runtime_root or root) / "key-helper"
        _atomic_private_write(helper, KEY_HELPER.encode("utf-8"), mode=0o700)
        _atomic_private_write(root / "api.key", api_key.encode("utf-8"))

        settings = {
            "env": {"ANTHROPIC_BASE_URL": CLAUDE_BASE_URL},
            "apiKeyHelper": _claude_helper_command(runtime_root or root),
            CLAUDE_SKIP_DANGEROUS_PROMPT: True,
        }
        _atomic_private_json(claude_dir / "settings.json", settings)
        _atomic_private_json(
            claude_dir / ".claude.json", {"hasCompletedOnboarding": True},
        )

        quoted_helper = json.dumps(str(runtime_helper))
        codex_config = (
            'model_provider = "cloudrouter"\n\n'
            "[model_providers.cloudrouter]\n"
            'name = "CloudRouter"\n'
            f"base_url = {json.dumps(CODEX_BASE_URL)}\n"
            'wire_api = "responses"\n'
            "supports_websockets = false\n\n"
            "[model_providers.cloudrouter.auth]\n"
            f"command = {quoted_helper}\n"
            "timeout_ms = 5000\n"
            "refresh_interval_ms = 0\n"
        )
        _atomic_private_write(
            codex_dir / "config.toml", codex_config.encode("utf-8"),
        )
        _atomic_private_json(
            root / "account.json",
            self._metadata(account_id, name, models, _key_hint(api_key)),
        )

    async def add_account(self, name: str, api_key: str) -> CloudRouterAccount:
        clean_name = str(name or "").strip()
        if not clean_name or len(clean_name) > 100 or any(
            ord(character) < 32 for character in clean_name
        ):
            raise ValueError("Account name must be 1-100 printable characters")
        if (
            not isinstance(api_key, str)
            or not api_key
            or api_key.strip() != api_key
            or "\r" in api_key
            or "\n" in api_key
            or len(api_key.encode("utf-8")) > MAX_API_KEY_BYTES
        ):
            raise ValueError("Invalid API key")

        models = await self.probe_models(api_key)
        async with self._mutation_lock:
            self.reload()
            account_id = self._next_account_id()
            target = self._account_root(account_id)
            temporary = Path(tempfile.mkdtemp(
                prefix=f".{account_id}.", suffix=".tmp", dir=self.root,
            ))
            os.chmod(temporary, 0o700)
            try:
                self._write_account_files(
                    temporary,
                    runtime_root=target,
                    account_id=account_id,
                    name=clean_name,
                    api_key=api_key,
                    models=models,
                )
                if target.exists() or target.is_symlink():
                    raise CloudRouterUnsafePathError("Account destination already exists")
                os.rename(temporary, target)
                _fsync_directory(self.root)
            finally:
                if temporary.exists() and not temporary.is_symlink():
                    shutil.rmtree(temporary)
            self.reload()
            return self._require_account(account_id)

    async def refresh_account(self, account_id: str) -> CloudRouterAccount:
        async with self._mutation_lock:
            self.reload()
            account = self._require_account(account_id)
            api_key = self._read_api_key(account)
            models = await self.probe_models(api_key)
            metadata_path = account.root / "account.json"
            data = json.loads(
                _open_regular_nofollow(
                    metadata_path, maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"),
            )
            data["models"] = models
            data["updated_at"] = _now()
            _atomic_private_json(metadata_path, data)
            self.reload()
            return self._require_account(account_id)

    async def fetch_usage(
        self, account_id: str, force: bool = False,
    ) -> dict[str, Any]:
        account = self._require_account(account_id)
        current = _now()
        cached = self._quota_cache.get(account_id)
        if (
            not force
            and cached is not None
            and current - self._quota_cached_at.get(account_id, 0.0)
            < self._quota_cache_ttl
        ):
            return dict(cached)
        try:
            payload = await self._request_json(
                USAGE_URL, self._read_api_key(account),
            )
            snapshot = _normalise_usage(account_id, payload)
        except CloudRouterUpstreamError as exc:
            if exc.status_code in {401, 403}:
                snapshot = _unavailable_snapshot(account_id, exc.code)
            else:
                snapshot = _unknown_snapshot(
                    account_id, exc.code, previous=cached,
                )
        except CloudRouterUnsafePathError:
            snapshot = _unavailable_snapshot(account_id, "invalid_local_credentials")
        self._quota_cache[account_id] = snapshot
        self._quota_cached_at[account_id] = current
        return dict(snapshot)

    def cached_quota_decision(self, account_id: str) -> dict[str, Any]:
        account = self.account(account_id)
        if account is None or account.retired or not account.enabled:
            return {"available": False, "known": True, "reason": "disabled"}
        snapshot = self._quota_cache.get(account_id)
        if not snapshot:
            return {"available": True, "known": False, "reason": "not_fetched"}
        if (
            not bool(snapshot.get("known"))
            and snapshot.get("last_known_available") is False
        ):
            return {
                "available": False,
                "known": True,
                "reason": str(
                    snapshot.get("last_known_reason")
                    or snapshot.get("reason")
                    or "last_known_unavailable"
                ),
            }
        if (
            bool(snapshot.get("known"))
            and snapshot.get("available") is False
        ):
            return {
                "available": False,
                "known": True,
                "reason": str(snapshot.get("reason") or "unavailable"),
            }
        return {
            "available": bool(snapshot.get("available", True)),
            "known": bool(snapshot.get("known", False)),
            "reason": str(snapshot.get("reason") or "unknown"),
        }

    @staticmethod
    def _remove_except(directory: Path, preserved_name: str) -> None:
        if directory.is_symlink() or not directory.is_dir():
            raise CloudRouterUnsafePathError(f"Unsafe account directory: {directory}")
        for child in directory.iterdir():
            if child.name == preserved_name:
                if child.is_symlink() or not child.is_dir():
                    raise CloudRouterUnsafePathError(
                        f"Unsafe preserved account directory: {child}",
                    )
                continue
            if child.is_symlink() or child.is_file():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    async def retire_account(self, account_id: str) -> CloudRouterAccount:
        async with self._mutation_lock:
            self.reload()
            account = self._require_account(account_id, allow_retired=True)
            if account.retired and not account.cleanup_pending:
                return account

            metadata_path = account.root / "account.json"
            data = json.loads(
                _open_regular_nofollow(
                    metadata_path, maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"),
            )
            if not account.retired:
                # Disable first.  Existing pool projections consult the Store's
                # quota decision on every selection, so no new turn can enter
                # while credentials and runtime configuration are removed.
                data.update({
                    "enabled": False,
                    "retired": True,
                    "cleanup_pending": True,
                    "updated_at": _now(),
                })
                _atomic_private_json(metadata_path, data)
                _fsync_directory(account.root)
                self.reload()
                account = self._require_account(account_id, allow_retired=True)

            self._remove_except(account.root / "claude", "projects")
            self._remove_except(account.root / "codex", "sessions")
            for name in ("api.key", "key-helper"):
                target = account.root / name
                if target.is_symlink():
                    target.unlink()
                elif target.exists():
                    if not target.is_file():
                        raise CloudRouterUnsafePathError(
                            f"Unsafe account credential path: {target}",
                        )
                    target.unlink()
            data = json.loads(
                _open_regular_nofollow(
                    metadata_path, maximum=MAX_METADATA_BYTES,
                ).decode("utf-8"),
            )
            data.update({
                "enabled": False,
                "retired": True,
                "cleanup_pending": False,
                "updated_at": _now(),
            })
            _atomic_private_json(metadata_path, data)
            _fsync_directory(account.root)
            self._quota_cache.pop(account_id, None)
            self._quota_cached_at.pop(account_id, None)
            self.reload()
            return self._require_account(account_id, allow_retired=True)
