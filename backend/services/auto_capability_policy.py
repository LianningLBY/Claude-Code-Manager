"""Canonical validation boundary for Task-scoped Auto capability policy."""

from copy import deepcopy
from typing import Any

from pydantic import ValidationError

from backend.schemas.capability import AutoCapabilityPolicy


def normalize_auto_capability_policy(value: Any) -> dict | None:
    """Return canonical JSON or ``None``; reject every malformed non-NULL value."""

    if value is None:
        return None
    try:
        # Pydantic models are mutable by default and ``model_validate`` does
        # not revalidate an instance of the requested class.  Treat even an
        # already-typed policy as untrusted: a caller may have mutated it after
        # construction or produced it through ``model_construct``.  A detached
        # snapshot plus strict validation keeps this canonical boundary from
        # inheriting either invalid fields or caller-owned nested mappings.
        candidate = deepcopy(
            value.model_dump(mode="python")
            if isinstance(value, AutoCapabilityPolicy)
            else value
        )
        policy = AutoCapabilityPolicy.model_validate(candidate, strict=True)
    except ValidationError as exc:
        raise ValueError(f"Invalid capability_policy: {exc}") from exc
    return deepcopy(policy.model_dump(mode="json"))


def validate_auto_capability_task_scope(
    policy: Any,
    *,
    task_id: int | None = None,
    mode: str | None,
    worker_id: int | None,
    shared_from_id: int | None = None,
    delivery_run_id: int | None = None,
    delivery_role: str | None = None,
    plan_target_task_id: int | None = None,
) -> dict | None:
    """Normalize policy and enforce the V1 local ordinary-Task boundary."""

    normalized = normalize_auto_capability_policy(policy)
    if normalized is None:
        return None
    if mode != "auto":
        raise ValueError("capability_policy requires mode=auto")
    if worker_id is not None:
        raise ValueError("capability_policy is local-task only")
    if task_id is not None:
        raise ValueError(
            "Manager-forwarded Worker Tasks cannot use capability_policy"
        )
    if shared_from_id is not None:
        raise ValueError("Shared shadow Tasks cannot use capability_policy")
    if delivery_run_id is not None or delivery_role is not None:
        raise ValueError("Delivery-owned Tasks cannot use capability_policy")
    if plan_target_task_id is not None:
        raise ValueError("Plan helper Tasks cannot use capability_policy")
    return normalized
