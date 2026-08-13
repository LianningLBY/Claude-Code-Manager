"""Effective values for persisted Manager runtime settings."""

from backend.config import settings


def effective_agent_sandbox_unrestricted_enabled(row) -> bool:
    """Resolve the DB operator override, falling back to the boot setting."""

    override = getattr(
        row,
        "agent_sandbox_unrestricted_enabled",
        None,
    )
    if override is None:
        return bool(settings.agent_sandbox_unrestricted_enabled)
    return bool(override)
