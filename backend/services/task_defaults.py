"""Canonical runtime defaults for newly-created executable Tasks."""

from backend.config import settings


def resolve_task_runtime_defaults(
    *,
    provider: str | None,
    model: str | None,
    effort_level: str | None,
) -> tuple[str, str, str]:
    """Resolve the persisted provider/model/effort tuple for a new Task.

    Every Task creation adapter must persist this tuple explicitly.  Falling
    through to ORM defaults is unsafe because the legacy database default for
    ``provider`` remains Claude while the deployment default may be Codex.
    """

    resolved_provider = (provider or settings.default_provider).strip().lower()
    if resolved_provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' or 'codex'")
    resolved_model = model or (
        settings.default_codex_model
        if resolved_provider == "codex"
        else settings.default_model
    )
    resolved_effort = effort_level or settings.default_effort
    return resolved_provider, resolved_model, resolved_effort
