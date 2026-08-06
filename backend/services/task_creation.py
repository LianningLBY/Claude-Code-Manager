"""Canonical transaction-aware creation boundary for executable Tasks."""

from collections.abc import Mapping
import secrets

from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.task import Task
from backend.models.task_share import TaskShare
from backend.models.team_share import TeamTaskShare
from backend.services.codex_models import validate_codex_service_tier


async def purge_task_access_grants(db: AsyncSession, task_id: int) -> None:
    """Remove every ACL row whose authority is one exact Task id.

    SQLite deployments have historically run without foreign-key enforcement,
    and ``TeamTaskShare`` has no database foreign key at all.  Explicit cleanup
    is therefore required both when deleting a Task and when admitting a new
    Task whose integer id may have been reused.
    """

    from sqlalchemy import delete

    await db.execute(delete(TaskShare).where(TaskShare.task_id == task_id))
    await db.execute(
        delete(TeamTaskShare).where(TeamTaskShare.task_id == task_id)
    )


def resolve_task_runtime_defaults(
    *,
    provider: str | None,
    model: str | None,
    effort_level: str | None,
) -> tuple[str, str, str]:
    """Resolve the explicit provider/model/effort tuple for a new Task."""

    raw_provider = settings.default_provider if provider is None else provider
    if not isinstance(raw_provider, str) or not raw_provider.strip():
        raise ValueError("provider must be 'claude' or 'codex'")
    resolved_provider = raw_provider.strip().lower()
    if resolved_provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' or 'codex'")
    resolved_model = model or (
        settings.default_codex_model
        if resolved_provider == "codex"
        else settings.default_model
    )
    resolved_effort = effort_level or settings.default_effort
    return resolved_provider, resolved_model, resolved_effort


def prepare_task_create_values(values: Mapping[str, object]) -> dict:
    """Return canonical persisted values shared by every creation adapter."""

    prepared = dict(values)
    provider, model, effort_level = resolve_task_runtime_defaults(
        provider=prepared.get("provider"),
        model=prepared.get("model"),
        effort_level=prepared.get("effort_level"),
    )
    prepared.update(
        provider=provider,
        model=model,
        effort_level=effort_level,
        codex_service_tier=prepared.get("codex_service_tier") or "default",
    )
    return prepared


async def stage_task_record(db: AsyncSession, **values) -> Task:
    """Add and flush one canonical Task without owning the transaction.

    Callers such as Plan materialization can atomically persist related rows
    before committing.  Standalone creation adapters may commit immediately.
    """

    prepared = prepare_task_create_values(values)
    # Incarnations are system authority, never caller-controlled.  This also
    # upgrades explicit-id/internal creation paths to the same ABA fence.
    prepared["incarnation_id"] = secrets.token_hex(16)
    validate_task_service_tier_configuration(
        provider=prepared["provider"],
        model=prepared["model"],
        codex_service_tier=prepared["codex_service_tier"],
        mode=prepared.get("mode"),
        goal_evaluator_model=prepared.get("goal_evaluator_model"),
    )
    task = Task(**prepared)
    db.add(task)
    await db.flush()
    # A newly flushed Task cannot yet have legitimate grants.  Clear orphaned
    # rows left by older deployments before this reused integer id becomes
    # visible, otherwise the new Task would inherit the previous Task's ACL.
    await purge_task_access_grants(db, task.id)
    return task


def validate_task_service_tier_configuration(
    *,
    provider: str | None,
    model: str | None,
    codex_service_tier: str | None,
    mode: str | None,
    goal_evaluator_model: str | None,
) -> None:
    """Validate every model request hidden behind one Task configuration."""

    validate_codex_service_tier(provider, model, codex_service_tier)
    if (
        (provider or "claude").lower() == "codex"
        and (codex_service_tier or "default") == "priority"
        and mode == "plan"
    ):
        raise ValueError(
            "Codex Fast is not supported for read-only Plan Agent tasks; "
            "use Standard"
        )
    if not (
        (provider or "claude").lower() == "codex"
        and (codex_service_tier or "default") == "priority"
        and mode == "goal"
    ):
        return

    task_model = model
    if not task_model or task_model == "default":
        task_model = settings.default_codex_model
    evaluator_model = goal_evaluator_model
    if not evaluator_model or evaluator_model == "default":
        evaluator_model = task_model
    if evaluator_model != task_model:
        raise ValueError(
            "Codex Fast Goal tasks must use the Task model for goal "
            "evaluation; clear goal_evaluator_model or select the same model"
        )
    validate_codex_service_tier("codex", evaluator_model, "priority")
