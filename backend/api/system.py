import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import require_admin
from backend.config import settings
from backend.database import get_db
from backend.models.task import Task
from backend.models.instance import Instance

from backend.services.codex_models import (
    CODEX_MODEL_EFFORTS,
    CODEX_MODEL_SERVICE_TIERS,
    CODEX_SERVICE_TIERS,
    DEFAULT_CODEX_SERVICE_TIER,
)
from backend.services.claude_models import (
    CLAUDE_CONTEXT_WINDOWS,
    CLAUDE_MODEL_EFFORTS,
)
from backend.services.git_info import git_head_commit
from backend.services.legacy_plan_execution import (
    LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION,
)
from backend.services.pr_review_runtime import (
    PR_REVIEW_SNAPSHOT_CONTEXT_VERSION,
    PR_REVIEW_TERMINAL_CHAT_VERSION,
)
from backend.services.task_artifact_contract import (
    TASK_ARTIFACT_SCOPE_VERSION,
)

router = APIRouter(prefix="/api/system", tags=["system"])

# import 时一次性求值（~10ms）：health 端点保持零阻塞；cwd 固定仓库根
_GIT_COMMIT: str = git_head_commit()


@router.get("/health")
async def health():
    return {"status": "ok", "commit": _GIT_COMMIT}


@router.get("/stats")
async def stats(db: AsyncSession = Depends(get_db)):
    task_counts = {}
    for status in ("pending", "in_progress", "executing", "completed", "failed"):
        result = await db.execute(
            select(func.count()).select_from(Task).where(Task.status == status)
        )
        task_counts[status] = result.scalar()

    result = await db.execute(
        select(func.count()).select_from(Instance).where(Instance.status == "running")
    )
    running_instances = result.scalar()

    return {
        "tasks": task_counts,
        "running_instances": running_instances,
    }


@router.get("/config")
async def get_config(db: AsyncSession = Depends(get_db)):
    from backend.main import instance_manager
    from backend.services.plan_pipeline_settings import (
        effective_plan_pipeline_config,
    )

    plan_pipeline = await effective_plan_pipeline_config(db)

    return {
        "default_model": settings.default_model,
        "model_options": [m.strip() for m in settings.model_options.split(",") if m.strip()],
        "default_provider": settings.default_provider,
        "provider_options": [p.strip() for p in settings.provider_options.split(",") if p.strip()],
        "default_codex_model": settings.default_codex_model,
        "codex_model_options": [m.strip() for m in settings.codex_model_options.split(",") if m.strip()],
        "default_effort": settings.default_effort,
        "effort_options": [e.strip() for e in settings.effort_options.split(",") if e.strip()],
        "claude_model_efforts": CLAUDE_MODEL_EFFORTS,
        "claude_model_context_windows": CLAUDE_CONTEXT_WINDOWS,
        "codex_effort_options": [e.strip() for e in settings.codex_effort_options.split(",") if e.strip()],
        # GPT-5.6 系列按模型区分档位（sol/terra 到 ultra，luna 到 max）；未列出的模型用 codex_effort_options
        "codex_model_efforts": CODEX_MODEL_EFFORTS,
        "default_codex_service_tier": DEFAULT_CODEX_SERVICE_TIER,
        "codex_service_tier_options": list(CODEX_SERVICE_TIERS),
        "codex_model_service_tiers": CODEX_MODEL_SERVICE_TIERS,
        "versioned_plan_worker_protocol": 3,
        # Worker Task deletion returns/serves an exact Task+Plan cascade proof.
        # Managers with first-class Plan mirrors fail closed on older Workers.
        "plan_cascade_protocol": 1,
        "worker_plan_reconciliation_protocol": 1,
        # Exact cancellation is authenticated by the permanent Worker import
        # receipt; an absent Run creates a tombstone before returning success.
        "worker_plan_exact_cancel_protocol": 1,
        # A Manager may resume a pre-Plan-v2 approved carrier already present
        # on a Worker only after exact semantic readback.  This protocol never
        # authorizes generic mode=plan Task creation.
        "legacy_plan_execution_carrier_protocol": (
            LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
        ),
        "plan_pipeline_defaults": plan_pipeline.model_dump(mode="json"),
        "capability_core_enabled": settings.capability_core_enabled,
        "auto_capability_enabled": (
            settings.auto_capability_enabled
            and settings.capability_core_enabled
        ),
        "delivery_loop_enabled": (
            settings.delivery_loop_enabled
            and settings.capability_core_enabled
        ),
        # Operator-owned emergency switch. Expose the live effective value so
        # Task creation can warn before a new turn is admitted.
        "agent_sandbox_unrestricted_enabled": (
            instance_manager.agent_sandbox_unrestricted_enabled
        ),
        # Manager must see this exact capability before forwarding a PR review
        # to a Worker. Older Workers would run it from their CCM checkout and
        # silently load unrelated CLAUDE.md/AGENTS.md instructions.
        "pr_review_snapshot_context_version": (
            PR_REVIEW_SNAPSHOT_CONTEXT_VERSION
        ),
        # A Manager must confirm this before persisting a terminal PR-review
        # follow-up for a Worker. Older Workers still freeze every PR chat.
        "pr_review_terminal_chat_version": PR_REVIEW_TERMINAL_CHAT_VERSION,
        # Manager-side ACL must not proxy the managed Task namespace to an
        # older Worker that lacks the same cross-Task path fence.
        "task_artifact_scope_version": TASK_ARTIFACT_SCOPE_VERSION,
        # Manager-side Task operation locks are not enough across processes.
        # This proves Worker Task mutations validate the exact logical
        # incarnation carried by the Manager proxy.
        "worker_task_incarnation_proxy_version": 1,
    }


@router.get("/skills/usage")
async def skill_usage_report(db: AsyncSession = Depends(get_db)):
    """Get skill usage statistics."""
    from backend.services.skill_curator import get_usage_report
    return await get_usage_report(db)


@router.post("/skills/curator", dependencies=[Depends(require_admin)])
async def run_skill_curator(db: AsyncSession = Depends(get_db)):
    """Manually trigger curator lifecycle management."""
    from backend.services.skill_curator import run_curator
    return await run_curator(db)


@router.post("/skills/distill", dependencies=[Depends(require_admin)])
async def distill_skills(db: AsyncSession = Depends(get_db)):
    """Analyze conversation history and propose new skill candidates."""
    from backend.services.skill_distill import analyze_patterns
    return await analyze_patterns(db)


_BRANCH_RE = re.compile(r'^[a-zA-Z0-9._/\-]+$')


class UpdateRequest(BaseModel):
    skip_frontend_build: bool = False
    dry_run: bool = False
    force: bool = False
    branch: str | None = None


class RollbackRequest(BaseModel):
    confirm_database_restore: bool = False


def _get_update_service():
    from backend.main import update_service
    if update_service is None:
        raise HTTPException(status_code=503, detail="UpdateService not initialized")
    return update_service


@router.post("/update", dependencies=[Depends(require_admin)])
async def start_update(req: UpdateRequest):
    if req.branch and not _BRANCH_RE.match(req.branch):
        raise HTTPException(status_code=400, detail="Invalid branch name")
    svc = _get_update_service()
    if req.dry_run:
        return await svc.dry_run(branch=req.branch, force=req.force)
    result = await svc.start_update(
        skip_frontend_build=req.skip_frontend_build,
        force=req.force,
        branch=req.branch,
    )
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.get("/update/status", dependencies=[Depends(require_admin)])
async def update_status():
    svc = _get_update_service()
    return await svc.get_status()


@router.post("/update/reconcile", dependencies=[Depends(require_admin)])
async def reconcile_update_blockers():
    result = await _get_update_service().reconcile_blockers()
    if "error" in result:
        raise HTTPException(status_code=409, detail=result)
    return result


@router.post("/update/rollback", dependencies=[Depends(require_admin)])
async def rollback_update(req: RollbackRequest | None = None):
    svc = _get_update_service()
    result = await svc.rollback(
        confirm_database_restore=bool(
            req and req.confirm_database_restore
        )
    )
    if "error" in result:
        if result.get("confirmation_required"):
            raise HTTPException(status_code=409, detail=result)
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/restart", dependencies=[Depends(require_admin)])
async def restart_service():
    result = await _get_update_service().restart()
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.post("/update/repair", dependencies=[Depends(require_admin)])
async def repair_update(req: UpdateRequest | None = None):
    result = await _get_update_service().start_repair(
        skip_frontend_build=req.skip_frontend_build if req else False
    )
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.get("/skills")
async def list_skills():
    """List all available skills (from SKILL.md files)."""
    from backend.services.skill_loader import discover_skills
    skills = discover_skills()
    return [
        {
            "key": name,
            "label": skill.name,
            "description": skill.description,
            "always": skill.ccm.always,
            "priority": skill.ccm.priority,
            "version": skill.ccm.version,
            "tags": skill.ccm.tags,
            "commands": skill.ccm.commands,
            "scope": skill.scope,
            "heavy": skill.ccm.heavy,
        }
        for name, skill in sorted(skills.items(), key=lambda x: x[1].ccm.priority, reverse=True)
    ]
