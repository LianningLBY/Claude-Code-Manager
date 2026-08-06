"""Database-backed defaults for newly created independent Plan Tasks."""

from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.global_settings import GlobalSettings
from backend.schemas.plan import PlanPipelineConfig, default_plan_pipeline_config


async def effective_plan_pipeline_config(
    db: AsyncSession,
) -> PlanPipelineConfig:
    """Return the persisted global default or the deployment fallback."""

    row = await db.get(GlobalSettings, 1)
    if row is None or row.plan_pipeline_config is None:
        return default_plan_pipeline_config()
    return PlanPipelineConfig.model_validate(row.plan_pipeline_config)
