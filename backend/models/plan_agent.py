"""Durable audit records for independent Plan Task pipelines."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class PlanAgentRun(Base):
    __tablename__ = "plan_agent_runs"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    # Nullable legacy mapping during cutover. New runs are owned by ``plan_id``.
    plan_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    plan_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    run_type: Mapped[str] = mapped_column(String(30), nullable=False, default="legacy")
    source_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    base_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Planner proposals remain mutable Run-scoped candidates until the
    # Planner/Reviewer pipeline reaches a terminal review outcome. Only then
    # is the final candidate materialized as an immutable PlanVersion.
    draft_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    draft_step_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    draft_repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    request_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachments: Mapped[list | None] = mapped_column(JSON, nullable=True)
    context_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    context_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    context_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    current_stage: Mapped[str] = mapped_column(String(30), nullable=False, default="planner")
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    relay_origin: Mapped[str | None] = mapped_column(String(30), nullable=True)
    open_input_request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    interaction_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_interactions: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    execution_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    last_execution_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="planning", index=True
    )
    combo_used: Mapped[str | None] = mapped_column(String(20), nullable=True)
    planner_provider: Mapped[str | None] = mapped_column(String(20), nullable=True)
    planner_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    planner_effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    reviewer_provider: Mapped[str | None] = mapped_column(String(20), nullable=True)
    reviewer_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    reviewer_effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    pipeline_config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    review_verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    review_feedback: Mapped[str | None] = mapped_column(Text, nullable=True)
    review_exhausted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PlanAgentStep(Base):
    __tablename__ = "plan_agent_steps"
    __table_args__ = (
        UniqueConstraint(
            "worker_id", "worker_step_id", name="uq_plan_steps_worker_id"
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    plan_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    worker_step_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    step_type: Mapped[str] = mapped_column(String(20), nullable=False)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    provider: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    route_slot: Mapped[str | None] = mapped_column(String(20), nullable=True)
    account_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running"
    )
    output: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_delta_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    streamed_output_chars: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    last_event_type: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
