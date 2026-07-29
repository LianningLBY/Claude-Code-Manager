"""Durable audit records for independent Plan Task pipelines."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class PlanAgentRun(Base):
    __tablename__ = "plan_agent_runs"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    plan_task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
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

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    step_type: Mapped[str] = mapped_column(String(20), nullable=False)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    provider: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running"
    )
    output: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
