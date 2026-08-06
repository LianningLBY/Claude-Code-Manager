"""First-class, versioned Plan aggregate models.

These tables deliberately use logical integer references instead of ORM
relationships/cascades. CCM supports SQLite, PostgreSQL, and MySQL and all
aggregate mutations validate references explicitly in ``plan_service``.
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class Plan(Base):
    __tablename__ = "plans"
    __table_args__ = (
        Index("ix_plans_target_task_archived", "target_task_id", "archived_at"),
        Index("ix_plans_created_by_archived", "created_by", "archived_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    initial_request: Mapped[str] = mapped_column(Text, nullable=False)
    initial_attachments: Mapped[list | None] = mapped_column(JSON, nullable=True)
    target_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    target_repo: Mapped[str | None] = mapped_column(String(500), nullable=True)
    target_branch: Mapped[str | None] = mapped_column(String(200), nullable=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    relay_origin: Mapped[str | None] = mapped_column(String(30), nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    timeout_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    pipeline_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    current_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    active_run_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    forked_from_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    lock_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PlanVersion(Base):
    __tablename__ = "plan_versions"
    __table_args__ = (
        UniqueConstraint(
            "plan_id", "version_number", name="uq_plan_versions_plan_number"
        ),
        UniqueConstraint("produced_by_step_id", name="uq_plan_versions_produced_step"),
        UniqueConstraint(
            "worker_id", "worker_version_id", name="uq_plan_versions_worker_id"
        ),
        Index("ix_plan_versions_plan_created", "plan_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    worker_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    parent_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    produced_by_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    produced_by_step_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    context_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    context_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    context_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reviewer_repo_revision: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    review_verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    review_feedback: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_by_step_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    review_exhausted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    human_decision: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    decided_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    decided_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    superseded_by_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PlanInputRequest(Base):
    __tablename__ = "plan_input_requests"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_plan_input_idempotency"),
        UniqueConstraint(
            "worker_id", "worker_input_request_id", name="uq_plan_inputs_worker_id"
        ),
        Index("ix_plan_inputs_plan_status", "plan_id", "status"),
        Index("ix_plan_inputs_run_status", "run_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    worker_input_request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_step_id: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_by: Mapped[str] = mapped_column(String(20), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The complete question list is durable and intentionally has no business
    # count limit. API/model payload size limits are the only outer bound.
    questions: Mapped[list] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="prepared")
    answers: Mapped[list | None] = mapped_column(JSON, nullable=True)
    response_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachments: Mapped[list | None] = mapped_column(JSON, nullable=True)
    answered_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    answer_idempotency_key: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    opened_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    answered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PlanApplication(Base):
    __tablename__ = "plan_applications"
    __table_args__ = (
        UniqueConstraint("plan_version_id", name="uq_plan_application_version"),
        CheckConstraint(
            "(application_type = 'chat_message' AND user_log_id IS NOT NULL "
            "AND execution_task_id IS NULL) OR "
            "(application_type = 'execution_task' AND execution_task_id IS NOT NULL "
            "AND user_log_id IS NULL)",
            name="ck_plan_application_target",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    plan_version_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    application_type: Mapped[str] = mapped_column(String(30), nullable=False)
    target_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    user_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    execution_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    applied_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    application_receipt_key: Mapped[str | None] = mapped_column(
        String(200), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PlanLegacyTaskLink(Base):
    __tablename__ = "plan_legacy_task_links"

    legacy_task_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    plan_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PlanApplicationReceipt(Base):
    """Durable Plan-application receipt and local chat delivery outbox."""

    __tablename__ = "plan_application_receipts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    receipt_key: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    target_task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    manager_user_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_version_ids: Mapped[list] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="prepared")
    response: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # ``status`` is the HTTP/Manager receipt state. Delivery is independent so
    # a committed response can still be recovered after a process restart.
    delivery_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    outbox_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    payload_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    delivery_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    launch_evidence: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    delivery_resolution: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PlanApplicationAttempt(Base):
    """Immutable Plan/application link retained after an application release."""

    __tablename__ = "plan_application_attempts"
    __table_args__ = (
        UniqueConstraint(
            "application_receipt_key",
            "plan_version_id",
            name="uq_plan_application_attempt_receipt_version",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    plan_version_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    application_receipt_key: Mapped[str] = mapped_column(
        String(200), nullable=False, index=True
    )
    application_type: Mapped[str] = mapped_column(String(30), nullable=False)
    target_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    user_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    execution_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    applied_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    application_created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    released_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
