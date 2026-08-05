"""Durable, provider-neutral capability invocation audit models.

Capability invocations are orchestration records.  They do not directly own a
Claude/Codex process; an executor adapter links each execution attempt to its
own durable handle (for example a PlanAgentRun or a future CodeReviewRun).
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


ACTIVE_INVOCATION_STATUSES = (
    "queued",
    "running",
    "waiting_user",
    "ready",
    "resuming",
    "cancelling",
)
TERMINAL_INVOCATION_STATUSES = (
    "completed",
    "failed",
    "cancelled",
    "stale",
)
INVOCATION_STATUSES = ACTIVE_INVOCATION_STATUSES + TERMINAL_INVOCATION_STATUSES

ACTIVE_EXECUTION_STATUSES = (
    "queued",
    "running",
    "waiting_user",
    "cancelling",
)
TERMINAL_EXECUTION_STATUSES = (
    "completed",
    "failed",
    "cancelled",
    "stale",
)
EXECUTION_STATUSES = ACTIVE_EXECUTION_STATUSES + TERMINAL_EXECUTION_STATUSES


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class CapabilityInvocation(Base):
    """One logical request for a Plan, review, or future capability."""

    __tablename__ = "capability_invocations"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "idempotency_key",
            name="uq_cap_inv_task_idem",
        ),
        UniqueConstraint("active_task_id", name="uq_cap_inv_active_task"),
        CheckConstraint(
            f"status IN ({_sql_values(INVOCATION_STATUSES)})",
            name="ck_cap_inv_status",
        ),
        CheckConstraint(
            "source IN ('human_request', 'agent_request', "
            "'delivery_controller')",
            name="ck_cap_inv_source",
        ),
        CheckConstraint(
            "purpose IN ('advisory', 'required_gate')",
            name="ck_cap_inv_purpose",
        ),
        CheckConstraint(
            "resume_policy IN ('attach_only', 'resume_task', 'controller')",
            name="ck_cap_inv_resume_policy",
        ),
        CheckConstraint("state_version >= 1", name="ck_cap_inv_state_version"),
        CheckConstraint("max_attempts >= 1", name="ck_cap_inv_max_attempts"),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(ACTIVE_INVOCATION_STATUSES)}"
            ") AND active_task_id IS NOT NULL AND active_task_id = task_id) "
            "OR (status NOT IN ("
            f"{_sql_values(ACTIVE_INVOCATION_STATUSES)}"
            ") AND active_task_id IS NULL)",
            name="ck_cap_inv_active_slot",
        ),
        CheckConstraint(
            "status NOT IN ('ready', 'resuming', 'completed') OR "
            "(result_kind IS NOT NULL AND result_id IS NOT NULL AND "
            "result_hash IS NOT NULL)",
            name="ck_cap_inv_result",
        ),
        Index("ix_cap_inv_task_created", "task_id", "created_at"),
        Index("ix_cap_inv_status_created", "status", "created_at"),
        Index("ix_cap_inv_key_status", "capability_key", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    capability_key: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    purpose: Mapped[str] = mapped_column(String(24), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="queued", server_default="queued"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    input_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    subject_ref: Mapped[dict] = mapped_column(JSON, nullable=False)
    subject_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    executor_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    executor_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    executor_config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    resume_policy: Mapped[str] = mapped_column(String(24), nullable=False)
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    # Cross-dialect active-slot fence. Multiple NULL values are allowed by all
    # supported databases, while one active invocation stores its task id.
    active_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_task_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_task_instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    request_task_session_id: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    # Reserved for the later exact-turn MCP adapter. The first capability-core
    # API never accepts agent_request and therefore leaves it NULL.
    request_task_turn_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    request_source_log_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    result_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    ready_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CapabilityExecution(Base):
    """One technical attempt to execute a CapabilityInvocation."""

    __tablename__ = "capability_executions"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id",
            "attempt",
            name="uq_cap_exec_inv_attempt",
        ),
        UniqueConstraint("idempotency_key", name="uq_cap_exec_idem"),
        UniqueConstraint(
            "active_invocation_id",
            name="uq_cap_exec_active_inv",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(EXECUTION_STATUSES)})",
            name="ck_cap_exec_status",
        ),
        CheckConstraint("attempt >= 1", name="ck_cap_exec_attempt"),
        CheckConstraint("state_version >= 1", name="ck_cap_exec_state_version"),
        CheckConstraint(
            "(status IN ("
            f"{_sql_values(ACTIVE_EXECUTION_STATUSES)}"
            ") AND active_invocation_id IS NOT NULL "
            "AND active_invocation_id = invocation_id) OR "
            "(status NOT IN ("
            f"{_sql_values(ACTIVE_EXECUTION_STATUSES)}"
            ") AND active_invocation_id IS NULL)",
            name="ck_cap_exec_active_slot",
        ),
        CheckConstraint(
            "(handle_kind IS NULL AND handle_id IS NULL) OR "
            "(handle_kind IS NOT NULL AND handle_id IS NOT NULL)",
            name="ck_cap_exec_handle",
        ),
        CheckConstraint(
            "status <> 'completed' OR "
            "(output_kind IS NOT NULL AND output_id IS NOT NULL AND "
            "output_hash IS NOT NULL)",
            name="ck_cap_exec_output",
        ),
        Index("ix_cap_exec_inv_created", "invocation_id", "created_at"),
        Index("ix_cap_exec_status_created", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    invocation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("capability_invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="queued", server_default="queued"
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    active_invocation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    executor_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    handle_kind: Mapped[str | None] = mapped_column(String(64), nullable=True)
    handle_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    handle_generation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    output_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    output_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
