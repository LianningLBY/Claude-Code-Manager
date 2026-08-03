from datetime import datetime
from sqlalchemy import (
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    Boolean,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column
from backend.database import Base


class MonitoredRepo(Base):
    __tablename__ = "monitored_repos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_full_name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # NULL = local, else Worker
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")
    auto_merge: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0")
    webhook_secret: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(20), default="claude", server_default="claude")
    review_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    review_effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    default_branch: Mapped[str] = mapped_column(String(100), default="main", server_default="main")
    allowed_authors: Mapped[dict | None] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(20), default="active", server_default="active")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PRReview(Base):
    __tablename__ = "pr_reviews"
    __table_args__ = (
        UniqueConstraint(
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
            name="uq_pr_reviews_repo_pr_base_head",
        ),
        UniqueConstraint(
            "repo_id",
            "delivery_id",
            name="uq_pr_reviews_repo_delivery",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id: Mapped[int] = mapped_column(Integer, ForeignKey("monitored_repos.id"), index=True, nullable=False)
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False)
    base_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    delivery_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    pr_title: Mapped[str] = mapped_column(String(500), nullable=False)
    pr_author: Mapped[str] = mapped_column(String(200), nullable=False)
    pr_url: Mapped[str] = mapped_column(String(500), nullable=False)
    task_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tasks.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending", server_default="pending")
    review_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_taken: Mapped[str | None] = mapped_column(String(50), nullable=True)
    # Durable GitHub publication outbox. The nonce is generated before the
    # review Task starts; pending fields are populated by the exact completed
    # Task generation before any external write.
    action_nonce: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pending_action: Mapped[str | None] = mapped_column(String(50), nullable=True)
    pending_review_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    publishing_actor: Mapped[str | None] = mapped_column(
        String(200),
        nullable=True,
    )
    publishing_retry_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    publishing_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    publishing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    # Cross-process publication lease.  ``publishing`` alone is a durable
    # outbox state, but multiple CCM processes can recover the same row at
    # once.  Only the holder of this random fencing token may call GitHub.
    publishing_lease_token: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    publishing_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    # Durable synchronize intent.  The immutable target snapshot is committed
    # before the old Task is stopped, so a crash can resume replacement
    # creation instead of stranding a completed Task in ``reviewing``.
    superseding_snapshot: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )
    superseding_token: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    superseding_started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
