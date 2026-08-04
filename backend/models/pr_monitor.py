from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    Boolean,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    false,
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
    review_mode: Mapped[str] = mapped_column(
        String(20), default="single", server_default="single"
    )
    wait_for_ci: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=false()
    )
    required_checks: Mapped[list] = mapped_column(
        JSON, default=list, nullable=False
    )
    auto_repair: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=false()
    )
    max_repair_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    merge_queue_mode: Mapped[str] = mapped_column(
        String(20), default="manual", server_default="manual"
    )
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
    monitor_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pr_monitor_runs.id"), nullable=True, index=True
    )
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
    ci_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ci_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    ci_details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
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


class PRReviewerRun(Base):
    """One independent reviewer role for an immutable PRReview snapshot."""

    __tablename__ = "pr_reviewer_runs"
    __table_args__ = (
        UniqueConstraint(
            "pr_review_id",
            "role",
            name="uq_pr_reviewer_runs_review_role",
        ),
        UniqueConstraint(
            "task_id",
            name="uq_pr_reviewer_runs_task_id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pr_review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    role: Mapped[str] = mapped_column(String(40), nullable=False)
    task_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tasks.id"), nullable=True, index=True
    )
    provider: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    effort: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", server_default="pending"
    )
    verdict: Mapped[str | None] = mapped_column(String(30), nullable=True)
    result_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    prompt_policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    guide_pack_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRFinding(Base):
    """Structured, exact-subject evidence emitted by one reviewer role."""

    __tablename__ = "pr_findings"
    __table_args__ = (
        UniqueConstraint(
            "reviewer_run_id",
            "fingerprint",
            name="uq_pr_findings_run_fingerprint",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pr_review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    reviewer_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviewer_runs.id"), nullable=False, index=True
    )
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    role: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    path: Mapped[str] = mapped_column(String(1000), nullable=False)
    line: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hunk: Mapped[str | None] = mapped_column(String(500), nullable=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    evidence: Mapped[str] = mapped_column(Text, nullable=False)
    impact: Mapped[str] = mapped_column(Text, nullable=False)
    required_fix: Mapped[str] = mapped_column(Text, nullable=False)
    test: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="open", server_default="open"
    )
    thread_nonce: Mapped[str] = mapped_column(String(64), nullable=False)
    thread_status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", server_default="pending"
    )
    # GitHub database IDs are not bounded by a signed 32-bit SQL INTEGER.
    github_comment_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    github_comment_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    github_thread_node_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    thread_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    thread_published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    thread_resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # One durable cross-process fence covers both accepted-rebuttal and
    # newer-green-head resolution effects for this exact Finding thread.
    resolution_lease_token: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    resolution_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    # Freeze the authenticated identity before a fixed-head fallback comment
    # is attempted so crash recovery can authenticate an existing marker.
    fixed_resolution_actor: Mapped[str | None] = mapped_column(
        String(200), nullable=True
    )
    base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class PRFindingAction(Base):
    """Audited user decision or confirmed patch for one review Finding."""

    __tablename__ = "pr_finding_actions"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key",
            name="uq_pr_finding_actions_idempotency_key",
        ),
        CheckConstraint(
            "action_type IN ('ignore', 'human_advice', 'ai_fix')",
            name="ck_pr_finding_actions_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'running', 'awaiting_confirmation', "
            "'completed', 'failed', 'cancelled', 'stale')",
            name="ck_pr_finding_actions_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finding_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pr_findings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    action_type: Mapped[str] = mapped_column(String(30), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30),
        default="pending",
        server_default="pending",
        nullable=False,
        index=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    human_advice: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("tasks.id"),
        nullable=True,
        index=True,
    )
    expected_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    patch_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRFindingRebuttal(Base):
    """Evidence-based challenge adjudicated by an isolated Reviewer Task."""

    __tablename__ = "pr_finding_rebuttals"
    __table_args__ = (
        UniqueConstraint(
            "finding_id", "attempt", name="uq_pr_finding_rebuttals_attempt"
        ),
        UniqueConstraint("task_id", name="uq_pr_finding_rebuttals_task_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finding_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_findings.id"), nullable=False, index=True
    )
    pr_review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    monitor_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_monitor_runs.id"), nullable=False, index=True
    )
    developer_task_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tasks.id"), nullable=False, index=True
    )
    task_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tasks.id"), nullable=True, index=True
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", server_default="pending"
    )
    verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    result_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    resolution_nonce: Mapped[str] = mapped_column(String(64), nullable=False)
    resolution_actor: Mapped[str | None] = mapped_column(String(200), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRMonitorRun(Base):
    """One durable PR lifecycle spanning immutable review heads."""

    __tablename__ = "pr_monitor_runs"
    __table_args__ = (UniqueConstraint("repo_id", "pr_number", name="uq_pr_monitor_runs_repo_pr"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id: Mapped[int] = mapped_column(Integer, ForeignKey("monitored_repos.id"), nullable=False, index=True)
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="observing", server_default="observing")
    current_base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    current_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    current_review_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    developer_task_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tasks.id"), nullable=True, index=True)
    head_repo_full_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    head_branch: Mapped[str | None] = mapped_column(String(200), nullable=True)
    repair_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_repair_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    no_progress_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    state_version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    pause_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    binding_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRRepairWake(Base):
    """Durable idempotent instruction to resume one Developer Task."""

    __tablename__ = "pr_repair_wakes"
    __table_args__ = (UniqueConstraint("monitor_run_id", "trigger_head_sha", "evidence_hash", name="uq_pr_repair_wakes_subject_evidence"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    monitor_run_id: Mapped[int] = mapped_column(Integer, ForeignKey("pr_monitor_runs.id"), nullable=False, index=True)
    review_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("pr_reviews.id"), nullable=True, index=True)
    developer_task_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tasks.id"), nullable=True, index=True)
    trigger_base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_kind: Mapped[str] = mapped_column(String(30), nullable=False)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="shadow", server_default="shadow")
    attempt: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    delivery_token: Mapped[str] = mapped_column(String(64), nullable=False)
    accepted_worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_task_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_session_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    accepted_task_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    accepted_task_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PRMergeQueueAction(Base):
    """Durable enqueue/merge-group outbox for one exact PR head."""

    __tablename__ = "pr_merge_queue_actions"
    __table_args__ = (
        UniqueConstraint(
            "monitor_run_id", "trigger_head_sha",
            name="uq_pr_merge_queue_actions_run_head",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    monitor_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_monitor_runs.id"), nullable=False, index=True
    )
    review_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pr_reviews.id"), nullable=False, index=True
    )
    trigger_base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_head_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), default="shadow", server_default="shadow", nullable=False
    )
    action_nonce: Mapped[str] = mapped_column(String(64), nullable=False)
    github_pr_node_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    github_queue_entry_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    merge_group_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    merge_group_ref: Mapped[str | None] = mapped_column(String(500), nullable=True)
    ci_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ci_details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
