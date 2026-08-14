"""Public contracts for the autonomous Delivery Loop mode."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.schemas.plan_resource import (
    PlanInputRequestResponse,
    PlanRunResource,
)


class DeliveryRunCreate(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=128)
    project_id: int = Field(gt=0)
    monitored_repo_id: int = Field(gt=0)
    title: str = Field(min_length=1, max_length=200)
    requirements: str = Field(min_length=1, max_length=200_000)
    source_todo_id: int | None = Field(default=None, gt=0)
    base_branch: str | None = Field(default=None, min_length=1, max_length=200)
    # Both local coding providers use a fail-closed Delivery isolation
    # profile.  Keep Codex as the wire-compatible default for older clients.
    provider: Literal["claude", "codex"] = "codex"
    model: str | None = Field(default=None, max_length=100)
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = Field(default=None, max_length=20)
    timeout_hours: float | None = Field(default=None, ge=0, le=168)
    max_cycles: int = Field(default=10, ge=1, le=100)
    max_no_progress: int = Field(default=3, ge=1, le=20)
    # Omitted callers retain the selected Monitor's legacy repository policy.
    # First-party quick-start always freezes an explicit per-Run choice.
    auto_merge: bool | None = None
    frontend_review: Literal["auto", "required", "off"] = "auto"

    model_config = ConfigDict(extra="forbid")

    @field_validator(
        "idempotency_key",
        "title",
        "requirements",
        "base_branch",
        "model",
        "effort_level",
    )
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be blank")
        return normalized


class DeliveryQuickStartCreate(BaseModel):
    """One-message Delivery admission with lazy PR Monitor bootstrap."""

    idempotency_key: str = Field(min_length=1, max_length=128)
    project_id: int = Field(gt=0)
    requirements: str = Field(min_length=1, max_length=200_000)
    title: str | None = Field(default=None, max_length=200)
    timeout_hours: float | None = Field(default=None, ge=0, le=168)
    max_cycles: int = Field(default=10, ge=1, le=100)
    max_no_progress: int = Field(default=3, ge=1, le=20)
    auto_merge: bool = False
    frontend_review: Literal["auto", "required", "off"] = "auto"

    model_config = ConfigDict(extra="forbid")

    @field_validator("idempotency_key", "requirements", "title")
    @classmethod
    def strip_quick_start_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be blank")
        return normalized


class DeliveryRunResponse(BaseModel):
    id: int
    created_by: int | None
    project_id: int
    monitored_repo_id: int | None
    source_todo_id: int | None
    developer_task_id: int | None
    pr_monitor_run_id: int | None
    worktree_id: int | None
    title: str
    requirements: str
    requirements_hash: str
    policy_hash: str
    base_branch: str
    delivery_branch: str
    workspace_path: str | None
    base_sha: str | None
    head_sha: str | None
    head_tree_sha: str | None
    patch_sha256: str | None
    head_generation: int
    pr_number: int | None
    pr_url: str | None
    phase: str
    activity: str
    outcome: str | None
    # Frozen completion policy selected from the PR Monitor at admission.
    terminal: Literal["ready_to_merge", "merged"] | None = None
    wait_reason: str | None
    pause_reason: str | None
    error_code: str | None
    error_message: str | None
    state_version: int
    current_cycle_id: int | None
    cycle_count: int
    turn_count: int
    max_cycles: int
    no_progress_count: int
    max_no_progress: int
    next_reconcile_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    allowed_actions: list[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class DeliveryCycleResponse(BaseModel):
    id: int
    run_id: int
    cycle_number: int
    status: str
    state_version: int
    trigger_kind: str
    trigger_payload: dict
    base_sha: str | None
    start_head_sha: str | None
    result_head_sha: str | None
    result_head_tree_sha: str | None
    result_patch_sha256: str | None
    plan_invocation_id: int | None
    plan_version_id: int | None
    review_invocation_id: int | None
    review_result_id: int | None
    review_verdict: str | None
    review_summary: str | None
    frontend_review_run_id: str | None
    frontend_review_profile_ids: list[str] = Field(default_factory=list)
    frontend_review_profile_index: int = 0
    frontend_review_results: list[dict] = Field(default_factory=list)
    frontend_review_verdict: str | None
    frontend_review_summary: str | None
    frontend_review_skip_reason: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class DeliveryTurnResponse(BaseModel):
    id: int
    run_id: int
    cycle_id: int
    generation: int
    correlation_id: str
    purpose: str
    trigger_kind: str
    trigger_payload: dict
    status: str
    task_id: int | None
    task_retry_count: int | None
    task_instance_id: int | None
    task_started_at: datetime | None
    task_session_id: str | None
    checkpoint: dict | None
    checkpoint_status: str | None
    attempts: int
    last_error: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class DeliveryTransitionResponse(BaseModel):
    id: int
    run_id: int
    state_version: int
    cause: str
    actor_kind: str
    actor_id: str | None
    before_state: dict
    after_state: dict
    metadata: dict | None = Field(default=None, validation_alias="metadata_")
    created_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class DeliveryRunDetail(DeliveryRunResponse):
    policy_snapshot: dict
    cycles: list[DeliveryCycleResponse]
    turns: list[DeliveryTurnResponse]
    transitions: list[DeliveryTransitionResponse]


class DeliveryCommand(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)

    model_config = ConfigDict(extra="forbid")

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("reason must not be blank")
        return normalized


class DeliveryResumeCommand(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)

    model_config = ConfigDict(extra="forbid")


class DeliveryRetryCommand(BaseModel):
    """Explicit operator retry of one failed pre-publication Run."""

    expected_state_version: int = Field(ge=1)
    reason: str | None = Field(default=None, max_length=2000)

    model_config = ConfigDict(extra="forbid")

    @field_validator("reason")
    @classmethod
    def normalize_optional_reason(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class DeliveryAgentActivity(BaseModel):
    role: Literal[
        "planner", "plan_reviewer", "developer", "code_reviewer", "browser_reviewer"
    ]
    provider: str | None = None
    model: str | None = None
    effort: str | None = None
    service_tier: str | None = None
    status: str
    activity_kind: str
    headline: str
    detail: str | None = None
    task_id: int | None = None
    source_kind: str
    source_id: str | None = None
    started_at: datetime | None = None
    first_output_at: datetime | None = None
    last_activity_at: datetime | None = None
    output_chars: int = 0


class DeliveryStageProgress(BaseModel):
    key: Literal[
        "planning",
        "coding",
        "pre_review",
        "frontend_review",
        "publishing",
        "monitoring",
    ]
    label: str
    state: Literal[
        "pending",
        "ready",
        "running",
        "waiting",
        "paused",
        "completed",
        "failed",
        "cancelled",
        "skipped",
    ]
    summary: str
    started_at: datetime | None = None
    completed_at: datetime | None = None


class DeliveryTimelineEvent(BaseModel):
    id: str
    stage: str
    kind: str
    source: str
    title: str
    detail: str | None = None
    status: str | None = None
    created_at: datetime


class DeliveryPlanInputProjection(BaseModel):
    plan_id: int
    run: PlanRunResource
    request: PlanInputRequestResponse


class DeliveryFrontendReviewProgress(BaseModel):
    policy: Literal["auto", "required", "off"]
    run_id: str | None = None
    status: str | None = None
    stage: str | None = None
    verdict: str | None = None
    report: str | None = None
    error: str | None = None
    cleanup_status: str | None = None
    evidence_archive_state: str | None = None
    finding_count: int = 0
    evidence_count: int = 0
    skip_reason: str | None = None


class DeliveryProgressResponse(BaseModel):
    run_id: int
    state_version: int
    phase: str
    activity: str
    headline: str
    detail: str | None = None
    attention_required: bool = False
    attention_kind: str | None = None
    last_activity_at: datetime | None = None
    stages: list[DeliveryStageProgress]
    active_agent: DeliveryAgentActivity | None = None
    events: list[DeliveryTimelineEvent]
    plan_input: DeliveryPlanInputProjection | None = None
    frontend_review: DeliveryFrontendReviewProgress


class DeliveryAttentionCount(BaseModel):
    total: int
