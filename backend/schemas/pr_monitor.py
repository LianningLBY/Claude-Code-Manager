import re
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from backend.config import settings

_GITHUB_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")


class RequiredCheckPolicy(BaseModel):
    """Stable identity of one required GitHub check/status producer."""

    name: str = Field(min_length=1, max_length=200)
    app_slug: str = Field(min_length=1, max_length=200)
    kind: str = "check_run"

    @field_validator("name", "app_slug")
    @classmethod
    def strip_identity(cls, value: str) -> str:
        value = value.strip()
        if not value or "\x00" in value or "\n" in value or "\r" in value:
            raise ValueError("required check identity must be one non-empty line")
        return value

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        if value not in {"check_run", "status"}:
            raise ValueError("required check kind must be 'check_run' or 'status'")
        return value


class MonitoredRepoCreate(BaseModel):
    repo_full_name: str
    project_id: int | None = None
    worker_id: int | None = None  # NULL = local, else Worker ID
    auto_merge: bool = False
    provider: str = Field(default_factory=lambda: settings.default_provider)
    review_model: str | None = None
    review_effort: str | None = None
    # API compatibility: callers must opt in explicitly. The first-party UI
    # defaults new monitors to the panel harness.
    review_mode: str = "single"
    wait_for_ci: bool = False
    required_checks: list[RequiredCheckPolicy] = Field(default_factory=list)
    auto_repair: bool = False
    max_repair_attempts: int = Field(default=3, ge=1, le=20)
    merge_queue_mode: str = "manual"
    default_branch: str = "main"
    allowed_authors: list[str] = []

    @field_validator("repo_full_name")
    @classmethod
    def validate_repo_name(cls, v: str) -> str:
        if _GITHUB_REPO_RE.fullmatch(v) is None:
            raise ValueError(
                "repo_full_name must be a literal GitHub 'owner/repo' name"
            )
        return v

    @field_validator("review_mode")
    @classmethod
    def validate_review_mode(cls, v: str) -> str:
        if v not in {"single", "panel"}:
            raise ValueError("review_mode must be 'single' or 'panel'")
        return v

    @field_validator("merge_queue_mode")
    @classmethod
    def validate_merge_queue_mode(cls, v: str) -> str:
        if v not in {"manual", "shadow", "auto"}:
            raise ValueError("merge_queue_mode must be manual, shadow, or auto")
        return v


class MonitoredRepoUpdate(BaseModel):
    project_id: int | None = None
    auto_merge: bool | None = None
    provider: str | None = None
    review_model: str | None = None
    review_effort: str | None = None
    review_mode: str | None = None
    wait_for_ci: bool | None = None
    required_checks: list[RequiredCheckPolicy] | None = None
    auto_repair: bool | None = None
    max_repair_attempts: int | None = Field(default=None, ge=1, le=20)
    merge_queue_mode: str | None = None
    default_branch: str | None = None
    allowed_authors: list[str] | None = None
    enabled: bool | None = None

    @field_validator(
        "auto_merge",
        "provider",
        "review_mode",
        "wait_for_ci",
        "required_checks",
        "auto_repair",
        "max_repair_attempts",
        "merge_queue_mode",
        "default_branch",
        "allowed_authors",
        "enabled",
        mode="before",
    )
    @classmethod
    def reject_explicit_null_for_non_nullable_fields(cls, value):
        if value is None:
            raise ValueError("field cannot be null")
        return value

    @field_validator("review_mode")
    @classmethod
    def validate_review_mode(cls, v: str | None) -> str | None:
        if v is not None and v not in {"single", "panel"}:
            raise ValueError("review_mode must be 'single' or 'panel'")
        return v

    @field_validator("merge_queue_mode")
    @classmethod
    def validate_merge_queue_mode(cls, v: str | None) -> str | None:
        if v is not None and v not in {"manual", "shadow", "auto"}:
            raise ValueError("merge_queue_mode must be manual, shadow, or auto")
        return v


class MonitoredRepoResponse(BaseModel):
    id: int
    repo_full_name: str
    project_id: int | None
    worker_id: int | None = None
    enabled: bool
    auto_merge: bool
    webhook_secret: str
    provider: str = "claude"
    review_model: str | None
    review_effort: str | None
    review_mode: str = "single"
    wait_for_ci: bool = False
    required_checks: list[RequiredCheckPolicy] = Field(default_factory=list)
    auto_repair: bool = False
    max_repair_attempts: int = 3
    merge_queue_mode: str = "manual"
    default_branch: str
    allowed_authors: list[str]
    status: str
    error_message: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

    @field_validator("webhook_secret", mode="before")
    @classmethod
    def mask_secret(cls, v: str) -> str:
        if v and len(v) > 4:
            return v[:4] + "***"
        return "***"

    @field_validator("allowed_authors", mode="before")
    @classmethod
    def ensure_list(cls, v) -> list[str]:
        if v is None:
            return []
        return v

    @field_validator("required_checks", mode="before")
    @classmethod
    def ensure_required_checks_list(cls, v):
        if v is None:
            return []
        return v


class MonitoredRepoDetailResponse(MonitoredRepoResponse):
    """Full detail response — shows unmasked webhook_secret."""

    # NOTE: must reuse the parent's validator method name ("mask_secret") so it
    # actually overrides it in Pydantic v2; a differently-named validator would
    # be registered IN ADDITION to the parent's, leaving the secret masked.
    @field_validator("webhook_secret", mode="before")
    @classmethod
    def mask_secret(cls, v: str) -> str:
        return v


class PRReviewResponse(BaseModel):
    id: int
    monitor_run_id: int | None = None
    repo_id: int
    pr_number: int
    base_sha: str | None
    head_sha: str | None
    delivery_id: str | None
    pr_title: str
    pr_author: str
    pr_url: str
    task_id: int | None
    status: str
    review_summary: str | None
    action_taken: str | None
    ci_status: str | None = None
    ci_summary: str | None = None
    ci_details: dict | None = None
    created_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class FindingActionRequest(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=64)

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key(cls, value: str) -> str:
        if re.fullmatch(r"[A-Za-z0-9._:-]+", value) is None:
            raise ValueError("idempotency_key contains unsafe characters")
        return value

    model_config = {"extra": "forbid"}


class HumanAdviceRequest(FindingActionRequest):
    advice: str = Field(min_length=1, max_length=8000)

    @field_validator("advice")
    @classmethod
    def normalize_advice(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("advice must not be blank")
        if any(ord(char) < 32 and char not in "\n\r\t" for char in normalized):
            raise ValueError("advice contains control characters")
        return normalized


class ConfirmFixRequest(BaseModel):
    confirmation_token: str = Field(min_length=10, max_length=100)
    patch_sha256: str = Field(pattern=r"[0-9a-f]{64}")
    download_receipt: str = Field(min_length=32, max_length=128)

    model_config = {"extra": "forbid"}


class PRFindingActionResponse(BaseModel):
    id: int
    finding_id: int
    action_type: str
    status: str
    idempotency_key: str
    actor_user_id: int | None
    human_advice: str | None
    task_id: int | None
    expected_head_sha: str
    patch_sha256: str | None
    downloaded_by_user_id: int | None
    downloaded_at: datetime | None
    confirmed_by_user_id: int | None
    confirmed_at: datetime | None
    candidate_commit_sha: str | None
    candidate_created_at: datetime | None
    push_attempted_at: datetime | None
    cancelled_by_user_id: int | None
    cancelled_at: datetime | None
    result: dict | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    diff_download_url: str | None = None

    model_config = {"from_attributes": True}


class PRFindingResponse(BaseModel):
    id: int
    reviewer_run_id: int
    role: str
    severity: str
    category: str
    path: str
    line: int | None
    hunk: str | None
    title: str
    evidence: str
    impact: str
    required_fix: str
    test: str
    status: str
    thread_status: str = "pending"
    github_comment_id: int | None = None
    github_comment_url: str | None = None
    github_thread_node_id: str | None = None
    thread_error: str | None = None
    rebuttals: list["PRFindingRebuttalResponse"] = Field(default_factory=list)
    latest_action: PRFindingActionResponse | None = None

    model_config = {"from_attributes": True}


class PRFindingRebuttalCreate(BaseModel):
    evidence: str = Field(min_length=20, max_length=8000)

    @field_validator("evidence")
    @classmethod
    def validate_evidence(cls, value: str) -> str:
        value = value.strip()
        if "\x00" in value:
            raise ValueError("rebuttal evidence contains NUL")
        return value


class PRFindingRebuttalResponse(BaseModel):
    id: int
    finding_id: int
    developer_task_id: int
    task_id: int | None
    attempt: int
    base_sha: str
    head_sha: str
    evidence: str
    status: str
    verdict: str | None
    result_body: str | None
    error_message: str | None
    created_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class PRReviewerRunResponse(BaseModel):
    id: int
    role: str
    task_id: int | None
    provider: str
    model: str | None
    effort: str | None
    status: str
    verdict: str | None
    error_message: str | None
    created_at: datetime
    completed_at: datetime | None
    findings: list[PRFindingResponse] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class PRReviewDetailResponse(PRReviewResponse):
    reviewer_runs: list[PRReviewerRunResponse] = Field(default_factory=list)
    is_current_snapshot: bool = False


class PRMonitorBindRequest(BaseModel):
    task_id: int = Field(gt=0)


class PRRepairWakeResponse(BaseModel):
    id: int
    review_id: int | None
    developer_task_id: int | None
    trigger_base_sha: str
    trigger_head_sha: str
    reason_kind: str
    status: str
    attempt: int
    evidence: dict
    last_error: str | None
    created_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class PRMergeQueueActionResponse(BaseModel):
    id: int
    review_id: int
    trigger_base_sha: str
    trigger_head_sha: str
    status: str
    github_queue_entry_id: str | None
    merge_group_sha: str | None
    merge_group_ref: str | None
    ci_status: str | None
    ci_details: dict | None
    attempt_count: int
    last_error: str | None
    created_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class PRMonitorRunResponse(BaseModel):
    id: int
    repo_id: int
    pr_number: int
    status: str
    current_base_sha: str
    current_head_sha: str
    current_review_id: int | None
    developer_task_id: int | None
    repair_attempts: int
    max_repair_attempts: int
    no_progress_count: int
    state_version: int
    pause_reason: str | None
    binding_verified_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    wakes: list[PRRepairWakeResponse] = Field(default_factory=list)
    merge_actions: list[PRMergeQueueActionResponse] = Field(default_factory=list)

    model_config = {"from_attributes": True}
