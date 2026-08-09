from datetime import datetime, timezone
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    field_serializer,
    model_validator,
)

from backend.config import settings
from backend.schemas.capability import AutoCapabilityPolicy
from backend.schemas.plan import PlanPipelineConfig
from backend.schemas.task_ssh_grant import TaskSSHGrantInput


TaskMode = Literal["auto", "plan", "loop", "goal"]


class UserSkillSnapshotPayload(BaseModel):
    """Internal Manager→Worker copy of one selected User Skill."""

    id: int = Field(gt=0)
    name: str = Field(min_length=1, max_length=100)
    description: str = ""
    content: str = ""


def _normalize_task_provider(value: object) -> str:
    """Canonicalize an explicit task provider without accepting empty input."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("provider must be 'claude' or 'codex'")
    provider = value.strip().lower()
    if provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' or 'codex'")
    return provider


def _normalize_attention_tag(value: object) -> object:
    """Trim a user attention tag and canonicalize blank input to NULL."""

    if not isinstance(value, str):
        return value
    normalized = value.strip()
    return normalized or None


class TaskCreate(BaseModel):
    # Internal Manager→Worker forwarding only: Manager allocates the global ID.
    id: int | None = None
    # Internal Manager→Worker identity fence. Worker mirrors reuse the exact
    # logical incarnation so every later remote mutation can reject Task-id
    # ABA. Public callers cannot combine this with an explicit id.
    source_incarnation_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{32}$",
    )
    # None = 本机执行；有值 = 创建后由 Dispatcher 转发到该 Worker
    worker_id: int | None = None
    # TaskMigrator 在目标机重建 task 时带上（跨机 --resume 续聊）
    session_id: str | None = None
    last_cwd: str | None = None
    title: str = ""
    description: str = ""
    project_id: int | None = None
    target_repo: str | None = None
    target_branch: str = "main"
    priority: int = 0
    max_retries: int = 2
    mode: TaskMode = "auto"
    # Explicit opt-in for model-requested Plan/Review. ``None`` is the only
    # disabled state; a non-NULL policy is immutable for this Task incarnation.
    capability_policy: AutoCapabilityPolicy | None = None
    # Reserved for the Delivery Controller.  They are declared explicitly so
    # a public caller cannot smuggle ownership hints through Pydantic's legacy
    # extra-field compatibility.  ``None`` remains harmless for clients that
    # serialize the read model back into a create form.
    delivery_run_id: int | None = Field(default=None, exclude=True)
    delivery_role: str | None = Field(default=None, exclude=True)
    todo_file_path: str | None = None  # required when mode="loop"
    max_iterations: int = 50  # loop only: max iterations before auto-abort
    must_complete: bool = False  # loop only: reject done until all items finished
    goal_condition: str | None = None  # goal only: natural-language completion condition
    goal_max_turns: int = 30  # goal only: max turns before auto-fail
    goal_evaluator_model: str | None = None  # goal only: evaluator model (default haiku)
    # API callers that omit provider follow the deployment-wide default.
    provider: str = Field(
        default_factory=lambda: settings.default_provider,
        validate_default=True,
    )
    model: str | None = None
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = None
    thinking_budget: int | None = None
    system_prompt_mode: str | None = None
    timeout_hours: float | None = None
    sort_order: float | None = None
    enable_workflows: bool = False
    enabled_skills: dict | None = None
    selected_user_skills: list[int] | None = None
    user_skill_snapshots: list[UserSkillSnapshotPayload] | None = None
    tags: list[str] | None = None
    attention_tag: str | None = Field(default=None, max_length=80)
    image_paths: list[str] | None = None  # kept for backwards compat
    file_paths: list[str] | None = None
    attachments: list[dict] | None = None  # [{url, name, is_image}, ...]
    secret_ids: list[int] | None = None
    ssh_grants: list[TaskSSHGrantInput] | None = Field(default=None, max_length=50)
    clone_from_task_id: int | None = None
    # Internal/Plan endpoints use these fields to preserve independent Plan
    # relationships across Manager→Worker copies. Public creation validates the
    # referenced Task before accepting them.
    plan_target_task_id: int | None = None
    plan_context_session_id: str | None = None
    plan_context_log_id: int | None = None
    plan_context_snapshot: str | None = Field(default=None, max_length=60_000)
    plan_repo_revision: dict | None = None
    supersedes_plan_task_id: int | None = None
    plan_pipeline_config: PlanPipelineConfig | None = None
    starred: bool = False

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        return _normalize_task_provider(value)

    @field_validator("attention_tag", mode="before")
    @classmethod
    def normalize_attention_tag(cls, value: object) -> object:
        return _normalize_attention_tag(value)

    @model_validator(mode='after')
    def validate_mode_fields(self):
        if self.delivery_run_id is not None or self.delivery_role is not None:
            raise ValueError(
                "delivery_run_id and delivery_role are reserved for the "
                "Delivery Controller"
            )
        if self.capability_policy is not None:
            if self.mode != "auto":
                raise ValueError("capability_policy requires mode=auto")
            if self.worker_id is not None:
                raise ValueError("capability_policy is local-task only")
            if self.id is not None:
                raise ValueError(
                    "Manager-forwarded Worker Tasks cannot use capability_policy"
                )
        if self.mode not in ('loop',) and not self.description:
            raise ValueError('description is required for non-loop tasks')
        if self.mode == 'loop' and not self.todo_file_path:
            raise ValueError('todo_file_path is required for loop tasks')
        if self.mode == 'goal' and not self.goal_condition:
            raise ValueError('goal_condition is required for goal tasks')
        return self


class TaskMigrationImport(TaskCreate):
    """Manager -> Worker migration payload.

    Unlike the public create endpoint, this path requires the Manager-assigned
    global ID and is persisted as an inert task on the destination Worker.
    """

    id: int
    # Auto capability history and resume state are Manager-local.  Inheriting
    # TaskCreate must never make the migration transport accept this field.
    capability_policy: None = None
    # Accept the reserved wire value so the internal endpoint can reject it
    # with a lifecycle conflict (409) instead of letting schema validation
    # disguise an attempted Delivery ownership migration as malformed input.
    mode: Literal["auto", "plan", "loop", "goal", "delivery_loop"] = "auto"
    # Keep Manager and destination Worker retry generations monotonic. This is
    # intentionally internal-only; public task creation always starts at zero.
    retry_count: int = Field(default=0, ge=0)
    turn_generation: int = Field(default=0, ge=0)
    # Migration may preserve an already-inert source state without ever
    # exposing a dispatchable ``pending`` row on the destination Worker.
    source_status: Literal[
        "plan_review",
        "completed",
        "failed",
        "cancelled",
        "conflict",
    ] = "cancelled"


class TaskTerminationRequest(BaseModel):
    """Manager→Worker fence for the hidden PR-review termination endpoint."""

    expected_status: str
    expected_retry_count: int = Field(ge=0)
    expected_turn_generation: int = Field(ge=0)
    expected_instance_id: int | None = None
    expected_started_at: datetime | None = None
    expected_completed_at: datetime | None = None
    # Required even when NULL: omission would let an old Manager snapshot
    # silently adopt the Worker's arrival-time PTY epoch.
    expected_pty_background_generation: str | None


class WorkerRoutingConfigRequest(BaseModel):
    """Internal Manager→Worker routing synchronization payload."""

    model_config = ConfigDict(extra="forbid")

    op_id: str = Field(min_length=1, max_length=128)
    provider: str = Field(min_length=1)
    model: str | None = None
    codex_service_tier: Literal["default", "priority"]

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        return _normalize_task_provider(value)


class WorkerRoutingConfigPending(WorkerRoutingConfigRequest):
    """Durable staged candidate returned by Worker readback."""


class WorkerRoutingConfigSnapshot(BaseModel):
    """Strict Worker-local live tuple plus its optional staged candidate."""

    model_config = ConfigDict(extra="forbid")

    id: int
    status: str
    worker_id: None = None
    shared_from_id: None = None
    provider: str
    model: str | None = None
    codex_service_tier: Literal["default", "priority"]
    pending: WorkerRoutingConfigPending | None = None


class TaskRoutingExpectation(BaseModel):
    """Client-visible routing tuple used to reject stale UI actions."""

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1)
    model: str | None = None
    codex_service_tier: Literal["default", "priority"]

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        return _normalize_task_provider(value)


class TaskActionRequest(BaseModel):
    """Optional stale-view fence for actions that can start a model turn."""

    model_config = ConfigDict(extra="forbid")

    expected_routing: TaskRoutingExpectation | None = None


class PlanApprovalRequest(TaskActionRequest):
    confirm_stale: bool = False


class TaskUpdate(BaseModel):
    # 执行位置切换：传 worker_id 触发 TaskMigrator（-1 表示切回本机，
    # 因为 None 在 exclude_unset 语义下无法与「未传」区分）
    worker_id: int | None = None
    title: str | None = None
    model: str | None = None
    # A concrete default keeps PATCH-style exclude_unset semantics while
    # rejecting an explicit JSON null for this non-null Task setting.
    codex_service_tier: Literal["default", "priority"] = "default"
    effort_level: str | None = None
    thinking_budget: int | None = None
    system_prompt_mode: str | None = None
    timeout_hours: float | None = None
    sort_order: float | None = None
    description: str | None = None
    priority: int | None = None
    project_id: int | None = None
    target_repo: str | None = None
    target_branch: str | None = None
    max_retries: int | None = None
    max_iterations: int | None = None
    must_complete: bool | None = None
    mode: TaskMode | None = None
    # Reserve the wire name so old Pydantic extra-field behavior cannot turn a
    # policy mutation into a misleading 200 response. Policies are create-only.
    capability_policy: AutoCapabilityPolicy | None = Field(
        default=None,
        exclude=True,
    )
    goal_condition: str | None = None
    goal_max_turns: int | None = None
    goal_evaluator_model: str | None = None
    enable_workflows: bool | None = None
    enabled_skills: dict | None = None
    selected_user_skills: list[int] | None = None
    user_skill_snapshots: list[UserSkillSnapshotPayload] | None = None
    provider: str | None = None
    starred: bool | None = None
    tags: list[str] | None = None
    attention_tag: str | None = Field(default=None, max_length=80)

    @field_validator("provider", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> str:
        # The field's None default represents "not supplied" under
        # exclude_unset. An explicit JSON null does run this validator and must
        # not become a database NULL or inherit a deployment default.
        return _normalize_task_provider(value)

    @field_validator("mode", mode="before")
    @classmethod
    def reject_mode_mutation(cls, value: object) -> object:
        # Mode selects a lifecycle state machine and is therefore part of the
        # Task's creation identity.  Changing it after dequeue can race a
        # detached dispatcher snapshot and turn a Plan into an ordinary coding
        # launch (or vice versa).  Keep the field on the wire so old clients
        # receive an explicit validation error instead of a misleading 200.
        raise ValueError("mode is immutable after Task creation")

    @field_validator("attention_tag", mode="before")
    @classmethod
    def normalize_attention_tag(cls, value: object) -> object:
        return _normalize_attention_tag(value)

    @model_validator(mode="after")
    def reject_capability_policy_mutation(self):
        if "capability_policy" in self.model_fields_set:
            raise ValueError("capability_policy is immutable after Task creation")
        return self


class InternalTaskSkillsUpdate(BaseModel):
    """Narrow payload accepted from the Task-scoped skills MCP server."""

    model_config = ConfigDict(extra="forbid")

    enabled_skills: dict


class TaskResponse(BaseModel):
    id: int
    worker_id: int | None = None
    created_by: int | None = None
    title: str
    description: str | None
    status: str
    priority: int
    project_id: int | None
    target_repo: str | None
    target_branch: str
    result_branch: str | None
    merge_status: str
    instance_id: int | None
    retry_count: int
    turn_generation: int
    max_retries: int
    mode: str
    capability_policy: AutoCapabilityPolicy | None = None
    delivery_run_id: int | None = None
    delivery_role: str | None = None
    # Read-only DeliveryRun projection.  A Delivery-owned Task remains a
    # scheduler shell; the Run is the authority for its user-facing lifecycle.
    delivery_phase: str | None = None
    delivery_activity: str | None = None
    delivery_outcome: str | None = None
    todo_file_path: str | None
    loop_progress: str | None
    max_iterations: int
    must_complete: bool
    goal_condition: str | None
    goal_evaluator_model: str | None
    goal_max_turns: int
    goal_turns_used: int
    goal_last_reason: str | None
    plan_content: str | None
    plan_approved: bool | None
    plan_target_task_id: int | None = None
    supersedes_plan_task_id: int | None = None
    plan_approved_at: datetime | None = None
    plan_approved_by: int | None = None
    plan_applied_at: datetime | None = None
    plan_applied_to_session_id: str | None = None
    plan_execution_task_id: int | None = None
    canonical_plan_id: int | None = None
    plan_pipeline_config: PlanPipelineConfig | None = None
    # Read-only projection of the latest PlanAgentRun. Task.status remains the
    # stable scheduler lifecycle (for example ``executing``).
    plan_stage: str | None = None
    plan_stage_round: int | None = None
    plan_stage_provider: str | None = None
    plan_stage_model: str | None = None
    plan_stage_effort: str | None = None
    plan_stage_route_slot: Literal["primary", "fallback"] | None = None
    session_id: str | None
    provider: str
    model: str | None
    codex_service_tier: Literal["default", "priority"]
    effort_level: str | None
    thinking_budget: int | None
    system_prompt_mode: str | None = None
    timeout_hours: float | None = None
    last_accessed_at: datetime | None = None
    sort_order: float | None = None
    enable_workflows: bool
    enabled_skills: dict | None
    selected_user_skills: list[int] | None = None
    starred: bool
    archived: bool
    has_unread: bool
    error_message: str | None
    tags: list[str] | None
    attention_tag: str | None = None
    metadata_: dict | None = None
    shared_from_id: int | None = None
    active_sub_agents: int = 0
    background_active: bool = False
    context_window_usage: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = {"from_attributes": True}

    @field_serializer("created_at", "started_at", "completed_at")
    @classmethod
    def _serialize_utc(cls, v: datetime | None) -> str | None:
        if v is None:
            return None
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()

    @model_validator(mode="after")
    def _ensure_default_skills(self):
        from backend.services.command_registry import ensure_default_skills
        self.enabled_skills = ensure_default_skills(self.enabled_skills)
        return self


class TaskMigrationImportResponse(TaskResponse):
    """Internal migration acknowledgement with the immutable identity fence."""

    incarnation_id: str = Field(pattern=r"^[0-9a-f]{32}$")


class TaskTerminationSnapshot(TaskResponse):
    """Internal-only Worker snapshot with the opaque PTY generation fence."""

    pty_background_generation: str | None
