"""add first-class versioned Plan v2 resources

Revision ID: 3f2a9c8e7b10
Revises: 2f6c8a1d4e90
Create Date: 2026-08-05
"""

from datetime import UTC, datetime
import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "3f2a9c8e7b10"
down_revision: Union[str, None] = "2f6c8a1d4e90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _json_value(value, fallback):
    """Normalize JSON selected through untyped SQL on every supported dialect."""

    if value is None:
        return fallback
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("legacy Plan contains invalid JSON") from exc
    return value


def _datetime_value(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError("legacy Plan contains invalid datetime") from exc
    return value


def _legacy_attachments(metadata_value):
    metadata = _json_value(metadata_value, {})
    if not isinstance(metadata, dict):
        raise RuntimeError("legacy Plan metadata must be a JSON object")
    records = metadata.get("attachments") or []
    paths = metadata.get("file_paths") or metadata.get("image_paths") or []
    if not isinstance(records, list) or not isinstance(paths, list):
        raise RuntimeError("legacy Plan attachment metadata is malformed")
    if not records and not paths:
        return None
    if records and len(records) != len(paths):
        raise RuntimeError("legacy Plan attachment records/paths do not match")
    result = []
    for index, path in enumerate(paths):
        if not isinstance(path, str) or not path:
            raise RuntimeError("legacy Plan attachment path is invalid")
        record = records[index] if records else {}
        if not isinstance(record, dict):
            raise RuntimeError("legacy Plan attachment record is invalid")
        result.append({
            "url": record.get("url"),
            "name": record.get("name") or path.rsplit("/", 1)[-1],
            "is_image": bool(record.get("is_image")),
            "path": path,
        })
    return result


def _legacy_pipeline_config(bind, value):
    """Return a complete frozen route snapshot for pre-route carrier rows."""

    parsed = _json_value(value, None)
    if isinstance(parsed, dict) and parsed.get("planner") and parsed.get("reviewer"):
        return parsed
    global_value = bind.execute(
        sa.text(
            "SELECT plan_pipeline_config FROM global_settings "
            "WHERE plan_pipeline_config IS NOT NULL ORDER BY id LIMIT 1"
        )
    ).scalar_one_or_none()
    parsed_global = _json_value(global_value, None)
    if (
        isinstance(parsed_global, dict)
        and parsed_global.get("planner")
        and parsed_global.get("reviewer")
    ):
        return parsed_global
    return {
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 2,
        "max_interactions": 3,
    }


def _create_carrier_schema() -> None:
    """Recreate the reverted carrier fields as the final Plan v2 schema.

    ``f7a1c3d9e5b2`` is published main history and intentionally remains
    unchanged.  This forward-only revision runs after that cleanup and owns
    every restored field/table, so a downgrade can return exactly to the
    published main head.
    """

    with op.batch_alter_table("tasks") as batch:
        batch.add_column(
            sa.Column("plan_target_task_id", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_context_session_id", sa.String(200), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_context_log_id", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_context_snapshot", sa.Text(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_repo_revision", sa.JSON(), nullable=True)
        )
        batch.add_column(
            sa.Column("supersedes_plan_task_id", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_approved_at", sa.DateTime(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_approved_by", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_applied_at", sa.DateTime(), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "plan_applied_to_session_id",
                sa.String(200),
                nullable=True,
            )
        )
        batch.add_column(
            sa.Column("plan_applied_log_id", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_execution_task_id", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("plan_pipeline_config", sa.JSON(), nullable=True)
        )
        batch.create_index(
            "ix_tasks_plan_target_task_id",
            ["plan_target_task_id"],
            unique=False,
        )
        batch.create_index(
            "ix_tasks_supersedes_plan_task_id",
            ["supersedes_plan_task_id"],
            unique=False,
        )

    with op.batch_alter_table("global_settings") as batch:
        batch.add_column(
            sa.Column("plan_pipeline_config", sa.JSON(), nullable=True)
        )

    op.create_table(
        "plan_agent_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_task_id", sa.Integer(), nullable=True),
        sa.Column("plan_id", sa.Integer(), nullable=True),
        sa.Column("run_type", sa.String(30), nullable=False),
        sa.Column("source_run_id", sa.Integer(), nullable=True),
        sa.Column("base_version_id", sa.Integer(), nullable=True),
        sa.Column("result_version_id", sa.Integer(), nullable=True),
        sa.Column("draft_content", sa.Text(), nullable=True),
        sa.Column("draft_step_id", sa.Integer(), nullable=True),
        sa.Column("draft_repo_revision", sa.JSON(), nullable=True),
        sa.Column("request_text", sa.Text(), nullable=True),
        sa.Column("attachments", sa.JSON(), nullable=True),
        sa.Column("context_session_id", sa.String(200), nullable=True),
        sa.Column("context_log_id", sa.Integer(), nullable=True),
        sa.Column("context_snapshot", sa.Text(), nullable=True),
        sa.Column("repo_revision", sa.JSON(), nullable=True),
        sa.Column("current_stage", sa.String(30), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("instance_id", sa.Integer(), nullable=True),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("relay_origin", sa.String(30), nullable=True),
        sa.Column("import_payload_digest", sa.String(64), nullable=True),
        sa.Column("import_attachment_receipt", sa.JSON(), nullable=True),
        sa.Column("open_input_request_id", sa.Integer(), nullable=True),
        sa.Column("interaction_count", sa.Integer(), nullable=False),
        sa.Column("max_interactions", sa.Integer(), nullable=False),
        sa.Column("execution_seconds", sa.Float(), nullable=False),
        sa.Column("last_execution_started_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("combo_used", sa.String(20), nullable=True),
        sa.Column("planner_provider", sa.String(20), nullable=True),
        sa.Column("planner_model", sa.String(100), nullable=True),
        sa.Column("planner_effort", sa.String(20), nullable=True),
        sa.Column("reviewer_provider", sa.String(20), nullable=True),
        sa.Column("reviewer_model", sa.String(100), nullable=True),
        sa.Column("reviewer_effort", sa.String(20), nullable=True),
        sa.Column("pipeline_config", sa.JSON(), nullable=True),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("review_verdict", sa.String(20), nullable=True),
        sa.Column("review_feedback", sa.Text(), nullable=True),
        sa.Column("review_exhausted", sa.Boolean(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_plan_agent_runs_plan_task_id",
        "plan_agent_runs",
        ["plan_task_id"],
    )
    op.create_index(
        "ix_plan_agent_runs_plan_id", "plan_agent_runs", ["plan_id"]
    )
    op.create_index(
        "ix_plan_agent_runs_instance_id", "plan_agent_runs", ["instance_id"]
    )
    op.create_index(
        "ix_plan_agent_runs_status", "plan_agent_runs", ["status"]
    )

    op.create_table(
        "plan_agent_steps",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=True),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("worker_step_id", sa.Integer(), nullable=True),
        sa.Column("plan_version_id", sa.Integer(), nullable=True),
        sa.Column("input_request_id", sa.Integer(), nullable=True),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("step_type", sa.String(20), nullable=False),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(20), nullable=False),
        sa.Column("model", sa.String(100), nullable=True),
        sa.Column("effort", sa.String(20), nullable=True),
        sa.Column("route_slot", sa.String(20), nullable=True),
        sa.Column("account_id", sa.String(100), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("output", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("last_delta_at", sa.DateTime(), nullable=True),
        sa.Column("streamed_output_chars", sa.Integer(), nullable=False),
        sa.Column("last_event_type", sa.String(100), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "worker_id", "worker_step_id", name="uq_plan_steps_worker_id"
        ),
    )
    op.create_index(
        "ix_plan_agent_steps_run_id", "plan_agent_steps", ["run_id"]
    )
    op.create_index(
        "ix_plan_agent_steps_plan_id", "plan_agent_steps", ["plan_id"]
    )


def _create_tables() -> None:
    op.create_table(
        "plans",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("initial_request", sa.Text(), nullable=False),
        sa.Column("initial_attachments", sa.JSON(), nullable=True),
        sa.Column("target_task_id", sa.Integer(), nullable=True),
        sa.Column("project_id", sa.Integer(), nullable=True),
        sa.Column("target_repo", sa.String(500), nullable=True),
        sa.Column("target_branch", sa.String(200), nullable=True),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("relay_origin", sa.String(30), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("timeout_hours", sa.Float(), nullable=True),
        sa.Column("created_by", sa.Integer(), nullable=True),
        sa.Column("pipeline_config", sa.JSON(), nullable=False),
        sa.Column("current_version_id", sa.Integer(), nullable=True),
        sa.Column("active_run_id", sa.Integer(), nullable=True),
        sa.Column("forked_from_version_id", sa.Integer(), nullable=True),
        sa.Column("archived_at", sa.DateTime(), nullable=True),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.Column("lock_version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_plans_project_id", "plans", ["project_id"])
    op.create_index("ix_plans_worker_id", "plans", ["worker_id"])
    op.create_index("ix_plans_created_by", "plans", ["created_by"])
    op.create_index("ix_plans_active_run_id", "plans", ["active_run_id"])
    op.create_index(
        "ix_plans_target_task_archived", "plans", ["target_task_id", "archived_at"]
    )
    op.create_index(
        "ix_plans_created_by_archived", "plans", ["created_by", "archived_at"]
    )

    op.create_table(
        "plan_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("worker_version_id", sa.Integer(), nullable=True),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("parent_version_id", sa.Integer(), nullable=True),
        sa.Column("produced_by_run_id", sa.Integer(), nullable=True),
        sa.Column("produced_by_step_id", sa.Integer(), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("context_session_id", sa.String(200), nullable=True),
        sa.Column("context_log_id", sa.Integer(), nullable=True),
        sa.Column("context_snapshot", sa.Text(), nullable=True),
        sa.Column("repo_revision", sa.JSON(), nullable=True),
        sa.Column("reviewer_repo_revision", sa.JSON(), nullable=True),
        sa.Column("review_verdict", sa.String(20), nullable=True),
        sa.Column("review_feedback", sa.Text(), nullable=True),
        sa.Column("reviewed_by_step_id", sa.Integer(), nullable=True),
        sa.Column("review_exhausted", sa.Boolean(), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("human_decision", sa.String(20), nullable=False),
        sa.Column("decided_at", sa.DateTime(), nullable=True),
        sa.Column("decided_by", sa.Integer(), nullable=True),
        sa.Column("superseded_by_version_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "plan_id", "version_number", name="uq_plan_versions_plan_number"
        ),
        sa.UniqueConstraint(
            "produced_by_step_id", name="uq_plan_versions_produced_step"
        ),
        sa.UniqueConstraint(
            "worker_id", "worker_version_id", name="uq_plan_versions_worker_id"
        ),
    )
    op.create_index("ix_plan_versions_plan_id", "plan_versions", ["plan_id"])
    op.create_index(
        "ix_plan_versions_plan_created", "plan_versions", ["plan_id", "created_at"]
    )

    op.create_table(
        "plan_input_requests",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("worker_input_request_id", sa.Integer(), nullable=True),
        sa.Column("source_step_id", sa.Integer(), nullable=False),
        sa.Column("requested_by", sa.String(20), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("questions", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("answers", sa.JSON(), nullable=True),
        sa.Column("response_text", sa.Text(), nullable=True),
        sa.Column("attachments", sa.JSON(), nullable=True),
        sa.Column("answered_by", sa.Integer(), nullable=True),
        sa.Column("idempotency_key", sa.String(200), nullable=False),
        sa.Column("answer_idempotency_key", sa.String(200), nullable=True),
        sa.Column("opened_at", sa.DateTime(), nullable=True),
        sa.Column("answered_at", sa.DateTime(), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_plan_input_idempotency"),
        sa.UniqueConstraint(
            "worker_id", "worker_input_request_id", name="uq_plan_inputs_worker_id"
        ),
    )
    op.create_index("ix_plan_input_requests_plan_id", "plan_input_requests", ["plan_id"])
    op.create_index("ix_plan_input_requests_run_id", "plan_input_requests", ["run_id"])
    op.create_index(
        "ix_plan_inputs_plan_status", "plan_input_requests", ["plan_id", "status"]
    )
    op.create_index(
        "ix_plan_inputs_run_status", "plan_input_requests", ["run_id", "status"]
    )

    op.create_table(
        "plan_applications",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("plan_version_id", sa.Integer(), nullable=False),
        sa.Column("application_type", sa.String(30), nullable=False),
        sa.Column("target_task_id", sa.Integer(), nullable=True),
        sa.Column("target_session_id", sa.String(200), nullable=True),
        sa.Column("user_log_id", sa.Integer(), nullable=True),
        sa.Column("execution_task_id", sa.Integer(), nullable=True),
        sa.Column("applied_by", sa.Integer(), nullable=True),
        sa.Column("application_receipt_key", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("plan_version_id", name="uq_plan_application_version"),
        sa.CheckConstraint(
            "(application_type = 'chat_message' AND user_log_id IS NOT NULL "
            "AND execution_task_id IS NULL) OR "
            "(application_type = 'execution_task' "
            "AND execution_task_id IS NOT NULL AND user_log_id IS NULL)",
            name="ck_plan_application_target",
        ),
    )
    op.create_index("ix_plan_applications_plan_id", "plan_applications", ["plan_id"])
    op.create_index(
        "ix_plan_applications_plan_version_id",
        "plan_applications",
        ["plan_version_id"],
    )
    op.create_index(
        "ix_plan_applications_application_receipt_key",
        "plan_applications",
        ["application_receipt_key"],
    )

    op.create_table(
        "plan_legacy_task_links",
        sa.Column("legacy_task_id", sa.Integer(), nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("plan_version_id", sa.Integer(), nullable=True),
        sa.Column("plan_run_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("legacy_task_id"),
    )
    op.create_index(
        "ix_plan_legacy_task_links_plan_id", "plan_legacy_task_links", ["plan_id"]
    )

    op.create_table(
        "plan_application_receipts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("receipt_key", sa.String(200), nullable=False),
        sa.Column("target_task_id", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("manager_user_log_id", sa.Integer(), nullable=True),
        sa.Column("plan_version_ids", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("response", sa.JSON(), nullable=True),
        sa.Column("delivery_status", sa.String(20), nullable=False),
        sa.Column("outbox_payload", sa.JSON(), nullable=True),
        sa.Column("payload_digest", sa.String(64), nullable=True),
        sa.Column("delivery_error", sa.Text(), nullable=True),
        sa.Column("launch_evidence", sa.JSON(), nullable=True),
        sa.Column("delivery_resolution", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "receipt_key", name="uq_plan_application_receipt_key"
        ),
    )
    op.create_index(
        "ix_plan_application_receipts_target_task_id",
        "plan_application_receipts",
        ["target_task_id"],
    )
    op.create_index(
        "ix_plan_application_receipts_delivery_status",
        "plan_application_receipts",
        ["delivery_status"],
    )

    op.create_table(
        "plan_application_attempts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("plan_version_id", sa.Integer(), nullable=False),
        sa.Column("application_receipt_key", sa.String(200), nullable=False),
        sa.Column("application_type", sa.String(30), nullable=False),
        sa.Column("target_task_id", sa.Integer(), nullable=True),
        sa.Column("target_session_id", sa.String(200), nullable=True),
        sa.Column("user_log_id", sa.Integer(), nullable=True),
        sa.Column("execution_task_id", sa.Integer(), nullable=True),
        sa.Column("applied_by", sa.Integer(), nullable=True),
        sa.Column("application_created_at", sa.DateTime(), nullable=False),
        sa.Column("released_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "application_receipt_key",
            "plan_version_id",
            name="uq_plan_application_attempt_receipt_version",
        ),
    )
    op.create_index(
        "ix_plan_application_attempts_plan_id",
        "plan_application_attempts",
        ["plan_id"],
    )
    op.create_index(
        "ix_plan_application_attempts_plan_version_id",
        "plan_application_attempts",
        ["plan_version_id"],
    )
    op.create_index(
        "ix_plan_application_attempts_application_receipt_key",
        "plan_application_attempts",
        ["application_receipt_key"],
    )


def _expand_existing_tables() -> None:
    with op.batch_alter_table("instances") as batch:
        batch.add_column(sa.Column("current_plan_run_id", sa.Integer(), nullable=True))
        batch.create_check_constraint(
            "ck_instances_task_xor_plan_run_owner",
            "NOT (current_task_id IS NOT NULL AND current_plan_run_id IS NOT NULL)",
        )


def _backfill_legacy_plans() -> None:
    """Project only Plan Tasks that could have been created by ``main``.

    The pre-cutover implementation used one ``Task(mode='plan')`` for both
    planning and, after approval, implementation.  Revision chains and every
    independent-Plan provenance column were introduced on this feature branch,
    so importing those rows would preserve test-only schemas that never existed
    on main.
    """

    bind = op.get_bind()
    metadata = sa.MetaData()
    plans_table = sa.Table("plans", metadata, autoload_with=bind)
    versions_table = sa.Table("plan_versions", metadata, autoload_with=bind)
    runs_table = sa.Table("plan_agent_runs", metadata, autoload_with=bind)
    active_evidence = list(bind.execute(sa.text(
        """SELECT i.id, i.current_task_id, i.pid
        FROM instances i JOIN tasks t ON t.id = i.current_task_id
        WHERE t.mode = 'plan' AND (i.pid IS NOT NULL OR i.status = 'running')"""
    )).mappings())
    if active_evidence:
        raise RuntimeError(
            "cannot migrate legacy Plans while an Instance has active process evidence"
        )
    active_task_evidence = list(bind.execute(sa.text(
        """SELECT id, status, instance_id, worker_id FROM tasks
        WHERE mode = 'plan' AND status IN ('in_progress', 'executing')"""
    )).mappings())
    if active_task_evidence:
        raise RuntimeError(
            "cannot migrate legacy Plans while a Plan Task has active state evidence"
        )
    active_run_evidence = list(bind.execute(sa.text(
        """SELECT r.id, r.plan_task_id, r.status FROM plan_agent_runs r
        JOIN tasks t ON t.id = r.plan_task_id
        WHERE t.mode = 'plan' AND r.status IN ('planning', 'reviewing')"""
    )).mappings())
    if active_run_evidence:
        raise RuntimeError(
            "cannot migrate legacy Plans while a Pipeline Run has active state evidence"
        )
    rows = list(
        bind.execute(
            sa.text(
                """SELECT id, title, description, status, priority, project_id,
                target_repo, target_branch, worker_id, timeout_hours, created_by,
                metadata, plan_content, plan_approved, created_at, completed_at
                FROM tasks
                WHERE mode = 'plan'
                  AND status IN (
                    'pending', 'plan_review', 'completed', 'failed', 'cancelled'
                  )
                  AND plan_target_task_id IS NULL
                  AND plan_context_session_id IS NULL
                  AND plan_context_log_id IS NULL
                  AND plan_context_snapshot IS NULL
                  AND plan_repo_revision IS NULL
                  AND supersedes_plan_task_id IS NULL
                  AND plan_approved_at IS NULL
                  AND plan_approved_by IS NULL
                  AND plan_applied_at IS NULL
                  AND plan_applied_to_session_id IS NULL
                  AND plan_applied_log_id IS NULL
                  AND plan_execution_task_id IS NULL
                  AND plan_pipeline_config IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM plan_agent_runs r WHERE r.plan_task_id = tasks.id
                  )
                ORDER BY created_at, id"""
            )
        ).mappings()
    )
    if not rows:
        return

    now = datetime.now(UTC).replace(tzinfo=None)
    for row in rows:
        task_id = int(row["id"])
        created_at = _datetime_value(row["created_at"], now)
        updated_at = _datetime_value(
            row["completed_at"] or row["created_at"], now
        )
        pipeline_config = _legacy_pipeline_config(bind, None)
        plan_result = bind.execute(
            plans_table.insert().values({
                "title": row["title"] or f"Plan #{task_id}",
                "initial_request": row["description"] or "Legacy Plan",
                "initial_attachments": _legacy_attachments(row["metadata"]),
                "target_task_id": None,
                "project_id": row["project_id"],
                "target_repo": row["target_repo"],
                "target_branch": row["target_branch"],
                "worker_id": row["worker_id"],
                "priority": row["priority"] or 0,
                "timeout_hours": row["timeout_hours"],
                "created_by": row["created_by"],
                "pipeline_config": pipeline_config,
                "current_version_id": None,
                "active_run_id": None,
                "forked_from_version_id": None,
                "archived_at": None,
                "closed_at": None,
                "lock_version": 0,
                "created_at": created_at,
                "updated_at": updated_at,
            })
        )
        plan_id = int(plan_result.inserted_primary_key[0])
        approved = row["plan_approved"] is not None and bool(row["plan_approved"])
        rejected = (
            row["plan_approved"] is not None
            and not bool(row["plan_approved"])
            and row["status"] == "cancelled"
        )
        version_id = None
        if row["plan_content"]:
            decision = "approved" if approved else "rejected" if rejected else "pending"
            review_ready = row["status"] == "plan_review"
            version_result = bind.execute(
                versions_table.insert().values({
                    "plan_id": plan_id,
                    "version_number": 1,
                    "content": row["plan_content"],
                    "review_verdict": "disabled" if review_ready else None,
                    "review_exhausted": False,
                    "reviewed_at": updated_at if review_ready else None,
                    "human_decision": decision,
                    # Main stored the decision but not an authoritative decision
                    # timestamp or actor.
                    "decided_at": None,
                    "decided_by": None,
                    "created_at": updated_at,
                })
            )
            version_id = int(version_result.inserted_primary_key[0])

        # Main approval queued the same carrier Task for implementation.  The
        # Application therefore points back to that exact Task, whether it is
        # still pending, completed, or failed.
        if approved and version_id is not None:
            bind.execute(
                sa.text(
                    """INSERT INTO plan_applications
                    (plan_id, plan_version_id, application_type,
                     execution_task_id, created_at)
                    VALUES (:plan_id, :version_id, 'execution_task',
                            :task_id, :created_at)"""
                ),
                {
                    "plan_id": plan_id,
                    "version_id": version_id,
                    "task_id": task_id,
                    "created_at": updated_at,
                },
            )

        queued = row["status"] == "pending" and not approved
        if queued:
            run_status = "queued"
        elif row["plan_content"]:
            run_status = "completed"
        elif row["status"] == "cancelled":
            run_status = "cancelled"
        else:
            run_status = "failed"
        run_result = bind.execute(
            runs_table.insert().values({
                "plan_task_id": task_id,
                "plan_id": plan_id,
                "run_type": "legacy_migration" if queued else "legacy",
                "base_version_id": version_id if queued else None,
                "result_version_id": None if queued else version_id,
                "request_text": row["description"] or "Legacy Plan",
                "current_stage": "planner" if queued else "complete",
                "generation": 0,
                "worker_id": row["worker_id"],
                "interaction_count": 0,
                "max_interactions": 3,
                "execution_seconds": 0,
                "status": run_status,
                "round": 1,
                "review_exhausted": False,
                "error": (
                    "Legacy Plan ended without persisted content"
                    if run_status == "failed"
                    else None
                ),
                "created_at": created_at,
                "updated_at": updated_at,
                "finished_at": None if queued else updated_at,
                "pipeline_config": pipeline_config,
            })
        )
        run_id = int(run_result.inserted_primary_key[0])

        bind.execute(
            sa.text(
                """INSERT INTO plan_legacy_task_links
                (legacy_task_id, plan_id, plan_version_id, plan_run_id,
                 created_at)
                VALUES (:task_id, :plan_id, :version_id, :run_id,
                        :created_at)"""
            ),
            {
                "task_id": task_id,
                "plan_id": plan_id,
                "version_id": version_id,
                "run_id": run_id,
                "created_at": created_at,
            },
        )
        bind.execute(
            sa.text(
                "UPDATE plans SET current_version_id=:version_id, "
                "active_run_id=:active_run_id WHERE id=:plan_id"
            ),
            {
                "version_id": version_id,
                "active_run_id": run_id if queued else None,
                "plan_id": plan_id,
            },
        )
        if queued:
            bind.execute(
                sa.text(
                    "UPDATE tasks SET status='superseded', "
                    "completed_at=COALESCE(completed_at, :completed_at), "
                    "error_message=:message WHERE id=:task_id AND status='pending'"
                ),
                {
                    "completed_at": now,
                    "message": (
                        f"Migrated to first-class Plan #{plan_id}; "
                        "the canonical Run owns execution"
                    ),
                    "task_id": task_id,
                },
            )


def upgrade() -> None:
    _create_carrier_schema()
    _create_tables()
    _expand_existing_tables()
    _backfill_legacy_plans()


def downgrade() -> None:
    # Only queued legacy carriers are changed by the backfill. Restore their
    # published-main shape before removing the canonical Plan audit tables so
    # a downgrade/re-upgrade remains deterministic.
    op.execute(
        sa.text(
            """UPDATE tasks
            SET status='pending', completed_at=NULL, error_message=NULL
            WHERE status='superseded'
              AND error_message LIKE
                  'Migrated to first-class Plan #%; the canonical Run owns execution'
              AND id IN (
                  SELECT l.legacy_task_id
                  FROM plan_legacy_task_links l
                  JOIN plan_agent_runs r ON r.id = l.plan_run_id
                  WHERE r.run_type = 'legacy_migration'
              )"""
        )
    )

    op.drop_index(
        "ix_plan_application_attempts_application_receipt_key",
        table_name="plan_application_attempts",
    )
    op.drop_index(
        "ix_plan_application_attempts_plan_version_id",
        table_name="plan_application_attempts",
    )
    op.drop_index(
        "ix_plan_application_attempts_plan_id",
        table_name="plan_application_attempts",
    )
    op.drop_table("plan_application_attempts")

    op.drop_index(
        "ix_plan_application_receipts_delivery_status",
        table_name="plan_application_receipts",
    )
    op.drop_index(
        "ix_plan_application_receipts_target_task_id",
        table_name="plan_application_receipts",
    )
    op.drop_table("plan_application_receipts")

    with op.batch_alter_table("instances") as batch:
        batch.drop_constraint(
            "ck_instances_task_xor_plan_run_owner",
            type_="check",
        )
        batch.drop_column("current_plan_run_id")

    op.drop_index(
        "ix_plan_legacy_task_links_plan_id",
        table_name="plan_legacy_task_links",
    )
    op.drop_table("plan_legacy_task_links")
    op.drop_index(
        "ix_plan_applications_application_receipt_key",
        table_name="plan_applications",
    )
    op.drop_index(
        "ix_plan_applications_plan_version_id",
        table_name="plan_applications",
    )
    op.drop_index(
        "ix_plan_applications_plan_id", table_name="plan_applications"
    )
    op.drop_table("plan_applications")
    op.drop_index(
        "ix_plan_inputs_run_status", table_name="plan_input_requests"
    )
    op.drop_index(
        "ix_plan_inputs_plan_status", table_name="plan_input_requests"
    )
    op.drop_index(
        "ix_plan_input_requests_run_id", table_name="plan_input_requests"
    )
    op.drop_index(
        "ix_plan_input_requests_plan_id", table_name="plan_input_requests"
    )
    op.drop_table("plan_input_requests")
    op.drop_index(
        "ix_plan_versions_plan_created", table_name="plan_versions"
    )
    op.drop_index("ix_plan_versions_plan_id", table_name="plan_versions")
    op.drop_table("plan_versions")
    op.drop_index("ix_plans_created_by_archived", table_name="plans")
    op.drop_index("ix_plans_target_task_archived", table_name="plans")
    op.drop_index("ix_plans_active_run_id", table_name="plans")
    op.drop_index("ix_plans_created_by", table_name="plans")
    op.drop_index("ix_plans_worker_id", table_name="plans")
    op.drop_index("ix_plans_project_id", table_name="plans")
    op.drop_table("plans")

    op.drop_index(
        "ix_plan_agent_steps_plan_id", table_name="plan_agent_steps"
    )
    op.drop_index(
        "ix_plan_agent_steps_run_id", table_name="plan_agent_steps"
    )
    op.drop_table("plan_agent_steps")
    op.drop_index(
        "ix_plan_agent_runs_status", table_name="plan_agent_runs"
    )
    op.drop_index(
        "ix_plan_agent_runs_instance_id", table_name="plan_agent_runs"
    )
    op.drop_index(
        "ix_plan_agent_runs_plan_id", table_name="plan_agent_runs"
    )
    op.drop_index(
        "ix_plan_agent_runs_plan_task_id", table_name="plan_agent_runs"
    )
    op.drop_table("plan_agent_runs")

    with op.batch_alter_table("global_settings") as batch:
        batch.drop_column("plan_pipeline_config")

    with op.batch_alter_table("tasks") as batch:
        batch.drop_index("ix_tasks_supersedes_plan_task_id")
        batch.drop_index("ix_tasks_plan_target_task_id")
        batch.drop_column("plan_pipeline_config")
        batch.drop_column("plan_execution_task_id")
        batch.drop_column("plan_applied_log_id")
        batch.drop_column("plan_applied_to_session_id")
        batch.drop_column("plan_applied_at")
        batch.drop_column("plan_approved_by")
        batch.drop_column("plan_approved_at")
        batch.drop_column("supersedes_plan_task_id")
        batch.drop_column("plan_repo_revision")
        batch.drop_column("plan_context_snapshot")
        batch.drop_column("plan_context_log_id")
        batch.drop_column("plan_context_session_id")
        batch.drop_column("plan_target_task_id")
