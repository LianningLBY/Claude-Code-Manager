"""add first-class versioned Plan resources

Revision ID: e7c4a21d9b30
Revises: d2b8f6a10c43
Create Date: 2026-08-02
"""

from collections import defaultdict
from datetime import datetime
import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e7c4a21d9b30"
down_revision: Union[str, None] = "d2b8f6a10c43"
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
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("plan_version_id", name="uq_plan_application_version"),
    )
    op.create_index("ix_plan_applications_plan_id", "plan_applications", ["plan_id"])
    op.create_index(
        "ix_plan_applications_plan_version_id",
        "plan_applications",
        ["plan_version_id"],
    )

    op.create_table(
        "plan_legacy_task_links",
        sa.Column("legacy_task_id", sa.Integer(), nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("plan_version_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("legacy_task_id"),
    )
    op.create_index(
        "ix_plan_legacy_task_links_plan_id", "plan_legacy_task_links", ["plan_id"]
    )


def _expand_existing_tables() -> None:
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.alter_column("plan_task_id", existing_type=sa.Integer(), nullable=True)
        batch.add_column(sa.Column("plan_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("run_type", sa.String(30), nullable=False, server_default="legacy"))
        batch.add_column(sa.Column("base_version_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("result_version_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("request_text", sa.Text(), nullable=True))
        batch.add_column(sa.Column("attachments", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("context_session_id", sa.String(200), nullable=True))
        batch.add_column(sa.Column("context_log_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("context_snapshot", sa.Text(), nullable=True))
        batch.add_column(sa.Column("repo_revision", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("current_stage", sa.String(30), nullable=False, server_default="planner"))
        batch.add_column(sa.Column("generation", sa.Integer(), nullable=False, server_default="0"))
        batch.add_column(sa.Column("instance_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("worker_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("relay_origin", sa.String(30), nullable=True))
        batch.add_column(sa.Column("open_input_request_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("interaction_count", sa.Integer(), nullable=False, server_default="0"))
        batch.add_column(sa.Column("max_interactions", sa.Integer(), nullable=False, server_default="3"))
        batch.add_column(sa.Column("execution_seconds", sa.Float(), nullable=False, server_default="0"))
        batch.add_column(sa.Column("last_execution_started_at", sa.DateTime(), nullable=True))
        batch.create_index("ix_plan_agent_runs_plan_id", ["plan_id"])
        batch.create_index("ix_plan_agent_runs_instance_id", ["instance_id"])

    with op.batch_alter_table("plan_agent_steps") as batch:
        batch.add_column(sa.Column("plan_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("worker_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("worker_step_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("plan_version_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("input_request_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("generation", sa.Integer(), nullable=False, server_default="0"))
        batch.create_index("ix_plan_agent_steps_plan_id", ["plan_id"])
        batch.create_unique_constraint(
            "uq_plan_steps_worker_id", ["worker_id", "worker_step_id"]
        )

    with op.batch_alter_table("instances") as batch:
        batch.add_column(sa.Column("current_plan_run_id", sa.Integer(), nullable=True))
        batch.create_check_constraint(
            "ck_instances_task_xor_plan_run_owner",
            "NOT (current_task_id IS NOT NULL AND current_plan_run_id IS NOT NULL)",
        )


def _backfill_legacy_plans() -> None:
    """Project each valid legacy revision chain into one stable Plan.

    The old rows remain untouched and authoritative for legacy endpoints. Any
    ambiguous chain aborts the deployment migration instead of guessing.
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
                metadata, instance_id, plan_target_task_id, plan_context_session_id,
                plan_context_log_id, plan_context_snapshot, plan_repo_revision,
                supersedes_plan_task_id, plan_content, plan_approved,
                plan_approved_at, plan_approved_by, plan_applied_at,
                plan_applied_to_session_id, plan_applied_log_id,
                plan_execution_task_id, plan_pipeline_config, created_at,
                completed_at
                FROM tasks WHERE mode = 'plan' ORDER BY created_at, id"""
            )
        ).mappings()
    )
    if not rows:
        return

    by_id = {int(row["id"]): row for row in rows}
    successors: dict[int, list[int]] = defaultdict(list)
    for row in rows:
        predecessor = row["supersedes_plan_task_id"]
        if predecessor is None:
            continue
        predecessor = int(predecessor)
        if predecessor not in by_id:
            raise RuntimeError(
                f"legacy Plan Task {row['id']} references missing predecessor {predecessor}"
            )
        successors[predecessor].append(int(row["id"]))
    forks = {key: value for key, value in successors.items() if len(value) > 1}
    if forks:
        raise RuntimeError(f"legacy Plan revision chain branches: {sorted(forks)}")

    roots = [
        int(row["id"])
        for row in rows
        if row["supersedes_plan_task_id"] is None
    ]
    visited: set[int] = set()
    now = datetime.utcnow()
    for root_id in roots:
        chain: list[int] = []
        cursor = root_id
        while cursor is not None:
            if cursor in visited:
                raise RuntimeError(f"legacy Plan revision cycle includes Task {cursor}")
            visited.add(cursor)
            chain.append(cursor)
            next_ids = successors.get(cursor, [])
            cursor = next_ids[0] if next_ids else None

        root = by_id[root_id]
        target_ids = {by_id[item]["plan_target_task_id"] for item in chain}
        project_ids = {by_id[item]["project_id"] for item in chain}
        if len(target_ids) != 1 or len(project_ids) != 1:
            raise RuntimeError(f"legacy Plan chain {chain} changes target or project")
        plan_result = bind.execute(
            plans_table.insert().values({
                "title": root["title"] or f"Plan #{root_id}",
                "initial_request": root["description"] or "Legacy Plan",
                "initial_attachments": _legacy_attachments(root["metadata"]),
                "target_task_id": root["plan_target_task_id"],
                "project_id": root["project_id"],
                "target_repo": root["target_repo"],
                "target_branch": root["target_branch"],
                "worker_id": root["worker_id"],
                "priority": root["priority"] or 0,
                "timeout_hours": root["timeout_hours"],
                "created_by": root["created_by"],
                "pipeline_config": _legacy_pipeline_config(
                    bind, root["plan_pipeline_config"]
                ),
                "current_version_id": None,
                "active_run_id": None,
                "forked_from_version_id": None,
                "archived_at": None,
                "closed_at": None,
                "lock_version": 0,
                "created_at": _datetime_value(root["created_at"], now),
                "updated_at": _datetime_value(
                    root["completed_at"] or root["created_at"], now
                ),
            })
        )
        plan_id = int(plan_result.inserted_primary_key[0])
        previous_version_id = None
        current_version_id = None
        version_by_task: dict[int, int] = {}
        version_number = 0
        for task_id in chain:
            row = by_id[task_id]
            if row["plan_content"]:
                version_number += 1
                decision = "pending"
                if row["plan_approved"] is True:
                    decision = "approved"
                elif row["plan_approved"] is False and row["status"] == "cancelled":
                    decision = "rejected"
                version_result = bind.execute(
                    versions_table.insert().values({
                        "plan_id": plan_id,
                        "version_number": version_number,
                        "parent_version_id": previous_version_id,
                        "produced_by_run_id": None,
                        "produced_by_step_id": None,
                        "content": row["plan_content"],
                        "context_session_id": row["plan_context_session_id"],
                        "context_log_id": row["plan_context_log_id"],
                        "context_snapshot": row["plan_context_snapshot"],
                        "repo_revision": _json_value(row["plan_repo_revision"], None),
                        "review_verdict": None,
                        "review_feedback": None,
                        "reviewed_by_step_id": None,
                        "review_exhausted": False,
                        "reviewed_at": None,
                        "human_decision": decision,
                        "decided_at": _datetime_value(row["plan_approved_at"], None),
                        "decided_by": row["plan_approved_by"],
                        "superseded_by_version_id": None,
                        "created_at": _datetime_value(
                            row["completed_at"] or row["created_at"], now
                        ),
                    })
                )
                version_id = int(version_result.inserted_primary_key[0])
                if previous_version_id is not None:
                    bind.execute(
                        sa.text(
                            "UPDATE plan_versions SET superseded_by_version_id=:next "
                            "WHERE id=:previous"
                        ),
                        {"next": version_id, "previous": previous_version_id},
                    )
                previous_version_id = current_version_id = version_id
                version_by_task[task_id] = version_id

                if row["plan_applied_at"] is not None or row["plan_execution_task_id"] is not None:
                    is_execution = row["plan_execution_task_id"] is not None
                    if not is_execution and row["plan_applied_log_id"] is None:
                        raise RuntimeError(
                            f"legacy Plan Task {task_id} has an incomplete chat application"
                        )
                    if is_execution:
                        execution_exists = bind.execute(
                            sa.text("SELECT id FROM tasks WHERE id=:id"),
                            {"id": row["plan_execution_task_id"]},
                        ).first()
                        if execution_exists is None:
                            raise RuntimeError(
                                f"legacy Plan Task {task_id} references a missing execution Task"
                            )
                    else:
                        log_exists = bind.execute(
                            sa.text("SELECT id FROM log_entries WHERE id=:id"),
                            {"id": row["plan_applied_log_id"]},
                        ).first()
                        if log_exists is None:
                            raise RuntimeError(
                                f"legacy Plan Task {task_id} references a missing application log"
                            )
                    bind.execute(
                        sa.text(
                            """INSERT INTO plan_applications
                            (plan_id, plan_version_id, application_type,
                             target_task_id, target_session_id, user_log_id,
                             execution_task_id, applied_by, created_at)
                            VALUES (:plan_id, :version_id, :kind, :target_task_id,
                                    :session_id, :log_id, :execution_task_id,
                                    :applied_by, :created_at)"""
                        ),
                        {
                            "plan_id": plan_id,
                            "version_id": version_id,
                            "kind": (
                                "execution_task" if is_execution else "chat_message"
                            ),
                            "target_task_id": row["plan_target_task_id"],
                            "session_id": row["plan_applied_to_session_id"],
                            "log_id": None if is_execution else row["plan_applied_log_id"],
                            "execution_task_id": row["plan_execution_task_id"],
                            "applied_by": row["plan_approved_by"],
                            "created_at": _datetime_value(
                                row["plan_applied_at"] or row["completed_at"], now
                            ),
                        },
                    )

            legacy_runs = list(bind.execute(
                sa.text(
                    "SELECT id, status FROM plan_agent_runs "
                    "WHERE plan_task_id=:task_id ORDER BY id"
                ),
                {"task_id": task_id},
            ).mappings())
            queued = row["status"] == "pending"
            if queued:
                # The first-class Run takes over the durable queue item. Keep
                # historical attempts as audit, but never let the old carrier
                # Task and the canonical Run both dispatch after restart.
                run_result = bind.execute(runs_table.insert().values({
                    "plan_task_id": task_id,
                    "plan_id": plan_id,
                    "run_type": "legacy_migration",
                    "base_version_id": current_version_id,
                    "result_version_id": None,
                    "request_text": row["description"] or "Legacy Plan",
                    "context_session_id": row["plan_context_session_id"],
                    "context_log_id": row["plan_context_log_id"],
                    "context_snapshot": row["plan_context_snapshot"],
                    "repo_revision": _json_value(row["plan_repo_revision"], None),
                    "current_stage": "planner",
                    "generation": 0,
                    "worker_id": row["worker_id"],
                    "interaction_count": 0,
                    "max_interactions": 3,
                    "execution_seconds": 0,
                    "status": "queued",
                    "round": 1,
                    "review_exhausted": False,
                    "created_at": _datetime_value(row["created_at"], now),
                    "updated_at": _datetime_value(row["completed_at"] or row["created_at"], now),
                    "pipeline_config": _legacy_pipeline_config(
                        bind, row["plan_pipeline_config"]
                    ),
                }))
                legacy_run_id = int(run_result.inserted_primary_key[0])
            elif len(legacy_runs) > 1:
                # Multiple historical attempts remain valid audit records, but
                # the link points to the latest exact Run.
                legacy_run_id = int(legacy_runs[-1]["id"])
            elif legacy_runs:
                legacy_run_id = int(legacy_runs[0]["id"])
            else:
                terminal = row["status"] in {"failed", "cancelled", "superseded"}
                run_status = "failed" if terminal else "completed"
                error = (
                    "Legacy Plan had no persisted Pipeline Run"
                    if run_status == "failed"
                    else None
                )
                run_result = bind.execute(runs_table.insert().values({
                    "plan_task_id": task_id,
                    "plan_id": plan_id,
                    "run_type": "legacy",
                    "result_version_id": version_by_task.get(task_id),
                    "request_text": row["description"] or "Legacy Plan",
                    "context_session_id": row["plan_context_session_id"],
                    "context_log_id": row["plan_context_log_id"],
                    "context_snapshot": row["plan_context_snapshot"],
                    "repo_revision": _json_value(row["plan_repo_revision"], None),
                    "current_stage": "complete",
                    "generation": 0,
                    "worker_id": row["worker_id"],
                    "interaction_count": 0,
                    "max_interactions": 3,
                    "execution_seconds": 0,
                    "status": run_status,
                    "round": 1,
                    "review_exhausted": False,
                    "error": error,
                    "created_at": _datetime_value(row["created_at"], now),
                    "updated_at": _datetime_value(row["completed_at"] or row["created_at"], now),
                    "finished_at": _datetime_value(row["completed_at"], now),
                    "pipeline_config": _legacy_pipeline_config(
                        bind, row["plan_pipeline_config"]
                    ),
                }))
                legacy_run_id = int(run_result.inserted_primary_key[0])

            bind.execute(
                sa.text(
                    """INSERT INTO plan_legacy_task_links
                    (legacy_task_id, plan_id, plan_version_id, created_at)
                    VALUES (:task_id, :plan_id, :version_id, :created_at)"""
                ),
                {
                    "task_id": task_id,
                    "plan_id": plan_id,
                    "version_id": version_by_task.get(task_id),
                    "created_at": _datetime_value(row["created_at"], now),
                },
            )
            bind.execute(
                sa.text(
                    "UPDATE plan_agent_runs SET plan_id=:plan_id, "
                    "result_version_id=:version_id, "
                    "status=CASE WHEN status IN ('planning','reviewing') THEN 'failed' "
                    "WHEN status='completed' THEN 'completed' ELSE status END, "
                    "current_stage=CASE WHEN status IN ('planning','reviewing') "
                    "THEN 'complete' ELSE current_stage END, "
                    "error=CASE WHEN status IN ('planning','reviewing') AND error IS NULL "
                    "THEN 'Legacy active Run interrupted by versioned Plan migration' "
                    "ELSE error END, "
                    "finished_at=CASE WHEN status IN ('planning','reviewing') "
                    "THEN COALESCE(finished_at, updated_at) ELSE finished_at END "
                    "WHERE plan_task_id=:task_id "
                    "AND (:canonical_run_id IS NULL OR id <> :canonical_run_id)"
                ),
                {
                    "plan_id": plan_id,
                    "version_id": version_by_task.get(task_id),
                    "task_id": task_id,
                    "canonical_run_id": legacy_run_id if queued else None,
                },
            )
            if queued:
                bind.execute(
                    sa.text(
                        "UPDATE plans SET active_run_id=:run_id WHERE id=:plan_id"
                    ),
                    {"run_id": legacy_run_id, "plan_id": plan_id},
                )
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

        bind.execute(
            sa.text("UPDATE plans SET current_version_id=:version_id WHERE id=:plan_id"),
            {"version_id": current_version_id, "plan_id": plan_id},
        )

    if visited != set(by_id):
        missing = sorted(set(by_id) - visited)
        raise RuntimeError(f"legacy Plan revision cycle has no root: {missing}")

    run_rows = bind.execute(
        sa.text("SELECT id, plan_id FROM plan_agent_runs WHERE plan_id IS NOT NULL")
    ).mappings()
    for row in run_rows:
        bind.execute(
            sa.text("UPDATE plan_agent_steps SET plan_id=:plan_id WHERE run_id=:run_id"),
            {"plan_id": row["plan_id"], "run_id": row["id"]},
        )


def upgrade() -> None:
    _create_tables()
    _expand_existing_tables()
    _backfill_legacy_plans()


def downgrade() -> None:
    with op.batch_alter_table("instances") as batch:
        batch.drop_constraint(
            "ck_instances_task_xor_plan_run_owner",
            type_="check",
        )
        batch.drop_column("current_plan_run_id")
    with op.batch_alter_table("plan_agent_steps") as batch:
        batch.drop_constraint("uq_plan_steps_worker_id", type_="unique")
        batch.drop_index("ix_plan_agent_steps_plan_id")
        batch.drop_column("generation")
        batch.drop_column("input_request_id")
        batch.drop_column("plan_version_id")
        batch.drop_column("plan_id")
        batch.drop_column("worker_step_id")
        batch.drop_column("worker_id")
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.drop_index("ix_plan_agent_runs_instance_id")
        batch.drop_index("ix_plan_agent_runs_plan_id")
        for column in (
            "last_execution_started_at", "execution_seconds", "max_interactions", "interaction_count", "open_input_request_id",
            "worker_id", "instance_id", "generation", "current_stage",
            "repo_revision", "context_snapshot", "context_log_id",
            "context_session_id", "attachments", "request_text", "relay_origin",
            "result_version_id", "base_version_id", "run_type", "plan_id",
        ):
            batch.drop_column(column)
        batch.alter_column("plan_task_id", existing_type=sa.Integer(), nullable=False)

    op.drop_index("ix_plan_legacy_task_links_plan_id", table_name="plan_legacy_task_links")
    op.drop_table("plan_legacy_task_links")
    op.drop_index("ix_plan_applications_plan_version_id", table_name="plan_applications")
    op.drop_index("ix_plan_applications_plan_id", table_name="plan_applications")
    op.drop_table("plan_applications")
    op.drop_index("ix_plan_inputs_run_status", table_name="plan_input_requests")
    op.drop_index("ix_plan_inputs_plan_status", table_name="plan_input_requests")
    op.drop_index("ix_plan_input_requests_run_id", table_name="plan_input_requests")
    op.drop_index("ix_plan_input_requests_plan_id", table_name="plan_input_requests")
    op.drop_table("plan_input_requests")
    op.drop_index("ix_plan_versions_plan_created", table_name="plan_versions")
    op.drop_index("ix_plan_versions_plan_id", table_name="plan_versions")
    op.drop_table("plan_versions")
    op.drop_index("ix_plans_created_by_archived", table_name="plans")
    op.drop_index("ix_plans_target_task_archived", table_name="plans")
    op.drop_index("ix_plans_active_run_id", table_name="plans")
    op.drop_index("ix_plans_created_by", table_name="plans")
    op.drop_index("ix_plans_worker_id", table_name="plans")
    op.drop_index("ix_plans_project_id", table_name="plans")
    op.drop_table("plans")
