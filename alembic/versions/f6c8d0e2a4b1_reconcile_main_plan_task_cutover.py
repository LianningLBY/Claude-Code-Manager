"""reconcile canonical Plans to the main Plan Task schema

Revision ID: f6c8d0e2a4b1
Revises: f5b7c9d1e3a2
Create Date: 2026-08-03
"""

from datetime import UTC, datetime
import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f6c8d0e2a4b1"
down_revision: Union[str, None] = "f5b7c9d1e3a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_MIGRATED_QUEUE_PATTERN = (
    "Migrated to first-class Plan #%; the canonical Run owns execution"
)


def _json_value(value, fallback):
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
        result.append(
            {
                "url": record.get("url"),
                "name": record.get("name") or path.rsplit("/", 1)[-1],
                "is_image": bool(record.get("is_image")),
                "path": path,
            }
        )
    return result


def _pipeline_config(bind, existing=None):
    parsed = _json_value(existing, None)
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


def _main_task_rows(bind):
    return list(
        bind.execute(
            sa.text(
                """SELECT id, title, description, status, priority, project_id,
                target_repo, target_branch, worker_id, timeout_hours, created_by,
                metadata, plan_content, plan_approved, error_message, created_at,
                completed_at
                FROM tasks
                WHERE mode = 'plan'
                  AND (
                    status IN (
                      'pending', 'plan_review', 'completed', 'failed', 'cancelled'
                    )
                    OR (
                      status = 'superseded'
                      AND error_message LIKE :migrated_queue_pattern
                    )
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
                ORDER BY created_at, id"""
            ),
            {"migrated_queue_pattern": _MIGRATED_QUEUE_PATTERN},
        ).mappings()
    )


def _assert_quiescent(bind) -> None:
    owner = bind.execute(
        sa.text(
            """SELECT id FROM instances
            WHERE current_plan_run_id IS NOT NULL
               OR (
                 current_task_id IN (SELECT id FROM tasks WHERE mode = 'plan')
                 AND (pid IS NOT NULL OR status = 'running')
               )
            ORDER BY id LIMIT 1"""
        )
    ).first()
    if owner is not None:
        raise RuntimeError(
            "cannot reconcile Plans while an Instance has active Plan evidence"
        )
    active_task = bind.execute(
        sa.text(
            """SELECT id FROM tasks
            WHERE mode = 'plan' AND status IN ('in_progress', 'executing')
            ORDER BY id LIMIT 1"""
        )
    ).first()
    if active_task is not None:
        raise RuntimeError(
            "cannot reconcile Plans while a Plan Task has active state evidence"
        )
    active_run = bind.execute(
        sa.text(
            """SELECT id FROM plan_agent_runs
            WHERE status IN ('planning', 'reviewing', 'running', 'waiting_user')
            ORDER BY id LIMIT 1"""
        )
    ).first()
    if active_run is not None:
        raise RuntimeError(
            "cannot reconcile Plans while a canonical Run has active state evidence"
        )


def _insert_plan(bind, table, row, now):
    created_at = _datetime_value(row["created_at"], now)
    updated_at = _datetime_value(row["completed_at"] or row["created_at"], now)
    result = bind.execute(
        table.insert().values(
            {
                "title": row["title"] or f"Plan #{row['id']}",
                "initial_request": row["description"] or "Legacy Plan",
                "initial_attachments": _legacy_attachments(row["metadata"]),
                "target_task_id": None,
                "project_id": row["project_id"],
                "target_repo": row["target_repo"],
                "target_branch": row["target_branch"],
                "worker_id": row["worker_id"],
                "relay_origin": None,
                "priority": row["priority"] or 0,
                "timeout_hours": row["timeout_hours"],
                "created_by": row["created_by"],
                "pipeline_config": _pipeline_config(bind),
                "current_version_id": None,
                "active_run_id": None,
                "forked_from_version_id": None,
                "archived_at": None,
                "closed_at": None,
                "lock_version": 0,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
    )
    return int(result.inserted_primary_key[0])


def upgrade() -> None:
    bind = op.get_bind()
    _assert_quiescent(bind)
    rows = _main_task_rows(bind)
    now = datetime.now(UTC).replace(tzinfo=None)
    preflight = {
        int(row["id"]): {
            "created_at": _datetime_value(row["created_at"], now),
            "updated_at": _datetime_value(
                row["completed_at"] or row["created_at"], now
            ),
            "attachments": _legacy_attachments(row["metadata"]),
        }
        for row in rows
    }

    metadata = sa.MetaData()
    plans = sa.Table("plans", metadata, autoload_with=bind)
    versions = sa.Table("plan_versions", metadata, autoload_with=bind)
    runs = sa.Table("plan_agent_runs", metadata, autoload_with=bind)
    links = sa.Table("plan_legacy_task_links", metadata, autoload_with=bind)
    applications = sa.Table("plan_applications", metadata, autoload_with=bind)

    plan_ids = set(bind.execute(sa.select(plans.c.id)).scalars())
    version_plan = dict(
        bind.execute(sa.select(versions.c.id, versions.c.plan_id)).all()
    )
    link_by_task = {
        int(item["legacy_task_id"]): item
        for item in bind.execute(sa.select(links)).mappings()
    }

    preserved: dict[int, tuple[int, int | None]] = {}
    used_plan_ids: dict[int, int] = {}
    for row in rows:
        task_id = int(row["id"])
        link = link_by_task.get(task_id)
        plan_id = int(link["plan_id"]) if link is not None else None
        if plan_id not in plan_ids:
            plan_id = _insert_plan(bind, plans, row, now)
            plan_ids.add(plan_id)
        prior_task_id = used_plan_ids.get(plan_id)
        if prior_task_id is not None and prior_task_id != task_id:
            raise RuntimeError(
                f"canonical Plan #{plan_id} maps multiple main Plan Tasks"
            )
        used_plan_ids[plan_id] = task_id
        version_id = (
            int(link["plan_version_id"])
            if link is not None
            and link["plan_version_id"] is not None
            and version_plan.get(int(link["plan_version_id"])) == plan_id
            else None
        )
        preserved[task_id] = (plan_id, version_id)

    pipeline_by_task = {}
    for row in rows:
        task_id = int(row["id"])
        plan_id = preserved[task_id][0]
        existing_pipeline = bind.execute(
            sa.select(plans.c.pipeline_config).where(plans.c.id == plan_id)
        ).scalar_one()
        pipeline_by_task[task_id] = _pipeline_config(bind, existing_pipeline)

    # Everything in these tables was created by an earlier implementation on
    # this feature branch.  Main had no Run/Input/Application schema, so rebuild
    # only the minimal audit/application records implied by its carrier Tasks.
    bind.execute(sa.text("DELETE FROM plan_application_receipts"))
    bind.execute(sa.text("DELETE FROM plan_input_requests"))
    bind.execute(sa.text("DELETE FROM plan_agent_steps"))
    bind.execute(sa.text("DELETE FROM plan_agent_runs"))
    bind.execute(sa.text("DELETE FROM plan_applications"))
    bind.execute(
        plans.update().values(
            current_version_id=None,
            active_run_id=None,
            forked_from_version_id=None,
        )
    )
    bind.execute(sa.text("DELETE FROM plan_legacy_task_links"))

    keep_plan_ids = {item[0] for item in preserved.values()}
    keep_version_ids = {item[1] for item in preserved.values() if item[1] is not None}
    if keep_version_ids:
        bind.execute(versions.delete().where(versions.c.id.not_in(keep_version_ids)))
    else:
        bind.execute(versions.delete())
    if keep_plan_ids:
        bind.execute(plans.delete().where(plans.c.id.not_in(keep_plan_ids)))
    else:
        bind.execute(plans.delete())

    for row in rows:
        task_id = int(row["id"])
        plan_id, version_id = preserved[task_id]
        created_at = preflight[task_id]["created_at"]
        updated_at = preflight[task_id]["updated_at"]
        attachments = preflight[task_id]["attachments"]
        pipeline_config = pipeline_by_task[task_id]
        bind.execute(
            plans.update()
            .where(plans.c.id == plan_id)
            .values(
                {
                    "title": row["title"] or f"Plan #{task_id}",
                    "initial_request": row["description"] or "Legacy Plan",
                    "initial_attachments": attachments,
                    "target_task_id": None,
                    "project_id": row["project_id"],
                    "target_repo": row["target_repo"],
                    "target_branch": row["target_branch"],
                    "worker_id": row["worker_id"],
                    "relay_origin": None,
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
                }
            )
        )

        approved = row["plan_approved"] is not None and bool(row["plan_approved"])
        rejected = (
            row["plan_approved"] is not None
            and not bool(row["plan_approved"])
            and row["status"] == "cancelled"
        )
        if row["plan_content"]:
            decision = "approved" if approved else "rejected" if rejected else "pending"
            review_ready = row["status"] == "plan_review"
            version_values = {
                "plan_id": plan_id,
                "worker_id": None,
                "worker_version_id": None,
                "version_number": 1,
                "parent_version_id": None,
                "produced_by_run_id": None,
                "produced_by_step_id": None,
                "content": row["plan_content"],
                "context_session_id": None,
                "context_log_id": None,
                "context_snapshot": None,
                "repo_revision": None,
                "reviewer_repo_revision": None,
                "review_verdict": "disabled" if review_ready else None,
                "review_feedback": None,
                "reviewed_by_step_id": None,
                "review_exhausted": False,
                "reviewed_at": updated_at if review_ready else None,
                "human_decision": decision,
                "decided_at": None,
                "decided_by": None,
                "superseded_by_version_id": None,
                "created_at": updated_at,
            }
            if version_id is None:
                result = bind.execute(versions.insert().values(version_values))
                version_id = int(result.inserted_primary_key[0])
            else:
                bind.execute(
                    versions.update()
                    .where(versions.c.id == version_id)
                    .values(version_values)
                )
        elif version_id is not None:
            bind.execute(versions.delete().where(versions.c.id == version_id))
            version_id = None

        if approved and version_id is not None:
            bind.execute(
                applications.insert().values(
                    {
                        "plan_id": plan_id,
                        "plan_version_id": version_id,
                        "application_type": "execution_task",
                        "target_task_id": None,
                        "target_session_id": None,
                        "user_log_id": None,
                        "execution_task_id": task_id,
                        "applied_by": None,
                        "application_receipt_key": None,
                        "created_at": updated_at,
                    }
                )
            )

        queued = (
            row["status"] == "pending"
            or (
                row["status"] == "superseded"
                and row["error_message"]
                and row["error_message"].startswith("Migrated to first-class Plan #")
            )
        ) and not approved
        if queued:
            run_status = "queued"
        elif row["plan_content"]:
            run_status = "completed"
        elif row["status"] == "cancelled":
            run_status = "cancelled"
        else:
            run_status = "failed"
        result = bind.execute(
            runs.insert().values(
                {
                    "plan_task_id": task_id,
                    "plan_id": plan_id,
                    "run_type": "legacy_migration" if queued else "legacy",
                    "source_run_id": None,
                    "base_version_id": version_id if queued else None,
                    "result_version_id": None if queued else version_id,
                    "request_text": row["description"] or "Legacy Plan",
                    "attachments": attachments,
                    "context_session_id": None,
                    "context_log_id": None,
                    "context_snapshot": None,
                    "repo_revision": None,
                    "current_stage": "planner" if queued else "complete",
                    "generation": 0,
                    "instance_id": None,
                    "worker_id": row["worker_id"],
                    "relay_origin": None,
                    "open_input_request_id": None,
                    "interaction_count": 0,
                    "max_interactions": 3,
                    "execution_seconds": 0,
                    "last_execution_started_at": None,
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
                }
            )
        )
        run_id = int(result.inserted_primary_key[0])
        bind.execute(
            links.insert().values(
                {
                    "legacy_task_id": task_id,
                    "plan_id": plan_id,
                    "plan_version_id": version_id,
                    "plan_run_id": run_id,
                    "created_at": created_at,
                }
            )
        )
        bind.execute(
            plans.update()
            .where(plans.c.id == plan_id)
            .values(
                current_version_id=version_id,
                active_run_id=run_id if queued else None,
            )
        )
        if queued:
            bind.execute(
                sa.text(
                    """UPDATE tasks
                    SET status='superseded',
                        completed_at=COALESCE(completed_at, :completed_at),
                        error_message=:message
                    WHERE id=:task_id"""
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


def downgrade() -> None:
    # The removed rows were created only by superseded feature-branch schemas.
    # Reconstructing them would invent data, so this one-time reconciliation is
    # intentionally irreversible.
    pass
