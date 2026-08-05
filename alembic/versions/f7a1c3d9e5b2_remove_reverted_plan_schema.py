"""remove schema from the reverted independent Plan workflow

Revision ID: f7a1c3d9e5b2
Revises: b6e1f4a2c9d7
Create Date: 2026-07-31
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f7a1c3d9e5b2"
down_revision: Union[str, None] = "b6e1f4a2c9d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_FIRST_CLASS_PLAN_REVISIONS = {
    "c1a7e4d92f30",
    "d2b8f6a10c43",
    "e7c4a21d9b30",
    "f1a8c4d72e90",
    "f5b7c9d1e3a2",
    "f6c8d0e2a4b1",
    "a4d9e2c7b1f0",
    "b5f7c9d1e3a4",
    "c6e8a1f4d2b7",
    "d4a7c9e2f1b6",
}


def _first_class_plan_history_started() -> bool:
    bind = op.get_bind()
    if "plans" in sa.inspect(bind).get_table_names():
        return True
    applied = {
        row[0]
        for row in bind.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).fetchall()
    }
    return bool(applied & _FIRST_CLASS_PLAN_REVISIONS)


def upgrade() -> None:
    # The feature history may already have installed the first-class Plan
    # aggregate before this published main-branch cleanup is encountered.
    # In that case the legacy carrier tables are still part of the supported
    # compatibility/migration boundary and must not be deleted underneath the
    # canonical Plan data.
    if _first_class_plan_history_started():
        return

    op.drop_index(
        "ix_plan_agent_steps_run_id",
        table_name="plan_agent_steps",
    )
    op.drop_table("plan_agent_steps")
    op.drop_index(
        "ix_plan_agent_runs_status",
        table_name="plan_agent_runs",
    )
    op.drop_index(
        "ix_plan_agent_runs_plan_task_id",
        table_name="plan_agent_runs",
    )
    op.drop_table("plan_agent_runs")

    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_index("ix_tasks_supersedes_plan_task_id")
        batch_op.drop_index("ix_tasks_plan_target_task_id")
        batch_op.drop_column("plan_execution_task_id")
        batch_op.drop_column("plan_applied_log_id")
        batch_op.drop_column("plan_applied_to_session_id")
        batch_op.drop_column("plan_applied_at")
        batch_op.drop_column("plan_approved_by")
        batch_op.drop_column("plan_approved_at")
        batch_op.drop_column("supersedes_plan_task_id")
        batch_op.drop_column("plan_repo_revision")
        batch_op.drop_column("plan_context_snapshot")
        batch_op.drop_column("plan_context_log_id")
        batch_op.drop_column("plan_context_session_id")
        batch_op.drop_column("plan_target_task_id")


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if (
        "plans" in inspector.get_table_names()
        or "plan_agent_runs" in inspector.get_table_names()
        or "plan_target_task_id"
        in {column["name"] for column in inspector.get_columns("tasks")}
    ):
        return

    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("plan_target_task_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "plan_context_session_id",
                sa.String(length=200),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("plan_context_log_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("plan_context_snapshot", sa.Text(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("plan_repo_revision", sa.JSON(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("supersedes_plan_task_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("plan_approved_at", sa.DateTime(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("plan_approved_by", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("plan_applied_at", sa.DateTime(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "plan_applied_to_session_id",
                sa.String(length=200),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("plan_applied_log_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("plan_execution_task_id", sa.Integer(), nullable=True)
        )
        batch_op.create_index(
            "ix_tasks_plan_target_task_id",
            ["plan_target_task_id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_tasks_supersedes_plan_task_id",
            ["supersedes_plan_task_id"],
            unique=False,
        )

    op.create_table(
        "plan_agent_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_task_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("combo_used", sa.String(length=20), nullable=True),
        sa.Column("planner_provider", sa.String(length=20), nullable=True),
        sa.Column("planner_model", sa.String(length=100), nullable=True),
        sa.Column("planner_effort", sa.String(length=20), nullable=True),
        sa.Column("reviewer_provider", sa.String(length=20), nullable=True),
        sa.Column("reviewer_model", sa.String(length=100), nullable=True),
        sa.Column("reviewer_effort", sa.String(length=20), nullable=True),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("review_verdict", sa.String(length=20), nullable=True),
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
        unique=False,
    )
    op.create_index(
        "ix_plan_agent_runs_status",
        "plan_agent_runs",
        ["status"],
        unique=False,
    )

    op.create_table(
        "plan_agent_steps",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("step_type", sa.String(length=20), nullable=False),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("model", sa.String(length=100), nullable=True),
        sa.Column("effort", sa.String(length=20), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("output", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_plan_agent_steps_run_id",
        "plan_agent_steps",
        ["run_id"],
        unique=False,
    )
