"""add independent Plan Task relationships and pipeline audit

Revision ID: b6e1f4a2c9d7
Revises: c8f5d3a72b10
Create Date: 2026-07-29
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b6e1f4a2c9d7"
down_revision: Union[str, None] = "c8f5d3a72b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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


def downgrade() -> None:
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
