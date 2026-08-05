"""add Plan pipeline primary and fallback route snapshots

Revision ID: c1a7e4d92f30
Revises: b6e1f4a2c9d7
Create Date: 2026-07-30
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c1a7e4d92f30"
down_revision: Union[str, None] = "b6e1f4a2c9d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _restore_reverted_plan_schema() -> None:
    """Restore the published carrier schema after main's Plan revert."""

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    task_columns = {column["name"] for column in inspector.get_columns("tasks")}
    carrier_columns = {
        "plan_target_task_id",
        "plan_context_session_id",
        "plan_context_log_id",
        "plan_context_snapshot",
        "plan_repo_revision",
        "supersedes_plan_task_id",
        "plan_approved_at",
        "plan_approved_by",
        "plan_applied_at",
        "plan_applied_to_session_id",
        "plan_applied_log_id",
        "plan_execution_task_id",
    }
    present_carrier_columns = carrier_columns & task_columns
    if present_carrier_columns and present_carrier_columns != carrier_columns:
        raise RuntimeError("partially restored legacy Plan Task schema")
    if not present_carrier_columns:
        with op.batch_alter_table("tasks", schema=None) as batch_op:
            batch_op.add_column(sa.Column("plan_target_task_id", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("plan_context_session_id", sa.String(length=200), nullable=True))
            batch_op.add_column(sa.Column("plan_context_log_id", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("plan_context_snapshot", sa.Text(), nullable=True))
            batch_op.add_column(sa.Column("plan_repo_revision", sa.JSON(), nullable=True))
            batch_op.add_column(sa.Column("supersedes_plan_task_id", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("plan_approved_at", sa.DateTime(), nullable=True))
            batch_op.add_column(sa.Column("plan_approved_by", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("plan_applied_at", sa.DateTime(), nullable=True))
            batch_op.add_column(sa.Column("plan_applied_to_session_id", sa.String(length=200), nullable=True))
            batch_op.add_column(sa.Column("plan_applied_log_id", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("plan_execution_task_id", sa.Integer(), nullable=True))
            batch_op.create_index("ix_tasks_plan_target_task_id", ["plan_target_task_id"], unique=False)
            batch_op.create_index("ix_tasks_supersedes_plan_task_id", ["supersedes_plan_task_id"], unique=False)

    carrier_tables = {"plan_agent_runs", "plan_agent_steps"} & tables
    if carrier_tables and carrier_tables != {"plan_agent_runs", "plan_agent_steps"}:
        raise RuntimeError("partially restored legacy Plan audit schema")
    if not carrier_tables:
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
        op.create_index("ix_plan_agent_runs_plan_task_id", "plan_agent_runs", ["plan_task_id"], unique=False)
        op.create_index("ix_plan_agent_runs_status", "plan_agent_runs", ["status"], unique=False)
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
        op.create_index("ix_plan_agent_steps_run_id", "plan_agent_steps", ["run_id"], unique=False)


def upgrade() -> None:
    _restore_reverted_plan_schema()

    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("plan_pipeline_config", sa.JSON(), nullable=True)
        )

    with op.batch_alter_table("plan_agent_runs", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("pipeline_config", sa.JSON(), nullable=True)
        )

    with op.batch_alter_table("plan_agent_steps", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("route_slot", sa.String(length=20), nullable=True)
        )
        batch_op.add_column(
            sa.Column("account_id", sa.String(length=100), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("plan_agent_steps", schema=None) as batch_op:
        batch_op.drop_column("account_id")
        batch_op.drop_column("route_slot")

    with op.batch_alter_table("plan_agent_runs", schema=None) as batch_op:
        batch_op.drop_column("pipeline_config")

    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_column("plan_pipeline_config")
