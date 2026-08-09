"""freeze Browser child launch identity and merge migration heads

Revision ID: 2c6e8a0b4d1f
Revises: f1c4e6a8b0d2, d3c8a7f1e620
Create Date: 2026-08-09
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "2c6e8a0b4d1f"
down_revision: Union[str, Sequence[str], None] = (
    "f1c4e6a8b0d2",
    "d3c8a7f1e620",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "test_harness_runs",
        sa.Column("task_incarnation_id", sa.String(length=32), nullable=True),
    )
    op.create_index(
        "ix_test_harness_runs_task_incarnation_id",
        "test_harness_runs",
        ["task_incarnation_id"],
    )
    op.add_column(
        "workspace_review_runs",
        sa.Column("task_incarnation_id", sa.String(length=32), nullable=True),
    )
    op.create_index(
        "ix_workspace_review_runs_task_incarnation_id",
        "workspace_review_runs",
        ["task_incarnation_id"],
    )

    for name, column_type in (
        ("owner_task_incarnation_id", sa.String(length=32)),
        ("child_task_incarnation_id", sa.String(length=32)),
        ("provider", sa.String(length=24)),
        ("model", sa.String(length=100)),
        ("reasoning_effort", sa.String(length=20)),
        ("codex_service_tier", sa.String(length=20)),
        ("skill_name", sa.String(length=80)),
    ):
        op.add_column(
            "test_harness_child_bindings",
            sa.Column(name, column_type, nullable=True),
        )

    # Historical terminal rows may outlive their Tasks, so the new columns
    # remain nullable. Every new binding requires the complete tuple, while
    # startup recovery fails closed for an active legacy row that cannot be
    # backfilled from its exact owner/child incarnation.
    op.execute(
        sa.text(
            "UPDATE test_harness_runs SET task_incarnation_id = "
            "(SELECT tasks.incarnation_id FROM tasks "
            "WHERE tasks.id = test_harness_runs.task_id) "
            "WHERE task_incarnation_id IS NULL"
        )
    )
    op.execute(
        sa.text(
            "UPDATE workspace_review_runs SET task_incarnation_id = "
            "(SELECT tasks.incarnation_id FROM tasks "
            "WHERE tasks.id = workspace_review_runs.task_id) "
            "WHERE task_incarnation_id IS NULL"
        )
    )
    op.execute(
        sa.text(
            "UPDATE test_harness_child_bindings SET "
            "owner_task_incarnation_id = (SELECT tasks.incarnation_id FROM tasks "
            "WHERE tasks.id = test_harness_child_bindings.owner_task_id), "
            "child_task_incarnation_id = (SELECT tasks.incarnation_id FROM tasks "
            "WHERE tasks.id = test_harness_child_bindings.child_task_id), "
            "provider = (SELECT tasks.provider FROM tasks "
            "WHERE tasks.id = test_harness_child_bindings.child_task_id), "
            "model = (SELECT tasks.model FROM tasks "
            "WHERE tasks.id = test_harness_child_bindings.child_task_id), "
            "reasoning_effort = (SELECT tasks.effort_level FROM tasks "
            "WHERE tasks.id = test_harness_child_bindings.child_task_id), "
            "codex_service_tier = (SELECT tasks.codex_service_tier FROM tasks "
            "WHERE tasks.id = test_harness_child_bindings.child_task_id), "
            "skill_name = 'browser-review'"
        )
    )


def downgrade() -> None:
    for name in (
        "skill_name",
        "codex_service_tier",
        "reasoning_effort",
        "model",
        "provider",
        "child_task_incarnation_id",
        "owner_task_incarnation_id",
    ):
        op.drop_column("test_harness_child_bindings", name)
    op.drop_index(
        "ix_workspace_review_runs_task_incarnation_id",
        table_name="workspace_review_runs",
    )
    op.drop_column("workspace_review_runs", "task_incarnation_id")
    op.drop_index(
        "ix_test_harness_runs_task_incarnation_id",
        table_name="test_harness_runs",
    )
    op.drop_column("test_harness_runs", "task_incarnation_id")
