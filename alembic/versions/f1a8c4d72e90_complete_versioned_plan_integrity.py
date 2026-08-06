"""complete versioned Plan integrity and recovery protocol

Revision ID: f1a8c4d72e90
Revises: e7c4a21d9b30
Create Date: 2026-08-02
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f1a8c4d72e90"
down_revision: Union[str, None] = "e7c4a21d9b30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("plan_versions") as batch:
        batch.add_column(sa.Column("reviewer_repo_revision", sa.JSON(), nullable=True))
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.add_column(sa.Column("source_run_id", sa.Integer(), nullable=True))
    with op.batch_alter_table("plan_legacy_task_links") as batch:
        batch.add_column(sa.Column("plan_run_id", sa.Integer(), nullable=True))
    op.execute(sa.text(
        """UPDATE plan_legacy_task_links SET plan_run_id=(
        SELECT MAX(plan_agent_runs.id) FROM plan_agent_runs
        WHERE plan_agent_runs.plan_task_id=plan_legacy_task_links.legacy_task_id
        )"""
    ))
    with op.batch_alter_table("plan_applications") as batch:
        batch.add_column(sa.Column("application_receipt_key", sa.String(200), nullable=True))
        batch.create_index(
            "ix_plan_applications_application_receipt_key",
            ["application_receipt_key"],
        )
        batch.create_check_constraint(
            "ck_plan_application_target",
            "(application_type = 'chat_message' AND user_log_id IS NOT NULL "
            "AND execution_task_id IS NULL) OR "
            "(application_type = 'execution_task' AND execution_task_id IS NOT NULL "
            "AND user_log_id IS NULL)",
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
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("receipt_key", name="uq_plan_application_receipt_key"),
    )
    op.create_index(
        "ix_plan_application_receipts_target_task_id",
        "plan_application_receipts",
        ["target_task_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_plan_application_receipts_target_task_id",
        table_name="plan_application_receipts",
    )
    op.drop_table("plan_application_receipts")
    with op.batch_alter_table("plan_applications") as batch:
        batch.drop_constraint("ck_plan_application_target", type_="check")
        batch.drop_index("ix_plan_applications_application_receipt_key")
        batch.drop_column("application_receipt_key")
    with op.batch_alter_table("plan_legacy_task_links") as batch:
        batch.drop_column("plan_run_id")
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.drop_column("source_run_id")
    with op.batch_alter_table("plan_versions") as batch:
        batch.drop_column("reviewer_repo_revision")
