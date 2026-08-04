"""add audited PR finding actions

Revision ID: b7c9e2f4a610
Revises: 7a1d4e9c2b60
Create Date: 2026-08-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b7c9e2f4a610"
down_revision: Union[str, None] = "7a1d4e9c2b60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pr_finding_actions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("finding_id", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("human_advice", sa.Text(), nullable=True),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("expected_head_sha", sa.String(length=64), nullable=False),
        sa.Column("patch_sha256", sa.String(length=64), nullable=True),
        sa.Column("operation_token", sa.String(length=64), nullable=True),
        sa.Column("operation_expires_at", sa.DateTime(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "action_type IN ('ignore', 'human_advice', 'ai_fix')",
            name="ck_pr_finding_actions_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'awaiting_confirmation', "
            "'completed', 'failed', 'cancelled', 'stale')",
            name="ck_pr_finding_actions_status",
        ),
        sa.ForeignKeyConstraint(["finding_id"], ["pr_findings.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_pr_finding_actions_idempotency_key",
        ),
    )
    op.create_index(
        "ix_pr_finding_actions_finding_id",
        "pr_finding_actions",
        ["finding_id"],
    )
    op.create_index(
        "ix_pr_finding_actions_status",
        "pr_finding_actions",
        ["status"],
    )
    op.create_index(
        "ix_pr_finding_actions_actor_user_id",
        "pr_finding_actions",
        ["actor_user_id"],
    )
    op.create_index(
        "ix_pr_finding_actions_task_id",
        "pr_finding_actions",
        ["task_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_pr_finding_actions_task_id", table_name="pr_finding_actions")
    op.drop_index("ix_pr_finding_actions_actor_user_id", table_name="pr_finding_actions")
    op.drop_index("ix_pr_finding_actions_status", table_name="pr_finding_actions")
    op.drop_index("ix_pr_finding_actions_finding_id", table_name="pr_finding_actions")
    op.drop_table("pr_finding_actions")
