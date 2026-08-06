"""add immutable Plan application attempt audit

Revision ID: d4a7c9e2f1b6
Revises: c6e8a1f4d2b7
Create Date: 2026-08-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4a7c9e2f1b6"
down_revision: Union[str, None] = "c6e8a1f4d2b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "plan_application_attempts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("plan_version_id", sa.Integer(), nullable=False),
        sa.Column(
            "application_receipt_key",
            sa.String(length=200),
            nullable=False,
        ),
        sa.Column("application_type", sa.String(length=30), nullable=False),
        sa.Column("target_task_id", sa.Integer(), nullable=True),
        sa.Column("target_session_id", sa.String(length=200), nullable=True),
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
        unique=False,
    )
    op.create_index(
        "ix_plan_application_attempts_plan_version_id",
        "plan_application_attempts",
        ["plan_version_id"],
        unique=False,
    )
    op.create_index(
        "ix_plan_application_attempts_application_receipt_key",
        "plan_application_attempts",
        ["application_receipt_key"],
        unique=False,
    )


def downgrade() -> None:
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
