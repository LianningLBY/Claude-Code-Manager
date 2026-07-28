"""add Codex service tier to tasks

Revision ID: f4c8d1a29b70
Revises: e4c9f2a71b03
Create Date: 2026-07-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f4c8d1a29b70"
down_revision: Union[str, None] = "e4c9f2a71b03"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # The non-null server default backfills every historical Task as Standard.
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "codex_service_tier",
                sa.String(length=20),
                nullable=False,
                server_default="default",
            )
        )
        batch_op.create_check_constraint(
            "ck_tasks_codex_service_tier",
            "codex_service_tier IN ('default', 'priority')",
        )


def downgrade() -> None:
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_constraint(
            "ck_tasks_codex_service_tier",
            type_="check",
        )
        batch_op.drop_column("codex_service_tier")
