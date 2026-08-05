"""add attention tag to tasks

Revision ID: 2f6c8a1d4e90
Revises: b7c9e2f4a610
Create Date: 2026-08-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "2f6c8a1d4e90"
down_revision: Union[str, None] = "b7c9e2f4a610"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "attention_tag",
                sa.String(length=80),
                nullable=True,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_column("attention_tag")
