"""add persisted global Plan pipeline settings

Revision ID: d2b8f6a10c43
Revises: c1a7e4d92f30
Create Date: 2026-07-31
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d2b8f6a10c43"
down_revision: Union[str, None] = "c1a7e4d92f30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("global_settings", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("plan_pipeline_config", sa.JSON(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("global_settings", schema=None) as batch_op:
        batch_op.drop_column("plan_pipeline_config")
