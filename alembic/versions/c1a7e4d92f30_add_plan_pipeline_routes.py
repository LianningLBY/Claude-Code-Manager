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


def upgrade() -> None:
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
