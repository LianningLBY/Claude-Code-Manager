"""add Plan Step stream telemetry

Revision ID: b5f7c9d1e3a4
Revises: a4d9e2c7b1f0
Create Date: 2026-08-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b5f7c9d1e3a4"
down_revision: Union[str, None] = "a4d9e2c7b1f0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("plan_agent_steps") as batch:
        batch.add_column(sa.Column("last_delta_at", sa.DateTime(), nullable=True))
        batch.add_column(
            sa.Column(
                "streamed_output_chars",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(
            sa.Column("last_event_type", sa.String(length=100), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("plan_agent_steps") as batch:
        batch.drop_column("last_event_type")
        batch.drop_column("streamed_output_chars")
        batch.drop_column("last_delta_at")
