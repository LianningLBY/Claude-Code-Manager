"""add monitor scheduled turn state

Revision ID: b7e4c2d91a63
Revises: a84f2d9c7e31
Create Date: 2026-07-29
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b7e4c2d91a63"
down_revision: Union[str, None] = "a84f2d9c7e31"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("sub_agent_sessions") as batch_op:
        batch_op.add_column(
            sa.Column(
                "provider",
                sa.String(length=20),
                server_default="claude",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("next_check_at", sa.DateTime(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "turn_generation",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("active_turn_generation", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("turn_started_at", sa.DateTime(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "consecutive_failures",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("last_error", sa.Text(), nullable=True)
        )

    # Existing inactive rows do not need a due time. A historical live CCM
    # monitor becomes immediately recoverable after the deployment restart.
    op.execute(
        sa.text(
            "UPDATE sub_agent_sessions "
            "SET next_check_at = CURRENT_TIMESTAMP "
            "WHERE agent_type = 'monitor' "
            "AND source = 'ccm' "
            "AND status = 'running'"
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("sub_agent_sessions") as batch_op:
        batch_op.drop_column("last_error")
        batch_op.drop_column("consecutive_failures")
        batch_op.drop_column("turn_started_at")
        batch_op.drop_column("active_turn_generation")
        batch_op.drop_column("turn_generation")
        batch_op.drop_column("next_check_at")
        batch_op.drop_column("provider")
