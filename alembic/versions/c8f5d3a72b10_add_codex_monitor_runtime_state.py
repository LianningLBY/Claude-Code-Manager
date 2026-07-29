"""add codex monitor runtime state

Revision ID: c8f5d3a72b10
Revises: b7e4c2d91a63
Create Date: 2026-07-29
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c8f5d3a72b10"
down_revision: Union[str, None] = "b7e4c2d91a63"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("sub_agent_sessions") as batch_op:
        batch_op.add_column(
            sa.Column("codex_thread_id", sa.String(length=255), nullable=True)
        )
        batch_op.add_column(
            sa.Column("codex_home", sa.Text(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("codex_account_id", sa.String(length=100), nullable=True)
        )
        batch_op.add_column(
            sa.Column("codex_effort_level", sa.String(length=20), nullable=True)
        )
        batch_op.add_column(
            sa.Column("codex_service_tier", sa.String(length=20), nullable=True)
        )
        batch_op.add_column(
            sa.Column("codex_cwd", sa.Text(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "codex_disable_project_config",
                sa.Boolean(),
                server_default=sa.false(),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "codex_cleanup_pending",
                sa.Boolean(),
                server_default=sa.false(),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("codex_cleanup_error", sa.Text(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("sub_agent_sessions") as batch_op:
        batch_op.drop_column("codex_cleanup_error")
        batch_op.drop_column("codex_cleanup_pending")
        batch_op.drop_column("codex_disable_project_config")
        batch_op.drop_column("codex_cwd")
        batch_op.drop_column("codex_service_tier")
        batch_op.drop_column("codex_effort_level")
        batch_op.drop_column("codex_account_id")
        batch_op.drop_column("codex_home")
        batch_op.drop_column("codex_thread_id")
