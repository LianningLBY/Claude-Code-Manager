"""add PTY background generation token

Revision ID: a84f2d9c7e31
Revises: f4c8d1a29b70
Create Date: 2026-07-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a84f2d9c7e31"
down_revision: Union[str, None] = "f4c8d1a29b70"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column(
            "pty_background_generation",
            sa.String(length=64),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("tasks", "pty_background_generation")
