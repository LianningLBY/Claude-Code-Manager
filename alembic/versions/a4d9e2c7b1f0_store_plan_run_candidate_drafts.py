"""store Plan Run candidate drafts until pipeline completion

Revision ID: a4d9e2c7b1f0
Revises: f6c8d0e2a4b1
Create Date: 2026-08-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a4d9e2c7b1f0"
down_revision: Union[str, None] = "f6c8d0e2a4b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.add_column(sa.Column("draft_content", sa.Text(), nullable=True))
        batch.add_column(sa.Column("draft_step_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("draft_repo_revision", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.drop_column("draft_repo_revision")
        batch.drop_column("draft_step_id")
        batch.drop_column("draft_content")
