"""add PR monitor review effort

Revision ID: d8f0a1b2c3d4
Revises: c7e9b1d42f60
Create Date: 2026-07-27 15:00:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "d8f0a1b2c3d4"
down_revision = "c7e9b1d42f60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "monitored_repos",
        sa.Column("review_effort", sa.String(length=20), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("monitored_repos", "review_effort")
