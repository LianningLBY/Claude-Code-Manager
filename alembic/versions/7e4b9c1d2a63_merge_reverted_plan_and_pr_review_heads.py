"""merge reverted Plan and PR review migration heads

Revision ID: 7e4b9c1d2a63
Revises: f7a1c3d9e5b2, 5f7a9c2e4d61
Create Date: 2026-08-03
"""

from typing import Sequence, Union


revision: str = "7e4b9c1d2a63"
down_revision: Union[str, Sequence[str], None] = (
    "f7a1c3d9e5b2",
    "5f7a9c2e4d61",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Join the two published branches without rewriting either history."""


def downgrade() -> None:
    """Split back to the two published branch heads without schema changes."""
