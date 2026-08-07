"""merge first-class Plan and attention-tag migration heads

Revision ID: e5b8d1c4a7f2
Revises: d4a7c9e2f1b6, 2f6c8a1d4e90
Create Date: 2026-08-05
"""

from typing import Sequence, Union


revision: str = "e5b8d1c4a7f2"
down_revision: Union[str, Sequence[str], None] = (
    "d4a7c9e2f1b6",
    "2f6c8a1d4e90",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Join the feature and main histories without changing schema."""


def downgrade() -> None:
    """Split back to both published heads without changing schema."""
