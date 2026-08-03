"""repair applied Plan Version human decisions

Revision ID: f5b7c9d1e3a2
Revises: f1a8c4d72e90
Create Date: 2026-08-03
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f5b7c9d1e3a2"
down_revision: Union[str, None] = "f1a8c4d72e90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    contradictory = (
        bind.execute(
            sa.text(
                """SELECT plan_versions.id
        FROM plan_versions
        WHERE plan_versions.human_decision = 'rejected'
          AND EXISTS (
            SELECT 1 FROM plan_applications
            WHERE plan_applications.plan_version_id = plan_versions.id
          )
        ORDER BY plan_versions.id"""
            )
        )
        .scalars()
        .all()
    )
    if contradictory:
        raise RuntimeError(
            f"rejected Plan Versions have Application records: {list(contradictory)}"
        )

    # An Application is durable proof that the exact Version was accepted for
    # use. Some legacy rows recorded the application timestamp/target but left
    # plan_approved NULL, which the first backfill incorrectly mapped to
    # human_decision=pending. Preserve undecided superseded Versions, while
    # repairing only Versions with this unambiguous application evidence.
    bind.execute(
        sa.text(
            """UPDATE plan_versions
        SET human_decision = 'approved',
            decided_at = COALESCE(
              decided_at,
              (SELECT MIN(plan_applications.created_at)
               FROM plan_applications
               WHERE plan_applications.plan_version_id = plan_versions.id)
            ),
            decided_by = COALESCE(
              decided_by,
              (SELECT MAX(plan_applications.applied_by)
               FROM plan_applications
               WHERE plan_applications.plan_version_id = plan_versions.id)
            )
        WHERE human_decision = 'pending'
          AND EXISTS (
            SELECT 1 FROM plan_applications
            WHERE plan_applications.plan_version_id = plan_versions.id
          )"""
        )
    )


def downgrade() -> None:
    # This repair cannot distinguish a formerly inconsistent pending row from
    # a legitimately approved row after the fact. Reintroducing the invalid
    # pending+applied combination would be unsafe, so downgrade is a no-op.
    pass
