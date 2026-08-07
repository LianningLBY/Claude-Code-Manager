"""add durable Plan delivery and Worker import identity

Revision ID: c6e8a1f4d2b7
Revises: b5f7c9d1e3a4
Create Date: 2026-08-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c6e8a1f4d2b7"
down_revision: Union[str, None] = "b5f7c9d1e3a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.add_column(
            sa.Column("import_payload_digest", sa.String(length=64), nullable=True)
        )
        batch.add_column(
            sa.Column("import_attachment_receipt", sa.JSON(), nullable=True)
        )

    with op.batch_alter_table("plan_application_receipts") as batch:
        batch.add_column(
            sa.Column(
                "delivery_status",
                sa.String(length=20),
                nullable=False,
                server_default="pending",
            )
        )
        batch.add_column(sa.Column("outbox_payload", sa.JSON(), nullable=True))
        batch.add_column(
            sa.Column("payload_digest", sa.String(length=64), nullable=True)
        )
        batch.add_column(sa.Column("delivery_error", sa.Text(), nullable=True))
        batch.add_column(sa.Column("launch_evidence", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("delivery_resolution", sa.JSON(), nullable=True))
        batch.create_index(
            "ix_plan_application_receipts_delivery_status",
            ["delivery_status"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("plan_application_receipts") as batch:
        batch.drop_index("ix_plan_application_receipts_delivery_status")
        batch.drop_column("delivery_error")
        batch.drop_column("delivery_resolution")
        batch.drop_column("launch_evidence")
        batch.drop_column("payload_digest")
        batch.drop_column("outbox_payload")
        batch.drop_column("delivery_status")

    with op.batch_alter_table("plan_agent_runs") as batch:
        batch.drop_column("import_attachment_receipt")
        batch.drop_column("import_payload_digest")
