"""add PR review base SHA snapshot

Revision ID: 5f7a9c2e4d61
Revises: c8f5d3a72b10
Create Date: 2026-07-31
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "5f7a9c2e4d61"
down_revision: Union[str, None] = "c8f5d3a72b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("log_entries", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("task_retry_count", sa.Integer(), nullable=True)
        )

    # Legacy rows keep NULL base_sha. New webhook rows always persist a
    # validated base/head pair, so the exact snapshot is the idempotency key.
    with op.batch_alter_table("pr_reviews", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("base_sha", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(
            sa.Column("action_nonce", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(
            sa.Column("pending_action", sa.String(length=50), nullable=True)
        )
        batch_op.add_column(
            sa.Column("pending_review_body", sa.Text(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("publishing_actor", sa.String(length=200), nullable=True)
        )
        batch_op.add_column(
            sa.Column("publishing_retry_count", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "publishing_task_started_at",
                sa.DateTime(),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("publishing_started_at", sa.DateTime(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "publishing_lease_token",
                sa.String(length=64),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "publishing_lease_expires_at",
                sa.DateTime(),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("superseding_snapshot", sa.JSON(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "superseding_token",
                sa.String(length=64),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "superseding_started_at",
                sa.DateTime(),
                nullable=True,
            )
        )
        batch_op.drop_constraint(
            "uq_pr_reviews_repo_pr_head",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_pr_reviews_repo_pr_base_head",
            ["repo_id", "pr_number", "base_sha", "head_sha"],
        )


def downgrade() -> None:
    # The upgraded schema legitimately permits one head to be reviewed
    # against multiple base commits. The legacy constraint did not. Preserve
    # every row during rollback by keeping the newest row's head key and
    # clearing the nullable head_sha on older conflicting snapshots before
    # recreating the narrower unique constraint.
    bind = op.get_bind()
    duplicate_heads = bind.execute(sa.text("""
        SELECT repo_id, pr_number, head_sha
        FROM pr_reviews
        WHERE head_sha IS NOT NULL
        GROUP BY repo_id, pr_number, head_sha
        HAVING COUNT(*) > 1
    """)).fetchall()
    for repo_id, pr_number, head_sha in duplicate_heads:
        conflicting_ids = bind.execute(
            sa.text("""
                SELECT id
                FROM pr_reviews
                WHERE repo_id = :repo_id
                  AND pr_number = :pr_number
                  AND head_sha = :head_sha
                ORDER BY id DESC
            """),
            {
                "repo_id": repo_id,
                "pr_number": pr_number,
                "head_sha": head_sha,
            },
        ).scalars().all()
        for review_id in conflicting_ids[1:]:
            bind.execute(
                sa.text("""
                    UPDATE pr_reviews
                    SET head_sha = NULL
                    WHERE id = :review_id
                      AND repo_id = :repo_id
                      AND pr_number = :pr_number
                      AND head_sha = :head_sha
                """),
                {
                    "review_id": review_id,
                    "repo_id": repo_id,
                    "pr_number": pr_number,
                    "head_sha": head_sha,
                },
            )

    with op.batch_alter_table("pr_reviews", schema=None) as batch_op:
        batch_op.drop_constraint(
            "uq_pr_reviews_repo_pr_base_head",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_pr_reviews_repo_pr_head",
            ["repo_id", "pr_number", "head_sha"],
        )
        batch_op.drop_column("superseding_started_at")
        batch_op.drop_column("superseding_token")
        batch_op.drop_column("superseding_snapshot")
        batch_op.drop_column("publishing_lease_expires_at")
        batch_op.drop_column("publishing_lease_token")
        batch_op.drop_column("publishing_started_at")
        batch_op.drop_column("publishing_task_started_at")
        batch_op.drop_column("publishing_retry_count")
        batch_op.drop_column("publishing_actor")
        batch_op.drop_column("pending_review_body")
        batch_op.drop_column("pending_action")
        batch_op.drop_column("action_nonce")
        batch_op.drop_column("base_sha")

    with op.batch_alter_table("log_entries", schema=None) as batch_op:
        batch_op.drop_column("task_retry_count")
