"""add PR review panel and findings

Revision ID: 7a1d4e9c2b60
Revises: 5f7a9c2e4d61
Create Date: 2026-08-02
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "7a1d4e9c2b60"
down_revision: Union[str, None] = "5f7a9c2e4d61"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("monitored_repos", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "review_mode",
                sa.String(length=20),
                server_default="single",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "wait_for_ci",
                sa.Boolean(),
                server_default=sa.text("0"),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("required_checks", sa.JSON(), nullable=True)
        )
        batch_op.add_column(sa.Column("auto_repair", sa.Boolean(), server_default=sa.text("0"), nullable=False))
        batch_op.add_column(sa.Column("max_repair_attempts", sa.Integer(), server_default="3", nullable=False))
        batch_op.add_column(sa.Column("merge_queue_mode", sa.String(length=20), server_default="manual", nullable=False))

    op.create_table(
        "pr_monitor_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("repo_id", sa.Integer(), sa.ForeignKey("monitored_repos.id"), nullable=False),
        sa.Column("pr_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="observing", nullable=False),
        sa.Column("current_base_sha", sa.String(length=64), nullable=False),
        sa.Column("current_head_sha", sa.String(length=64), nullable=False),
        sa.Column("current_review_id", sa.Integer(), nullable=True),
        sa.Column("developer_task_id", sa.Integer(), sa.ForeignKey("tasks.id"), nullable=True),
        sa.Column("head_repo_full_name", sa.String(length=200), nullable=True),
        sa.Column("head_branch", sa.String(length=200), nullable=True),
        sa.Column("repair_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_repair_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column("no_progress_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("state_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("pause_reason", sa.Text(), nullable=True),
        sa.Column("binding_verified_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("repo_id", "pr_number", name="uq_pr_monitor_runs_repo_pr"),
    )
    op.create_index("ix_pr_monitor_runs_repo_id", "pr_monitor_runs", ["repo_id"])
    op.create_index("ix_pr_monitor_runs_current_review_id", "pr_monitor_runs", ["current_review_id"])
    op.create_index("ix_pr_monitor_runs_developer_task_id", "pr_monitor_runs", ["developer_task_id"])

    with op.batch_alter_table("pr_reviews", schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            "monitor_run_id",
            sa.Integer(),
            sa.ForeignKey("pr_monitor_runs.id", name="fk_pr_reviews_monitor_run_id_pr_monitor_runs"),
            nullable=True,
        ))
        batch_op.create_index("ix_pr_reviews_monitor_run_id", ["monitor_run_id"])
        batch_op.add_column(sa.Column("ci_status", sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column("ci_summary", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("ci_details", sa.JSON(), nullable=True))

    op.create_table(
        "pr_reviewer_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("pr_review_id", sa.Integer(), nullable=False),
        sa.Column("role", sa.String(length=40), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("model", sa.String(length=100), nullable=True),
        sa.Column("effort", sa.String(length=20), nullable=True),
        sa.Column("status", sa.String(length=30), server_default="pending", nullable=False),
        sa.Column("verdict", sa.String(length=30), nullable=True),
        sa.Column("result_body", sa.Text(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("prompt_policy_hash", sa.String(length=64), nullable=False),
        sa.Column("guide_pack_hash", sa.String(length=64), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["pr_review_id"], ["pr_reviews.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("pr_review_id", "role", name="uq_pr_reviewer_runs_review_role"),
        sa.UniqueConstraint("task_id", name="uq_pr_reviewer_runs_task_id"),
    )
    op.create_index("ix_pr_reviewer_runs_pr_review_id", "pr_reviewer_runs", ["pr_review_id"])
    op.create_index("ix_pr_reviewer_runs_task_id", "pr_reviewer_runs", ["task_id"])

    op.create_table(
        "pr_findings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("pr_review_id", sa.Integer(), nullable=False),
        sa.Column("reviewer_run_id", sa.Integer(), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("role", sa.String(length=40), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column("path", sa.String(length=1000), nullable=False),
        sa.Column("line", sa.Integer(), nullable=True),
        sa.Column("hunk", sa.String(length=500), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=False),
        sa.Column("impact", sa.Text(), nullable=False),
        sa.Column("required_fix", sa.Text(), nullable=False),
        sa.Column("test", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="open", nullable=False),
        sa.Column("thread_nonce", sa.String(length=64), nullable=False),
        sa.Column("thread_status", sa.String(length=30), server_default="pending", nullable=False),
        sa.Column("github_comment_id", sa.Integer(), nullable=True),
        sa.Column("github_comment_url", sa.String(length=1000), nullable=True),
        sa.Column("github_thread_node_id", sa.String(length=200), nullable=True),
        sa.Column("thread_error", sa.Text(), nullable=True),
        sa.Column("thread_published_at", sa.DateTime(), nullable=True),
        sa.Column("thread_resolved_at", sa.DateTime(), nullable=True),
        sa.Column("base_sha", sa.String(length=64), nullable=False),
        sa.Column("head_sha", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["pr_review_id"], ["pr_reviews.id"]),
        sa.ForeignKeyConstraint(["reviewer_run_id"], ["pr_reviewer_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("reviewer_run_id", "fingerprint", name="uq_pr_findings_run_fingerprint"),
    )
    op.create_index("ix_pr_findings_pr_review_id", "pr_findings", ["pr_review_id"])
    op.create_index("ix_pr_findings_reviewer_run_id", "pr_findings", ["reviewer_run_id"])

    op.create_table(
        "pr_finding_rebuttals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("finding_id", sa.Integer(), sa.ForeignKey("pr_findings.id"), nullable=False),
        sa.Column("pr_review_id", sa.Integer(), sa.ForeignKey("pr_reviews.id"), nullable=False),
        sa.Column("monitor_run_id", sa.Integer(), sa.ForeignKey("pr_monitor_runs.id"), nullable=False),
        sa.Column("developer_task_id", sa.Integer(), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("task_id", sa.Integer(), sa.ForeignKey("tasks.id"), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("base_sha", sa.String(length=64), nullable=False),
        sa.Column("head_sha", sa.String(length=64), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=False),
        sa.Column("evidence_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="pending", nullable=False),
        sa.Column("verdict", sa.String(length=20), nullable=True),
        sa.Column("result_body", sa.Text(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("resolution_nonce", sa.String(length=64), nullable=False),
        sa.Column("resolution_actor", sa.String(length=200), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("finding_id", "attempt", name="uq_pr_finding_rebuttals_attempt"),
        sa.UniqueConstraint("task_id", name="uq_pr_finding_rebuttals_task_id"),
    )
    op.create_index("ix_pr_finding_rebuttals_finding_id", "pr_finding_rebuttals", ["finding_id"])
    op.create_index("ix_pr_finding_rebuttals_pr_review_id", "pr_finding_rebuttals", ["pr_review_id"])
    op.create_index("ix_pr_finding_rebuttals_monitor_run_id", "pr_finding_rebuttals", ["monitor_run_id"])
    op.create_index("ix_pr_finding_rebuttals_developer_task_id", "pr_finding_rebuttals", ["developer_task_id"])
    op.create_index("ix_pr_finding_rebuttals_task_id", "pr_finding_rebuttals", ["task_id"])

    op.create_table(
        "pr_repair_wakes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("monitor_run_id", sa.Integer(), sa.ForeignKey("pr_monitor_runs.id"), nullable=False),
        sa.Column("review_id", sa.Integer(), sa.ForeignKey("pr_reviews.id"), nullable=True),
        sa.Column("developer_task_id", sa.Integer(), sa.ForeignKey("tasks.id"), nullable=True),
        sa.Column("trigger_base_sha", sa.String(length=64), nullable=False),
        sa.Column("trigger_head_sha", sa.String(length=64), nullable=False),
        sa.Column("reason_kind", sa.String(length=30), nullable=False),
        sa.Column("evidence_hash", sa.String(length=64), nullable=False),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="shadow", nullable=False),
        sa.Column("attempt", sa.Integer(), server_default="1", nullable=False),
        sa.Column("delivery_token", sa.String(length=64), nullable=False),
        sa.Column("accepted_worker_id", sa.Integer(), nullable=True),
        sa.Column("accepted_task_retry_count", sa.Integer(), nullable=True),
        sa.Column("accepted_session_id", sa.String(length=200), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("monitor_run_id", "trigger_head_sha", "evidence_hash", name="uq_pr_repair_wakes_subject_evidence"),
    )
    op.create_index("ix_pr_repair_wakes_monitor_run_id", "pr_repair_wakes", ["monitor_run_id"])
    op.create_index("ix_pr_repair_wakes_review_id", "pr_repair_wakes", ["review_id"])
    op.create_index("ix_pr_repair_wakes_developer_task_id", "pr_repair_wakes", ["developer_task_id"])

    op.create_table(
        "pr_merge_queue_actions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("monitor_run_id", sa.Integer(), sa.ForeignKey("pr_monitor_runs.id"), nullable=False),
        sa.Column("review_id", sa.Integer(), sa.ForeignKey("pr_reviews.id"), nullable=False),
        sa.Column("trigger_base_sha", sa.String(length=64), nullable=False),
        sa.Column("trigger_head_sha", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="shadow", nullable=False),
        sa.Column("action_nonce", sa.String(length=64), nullable=False),
        sa.Column("github_pr_node_id", sa.String(length=200), nullable=True),
        sa.Column("github_queue_entry_id", sa.String(length=200), nullable=True),
        sa.Column("merge_group_sha", sa.String(length=64), nullable=True),
        sa.Column("merge_group_ref", sa.String(length=500), nullable=True),
        sa.Column("ci_status", sa.String(length=20), nullable=True),
        sa.Column("ci_details", sa.JSON(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("monitor_run_id", "trigger_head_sha", name="uq_pr_merge_queue_actions_run_head"),
    )
    op.create_index("ix_pr_merge_queue_actions_monitor_run_id", "pr_merge_queue_actions", ["monitor_run_id"])
    op.create_index("ix_pr_merge_queue_actions_review_id", "pr_merge_queue_actions", ["review_id"])


def downgrade() -> None:
    op.drop_index("ix_pr_merge_queue_actions_review_id", table_name="pr_merge_queue_actions")
    op.drop_index("ix_pr_merge_queue_actions_monitor_run_id", table_name="pr_merge_queue_actions")
    op.drop_table("pr_merge_queue_actions")
    op.drop_index("ix_pr_repair_wakes_developer_task_id", table_name="pr_repair_wakes")
    op.drop_index("ix_pr_repair_wakes_review_id", table_name="pr_repair_wakes")
    op.drop_index("ix_pr_repair_wakes_monitor_run_id", table_name="pr_repair_wakes")
    op.drop_table("pr_repair_wakes")
    op.drop_index("ix_pr_finding_rebuttals_task_id", table_name="pr_finding_rebuttals")
    op.drop_index("ix_pr_finding_rebuttals_developer_task_id", table_name="pr_finding_rebuttals")
    op.drop_index("ix_pr_finding_rebuttals_monitor_run_id", table_name="pr_finding_rebuttals")
    op.drop_index("ix_pr_finding_rebuttals_pr_review_id", table_name="pr_finding_rebuttals")
    op.drop_index("ix_pr_finding_rebuttals_finding_id", table_name="pr_finding_rebuttals")
    op.drop_table("pr_finding_rebuttals")
    op.drop_index("ix_pr_findings_reviewer_run_id", table_name="pr_findings")
    op.drop_index("ix_pr_findings_pr_review_id", table_name="pr_findings")
    op.drop_table("pr_findings")
    op.drop_index("ix_pr_reviewer_runs_task_id", table_name="pr_reviewer_runs")
    op.drop_index("ix_pr_reviewer_runs_pr_review_id", table_name="pr_reviewer_runs")
    op.drop_table("pr_reviewer_runs")
    with op.batch_alter_table("pr_reviews", schema=None) as batch_op:
        batch_op.drop_column("ci_details")
        batch_op.drop_column("ci_summary")
        batch_op.drop_column("ci_status")
        batch_op.drop_index("ix_pr_reviews_monitor_run_id")
        batch_op.drop_column("monitor_run_id")
    op.drop_index("ix_pr_monitor_runs_developer_task_id", table_name="pr_monitor_runs")
    op.drop_index("ix_pr_monitor_runs_current_review_id", table_name="pr_monitor_runs")
    op.drop_index("ix_pr_monitor_runs_repo_id", table_name="pr_monitor_runs")
    op.drop_table("pr_monitor_runs")
    with op.batch_alter_table("monitored_repos", schema=None) as batch_op:
        batch_op.drop_column("max_repair_attempts")
        batch_op.drop_column("auto_repair")
        batch_op.drop_column("merge_queue_mode")
        batch_op.drop_column("required_checks")
        batch_op.drop_column("wait_for_ci")
        batch_op.drop_column("review_mode")
