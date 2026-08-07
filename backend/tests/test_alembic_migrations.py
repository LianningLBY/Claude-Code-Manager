"""Tests for Alembic migrations.

Ensures:
1. A legacy database (no alembic_version) can be migrated to head.
2. A fresh database can be created from scratch via migrations.
3. The final migrated schema matches the ORM models (no drift).
"""
import importlib.util
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
import pytest
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKeyConstraint,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.schema import CreateTable

# All ORM models must be imported so Base.metadata is complete.
from backend.database import Base
import backend.models.task  # noqa: F401
import backend.models.instance  # noqa: F401
import backend.models.project  # noqa: F401
import backend.models.project_todo  # noqa: F401
import backend.models.log_entry  # noqa: F401
import backend.models.worktree  # noqa: F401
import backend.models.global_settings  # noqa: F401
import backend.models.secret  # noqa: F401
import backend.models.quick_phrase  # noqa: F401
import backend.models.plan  # noqa: F401
import backend.models.capability  # noqa: F401
import backend.models.code_review  # noqa: F401
import backend.models.delivery  # noqa: F401
import backend.models.worker_task_termination  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PUBLISHED_PLAN_REVISION = "b6e1f4a2c9d7"
PLAN_CLEANUP_REVISION = "f7a1c3d9e5b2"
PR_REVIEW_SNAPSHOT_REVISION = "5f7a9c2e4d61"
PUBLISHED_BRANCH_MERGE_REVISION = "7e4b9c1d2a63"
PR_REVIEW_PANEL_REVISION = "7a1d4e9c2b60"
PR_FINDING_ACTIONS_REVISION = "b7c9e2f4a610"
ATTENTION_TAG_REVISION = "2f6c8a1d4e90"
PLAN_V2_REVISION = "3f2a9c8e7b10"
CAPABILITY_CORE_REVISION = "6a4c2e9f1b73"
CODE_REVIEW_REVISION = "8d4e1f7a9c20"
DELIVERY_LOOP_REVISION = "9e5b2a7c4d10"
AUTO_CAPABILITY_TURN_REVISION = "c3a7e9f1b2d4"
TERMINAL_ARBITRATION_REVISION = "4b8d2f6a1c90"
CURRENT_HEAD_REVISION = TERMINAL_ARBITRATION_REVISION
PUBLISHED_PLAN_CLEANUP_SHA256 = (
    "dd8cce93f05599ebc580cb95cad5d7d8875f03415775312718d3e42ef4369d16"
)
REMOVED_UNPUBLISHED_PLAN_REVISIONS = {
    "a4d9e2c7b1f0",
    "b5f7c9d1e3a4",
    "c1a7e4d92f30",
    "c6e8a1f4d2b7",
    "d2b8f6a10c43",
    "d4a7c9e2f1b6",
    "e5b8d1c4a7f2",
    "e7c4a21d9b30",
    "f1a8c4d72e90",
    "f5b7c9d1e3a2",
    "f6c8d0e2a4b1",
}


def _alembic_cfg(db_path: str) -> Config:
    """Create an Alembic Config pointing at a specific database file.

    Also patches backend.config.settings.database_url so that env.py
    (which reads settings at import time) uses the test DB, not production.
    """
    db_url = f"sqlite:///{db_path}"
    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _get_head_revision(cfg: Config) -> str:
    """Return the current head revision ID from migration scripts."""
    return ScriptDirectory.from_config(cfg).get_current_head()


def _run_alembic(cfg: Config, func, *args):
    """Run an Alembic command with settings.database_url patched to match cfg."""
    db_url = cfg.get_main_option("sqlalchemy.url")
    # env.py reads settings.database_url and overrides sqlalchemy.url,
    # so we must patch it to point at the test DB.
    async_url = db_url.replace("sqlite:///", "sqlite+aiosqlite:///")
    with patch("backend.config.settings.database_url", async_url):
        func(cfg, *args)


def _load_terminal_arbitration_migration(module_suffix: str = "test"):
    migration_path = (
        PROJECT_ROOT
        / "alembic"
        / "versions"
        / "4b8d2f6a1c90_add_terminal_arbitration_identity.py"
    )
    spec = importlib.util.spec_from_file_location(
        f"terminal_arbitration_migration_{module_suffix}",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _mysql_terminal_state(
    *,
    canonical: str | None,
    shadow: str | None = None,
    gate: bool = False,
    columns: set[str] | None = None,
    unique: bool = False,
) -> dict[str, object]:
    return {
        "columns": set(columns or ()),
        "column_shapes": {
            "request_reason": True,
            "request_protocol_version": True,
            "request_output_hash": True,
        },
        "unique": unique,
        "unique_present": unique,
        "canonical": canonical,
        "canonical_present": canonical is not None,
        "canonical_enforced": canonical is not None,
        "shadow": shadow,
        "shadow_present": shadow is not None,
        "shadow_enforced": shadow is not None,
        "gate": gate,
        "gate_present": gate,
        "gate_enforced": gate,
    }


def _mysql_auxiliary_state(
    *,
    task_source: bool = True,
    log_columns: bool = True,
    task_gate: bool = False,
    log_gate: bool = False,
) -> dict[str, object]:
    return {
        "task_source_present": task_source,
        "task_source_shape": True,
        "task_gate": task_gate,
        "task_gate_present": task_gate,
        "task_gate_enforced": task_gate,
        "turn_scope_present": log_columns,
        "turn_scope_shape": True,
        "actual_transport_present": log_columns,
        "actual_transport_shape": True,
        "scope_check": log_columns,
        "scope_check_present": log_columns,
        "scope_check_enforced": log_columns,
        "transport_check": log_columns,
        "transport_check_present": log_columns,
        "transport_check_enforced": log_columns,
        "log_gate": log_gate,
        "log_gate_present": log_gate,
        "log_gate_enforced": log_gate,
    }


def _get_table_columns(engine, table_name: str) -> dict[str, str]:
    """Return {column_name: column_type_str} for a table."""
    insp = inspect(engine)
    if table_name not in insp.get_table_names():
        return {}
    cols = insp.get_columns(table_name)
    return {c["name"]: str(c["type"]) for c in cols}


def _get_all_tables(engine) -> set[str]:
    """Return set of all user table names (excluding alembic_version)."""
    insp = inspect(engine)
    return {t for t in insp.get_table_names() if t != "alembic_version"}


def _create_legacy_db(db_path: str):
    """Create a legacy database matching the backup structure (no alembic_version,
    no loop-task columns). This mirrors claude_manager_backup_20260307_2.db."""
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100) NOT NULL,
                pid INTEGER,
                status VARCHAR(20),
                current_task_id INTEGER,
                worktree_path VARCHAR(500),
                worktree_branch VARCHAR(100),
                model VARCHAR(50),
                total_tasks_completed INTEGER,
                total_cost_usd FLOAT,
                config JSON,
                started_at DATETIME,
                last_heartbeat DATETIME
            )
        """))
        conn.execute(text("""
            CREATE TABLE projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100) NOT NULL UNIQUE,
                git_url VARCHAR(500),
                has_remote BOOLEAN,
                local_path VARCHAR(500),
                default_branch VARCHAR(100),
                status VARCHAR(20),
                error_message VARCHAR(1000),
                created_at DATETIME
            )
        """))
        conn.execute(text("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                status VARCHAR(20) NOT NULL,
                priority INTEGER NOT NULL,
                project_id INTEGER,
                target_repo VARCHAR(500),
                target_branch VARCHAR(100),
                result_branch VARCHAR(100),
                merge_status VARCHAR(20),
                instance_id INTEGER,
                retry_count INTEGER,
                max_retries INTEGER,
                mode VARCHAR(20),
                plan_content TEXT,
                plan_approved BOOLEAN,
                session_id VARCHAR(200),
                last_cwd VARCHAR(500),
                error_message TEXT,
                tags JSON,
                metadata JSON,
                created_at DATETIME,
                started_at DATETIME,
                completed_at DATETIME
            )
        """))
        conn.execute(text("CREATE INDEX ix_tasks_status ON tasks (status)"))
        conn.execute(text("CREATE INDEX ix_tasks_priority ON tasks (priority)"))
        conn.execute(text("CREATE INDEX ix_tasks_project_id ON tasks (project_id)"))
        conn.execute(text("""
            CREATE TABLE log_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                task_id INTEGER,
                event_type VARCHAR(50) NOT NULL,
                role VARCHAR(20),
                content TEXT,
                tool_name VARCHAR(100),
                tool_input TEXT,
                tool_output TEXT,
                raw_json TEXT,
                is_error BOOLEAN,
                timestamp DATETIME
            )
        """))
        conn.execute(text("CREATE INDEX ix_log_entries_instance_id ON log_entries (instance_id)"))
        conn.execute(text("CREATE INDEX ix_log_entries_task_id ON log_entries (task_id)"))
        conn.execute(text("CREATE INDEX ix_log_entries_event_type ON log_entries (event_type)"))
        conn.execute(text("""
            CREATE TABLE worktrees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_path VARCHAR(500) NOT NULL,
                worktree_path VARCHAR(500) NOT NULL UNIQUE,
                branch_name VARCHAR(100) NOT NULL,
                base_branch VARCHAR(100),
                instance_id INTEGER,
                status VARCHAR(20),
                created_at DATETIME,
                removed_at DATETIME
            )
        """))
        # Insert a sample row so we can verify data survives migration
        conn.execute(text(
            "INSERT INTO tasks (title, description, status, priority, mode, created_at) "
            "VALUES ('test task', 'test desc', 'pending', 0, 'auto', '2026-01-01 00:00:00')"
        ))
    engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLegacyMigration:
    """A legacy database (pre-Alembic) can be migrated to head."""

    def test_legacy_db_upgrades_successfully(self, tmp_path):
        """init_db logic: stamp initial, then upgrade to head."""
        db_path = str(tmp_path / "legacy.db")
        _create_legacy_db(db_path)

        cfg = _alembic_cfg(db_path)

        # Simulate init_db() logic for legacy DB:
        # stamp the initial revision, then upgrade to head
        _run_alembic(cfg, command.stamp, "6b3f8a1c2d9e")
        _run_alembic(cfg, command.upgrade, "head")

        # Verify alembic_version is at head
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            version = result.scalar()
            assert version == _get_head_revision(cfg), f"Expected head revision, got {version}"

        # Verify new columns exist
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        assert "loop_progress" in task_cols
        assert "max_iterations" in task_cols
        assert "context_window_usage" in task_cols
        assert "attention_tag" in task_cols
        assert "delivery_run_id" in task_cols
        assert "delivery_role" in task_cols
        assert "turn_generation" in task_cols
        assert "capability_policy" in task_cols

        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" in log_cols
        assert "task_retry_count" in log_cols
        assert "turn_scope" in log_cols
        assert "actual_transport" in log_cols
        assert "task_turn_generation" in log_cols
        assert "native_turn_id" in log_cols

        plan_step_cols = _get_table_columns(engine, "plan_agent_steps")
        assert "last_delta_at" in plan_step_cols
        assert "streamed_output_chars" in plan_step_cols
        assert "last_event_type" in plan_step_cols
        assert "capability_execution_id" in _get_table_columns(
            engine, "plan_agent_runs"
        )

        worktree_cols = _get_table_columns(engine, "worktrees")
        assert "delivery_run_id" in worktree_cols
        assert "cleanup_status" in worktree_cols
        assert "delivery_runs" in _get_all_tables(engine)

        project_cols = _get_table_columns(engine, "projects")
        assert "sort_order" in project_cols
        assert "tags" in project_cols

        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" in pr_review_cols
        assert "head_sha" in pr_review_cols
        assert "delivery_id" in pr_review_cols

        # Verify existing data survived
        with engine.connect() as conn:
            result = conn.execute(text("SELECT title FROM tasks WHERE id = 1"))
            assert result.scalar() == "test task"

        engine.dispose()

    def test_legacy_db_data_preserved(self, tmp_path):
        """Migration preserves all existing data including nullable new columns."""
        db_path = str(tmp_path / "legacy_data.db")
        _create_legacy_db(db_path)

        # Insert more data
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO log_entries (instance_id, task_id, event_type, content, timestamp) "
                "VALUES (1, 1, 'message', 'hello', '2026-01-01 00:00:00')"
            ))
        engine.dispose()

        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.stamp, "6b3f8a1c2d9e")
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            # New nullable columns default to NULL for existing rows
            row = conn.execute(text("SELECT todo_file_path, loop_progress FROM tasks WHERE id = 1")).fetchone()
            assert row[0] is None
            assert row[1] is None

            # max_iterations has server_default=50, so existing rows get 50
            row = conn.execute(text("SELECT max_iterations FROM tasks WHERE id = 1")).fetchone()
            assert row[0] == 50

            row = conn.execute(text("SELECT loop_iteration FROM log_entries WHERE id = 1")).fetchone()
            assert row[0] is None

        engine.dispose()


class TestLegacyDefaultAdminMigration:
    def test_known_seeded_account_is_disabled_and_password_rotated(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "legacy-admin.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d8f0a1b2c3d4")

        engine = create_engine(f"sqlite:///{db_path}")
        import bcrypt

        old_hash = bcrypt.hashpw(
            b"admin123456",
            bcrypt.gensalt(),
        ).decode()
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO users "
                    "(email, name, password_hash, role, avatar_url, "
                    "is_active, feishu_open_id, feishu_name, created_at) "
                    "VALUES "
                    "(:email, 'Admin', :password_hash, 'super_admin', "
                    "'', TRUE, '', '', CURRENT_TIMESTAMP)"
                ),
                {
                    "email": "admin@apexin.ai",
                    "password_hash": old_hash,
                },
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT password_hash, is_active FROM users "
                    "WHERE email = :email"
                ),
                {"email": "admin@apexin.ai"},
            ).one()
        engine.dispose()

        assert row.password_hash != old_hash
        assert bool(row.is_active) is False

    def test_changed_legacy_admin_password_is_preserved(self, tmp_path):
        import bcrypt

        db_path = str(tmp_path / "changed-legacy-admin.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "d8f0a1b2c3d4")

        changed_hash = bcrypt.hashpw(
            b"a-deployment-owned-password",
            bcrypt.gensalt(),
        ).decode()
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO users "
                    "(email, name, password_hash, role, avatar_url, "
                    "is_active, feishu_open_id, feishu_name, created_at) "
                    "VALUES "
                    "(:email, 'Admin', :password_hash, 'super_admin', "
                    "'', TRUE, '', '', CURRENT_TIMESTAMP)"
                ),
                {
                    "email": "admin@apexin.ai",
                    "password_hash": changed_hash,
                },
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT password_hash, is_active FROM users "
                    "WHERE email = :email"
                ),
                {"email": "admin@apexin.ai"},
            ).one()
        engine.dispose()

        assert row.password_hash == changed_hash
        assert bool(row.is_active) is True


class TestCodexServiceTierMigration:
    def test_existing_tasks_are_backfilled_as_standard(self, tmp_path):
        db_path = str(tmp_path / "codex-service-tier.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "e4c9f2a71b03")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES "
                "('existing task', 'd', 'pending', 0, 'main', 'pending', "
                "0, 2, 'auto', '2026-07-28 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            tier = conn.execute(text(
                "SELECT codex_service_tier FROM tasks "
                "WHERE title = 'existing task'"
            )).scalar_one()
            assert tier == "default"

            column = inspect(conn).get_columns("tasks")
            column = next(
                item for item in column
                if item["name"] == "codex_service_tier"
            )
            assert column["nullable"] is False
        engine.dispose()


class TestDeliveryLoopMigration:
    def test_upgrade_closes_task_id_reuse_and_purges_stale_acl(self, tmp_path):
        db_path = str(tmp_path / "delivery-task-id-aba.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, shared_from_id, "
                "created_at) "
                "VALUES (10, 'current task', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', 55, '2026-08-05 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO task_shares "
                "(task_id, shared_to_open_id, shared_to_ccm_url, share_token, "
                "status, created_at) VALUES "
                "(10, 'stale', 'https://old.example', 'stale-token', 'active', "
                "'2026-08-04 00:00:00'), "
                "(10, 'current', 'https://new.example', 'current-token', "
                "'active', '2026-08-06 00:00:00'), "
                "(25, 'orphan', 'https://old.example', 'orphan-token', "
                "'active', '2026-08-01 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO team_task_shares "
                "(task_id, target_type, target_id, permission, shared_by, "
                "created_at) VALUES "
                "(10, 'user', 1, 'chat', 1, '2026-08-04 00:00:00'), "
                "(30, 'user', 2, 'chat', 1, '2026-08-01 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO shared_tasks_received "
                "(owner_ccm_url, remote_task_id, share_token, local_task_id, "
                "status, received_at) VALUES "
                "('https://owner.example', 9, 'relay-token', 40, 'active', "
                "'2026-08-01 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            task_share_tokens = conn.execute(text(
                "SELECT share_token FROM task_shares ORDER BY share_token"
            )).scalars().all()
            assert task_share_tokens == ["current-token"]
            assert conn.execute(text(
                "SELECT COUNT(*) FROM team_task_shares"
            )).scalar_one() == 0
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            shared_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'shared_tasks_received'"
            )).scalar_one()
            assert "AUTOINCREMENT" in shared_ddl.upper()
            assert conn.execute(text(
                "SELECT incarnation_id FROM tasks WHERE id = 10"
            )).scalar_one() is None
            unique_names = {
                item["name"]
                for item in inspect(conn).get_unique_constraints("tasks")
            }
            assert "uq_tasks_incarnation_id" in unique_names

            conn.execute(text(
                "INSERT INTO shared_tasks_received "
                "(owner_ccm_url, remote_task_id, share_token, status, received_at) "
                "VALUES ('https://new-owner.example', 10, 'new-relay-token', "
                "'active', '2026-08-07 00:00:00')"
            ))
            new_shared_id = conn.execute(text(
                "SELECT id FROM shared_tasks_received "
                "WHERE share_token = 'new-relay-token'"
            )).scalar_one()
            assert new_shared_id > 55

            conn.execute(text("DELETE FROM tasks WHERE id = 10"))
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('new task', 'd', 'pending', 0, 'main', 'pending', "
                "0, 2, 'auto', '2026-08-07 00:00:00')"
            ))
            new_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'new task'"
            )).scalar_one()
            assert new_id > 40
        engine.dispose()

    def test_upgrade_from_code_review_head_preserves_existing_rows(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "delivery-existing.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('existing auto task', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-05 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO worktrees "
                "(repo_path, worktree_path, branch_name, base_branch, status, "
                "created_at) VALUES ('/repo', '/repo-wt', 'feature', 'main', "
                "'active', '2026-08-05 00:00:00')"
            ))
            conn.execute(text(
                "INSERT INTO plan_agent_runs "
                "(run_type, current_stage, generation, interaction_count, "
                "max_interactions, execution_seconds, status, round, "
                "review_exhausted, created_at, updated_at) VALUES "
                "('legacy', 'planner', 0, 0, 3, 0, 'completed', 1, 0, "
                "'2026-08-05 00:00:00', '2026-08-05 00:00:00')"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            task_owner = conn.execute(text(
                "SELECT delivery_run_id, delivery_role FROM tasks "
                "WHERE title = 'existing auto task'"
            )).one()
            assert task_owner == (None, None)

            worktree_owner = conn.execute(text(
                "SELECT task_id, delivery_run_id, last_verified_head, "
                "cleanup_status FROM worktrees WHERE worktree_path = '/repo-wt'"
            )).one()
            assert worktree_owner == (None, None, None, "retained")

            capability_execution_id = conn.execute(text(
                "SELECT capability_execution_id FROM plan_agent_runs"
            )).scalar_one()
            assert capability_execution_id is None

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE tasks SET delivery_run_id = 99 "
                    "WHERE title = 'existing auto task'"
                ))

        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE tasks SET mode = 'delivery_loop', delivery_run_id = 99, "
                "delivery_role = 'developer' "
                "WHERE title = 'existing auto task'"
            ))
        engine.dispose()

    def test_delivery_revision_downgrades_and_reupgrades(self, tmp_path):
        db_path = str(tmp_path / "delivery-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert not {
            "delivery_runs",
            "delivery_cycles",
            "delivery_turns",
            "delivery_events",
            "delivery_actions",
            "delivery_transitions",
        }.intersection(_get_all_tables(engine))
        assert "delivery_run_id" not in _get_table_columns(engine, "tasks")
        assert "delivery_run_id" not in _get_table_columns(engine, "worktrees")
        assert "capability_execution_id" not in _get_table_columns(
            engine, "plan_agent_runs"
        )
        assert "incarnation_id" not in _get_table_columns(engine, "tasks")
        with engine.begin() as conn:
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            shared_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'shared_tasks_received'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            assert "AUTOINCREMENT" in shared_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('downgrade highest', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-06 00:00:00')"
            ))
            old_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'downgrade highest'"
            )).scalar_one()
            conn.execute(text("DELETE FROM tasks WHERE id = :id"), {"id": old_id})
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('downgrade next', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-06 00:00:01')"
            ))
            new_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'downgrade next'"
            )).scalar_one()
            assert new_id > old_id
        engine.dispose()

        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "delivery_runs" in _get_all_tables(engine)
        assert "delivery_run_id" in _get_table_columns(engine, "tasks")
        assert "delivery_run_id" in _get_table_columns(engine, "worktrees")
        assert "capability_execution_id" in _get_table_columns(
            engine, "plan_agent_runs"
        )
        assert "incarnation_id" in _get_table_columns(engine, "tasks")
        engine.dispose()

    def test_delivery_revision_refuses_downgrade_with_run_history(self, tmp_path):
        db_path = str(tmp_path / "delivery-downgrade-history.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO delivery_runs "
                "(admission_scope, idempotency_key, request_hash, project_id, "
                "title, requirements, requirements_hash, "
                "policy_snapshot, policy_hash, base_branch, delivery_branch, "
                "created_at, updated_at) VALUES "
                "('system', 'downgrade-test', :digest, 1, "
                "'retained delivery', 'requirements', :digest, '{}', "
                ":digest, 'main', 'ccm/delivery/1-retained', "
                "'2026-08-05 00:00:00', '2026-08-05 00:00:00')"
            ), {"digest": "a" * 64})
        engine.dispose()

        with pytest.raises(RuntimeError, match="delivery_runs contains history"):
            _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert "delivery_runs" in _get_all_tables(engine)
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT COUNT(*) FROM delivery_runs"
            )).scalar_one() == 1
            assert conn.execute(text(
                "SELECT version_num FROM alembic_version"
            )).scalar_one() == DELIVERY_LOOP_REVISION
        engine.dispose()

    def test_delivery_revision_refuses_residual_owner_downgrade(self, tmp_path):
        db_path = str(tmp_path / "delivery-downgrade-owners.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")

        residues = (
            (
                "tasks",
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, "
                "delivery_run_id, delivery_role, created_at) VALUES "
                "('orphan delivery', 'd', 'cancelled', 0, 'main', "
                "'pending', 0, 2, 'delivery_loop', 91, 'developer', "
                "'2026-08-05 00:00:00')",
                "DELETE FROM tasks WHERE title = 'orphan delivery'",
            ),
            (
                "worktrees",
                "INSERT INTO worktrees "
                "(repo_path, worktree_path, branch_name, base_branch, "
                "delivery_run_id, cleanup_status, status, created_at) VALUES "
                "('/repo', '/repo/delivery-92', 'delivery-92', 'main', 92, "
                "'retained', 'active', '2026-08-05 00:00:00')",
                "DELETE FROM worktrees WHERE delivery_run_id = 92",
            ),
            (
                "plan_agent_runs",
                "INSERT INTO plan_agent_runs "
                "(run_type, current_stage, generation, interaction_count, "
                "max_interactions, execution_seconds, status, round, "
                "review_exhausted, capability_execution_id, created_at, "
                "updated_at) VALUES ('capability', 'planner', 0, 0, 3, 0, "
                "'cancelled', 1, 0, 93, '2026-08-05 00:00:00', "
                "'2026-08-05 00:00:00')",
                "DELETE FROM plan_agent_runs WHERE capability_execution_id = 93",
            ),
        )
        for table_name, insert_sql, delete_sql in residues:
            with engine.begin() as conn:
                conn.execute(text(insert_sql))
            with pytest.raises(RuntimeError, match=table_name):
                _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)
            with engine.begin() as conn:
                conn.execute(text(delete_sql))

        engine.dispose()
        _run_alembic(cfg, command.downgrade, CODE_REVIEW_REVISION)


class TestAutoCapabilityTurnMigration:
    def test_upgrade_downgrade_preserves_rows_and_task_identity(self, tmp_path):
        db_path = str(tmp_path / "auto-capability-turn.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)

        digest = "a" * 64
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('existing exact turn', 'd', 'pending', 0, 'main', "
                "'pending', 0, 2, 'auto', '2026-08-06 00:00:00')"
            ))
            task_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'existing exact turn'"
            )).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO log_entries "
                    "(task_id, event_type, content, is_error, timestamp) "
                    "VALUES (:task_id, 'result', 'existing output', 0, "
                    "'2026-08-06 00:00:01')"
                ),
                {"task_id": task_id},
            )
            log_id = conn.execute(text(
                "SELECT id FROM log_entries WHERE task_id = :task_id"
            ), {"task_id": task_id}).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO capability_invocations "
                    "(task_id, capability_key, source, purpose, status, "
                    "state_version, idempotency_key, input_payload, input_hash, "
                    "subject_kind, subject_ref, subject_hash, executor_kind, "
                    "executor_config, executor_config_hash, policy_snapshot, "
                    "policy_hash, resume_policy, max_attempts, active_task_id, "
                    "created_at, updated_at) VALUES "
                    "(:task_id, 'plan', 'human_request', 'advisory', 'failed', "
                    "1, 'existing-exact-turn', '{}', :digest, "
                    "'task_generation', '{}', :digest, 'plan_agent', '{}', "
                    ":digest, '{}', :digest, 'attach_only', 1, NULL, "
                    "'2026-08-06 00:00:02', '2026-08-06 00:00:02')"
                ),
                {"task_id": task_id, "digest": digest},
            )
            invocation_id = conn.execute(text(
                "SELECT id FROM capability_invocations "
                "WHERE idempotency_key = 'existing-exact-turn'"
            )).scalar_one()
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES (80, 'deleted high-water task', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', '2026-08-06 00:00:03')"
            ))
            conn.execute(text("DELETE FROM tasks WHERE id = 80"))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        columns = {
            table: {
                column["name"]: column
                for column in inspect(engine).get_columns(table)
            }
            for table in ("tasks", "log_entries", "capability_invocations")
        }
        assert "BIGINT" in str(columns["tasks"]["turn_generation"]["type"]).upper()
        assert columns["tasks"]["turn_generation"]["nullable"] is False
        assert columns["tasks"]["turn_generation"]["default"] is not None
        assert columns["tasks"]["capability_policy"]["nullable"] is True
        assert "BIGINT" in str(
            columns["log_entries"]["task_turn_generation"]["type"]
        ).upper()
        assert columns["log_entries"]["native_turn_id"]["type"].length == 200
        assert columns["capability_invocations"]["request_output_log_id"][
            "nullable"
        ] is True
        assert columns["capability_invocations"]["request_native_turn_id"][
            "type"
        ].length == 200
        capability_checks = {
            item["name"]
            for item in inspect(engine).get_check_constraints(
                "capability_invocations"
            )
        }
        assert "ck_cap_inv_agent_request_identity" in capability_checks
        handoff_columns = _get_table_columns(
            engine,
            "worker_turn_handoff_receipts",
        )
        assert {
            "handoff_id",
            "task_id",
            "source_log_id",
            "side",
            "worker_id",
            "retry_count",
            "from_generation",
            "status",
            "request_payload",
            "request_digest",
            "queue_payload",
            "queue_payload_digest",
            "response",
            "claimed_turn_generation",
            "terminal_pr_review_chat",
            "cancel_reason",
            "created_at",
            "updated_at",
        } == set(handoff_columns)
        handoff_checks = {
            item["name"]: item["sqltext"]
            for item in inspect(engine).get_check_constraints(
                "worker_turn_handoff_receipts"
            )
        }
        assert "CLAIMED_TURN_GENERATION IS NOT NULL" in handoff_checks[
            "ck_worker_turn_handoff_claim"
        ].upper()
        with engine.begin() as conn:
            assert conn.execute(
                text(
                    "SELECT turn_generation, capability_policy FROM tasks "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            ).one() == (0, None)
            assert conn.execute(
                text(
                    "SELECT task_turn_generation, native_turn_id "
                    "FROM log_entries WHERE id = :log_id"
                ),
                {"log_id": log_id},
            ).one() == (None, None)
            assert conn.execute(
                text(
                    "SELECT request_output_log_id, request_native_turn_id "
                    "FROM capability_invocations WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            ).one() == (None, None)
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('post-upgrade sequence task', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', '2026-08-06 00:00:04')"
            ))
            post_upgrade_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'post-upgrade sequence task'"
            )).scalar_one()
            assert post_upgrade_id > 80
            conn.execute(text(
                "DELETE FROM tasks WHERE id = :task_id"
            ), {"task_id": post_upgrade_id})
        handoff_insert = text(
            "INSERT INTO worker_turn_handoff_receipts "
            "(handoff_id, task_id, source_log_id, side, worker_id, "
            "retry_count, from_generation, status, request_payload, "
            "request_digest, queue_payload, queue_payload_digest, response, "
            "claimed_turn_generation, terminal_pr_review_chat, created_at, "
            "updated_at) VALUES (:handoff_id, :task_id, :log_id, 'worker', "
            "NULL, 0, 4, :status, '{}', :digest, '{}', :digest, '{}', "
            ":claimed_turn_generation, 0, '2026-08-06 00:00:05', "
            "'2026-08-06 00:00:05')"
        )
        for handoff_id, status, claimed_generation in (
            ("1" * 32, "claimed", None),
            ("2" * 32, "accepted", 5),
        ):
            with pytest.raises(IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        handoff_insert,
                        {
                            "handoff_id": handoff_id,
                            "task_id": task_id,
                            "log_id": log_id,
                            "status": status,
                            "digest": digest,
                            "claimed_turn_generation": claimed_generation,
                        },
                    )
        with engine.begin() as conn:
            conn.execute(
                handoff_insert,
                {
                    "handoff_id": "3" * 32,
                    "task_id": task_id,
                    "log_id": log_id,
                    "status": "launching",
                    "digest": digest,
                    "claimed_turn_generation": 5,
                },
            )
            conn.execute(text(
                "DELETE FROM worker_turn_handoff_receipts "
                "WHERE handoff_id = :handoff_id"
            ), {"handoff_id": "3" * 32})
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE capability_invocations "
                        "SET source = 'agent_request', "
                        "resume_policy = 'resume_task' "
                        "WHERE id = :invocation_id"
                    ),
                    {"invocation_id": invocation_id},
                )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_generation" not in _get_table_columns(engine, "tasks")
        assert "capability_policy" not in _get_table_columns(engine, "tasks")
        assert "task_turn_generation" not in _get_table_columns(
            engine, "log_entries"
        )
        assert "request_output_log_id" not in _get_table_columns(
            engine, "capability_invocations"
        )
        assert "worker_turn_handoff_receipts" not in _get_all_tables(engine)
        with engine.begin() as conn:
            assert conn.execute(text(
                "SELECT COUNT(*) FROM tasks WHERE id = :task_id"
            ), {"task_id": task_id}).scalar_one() == 1
            assert conn.execute(text(
                "SELECT COUNT(*) FROM log_entries WHERE id = :log_id"
            ), {"log_id": log_id}).scalar_one() == 1
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('post-downgrade sequence task', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', '2026-08-06 00:00:05')"
            ))
            post_downgrade_id = conn.execute(text(
                "SELECT id FROM tasks "
                "WHERE title = 'post-downgrade sequence task'"
            )).scalar_one()
            assert post_downgrade_id > post_upgrade_id
        engine.dispose()

        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_generation" in _get_table_columns(engine, "tasks")
        assert "request_native_turn_id" in _get_table_columns(
            engine, "capability_invocations"
        )
        engine.dispose()


class TestTerminalArbitrationMigration:
    def test_worker_termination_table_partial_index_replay(self, tmp_path):
        db_path = str(tmp_path / "termination-receipt-index-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        module = _load_terminal_arbitration_migration(
            "termination_receipt_index_replay"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            context = MigrationContext.configure(connection=connection)
            with patch.object(module, "op", Operations(context)):
                module._create_worker_task_termination_table()
            connection.execute(
                text("DROP INDEX ix_worker_task_term_due")
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        indexes = {
            item["name"]: tuple(item["column_names"])
            for item in inspect(engine).get_indexes(
                "worker_task_termination_receipts"
            )
        }
        assert indexes == module._WORKER_TASK_TERMINATION_INDEXES
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
        engine.dispose()

    def test_worker_termination_table_malformed_replay_fails_before_ddl(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "termination-receipt-malformed-replay.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE worker_task_termination_receipts ("
                "operation_id VARCHAR(32) PRIMARY KEY)"
            ))
        engine.dispose()

        with pytest.raises(RuntimeError, match="partial or foreign column set"):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
        engine.dispose()

    @pytest.mark.parametrize("malformation", ("named_unique", "foreign_unique"))
    def test_worker_termination_table_unique_index_replay_fails_closed(
        self,
        tmp_path,
        malformation,
    ):
        db_path = str(tmp_path / f"termination-receipt-{malformation}.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        module = _load_terminal_arbitration_migration(
            f"termination_receipt_{malformation}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            context = MigrationContext.configure(connection=connection)
            with patch.object(module, "op", Operations(context)):
                module._create_worker_task_termination_table()
            if malformation == "named_unique":
                connection.execute(text(
                    "DROP INDEX ix_worker_task_term_due"
                ))
                connection.execute(text(
                    "CREATE UNIQUE INDEX ix_worker_task_term_due ON "
                    "worker_task_termination_receipts"
                    "(side, status, next_reconcile_at)"
                ))
            else:
                connection.execute(text(
                    "CREATE UNIQUE INDEX uq_worker_task_term_foreign ON "
                    "worker_task_termination_receipts(task_id, status)"
                ))
        engine.dispose()

        expected = (
            "index ix_worker_task_term_due is malformed"
            if malformation == "named_unique"
            else "foreign UNIQUE index"
        )
        with pytest.raises(RuntimeError, match=expected):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
        engine.dispose()

    def test_downgrade_refuses_worker_termination_receipt_history(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "termination-receipt-downgrade-fence.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('termination downgrade fence', 'd', 'pending', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 00:00:00')"
            ))
            task_id = connection.execute(text(
                "SELECT id FROM tasks "
                "WHERE title = 'termination downgrade fence'"
            )).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO worker_task_termination_receipts "
                    "(operation_id, task_id, active_task_id, side, worker_id, "
                    "operation, status, source_task_status, "
                    "source_task_retry_count, source_task_turn_generation, "
                    "request_payload, request_digest, created_at, updated_at) "
                    "VALUES (:operation_id, :task_id, :task_id, 'manager', 4, "
                    "'cancel', 'pending_remote', 'pending', 0, 0, '{}', "
                    ":digest, '2026-08-06 00:00:01', "
                    "'2026-08-06 00:00:01')"
                ),
                {
                    "operation_id": "f" * 32,
                    "task_id": task_id,
                    "digest": "f" * 64,
                },
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="receipt history"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_task_termination_receipts" in _get_all_tables(engine)
        with engine.begin() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
            connection.execute(
                text("DELETE FROM worker_task_termination_receipts")
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "worker_task_termination_receipts" not in _get_all_tables(engine)
        engine.dispose()

    def test_upgrade_downgrade_preserves_rows_constraints_and_task_ids(
        self,
        tmp_path,
    ):
        db_path = str(tmp_path / "terminal-arbitration.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)

        digest = "d" * 64
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal arbitration', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:00')"
            ))
            task_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal arbitration'"
            )).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO log_entries "
                    "(task_id, task_retry_count, task_turn_generation, "
                    "event_type, role, content, is_error, timestamp) VALUES "
                    "(:task_id, 0, 7, 'user_message', 'user', 'source', 0, "
                    "'2026-08-06 01:00:01'), "
                    "(:task_id, 0, 7, 'result', 'assistant', 'output', 0, "
                    "'2026-08-06 01:00:02')"
                ),
                {"task_id": task_id},
            )
            log_ids = conn.execute(text(
                "SELECT id FROM log_entries WHERE task_id = :task_id "
                "ORDER BY id"
            ), {"task_id": task_id}).scalars().all()
            source_log_id, output_log_id = log_ids
            conn.execute(
                text(
                    "INSERT INTO capability_invocations "
                    "(task_id, capability_key, source, purpose, status, "
                    "state_version, idempotency_key, input_payload, input_hash, "
                    "subject_kind, subject_ref, subject_hash, executor_kind, "
                    "executor_config, executor_config_hash, policy_snapshot, "
                    "policy_hash, resume_policy, max_attempts, active_task_id, "
                    "request_task_retry_count, request_task_turn_generation, "
                    "request_source_log_id, request_output_log_id, created_at, "
                    "updated_at) VALUES "
                    "(:task_id, 'plan', 'human_request', 'advisory', 'failed', "
                    "1, 'terminal-arbitration-existing', '{}', :digest, "
                    "'task_generation', '{}', :digest, 'plan_agent', '{}', "
                    ":digest, '{}', :digest, 'attach_only', 1, NULL, 0, 7, "
                    ":source_log_id, :output_log_id, "
                    "'2026-08-06 01:00:03', '2026-08-06 01:00:03')"
                ),
                {
                    "task_id": task_id,
                    "digest": digest,
                    "source_log_id": source_log_id,
                    "output_log_id": output_log_id,
                },
            )
            invocation_id = conn.execute(text(
                "SELECT id FROM capability_invocations WHERE "
                "idempotency_key = 'terminal-arbitration-existing'"
            )).scalar_one()
            conn.execute(text(
                "INSERT INTO tasks "
                "(id, title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES (180, 'terminal deleted high-water', 'd', 'completed', "
                "0, 'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:04')"
            ))
            conn.execute(text("DELETE FROM tasks WHERE id = 180"))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        task_columns = {
            item["name"]: item for item in inspector.get_columns("tasks")
        }
        log_columns = {
            item["name"]: item for item in inspector.get_columns("log_entries")
        }
        invocation_columns = {
            item["name"]: item
            for item in inspector.get_columns("capability_invocations")
        }
        assert task_columns["turn_source_log_id"]["nullable"] is True
        assert log_columns["turn_scope"]["nullable"] is True
        assert log_columns["turn_scope"]["type"].length == 16
        assert log_columns["actual_transport"]["nullable"] is True
        assert log_columns["actual_transport"]["type"].length == 24
        for column_name in (
            "request_reason",
            "request_protocol_version",
            "request_output_hash",
        ):
            assert invocation_columns[column_name]["nullable"] is True
        assert invocation_columns["request_output_hash"]["type"].length == 64

        log_checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints("log_entries")
        }
        assert "ck_log_entries_turn_scope" in log_checks
        assert "AUTONOMOUS" in log_checks["ck_log_entries_turn_scope"].upper()
        assert "ck_log_entries_actual_transport" in log_checks
        actual_transport_check = log_checks[
            "ck_log_entries_actual_transport"
        ].upper()
        assert "TURN_SCOPE IS NOT NULL" in actual_transport_check
        assert "TURN_SCOPE = 'SOURCE'" in actual_transport_check
        assert "CODEX_APP_SERVER" in actual_transport_check
        invocation_checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints(
                "capability_invocations"
            )
        }
        identity_check = invocation_checks[
            "ck_cap_inv_agent_request_identity"
        ].upper()
        assert "REQUEST_REASON IS NOT NULL" in identity_check
        assert "REQUEST_PROTOCOL_VERSION >= 1" in identity_check
        assert "REQUEST_OUTPUT_HASH IS NOT NULL" in identity_check
        invocation_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(
                "capability_invocations"
            )
        }
        assert invocation_uniques["uq_cap_inv_task_output_log"] == (
            "task_id",
            "request_output_log_id",
        )
        assert "worker_task_termination_receipts" in inspector.get_table_names()
        termination_columns = {
            item["name"]: item
            for item in inspector.get_columns(
                "worker_task_termination_receipts"
            )
        }
        assert termination_columns["operation_id"]["type"].length == 32
        assert termination_columns["source_task_incarnation_id"][
            "nullable"
        ] is True
        assert termination_columns["source_task_turn_generation"][
            "nullable"
        ] is False
        assert termination_columns["reconcile_count"]["nullable"] is False
        assert termination_columns["ack_intent_at"]["nullable"] is True
        termination_checks = {
            item["name"]
            for item in inspector.get_check_constraints(
                "worker_task_termination_receipts"
            )
        }
        assert termination_checks == set(
            _load_terminal_arbitration_migration(
                "roundtrip_checks"
            )._WORKER_TASK_TERMINATION_CHECKS
        )
        termination_indexes = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_indexes(
                "worker_task_termination_receipts"
            )
        }
        assert termination_indexes == {
            "ix_worker_task_term_task_created": ("task_id", "created_at"),
            "ix_worker_task_term_due": (
                "side",
                "status",
                "next_reconcile_at",
            ),
            "ix_worker_task_term_worker_status": ("worker_id", "status"),
        }

        with engine.begin() as conn:
            assert conn.execute(
                text(
                    "SELECT turn_source_log_id FROM tasks WHERE id = :task_id"
                ),
                {"task_id": task_id},
            ).scalar_one() is None
            assert conn.execute(
                text(
                    "SELECT turn_scope, actual_transport FROM log_entries "
                    "WHERE task_id = :task_id "
                    "ORDER BY id"
                ),
                {"task_id": task_id},
            ).all() == [(None, None), (None, None)]
            assert conn.execute(
                text(
                    "SELECT request_reason, request_protocol_version, "
                    "request_output_hash FROM capability_invocations "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            ).one() == (None, None, None)
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = :source_log_id "
                    "WHERE id = :task_id"
                ),
                {"source_log_id": source_log_id, "task_id": task_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = CASE id "
                    "WHEN :source_log_id THEN 'source' ELSE 'foreground' END, "
                    "actual_transport = CASE id WHEN :source_log_id "
                    "THEN 'codex_exec' ELSE NULL END "
                    "WHERE task_id = :task_id"
                ),
                {
                    "source_log_id": source_log_id,
                    "task_id": task_id,
                },
            )
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal post-upgrade', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:05')"
            ))
            post_upgrade_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal post-upgrade'"
            )).scalar_one()
            assert post_upgrade_id > 180
            conn.execute(
                text("DELETE FROM tasks WHERE id = :task_id"),
                {"task_id": post_upgrade_id},
            )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE log_entries SET turn_scope = 'background' "
                        "WHERE id = :output_log_id"
                    ),
                    {"output_log_id": output_log_id},
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE log_entries SET turn_scope = NULL, "
                        "actual_transport = 'codex_exec' "
                        "WHERE id = :output_log_id"
                    ),
                    {"output_log_id": output_log_id},
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE log_entries SET actual_transport = 'claude_exec' "
                        "WHERE id = :output_log_id"
                    ),
                    {"output_log_id": output_log_id},
                )

        agent_identity_update = (
            "UPDATE capability_invocations SET source = 'agent_request', "
            "resume_policy = 'resume_task', request_reason = :reason, "
            "request_protocol_version = :protocol_version, "
            "request_output_hash = :output_hash WHERE id = :invocation_id"
        )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(agent_identity_update),
                    {
                        "reason": None,
                        "protocol_version": None,
                        "output_hash": None,
                        "invocation_id": invocation_id,
                    },
                )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(agent_identity_update),
                    {
                        "reason": "Need terminal Plan",
                        "protocol_version": 0,
                        "output_hash": digest,
                        "invocation_id": invocation_id,
                    },
                )
        with engine.begin() as conn:
            conn.execute(
                text(agent_identity_update),
                {
                    "reason": "Need terminal Plan",
                    "protocol_version": 1,
                    "output_hash": digest,
                    "invocation_id": invocation_id,
                },
            )

        duplicate_insert = text(
            "INSERT INTO capability_invocations "
            "(task_id, capability_key, source, purpose, status, state_version, "
            "idempotency_key, input_payload, input_hash, subject_kind, "
            "subject_ref, subject_hash, executor_kind, executor_config, "
            "executor_config_hash, policy_snapshot, policy_hash, resume_policy, "
            "max_attempts, active_task_id, request_output_log_id, created_at, "
            "updated_at) VALUES (:task_id, 'code_review', 'human_request', "
            "'advisory', 'failed', 1, :idempotency_key, '{}', :digest, "
            "'task_generation', '{}', :digest, 'code_review', '{}', :digest, "
            "'{}', :digest, 'attach_only', 1, NULL, :output_log_id, "
            "'2026-08-06 01:00:06', '2026-08-06 01:00:06')"
        )
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    duplicate_insert,
                    {
                        "task_id": task_id,
                        "idempotency_key": "terminal-output-duplicate",
                        "digest": digest,
                        "output_log_id": output_log_id,
                    },
                )
        # The downgrade deliberately refuses to discard protocol audit fields
        # from a real Agent request.  Return this round-trip fixture to the
        # human-request shape; a dedicated guard test below covers refusal.
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE capability_invocations SET "
                    "source = 'human_request', resume_policy = 'attach_only' "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            )
            # A downgrade must never erase terminal-arbitration provenance.
            # This fixture already exercised the fields above; clear them
            # explicitly so the schema round-trip itself remains admissible.
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = NULL "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = NULL, "
                    "actual_transport = NULL WHERE task_id = :task_id"
                ),
                {"task_id": task_id},
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        assert "turn_scope" not in _get_table_columns(engine, "log_entries")
        assert "actual_transport" not in _get_table_columns(engine, "log_entries")
        assert (
            "worker_task_termination_receipts"
            not in _get_all_tables(engine)
        )
        downgraded_invocation_columns = _get_table_columns(
            engine,
            "capability_invocations",
        )
        assert "request_reason" not in downgraded_invocation_columns
        assert "request_protocol_version" not in downgraded_invocation_columns
        assert "request_output_hash" not in downgraded_invocation_columns
        downgraded_uniques = {
            item["name"]
            for item in inspect(engine).get_unique_constraints(
                "capability_invocations"
            )
        }
        assert "uq_cap_inv_task_output_log" not in downgraded_uniques
        downgraded_checks = {
            item["name"]: item["sqltext"].upper()
            for item in inspect(engine).get_check_constraints(
                "capability_invocations"
            )
        }
        downgraded_identity = downgraded_checks[
            "ck_cap_inv_agent_request_identity"
        ]
        assert "REQUEST_OUTPUT_LOG_ID IS NOT NULL" in downgraded_identity
        assert "REQUEST_REASON" not in downgraded_identity
        assert "REQUEST_PROTOCOL_VERSION" not in downgraded_identity
        assert "REQUEST_OUTPUT_HASH" not in downgraded_identity
        with engine.begin() as conn:
            assert conn.execute(
                text(
                    "SELECT source FROM capability_invocations "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            ).scalar_one() == "human_request"
            task_ddl = conn.execute(text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'tasks'"
            )).scalar_one()
            assert "AUTOINCREMENT" in task_ddl.upper()
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal post-downgrade', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 01:00:07')"
            ))
            post_downgrade_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal post-downgrade'"
            )).scalar_one()
            assert post_downgrade_id > post_upgrade_id
            conn.execute(
                duplicate_insert,
                {
                    "task_id": task_id,
                    "idempotency_key": "terminal-output-duplicate",
                    "digest": digest,
                    "output_log_id": output_log_id,
                },
            )
            # The old identity CHECK still accepts its complete legacy shape.
            conn.execute(
                text(
                    "UPDATE capability_invocations SET "
                    "source = 'agent_request', resume_policy = 'resume_task' "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE capability_invocations "
                        "SET request_output_log_id = NULL "
                        "WHERE id = :invocation_id"
                    ),
                    {"invocation_id": invocation_id},
                )
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM capability_invocations WHERE "
                "idempotency_key = 'terminal-output-duplicate'"
            ))
            conn.execute(
                text(
                    "UPDATE capability_invocations SET "
                    "source = 'human_request', resume_policy = 'attach_only' "
                    "WHERE id = :invocation_id"
                ),
                {"invocation_id": invocation_id},
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" in _get_table_columns(engine, "tasks")
        assert "turn_scope" in _get_table_columns(engine, "log_entries")
        assert "request_output_hash" in _get_table_columns(
            engine,
            "capability_invocations",
        )
        assert "worker_task_termination_receipts" in _get_all_tables(engine)
        engine.dispose()

    def test_preflight_refuses_unsafe_data_before_schema_changes(self, tmp_path):
        db_path = str(tmp_path / "terminal-arbitration-preflight.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)

        digest = "e" * 64
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, created_at) "
                "VALUES ('terminal preflight', 'd', 'completed', 0, "
                "'main', 'pending', 0, 2, 'auto', "
                "'2026-08-06 02:00:00')"
            ))
            task_id = conn.execute(text(
                "SELECT id FROM tasks WHERE title = 'terminal preflight'"
            )).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO log_entries "
                    "(task_id, task_retry_count, task_turn_generation, "
                    "event_type, role, content, is_error, timestamp) VALUES "
                    "(:task_id, 0, 1, 'user_message', 'user', 'source', 0, "
                    "'2026-08-06 02:00:01'), "
                    "(:task_id, 0, 1, 'result', 'assistant', 'output', 0, "
                    "'2026-08-06 02:00:02')"
                ),
                {"task_id": task_id},
            )
            source_log_id, output_log_id = conn.execute(
                text(
                    "SELECT id FROM log_entries WHERE task_id = :task_id "
                    "ORDER BY id"
                ),
                {"task_id": task_id},
            ).scalars().all()
            invocation_insert = text(
                "INSERT INTO capability_invocations "
                "(task_id, capability_key, source, purpose, status, "
                "state_version, idempotency_key, input_payload, input_hash, "
                "subject_kind, subject_ref, subject_hash, executor_kind, "
                "executor_config, executor_config_hash, policy_snapshot, "
                "policy_hash, resume_policy, max_attempts, active_task_id, "
                "request_task_retry_count, request_task_turn_generation, "
                "request_source_log_id, request_output_log_id, created_at, "
                "updated_at) VALUES "
                "(:task_id, 'plan', :source, 'advisory', 'failed', 1, "
                ":idempotency_key, '{}', :digest, 'task_generation', '{}', "
                ":digest, 'plan_agent', '{}', :digest, '{}', :digest, "
                ":resume_policy, 1, NULL, 0, 1, :source_log_id, "
                ":output_log_id, '2026-08-06 02:00:03', "
                "'2026-08-06 02:00:03')"
            )
            conn.execute(
                invocation_insert,
                {
                    "task_id": task_id,
                    "source": "agent_request",
                    "idempotency_key": "terminal-preflight-agent",
                    "digest": digest,
                    "resume_policy": "resume_task",
                    "source_log_id": source_log_id,
                    "output_log_id": output_log_id,
                },
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="zero legacy agent_request"):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        assert "turn_scope" not in _get_table_columns(engine, "log_entries")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
            conn.execute(text(
                "UPDATE capability_invocations SET source = 'human_request', "
                "resume_policy = 'attach_only' WHERE "
                "idempotency_key = 'terminal-preflight-agent'"
            ))
            conn.execute(
                invocation_insert,
                {
                    "task_id": task_id,
                    "source": "human_request",
                    "idempotency_key": "terminal-preflight-duplicate",
                    "digest": digest,
                    "resume_policy": "attach_only",
                    "source_log_id": source_log_id,
                    "output_log_id": output_log_id,
                },
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="duplicate task/output-log"):
            _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == AUTO_CAPABILITY_TURN_REVISION
            conn.execute(text(
                "DELETE FROM capability_invocations WHERE "
                "idempotency_key = 'terminal-preflight-duplicate'"
            ))
        engine.dispose()

        _run_alembic(cfg, command.upgrade, TERMINAL_ARBITRATION_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE capability_invocations SET source = 'agent_request', "
                    "resume_policy = 'resume_task', request_reason = :reason, "
                    "request_protocol_version = 1, request_output_hash = :digest "
                    "WHERE idempotency_key = 'terminal-preflight-agent'"
                ),
                {"reason": "Need a durable Plan", "digest": digest},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="audit history would be destroyed"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" in _get_table_columns(engine, "tasks")
        assert "turn_scope" in _get_table_columns(engine, "log_entries")
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
        engine.dispose()

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE capability_invocations SET source = 'human_request', "
                "resume_policy = 'attach_only' WHERE "
                "idempotency_key = 'terminal-preflight-agent'"
            ))
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = :source_log_id "
                    "WHERE id = :task_id"
                ),
                {"source_log_id": source_log_id, "task_id": task_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="Task turn provenance"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
            conn.execute(
                text(
                    "UPDATE tasks SET turn_source_log_id = NULL "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = 'foreground' "
                    "WHERE id = :output_log_id"
                ),
                {"output_log_id": output_log_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="LogEntry turn provenance"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = NULL "
                    "WHERE id = :output_log_id"
                ),
                {"output_log_id": output_log_id},
            )
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = 'source', "
                    "actual_transport = 'codex_exec' "
                    "WHERE id = :source_log_id"
                ),
                {"source_log_id": source_log_id},
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="LogEntry turn provenance"):
            _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == TERMINAL_ARBITRATION_REVISION
            conn.execute(
                text(
                    "UPDATE log_entries SET turn_scope = NULL, "
                    "actual_transport = NULL WHERE id = :source_log_id"
                ),
                {"source_log_id": source_log_id},
            )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, AUTO_CAPABILITY_TURN_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "turn_source_log_id" not in _get_table_columns(engine, "tasks")
        assert "turn_scope" not in _get_table_columns(engine, "log_entries")
        engine.dispose()

    def test_sqlite_preflight_fence_blocks_a_second_writer(self, tmp_path):
        db_path = str(tmp_path / "terminal-preflight-fence.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, AUTO_CAPABILITY_TURN_REVISION)
        module = _load_terminal_arbitration_migration("sqlite_fence")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"timeout": 0},
        )
        first = engine.connect()
        transaction = first.begin()
        context = MigrationContext.configure(connection=first)
        try:
            with patch.object(module, "op", Operations(context)):
                module._acquire_preflight_fence(
                    expected_revision=AUTO_CAPABILITY_TURN_REVISION,
                )
                module._assert_upgrade_preconditions()
                with engine.connect() as second:
                    second.exec_driver_sql("PRAGMA busy_timeout = 0")
                    with pytest.raises(OperationalError, match="locked"):
                        second.execute(
                            text(
                                "UPDATE alembic_version "
                                "SET version_num = version_num"
                            )
                        )
        finally:
            transaction.rollback()
            first.close()

        # Prove the second statement was blocked by the held writer transaction,
        # not because the statement or database was otherwise invalid.
        with engine.begin() as connection:
            updated = connection.execute(
                text("UPDATE alembic_version SET version_num = version_num")
            )
            assert updated.rowcount == 1
        engine.dispose()

    def test_mysql_reflected_identity_checks_require_exact_boolean_shape(self):
        module = _load_terminal_arbitration_migration("mysql_check_shape")
        old = (
            "((`source` <> _utf8mb4'agent_request') or "
            "((`purpose` = _utf8mb4'advisory') and "
            "(`resume_policy` = _utf8mb4'resume_task') and "
            "(`requested_by_user_id` is null) and "
            "(`request_task_retry_count` is not null) and "
            "(`request_task_turn_generation` is not null) and "
            "(`request_source_log_id` is not null) and "
            "(`request_output_log_id` is not null)))"
        )
        new = old[:-2] + (
            " and (`request_reason` is not null)"
            " and (`request_protocol_version` is not null)"
            " and (`request_protocol_version` >= 1)"
            " and (`request_output_hash` is not null)))"
        )
        gate = "(`source` <> _latin1'agent_request')"

        assert module._identity_check_kind(old) == "old"
        assert module._identity_check_kind(new) == "new"
        assert module._is_mysql_downgrade_gate(gate)
        assert module._identity_check_kind(new.replace(">= 1", ">= 0")) is None
        assert module._identity_check_kind(f"({new}) or (1 = 1)") is None
        regrouped = new.replace(
            ") or ((`purpose`",
            ") or (`purpose`",
        ).replace("is not null)))", "is not null)) and (1 = 1)")
        assert module._identity_check_kind(regrouped) is None

    @pytest.mark.parametrize("failure_call", (0, 1, 2))
    def test_mysql_upgrade_phase_failure_keeps_a_guard_and_replays(
        self,
        failure_call,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_upgrade_failure_{failure_call}"
        )
        model = {
            "canonical": "old",
            "shadow": None,
            "gate": False,
            "columns": set(),
            "unique": False,
        }
        control = {"failure": failure_call, "calls": 0}

        def state():
            return _mysql_terminal_state(**model)

        def alter(table, actions):
            assert table == "capability_invocations"
            if not actions:
                return
            call = control["calls"]
            control["calls"] += 1
            if control["failure"] == call:
                raise RuntimeError("injected atomic ALTER failure")
            joined = " ".join(actions)
            if "ADD COLUMN request_reason" in joined:
                model["columns"] = set(module._MYSQL_NEW_COLUMNS)
                model["unique"] = True
                model["shadow"] = "new"
            if any(
                action.startswith(
                    "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
                )
                for action in actions
            ):
                model["canonical"] = "new"
            if any(
                action == (
                    "DROP CHECK "
                    f"{module._MYSQL_SHADOW_IDENTITY_CHECK}"
                )
                for action in actions
            ):
                model["shadow"] = None

        with (
            patch.object(module, "_mysql_capability_state", side_effect=state),
            patch.object(module, "_mysql_alter", side_effect=alter),
        ):
            with pytest.raises(RuntimeError, match="injected atomic ALTER"):
                module._mysql_upgrade_capability_online()
            assert model["canonical"] in {"old", "new"} or model["shadow"] == "new"

            control.update(failure=None, calls=0)
            module._mysql_upgrade_capability_online()

        assert model == {
            "canonical": "new",
            "shadow": None,
            "gate": False,
            "columns": set(module._MYSQL_NEW_COLUMNS),
            "unique": True,
        }

    @pytest.mark.parametrize("failure_call", (0, 1, 2))
    def test_mysql_downgrade_phase_failure_keeps_a_guard_and_replays(
        self,
        failure_call,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_downgrade_failure_{failure_call}"
        )
        model = {
            "canonical": "new",
            "shadow": None,
            "gate": False,
            "columns": set(module._MYSQL_NEW_COLUMNS),
            "unique": True,
        }
        control = {"failure": failure_call, "calls": 0}

        def state():
            return _mysql_terminal_state(**model)

        def alter(table, actions):
            assert table == "capability_invocations"
            if not actions:
                return
            call = control["calls"]
            control["calls"] += 1
            if control["failure"] == call:
                raise RuntimeError("injected atomic ALTER failure")
            joined = " ".join(actions)
            if "ADD CONSTRAINT ck_cap_inv_no_agent_request_downgrade" in joined:
                model["gate"] = True
            if any(
                action.startswith(
                    "ADD CONSTRAINT ck_cap_inv_agent_request_identity "
                )
                for action in actions
            ):
                model["canonical"] = "old"
            if "DROP COLUMN request_reason" in joined:
                model["columns"] = set()
                model["unique"] = False
            if "DROP CHECK ck_cap_inv_no_agent_request_downgrade" in joined:
                model["gate"] = False

        with (
            patch.object(module, "_mysql_capability_state", side_effect=state),
            patch.object(
                module,
                "_mysql_auxiliary_state",
                return_value=_mysql_auxiliary_state(
                    task_source=False,
                    log_columns=False,
                ),
            ),
            patch.object(module, "_mysql_alter", side_effect=alter),
        ):
            with pytest.raises(RuntimeError, match="injected atomic ALTER"):
                module._mysql_downgrade_capability_online()
            assert model["canonical"] in {"old", "new"} or model["gate"]

            control.update(failure=None, calls=0)
            module._mysql_downgrade_capability_online()
            module._mysql_finish_downgrade_online()

        assert model == {
            "canonical": "old",
            "shadow": None,
            "gate": False,
            "columns": set(),
            "unique": False,
        }

    @pytest.mark.parametrize("failure_call", (0, 1, 2, 3))
    def test_mysql_auxiliary_gate_failure_is_atomic_and_replayable(
        self,
        failure_call,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_auxiliary_failure_{failure_call}"
        )
        model = {
            "task_source": True,
            "log_columns": True,
            "task_gate": False,
            "log_gate": False,
        }
        control = {"failure": failure_call, "calls": 0}

        def state():
            return _mysql_auxiliary_state(**model)

        def alter(table, actions):
            if not actions:
                return
            call = control["calls"]
            control["calls"] += 1
            if control["failure"] == call:
                raise RuntimeError("injected atomic auxiliary ALTER failure")
            joined = " ".join(actions)
            if (
                table == "tasks"
                and f"ADD CONSTRAINT {module._MYSQL_TASK_DOWNGRADE_GATE}"
                in joined
            ):
                model["task_gate"] = True
            if (
                table == "log_entries"
                and f"ADD CONSTRAINT {module._MYSQL_LOG_DOWNGRADE_GATE}"
                in joined
            ):
                model["log_gate"] = True
            if table == "log_entries" and "DROP COLUMN actual_transport" in joined:
                assert model["log_gate"] is True
                model["log_columns"] = False
                model["log_gate"] = False
            if table == "tasks" and "DROP COLUMN turn_source_log_id" in joined:
                assert model["task_gate"] is True
                model["task_source"] = False
                model["task_gate"] = False

        def run_downgrade():
            module._mysql_install_auxiliary_downgrade_gates_online()
            module._mysql_downgrade_auxiliary_online()

        with (
            patch.object(module, "_mysql_auxiliary_state", side_effect=state),
            patch.object(module, "_mysql_alter", side_effect=alter),
        ):
            with pytest.raises(
                RuntimeError,
                match="injected atomic auxiliary ALTER failure",
            ):
                run_downgrade()

            # Every successful destructive phase retained the other table's
            # durable gate. Reset only the injected failure and replay from the
            # exact reflected state.
            control.update(failure=None, calls=0)
            run_downgrade()

        assert model == {
            "task_source": False,
            "log_columns": False,
            "task_gate": False,
            "log_gate": False,
        }

    def test_mysql_worker_termination_gate_fences_drop_and_replays(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_gate"
        )
        model = {"present": True, "gate": False}
        control = {"fail": True}

        def state(*, allow_downgrade_gate=False):
            assert allow_downgrade_gate
            return {
                "present": model["present"],
                "missing_indexes": set(),
                "downgrade_gate": model["gate"],
                "downgrade_gate_enforced": model["gate"],
            }

        def alter(table, actions):
            assert table == "worker_task_termination_receipts"
            assert "operation_id IS NULL" in " ".join(actions)
            if control["fail"]:
                raise RuntimeError("injected termination gate failure")
            model["gate"] = True

        def drop_table(table):
            assert table == "worker_task_termination_receipts"
            assert model["gate"] is True
            model["present"] = False

        fake_op = SimpleNamespace(drop_table=drop_table)
        with (
            patch.object(
                module,
                "_worker_task_termination_state",
                side_effect=state,
            ),
            patch.object(module, "_mysql_alter", side_effect=alter),
            patch.object(module, "op", fake_op),
        ):
            with pytest.raises(
                RuntimeError,
                match="injected termination gate failure",
            ):
                module._mysql_install_worker_task_termination_downgrade_gate()
            assert model == {"present": True, "gate": False}

            control["fail"] = False
            module._mysql_install_worker_task_termination_downgrade_gate()
            module._mysql_drop_worker_task_termination_table()

        assert model == {"present": False, "gate": True}

    def test_mysql_worker_termination_drop_refuses_without_fence(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_unfenced_drop"
        )
        state = {
            "present": True,
            "missing_indexes": set(),
            "downgrade_gate": False,
            "downgrade_gate_enforced": False,
        }
        with (
            patch.object(
                module,
                "_worker_task_termination_state",
                return_value=state,
            ),
            patch.object(module, "op") as fake_op,
            pytest.raises(RuntimeError, match="without its durable writer fence"),
        ):
            module._mysql_drop_worker_task_termination_table()
        fake_op.drop_table.assert_not_called()

    def test_mysql_worker_termination_missing_index_suffix_replays(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_index_replay"
        )
        due = "ix_worker_task_term_due"
        states = [
            {
                "present": True,
                "missing_indexes": {due},
                "downgrade_gate": False,
                "downgrade_gate_enforced": False,
            },
            {
                "present": True,
                "missing_indexes": set(),
                "downgrade_gate": False,
                "downgrade_gate_enforced": False,
            },
        ]
        with (
            patch.object(
                module,
                "_worker_task_termination_state",
                side_effect=states,
            ),
            patch.object(module, "_is_offline", return_value=False),
            patch.object(module, "op") as fake_op,
        ):
            module._ensure_worker_task_termination_table()
        fake_op.create_index.assert_called_once_with(
            due,
            "worker_task_termination_receipts",
            ["side", "status", "next_reconcile_at"],
            unique=False,
        )

    @pytest.mark.parametrize(
        ("reflected", "dialect_name", "expected"),
        (
            (mysql.TINYINT(display_width=1), "mysql", True),
            (mysql.BOOLEAN(), "mysql", True),
            (mysql.TINYINT(display_width=2), "mysql", False),
            (mysql.TINYINT(display_width=None), "mysql", False),
            (mysql.TINYINT(display_width=1, unsigned=True), "mysql", False),
            (mysql.TINYINT(display_width=1, zerofill=True), "mysql", False),
            (mysql.TINYINT(display_width=1), "sqlite", False),
        ),
    )
    def test_mysql_worker_termination_boolean_reflection_is_exact(
        self,
        reflected,
        dialect_name,
        expected,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_worker_termination_boolean_{dialect_name}_{expected}"
        )
        assert module._worker_task_termination_type_matches(
            reflected,
            module.sa.Boolean,
            None,
            dialect_name=dialect_name,
        ) is expected

    def test_mysql_worker_termination_legal_check_reflection_is_accepted(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_legal_check_reflection"
        )
        actual = dict(module._WORKER_TASK_TERMINATION_CHECKS)
        actual["ck_worker_task_term_operation_id"] = (
            "((LENGTH(`operation_id`) = 32))"
        )
        actual["ck_worker_task_term_side"] = (
            "(`side` IN (_utf8mb4'manager', _utf8mb4'worker'))"
        )

        module._assert_worker_task_termination_check_semantics(
            actual,
            module._WORKER_TASK_TERMINATION_CHECKS,
        )

    def test_mysql_worker_termination_weakened_check_replay_fails_closed(self):
        module = _load_terminal_arbitration_migration(
            "mysql_worker_termination_weakened_check"
        )
        actual = dict(module._WORKER_TASK_TERMINATION_CHECKS)
        actual["ck_worker_task_term_active_slot"] = "TRUE"

        with pytest.raises(
            RuntimeError,
            match="ck_worker_task_term_active_slot is malformed",
        ):
            module._assert_worker_task_termination_check_semantics(
                actual,
                module._WORKER_TASK_TERMINATION_CHECKS,
            )

    def test_postgresql_worker_termination_legal_check_reflection_is_accepted(
        self,
    ):
        module = _load_terminal_arbitration_migration(
            "postgresql_worker_termination_legal_check_reflection"
        )
        canonical = {
            name: f"CHECK ({expression})"
            for name, expression in module._WORKER_TASK_TERMINATION_CHECKS.items()
        }
        actual = dict(canonical)
        actual["ck_worker_task_term_operation_id"] = (
            "CHECK (((LENGTH(operation_id) = 32)))"
        )

        module._assert_worker_task_termination_check_semantics(
            actual,
            canonical,
        )

    def test_postgresql_worker_termination_weakened_check_replay_fails_closed(
        self,
    ):
        module = _load_terminal_arbitration_migration(
            "postgresql_worker_termination_weakened_check"
        )
        canonical = {
            name: f"CHECK ({expression})"
            for name, expression in module._WORKER_TASK_TERMINATION_CHECKS.items()
        }
        actual = dict(canonical)
        actual["ck_worker_task_term_active_slot"] = "CHECK (TRUE)"

        with pytest.raises(
            RuntimeError,
            match="ck_worker_task_term_active_slot is malformed",
        ):
            module._assert_worker_task_termination_check_semantics(
                actual,
                canonical,
            )

    def test_mysql_capability_gate_is_not_released_before_auxiliary_settles(self):
        module = _load_terminal_arbitration_migration("mysql_gate_release_order")
        capability = _mysql_terminal_state(
            canonical="old",
            gate=True,
        )
        auxiliary = _mysql_auxiliary_state(
            task_gate=True,
            log_gate=True,
        )
        with (
            patch.object(
                module,
                "_mysql_capability_state",
                return_value=capability,
            ),
            patch.object(
                module,
                "_mysql_auxiliary_state",
                return_value=auxiliary,
            ),
            patch.object(module, "_mysql_alter") as alter,
        ):
            with pytest.raises(
                RuntimeError,
                match="before auxiliary downgrade settles",
            ):
                module._mysql_finish_downgrade_online()
            alter.assert_not_called()

    @pytest.mark.parametrize("gate", ("task", "log"))
    def test_mysql_auxiliary_gate_reflection_requires_enforcement(self, gate):
        module = _load_terminal_arbitration_migration(
            f"mysql_auxiliary_gate_enforcement_{gate}"
        )
        state = _mysql_auxiliary_state(
            task_gate=gate == "task",
            log_gate=gate == "log",
        )
        state[f"{gate}_gate_enforced"] = False
        with pytest.raises(RuntimeError, match="gate.*not enforced"):
            module._assert_mysql_auxiliary_state(state)

    def test_mysql_completed_before_stamp_skips_destructive_preflight(self):
        module = _load_terminal_arbitration_migration("mysql_stamp_replay")
        upgraded = _mysql_terminal_state(
            canonical="new",
            columns=set(module._MYSQL_NEW_COLUMNS),
            unique=True,
        )
        downgraded = _mysql_terminal_state(canonical="old")

        assert module._mysql_has_v2_identity_guard(upgraded)
        assert module._mysql_has_v2_audit_schema(upgraded)
        assert not module._mysql_has_v2_identity_guard(downgraded)
        assert not module._mysql_has_v2_audit_schema(downgraded)

        bind = MagicMock()
        duplicate_result = MagicMock()
        duplicate_result.first.return_value = None
        bind.execute.return_value = duplicate_result
        fake_op = SimpleNamespace(
            get_context=lambda: SimpleNamespace(as_sql=False),
            get_bind=lambda: bind,
        )
        with patch.object(module, "op", fake_op):
            module._assert_upgrade_preconditions(require_zero_agent=False)
            assert bind.execute.call_count == 1
            assert "GROUP BY" in str(bind.execute.call_args.args[0])
            bind.reset_mock()
            module._assert_downgrade_preconditions(
                require_zero_agent=False,
                require_zero_task_source=False,
                require_zero_log_provenance=False,
                require_zero_worker_terminations=False,
            )
            bind.execute.assert_not_called()

    @pytest.mark.parametrize(
        ("version", "is_mariadb", "engines", "error"),
        [
            ((8, 0, 15), False, (), "8.0.16"),
            ((8, 0, 36), True, (), "MariaDB"),
            (
                (8, 0, 36),
                False,
                (
                    ("capability_invocations", "InnoDB"),
                    ("log_entries", "InnoDB"),
                ),
                "InnoDB tables",
            ),
            (
                (8, 0, 36),
                False,
                (
                    ("capability_invocations", "InnoDB"),
                    ("log_entries", "MyISAM"),
                    ("tasks", "InnoDB"),
                ),
                "InnoDB tables",
            ),
        ],
    )
    def test_mysql_runtime_requirements_fail_closed(
        self,
        version,
        is_mariadb,
        engines,
        error,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_requirement_{error}"
        )
        bind = MagicMock()
        bind.dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=is_mariadb,
            server_version_info=version,
        )
        bind.execute.return_value = engines
        fake_op = SimpleNamespace(
            get_context=lambda: SimpleNamespace(as_sql=False),
            get_bind=lambda: bind,
        )
        with patch.object(module, "op", fake_op):
            with pytest.raises(RuntimeError, match=error):
                module._require_supported_mysql()

    def test_mysql_runtime_requirements_accept_supported_innodb(self):
        module = _load_terminal_arbitration_migration("mysql_requirement_ok")
        bind = MagicMock()
        bind.dialect = SimpleNamespace(
            name="mysql",
            is_mariadb=False,
            server_version_info=(8, 4, 0),
        )
        bind.execute.return_value = (
            ("capability_invocations", "InnoDB"),
            ("log_entries", "InnoDB"),
            ("tasks", "InnoDB"),
        )
        fake_op = SimpleNamespace(
            get_context=lambda: SimpleNamespace(as_sql=False),
            get_bind=lambda: bind,
        )
        with patch.object(module, "op", fake_op):
            module._require_supported_mysql()

    def test_mysql_guard_reflection_requires_enforcement_and_exact_shapes(self):
        module = _load_terminal_arbitration_migration("mysql_guard_validation")
        state = _mysql_terminal_state(canonical="new")
        state["canonical_enforced"] = False
        with pytest.raises(RuntimeError, match="no enforceable"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old", shadow="new")
        state["canonical_enforced"] = False
        with pytest.raises(RuntimeError, match="canonical.*not enforced"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old", shadow="new")
        state["shadow_enforced"] = False
        with pytest.raises(RuntimeError, match="not enforced"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old", unique=True)
        state["unique"] = False
        with pytest.raises(RuntimeError, match="unique constraint is malformed"):
            module._assert_mysql_guarded(state)

        state = _mysql_terminal_state(canonical="old")
        state["column_shapes"]["request_output_hash"] = False
        with pytest.raises(RuntimeError, match="column shape is malformed"):
            module._assert_mysql_guarded(state)

    @pytest.mark.parametrize(
        ("transport_sql", "enforced", "should_pass"),
        [
            (
                "actual_transport IS NULL OR (turn_scope IS NOT NULL AND "
                "turn_scope = 'source' AND "
                "actual_transport IN ('claude_pty', 'claude_exec', "
                "'codex_app_server', 'codex_exec'))",
                {
                    "ck_log_entries_turn_scope",
                    "ck_log_entries_actual_transport",
                },
                True,
            ),
            (
                "actual_transport IS NULL OR actual_transport IN "
                "('claude_pty', 'claude_exec', 'codex_app_server', "
                "'codex_exec')",
                {
                    "ck_log_entries_turn_scope",
                    "ck_log_entries_actual_transport",
                },
                False,
            ),
            (
                # This weaker expression admits a valid transport with NULL
                # scope because a SQL CHECK treats UNKNOWN as satisfied.
                "actual_transport IS NULL OR (turn_scope = 'source' AND "
                "actual_transport IN ('claude_pty', 'claude_exec', "
                "'codex_app_server', 'codex_exec'))",
                {
                    "ck_log_entries_turn_scope",
                    "ck_log_entries_actual_transport",
                },
                False,
            ),
            (
                "actual_transport IS NULL OR (turn_scope IS NOT NULL AND "
                "turn_scope = 'source' AND "
                "actual_transport IN ('claude_pty', 'claude_exec', "
                "'codex_app_server', 'codex_exec'))",
                {"ck_log_entries_turn_scope"},
                False,
            ),
        ],
    )
    def test_mysql_actual_transport_reflection_requires_source_scope_and_enforcement(
        self,
        transport_sql,
        enforced,
        should_pass,
    ):
        module = _load_terminal_arbitration_migration(
            f"mysql_actual_transport_{should_pass}_{len(enforced)}"
        )
        inspector = MagicMock()

        def columns(table):
            if table == "tasks":
                return [
                    {
                        "name": "turn_source_log_id",
                        "type": mysql.INTEGER(),
                        "nullable": True,
                    }
                ]
            assert table == "log_entries"
            return [
                {
                    "name": "turn_scope",
                    "type": mysql.VARCHAR(length=16),
                    "nullable": True,
                },
                {
                    "name": "actual_transport",
                    "type": mysql.VARCHAR(length=24),
                    "nullable": True,
                },
            ]

        inspector.get_columns.side_effect = columns
        inspector.get_check_constraints.return_value = [
            {
                "name": "ck_log_entries_turn_scope",
                "sqltext": module._TURN_SCOPE_CHECK,
            },
            {
                "name": "ck_log_entries_actual_transport",
                "sqltext": transport_sql,
            },
        ]
        fake_op = SimpleNamespace(get_bind=MagicMock())
        with (
            patch.object(module, "op", fake_op),
            patch.object(module.sa, "inspect", return_value=inspector),
            patch.object(
                module,
                "_mysql_enforced_checks",
                return_value=enforced,
            ),
            patch.object(module, "_mysql_alter") as alter,
        ):
            if should_pass:
                module._mysql_upgrade_auxiliary_online()
                alter.assert_called_once_with("log_entries", [])
            else:
                with pytest.raises(
                    RuntimeError,
                    match="actual-transport CHECK.*malformed or not enforced",
                ):
                    module._mysql_upgrade_auxiliary_online()


class TestCodeReviewMigration:
    def test_refuses_review_history_or_reviewer_task_downgrade(self, tmp_path):
        db_path = str(tmp_path / "code-review-downgrade-guard.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CODE_REVIEW_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        digest = "b" * 64
        sha = "c" * 40

        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO code_review_runs "
                "(capability_invocation_id, capability_execution_id, attempt, "
                "developer_task_id, reviewer_task_id, reviewer_task_retry_count, "
                "repo_path, base_sha, head_sha, head_tree_sha, patch_sha256, "
                "subject_ref, subject_hash, prompt_hash, created_at, updated_at) "
                "VALUES (1, 1, 1, 1, 2, 0, '/repo', :sha, :sha, :sha, "
                ":digest, '{}', :digest, :digest, "
                "'2026-08-05 00:00:00', '2026-08-05 00:00:00')"
            ), {"sha": sha, "digest": digest})
        with pytest.raises(RuntimeError, match="code_review_runs contains history"):
            _run_alembic(cfg, command.downgrade, CAPABILITY_CORE_REVISION)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM code_review_runs"))
            conn.execute(text(
                "INSERT INTO tasks "
                "(title, description, status, priority, target_branch, "
                "merge_status, retry_count, max_retries, mode, tags, metadata, "
                "created_at) VALUES ('orphan reviewer', 'd', 'pending', 0, "
                "'main', 'pending', 0, 0, 'auto', "
                ":tags, :metadata, '2026-08-05 00:00:00')"
            ), {
                "tags": '["pre-pr-code-review"]',
                "metadata": (
                    '{"code_review_run_id":1,"capability_invocation_id":1,'
                    '"capability_execution_id":1}'
                ),
            })
        with pytest.raises(RuntimeError, match="retains reviewer ownership"):
            _run_alembic(cfg, command.downgrade, CAPABILITY_CORE_REVISION)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM tasks WHERE title = 'orphan reviewer'"))
        engine.dispose()

        _run_alembic(cfg, command.downgrade, CAPABILITY_CORE_REVISION)


class TestFreshMigration:
    """A fresh database (no tables) can be fully created via Alembic upgrade."""

    def test_fresh_db_upgrade_from_scratch(self, tmp_path):
        """Running upgrade head on empty DB creates all tables."""
        db_path = str(tmp_path / "fresh.db")

        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        tables = _get_all_tables(engine)
        expected_tables = {"instances", "projects", "project_todos", "tasks", "log_entries", "worktrees", "global_settings", "secrets", "tags", "discussions", "discussion_messages", "discussion_agents", "discussion_events", "quick_phrases", "sub_agent_sessions", "sub_agent_reports", "pr_reviews", "pr_reviewer_runs", "pr_findings", "pr_finding_actions", "pr_finding_rebuttals", "pr_monitor_runs", "pr_repair_wakes", "pr_merge_queue_actions", "monitored_repos", "workers", "worker_turn_handoff_receipts", "worker_task_termination_receipts", "skill_lessons", "skill_usage", "feishu_user_binding", "org_members", "org_teams", "org_team_members", "task_shares", "project_shares", "shared_tasks_received", "user_skills", "users", "user_groups", "user_group_members", "team_task_shares", "team_project_shares", "plan_agent_runs", "plan_agent_steps", "plans", "plan_versions", "plan_input_requests", "plan_applications", "plan_application_receipts", "plan_application_attempts", "plan_legacy_task_links", "capability_invocations", "capability_executions", "code_review_runs", "code_review_results", "delivery_runs", "delivery_cycles", "delivery_turns", "delivery_events", "delivery_actions", "delivery_transitions"}
        assert tables == expected_tables, f"Missing tables: {expected_tables - tables}"

        # Verify all columns from latest migration exist
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        assert "loop_progress" in task_cols
        assert "max_iterations" in task_cols
        assert "context_window_usage" in task_cols
        assert "plan_target_task_id" in task_cols
        assert "plan_context_snapshot" in task_cols
        assert "plan_applied_log_id" in task_cols
        assert "attention_tag" in task_cols
        assert "delivery_run_id" in task_cols
        assert "delivery_role" in task_cols

        worktree_cols = _get_table_columns(engine, "worktrees")
        assert {
            "task_id",
            "delivery_run_id",
            "last_verified_head",
            "cleanup_status",
        }.issubset(worktree_cols)

        plan_run_cols = _get_table_columns(engine, "plan_agent_runs")
        assert "capability_execution_id" in plan_run_cols

        delivery_run_cols = set(_get_table_columns(engine, "delivery_runs"))
        assert {
            "admission_scope",
            "idempotency_key",
            "request_hash",
            "developer_task_id",
            "worktree_id",
            "current_cycle_id",
            "controller_generation",
            "lease_owner",
            "lease_expires_at",
            "next_reconcile_at",
        }.issubset(delivery_run_cols)
        delivery_run_unique_columns = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints(
                "delivery_runs"
            )
        }
        assert (
            "admission_scope",
            "project_id",
            "idempotency_key",
        ) in delivery_run_unique_columns
        assert ("source_todo_id",) in delivery_run_unique_columns

        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" in log_cols
        assert "task_retry_count" in log_cols

        project_cols = _get_table_columns(engine, "projects")
        assert "sort_order" in project_cols
        assert "tags" in project_cols

        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" in pr_review_cols
        assert "head_sha" in pr_review_cols
        assert "delivery_id" in pr_review_cols
        unique_column_sets = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert (
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
        ) in unique_column_sets
        assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        assert ("repo_id", "delivery_id") in unique_column_sets

        pr_finding_cols = {
            item["name"]: item
            for item in inspect(engine).get_columns("pr_findings")
        }
        assert "resolution_lease_token" in pr_finding_cols
        assert "resolution_lease_expires_at" in pr_finding_cols
        assert "fixed_resolution_actor" in pr_finding_cols
        assert "BIGINT" in str(pr_finding_cols["github_comment_id"]["type"]).upper()
        assert isinstance(
            Base.metadata.tables["pr_findings"].c.github_comment_id.type,
            BigInteger,
        )

        capability_invocation_columns = set(
            _get_table_columns(engine, "capability_invocations")
        )
        assert {
            "active_task_id",
            "idempotency_key",
            "request_task_turn_generation",
            "request_output_log_id",
            "request_reason",
            "request_protocol_version",
            "request_output_hash",
            "request_native_turn_id",
            "result_hash",
        }.issubset(capability_invocation_columns)
        capability_execution_columns = set(
            _get_table_columns(engine, "capability_executions")
        )
        assert {
            "active_invocation_id",
            "lease_token",
            "handle_generation",
            "output_hash",
        }.issubset(capability_execution_columns)
        invocation_unique_columns = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints(
                "capability_invocations"
            )
        }
        assert ("task_id", "idempotency_key") in invocation_unique_columns
        assert ("active_task_id",) in invocation_unique_columns
        execution_unique_columns = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints(
                "capability_executions"
            )
        }
        assert ("invocation_id", "attempt") in execution_unique_columns
        assert ("active_invocation_id",) in execution_unique_columns

        action_columns = {
            item["name"]
            for item in inspect(engine).get_columns("pr_finding_actions")
        }
        assert {
            "finding_id",
            "action_type",
            "status",
            "idempotency_key",
            "actor_user_id",
            "human_advice",
            "task_id",
            "expected_head_sha",
            "active_fix_finding_id",
            "patch_sha256",
            "download_receipt_hash",
            "downloaded_by_user_id",
            "downloaded_at",
            "confirmed_by_user_id",
            "confirmed_at",
            "candidate_commit_sha",
            "candidate_created_at",
            "push_attempted_at",
            "cancelled_by_user_id",
            "cancelled_at",
            "operation_token",
            "operation_expires_at",
            "result",
            "error_message",
            "created_at",
            "updated_at",
            "completed_at",
        }.issubset(action_columns)
        action_unique_constraints = {
            (constraint["name"], tuple(constraint["column_names"]))
            for constraint in inspect(engine).get_unique_constraints(
                "pr_finding_actions"
            )
        }
        assert (
            "uq_pr_finding_actions_idempotency_key",
            ("idempotency_key",),
        ) in action_unique_constraints
        assert (
            "uq_pr_finding_actions_active_fix",
            ("active_fix_finding_id",),
        ) in action_unique_constraints
        action_check_constraints = {
            constraint["name"]: constraint.get("sqltext", "")
            for constraint in inspect(engine).get_check_constraints(
                "pr_finding_actions"
            )
        }
        assert set(action_check_constraints) == {
            "ck_pr_finding_actions_active_slot",
            "ck_pr_finding_actions_status",
            "ck_pr_finding_actions_type",
        }
        active_slot_sql = " ".join(
            action_check_constraints["ck_pr_finding_actions_active_slot"]
            .lower()
            .split()
        )
        assert "active_fix_finding_id is not null" in active_slot_sql
        assert "active_fix_finding_id = finding_id" in active_slot_sql
        action_foreign_keys = {
            (
                tuple(constraint["constrained_columns"]),
                constraint["referred_table"],
                tuple(constraint["referred_columns"]),
                (constraint.get("options") or {}).get("ondelete"),
            )
            for constraint in inspect(engine).get_foreign_keys(
                "pr_finding_actions"
            )
        }
        assert (("finding_id",), "pr_findings", ("id",), "CASCADE") in (
            action_foreign_keys
        )
        assert (("task_id",), "tasks", ("id",), None) in action_foreign_keys
        action_indexes = {
            (index["name"], tuple(index["column_names"]))
            for index in inspect(engine).get_indexes("pr_finding_actions")
        }
        assert {
            ("ix_pr_finding_actions_finding_id", ("finding_id",)),
            ("ix_pr_finding_actions_status", ("status",)),
            ("ix_pr_finding_actions_actor_user_id", ("actor_user_id",)),
            ("ix_pr_finding_actions_task_id", ("task_id",)),
        }.issubset(action_indexes)
        assert (
            Base.metadata.tables["monitored_repos"]
            .c.required_checks.server_default
            is None
        )

        # Verify alembic_version at head
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == _get_head_revision(cfg)

        engine.dispose()

    def test_fresh_db_downgrade_and_upgrade(self, tmp_path):
        """Migrations are reversible: upgrade → downgrade → upgrade."""
        db_path = str(tmp_path / "roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, "head")
        _run_alembic(cfg, command.downgrade, "6b3f8a1c2d9e")

        engine = create_engine(f"sqlite:///{db_path}")
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" not in task_cols
        assert "loop_progress" not in task_cols
        log_cols = _get_table_columns(engine, "log_entries")
        assert "loop_iteration" not in log_cols
        engine.dispose()

        # Upgrade again
        _run_alembic(cfg, command.upgrade, "head")
        engine = create_engine(f"sqlite:///{db_path}")
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        engine.dispose()

    def test_finding_actions_revision_downgrades_and_reupgrades(self, tmp_path):
        """The finding-action table is owned by the new linear head."""

        db_path = str(tmp_path / "finding-actions-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "pr_finding_actions" in _get_all_tables(engine)
        engine.dispose()

        _run_alembic(cfg, command.downgrade, PR_REVIEW_PANEL_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "pr_finding_actions" not in _get_all_tables(engine)
        with engine.connect() as conn:
            revisions = {
                row[0]
                for row in conn.execute(
                text("SELECT version_num FROM alembic_version")
                ).fetchall()
            }
        assert revisions == {PR_REVIEW_PANEL_REVISION}
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "pr_finding_actions" in _get_all_tables(engine)
        with engine.connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert revision == CURRENT_HEAD_REVISION
        engine.dispose()


class TestAlreadyMigratedDb:
    """A database already at head is a no-op."""

    def test_upgrade_head_is_noop(self, tmp_path):
        db_path = str(tmp_path / "current.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, "head")
        # Running again should not raise
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == _get_head_revision(cfg)
        engine.dispose()

    def test_idempotency_migration_preserves_existing_pr_reviews(self, tmp_path):
        db_path = str(tmp_path / "existing_pr_reviews.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "31fe767354b7")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/repo', 1, 0, 'secret', 'codex', 'main', '[]',
                    'active', '2026-07-22 00:00:00', '2026-07-22 00:00:00'
                )
            """))
            for created_at in ("2026-07-22 00:00:00", "2026-07-22 00:01:00"):
                conn.execute(text("""
                    INSERT INTO pr_reviews (
                        repo_id, pr_number, pr_title, pr_author, pr_url,
                        status, created_at
                    ) VALUES (
                        1, 42, 'Title', 'alice',
                        'https://github.com/owner/repo/pull/42',
                        'approved', :created_at
                    )
                """), {"created_at": created_at})
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT base_sha, head_sha, delivery_id "
                "FROM pr_reviews ORDER BY id"
            )).fetchall()
            assert rows == [(None, None, None), (None, None, None)]
            required_checks = conn.execute(text(
                "SELECT required_checks FROM monitored_repos WHERE id = 1"
            )).scalar_one()
            if isinstance(required_checks, str):
                required_checks = json.loads(required_checks)
            assert required_checks == []
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, required_checks, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/default-checks', 1, 0, 'secret', 'codex', 'main',
                    '[]', '[]', 'active', '2026-07-22 00:02:00',
                    '2026-07-22 00:02:00'
                )
            """))
            inserted_default = conn.execute(text(
                "SELECT required_checks FROM monitored_repos "
                "WHERE repo_full_name = 'owner/default-checks'"
            )).scalar_one()
            if isinstance(inserted_default, str):
                inserted_default = json.loads(inserted_default)
            assert inserted_default == []
        required_column = next(
            item for item in inspect(engine).get_columns("monitored_repos")
            if item["name"] == "required_checks"
        )
        assert required_column["nullable"] is False
        assert required_column["default"] is None
        engine.dispose()

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_pr_panel_migration_compiles_portable_schema(self, dialect_name):
        """The Panel schema uses portable Boolean/JSON defaults and bigint IDs."""

        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "7a1d4e9c2b60_add_pr_review_panel.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"pr_panel_migration_for_{dialect_name}", migration_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module.upgrade()
        ddl = output.getvalue().lower()
        if dialect_name == "postgresql":
            assert "boolean default false not null" in ddl
            assert "required_checks set not null" in ddl
            assert "cast('[]' as json)" in ddl
        else:
            assert "bool not null default false" in ddl
            assert "modify required_checks json not null" in ddl
            assert "required_checks = json_array()" in ddl
        assert "boolean default 0" not in ddl
        assert all(
            "default" not in line
            for line in ddl.splitlines()
            if "required_checks" in line
        )
        assert "github_comment_id bigint" in ddl

    def test_base_sha_migration_preserves_existing_snapshot_keys(self, tmp_path):
        db_path = str(tmp_path / "existing_pr_review_snapshot.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "c8f5d3a72b10")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/repo', 1, 0, 'secret', 'codex', 'main', '[]',
                    'active', '2026-07-31 00:00:00', '2026-07-31 00:00:00'
                )
            """))
            conn.execute(
                text("""
                    INSERT INTO pr_reviews (
                        repo_id, pr_number, head_sha, delivery_id, pr_title,
                        pr_author, pr_url, status, created_at
                    ) VALUES (
                        1, 42, :head_sha, 'delivery-1', 'Title', 'alice',
                        'https://github.com/owner/repo/pull/42',
                        'approved', '2026-07-31 00:00:00'
                    )
                """),
                {"head_sha": "a" * 40},
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT base_sha, head_sha, delivery_id, action_nonce, "
                "pending_action, pending_review_body, publishing_actor, "
                "publishing_retry_count, publishing_task_started_at, "
                "publishing_started_at FROM pr_reviews"
            )).one()
            assert row == (
                None,
                "a" * 40,
                "delivery-1",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )

            unique_column_sets = {
                tuple(constraint["column_names"])
                for constraint in inspect(conn).get_unique_constraints("pr_reviews")
            }
            assert (
                "repo_id",
                "pr_number",
                "base_sha",
                "head_sha",
            ) in unique_column_sets
            assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        engine.dispose()

    def test_base_sha_migration_downgrade_restores_head_constraint(self, tmp_path):
        db_path = str(tmp_path / "base_sha_roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitored_repos (
                    repo_full_name, enabled, auto_merge, webhook_secret,
                    provider, default_branch, allowed_authors, required_checks, status,
                    created_at, updated_at
                ) VALUES (
                    'owner/rollback', 1, 0, 'secret', 'claude', 'main', '[]',
                    '[]', 'active', '2026-07-31 00:00:00',
                    '2026-07-31 00:00:00'
                )
            """))
            for base_sha in ("1" * 40, "2" * 40):
                conn.execute(
                    text("""
                        INSERT INTO pr_reviews (
                            repo_id, pr_number, base_sha, head_sha, pr_title,
                            pr_author, pr_url, status, created_at
                        ) VALUES (
                            1, 42, :base_sha, :head_sha, 'Title', 'alice',
                            'https://github.com/owner/rollback/pull/42',
                            'approved', '2026-07-31 00:00:00'
                        )
                    """),
                    {"base_sha": base_sha, "head_sha": "a" * 40},
                )
        engine.dispose()

        _run_alembic(cfg, command.downgrade, "c8f5d3a72b10")

        engine = create_engine(f"sqlite:///{db_path}")
        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" not in pr_review_cols
        assert "publishing_actor" not in pr_review_cols
        log_cols = _get_table_columns(engine, "log_entries")
        assert "task_retry_count" not in log_cols
        unique_column_sets = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert ("repo_id", "pr_number", "head_sha") in unique_column_sets
        assert (
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
        ) not in unique_column_sets
        with engine.connect() as conn:
            heads = [
                row[0]
                for row in conn.execute(
                    text("SELECT head_sha FROM pr_reviews ORDER BY id")
                ).fetchall()
            ]
            assert heads == [None, "a" * 40]
        engine.dispose()

        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        pr_review_cols = _get_table_columns(engine, "pr_reviews")
        assert "base_sha" in pr_review_cols
        assert "publishing_actor" in pr_review_cols
        log_cols = _get_table_columns(engine, "log_entries")
        assert "task_retry_count" in log_cols
        unique_column_sets = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("pr_reviews")
        }
        assert (
            "repo_id",
            "pr_number",
            "base_sha",
            "head_sha",
        ) in unique_column_sets
        assert ("repo_id", "pr_number", "head_sha") not in unique_column_sets
        engine.dispose()


class TestVersionedPlanBackfill:
    """Plan v2 backfills only the legacy Task state still present on main."""

    def test_main_plan_states_and_attachments_are_backfilled(self, tmp_path):
        db_path = str(tmp_path / "main-plan-states.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, ATTENTION_TAG_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            required = """
                id, title, description, status, priority, target_branch,
                merge_status, retry_count, max_retries, mode, created_at
            """
            conn.execute(
                text(
                    f"""INSERT INTO tasks ({required}, metadata) VALUES
                    (21, 'Pending legacy Plan', 'Wait to run', 'pending', 0,
                     'main', 'pending', 0, 2, 'plan',
                     '2026-08-01 10:00:00', :metadata)"""
                ),
                {
                    "metadata": json.dumps(
                        {
                            "file_paths": ["/uploads/requirements.txt"],
                            "attachments": [
                                {
                                    "url": "/api/uploads/requirements.txt",
                                    "name": "requirements.txt",
                                    "is_image": False,
                                }
                            ],
                        }
                    )
                },
            )
            conn.execute(
                text(
                    f"""INSERT INTO tasks ({required}, plan_content,
                    plan_approved, completed_at) VALUES
                    (22, 'Failed legacy Plan', 'Failed before output',
                     'failed', 0, 'main', 'pending', 0, 2, 'plan',
                     '2026-08-01 11:00:00', NULL, NULL,
                     '2026-08-01 11:30:00'),
                    (31, 'Needs decision', 'Review this', 'plan_review', 0,
                     'main', 'pending', 0, 2, 'plan',
                     '2026-08-01 12:00:00', '# Review', NULL, NULL),
                    (32, 'Approved and queued', 'Execute this', 'pending', 0,
                     'main', 'pending', 0, 2, 'plan',
                     '2026-08-01 13:00:00', '# Queued', 1, NULL),
                    (33, 'Already executed', 'Was executed', 'completed', 0,
                     'main', 'pending', 0, 2, 'plan',
                     '2026-08-01 14:00:00', '# Done', 1,
                     '2026-08-01 15:00:00'),
                    (34, 'Rejected', 'Do not execute', 'cancelled', 0,
                     'main', 'pending', 0, 2, 'plan',
                     '2026-08-01 16:00:00', '# Rejected', 0,
                     '2026-08-01 17:00:00')"""
                )
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """SELECT l.legacy_task_id, l.plan_run_id,
                    r.status AS run_status, p.active_run_id,
                    p.initial_attachments, p.pipeline_config,
                    t.status AS task_status, v.review_verdict,
                    v.human_decision, a.application_type,
                    a.execution_task_id
                    FROM plan_legacy_task_links l
                    JOIN plan_agent_runs r ON r.id = l.plan_run_id
                    JOIN plans p ON p.id = l.plan_id
                    JOIN tasks t ON t.id = l.legacy_task_id
                    LEFT JOIN plan_versions v ON v.id = l.plan_version_id
                    LEFT JOIN plan_applications a
                      ON a.plan_version_id = l.plan_version_id
                    ORDER BY l.legacy_task_id"""
                )
            ).mappings().all()

            assert [row["legacy_task_id"] for row in rows] == [21, 22, 31, 32, 33, 34]
            assert rows[0]["run_status"] == "queued"
            assert rows[0]["task_status"] == "superseded"
            assert rows[0]["active_run_id"] == rows[0]["plan_run_id"]
            attachments = rows[0]["initial_attachments"]
            if isinstance(attachments, str):
                attachments = json.loads(attachments)
            assert attachments == [
                {
                    "url": "/api/uploads/requirements.txt",
                    "name": "requirements.txt",
                    "is_image": False,
                    "path": "/uploads/requirements.txt",
                }
            ]
            pipeline = rows[0]["pipeline_config"]
            if isinstance(pipeline, str):
                pipeline = json.loads(pipeline)
            assert pipeline["planner"]["primary"]["provider"] == "claude"
            assert pipeline["max_interactions"] == 3

            assert rows[1]["run_status"] == "failed"
            assert rows[1]["task_status"] == "failed"
            assert rows[1]["human_decision"] is None
            assert rows[2]["review_verdict"] == "disabled"
            assert rows[2]["human_decision"] == "pending"
            assert rows[2]["application_type"] is None
            for index, task_id in ((3, 32), (4, 33)):
                assert rows[index]["human_decision"] == "approved"
                assert rows[index]["application_type"] == "execution_task"
                assert rows[index]["execution_task_id"] == task_id
            assert rows[5]["human_decision"] == "rejected"
            assert rows[5]["application_type"] is None
        engine.dispose()

    def test_active_legacy_plan_process_blocks_backfill(self, tmp_path):
        db_path = str(tmp_path / "active-legacy-plan.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, ATTENTION_TAG_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                    ) VALUES (
                    31, 'Active legacy Plan', 'Still running', 'executing', 0,
                    'main', 'pending', 0, 2, 'plan',
                    '2026-08-01 10:00:00')"""
                )
            )
            conn.execute(
                text(
                    """INSERT INTO instances (
                    id, name, pid, status, current_task_id, provider, model,
                    total_tasks_completed, total_cost_usd
                    ) VALUES (
                    41, 'legacy-owner', 12345, 'running', 31, 'claude',
                    'default', 0, 0)"""
                )
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="active process evidence"):
            _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)

    def test_active_legacy_plan_task_state_blocks_without_instance(self, tmp_path):
        db_path = str(tmp_path / "active-plan-without-instance.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, ATTENTION_TAG_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                    ) VALUES (
                    32, 'Unowned active Plan', 'State is authoritative',
                    'in_progress', 0, 'main', 'pending', 0, 2, 'plan',
                    '2026-08-01 10:00:00')"""
                )
            )
        engine.dispose()

        with pytest.raises(RuntimeError, match="active state evidence"):
            _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)

    def test_plan_v2_roundtrip_restores_pending_legacy_carrier(self, tmp_path):
        db_path = str(tmp_path / "plan-v2-roundtrip.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, ATTENTION_TAG_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """INSERT INTO tasks (
                    id, title, description, status, priority, target_branch,
                    merge_status, retry_count, max_retries, mode, created_at
                    ) VALUES (
                    77, 'Queued Plan', 'Plan it', 'pending', 0, 'main',
                    'pending', 0, 2, 'plan', '2026-08-01 10:00:00')"""
                )
            )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT status FROM tasks WHERE id=77")
            ).scalar_one() == "superseded"
            assert conn.execute(
                text("SELECT COUNT(*) FROM plan_legacy_task_links")
            ).scalar_one() == 1
        engine.dispose()

        _run_alembic(cfg, command.downgrade, ATTENTION_TAG_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plans" not in _get_all_tables(engine)
        with engine.connect() as conn:
            task = conn.execute(
                text(
                    "SELECT status, completed_at, error_message "
                    "FROM tasks WHERE id=77"
                )
            ).mappings().one()
            assert dict(task) == {
                "status": "pending",
                "completed_at": None,
                "error_message": None,
            }
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT COUNT(*) FROM plan_legacy_task_links")
            ).scalar_one() == 1
            assert conn.execute(
                text("SELECT status FROM tasks WHERE id=77")
            ).scalar_one() == "superseded"
        engine.dispose()

    def test_worker_receipt_and_application_audit_schema_is_complete(self, tmp_path):
        db_path = str(tmp_path / "plan-delivery-schema.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")

        receipt_columns = set(_get_table_columns(engine, "plan_application_receipts"))
        assert {
            "receipt_key",
            "worker_id",
            "manager_user_log_id",
            "delivery_status",
            "outbox_payload",
            "payload_digest",
            "delivery_error",
            "launch_evidence",
            "delivery_resolution",
        }.issubset(receipt_columns)
        run_columns = set(_get_table_columns(engine, "plan_agent_runs"))
        assert {
            "import_payload_digest",
            "import_attachment_receipt",
            "draft_content",
            "draft_step_id",
            "draft_repo_revision",
        }.issubset(run_columns)
        step_columns = set(_get_table_columns(engine, "plan_agent_steps"))
        assert {
            "last_delta_at",
            "streamed_output_chars",
            "last_event_type",
        }.issubset(step_columns)
        assert "plan_application_attempts" in _get_all_tables(engine)
        engine.dispose()


class TestSchemaConsistency:
    """The schema produced by Alembic migrations matches the ORM models.

    This is the critical test: if someone adds a column to an ORM model
    but forgets to create an Alembic migration, this test will catch it.
    """

    def test_plan_application_integrity_constraint_compiles_on_all_dialects(self):
        table = backend.models.plan.PlanApplication.__table__
        for dialect in (sqlite.dialect(), postgresql.dialect(), mysql.dialect()):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_plan_application_target" in ddl
            assert "execution_task" in ddl

    def test_capability_integrity_constraints_compile_on_all_dialects(self):
        for table_name in (
            "capability_invocations",
            "capability_executions",
        ):
            table = Base.metadata.tables[table_name]
            for dialect in (
                sqlite.dialect(),
                postgresql.dialect(),
                mysql.dialect(),
            ):
                ddl = str(CreateTable(table).compile(dialect=dialect))
                assert "active_slot" in ddl
                assert "UNIQUE" in ddl
                if table_name == "capability_invocations":
                    assert "uq_cap_inv_task_output_log" in ddl
                    assert "request_protocol_version >= 1" in ddl

    def test_terminal_turn_constraints_compile_on_all_dialects(self):
        table = Base.metadata.tables["log_entries"]
        for dialect in (
            sqlite.dialect(),
            postgresql.dialect(),
            mysql.dialect(),
        ):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_log_entries_turn_scope" in ddl
            assert "'foreground'" in ddl
            assert "ck_log_entries_actual_transport" in ddl
            assert "actual_transport IS NULL" in ddl
            assert "turn_scope IS NOT NULL" in ddl
            assert "turn_scope = 'source'" in ddl
            assert "'codex_app_server'" in ddl

    def test_worker_termination_constraints_compile_on_all_dialects(self):
        table = Base.metadata.tables["worker_task_termination_receipts"]
        for dialect in (
            sqlite.dialect(),
            postgresql.dialect(),
            mysql.dialect(),
        ):
            ddl = str(CreateTable(table).compile(dialect=dialect))
            assert "ck_worker_task_term_active_slot" in ddl
            assert "ck_worker_task_term_source_status" in ddl
            assert "ck_worker_task_term_handoff_shape" in ddl
            assert (
                "source_worker_turn_handoff_acknowledged IN (TRUE, FALSE)"
                in ddl
            )
            assert "ck_worker_task_term_counters" in ddl
            assert "ck_worker_task_term_execution_owner" in ddl
            assert "execution_token" in ddl
            assert "state_version" in ddl
            assert "next_reconcile_at IS NOT NULL" in ddl
            assert "ck_worker_task_term_ack_intent" in ddl
            assert "ack_intent_at" in ddl
            assert "reconcile_count >= 0" in ddl
            assert (
                "'accepted', 'executing', 'succeeded', 'rejected', 'conflict'"
                in ddl
            )
            assert "uq_worker_task_term_active_task" in ddl
            assert "FOREIGN KEY(task_id) REFERENCES tasks" in ddl

        mysql_ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
        assert "ENGINE=InnoDB" in mysql_ddl

    def test_worker_termination_migration_constraints_match_orm(self, tmp_path):
        db_path = str(tmp_path / "worker-termination-schema.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)
        table_name = "worker_task_termination_receipts"
        table = Base.metadata.tables[table_name]

        expected_indexes = {
            (index.name, tuple(column.name for column in index.columns))
            for index in table.indexes
        }
        actual_indexes = {
            (index["name"], tuple(index["column_names"]))
            for index in inspector.get_indexes(table_name)
        }
        assert actual_indexes == expected_indexes

        expected_uniques = {
            (
                constraint.name,
                tuple(column.name for column in constraint.columns),
            )
            for constraint in table.constraints
            if isinstance(constraint, UniqueConstraint)
        }
        actual_uniques = {
            (constraint["name"], tuple(constraint["column_names"]))
            for constraint in inspector.get_unique_constraints(table_name)
        }
        assert actual_uniques == expected_uniques

        expected_checks = {
            constraint.name
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        actual_checks = {
            constraint["name"]
            for constraint in inspector.get_check_constraints(table_name)
        }
        assert actual_checks == expected_checks
        module = _load_terminal_arbitration_migration(
            "worker_termination_orm_expression_match"
        )
        orm_check_sql = {
            constraint.name: constraint.sqltext
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        assert set(orm_check_sql) == set(
            module._WORKER_TASK_TERMINATION_CHECKS
        )
        for name, expected_sql in (
            module._WORKER_TASK_TERMINATION_CHECKS.items()
        ):
            assert module._boolean_check_shape(
                orm_check_sql[name]
            ) == module._boolean_check_shape(expected_sql)

        expected_foreign_keys = {
            (
                tuple(
                    element.parent.name for element in constraint.elements
                ),
                constraint.elements[0].column.table.name,
                tuple(
                    element.column.name for element in constraint.elements
                ),
                constraint.ondelete,
            )
            for constraint in table.constraints
            if isinstance(constraint, ForeignKeyConstraint)
        }
        actual_foreign_keys = {
            (
                tuple(constraint["constrained_columns"]),
                constraint["referred_table"],
                tuple(constraint["referred_columns"]),
                (constraint.get("options") or {}).get("ondelete"),
            )
            for constraint in inspector.get_foreign_keys(table_name)
        }
        assert actual_foreign_keys == expected_foreign_keys
        engine.dispose()

    def test_code_review_integrity_constraints_compile_on_all_dialects(self):
        for table_name in ("code_review_runs", "code_review_results"):
            table = Base.metadata.tables[table_name]
            for dialect in (
                sqlite.dialect(),
                postgresql.dialect(),
                mysql.dialect(),
            ):
                ddl = str(CreateTable(table).compile(dialect=dialect))
                assert "code_review" in ddl
                assert "UNIQUE" in ddl

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_auto_capability_turn_migration_compiles_offline(self, dialect_name):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "c3a7e9f1b2d4_add_auto_capability_turn_identity.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"auto_capability_turn_migration_for_{dialect_name}",
            migration_path,
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        assert "turn_generation" in upgrade_ddl
        assert "capability_policy" in upgrade_ddl
        assert "task_turn_generation" in upgrade_ddl
        assert "native_turn_id" in upgrade_ddl
        assert "request_output_log_id" in upgrade_ddl
        assert "request_native_turn_id" in upgrade_ddl

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        assert "drop column turn_generation" in downgrade_ddl
        assert "drop column capability_policy" in downgrade_ddl
        assert "drop column task_turn_generation" in downgrade_ddl
        assert "drop column native_turn_id" in downgrade_ddl
        assert "drop column request_output_log_id" in downgrade_ddl
        assert "drop column request_native_turn_id" in downgrade_ddl

    def test_terminal_arbitration_postgresql_migration_compiles_offline(self):
        module = _load_terminal_arbitration_migration("postgresql")

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        assert "turn_source_log_id" in upgrade_ddl
        assert "turn_scope" in upgrade_ddl
        assert "ck_log_entries_turn_scope" in upgrade_ddl
        assert "actual_transport" in upgrade_ddl
        assert "ck_log_entries_actual_transport" in upgrade_ddl
        assert "turn_scope is not null" in upgrade_ddl
        assert "turn_scope = 'source'" in upgrade_ddl
        assert "request_reason" in upgrade_ddl
        assert "request_protocol_version" in upgrade_ddl
        assert "request_output_hash" in upgrade_ddl
        assert "uq_cap_inv_task_output_log" in upgrade_ddl
        assert "request_protocol_version >= 1" in upgrade_ddl
        assert "create table worker_task_termination_receipts" in upgrade_ddl
        assert "ck_worker_task_term_active_slot" in upgrade_ddl
        assert "ck_worker_task_term_ack_intent" in upgrade_ddl
        assert "ack_intent_at" in upgrade_ddl
        upgrade_lock_sql = (
            "lock table tasks, log_entries, capability_invocations "
            "in access exclusive mode"
        )
        assert upgrade_ddl.index(upgrade_lock_sql) < upgrade_ddl.index(
            "alter table"
        )

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name="postgresql",
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        assert "drop column turn_source_log_id" in downgrade_ddl
        assert "drop column turn_scope" in downgrade_ddl
        assert "drop column actual_transport" in downgrade_ddl
        assert "drop column request_reason" in downgrade_ddl
        assert "drop column request_protocol_version" in downgrade_ddl
        assert "drop column request_output_hash" in downgrade_ddl
        assert "drop constraint uq_cap_inv_task_output_log" in downgrade_ddl
        assert "drop table worker_task_termination_receipts" in downgrade_ddl
        guard_index = downgrade_ddl.index("do $ccm_terminal_arbitration$")
        downgrade_lock_sql = (
            "lock table tasks, log_entries, capability_invocations, "
            "worker_task_termination_receipts in access exclusive mode"
        )
        assert downgrade_ddl.index(downgrade_lock_sql) < guard_index
        assert guard_index < downgrade_ddl.index("alter table")
        assert "where source = 'agent_request'" in downgrade_ddl
        assert "where turn_source_log_id is not null" in downgrade_ddl
        assert (
            "where turn_scope is not null or actual_transport is not null"
            in downgrade_ddl
        )
        assert "select 1 from worker_task_termination_receipts" in downgrade_ddl
        assert downgrade_ddl.count("raise exception") == 4

    @pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
    def test_terminal_arbitration_mysql_offline_is_refused(self, direction):
        module = _load_terminal_arbitration_migration(
            f"mysql_offline_{direction}"
        )

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="mysql",
            opts={"as_sql": True, "output_buffer": output},
        )
        with (
            patch.object(module, "op", Operations(context)),
            pytest.raises(RuntimeError, match="refuses MySQL offline SQL"),
        ):
            getattr(module, direction)()
        assert output.getvalue() == ""

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_delivery_migration_compiles_offline(self, dialect_name):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "9e5b2a7c4d10_add_delivery_loop_state.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"delivery_migration_for_{dialect_name}", migration_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        upgrade_output = io.StringIO()
        upgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": upgrade_output},
        )
        with patch.object(module, "op", Operations(upgrade_context)):
            module.upgrade()
        upgrade_ddl = upgrade_output.getvalue().lower()
        assert "create table delivery_runs" in upgrade_ddl
        assert "create table delivery_transitions" in upgrade_ddl
        assert "ck_tasks_delivery_owner_shape" in upgrade_ddl
        assert "uq_plan_agent_runs_capability_execution" in upgrade_ddl
        assert "uq_worktrees_delivery_run" in upgrade_ddl
        assert "idempotency_key varchar(191)" in upgrade_ddl

        downgrade_output = io.StringIO()
        downgrade_context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": downgrade_output},
        )
        with patch.object(module, "op", Operations(downgrade_context)):
            with pytest.raises(RuntimeError, match="Offline downgrade"):
                module.downgrade()
            # Compile the destructive statements separately only to prove the
            # dialect syntax. Production offline downgrade remains refused.
            with patch.object(
                module,
                "_assert_delivery_history_empty",
                return_value=None,
            ):
                module.downgrade()
        downgrade_ddl = downgrade_output.getvalue().lower()
        assert "drop table delivery_runs" in downgrade_ddl
        assert "drop column delivery_run_id" in downgrade_ddl
        assert "uq_plan_agent_runs_capability_execution" in downgrade_ddl

    def test_delivery_schema_constraints_and_indexes_match_orm(self, tmp_path):
        db_path = str(tmp_path / "delivery-schema.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, DELIVERY_LOOP_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        inspector = inspect(engine)

        for table_name in (
            "delivery_runs",
            "delivery_cycles",
            "delivery_turns",
            "delivery_events",
            "delivery_actions",
            "delivery_transitions",
        ):
            table = Base.metadata.tables[table_name]

            expected_indexes = {
                (index.name, tuple(column.name for column in index.columns))
                for index in table.indexes
            }
            actual_indexes = {
                (index["name"], tuple(index["column_names"]))
                for index in inspector.get_indexes(table_name)
            }
            assert actual_indexes == expected_indexes

            expected_uniques = {
                (
                    constraint.name,
                    tuple(column.name for column in constraint.columns),
                )
                for constraint in table.constraints
                if isinstance(constraint, UniqueConstraint)
            }
            actual_uniques = {
                (constraint["name"], tuple(constraint["column_names"]))
                for constraint in inspector.get_unique_constraints(table_name)
            }
            assert actual_uniques == expected_uniques

            expected_checks = {
                constraint.name
                for constraint in table.constraints
                if isinstance(constraint, CheckConstraint)
            }
            actual_checks = {
                constraint["name"]
                for constraint in inspector.get_check_constraints(table_name)
            }
            assert actual_checks == expected_checks

            expected_foreign_keys = {
                (
                    tuple(element.parent.name for element in constraint.elements),
                    constraint.elements[0].column.table.name,
                    tuple(element.column.name for element in constraint.elements),
                    constraint.ondelete,
                )
                for constraint in table.constraints
                if isinstance(constraint, ForeignKeyConstraint)
            }
            actual_foreign_keys = {
                (
                    tuple(constraint["constrained_columns"]),
                    constraint["referred_table"],
                    tuple(constraint["referred_columns"]),
                    (constraint.get("options") or {}).get("ondelete"),
                )
                for constraint in inspector.get_foreign_keys(table_name)
            }
            assert actual_foreign_keys == expected_foreign_keys

        task_checks = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("tasks")
        }
        assert "ck_tasks_delivery_owner_shape" in task_checks
        worktree_checks = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("worktrees")
        }
        assert "ck_worktrees_cleanup_status" in worktree_checks
        worktree_uniques = {
            constraint["name"]
            for constraint in inspector.get_unique_constraints("worktrees")
        }
        assert "uq_worktrees_delivery_run" in worktree_uniques
        plan_run_uniques = {
            constraint["name"]
            for constraint in inspector.get_unique_constraints("plan_agent_runs")
        }
        assert "uq_plan_agent_runs_capability_execution" in plan_run_uniques
        delivery_action_columns = {
            column["name"]: column
            for column in inspector.get_columns("delivery_actions")
        }
        assert delivery_action_columns["idempotency_key"]["type"].length == 191
        engine.dispose()

    @pytest.mark.parametrize("dialect_name", ("postgresql", "mysql"))
    def test_capability_migration_compiles_offline(self, dialect_name):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "6a4c2e9f1b73_add_capability_core.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"capability_migration_for_{dialect_name}", migration_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name=dialect_name,
            opts={"as_sql": True, "output_buffer": output},
        )
        with patch.object(module, "op", Operations(context)):
            module.upgrade()
        ddl = output.getvalue().lower()
        assert "create table capability_invocations" in ddl
        assert "create table capability_executions" in ddl
        assert "ck_cap_inv_active_slot" in ddl
        assert "ck_cap_exec_active_slot" in ddl

    def test_migrated_schema_matches_orm(self, tmp_path):
        """Compare columns from Alembic-migrated DB vs ORM metadata.create_all."""
        # DB 1: created by Alembic migrations
        alembic_path = str(tmp_path / "alembic.db")
        cfg = _alembic_cfg(alembic_path)
        _run_alembic(cfg, command.upgrade, "head")
        alembic_engine = create_engine(f"sqlite:///{alembic_path}")

        # DB 2: created by ORM metadata.create_all
        orm_path = str(tmp_path / "orm.db")
        orm_engine = create_engine(f"sqlite:///{orm_path}")
        Base.metadata.create_all(orm_engine)

        # Compare tables
        alembic_tables = _get_all_tables(alembic_engine)
        orm_tables = _get_all_tables(orm_engine)
        assert alembic_tables == orm_tables, (
            f"Table mismatch.\n"
            f"  Only in Alembic: {alembic_tables - orm_tables}\n"
            f"  Only in ORM: {orm_tables - alembic_tables}"
        )

        # Compare columns for each table
        for table in sorted(orm_tables):
            alembic_cols = set(_get_table_columns(alembic_engine, table).keys())
            orm_cols = set(_get_table_columns(orm_engine, table).keys())
            assert alembic_cols == orm_cols, (
                f"Column mismatch in table '{table}'.\n"
                f"  Only in Alembic: {alembic_cols - orm_cols}\n"
                f"  Only in ORM (missing migration!): {orm_cols - alembic_cols}"
            )

        alembic_engine.dispose()
        orm_engine.dispose()

    def test_no_pending_autogenerate_changes(self, tmp_path):
        """Alembic autogenerate should detect no new changes.

        This verifies that the migrations fully cover the ORM models.
        If this fails, run: alembic revision --autogenerate -m 'description'
        """
        from alembic.autogenerate import compare_metadata

        db_path = str(tmp_path / "autogen.db")
        cfg = _alembic_cfg(db_path)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            from alembic.migration import MigrationContext
            mc = MigrationContext.configure(conn)
            diffs = compare_metadata(mc, Base.metadata)

            # Filter out differences that are cosmetic for SQLite:
            # - index differences (SQLite doesn't preserve index info perfectly)
            # - nullable differences (SQLite doesn't enforce NOT NULL strictly,
            #   and initial migration used nullable=True for columns with defaults)
            significant_diffs = [
                d for d in diffs
                if not (isinstance(d, tuple) and d[0] in ("add_index", "remove_index"))
                and not (isinstance(d, list) and len(d) == 1 and isinstance(d[0], tuple)
                         and d[0][0] == "modify_nullable")
            ]

            assert len(significant_diffs) == 0, (
                "Alembic autogenerate found pending changes (need a new migration!):\n"
                + "\n".join(str(d) for d in significant_diffs)
            )

        engine.dispose()


class TestPublishedMigrationHistory:
    """Published main history stays intact and Plan v2 advances linearly."""

    def _assert_revision_schema(
        self,
        engine,
        *,
        revisions,
        plan_schema_present,
        snapshot_schema_present,
    ):
        tables = _get_all_tables(engine)
        task_columns = _get_table_columns(engine, "tasks")
        log_columns = _get_table_columns(engine, "log_entries")
        review_columns = _get_table_columns(engine, "pr_reviews")

        assert ("plan_agent_runs" in tables) is plan_schema_present
        assert ("plan_agent_steps" in tables) is plan_schema_present
        assert (
            "plan_target_task_id" in task_columns
        ) is plan_schema_present
        assert (
            "task_retry_count" in log_columns
        ) is snapshot_schema_present
        assert ("base_sha" in review_columns) is snapshot_schema_present

        with engine.connect() as conn:
            current_revisions = {
                row[0]
                for row in conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).fetchall()
            }
        assert current_revisions == set(revisions)

    def test_migration_graph_has_one_linear_head_after_plan_v2(self, tmp_path):
        cfg = _alembic_cfg(str(tmp_path / "graph.db"))
        script = ScriptDirectory.from_config(cfg)

        assert script.get_heads() == [CURRENT_HEAD_REVISION]
        assert script.get_current_head() == CURRENT_HEAD_REVISION
        assert (
            script.get_revision(TERMINAL_ARBITRATION_REVISION).down_revision
            == AUTO_CAPABILITY_TURN_REVISION
        )
        assert (
            script.get_revision(AUTO_CAPABILITY_TURN_REVISION).down_revision
            == DELIVERY_LOOP_REVISION
        )
        assert (
            script.get_revision(DELIVERY_LOOP_REVISION).down_revision
            == CODE_REVIEW_REVISION
        )
        assert (
            script.get_revision(CODE_REVIEW_REVISION).down_revision
            == CAPABILITY_CORE_REVISION
        )
        assert (
            script.get_revision(CAPABILITY_CORE_REVISION).down_revision
            == PLAN_V2_REVISION
        )
        assert (
            script.get_revision(PLAN_V2_REVISION).down_revision
            == ATTENTION_TAG_REVISION
        )
        graph_revision_ids = {
            migration.revision for migration in script.walk_revisions()
        }
        assert REMOVED_UNPUBLISHED_PLAN_REVISIONS.isdisjoint(graph_revision_ids)
        assert (
            script.get_revision(PR_FINDING_ACTIONS_REVISION).down_revision
            == PR_REVIEW_PANEL_REVISION
        )
        assert (
            script.get_revision(PR_REVIEW_PANEL_REVISION).down_revision
            == PUBLISHED_BRANCH_MERGE_REVISION
        )

    def test_published_plan_cleanup_migration_is_immutable(self):
        migration_path = (
            PROJECT_ROOT
            / "alembic"
            / "versions"
            / "f7a1c3d9e5b2_remove_reverted_plan_schema.py"
        )
        assert hashlib.sha256(migration_path.read_bytes()).hexdigest() == (
            PUBLISHED_PLAN_CLEANUP_SHA256
        )

    def test_deployed_main_head_upgrades_to_plan_v2_head(self, tmp_path):
        db_path = str(tmp_path / "main-to-plan-v2.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, ATTENTION_TAG_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plans" not in _get_all_tables(engine)
        assert "attention_tag" in _get_table_columns(engine, "tasks")
        engine.dispose()

        _run_alembic(cfg, command.upgrade, CURRENT_HEAD_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        assert "plans" in _get_all_tables(engine)
        task_columns = _get_table_columns(engine, "tasks")
        assert "attention_tag" in task_columns
        assert "plan_target_task_id" in task_columns
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == CURRENT_HEAD_REVISION
        engine.dispose()

    @pytest.mark.parametrize(
        ("start_revision", "plan_schema_present", "snapshot_schema_present"),
        [
            (PUBLISHED_PLAN_REVISION, True, False),
            (PLAN_CLEANUP_REVISION, False, False),
            (PR_REVIEW_SNAPSHOT_REVISION, False, True),
        ],
    )
    def test_each_published_branch_upgrades_to_merge_head(
        self,
        tmp_path,
        start_revision,
        plan_schema_present,
        snapshot_schema_present,
    ):
        db_path = str(tmp_path / f"published-{start_revision}.db")
        cfg = _alembic_cfg(db_path)

        # Each revision was a deployable branch head before the histories met.
        _run_alembic(cfg, command.upgrade, start_revision)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={start_revision},
            plan_schema_present=plan_schema_present,
            snapshot_schema_present=snapshot_schema_present,
        )
        engine.dispose()

        # The no-op merge applies the missing sibling branch and converges all
        # deployed states on one schema/head.
        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={PUBLISHED_BRANCH_MERGE_REVISION},
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()

    def test_merge_revision_downgrades_and_reupgrades(self, tmp_path):
        db_path = str(tmp_path / "merge-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        # Relative ``-1`` is ambiguous at a mergepoint, so select either
        # published parent explicitly; Alembic retains the sibling head.
        _run_alembic(cfg, command.downgrade, PLAN_CLEANUP_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={
                PLAN_CLEANUP_REVISION,
                PR_REVIEW_SNAPSHOT_REVISION,
            },
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={PUBLISHED_BRANCH_MERGE_REVISION},
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()

    def test_reverted_plan_cleanup_downgrades_and_reupgrades(self, tmp_path):
        db_path = str(tmp_path / "plan-cleanup-roundtrip.db")
        cfg = _alembic_cfg(db_path)

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        _run_alembic(cfg, command.downgrade, PUBLISHED_PLAN_REVISION)

        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={
                PUBLISHED_PLAN_REVISION,
                PR_REVIEW_SNAPSHOT_REVISION,
            },
            plan_schema_present=True,
            snapshot_schema_present=True,
        )
        engine.dispose()

        _run_alembic(cfg, command.upgrade, PUBLISHED_BRANCH_MERGE_REVISION)
        engine = create_engine(f"sqlite:///{db_path}")
        self._assert_revision_schema(
            engine,
            revisions={PUBLISHED_BRANCH_MERGE_REVISION},
            plan_schema_present=False,
            snapshot_schema_present=True,
        )
        engine.dispose()


class TestInitDbLogic:
    """Test the init_db() branching logic from database.py."""

    def test_init_db_fresh_database(self, tmp_path):
        """Fresh DB (no tables): upgrade head creates everything."""
        db_path = str(tmp_path / "fresh_init.db")

        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)
        tables = insp.get_table_names()
        has_tables = "tasks" in tables
        has_alembic = "alembic_version" in tables
        engine.dispose()

        assert not has_tables
        assert not has_alembic

        cfg = _alembic_cfg(db_path)
        # Same logic as init_db: else branch (fresh install)
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        assert "tasks" in _get_all_tables(engine)
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        engine.dispose()

    def test_init_db_legacy_database(self, tmp_path):
        """Legacy DB (has tables, no alembic_version): stamp initial + upgrade."""
        db_path = str(tmp_path / "legacy_init.db")
        _create_legacy_db(db_path)

        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)
        has_tasks = "tasks" in insp.get_table_names()
        has_alembic = "alembic_version" in insp.get_table_names()
        engine.dispose()

        assert has_tasks
        assert not has_alembic

        cfg = _alembic_cfg(db_path)
        # Same logic as init_db: stamp initial, then upgrade
        _run_alembic(cfg, command.stamp, "6b3f8a1c2d9e")
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        task_cols = _get_table_columns(engine, "tasks")
        assert "todo_file_path" in task_cols
        assert "loop_progress" in task_cols
        engine.dispose()

    def test_init_db_already_tracked(self, tmp_path):
        """Already tracked DB: upgrade head is no-op."""
        db_path = str(tmp_path / "tracked_init.db")
        cfg = _alembic_cfg(db_path)

        # First run creates everything
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)
        has_tasks = "tasks" in insp.get_table_names()
        has_alembic = "alembic_version" in insp.get_table_names()
        engine.dispose()

        assert has_tasks
        assert has_alembic

        # Second run is no-op
        _run_alembic(cfg, command.upgrade, "head")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == _get_head_revision(cfg)
        engine.dispose()
