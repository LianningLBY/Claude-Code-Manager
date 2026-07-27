"""Tests for BackupService."""
import sqlite3
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from backend.services.backup_service import BackupService, _PROJECT_ROOT

_REAL_CREATE_SQLITE_SNAPSHOT = BackupService._create_sqlite_snapshot


@pytest.fixture(autouse=True)
def _isolate_scheduled_snapshots(monkeypatch):
    def create_test_snapshot(_db_file: str, snapshot_file: str) -> None:
        Path(snapshot_file).write_bytes(b"isolated sqlite snapshot")

    monkeypatch.setattr(
        BackupService,
        "_create_sqlite_snapshot",
        staticmethod(create_test_snapshot),
    )


def _wait_for_call(mock, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not mock.called and time.monotonic() < deadline:
        time.sleep(0.01)
    assert mock.called


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_svc(**overrides) -> BackupService:
    """Build a BackupService with sensible defaults and an injected mock class."""
    mock_cls = overrides.pop("_auto_backup_cls", MagicMock())
    defaults = dict(
        db_path="sqlite+aiosqlite:///./claude_manager.db",
        backup_type="local",
        interval_seconds=3600,
        max_copies=10,
        destination_path="/tmp/backup",
        _auto_backup_cls=mock_cls,
    )
    defaults.update(overrides)
    return BackupService(**defaults)


# ── _build_destination ────────────────────────────────────────────────────────


class TestBuildDestination:
    def test_local_ok(self):
        svc = _make_svc(backup_type="local", destination_path="/mnt/bak")
        dest = svc._build_destination()
        assert dest["type"] == "local"
        assert dest["path"] == "/mnt/bak"

    def test_local_empty_path_returns_none(self):
        svc = _make_svc(backup_type="local", destination_path="")
        assert svc._build_destination() is None

    def test_local_tilde_path_expanded(self):
        svc = _make_svc(backup_type="local", destination_path="~/backup/data")
        dest = svc._build_destination()
        assert dest is not None
        assert "~" not in dest["path"]
        assert dest["path"].startswith("/")

    def test_local_relative_path_resolved(self):
        svc = _make_svc(backup_type="local", destination_path="./backups")
        dest = svc._build_destination()
        assert dest["path"].startswith("/")

    def test_s3_ok(self):
        svc = _make_svc(
            backup_type="s3",
            s3_bucket="my-bucket",
            s3_region="us-east-1",
            s3_access_key="AKID",
            s3_secret_key="secret",
        )
        assert svc._build_destination() == {
            "type": "s3",
            "bucket": "my-bucket",
            "region": "us-east-1",
            "access_key": "AKID",
            "secret_key": "secret",
        }

    def test_s3_missing_bucket_returns_none(self):
        svc = _make_svc(backup_type="s3", s3_bucket="")
        assert svc._build_destination() is None

    def test_oss_ok(self):
        svc = _make_svc(
            backup_type="oss",
            oss_endpoint="oss-cn-hangzhou.aliyuncs.com",
            oss_bucket="my-bucket",
            oss_access_key="key",
            oss_secret_key="secret",
        )
        assert svc._build_destination() == {
            "type": "oss",
            "endpoint": "oss-cn-hangzhou.aliyuncs.com",
            "bucket": "my-bucket",
            "access_key": "key",
            "secret_key": "secret",
        }

    def test_oss_missing_endpoint_returns_none(self):
        svc = _make_svc(backup_type="oss", oss_endpoint="", oss_bucket="my-bucket")
        assert svc._build_destination() is None

    def test_oss_missing_bucket_returns_none(self):
        svc = _make_svc(backup_type="oss", oss_endpoint="oss-cn-hz.aliyuncs.com", oss_bucket="")
        assert svc._build_destination() is None

    def test_unknown_type_returns_none(self):
        svc = _make_svc(backup_type="gcs")
        assert svc._build_destination() is None


# ── _resolve_db_path ──────────────────────────────────────────────────────────


class TestResolveDbPath:
    def test_strips_async_prefix(self):
        svc = _make_svc(db_path="sqlite+aiosqlite:///./claude_manager.db")
        path = svc._resolve_db_path()
        assert "sqlite+aiosqlite" not in path
        assert path.endswith("claude_manager.db")
        assert path == str((_PROJECT_ROOT / "claude_manager.db").resolve())

    def test_strips_sync_prefix(self):
        svc = _make_svc(db_path="sqlite:///./mydb.db")
        path = svc._resolve_db_path()
        assert "sqlite" not in path
        assert path.endswith("mydb.db")

    def test_absolute_path_unchanged(self, tmp_path):
        db = str(tmp_path / "data.db")
        svc = _make_svc(db_path=f"sqlite+aiosqlite:///{db}")
        assert svc._resolve_db_path() == db

    @pytest.mark.parametrize(
        "url",
        [
            "postgresql+asyncpg://user:pass@db/ccm",
            "mysql+aiomysql://user:pass@db/ccm",
        ],
    )
    def test_rejects_external_database_urls(self, url):
        svc = _make_svc(db_path=url)
        with pytest.raises(ValueError, match="only supports SQLite"):
            svc._resolve_db_path()


# ── start ─────────────────────────────────────────────────────────────────────


class TestStart:
    def test_local_starts_scheduler(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(backup_type="local", destination_path="/tmp/bak", _auto_backup_cls=mock_cls)

        result = svc.start()

        assert result is True
        mock_cls.assert_called_once()
        _wait_for_call(mock_instance.run_once)
        kwargs = mock_instance.run_once.call_args.kwargs
        assert kwargs["max_copies"] == 10
        assert svc._interval_seconds == 3600
        svc.stop()

    def test_returns_false_when_destination_not_configured(self):
        mock_cls = MagicMock()
        svc = _make_svc(backup_type="local", destination_path="", _auto_backup_cls=mock_cls)

        result = svc.start()

        assert result is False
        mock_cls.assert_not_called()

    def test_external_database_does_not_start_fake_file_backup(self):
        mock_cls = MagicMock()
        svc = _make_svc(
            db_path="postgresql+asyncpg://user:pass@db/ccm",
            _auto_backup_cls=mock_cls,
        )

        assert svc.start() is False
        mock_cls.assert_not_called()

    def test_s3_passes_correct_destination(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(
            backup_type="s3",
            s3_bucket="bucket",
            s3_region="ap-east-1",
            s3_access_key="key",
            s3_secret_key="secret",
            _auto_backup_cls=mock_cls,
        )

        svc.start()

        _wait_for_call(mock_instance.run_once)
        destinations = mock_instance.run_once.call_args.kwargs["destinations"]
        assert destinations[0]["type"] == "s3"
        assert destinations[0]["bucket"] == "bucket"
        svc.stop()

    def test_oss_passes_correct_destination(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(
            backup_type="oss",
            oss_endpoint="oss-cn-hangzhou.aliyuncs.com",
            oss_bucket="bkt",
            oss_access_key="key",
            oss_secret_key="secret",
            _auto_backup_cls=mock_cls,
        )

        svc.start()

        _wait_for_call(mock_instance.run_once)
        destinations = mock_instance.run_once.call_args.kwargs["destinations"]
        assert destinations[0]["type"] == "oss"
        assert destinations[0]["endpoint"] == "oss-cn-hangzhou.aliyuncs.com"
        svc.stop()

    def test_custom_interval_and_max_copies(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(
            interval_seconds=7200,
            max_copies=5,
            _auto_backup_cls=mock_cls,
        )

        svc.start()

        _wait_for_call(mock_instance.run_once)
        kwargs = mock_instance.run_once.call_args.kwargs
        assert svc._interval_seconds == 7200
        assert kwargs["max_copies"] == 5
        svc.stop()

    def test_temp_dir_passed_to_auto_backup(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(
            temp_dir="~/cache/backup-tmp",
            _auto_backup_cls=mock_cls,
        )

        svc.start()

        init_kwargs = mock_cls.call_args.kwargs
        assert "tmp_base_dir" in init_kwargs
        assert "~" not in init_kwargs["tmp_base_dir"]
        assert init_kwargs["tmp_base_dir"].startswith("/")
        svc.stop()

    def test_no_temp_dir_passes_none(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(temp_dir="", _auto_backup_cls=mock_cls)

        svc.start()

        init_kwargs = mock_cls.call_args.kwargs
        assert init_kwargs.get("tmp_base_dir") is None
        svc.stop()

    def test_tilde_destination_expanded_in_destination_dict(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(
            backup_type="local",
            destination_path="~/backup/data",
            _auto_backup_cls=mock_cls,
        )

        svc.start()

        _wait_for_call(mock_instance.run_once)
        destinations = mock_instance.run_once.call_args.kwargs["destinations"]
        assert "~" not in destinations[0]["path"]
        assert destinations[0]["path"].startswith("/")
        svc.stop()


# ── stop ──────────────────────────────────────────────────────────────────────


class TestStop:
    def test_stop_calls_backup_stop(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(_auto_backup_cls=mock_cls)
        svc.start()

        svc.stop()

        assert svc._backup is None
        assert svc._thread is None

    def test_stop_without_start_is_safe(self):
        svc = _make_svc()
        svc.stop()  # must not raise

    def test_stop_idempotent(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        svc = _make_svc(_auto_backup_cls=mock_cls)
        svc.start()

        svc.stop()
        svc.stop()  # second call is safe

        assert svc._backup is None
        assert svc._thread is None


def test_sqlite_snapshot_includes_committed_wal_pages(tmp_path):
    source = tmp_path / "wal-source.db"
    snapshot = tmp_path / "snapshot.db"
    writer = sqlite3.connect(source)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE entries (value TEXT NOT NULL)")
        writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        writer.execute("INSERT INTO entries VALUES ('committed-in-wal')")
        writer.commit()
        assert Path(f"{source}-wal").stat().st_size > 0

        _REAL_CREATE_SQLITE_SNAPSHOT(str(source), str(snapshot))
    finally:
        writer.close()

    restored = sqlite3.connect(snapshot)
    try:
        assert restored.execute("SELECT value FROM entries").fetchall() == [
            ("committed-in-wal",)
        ]
        assert restored.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        restored.close()
