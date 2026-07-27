"""Backup service: wraps auto-backup SDK to periodically back up the SQLite database."""
import logging
import shutil
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import Optional

from sqlalchemy.engine import make_url

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class BackupService:
    """Schedules periodic database backups using the auto-backup SDK.

    Supports local filesystem, AWS S3, and Alibaba Cloud OSS destinations.
    The backup is only started when a valid destination is configured.

    Args:
        db_path: SQLAlchemy database URL (e.g. ``sqlite+aiosqlite:///./claude_manager.db``).
        backup_type: Destination type — ``"local"``, ``"s3"``, or ``"oss"``.
        interval_seconds: How often to run a backup (default 3600).
        max_copies: How many backup copies to keep per destination (default 10).
        destination_path: Local directory path (required when *backup_type* is ``"local"``).
        temp_dir: Custom directory for temporary archive files (avoids filling /tmp).
        s3_bucket / s3_region / s3_access_key / s3_secret_key: S3 credentials.
        oss_endpoint / oss_bucket / oss_access_key / oss_secret_key: OSS credentials.
        _auto_backup_cls: Injectable AutoBackup class (for testing).
    """

    def __init__(
        self,
        db_path: str,
        backup_type: str = "local",
        interval_seconds: int = 3600,
        max_copies: int = 10,
        destination_path: str = "",
        temp_dir: str = "",
        s3_bucket: str = "",
        s3_region: str = "",
        s3_access_key: str = "",
        s3_secret_key: str = "",
        oss_endpoint: str = "",
        oss_bucket: str = "",
        oss_access_key: str = "",
        oss_secret_key: str = "",
        _auto_backup_cls=None,
    ):
        self._db_path = db_path
        self._backup_type = backup_type
        self._interval_seconds = interval_seconds
        self._max_copies = max_copies
        self._destination_path = destination_path
        self._temp_dir = temp_dir
        self._s3_bucket = s3_bucket
        self._s3_region = s3_region
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key
        self._oss_endpoint = oss_endpoint
        self._oss_bucket = oss_bucket
        self._oss_access_key = oss_access_key
        self._oss_secret_key = oss_secret_key
        self._auto_backup_cls = _auto_backup_cls
        self._backup = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def _build_destination(self) -> Optional[dict]:
        t = self._backup_type
        if t == "local":
            if not self._destination_path:
                return None
            resolved = str(Path(self._destination_path).expanduser().resolve())
            return {"type": "local", "path": resolved}
        elif t == "s3":
            if not self._s3_bucket:
                return None
            return {
                "type": "s3",
                "bucket": self._s3_bucket,
                "region": self._s3_region,
                "access_key": self._s3_access_key,
                "secret_key": self._s3_secret_key,
            }
        elif t == "oss":
            if not self._oss_endpoint or not self._oss_bucket:
                return None
            return {
                "type": "oss",
                "endpoint": self._oss_endpoint,
                "bucket": self._oss_bucket,
                "access_key": self._oss_access_key,
                "secret_key": self._oss_secret_key,
            }
        logger.warning(f"Unknown backup type: {t!r}")
        return None

    def _resolve_db_path(self) -> str:
        url = make_url(self._db_path)
        if not url.drivername.startswith("sqlite"):
            raise ValueError(
                "The built-in backup service only supports SQLite database files"
            )
        raw = url.database
        if not raw or raw == ":memory:":
            raise ValueError("In-memory SQLite databases cannot be backed up")
        path = Path(raw).expanduser()
        if not path.is_absolute():
            # Keep this identical to backend.database: relative SQLite URLs are
            # anchored at the repository root, not the systemd working dir.
            path = _PROJECT_ROOT / path
        return str(path.resolve())

    def start(self) -> bool:
        """Start the background backup scheduler. Returns True if started."""
        destination = self._build_destination()
        if destination is None:
            logger.info(
                "Backup destination not fully configured (backup_type=%r), skipping backup service",
                self._backup_type,
            )
            return False

        cls = self._auto_backup_cls
        if cls is None:
            from auto_backup import AutoBackup  # noqa: PLC0415 — lazy import
            cls = AutoBackup

        try:
            db_file = self._resolve_db_path()
        except (TypeError, ValueError) as exc:
            logger.error("Backup service not started: %s", exc)
            return False
        tmp_dir = str(Path(self._temp_dir).expanduser().resolve()) if self._temp_dir else None
        if tmp_dir:
            Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        self._backup = cls(tmp_base_dir=tmp_dir)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._backup_loop,
            args=(db_file, destination, tmp_dir),
            name="ccm-sqlite-backup",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Backup service started (type=%r, db=%r, interval=%ds)",
            self._backup_type,
            db_file,
            self._interval_seconds,
        )
        return True

    @staticmethod
    def _create_sqlite_snapshot(db_file: str, snapshot_file: str) -> None:
        """Create a transactionally consistent SQLite snapshot, including WAL."""
        source = sqlite3.connect(
            f"{Path(db_file).resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=30,
        )
        destination = sqlite3.connect(snapshot_file)
        try:
            source.backup(destination)
        finally:
            destination.close()
            source.close()

    def _run_backup_once(
        self,
        db_file: str,
        destination: dict,
        tmp_dir: str | None,
    ) -> None:
        snapshot_dir = tempfile.mkdtemp(
            prefix="ccm-sqlite-snapshot-",
            dir=tmp_dir,
        )
        try:
            snapshot_file = str(Path(snapshot_dir) / Path(db_file).name)
            self._create_sqlite_snapshot(db_file, snapshot_file)
            results = self._backup.run_once(
                source_paths=[snapshot_file],
                destinations=[destination],
                task_name="claude-manager-db",
                max_copies=self._max_copies,
                tmp_base_dir=tmp_dir,
            )
            failed = [
                result
                for result in (results or [])
                if result.get("status") != "success"
            ]
            if failed:
                logger.error("SQLite backup destination failed: %s", failed)
        finally:
            shutil.rmtree(snapshot_dir, ignore_errors=True)

    def _backup_loop(
        self,
        db_file: str,
        destination: dict,
        tmp_dir: str | None,
    ) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_backup_once(db_file, destination, tmp_dir)
            except Exception:
                logger.exception("SQLite backup attempt failed")
            if self._stop_event.wait(max(1, self._interval_seconds)):
                break

    def stop(self):
        """Stop the background backup scheduler."""
        if self._backup is None:
            return
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=30)
            if thread.is_alive():
                logger.error(
                    "SQLite backup worker did not stop within 30 seconds"
                )
                return
        self._thread = None
        self._backup = None
        logger.info("Backup service stopped")
