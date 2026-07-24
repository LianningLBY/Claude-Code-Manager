"""Guards against pytest mutating the developer's real CCM database."""

from pathlib import Path

import backend.database as database
import backend.main as main
from backend.config import settings


def test_process_wide_test_services_use_ephemeral_database():
    project_database = (
        Path(__file__).resolve().parents[2] / "claude_manager.db"
    ).resolve()
    global_database = Path(str(database.engine.url.database)).resolve()

    assert global_database != project_database
    assert "ccm-pytest-global-" in str(global_database)
    assert main.instance_manager.db_factory is database.async_session
    assert main.dispatcher.db_factory is database.async_session


def test_process_wide_services_use_ephemeral_external_state():
    project_root = Path(__file__).resolve().parents[2]
    isolated_root = Path(main.update_service.project_dir)

    assert isolated_root != project_root
    assert "ccm-pytest-global-" in str(isolated_root)
    assert "ccm-pytest-global-" in str(
        Path(settings.codex_pool_config_path)
    )
    assert "ccm-pytest-global-" in str(Path(settings.pool_config_path))
    assert settings.worker_enabled is False
    assert settings.pool_enabled is False
    assert settings.codex_pool_enabled is False
    assert settings.backup_enabled is False
    assert settings.auto_start_dispatcher is False
