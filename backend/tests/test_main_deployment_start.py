import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.services.deployment_start_guard import StartDecision
from backend.services.deployment_start_guard import assess_deployment_start


def test_prepare_deployment_start_recovers_before_return(monkeypatch):
    import backend.main as main

    recovered = MagicMock()
    monkeypatch.setattr(
        main,
        "assess_deployment_start",
        lambda *args, **kwargs: StartDecision(
            "skip_mutations", "controlled handoff"
        ),
    )
    monkeypatch.setattr(
        main.update_service, "recover_from_status_file", recovered
    )

    decision = main._prepare_deployment_start()

    assert decision.skip_mutations is True
    recovered.assert_called_once_with()


def test_prepare_deployment_start_fails_closed_before_recovery(monkeypatch):
    import backend.main as main

    recovered = MagicMock()
    monkeypatch.setattr(
        main,
        "assess_deployment_start",
        lambda *args, **kwargs: StartDecision("block", "unsafe phase"),
    )
    monkeypatch.setattr(
        main.update_service, "recover_from_status_file", recovered
    )

    with pytest.raises(RuntimeError, match="unsafe phase"):
        main._prepare_deployment_start()

    recovered.assert_not_called()


@pytest.mark.asyncio
async def test_incomplete_deployment_starts_maintenance_only_without_db(
    monkeypatch,
):
    import backend.main as main

    init_db = AsyncMock()
    dispatcher_start = AsyncMock()
    monkeypatch.setattr(
        main,
        "_prepare_deployment_start",
        lambda: StartDecision(
            "skip_mutations",
            "partial migration",
            maintenance_only=True,
        ),
    )
    monkeypatch.setattr(main, "init_db", init_db)
    monkeypatch.setattr(main.dispatcher, "start", dispatcher_start)
    monkeypatch.setattr(main.update_service, "maintenance_only", False)
    app = SimpleNamespace(state=SimpleNamespace())

    async with main.lifespan(app):
        assert app.state.deployment_maintenance_only is True

    init_db.assert_not_awaited()
    dispatcher_start.assert_not_awaited()


@pytest.mark.asyncio
async def test_incomplete_controlled_handoff_only_starts_recovery_app(
    monkeypatch, tmp_path,
):
    import backend.main as main

    commit = "c" * 40
    lease = tmp_path / "backups" / "deployment-lease.json"
    lease.parent.mkdir(parents=True)
    lease.write_text(
        json.dumps(
            {
                "status": "starting",
                "port": 8000,
                "handoff": True,
                "owner_token": "token",
                "expected_commit": commit,
                "terminal_intent": "rolled_back",
                "deployment_incomplete": True,
            }
        )
    )
    decision = assess_deployment_start(
        tmp_path, port=8000, running_commit=commit
    )
    assert decision.maintenance_only is True

    init_db = AsyncMock()
    dispatcher_start = AsyncMock()
    monkeypatch.setattr(
        main, "_prepare_deployment_start", lambda: decision
    )
    monkeypatch.setattr(main, "init_db", init_db)
    monkeypatch.setattr(main.dispatcher, "start", dispatcher_start)
    monkeypatch.setattr(main.update_service, "maintenance_only", False)
    app = SimpleNamespace(state=SimpleNamespace())

    async with main.lifespan(app):
        assert app.state.deployment_maintenance_only is True
        assert main.update_service.maintenance_only is True

    init_db.assert_not_awaited()
    dispatcher_start.assert_not_awaited()
