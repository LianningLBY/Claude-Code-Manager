import json
import logging
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
    recover_publications = AsyncMock()
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
    monkeypatch.setattr(
        main,
        "_recover_pending_pr_review_publications",
        recover_publications,
    )
    monkeypatch.setattr(main.update_service, "maintenance_only", False)
    app = SimpleNamespace(state=SimpleNamespace())

    async with main.lifespan(app):
        assert app.state.deployment_maintenance_only is True

    init_db.assert_not_awaited()
    dispatcher_start.assert_not_awaited()
    recover_publications.assert_not_awaited()


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
    lease.chmod(0o600)
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


@pytest.mark.asyncio
async def test_pr_review_publication_recovery_uses_runtime_db_factory(
    monkeypatch,
):
    import backend.main as main
    from backend.services import pr_review_service

    recover = AsyncMock()
    monkeypatch.setattr(
        pr_review_service,
        "recover_incomplete_pr_reviews",
        recover,
        raising=False,
    )

    await main._recover_pending_pr_review_publications()

    recover.assert_awaited_once_with(main.async_session)


@pytest.mark.asyncio
async def test_pr_review_publication_recovery_failure_does_not_block_startup(
    monkeypatch,
    caplog,
):
    import backend.main as main
    from backend.services import pr_review_service

    monkeypatch.setattr(
        pr_review_service,
        "recover_incomplete_pr_reviews",
        AsyncMock(side_effect=RuntimeError("GitHub unavailable")),
        raising=False,
    )

    with caplog.at_level(logging.ERROR):
        await main._recover_pending_pr_review_publications()

    assert "Incomplete PR review recovery pass failed" in caplog.text
