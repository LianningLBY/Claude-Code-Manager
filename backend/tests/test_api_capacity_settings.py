import pytest
from sqlalchemy import select

from backend.models.global_settings import GlobalSettings


@pytest.mark.asyncio
async def test_capacity_defaults_to_environment_and_persists_override(
    client,
    session_factory,
):
    from backend.config import settings
    from backend.main import dispatcher

    original_override = dispatcher._max_concurrent_instances_override
    try:
        dispatcher.configure_capacity_override(None)
        response = await client.get("/api/settings/capacity")
        assert response.status_code == 200
        assert response.json()["max_concurrent_instances"] == settings.max_concurrent_instances
        assert response.json()["configured_override"] is None

        response = await client.put(
            "/api/settings/capacity",
            json={"max_concurrent_instances": 3},
        )
        assert response.status_code == 200
        assert response.json()["max_concurrent_instances"] == 3
        assert dispatcher.max_concurrent_instances == 3

        async with session_factory() as db:
            row = await db.scalar(select(GlobalSettings).where(GlobalSettings.id == 1))
            assert row is not None
            assert row.max_concurrent_instances == 3

        response = await client.put(
            "/api/settings/capacity",
            json={"max_concurrent_instances": None},
        )
        assert response.status_code == 200
        assert response.json()["configured_override"] is None
        assert dispatcher.max_concurrent_instances == settings.max_concurrent_instances
    finally:
        dispatcher._max_concurrent_instances_override = original_override


@pytest.mark.asyncio
async def test_capacity_rejects_unsafe_values(client):
    for value in (0, -1, 65, 1.5):
        response = await client.put(
            "/api/settings/capacity",
            json={"max_concurrent_instances": value},
        )
        assert response.status_code == 422
