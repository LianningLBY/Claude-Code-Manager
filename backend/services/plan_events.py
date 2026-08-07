"""Canonical realtime events for the first-class Plan aggregate."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def broadcast_plan_event(
    *,
    event: str,
    plan_id: int,
    target_task_id: int | None = None,
    broadcaster=None,
    **payload,
) -> None:
    data = {"event": event, "plan_id": plan_id, **payload}
    try:
        selected_broadcaster = broadcaster
        if selected_broadcaster is None:
            from backend.main import broadcaster as selected_broadcaster

        if selected_broadcaster is None:
            return
        await selected_broadcaster.broadcast("plans", data)
        await selected_broadcaster.broadcast(f"plan:{plan_id}", data)
        if target_task_id is not None:
            await selected_broadcaster.broadcast(f"task:{target_task_id}", data)
    except Exception:
        # A committed Plan mutation must never be rolled back by notification
        # transport failure; snapshot refetch remains authoritative.
        logger.exception("Failed to broadcast %s for Plan %s", event, plan_id)
