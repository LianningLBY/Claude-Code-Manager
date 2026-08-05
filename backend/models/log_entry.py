import json
from datetime import datetime

from sqlalchemy import Integer, String, Text, DateTime, Boolean, Index
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class LogEntry(Base):
    __tablename__ = "log_entries"
    __table_args__ = (
        # Speeds up chat history query: filter by task_id, order/limit by id
        Index("ix_log_entries_task_id_id", "task_id", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # nullable：Worker 上执行的远程 task 的日志副本没有本地 instance
    instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # Exact Task retry generation that produced this event. Legacy and
    # non-generation-scoped rows remain NULL and must not be used as terminal
    # evidence for generation-sensitive workflows such as PR Monitor.
    task_retry_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    event_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    role: Mapped[str | None] = mapped_column(String(20), nullable=True)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    tool_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    tool_input: Mapped[str | None] = mapped_column(Text, nullable=True)
    tool_output: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_error: Mapped[bool] = mapped_column(Boolean, default=False)
    loop_iteration: Mapped[int | None] = mapped_column(Integer, nullable=True)  # loop tasks: which iteration produced this entry
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    @property
    def item_id(self) -> str | None:
        """Extract the public stream correlation id without exposing raw_json."""
        payload = self._raw_payload()
        if payload is None:
            return None
        item = payload.get("item")
        nested_id = item.get("id") if isinstance(item, dict) else None
        value = payload.get("item_id") or payload.get("itemId") or nested_id
        return str(value) if value not in (None, "") else None

    @property
    def turn_id(self) -> str | None:
        """Extract the native Codex turn id used as a safe fork boundary."""
        payload = self._raw_payload()
        if payload is None:
            return None
        turn = payload.get("turn")
        nested_id = turn.get("id") if isinstance(turn, dict) else None
        value = payload.get("turn_id") or payload.get("turnId") or nested_id
        return str(value) if value not in (None, "") else None

    def _raw_payload(self) -> dict | None:
        if not self.raw_json:
            return None
        try:
            payload = json.loads(self.raw_json)
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        return payload
