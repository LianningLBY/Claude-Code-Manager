"""Context-window usage and overflow classification shared by task paths."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import json
from pathlib import Path
from typing import Any


_CONTEXT_LIMIT_MARKERS = (
    "prompt is too long",
    "context window exceeded",
    "contextwindowexceeded",
    "context length exceeded",
    "context_length_exceeded",
    "exceeds the context window",
    "exceed the context window",
    "maximum context length",
    "maximum context window",
    "too many tokens for the model",
    "input is too long for the requested model",
)


def _text_fragments(value: Any) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, Mapping):
        for nested in value.values():
            yield from _text_fragments(nested)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for nested in value:
            yield from _text_fragments(nested)
        return
    yield str(value)


def is_context_window_exceeded(provider: str | None, *details: Any) -> bool:
    """Recognize provider text and Codex app-server's structured error code."""

    del provider  # Markers are intentionally valid for both supported CLIs.
    text = " ".join(
        fragment.strip().lower()
        for detail in details
        for fragment in _text_fragments(detail)
        if fragment.strip()
    )
    return any(marker in text for marker in _CONTEXT_LIMIT_MARKERS)


def context_tokens_used(provider: str | None, usage: Mapping[str, Any]) -> int:
    """Return the token count that should be compared with the model window.

    Codex reports ``context_tokens`` from its latest request. Older CCM rows do
    not have that field, so include the latest output in the fallback because
    it becomes part of the next request. Claude keeps its established
    input/cache accounting.
    """

    total_input = (
        int(usage.get("input_tokens") or 0)
        + int(usage.get("cache_read_input_tokens") or 0)
        + int(usage.get("cache_creation_input_tokens") or 0)
    )
    if (provider or "claude").lower() != "codex":
        return total_input
    reported = usage.get("context_tokens")
    if reported is not None:
        return max(int(reported), 0)
    return max(total_input + int(usage.get("output_tokens") or 0), 0)


def read_codex_rollout_last_usage(path: Path) -> dict[str, int] | None:
    """Read Codex's latest request usage, not its cumulative thread total."""

    from backend.services.codex_pool import _iter_rollout_lines_reverse

    try:
        lines = _iter_rollout_lines_reverse(path)
        for raw_line in lines:
            if b'"token_count"' not in raw_line:
                continue
            try:
                event = json.loads(raw_line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            payload = event.get("payload") if isinstance(event, dict) else None
            if not isinstance(payload, dict) or payload.get("type") != "token_count":
                continue
            info = payload.get("info")
            if not isinstance(info, dict):
                continue
            last = info.get("last_token_usage")
            if not isinstance(last, dict):
                continue
            return {
                "input_tokens": int(last.get("input_tokens") or 0),
                "cached_input_tokens": int(
                    last.get("cached_input_tokens") or 0
                ),
                "output_tokens": int(last.get("output_tokens") or 0),
                "reasoning_output_tokens": int(
                    last.get("reasoning_output_tokens") or 0
                ),
                "total_tokens": int(last.get("total_tokens") or 0),
                "context_window": int(
                    info.get("model_context_window") or 0
                ),
            }
    except OSError:
        return None
    return None
