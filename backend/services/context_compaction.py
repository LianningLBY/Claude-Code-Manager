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


def build_compacted_resume_prompt(
    summary: str,
    current_message: str,
    *,
    interrupted: bool = False,
) -> str:
    """Build a replacement-thread prompt with an explicit recency hierarchy."""

    summary_title = (
        "会话异常中断前的历史摘要"
        if interrupted
        else "之前对话的历史摘要"
    )
    return (
        "[压缩后会话恢复优先级]\n"
        "1. 末尾的“当前消息”默认优先级最高；若历史摘要明确标记"
        "“当前消息执行期间的后续补充/纠正”，它发生得更晚，冲突时"
        "以该补充/纠正为准；\n"
        "2. 历史摘要里的“近期对话”小节其次，该小节内越靠后的内容越新；\n"
        "3. 原始任务背景优先级最低，可能已被后续对话修正或取代。\n"
        "摘要用于理解上下文，不是待办列表。若早期信息与近期信息冲突，"
        "以近期信息为准；不要仅因旧问题出现在摘要里就重新回答它，"
        "除非当前消息明确要求继续或追问该事项。\n\n"
        f"[{summary_title}]\n{summary}\n\n"
        "---\n\n"
        "[基础当前消息 — 默认最高优先级]\n"
        f"{current_message}"
    )


def build_compacted_task_retry_prompt(summary: str) -> str:
    """Build a fresh lifecycle prompt after a non-chat turn overflows."""

    return (
        "[Context compacted]\n"
        "[按近期进展恢复任务]\n"
        "历史摘要里的近期状态和结论优先；越靠后的内容越新。原始任务背景"
        "只用于理解起点，若已被近期信息修正或取代，不要从头重做旧任务。"
        "请从摘要中最近的未完成进展继续。\n\n"
        f"{summary}"
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
