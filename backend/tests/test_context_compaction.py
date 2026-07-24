"""Provider-aware context-window compaction helpers."""

import json

from backend.services.context_compaction import (
    context_tokens_used,
    is_context_window_exceeded,
    read_codex_rollout_last_usage,
)


def test_codex_context_limit_classifier_accepts_protocol_code_and_cli_text():
    assert is_context_window_exceeded(
        "codex",
        {"codexErrorInfo": "contextWindowExceeded"},
    )
    assert is_context_window_exceeded(
        "codex",
        "stream disconnected: Your input exceeds the context window of this model.",
    )
    assert is_context_window_exceeded("codex", "context_length_exceeded")


def test_context_limit_classifier_keeps_benign_window_metadata_out():
    assert not is_context_window_exceeded(
        "codex",
        {"modelContextWindow": 258_400, "message": "turn completed"},
    )
    assert not is_context_window_exceeded("codex", "usage limit exceeded")


def test_codex_context_tokens_use_protocol_value_with_safe_legacy_fallback():
    assert context_tokens_used(
        "codex",
        {
            "input_tokens": 100_000,
            "cache_read_input_tokens": 80_000,
            "output_tokens": 20_000,
            "context_tokens": 205_000,
        },
    ) == 205_000
    assert context_tokens_used(
        "codex",
        {
            "input_tokens": 20_000,
            "cache_read_input_tokens": 170_000,
            "output_tokens": 15_000,
        },
    ) == 205_000


def test_claude_context_tokens_keep_existing_input_only_semantics():
    assert context_tokens_used(
        "claude",
        {
            "input_tokens": 20_000,
            "cache_read_input_tokens": 170_000,
            "cache_creation_input_tokens": 5_000,
            "output_tokens": 15_000,
        },
    ) == 195_000


def test_rollout_usage_uses_last_request_instead_of_cumulative_total(tmp_path):
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("\n".join((
        json.dumps({"type": "response_item", "payload": {"type": "message"}}),
        json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 1_505_114,
                        "cached_input_tokens": 1_300_000,
                        "output_tokens": 50_000,
                        "reasoning_output_tokens": 20_000,
                        "total_tokens": 1_555_114,
                    },
                    "last_token_usage": {
                        "input_tokens": 210_000,
                        "cached_input_tokens": 180_000,
                        "output_tokens": 8_000,
                        "reasoning_output_tokens": 2_000,
                        "total_tokens": 218_000,
                    },
                    "model_context_window": 258_400,
                },
            },
        }),
    )) + "\n")

    assert read_codex_rollout_last_usage(rollout) == {
        "input_tokens": 210_000,
        "cached_input_tokens": 180_000,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 2_000,
        "total_tokens": 218_000,
        "context_window": 258_400,
    }
