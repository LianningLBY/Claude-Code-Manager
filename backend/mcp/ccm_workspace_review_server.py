"""Task-scoped high-level tools for testing the current Git workspace."""

from __future__ import annotations

import argparse
import json
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP


_TASK_ID = 0
_API_BASE = "http://localhost:8000"
_AUTH_TOKEN = ""

mcp = FastMCP(
    "ccm_workspace_review",
    instructions=(
        "Use these tools when the user asks to test the current trusted workspace, "
        "worktree, uncommitted frontend changes, or the feature just developed. "
        "CCM prepares the preview URL and assigns a separate black-box Browser Agent. "
        "PR/ref execution is fail-closed until an untrusted-code sandbox is available."
    ),
)


def _headers() -> dict[str, str]:
    return (
        {"Authorization": f"Bearer {_AUTH_TOKEN}"}
        if _AUTH_TOKEN
        else {}
    )


def _url(path: str) -> str:
    return f"{_API_BASE}/api/tasks/{_TASK_ID}/test-runs{path}"


async def _request(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.request(
            method,
            _url(path),
            headers=_headers(),
            json=payload,
        )
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = response.text
        raise RuntimeError(str(detail or f"CCM returned HTTP {response.status_code}"))
    return response.json()


@mcp.tool(structured_output=False)
async def workspace_review_capabilities() -> str:
    """Check whether this Task has a trusted current-workspace Preview profile."""

    result = await _request("GET", "/capabilities")
    return json.dumps(result, ensure_ascii=False)


@mcp.tool(structured_output=False)
async def test_current_changes(
    goal: str,
    mode: str = "review_only",
    profile: str = "standard",
    allow_actions: bool = True,
    browser_channel: str = "chrome",
    viewport_width: int = 1440,
    viewport_height: int = 900,
    provider: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    codex_service_tier: str | None = None,
) -> str:
    """Test this Task's exact branch and uncommitted changes in a fresh preview.

    Do not ask the user for a URL. CCM fingerprints the current worktree,
    starts its trusted isolated Preview profile, and creates a separate
    black-box Browser Agent. Omit provider/model/reasoning_effort to use the
    Browser Review configuration saved in CCM; it is independent from the
    parent Task. Use review_only unless the Task is already in an explicit
    fix-and-retest Goal. Poll the returned run with
    check_current_changes_review; do not claim success until it is completed,
    not stale, and contains a report.
    """

    if mode != "review_only":
        raise RuntimeError(
            "The Test Harness executes one black-box run. Code changes and repetition belong to the parent Goal/Loop."
        )
    result = await _request(
        "POST",
        "/internal/start",
        {
            "target_kind": "current_workspace",
            "target": {},
            "goal": goal,
            "profile": profile,
            "allow_actions": allow_actions,
            "browser_channel": browser_channel,
            "viewport_width": viewport_width,
            "viewport_height": viewport_height,
            "provider": provider,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "codex_service_tier": codex_service_tier,
        },
    )
    return json.dumps(result, ensure_ascii=False)


@mcp.tool(structured_output=False)
async def check_current_changes_review(run_id: str) -> str:
    """Return preview, Browser Agent, report, cleanup, and staleness state."""

    result = await _request("GET", f"/{run_id}/internal/status")
    return json.dumps(result, ensure_ascii=False)


@mcp.tool(structured_output=False)
async def stop_current_changes_review(
    run_id: str,
    reason: str = "Stopped by the parent Task",
) -> str:
    """Stop the isolated Browser Agent and clean up its preview processes."""

    _ = reason  # Kept in the tool contract for an auditable model decision.
    result = await _request("POST", f"/{run_id}/internal/stop")
    return json.dumps(result, ensure_ascii=False)


@mcp.tool(structured_output=False)
async def test_git_target(
    goal: str,
    target_kind: str,
    pr_number: int | None = None,
    git_ref: str | None = None,
    remote: str = "origin",
    fetch: bool = False,
    profile: str = "standard",
    allow_actions: bool = True,
    browser_channel: str = "chrome",
    viewport_width: int = 1440,
    viewport_height: int = 900,
    provider: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    codex_service_tier: str | None = None,
) -> str:
    """Report that untrusted PR/ref execution needs a sandbox.

    Omit the runtime fields to use the saved Browser Review configuration,
    which may intentionally differ from the parent Task.
    """

    _ = (
        goal,
        target_kind,
        pr_number,
        git_ref,
        remote,
        fetch,
        profile,
        allow_actions,
        browser_channel,
        viewport_width,
        viewport_height,
        provider,
        model,
        reasoning_effort,
        codex_service_tier,
    )
    from backend.services.test_harness_targets import UNTRUSTED_GIT_TARGETS_REASON

    raise RuntimeError(UNTRUSTED_GIT_TARGETS_REASON)


@mcp.tool(structured_output=False)
async def compare_test_runs(base_run_id: str, candidate_run_id: str) -> str:
    """Compare stable findings across two completed runs for the same Task."""

    result = await _request(
        "GET",
        f"/{base_run_id}/compare/{candidate_run_id}",
    )
    return json.dumps(result, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--auth-token", default="")
    args = parser.parse_args()
    if args.task_id <= 0:
        parser.error("--task-id must be positive")
    global _TASK_ID, _API_BASE, _AUTH_TOKEN
    _TASK_ID = args.task_id
    _API_BASE = args.api_base.rstrip("/")
    _AUTH_TOKEN = args.auth_token
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
