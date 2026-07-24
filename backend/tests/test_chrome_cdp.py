from __future__ import annotations

from pathlib import Path

import pytest

from scripts.chrome_cdp import (
    ChromeCdpError,
    dynamic_debugging_arguments,
    wait_for_owned_chrome_cdp,
)


class FakeProcess:
    returncode = None

    def poll(self):
        return self.returncode


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, urls: list[str], *, matching: bool = True):
        self.urls = urls
        self.matching = matching

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, url: str, **_kwargs):
        self.urls.append(url)
        if url.endswith("/json/version"):
            path = "/devtools/browser/owned" if self.matching else "/devtools/browser/orphan"
            return FakeResponse(
                {
                    "webSocketDebuggerUrl": (
                        f"ws://127.0.0.1:45678{path}"
                    ),
                },
            )
        return FakeResponse(
            [
                {
                    "type": "page",
                    "webSocketDebuggerUrl": (
                        "ws://127.0.0.1:45678/devtools/page/owned"
                    ),
                },
            ],
        )


async def _no_sleep(_seconds):
    return None


def _write_active_port(profile_dir: Path) -> None:
    path = profile_dir / "DevToolsActivePort"
    path.write_text(
        "45678\n/devtools/browser/owned\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_dynamic_debugging_arguments_never_use_configured_fixed_port(tmp_path):
    args = dynamic_debugging_arguments(tmp_path)

    assert "--remote-debugging-port=0" in args
    assert not any(arg == "--remote-debugging-port=9222" for arg in args)
    assert f"--user-data-dir={tmp_path}" in args


@pytest.mark.asyncio
async def test_orphan_on_configured_port_is_never_reused(monkeypatch, tmp_path):
    monkeypatch.setenv("CCM_LOGIN_CDP_PORT", "9222")
    _write_active_port(tmp_path)
    urls: list[str] = []

    result = await wait_for_owned_chrome_cdp(
        FakeProcess(),
        tmp_path,
        client_factory=lambda: FakeClient(urls),
        sleep=_no_sleep,
        attempts=1,
    )

    assert result.port == 45678
    assert urls == [
        "http://127.0.0.1:45678/json/version",
        "http://127.0.0.1:45678/json",
    ]
    assert all(":9222/" not in url for url in urls)


@pytest.mark.asyncio
async def test_profile_endpoint_must_match_devtools_browser_identity(tmp_path):
    _write_active_port(tmp_path)
    urls: list[str] = []

    with pytest.raises(ChromeCdpError, match="does not match"):
        await wait_for_owned_chrome_cdp(
            FakeProcess(),
            tmp_path,
            client_factory=lambda: FakeClient(urls, matching=False),
            sleep=_no_sleep,
            attempts=1,
        )
