from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _make_executable(path: Path, body: str) -> None:
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(0o700)


def _run_pre_start(
    tmp_path: Path,
    guard_rc: int,
    *,
    port_env: str | None = "8123",
):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy2(PROJECT_ROOT / "scripts" / "pre-start.sh", scripts)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls = tmp_path / "uv-calls"
    guard_calls = tmp_path / "guard-calls"

    _make_executable(fake_bin / "git", "echo " + "a" * 40 + "\n")
    _make_executable(
        fake_bin / "python3",
        f'echo "$*" >> {guard_calls}\nexit {guard_rc}\n',
    )
    _make_executable(
        fake_bin / "uv",
        f"echo \"$*\" >> {calls}\nexit 0\n",
    )
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "PYTHON3": str(fake_bin / "python3"),
        "UV": str(fake_bin / "uv"),
    }
    if port_env is not None:
        env["PORT"] = port_env
    else:
        env.pop("PORT", None)
    result = subprocess.run(
        ["/bin/bash", str(scripts / "pre-start.sh")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result, calls


def test_pre_start_skips_all_mutations_during_controlled_handoff(tmp_path):
    result, calls = _run_pre_start(tmp_path, 10)

    assert result.returncode == 0
    assert "跳过依赖同步与数据库迁移" in result.stdout
    assert not calls.exists()


def test_pre_start_fails_closed_when_guard_blocks(tmp_path):
    result, calls = _run_pre_start(tmp_path, 20)

    assert result.returncode == 20
    assert "启动保护拒绝" in result.stderr
    assert not calls.exists()


@pytest.mark.parametrize("guard_rc", [1, 127])
def test_pre_start_propagates_unexpected_guard_failure(
    tmp_path, guard_rc
):
    result, calls = _run_pre_start(tmp_path, guard_rc)

    assert result.returncode == guard_rc
    assert not calls.exists()


def test_pre_start_runs_normal_steps_only_after_guard_allows(tmp_path):
    result, calls = _run_pre_start(tmp_path, 0)

    assert result.returncode == 0
    assert calls.read_text().splitlines() == [
        "sync --quiet",
        "run alembic upgrade head",
    ]


def test_pre_start_reads_non_default_port_from_dotenv_without_sourcing(
    tmp_path,
):
    (tmp_path / ".env").write_text("PORT=8456\nUNRELATED=value\n")

    result, calls = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode == 0
    guard_args = (tmp_path / "guard-calls").read_text()
    assert "--port 8456" in guard_args
    assert not calls.exists()


def test_pre_start_explicit_port_wins_over_dotenv(tmp_path):
    (tmp_path / ".env").write_text("PORT=8456\n")

    result, _ = _run_pre_start(tmp_path, 10, port_env="8123")

    assert result.returncode == 0
    guard_args = (tmp_path / "guard-calls").read_text()
    assert "--port 8123" in guard_args
    assert "--port 8456" not in guard_args


@pytest.mark.parametrize(
    "dotenv",
    [
        "PORT=8003\nPORT=8004\n",
        "PORT=$(touch /tmp/should-not-run)\n",
        "PORT=70000\n",
    ],
)
def test_pre_start_fails_closed_when_dotenv_port_is_unsafe(
    tmp_path, dotenv
):
    (tmp_path / ".env").write_text(dotenv)

    result, calls = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode != 0
    assert not (tmp_path / "guard-calls").exists()
    assert not calls.exists()


@pytest.mark.parametrize(
    "dotenv",
    [
        'export PORT="8456" # deployment port\n',
        "PORT='8456'\n",
        "PORT=8456 # deployment port\n",
        "PORT=8456\nPORT='8456'\n",
    ],
)
def test_pre_start_compatibly_parses_inert_dotenv_port(
    tmp_path, dotenv
):
    (tmp_path / ".env").write_text(dotenv)

    result, _ = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode == 0
    assert "--port 8456" in (tmp_path / "guard-calls").read_text()


def test_pre_start_uses_historical_default_when_port_is_absent(tmp_path):
    (tmp_path / ".env").write_text("OTHER=value\n")

    result, _ = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode == 0
    assert "--port 8000" in (tmp_path / "guard-calls").read_text()


def test_pre_start_uses_historical_default_when_dotenv_is_missing(
    tmp_path,
):
    result, _ = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode == 0
    assert "--port 8000" in (tmp_path / "guard-calls").read_text()


def test_pre_start_authoritative_lease_port_precedes_dotenv(tmp_path):
    backups = tmp_path / "backups"
    backups.mkdir()
    (backups / "deployment-lease.json").write_text(
        '{"status":"starting","owner_token":"token","port":8543}'
    )
    (tmp_path / ".env").write_text("PORT=8003\n")

    result, _ = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode == 0
    guard_args = (tmp_path / "guard-calls").read_text()
    assert "--port 8543" in guard_args
    assert "--port 8003" not in guard_args


def test_pre_start_clean_terminal_lease_does_not_override_new_dotenv_port(
    tmp_path,
):
    backups = tmp_path / "backups"
    backups.mkdir()
    (backups / "deployment-lease.json").write_text(
        '{"status":"completed","owner_token":"old","port":8002,'
        '"deployment_incomplete":false}'
    )
    (tmp_path / ".env").write_text("PORT=8003\n")

    result, _ = _run_pre_start(tmp_path, 10, port_env=None)

    assert result.returncode == 0
    guard_args = (tmp_path / "guard-calls").read_text()
    assert "--port 8003" in guard_args
    assert "--port 8002" not in guard_args
