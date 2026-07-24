#!/usr/bin/env bash
# systemd ExecStartPre：服务启动前自动同步依赖和数据库。
# 轻量设计：每步都先检测是否需要更新，无变化秒过。
set -uo pipefail
cd "$(dirname "$0")/.."

UV="${UV:-$HOME/.local/bin/uv}"
LOG_PREFIX="[pre-start]"
PROJECT_DIR="$(pwd -P)"
# The guard is stdlib-only and runs before dependency synchronization. Use the
# system interpreter by default so a stale/broken project venv cannot bypass
# the deployment fence; tests or unusual distributions may override PYTHON3.
PYTHON3="${PYTHON3:-/usr/bin/python3}"

# The deployment lease is keyed by the real listening port.  Resolution order:
# explicit environment (including systemd EnvironmentFile), authoritative safe
# repo lease, inert dotenv parsing, then the historical 8000 default.  Never
# source .env: compatible quotes/export/comments are parsed as text only.
if [ "${PORT+x}" = "x" ]; then
    RESOLVED_PORT="$PORT"
else
    if [ ! -x /usr/bin/python3 ]; then
        echo "$LOG_PREFIX 无法使用系统 Python 安全解析 .env PORT" >&2
        exit 1
    fi
    if ! RESOLVED_PORT="$(
        /usr/bin/python3 -I -S - "$PROJECT_DIR" <<'PY'
import json
import os
import re
import stat
import sys
from pathlib import Path

project = Path(sys.argv[1])
lease = project / "backups" / "deployment-lease.json"
try:
    lease_metadata = lease.lstat()
except FileNotFoundError:
    lease_metadata = None
if lease_metadata is not None:
    if (
        not stat.S_ISREG(lease_metadata.st_mode)
        or lease_metadata.st_uid != os.getuid()
        or lease_metadata.st_nlink != 1
        or lease_metadata.st_mode & 0o022
    ):
        raise SystemExit("deployment lease is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(lease, flags)
    with os.fdopen(descriptor) as stream:
        opened = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or opened.st_mode & 0o022
        ):
            raise SystemExit("opened deployment lease is unsafe")
        payload = json.load(stream)
    if not isinstance(payload, dict):
        raise SystemExit("deployment lease is not a JSON object")
    active_statuses = {
        "claimed", "running", "backing_up", "restarting", "starting",
        "stopping", "migrating", "rolling_back",
    }
    status = str(payload.get("status") or "")
    lease_requires_port = (
        status in active_statuses
        or bool(payload.get("deployment_incomplete"))
        or bool(payload.get("rollback_incomplete"))
        or status == "rollback_failed"
    )
    lease_port = payload.get("port")
    if lease_requires_port:
        if lease_port is None:
            raise SystemExit("active/incomplete deployment lease has no PORT")
        value = str(lease_port)
        if not value.isascii() or not value.isdigit():
            raise SystemExit("deployment lease PORT is invalid")
        print(value)
        raise SystemExit(0)

path = project / ".env"
try:
    dotenv_metadata = path.lstat()
except FileNotFoundError:
    print("8000")
    raise SystemExit(0)
if (
    not stat.S_ISREG(dotenv_metadata.st_mode)
    or dotenv_metadata.st_uid != os.getuid()
    or dotenv_metadata.st_nlink != 1
):
    raise SystemExit(".env is not a safe regular file")
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(path, flags)
with os.fdopen(descriptor, "rb") as stream:
    opened = os.fstat(stream.fileno())
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
    ):
        raise SystemExit("opened .env is not a safe regular file")
    raw = stream.read(1024 * 1024 + 1)
if len(raw) > 1024 * 1024:
    raise SystemExit(".env is unexpectedly large")
try:
    text = raw.decode("utf-8")
except UnicodeDecodeError as exc:
    raise SystemExit(f".env is not UTF-8: {exc}")
values = []
assignment_prefix = re.compile(r"^[ \t]*(?:export[ \t]+)?PORT[ \t]*=")
pattern = re.compile(
    r"^[ \t]*(?:export[ \t]+)?PORT[ \t]*=[ \t]*"
    r"(?:\"([0-9]+)\"|'([0-9]+)'|([0-9]+))"
    r"[ \t]*(?:[ \t]+#.*)?$"
)
for line in text.splitlines():
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        continue
    match = pattern.fullmatch(line)
    if match:
        values.append(next(group for group in match.groups() if group is not None))
    elif assignment_prefix.match(line):
        raise SystemExit(".env contains an invalid PORT assignment")
if not values:
    print("8000")
elif len(set(values)) == 1:
    print(values[0])
else:
    raise SystemExit(".env contains conflicting PORT assignments")
PY
    )"; then
        echo "$LOG_PREFIX 无法从 .env 安全确认 PORT；拒绝启动" >&2
        exit 1
    fi
fi
case "$RESOLVED_PORT" in
    ''|*[!0-9]*)
        echo "$LOG_PREFIX PORT 必须是 1..65535 的整数" >&2
        exit 1
        ;;
esac
if [ "${#RESOLVED_PORT}" -gt 5 ] || \
   [ "$((10#$RESOLVED_PORT))" -lt 1 ] || \
   [ "$((10#$RESOLVED_PORT))" -gt 65535 ]; then
    echo "$LOG_PREFIX PORT 必须是 1..65535 的整数" >&2
    exit 1
fi
PORT="$RESOLVED_PORT"

# A self-update owns dependency, frontend and migration mutations until its
# external handoff script has verified the replacement process. Running the
# normal ExecStartPre steps in that window would escape the backup/rollback
# boundary and can immediately undo a successful rollback.
CURRENT_COMMIT="$(git -C "$PROJECT_DIR" rev-parse HEAD 2>/dev/null || true)"
if [ -z "$CURRENT_COMMIT" ] || [ ! -x "$PYTHON3" ]; then
    echo "$LOG_PREFIX 无法确认 Python 或当前 commit；为保护部署事务拒绝启动" >&2
    exit 1
fi

"$PYTHON3" -m backend.services.deployment_start_guard \
    --project "$PROJECT_DIR" \
    --port "$PORT" \
    --commit "$CURRENT_COMMIT"
GUARD_RC=$?
case "$GUARD_RC" in
    0)
        ;;
    10)
        echo "$LOG_PREFIX 受控部署/恢复启动：跳过依赖同步与数据库迁移"
        exit 0
        ;;
    *)
        echo "$LOG_PREFIX 部署启动保护拒绝本次启动 (rc=$GUARD_RC)" >&2
        exit "$GUARD_RC"
        ;;
esac

echo "$LOG_PREFIX 检查依赖..."

# 1. Python 依赖（uv sync —— 仅 uv.lock 与 venv 不一致时才安装）
"$UV" sync --quiet 2>&1 || echo "$LOG_PREFIX uv sync 失败（非致命，继续）"

# 2. claude-pty git 依赖（安装时快照，不随 git pull 更新）
if [ -x scripts/refresh_pty.sh ]; then
    scripts/refresh_pty.sh 2>&1 || echo "$LOG_PREFIX refresh_pty 失败（非致命，继续）"
fi

# 3. 数据库迁移（init_db 启动时也会跑，这里提前跑避免启动报错）
"$UV" run alembic upgrade head 2>&1 || echo "$LOG_PREFIX alembic upgrade 失败（非致命，继续）"

echo "$LOG_PREFIX 完成"
