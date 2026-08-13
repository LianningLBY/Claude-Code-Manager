#!/bin/sh

# claude-pty intentionally exposes only a small fixed CLI surface.  CCM uses
# this wrapper to apply the exact per-Task settings that the direct `-p` path
# receives, without putting any credential value in argv.
permission_profile=${CCM_TASK_CLAUDE_PROFILE:-isolated}
claude_binary=${CCM_TASK_CLAUDE_BINARY:-claude}
tool_allowlist=${CCM_TASK_CLAUDE_TOOLS:-AskUserQuestion,Bash,Edit,Glob,Grep,MultiEdit,NotebookEdit,Read,Write}

if [ "$permission_profile" = "unrestricted" ]; then
  unset CCM_TASK_CLAUDE_PROFILE
  unset CCM_TASK_CLAUDE_BINARY
  unset CCM_TASK_CLAUDE_TOOLS

  exec "$claude_binary" \
    --setting-sources "" \
    --strict-mcp-config \
    --disable-slash-commands \
    --no-chrome \
    --tools "$tool_allowlist" \
    --allowedTools "$tool_allowlist" \
    "$@"
fi

settings_path=${CCM_TASK_CLAUDE_SETTINGS:?missing Task Claude settings}

unset CCM_TASK_CLAUDE_PROFILE
unset CCM_TASK_CLAUDE_SETTINGS
unset CCM_TASK_CLAUDE_BINARY
unset CCM_TASK_CLAUDE_TOOLS

exec "$claude_binary" \
  --permission-mode acceptEdits \
  --settings "$settings_path" \
  --setting-sources "" \
  --strict-mcp-config \
  --disable-slash-commands \
  --no-chrome \
  --tools "$tool_allowlist" \
  --allowedTools "$tool_allowlist" \
  "$@"
