#!/bin/sh

# Claude Code treats even empty auth variables as an explicit login choice.
# CloudRouter accounts obtain their key through the private apiKeyHelper in
# CLAUDE_CONFIG_DIR, so inherited official-account credentials must be absent
# from the final Claude process rather than merely overwritten with "".
unset ANTHROPIC_AUTH_TOKEN
unset ANTHROPIC_API_KEY
unset CLAUDE_CODE_OAUTH_TOKEN

# Keep Claude's mutable state private when the CLI atomically rewrites files
# such as .claude.json.
umask 077

claude_binary=${CCM_CLOUDROUTER_CLAUDE_BINARY:-claude}
unset CCM_CLOUDROUTER_CLAUDE_BINARY
exec "$claude_binary" "$@"
