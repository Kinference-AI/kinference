#!/usr/bin/env bash
# shellcheck shell=bash
# Points the claude CLI at the chosen provider. Sourced by ai-review-run.sh and ai-review-format.sh.
#
# Usage:
#   PROVIDER=zoltar ZOLTAR_TOKEN=<token> source ./ai-review-provider.sh
#   PROVIDER=claude ANTHROPIC_API_KEY=<key> source ./ai-review-provider.sh
#
# Env vars and defaults:
#   PROVIDER           zoltar or claude (required)
#   ZOLTAR_TOKEN       Zoltar bearer token (required when PROVIDER=zoltar)
#   ANTHROPIC_API_KEY  read by the claude CLI (required when PROVIDER=claude)
#   MODEL              set to `coding` when PROVIDER=zoltar

# Point the claude CLI at the chosen provider.
case "$PROVIDER" in
  zoltar)
    ZOLTAR_TOKEN=${ZOLTAR_TOKEN:?ZOLTAR_TOKEN is required when PROVIDER=zoltar}
    unset ANTHROPIC_API_KEY
    export ANTHROPIC_BASE_URL=https://zoltar.buoyant.io
    export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
    # Map every model alias the CLI or a sub-agent might ask for to Zoltar's coding model.
    export ANTHROPIC_DEFAULT_OPUS_MODEL=coding
    export ANTHROPIC_DEFAULT_SONNET_MODEL=coding
    export ANTHROPIC_DEFAULT_HAIKU_MODEL=coding
    # shellcheck disable=SC2034 # read by the sourcing script
    MODEL=coding
    # apiKeyHelper sends the token the way the local Zoltar setup does (zoltar-auth there).
    # Reading it from the env keeps the token out of the file.
    mkdir -p ~/.claude
    echo '{"apiKeyHelper": "printenv ZOLTAR_TOKEN"}' > ~/.claude/settings.json
    ;;
  claude)
    ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY:?ANTHROPIC_API_KEY is required when PROVIDER=claude}
    ;;
  *)
    echo "usage: PROVIDER must be zoltar or claude, got '${PROVIDER}'" >&2
    exit 1
    ;;
esac
