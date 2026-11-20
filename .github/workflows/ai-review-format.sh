#!/usr/bin/env bash
# Writes the PR comment body from all check reports.
#
# Usage:
#   REPORTS=/tmp/review OUT=/tmp/review/comment.md MAX_BUDGET_USD=1 PROVIDER=claude \
#     ./ai-review-format.sh
#
# Env vars and defaults:
#   REPORTS            dir holding the <check>.md reports from ai-review-run.sh (required)
#   OUT                PR comment body output path (required)
#   MAX_BUDGET_USD     spend cap for this run (required)
#   PROVIDER           zoltar or claude (required)
#   MODEL              model id (optional; the CLI default when unset; zoltar forces `coding`)
#   ZOLTAR_TOKEN       Zoltar bearer token (required when PROVIDER=zoltar)
#   ANTHROPIC_API_KEY  read by the claude CLI (required when PROVIDER=claude)

set -euo pipefail

# Required inputs.
REPORTS=${REPORTS:?usage}
OUT=${OUT:?usage}
MAX_BUDGET_USD=${MAX_BUDGET_USD:?usage}
PROVIDER=${PROVIDER:?usage}

# Optional inputs.
MODEL=${MODEL:-}

# Point the claude CLI at the chosen provider.
# shellcheck source=ai-review-provider.sh
source "$(dirname "$0")/ai-review-provider.sh"

# Append each check report under its own heading; skip OUT, which may live in REPORTS.
PROMPT="$(cat "$(dirname "$0")/ai-review-format-prompt.md")
"
out_base=$(basename "$OUT")
found=0
for report in "$REPORTS"/*.md; do
  if [ ! -f "$report" ] || [ "$(basename "$report")" = "$out_base" ]; then
    continue
  fi
  found=1
  name=$(basename "$report" .md)
  PROMPT="${PROMPT}
## ${name} report
$(cat "$report")
"
done

# No reports means nothing to format.
if [ "$found" -eq 0 ]; then
  echo "[-] No reports in ${REPORTS}, skipping PR comment"
  exit 0
fi

echo "[-] Writing PR comment with ${PROVIDER}/${MODEL:-default} into ${OUT}"
# Only read tools: the formatter copies code snippets from the checkout and changes nothing.
args=(
  -p "$PROMPT"
  --max-budget-usd "$MAX_BUDGET_USD"
  --allowedTools "Read,Grep,Glob"
  --output-format text
)
if [ -n "$MODEL" ]; then
  args+=(--model "$MODEL")
fi
claude "${args[@]}" > "$OUT"
