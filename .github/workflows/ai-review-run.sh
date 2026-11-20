#!/usr/bin/env bash
# Runs one rust-review-<check> skill with the Claude Code CLI and writes its report.
#
# Usage:
#   CHECK=security BASE=<sha> HEAD=<sha> \
#     CONTEXT=/tmp/review/pr_context.md OUT=/tmp/review/security.md MAX_BUDGET_USD=5 \
#     ./ai-review-run.sh
#
# Env vars and defaults:
#   CHECK              review skill suffix, e.g. security (required)
#   MODEL              model id (optional; the CLI default when unset; zoltar forces `coding`)
#   BASE               PR base sha (required)
#   HEAD               PR head sha (required)
#   CONTEXT            PR context file from ai-review-context.sh (required)
#   OUT                report output path (required)
#   MAX_BUDGET_USD     spend cap for this run (required)
#   PROVIDER           zoltar or claude (required)
#   ZOLTAR_TOKEN       Zoltar bearer token (required when PROVIDER=zoltar)
#   ANTHROPIC_API_KEY  read by the claude CLI (required when PROVIDER=claude)

set -euo pipefail

# Required inputs.
CHECK=${CHECK:?usage}
BASE=${BASE:?usage}
HEAD=${HEAD:?usage}
CONTEXT=${CONTEXT:?usage}
OUT=${OUT:?usage}
MAX_BUDGET_USD=${MAX_BUDGET_USD:?usage}
PROVIDER=${PROVIDER:?usage}

# Optional inputs.
MODEL=${MODEL:-}

# Point the claude CLI at the chosen provider.
# shellcheck source=ai-review-provider.sh
source "$(dirname "$0")/ai-review-provider.sh"

# Review the PR's own changes: diff from the merge base, not the moving base tip.
MERGE_BASE=$(git merge-base "$BASE" "$HEAD")
PROMPT="Invoke the rust-review-${CHECK} skill with the Skill tool and follow it.
Target: this repository, the diff ${MERGE_BASE}..${HEAD}.
PR context file: ${CONTEXT}.

$(cat "$(dirname "$0")/ai-review-check-prompt.md")"

echo "[-] Running ${CHECK} review with ${PROVIDER}/${MODEL:-default} into ${OUT}"
# Only read tools: the review cannot change the checkout or reach the PR.
# --add-dir lets Read reach the context file, which lives outside the checkout.
args=(
  -p "$PROMPT"
  --max-budget-usd "$MAX_BUDGET_USD"
  --add-dir "$(dirname "$CONTEXT")"
  --allowedTools "Skill,Read,Grep,Glob,Bash(git diff:*),Bash(git log:*),Bash(git show:*)"
  --output-format text
)
if [ -n "$MODEL" ]; then
  args+=(--model "$MODEL")
fi
claude "${args[@]}" > "$OUT"
