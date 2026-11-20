#!/usr/bin/env bash
# Writes the PR context (title, body, comments, review threads) that the review skills read first.
#
# Usage:
#   PR=123 GITHUB_REPOSITORY=org/repo GH_TOKEN=... OUT=/tmp/review/pr_context.md ./ai-review-context.sh
#
# Env vars and defaults:
#   PR                 PR number (required)
#   GITHUB_REPOSITORY  owner/repo (required)
#   GH_TOKEN           token read by gh (required by gh)
#   OUT                output markdown path (required)

set -euo pipefail

# Required inputs.
PR=${PR:?usage}
GITHUB_REPOSITORY=${GITHUB_REPOSITORY:?usage}
OUT=${OUT:?usage}

# Create the output dir.
echo "[-] Writing PR #${PR} context to ${OUT}"
mkdir -p "$(dirname "$OUT")"

# PR title, body and top-level comments, then inline review comments.
{
  echo "# PR #${PR} context"
  gh pr view "$PR" --json title,body,comments --jq '"## " + .title + "\n\n" + (.body // "") + "\n\n## Comments\n" + ([.comments[] | "- " + .author.login + ": " + .body] | join("\n"))'
  echo
  echo "## Review threads"
  gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR}/comments" --paginate \
    --jq '.[] | "- " + .user.login + " on " + .path + ":" + ((.line // .original_line) | tostring) + ": " + .body'
} > "$OUT"
