# Helpers that pr.sh and issue.sh source. Every write goes through `write`,
# which POLICY_DRY_RUN=1 turns off.

set -euo pipefail

MARKER='<!-- kinference-policy -->'
BOT='github-actions[bot]'
# GraphQL reports the bot's login without the suffix.
BOT_GRAPHQL=${BOT%'[bot]'}
REPO=${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}

enforce() { [[ ${POLICY_ENFORCE:-} == true ]]; }

api() { gh api "repos/$REPO/$1" "${@:2}"; }

graphql() { gh api graphql -f owner="${REPO%/*}" -f name="${REPO#*/}" "$@"; }

write() { [[ ${POLICY_DRY_RUN:-} == 1 ]] || "$@" > /dev/null; }

bot_comment_id() {
  api "issues/$1/comments" --paginate --jq "[.[] | select(.body | startswith(\"$MARKER\"))][0].id // empty"
}

# Replaces the bot's comment, or writes the first one.
comment() {
  local id
  id=$(bot_comment_id "$1")
  if [[ -n $id ]]; then
    write api "issues/comments/$id" -X PATCH -f body="$MARKER"$'\n'"$2"
  else
    write api "issues/$1/comments" -f body="$MARKER"$'\n'"$2"
  fi
}

set_state() { write api "issues/$1" -X PATCH -f state="$2"; }

set_check() {
  local url=${GITHUB_SERVER_URL:-https://github.com}/$REPO/actions/runs/${GITHUB_RUN_ID:-0}
  write api check-runs -f name='Pull request policy' -f head_sha="$1" -f status=completed \
    -f conclusion="$2" -f details_url="$url" -f "output[title]=$3" -f "output[summary]=$3"
}

closed_by_bot() {
  [[ $(api "issues/$1/events" --paginate --jq '[.[] | select(.event == "closed")][-1].actor.login') == "$BOT" ]]
}

strip_comments() { perl -0pe 's/<!--.*?-->//gs'; }
