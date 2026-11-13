#!/usr/bin/env bash
# shellcheck disable=SC2016  # GraphQL queries hold literal $variables.
# Checks pull requests against docs/workflow.md and CONTRIBUTING.md
# through the API alone, and reports as the "Pull request policy"
# check run plus one comment that lists every broken rule.
#
# | Broken rule                                    | Check | Pull request          |
# |------------------------------------------------|-------|-----------------------|
# | names no issue                                 | fails | closed under enforce  |
# | the issue is closed                            | fails | closed under enforce  |
# | the issue carries neither accepted             | fails | closed under enforce  |
# |   nor needs-design, nor does its parent        |       |                       |
# | the author is not an assignee of the issue     | fails | closed under enforce  |
# | the title is longer than 72 characters         | fails | stays open            |
# | the description lacks one of the six parts     | fails | stays open            |
# | a commit subject is longer than 72 characters  | fails | stays open            |
# | the branch holds a merge commit                | fails | stays open            |
#
# "Under enforce" means the repository variable POLICY_ENFORCE is true.
# A pull request names its issue in the Development section or in the
# Issues part of its description. A draft skips the four shape rules.
# A pull request that the bot closed opens again once every rule
# passes. A pull request that a person closed is left alone.
#
# PR=<number> checks one pull request, PR=all every open one and every
# one the bot closed, and ISSUE=<number> every pull request that
# closes the issue. POLICY_DRY_RUN=1 skips every write.

cd "$(dirname "$0")"
. ./lib.sh

RULES_URL="https://github.com/$REPO/blob/main/docs/workflow.md#rules"

fetch_pr() {
  graphql -F number="$1" -f query='
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          state isDraft title body headRefOid
          author { login }
          closingIssuesReferences(first: 1) { nodes { number } }
        }
      }
    }' --jq '.data.repository.pullRequest'
}

fetch_issue() {
  graphql -F number="$1" -f query='
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        issue(number: $number) {
          state
          labels(first: 30) { nodes { name } }
          assignees(first: 20) { nodes { login } }
          parent { labels(first: 30) { nodes { name } } }
        }
      }
    }' --jq '.data.repository.issue'
}

PARTS='Problem|Solution|Operator-visible changes|Compatibility and rollout|Validation|Issues'
ISSUE_REF='(closes|fixes|resolves|part of) #[0-9]+'

# Prints the text under one part of the description.
part() {
  jq -r '.body // ""' <<< "$1" | tr -d '\r' | strip_comments | awk -v parts="$PARTS" -v want="$2" '
    { t = $0; gsub(/^[ \t]+|[ \t]+$/, "", t) }
    t ~ "^(" parts ")$" { cur = t; next }
    cur == want { print }'
}

# The issue that the Development section of the pull request links.
# A Closes keyword makes that link only on the default branch, so a
# stacked pull request has it only when someone links it by hand.
linked_issue() { jq -r '.closingIssuesReferences.nodes[0].number // empty' <<< "$1"; }

# The first issue that the Issues part of the description names with
# Closes or Part of.
described_issue() { part "$1" Issues | grep -oiE "$ISSUE_REF" | grep -oE '[0-9]+' | head -1; }

# A maintainer's label on the issue or on its parent greenlights the
# pull request, and the assignee is the claim. Prints one reason per
# broken rule.
issue_rules() {
  local pr=$1 number issue author
  number=$(linked_issue "$pr")
  [[ -n $number ]] || number=$(described_issue "$pr")
  if [[ -z $number ]]; then
    echo "neither the Development section nor the Issues part names an issue"
    return
  fi
  issue=$(fetch_issue "$number")
  if [[ $(jq -r .state <<< "$issue") != OPEN ]]; then
    echo "issue #$number is closed"
    return
  fi
  jq -e '[.labels.nodes[].name] + [(.parent.labels.nodes // [])[].name] | any(. == "accepted" or . == "needs-design")' <<< "$issue" > /dev/null \
    || echo "issue #$number carries neither \`accepted\` nor \`needs-design\`"
  author=$(jq -r .author.login <<< "$pr")
  jq -e --arg a "$author" '[.assignees.nodes[].login] | index($a) != null' <<< "$issue" > /dev/null \
    || echo "issue #$number is not assigned to @$author"
}

# The title and the commit subjects fit, and the description holds
# the template's six parts in order. Prints one reason per broken rule.
shape_rules() {
  local number=$1 pr=$2 title
  title=$(jq -r .title <<< "$pr")
  (( ${#title} <= 72 )) || echo "the title is longer than 72 characters"
  jq -r '.body // ""' <<< "$pr" | tr -d '\r' | strip_comments | awk -v parts="$PARTS" '
    BEGIN { n = split(parts, L, "|"); next_part = 1 }
    { gsub(/^[ \t]+|[ \t]+$/, ""); if ($0 == L[next_part]) next_part++ }
    END { if (next_part <= n) print "the description lacks the part " L[next_part] }'
  api "pulls/$number/commits" --paginate --jq '.[] | "\(.parents | length) \(.commit.message | split("\n")[0])"' | awk '
    $1 > 1 { merge = 1 }
    length($0) - 2 > 72 { long = 1 }
    END {
      if (long) print "a commit subject is longer than 72 characters"
      if (merge) print "the branch holds a merge commit"
    }'
}

check_pr() {
  local number=$1 pr state sha gate shape reasons
  pr=$(fetch_pr "$number")
  state=$(jq -r .state <<< "$pr")
  sha=$(jq -r .headRefOid <<< "$pr")
  [[ $state == OPEN ]] || closed_by_bot "$number" || return 0

  shape=$(jq -e .isDraft <<< "$pr" > /dev/null || shape_rules "$number" "$pr")
  gate=$(issue_rules "$pr")
  reasons=$(printf '%s\n' "$shape" "$gate" | sed '/^$/d')
  echo "#$number: ${reasons:-passes}"

  if [[ -z $reasons ]]; then
    set_check "$sha" success "follows the contribution workflow"
    [[ $state == OPEN ]] || set_state "$number" open
    [[ -z $(bot_comment_id "$number") ]] || comment "$number" "The pull request follows the contribution workflow."
    return
  fi
  set_check "$sha" failure "$(head -1 <<< "$reasons")"
  comment "$number" "The pull request does not follow the [contribution workflow]($RULES_URL):"$'\n\n'"- ${reasons//$'\n'/$'\n'- }"
  if [[ -n $gate && $state == OPEN ]] && enforce; then
    set_state "$number" closed
  fi
}

# Open pull requests, and closed ones that the bot closed.
all_prs() {
  gh pr list --repo "$REPO" --state open --limit 200 --json number --jq '.[].number'
  graphql -f query='
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        pullRequests(states: CLOSED, last: 100) {
          nodes {
            number
            timelineItems(last: 1, itemTypes: CLOSED_EVENT) { nodes { ... on ClosedEvent { actor { login } } } }
          }
        }
      }
    }' --jq ".data.repository.pullRequests.nodes[] | select(.timelineItems.nodes[0].actor.login == \"$BOT_GRAPHQL\") | .number"
}

# Pull requests whose body names the issue. check_pr skips the merged
# ones and the ones a person closed.
prs_for_issue() {
  gh pr list --repo "$REPO" --state all --limit 300 --json number,body \
    --jq ".[] | select(.body | test(\"$ISSUE_REF\"; \"i\") and test(\"#$1([^0-9]|$)\")) | .number"
}

if [[ -n ${ISSUE:-} ]]; then
  targets=$(prs_for_issue "$ISSUE")
elif [[ ${PR:-} == all ]]; then
  targets=$(all_prs)
elif [[ -n ${PR:-} ]]; then
  targets=$PR
else
  echo "set PR=<number>, PR=all or ISSUE=<number>" >&2
  exit 2
fi
for n in $targets; do check_pr "$n"; done
