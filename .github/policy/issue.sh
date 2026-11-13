#!/usr/bin/env bash
# shellcheck disable=SC2016  # GraphQL queries hold literal $variables.
# Checks issues against the issue forms and docs/workflow.md.
# ISSUE=<number> checks one, ISSUE=all checks every open issue and
# every one the bot closed. POLICY_DRY_RUN=1 skips every write.

cd "$(dirname "$0")"
. ./lib.sh

join() { local s; printf -v s '%s, ' "$@"; echo "${s%, }"; }
add_label() { write api "issues/$1/labels" -f "labels[]=$2"; }
remove_label() { write api "issues/$1/labels/$2" -X DELETE; }
permission() { api "collaborators/$1/permission" --jq .permission 2> /dev/null || echo none; }

CAP=${POLICY_ISSUE_CAP:-5}
TRIAGE_LABELS='accepted needs-design wontfix'
FORMS='bug.yml feature.yml'

form_label() { sed -n 's/^labels: *\[\(.*\)\]$/\1/p' "../ISSUE_TEMPLATE/$1"; }
has_label() { grep -qxF "$2" <<< "$1"; }

# Prints the labels of the form's required fields. Each field's label
# precedes its validations block in the form files.
required_fields() {
  awk '
    /^ *label:/ { sub(/^ *label: */, ""); label = $0 }
    /^ *required: *true/ { print label }' "../ISSUE_TEMPLATE/$1"
}

# Prints the form whose label the issue carries. GitHub drops a label
# that an account without push access sets through the API, so an
# issue without a form's label gets the form whose required headings
# the body holds most of.
form_of() {
  local labels=$1 body=$2 form n best="" best_n=0
  for form in $FORMS; do
    if has_label "$labels" "$(form_label "$form")"; then
      echo "$form"
      return
    fi
  done
  for form in $FORMS; do
    n=$(grep -cxFf <(required_fields "$form" | sed 's/^/### /') <<< "$body" || true)
    if (( n > best_n )); then best=$form best_n=$n; fi
  done
  echo "$best"
}

check_issue() {
  local number=$1 failures=()
  local issue state author body labels form
  issue=$(api "issues/$number")
  if jq -e '.pull_request' <<< "$issue" > /dev/null; then
    return
  fi
  state=$(jq -r .state <<< "$issue")
  author=$(jq -r .user.login <<< "$issue")
  labels=$(jq -r '.labels[].name' <<< "$issue")
  body=$(jq -r '.body // ""' <<< "$issue" | tr -d '\r' | strip_comments)

  # Rule: came through a form.
  form=$(form_of "$labels" "$body")
  if [[ -z $form ]]; then
    failures+=("The issue did not come through a form. The issue has no form's label, and the body has none of a form's \`### <field>\` headings. docs/workflow.md: \"The forms are the only way to open an issue, and each one asks for what a maintainer needs in order to decide.\"")
  fi

  # Rule: every required field has text.
  if [[ -n $form ]]; then
    local field missing=()
    while IFS= read -r field; do
      awk -v h="### $field" '
        $0 == h { in_field = 1; next }
        /^### / { in_field = 0 }
        in_field && $0 !~ /^[ \t]*$/ && $0 != "_No response_" { found = 1 }
        END { exit !found }' <<< "$body" || missing+=("$field")
    done < <(required_fields "$form")
    if (( ${#missing[@]} )); then
      failures+=("The form's required field$([[ ${#missing[@]} -gt 1 ]] && echo s) $(join "${missing[@]}") $([[ ${#missing[@]} -gt 1 ]] && echo have || echo has) no text. The forms ask for what a maintainer needs in order to decide.")
    fi
  fi

  # Rule: a read-only account waits for triage before filing more.
  local perm
  perm=$(permission "$author")
  if [[ $perm == read || $perm == none ]]; then
    local others
    others=$(api "issues?creator=$author&state=open&per_page=100" --paginate \
      | jq -s --argjson self "$number" --arg triage "$TRIAGE_LABELS" '
          ($triage | split(" ")) as $t
          | [add[] | select(.pull_request == null and .number != $self)
                   | select([.labels[].name] | any(IN($t[])) | not)]
          | length')
    if (( others >= CAP )); then
      failures+=("@$author has $others open issues that wait for triage. docs/workflow.md: \"A maintainer applies \`accepted\`, applies \`needs-design\`, or closes the issue with \`wontfix\`.\" File the next issue once those are triaged.")
    fi
  fi

  # The web form applies its label, and the API may have dropped it.
  if (( ${#failures[@]} == 0 )) && ! has_label "$labels" "$(form_label "$form")"; then
    add_label "$number" "$(form_label "$form")"
  fi

  report "$number" "$state" "$labels" "${failures[@]+"${failures[@]}"}"
}

report() {
  local number=$1 state=$2 labels=$3
  shift 3
  local failures=("$@") closed_by_bot=0
  closed_by_bot "$number" && closed_by_bot=1

  if (( ${#failures[@]} == 0 )); then
    if (( closed_by_bot )); then
      comment "$number" "The issue policy check passes. Reopened."
      set_state "$number" open
    elif [[ -n $(bot_comment_id "$number") ]]; then
      comment "$number" "The issue policy check passes."
    fi
    ! has_label "$labels" invalid || remove_label "$number" invalid
    echo "#$number passes"
    return
  fi

  local lines="The issue policy check found:"$'\n' f
  for f in "${failures[@]}"; do lines+=$'\n'"- $f"; done
  lines+=$'\n\n'"Edit the issue into the form's shape, with one \`### <field>\` heading per field, and the check runs again."
  if enforce && [[ $state == open ]]; then
    comment "$number" "$lines"
    add_label "$number" invalid
    set_state "$number" closed
  else
    if ! enforce; then lines+=" Enforcement is off, so the issue stays open."; fi
    comment "$number" "$lines"
  fi
  for f in "${failures[@]}"; do echo "#$number: $f"; done
}

all_issues() {
  gh issue list --repo "$REPO" --state open --limit 500 --json number --jq '.[].number'
  graphql -f query='
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        issues(states: CLOSED, last: 100) {
          nodes {
            number
            timelineItems(last: 1, itemTypes: CLOSED_EVENT) { nodes { ... on ClosedEvent { actor { login } } } }
          }
        }
      }
    }' --jq ".data.repository.issues.nodes[] | select(.timelineItems.nodes[0].actor.login == \"$BOT_GRAPHQL\") | .number"
}

main() {
  local targets
  if [[ ${ISSUE:-} == all ]]; then
    targets=$(all_issues)
  elif [[ -n ${ISSUE:-} ]]; then
    targets=$ISSUE
  else
    echo "set ISSUE=<number> or ISSUE=all" >&2
    exit 2
  fi
  local n
  for n in $targets; do check_issue "$n"; done
}

main
