---
name: breakdown
description: "File one feature request per Break-down line of a merged design and attach each one as a sub-issue of the proposal. Use when asked to break a design down, to file or create the sub-issues or child issues of a design, or to split a merged design into issues."
argument-hint: "[design-file]"
disable-model-invocation: true
---

# File the sub-issues of a design

Read `docs/workflow.md` first. This skill is the end of step 3 of
that page. The proposal's `accepted` label covers every sub-issue, so
the skill applies no label. Show every draft and wait for one yes for
the batch before any `gh issue create`. Sub-issues need gh 2.94 or
newer.

1. Run `gh --version`. Below 2.94, say "Upgrade gh to 2.94 or newer
   for sub-issues." and stop.
2. Check the design. Run `git fetch origin`.
   `git show origin/main:design/<file>` has `status: accepted` and
   `issue: <N>`. `gh issue view <N> --json labels,state` shows an
   open issue with `accepted`. When either check fails, say "The
   design for #<N> has not merged with status: accepted. `/design`
   and `/pr` finish it." and stop.
3. List the sub-issues that exist with
   `gh issue view <N> --json subIssues --jq '.subIssues.nodes[] | "\(.number) \(.title)"'`,
   and skip each Break-down line that already has one.
4. Read `.github/ISSUE_TEMPLATE/feature.yml`. Draft one feature
   request per remaining line, in the shape that `/issue` uses: one
   level-three heading per field, in the form's order. The body
   starts with `Part of #<N>. Design: design/<file>.` Condense Why
   and Today from the design's sections. What is the line. Done is
   the line's Done. The title is the line's title.
5. Check every sentence of each draft against the prose rules in
   AGENTS.md and rewrite each sentence that fails.
6. Show every draft with its command. On a yes, file them in landing
   order, each with
   `gh issue create --parent <N> --title "..." --label enhancement --body-file -`
   and the body on stdin.
7. Verify with `gh issue view <N> --json subIssuesSummary` that the
   count matches the Break-down.

Done: the sub-issue numbers in landing order. After filing, end with:
"Ask for the first sub-issue in a comment, wait for a maintainer to
assign it to you, branch, and run `/pr`."

## Rationalizations to reject

| Thought | Why it is wrong |
|---|---|
| "The design is approved in review, so I can file the sub-issues before the merge." | The proposal gets `accepted` when the design merges. Before that, nothing covers the sub-issues. |
| "Filing one at a time is slow, so I will skip showing the drafts." | Each sub-issue is a public write under the user's name. One yes covers the batch, after the drafts. |
