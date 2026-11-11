---
name: pr
description: "Check the current branch against the contribution workflow and draft a pull request title and description in the shape that CONTRIBUTING.md and the template require, for an implementation, a design or a sub-issue, then open or update the pull request when asked. Use when asked to open, write, draft, update or fix a PR or a pull request description, to answer review, or to fold fixups."
argument-hint: "[issue-number]"
disable-model-invocation: true
---

# Draft a pull request

Read `docs/workflow.md`, the "Pull requests" section of
CONTRIBUTING.md and `.github/pull_request_template.md` first. This
skill is steps 4 and 5 of `docs/workflow.md`. Only a label counts:
`accepted` greenlights an implementation and `needs-design` a design.
The skill never applies a label and never rewrites history
unprompted. The skill shows the full draft before `gh pr create`,
`gh pr edit`, `gh issue comment`, any commit and any push. Below,
`<base>` is the git ref `origin/<name>` and `<base branch>` is the
name itself.

1. Find the base. Run `git fetch origin`. The base is `origin/main`,
   unless the branch forks from the head branch of another open pull
   request. Then the base is that head branch, and the pull request
   targets it. GitHub retargets the pull request to `main` when that
   other pull request merges. `gh pr list --state open` lists the
   candidate pull requests. `git merge-base --is-ancestor
   origin/<head> HEAD` succeeds for the one that this branch builds
   on. When several succeed, take the one nearest to HEAD.
2. When `gh pr view --json number` finds a pull request for the
   branch, the skill updates it and ends with `gh pr edit`.
3. Read the branch: `git log --reverse <base>..HEAD` and
   `git diff --stat <base>...HEAD`. Every commit has a
   `Signed-off-by` trailer and no attribution trailer. Name the
   commits that fail, and go on.
4. Find the issue from the user or from the commit messages. When
   neither names one, say "The branch names no issue. The policy
   closes a pull request without one. Run `/issue`, wait for
   `accepted`, and run `/pr <N>`." and stop. Read the issue with
   `gh issue view <N> --json labels,state,assignees,parent,comments`.
   The mode is design when the diff adds a file under `design/`
   other than `TEMPLATE.md` with `issue: <N>`, sub-issue when the
   issue has a parent, and implementation otherwise.
5. Gate. Implementation: the issue has the `accepted` label. Design:
   the issue has the `needs-design` label. Sub-issue: the parent has
   the `accepted` label, no maintainer comment on the sub-issue holds
   it back, and every earlier Break-down line's sub-issue is closed.
   When the gate fails, say which one and stop: "Issue #<N> lacks
   `accepted`. The policy closes a pull request without one.", or
   "Issue #<N> has the `needs-design` label. Run `/design`.", or
   "Sub-issue #<M> lands before this one." There is no override. In
   update mode, an issue that went from `accepted` to `needs-design`
   means that a reviewer asked for a design. Say that the pull
   request waits for the design, and stop.
6. Claim. The assignee is the login from `gh api user --jq .login`.
   Someone else: say "Issue #<N> is assigned to <login>." and stop.
   Nobody: show a comment that asks for the issue, post it on a yes,
   and say that the pull request waits for the assignment.
7. Compare the diff with the issue's Done. When Done asks for
   something that the branch does not do, list that gap under Issues
   as work that the pull request leaves out, or ask the user to close
   the gap first.
8. Fixups. Never fold unprompted. When the user says that the review
   is done, offer `GIT_SEQUENCE_EDITOR=: git rebase -i --autosquash
   <base>` and one `git push --force-with-lease`.
9. Flip a status only when the user asks, as one signed commit. In
   design mode, set `status: accepted` before the fold. In sub-issue
   mode, when this sub-issue is the last open one, set
   `status: implemented` in the design file.
10. Title: one line in the imperative, under 72 characters, that
    names the whole change. Body: the parts of the template, in its
    order, each introduced by its label alone on a line, one short
    paragraph each. Leave out what the diff and the docs already say.
    No markdown headings, bold, task lists or links to this
    conversation. Issues says `Closes #<N>`. In design mode, Issues
    says `Part of #<N>` and no Closes. For the last sub-issue, Issues
    says `Closes #<M>` and `Closes #<N>` for the proposal. A stacked
    pull request names the pull request that it builds on. When
    Operator-visible changes is not "None", run the branch on a kind
    cluster with `just up`, `just test-smoke` and the steps in
    docs/local-development.md that exercise the change. Validation
    says what the run showed, or says that you did not run it.
11. Check every sentence of the draft against the prose rules in
    AGENTS.md and rewrite each sentence that fails.
12. Show the title and body. On a yes:
    `gh pr create --base <base branch> --title "..." --body-file -`
    with the body on stdin, or `gh pr edit` in update mode.

Done: the pull request URL. After opening or updating, end with:
"After a design pull request merges, run `/breakdown`. Otherwise
nothing is left."

## Rationalizations to reject

| Thought | Why it is wrong |
|---|---|
| "The change is tiny, so it needs no issue." | The page has no exemption. A pull request without an accepted issue gets closed. |
| "The maintainer said yes in a comment." | Only the label counts. |
| "I will fold the fixups now to save a round trip." | Once a reviewer has commented, the history stays until the review is done and the user asks. |
| "The user is a maintainer, so I can skip the gate." | The gate is the same for everyone. The maintainer applies the label first. |
