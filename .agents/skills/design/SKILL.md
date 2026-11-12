---
name: design
description: "Write a design document under design/ from design/TEMPLATE.md for a proposal issue that a maintainer labelled needs-design. Interviews the user, researches prior art in fresh contexts, has a fresh context attack the draft, and commits it on a branch when asked. Use when asked to design, propose or plan a change that a maintainer sent to design, or to write a design document."
argument-hint: "[issue-number]"
disable-model-invocation: true
---

# Write a design

Read `docs/workflow.md` first. This skill is step 3 of that page. A
design is a pull request to `design/`. Maintainers review the design
in that pull request. Only the `needs-design` label sends a change
here; a comment does not. The skill never applies a label. Show the
full draft and wait for a yes before any commit or `gh issue comment`.
Ask one question at a time, only for a decision, with a recommended
answer. Look every fact up first.

1. Check the issue with
   `gh issue view <N> --json labels,state,assignees`, in this order.
   A closed issue: say "Issue #<N> is closed." and stop. The
   `accepted` label without `needs-design`: say "The greenlight
   covers an implementation. Branch and run `/pr`." and stop.
   Neither label: say "Not triaged yet. Only a label counts, not a
   comment." and stop. When `grep -l "^issue: <N>$" design/*.md`
   finds a file, offer to continue that file. When the assignee is
   someone else, say "Issue #<N> is assigned to <login>." and stop.
   When nobody holds the issue, show a comment that asks for it, post
   the comment on a yes, and go on drafting locally. A stop in this
   step ends the skill without the closing line below.
2. Read `design/TEMPLATE.md`, ARCHITECTURE.md, `docs/prior-art.md`,
   and the issue with its comments.
3. Interview the user the way `/issue` does, starting from the
   issue's Why, Today and What. Cover the shape of the Proposal, what
   is out of scope, and what an operator or the gateway can observe
   when the whole change ships.
4. List the ARCHITECTURE.md invariants that the change touches: the
   import direction, who talks to the API server, who writes, the
   purity of the picker and of `resolve`, the locking rules.
5. Research prior art in fresh contexts. Write one question list per
   angle, and start one subagent per angle with the Agent tool and
   the brief in `references/research-brief.md`.
6. Draft the document from `design/TEMPLATE.md` as
   `design/<YYYY-MM-DD>-<slug>.md`. The front matter holds
   `status: draft`, the date, the author from `git config user.name`,
   and `issue: <N>`. The Proposal stops at crates, modules, types and
   contracts. A design that specifies the code goes past the design
   and into the implementation. The Prior art section is the
   research, pruned to what bears on the decision. For each approach,
   the section says where that approach falls short for Kinference.
   The prior art does not bound the Proposal. When a combination of
   approaches, or a new approach, beats every approach that you read,
   propose that one and say why. Compatibility and rollout says what
   survives the upgrade, the order in which the operator upgrades the
   pieces, and whether a rollback works. Each Break-down line follows
   the template's format and has its own Done. The lines are in
   landing order, and each line leaves `main` shippable. A project
   that the research cites for the first time gets an entry in
   `docs/prior-art.md` on the same branch.
7. Have a fresh context attack the draft, with the brief in
   `references/builder-check.md`. Play back to the user only the
   findings that need a decision, one at a time.
8. Check every sentence of the draft against the prose rules in
   AGENTS.md and rewrite each sentence that fails.
9. Show the user the path and the Summary, Proposal and Break-down
   sections. Revise until they approve.
10. Commit only when the user asks: a branch from `origin/main`, one
    commit with `git commit -s`. Never set `status: accepted` here.

Done: the committed draft on a branch. After the commit, end with:
"Run `/pr`. The pull request says `Part of #<N>`. When the review is
done, `/pr` sets `status: accepted` as the last commit. After the
merge, run `/breakdown` to file the sub-issues."

## Rationalizations to reject

| Thought | Why it is wrong |
|---|---|
| "The issue is obviously big, so I will start the design before triage." | Only a maintainer applies `needs-design`. Without it the document has no greenlight. |
| "The maintainer agreed in a comment, so the label is a formality." | The page counts labels, not comments. |
| "The research is thin, so I will fill the gap from memory." | Every claim in Prior art names a URL and a date, or says "nothing found". |
