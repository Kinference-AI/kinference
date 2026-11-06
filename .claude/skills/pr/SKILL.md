---
name: pr
description: Draft a pull request title and description for the current branch in the shape that AGENTS.md requires. The description becomes the merge commit message. Use when asked to open, write or fix a PR.
---

# Draft a pull request

1. Read the branch: `git log --reverse origin/main..HEAD` and
   `git diff --stat origin/main...HEAD`. Read the diff where a commit
   message leaves a question open.
2. Title: one line in the imperative, under 72 characters, that names the
   whole change. Not the first commit's subject unless it covers the
   branch.
3. Body: five parts in this order, each introduced by its label alone on
   a line, as plain text. No markdown headings, bold, task lists, links to
   this conversation, or attribution footer.
   1. `Problem`: what is wrong or missing today, and why it matters.
      Start from the symptom, not from the code.
   2. `Solution`: what is different once the PR is merged. For several
      commits, the overall shape. For one commit, a sentence or two that
      does not repeat the commit body.
   3. `Operator-visible changes`: config fields, environment variables,
      chart values, CRD fields, proto messages, metric names, and what an
      operator has to do about them.
   4. `Validation`: what was run beyond CI, against what, and what it
      showed.
   5. `Issues`: `Closes #N` for each issue the PR closes, and work left
      out.
4. A part that does not apply keeps its label and says in one sentence
   why: "None. The change touches only Rust internals." Never drop a
   label and never write a bare "None".
5. A branch with one commit still gets a merge commit. Its body is a
   shorter summary of that commit, not a copy of the commit body.
   CONTRIBUTING.md shows two filled-in examples.
6. Follow the prose rules in AGENTS.md: one claim per sentence, no dashes
   for asides, the project's own terms.
7. Show the title and body to the user. Open the PR only when the user
   asks, with `gh pr create --base main --title ... --body-file -`.
