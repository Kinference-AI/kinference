---
name: pr
description: Draft a pull request title and description for the current branch in the shape that AGENTS.md requires. The description becomes the merge commit message. Use when asked to open, write or fix a PR.
---

# Draft a pull request

1. Find the base. The base is `origin/main`, unless the branch forks
   from the head branch of another open pull request. Then the base is
   that branch, and the pull request targets it, so that GitHub shows
   only this branch's commits and retargets the pull request to `main`
   when the lower one merges. `gh pr list --state open` lists the
   candidates, and `git merge-base --is-ancestor origin/<head> HEAD`
   succeeds for the one that this branch builds on. Every later step
   uses the base where it says `<base>`.
2. Fold fixups. When the branch has no upstream
   (`git rev-parse --abbrev-ref @{u}` fails), run
   `git rebase -i --autosquash <base>` with `GIT_SEQUENCE_EDITOR=:`.
   When the branch is pushed, leave the fixups and say that the user
   folds them before merging.
3. Read the branch: `git log --reverse <base>..HEAD` and
   `git diff --stat <base>...HEAD`. Read the diff where a commit
   message leaves a question open.
4. Check the branch against the issue. Read the Done section of each
   issue the branch closes and compare it with the diff. What Done
   asks for that the branch does not do goes under Issues as work left
   out, or back to the user as a gap to close before the pull request.
5. Title: one line in the imperative, under 72 characters, that names the
   whole change. Not the first commit's subject unless it covers the
   branch.
6. Body: five parts in this order, each introduced by its label alone on
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
      showed. When Operator-visible changes is not "None", the branch
      has run on a kind cluster: `just up`, `just test-smoke` and the
      steps in docs/local-development.md that exercise the change, then
      `just down`. Say what the run showed. When you cannot run it, say
      that it was not run.
   5. `Issues`: `Closes #N` for each issue the PR closes, and work left
      out. A stacked pull request also names the pull request that it
      builds on and says that it merges after that one.
7. A part that does not apply keeps its label and says in one sentence
   why: "None. The change touches only Rust internals." Never drop a
   label and never write a bare "None".
8. A branch with one commit still gets a merge commit. Its body is a
   shorter summary of that commit, not a copy of the commit body.
   CONTRIBUTING.md shows two filled-in examples.
9. Check the prose. Read the prose rules in AGENTS.md again and
   check every sentence of the draft against them: one main claim per
   sentence, no aside inserted mid-sentence, one explicit antecedent
   for every pronoun, and the project's own terms. Rewrite each
   sentence that fails.
10. Show the title and body to the user. Open the PR only when the user
    asks, with `gh pr create --base <base branch> --title ... --body-file -`,
    where the base branch is `main` or the lower pull request's head
    branch.
