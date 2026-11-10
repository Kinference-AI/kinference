---
name: pr
description: Draft a pull request title and description for the current branch in the shape that CONTRIBUTING.md and the pull request template require. The description becomes the merge commit message. Use when asked to open, write or fix a PR.
---

# Draft a pull request

Read the "Pull requests" section of CONTRIBUTING.md and
`.github/pull_request_template.md` before you start.

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
6. Body: the five parts of `.github/pull_request_template.md`, in its
   order. Replace each comment in the template with the paragraphs
   that it asks for, and keep every label, as the "Pull requests"
   section of CONTRIBUTING.md says. No markdown headings, bold, task
   lists, links to this conversation, or attribution footer.
7. When Operator-visible changes is not "None", the branch has run on
   a kind cluster: `just up`, `just test-smoke` and the steps in
   docs/local-development.md that exercise the change, then
   `just down`. Say under Validation what the run showed. When you
   cannot run it, say that it was not run.
8. A stacked pull request names under Issues the pull request that it
   builds on and says that it merges after that one.
9. Check every sentence of the draft against the prose rules in
   AGENTS.md, and rewrite each sentence that fails.
10. Show the title and body to the user. Open the PR only when the user
    asks, with `gh pr create --base <base branch> --title ... --body-file -`,
    where the base branch is `main` or the lower pull request's head
    branch.
