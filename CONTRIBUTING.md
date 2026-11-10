# Contributing

These guidelines apply to everyone, whether you write the code yourself or
with a coding agent. The policy at the end says what an agent may do
without being asked.

## Issues

Open a bug report or a feature request through the issue forms. Wait
for a maintainer to accept the issue before you open a pull request for
it. For a question, use
[GitHub discussions](https://github.com/Kinference-AI/kinference/discussions).

Treat a bug report as a claim, not a fact. Read the report's manifests,
config and logs, find the code path, and reproduce the failure with a
test before changing code. Keep the test in the fix.

## Architecture

[ARCHITECTURE.md](ARCHITECTURE.md) is the map of the server: how the code is
grouped, which way imports go and where new code goes. Read it before you add
or move code, and change it in the same pull request as any move.

## Code comments

Keep comments short. Comment only what the code can't say on its own: the
*why* behind something non-obvious, such as invariants, locking and lifecycle
rules, protocol quirks, performance choices and workarounds for upstream bugs.

Don't write comments that:

- Narrate history ("originally", "used to", "after the refactor"). That
  belongs in commit messages and PR descriptions.
- Restate what the code plainly does.
- Argue for a design across several paragraphs.

Doc comments state a brief contract; one or two sentences is usually enough.
When you change code, update or delete the comments that describe it in the
same change.

## Commits

Maintainers merge PRs with a merge commit rather than squashing them, so
your commits land on `main` as you wrote them. The PR title and
description become the merge commit message. A commit message and a PR
description therefore have different readers, and the section "Pull
requests" below says what goes in each.

- Make each commit one logical change: the feature or fix, its tests, docs
  and unrelated small fixes go in separate commits when they make sense on
  their own.
- Every commit must build and pass tests, so `git bisect` keeps working.
- Each message explains what that commit changes and why. Use an imperative
  subject line under ~72 characters and wrap the body at 72.
- Write the message for someone who reaches the commit from `git blame`
  or `git bisect` and has never seen the PR. Explain the change at the
  level of the code: what was wrong or missing, what the commit does about
  it, and why that way.
- A branch's history goes through three phases:
  1. Until the first review, shape the branch as the change needs:
     amend, rebase and force-push it.
  2. Once a reviewer has commented, don't rewrite history until the
     review is done. Address review feedback with
     `git commit --fixup <sha>` or with new commits. Each fixup shows
     the reviewer one change on its own.
  3. Before the merge, fold the fixups with `git rebase -i --autosquash`
     and force-push once. The final branch holds no "address review" or
     "fix typo" commits.

## Sign your commits

Every commit needs a `Signed-off-by` trailer with your name and email.
The trailer states that you wrote the change or have the right to submit
it under the project's license, as the
[Developer Certificate of Origin](https://developercertificate.org)
describes. A check on each pull request rejects commits without one.

Run `just setup` once per clone. It points git at the repository's
`.githooks` directory, whose `prepare-commit-msg` hook adds the trailer
to every commit. Without the hook, `git commit -s` adds it.

## Pull requests

The PR title and description become the merge commit message. Write them
for someone reading `git log --first-parent main`, who sees one entry per
PR and does not open the commits. `git revert -m 1` reverts a PR as a
unit, so the merge commit is the record of what the PR delivered.

- Title: the whole change, in the imperative, under ~72 characters.
- Keep it short. One paragraph per part says what changed and why.
  Reasons, lists and per-file detail go in the docs or the commits
  that the PR carries, not in the description.
- Description: the five labeled parts of
  `.github/pull_request_template.md`, in its order. Each label stands
  on its own line as plain text, followed by one or more paragraphs.
  The template says what goes under each label.
- A part that does not apply keeps its label and says in one sentence
  why: "None. The change touches only Rust internals." The sentence
  tells the reader that you checked.
- Don't repeat the commits' messages. The commits hold the code-level
  reasoning. A PR with one commit still gets a merge commit, and its
  description is a shorter summary of that commit, not a copy.
- Leave out markdown headings, bold, checklists and review chatter. The
  description is a commit message, and markdown means nothing in
  `git log`.

An example:

```
Replace the observations CSV sink with structured tracing events

Problem

The router wrote each predicted-versus-observed row to a CSV file on an
emptyDir. A pod restart lost the file, and getting the rows out needed
kubectl cp. When the sink was on, every row took a mutex and flushed the
file from the gRPC handler.

Solution

The router now logs each streaming observation as a debug event under
the kinference::observation target, with the CSV's values as named
fields. The rows go through the cluster's log pipeline, and the event
costs almost nothing while its target is off.

Operator-visible changes

The CSV sink, its config field observations_csv, its environment
variable and the chart's observationsCsv value, volume and mount are
gone. A new KINFERENCE_LOG_FORMAT=json option, set by the chart's
logFormat value, lets a pipeline parse the fields. An operator who
collected the CSV switches to the jq recipe in docs/local-development.md.

Validation

Ran the smoke test on kind with logFormat=json and
logLevel=kinference::observation=debug, sent twenty streaming requests
and read twenty events back with jq.

Issues

Closes #104.
```

### End-to-end tests

The end-to-end suite runs in a separate repository and reports as the
`e2e` check on the pull request. A pull request from a branch in this
repository starts the suite on every push. A pull request from a fork
starts the suite when a maintainer comments `/e2e` on it. Each new push
to a fork needs a new `/e2e` comment.

## Policy on Generative AI & LLM-Assisted Contributions ##

### Ultimate Human Accountability ###

The human who submits a contribution remains entirely and solely
accountable for its content, correctness, security and runtime behavior.
An LLM or AI tool does not shift, diminish or share that accountability
with the tool, the model provider or the AI platform.

- Architectural & Logic Mastery: The contributor must understand the whole
submission thoroughly and be able to explain it.
- Maintainers review the safety, logic and quality of every contribution,
whether or not the contributor used generative AI or LLMs to create it.
- A tool-assisted submission must still follow all the contribution
guidelines above.
- The contributor decides each git action. An agent commits, pushes or
opens a pull request only as far as the contributor's request asks, and
runs no destructive git command unprompted.

### Attribution and Co-Author Guidelines ###

Attributing AI tools, models, or assistant platforms (for example, via
`Co-authored-by:` commit message trailers or pull request descriptions) is
**optional**.

- **Optional Tool Attribution:** A contributor who wants to acknowledge the
AI tools that they used may add a commit trailer or a PR footnote.
- **No Legal Co-Authorship:** Nobody may name an AI tool or LLM as a legal
copyright holder or a primary author, because accountability rests only
with the human contributor.

### Maintainer Authority and Enforcement ###

Kinference maintainers reserve the right to:

- Reject or close pull requests or issues that show signs of hallucinated
content or of a lack of human oversight.
- Require more explanation, a rationale for the design, or smaller
iterations for complex AI-assisted contributions.
- Block or restrict contributors who repeatedly submit unvalidated AI-generated
pull requests.
