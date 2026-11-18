# Automated review

The `ai-review` workflow reviews a pull request with a model before a
maintainer does. The workflow brings a pull request to a state that
looks mergeable, and then asks the maintainers for review. A maintainer
still approves and merges every pull request. The workflow never
approves, merges, closes or requests changes.

## For contributors

The review starts when CI and the `e2e` check both pass on a pull
request that is ready for review and that passes the `Pull request
policy` check. A draft gets no review until you mark it ready. The `e2e`
check runs on a pull request from a fork only after a maintainer
comments `/e2e`, so the review of such a pull request waits for that
comment.

- When CI or the `e2e` check fails, the bot lists the failing checks in
  its summary comment and applies `ai-review/changes-requested`. The bot
  spends no model call until both pass.
- When the review finds a blocking problem, the bot posts it as an
  inline comment and applies `ai-review/changes-requested`. Push a
  fixup commit, and the next passing CI and `e2e` run starts the
  next round.
- When no blocking problem remains, the bot applies `ai-review/ready`,
  asks the maintainers for review and turns its summary comment into a
  handoff summary. After the handoff, the bot stops reviewing on push.

The review runs at most three rounds. After the third round, the bot
hands off with the open findings listed. When you disagree with a
finding, comment `/ai-review ready` to hand the pull request to a
maintainer at once. The maintainer reads the open findings in the
handoff summary and decides.

A finding is blocking when it reports a bug, a security problem, new
behaviour without a test, a breaking change without a migration, or
behaviour that contradicts the linked issue. Every other finding is
advisory, such as a clearer name or a missing doc comment. Advisory
findings never reach you during the rounds. They appear in a collapsed
section of the handoff summary for the maintainer.

## For maintainers

- Reply `/dismiss` on an inline finding to drop it. The bot never raises
  a finding with the same key again on that pull request.
- Comment `/ai-review` to run a new review, for example after the
  handoff. The new review updates the handoff summary and posts no
  inline comments.
- Apply `ai-review/skip` to keep the bot off a pull request.
- Run the workflow by hand with a pull request number to review it
  without waiting for CI or the `e2e` check.

The bot skips drafts, pull requests from bots and pull requests that
change more than 1000 lines or 30 files. A pull request that is too
large gets `size/xl` and `ai-review/skipped` and goes straight to the
maintainers. On a pull request that a maintainer wrote, the bot posts
the labels and the handoff summary without the contributor rounds.

Each reviewed pull request gets `size/*`, `risk/*` and `complexity/*`
labels. The risk label is never below the floor that
`.github/ai-review/risk-floor` sets for the changed paths.

The model reads text that the pull request author wrote: the diff, the
description and the commit messages. An author can write that text to
talk the model into a low risk label. The risk floor depends only on the
changed paths, so the author can't lower it. Read a pull request from an
author you don't know in full, whatever its risk label says.

## Setup

- The repository secret `ANTHROPIC_API_KEY` holds the model API key.
- The repository variable `AI_REVIEW_PHASE` turns the workflow on.
  Without the variable, the workflow does nothing.
  - `shadow` writes nothing to the pull request. The job summary of each
    run shows the labels, comments and review request that the full flow
    would make.
  - `handoff` posts the labels and the handoff summary after one review,
    with no inline comments and no rounds.
  - `full` runs every step on this page.
- The handoff requests review from the `maintainers` team of the
  organization. The team needs write access to the repository, because
  GitHub requests a review only from a team that has access.

`.github/ai-review/config.json` pins the model, the effort, the round
cap and the size limits. The workflow always reads the prompt, the
schema, the config and the risk floor from the base branch, so a change
to any of them takes effect only after it merges.

## How the review stays safe

The workflow runs from the base branch with the triggers `workflow_run`,
`check_run`, `pull_request_target` and `issue_comment`. The job checks
out only the reviewer's own files from the base branch. It never builds
or runs anything from the pull request.

The job runs three steps in order. The steps pass files to each other
through a work directory on the runner. Only one secret reaches each
step. The GitHub token can read the code and write to issues and pull
requests. The token has no write access to the code, so it can't push
or merge.

### Step 1: prepare

The prepare step holds the GitHub token and writes nothing to GitHub.
It reads the pull request, the linked issue, the bot's summary comment
and the state of CI, the `e2e` check and the `Pull request policy`
check. From those it decides what the run does: nothing, a handoff, a
CI failure report or a review.

For a review, the prepare step fetches the pull request head as data
into a repository that it creates. It folds the `fixup!` commits with
git alone, with hooks and global config turned off. It extracts the
head's files into a plain directory without symlinks. It writes the
plan and the model's input to the work directory.

### Step 2: agent

The agent step runs only for a review. It holds the model API key and
no GitHub token. Before the agent step, a separate workflow step
installs the `anthropic` package from hash-pinned requirements, with no
secret in its environment. The agent step then sends the input to the
model.

The model reads the extracted files through three tools: `read_file`,
`grep` and `list_dir`. Each tool refuses a path outside the directory
and refuses symlinks. The model returns JSON that matches
`.github/ai-review/schema.json`, and the agent step writes that JSON to
the work directory.

### Step 3: decide

The decide step holds the GitHub token again. It runs even after a
failed agent step, so that the pull request still reaches the
maintainers with `ai-review/error`.

The decide step filters every finding before it writes anything. It
drops a finding that anchors to no added line and to no commit of the
series. It drops a finding that a maintainer dismissed. It strips
mentions, links, images and HTML from the model's text.

The decide step then applies labels from a fixed set, posts the inline
findings and the summary comment, and requests review from the
`maintainers` team. In the `shadow` phase, it writes only the job
summary.
