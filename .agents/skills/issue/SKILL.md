---
name: issue
description: "Draft a bug report or a feature request for this repository from its issue forms, since gh issue create skips them, and file it when the user approves the draft. Use when asked to file, write, report or open a bug, a feature request, a proposal or an issue."
disable-model-invocation: true
---

# Draft an issue

Read `docs/workflow.md` first. This skill is step 1 of that page. A
maintainer decides at triage whether the change needs a design. The
issue therefore holds no design, and the skill never sizes the
change. Show the full draft and wait for a yes before
`gh issue create` or `gh issue comment`. Ask one question at a time,
only for a decision, with a recommended answer. Look every fact up
first in this repository, the gateway repository or `kubectl`.

1. Pick the form under `.github/ISSUE_TEMPLATE` that matches:
   `bug.yml` for behaviour that differs from the docs, `feature.yml`
   for something that Kinference does not do. Read the form.
2. Search for a duplicate with `gh issue list --state all --search
   "<key words>"`. Show the matches. When an open issue describes the
   same symptom or the same capability, recommend a comment on that
   issue instead of a new one. Draft the comment, post it on a yes,
   and stop. Otherwise go on.
3. For a bug seen in a cluster, gather the reproduction from it:
   `kubectl get inferenceworkersets,inferencepools,inferencepolicies -o yaml`,
   `helm get values kinference`, `kubectl logs deploy/kinference`. For
   a bug that you found by reading the code, the reproduction is the
   failing test or the request that triggers the path. "Where in the
   code" names that path. State what happened and what you expected
   as facts, and keep a suspected cause marked as a guess.
4. For a feature, interview the user until every field of the form
   has an answer. Ask only for decisions: what to build, what to leave
   out, and what an operator or the gateway can observe once the whole
   change ships. Done states that observable behaviour. When the
   change looks like several pull requests, or alters a contract that
   the gateway or a CRD depends on, say so in one sentence under What.
   The maintainer reads that sentence at triage.
5. For a feature that touches the gateway contract, KV routing or the
   CRDs, read the entries in `docs/prior-art.md` that bear on the
   change. Put one paragraph under What that says what the projects
   in those entries do, with the file that you read and its date.
6. Title: one line that names the symptom or the capability, not the
   suspected cause. Body: one section per field of the form, in the
   form's order, headed by the field's label as a level-three heading.
   A required field that you cannot fill gets a sentence under its
   heading that says so. Put YAML, logs and commands in fenced blocks.
7. Check every sentence of the draft against the prose rules in
   AGENTS.md and rewrite each sentence that fails.
8. Show the title and body. File the issue only when the user says
   so, with `gh issue create --title "..." --label <the form's
   labels> --body-file -` and the body on stdin.

Done: the issue URL. After filing, end with: "Wait for triage. With
`needs-design`, run `/design`. With `accepted`, ask for the issue in a
comment, wait for a maintainer to assign it to you, branch, and run
`/pr`."

## Rationalizations to reject

| Thought | Why it is wrong |
|---|---|
| "This is clearly a design-sized change, so I will write the Done as 'the design merges'." | Only a maintainer applies `needs-design`. Done describes what ships. |
| "The maintainer already said yes in Slack, so I can skip the issue." | Every pull request closes an issue with `accepted`. A comment is not a label. |
| "The user is in a hurry, so I will file without showing the draft." | An issue is a public write under the user's name. The draft comes first. |
