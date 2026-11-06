---
name: issue
description: Draft a GitHub issue for this repository from its issue forms, since gh issue create skips them. Use when asked to file, write or open a bug report or a feature request.
---

# Draft an issue

1. Pick the form under `.github/ISSUE_TEMPLATE` that matches: `bug.yml`
   for behaviour that differs from the docs, `feature.yml` for something
   Kinference does not do. Read the form.
2. Title: one line that names the symptom or the capability, not the
   suspected cause.
3. Body: one section per field of the form, in the form's order, headed
   by the field's `label` as a level-three heading. Fill a required field
   from what you know; when you lack it, say so under the heading rather
   than leaving the heading out. Put YAML, logs and commands in fenced
   blocks.
4. For a bug seen in a cluster, gather the reproduction from it:
   `kubectl get inferenceworkersets,inferencepools,inferencepolicies -o yaml`,
   `helm get values kinference`, `kubectl logs deploy/kinference`. For a
   bug found by reading the code, the reproduction is the failing test
   or the request that triggers the path, and "Where in the code" names
   the path.
5. State what happened and what you expected as facts, and keep a
   suspected cause under "Where in the code" marked as a guess.
6. Show the title and body to the user. File the issue only when the user
   asks, with `gh issue create --title ... --label <the form's labels>
   --body-file -`.
