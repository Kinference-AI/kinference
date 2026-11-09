---
name: issue
description: Draft a GitHub issue for this repository from its issue forms, since gh issue create skips them. Use when asked to file, write or open a bug report or a feature request, or to define a change before anyone implements it.
---

# Draft an issue

1. Pick the form under `.github/ISSUE_TEMPLATE` that matches: `bug.yml`
   for behaviour that differs from the docs, `feature.yml` for something
   Kinference does not do. Read the form.
2. Size the change. A change that needs more than one pull request is a
   design, not an issue. Say so, point at the `design` skill, and stop.
3. For a feature, interview the user until every field of the form has
   an answer. Ask one question at a time, and give your recommended
   answer with it, so that "yes" accepts it. Never ask for a fact that
   this repo, the gateway repo or `kubectl` can answer; look it up
   first. Ask only for decisions: what to build, what to leave out, and
   what an operator or a gateway can observe once it ships. The
   interview ends when no question is left.
4. When the change touches the gateway contract, KV routing or the
   CRDs, read how the projects in `docs/prior-art.md` that bear on it
   handle the same problem. Put what they do under What, with the file
   or document that you read and its date. Report what each project
   does in one paragraph. In the next paragraph, say where each
   approach falls short for Kinference. Then recommend, in a paragraph
   of its own. The prior art does not bound the recommendation: combine
   approaches from several projects, or propose a new one, when that
   beats every approach you read, and say why it does.
5. Title: one line that names the symptom or the capability, not the
   suspected cause.
6. Body: one section per field of the form, in the form's order, headed
   by the field's `label` as a level-three heading. Fill a required field
   from what you know; when you lack it, say so under the heading rather
   than leaving the heading out. Put YAML, logs and commands in fenced
   blocks.
7. For a bug seen in a cluster, gather the reproduction from it:
   `kubectl get inferenceworkersets,inferencepools,inferencepolicies -o yaml`,
   `helm get values kinference`, `kubectl logs deploy/kinference`. For a
   bug found by reading the code, the reproduction is the failing test
   or the request that triggers the path, and "Where in the code" names
   the path.
8. State what happened and what you expected as facts, and keep a
   suspected cause under "Where in the code" marked as a guess.
9. Check the prose. Read the prose rules in AGENTS.md again and
   check every sentence of the draft against them: one main claim per
   sentence, no aside inserted mid-sentence, one explicit antecedent
   for every pronoun, and the project's own terms. Rewrite each
   sentence that fails.
10. Show the title and body to the user. File the issue only when the
    user asks, with `gh issue create --title ... --label <the form's
    labels> --body-file -`.
