<!-- The title and this description become the merge commit message.
CONTRIBUTING.md says how to write them. Keep the six labels and their
order. Replace each comment with one short paragraph that says what
changed and why. Leave out what the diff and the docs already say. A
part that does not apply says in one sentence why. -->

Problem

<!-- What is wrong or missing today, and why it matters. Start from the
symptom, not from the code. -->

Solution

<!-- What is different once the pull request is merged, as a reader of
main sees it. For several commits, the overall shape. For one commit, a
sentence or two that does not repeat the commit body. -->

Operator-visible changes

<!-- Config fields, environment variables, chart values, CRD fields,
proto messages, metric names, and what an operator has to do about
them. -->

Compatibility and rollout

<!-- Whether a running deployment survives the upgrade: an existing
config file, a chart values file, a CRD object, a BackendBinding, a
gateway on the previous picker protocol. The order in which the
operator upgrades the pieces, and whether a rollback works. -->

Validation

<!-- What you ran beyond CI, against what, and what you saw. -->

Issues

<!-- Closes #N for each issue the pull request closes, and work that
the pull request leaves out. -->
