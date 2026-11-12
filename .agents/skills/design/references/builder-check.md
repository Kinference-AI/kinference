# Brief for the builder check

Start one fresh-context subagent with the draft, ARCHITECTURE.md,
AGENTS.md and the research tables. Tell it to report, with the line
of the draft for each finding:

- where the Proposal does not follow from the Why;
- where the Proposal breaks an invariant that ARCHITECTURE.md states;
- where a rejected alternative is not worse than the Proposal;
- where the prior art says something that the draft ignores;
- where Compatibility and rollout leaves the upgrade order or a
  rollback unstated;
- where the Break-down leaves `main` unshippable between pull
  requests, or a line lacks its Done;
- where a sentence breaks the prose rules in AGENTS.md.

Sort the findings into three groups. The first group holds findings
that the draft already answers. For each of those findings, point the
subagent at the passage and drop the finding. The second group is
minor. Fix the draft and list each fix in one line. The third group
needs the user. Play those findings back one at a time, with a
recommended answer. Record each decision in the document, or list the
open decision under Open questions with who decides.
