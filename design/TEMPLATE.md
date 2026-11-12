---
# status is draft, accepted, implemented or superseded. A superseded
# design adds superseded_by with the path of the design that replaces it.
status: draft
date: <YYYY-MM-DD>
author: <name>
issue: <N>
---

# <Title: the change, in the imperative>

## Summary

<Two or three sentences: what changes and why it is worth the change.>

## Why

<What someone is trying to do, and what stops them. Name the person:
an operator, a gateway, a contributor.>

## Today

<How Kinference behaves now, with the modules and the numbers that a
reader can check against the tree. Name the workaround if there is one.>

## Proposal

<What Kinference does differently, at the level of crates, modules,
types and contracts. Stop before the code. Name every invariant in
ARCHITECTURE.md that the change touches, and say how it holds
afterwards.>

## Alternatives considered

<One paragraph per alternative: what it is, and the reason it loses.>

## Prior art

<One paragraph per project whose design decisions bear on the change,
with the file or document read and its date. What the project does,
what it rejected, where its approach falls short for Kinference, and
whether its reason applies here.>

## Operator-visible changes

<Config fields, environment variables, chart values, CRD fields, proto
messages, metric names, and what an operator has to do about them. Or
"None." and why.>

## Compatibility and rollout

<What keeps working across the upgrade and what breaks: an existing
config file, a CRD object, a BackendBinding, a gateway on the previous
picker protocol. The order in which the operator upgrades the pieces,
and whether a rollback works. Or "None." and why.>

## Break-down

<One line per pull request, in the order they land, each one leaving
main shippable: `- <title in the imperative>. Done: <what an operator
or the gateway observes>.` Each line becomes one feature request,
filed as a sub-issue of the proposal.>

## Open questions

<One per paragraph, with who decides and by when, or "None.">
