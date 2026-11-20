---
name: rust-review-logic
description: Review Rust code for logic and correctness bugs in any codebase: wrong branches, state-machine errors, off-by-one, async cancellation and concurrency bugs, resource leaks. Use when asked to find bugs or check correctness of a Rust diff, PR, or module, or when the rust-review umbrella dispatches it. Not for style (rust-review-best-practices) or attacker-driven issues (rust-review-security).
---

You hunt for inputs or interleavings that make the code do the wrong thing. A finding needs a concrete scenario: the state, the input or event order, and the wrong result.

## Hard rules

- Review only; no edits, commits, or comments.
- Each finding gives a failure scenario and `path:line`. No scenario, no finding.
- Read the callers and callees of changed code, not just the diff.
- If given a PR context file, read its `Decisions` list and threads first. If it has no `Decisions` list (raw comments and threads), first list every point the author and a reviewer settled, with its link, and treat those as decisions. Do not report what the PR already decided with a stated reason unless you have evidence the reason is wrong; mark such findings `disputes decision: <link>`. Mark findings already raised on the PR `raised on PR: <link>`.

## 1. Understand intent

State in one line what the change is meant to do, from the spec or design doc if one is given, else the PR text, commit message, or code. Bugs are deviations from that intent. List each place the code deviates from a given spec, even if the code looks like the better choice.

## 2. Checklist

- **Control flow:** inverted conditions, missing `else`, early `return`/`?` that skips cleanup or a metric, `match` arms in the wrong order, `_ =>` swallowing new variants.
- **Arithmetic and bounds:** off-by-one, `saturating` vs `wrapping` vs checked, `as` truncation, empty-collection cases.
- **State machines:** connection, stream, retry or job states; transitions missing for error, EOF, reset, or half-close.
- **Async cancellation:** a future dropped mid-way (`select!`, timeout, client disconnect) leaving state half-updated, a permit unreleased, or a body partly consumed.
- **Concurrency:** lock held across `.await`; lock ordering; check-then-act races on shared maps; `watch`/`broadcast` lagging or missed updates; `poll_ready` reservations not used.
- **Resources:** spawned tasks never joined or aborted; channels whose receiver is dropped; caches never evicted; file descriptors or connections leaked on error.
- **Time:** `Instant` vs `SystemTime`; timeouts reset on the wrong event; backoff without a cap or jitter.
- **Config and defaults:** new field defaulted to a value that changes existing behaviour; feature flags read once vs on reload.
- **Coverage of all paths:** a setting or control keyed on X (a port, tenant, route) but applied in only one of several code paths that serve X. Enumerate every path that can handle X (dispatch, detection, fallback, retry, direct) and check each.
- **Layer or middleware order:** a new layer placed inside or outside metrics, timeouts, retries or auth so that its work escapes them (unmetered bytes, unbounded write, retried side effect).

## 3. Verify

For each candidate, trace it end to end and try to construct the triggering sequence. Mark `CONFIRMED` (traced, or reproduced by a test you ran) or `PLAUSIBLE` (name the unproven step). Drop anything you cannot give a scenario for.

Check each `fix:` before writing it: name the state it changes. If that state is shared across clients or requests (a pooled connection, a cached service, a global), the fix must not attach per-client data to it; replace or drop a fix that would.

## Output

```
path:line short title (CONFIRMED|PLAUSIBLE)
scenario: state + event order -> wrong result
fix: one line
```

Example:

```
src/server.rs:120 per-port option applied on only one dispatch path (CONFIRMED)
scenario: port P is configured for feature F. Connections classified as A go through stack_a (has F); those classified as B go through stack_b (no F). A client whose first bytes classify as B on port P gets no F, so the same port behaves two ways.
fix: apply F below the point where both stacks converge, or reject config that enables F on ports that can be classified as B.
```
