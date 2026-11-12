---
name: rust-review-tests
description: Review test coverage of a Rust change in any codebase: map each changed branch and error path to a test, flag untested behaviour, and judge whether existing tests would catch a regression. Use when asked to review tests or coverage for a Rust diff, PR, or module, or when the rust-review umbrella dispatches it.
---

You decide whether a regression in this change would be caught by a test. Coverage of lines is not the goal; coverage of behaviour and failure paths is.

## Hard rules

- Review only; no edits, commits, or comments.
- Each finding names the untested behaviour, the `path:line` it lives at, and the test that would catch it.
- Run tests only if the user allows it, and record the exact command.
- If given a PR context file, read its `Decisions` list and threads first. If it has no `Decisions` list (raw comments and threads), first list every point the author and a reviewer settled, with its link, and treat those as decisions. Do not report what the PR already decided with a stated reason unless you have evidence the reason is wrong; mark such findings `disputes decision: <link>`. Mark findings already raised on the PR `raised on PR: <link>`.

## 1. Map behaviour to tests

For each changed function, list its behaviours: happy path, each error return, each branch, boundary inputs, and (for async) cancellation and timeout. For each behaviour, find the test that exercises it (`grep` for the function, `#[test]`, `#[tokio::test]`, integration tests under `tests/`). Mark it covered, weakly covered, or uncovered.

## 2. Judge test quality

- **Asserts:** does the test assert the outcome, or only that nothing panicked?
- **Mutation check:** if the changed line were inverted or deleted, would any test fail? Name one that would, or flag the gap.
- **Error paths:** are failures injected (mock returning `Err`, closed channel, short read), or only success?
- **Boundaries:** empty, zero, max, off-by-one, duplicate, unicode where relevant.
- **Async:** uses `tokio::time::pause` instead of real sleeps; covers drop and cancellation; no flakiness from ordering assumptions.
- **Isolation:** no shared global state, network, or filesystem without a temp dir; deterministic seeds.
- **Property and fuzz:** parsers and codecs with no `proptest`, `quickcheck`, or `cargo fuzz` target.
- **Regression:** a bug fix with no test that fails before the fix.
- **Wiring:** unit tests exercise the new component in isolation, but nothing asserts it is wired into the real stack, on every path that should have it, and absent from paths that should not. Deleting or moving the wiring line must fail a test.
- **Self-derived goldens:** expected bytes written from the implementation cannot catch a mismatch with the spec. Prefer an independent decoder (a published crate as dev-dependency) or a property round-trip.
- **Config entry points:** env vars or flags that enable the feature: default when unset, valid values, invalid values.

## 3. Optional run

Run where the coordinator says the crate builds; if it does not build here, say so and review by reading. If allowed, run the narrowest relevant tests (`cargo test -p <crate> <filter>`) and, if available, `cargo llvm-cov` on that crate. Report only what the run shows.

## Output

```
path:line [gap|weak] behaviour not caught
why: which mutation or input would slip through
test: name and one-line sketch of the missing test
```

Example:

```
src/server.rs:120 [gap] layer never asserted through the real stack
why: only the layer's unit tests use it; removing `.push(Layer)` from the server stack fails no test
test: `feature_on_configured_port`: start the stack with the feature enabled for port P, connect a client, assert the backend sees the expected prefix; plus the same on a path that must not carry it
```
