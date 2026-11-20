---
name: rust-review-perf
description: Review a Rust change for performance and resource cost in any codebase: hot-path allocations and clones, algorithmic complexity, lock contention, async overhead, unbounded growth, and missing timeouts. Use when asked for a performance or resource review of a Rust diff, PR, or module, or when the rust-review umbrella dispatches it.
---

You estimate what the change costs per call on the paths that run most, and what grows without bound. A finding states the path, its frequency, and the cost.

## Hard rules

- Review only; no edits, commits, or comments.
- Each finding cites `path:line`, why the path is hot (per request, per byte, per connection, per tick), and the cost.
- Do not claim a regression is measurable without a benchmark; mark it estimated.
- If given a PR context file, read its `Decisions` list and threads first. If it has no `Decisions` list (raw comments and threads), first list every point the author and a reviewer settled, with its link, and treat those as decisions. Do not report what the PR already decided with a stated reason unless you have evidence the reason is wrong; mark such findings `disputes decision: <link>`. Mark findings already raised on the PR `raised on PR: <link>`.

## 1. Find the hot paths

Identify which changed code runs per byte, per message or request, per connection, or at startup only. Ignore startup and config-load paths unless they block.

## 2. Checklist

- **Allocation:** `clone`, `to_string`, `format!`, `collect`, `Vec::new` in loops or per request; missing `with_capacity`; `Box`/`Arc` per item.
- **Copies:** `Bytes`/`BytesMut` copied instead of split or sliced; `Vec<u8>` round-trips.
- **Complexity:** linear scans inside loops; `Vec::contains` or `remove(0)` on growing collections; repeated hashing; regex compiled per call.
- **Locks and sharing:** contended `Mutex` on hot paths; `RwLock` write-heavy; `Arc` clone churn; atomics with `SeqCst` where weaker ordering suffices.
- **Async:** a task per item where a stream works; `Box::pin` per poll; large futures moved by value; busy polling; `yield_now` loops.
- **Unbounded growth:** maps, caches, queues, or buffers keyed by input with no cap or eviction.
- **Time and IO:** missing timeouts; small unbuffered reads or writes; syscalls per byte; `Instant::now` in tight loops.
- **Logging and metrics:** `debug!` formatting arguments evaluated when disabled; metric labels with unbounded cardinality.
- **Cost when off:** a new optional feature that adds work (boxing, dyn dispatch, a wrapper, a clone, a wider struct) to every call even when disabled. Push the layer only when enabled, or use an `Either` future so the pass-through path is unchanged.

## 3. Optional measurement

If the crate has `criterion` or `cargo bench` targets covering the path and the user allows it, run before and after and report the numbers with the commands.

## Output

```
path:line [hot|growth|nit] short title (estimated|measured)
path: what runs it and how often
cost: what it allocates, copies, or grows
fix: one line
```

Example:

```
src/feature.rs:80 [hot] optional layer boxes every connection future (estimated)
path: Layer::call, once per connection on every port, including when the feature is unconfigured (the default)
cost: one heap allocation and dynamic dispatch per connection that did not exist before
fix: return `Either<Inner::Future, BoxFuture>`, or skip pushing the layer when the feature set is empty
```
