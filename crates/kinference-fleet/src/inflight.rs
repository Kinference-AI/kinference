//! In-flight state: work dispatched to a worker and not yet observed.
//!
//! Two per-endpoint trackers with the same lifecycle and endpoint key
//! (`score_addr`): [`InflightRegistry`] counts picks not yet observed as
//! first-token; [`InflightPrefixes`] remembers the prompt prefixes whose
//! `BlockStored` events have not landed yet.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Per-endpoint count of picks not yet observed as first-token.
///
/// Scraped metrics lag, so during a burst several picks can land before a
/// scrape reflects the first. The picker uses this count to break ties and
/// avoid piling a burst onto one worker.
///
/// Keyed by `endpoint` (`host:port`), which every `WorkerScore` carries. A
/// dead endpoint's entry just becomes an unused map slot.
#[derive(Default)]
pub struct InflightRegistry {
    counts: DashMap<String, AtomicU32>,
}

impl InflightRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump the in-flight count for an endpoint, after a pick.
    pub fn inc(&self, endpoint: &str) {
        self.counts
            .entry(endpoint.to_string())
            .or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the in-flight count for an endpoint, saturating at zero.
    /// Called when the pick's observation is released.
    pub fn dec(&self, endpoint: &str) {
        if let Some(entry) = self.counts.get(endpoint) {
            // Saturate at 0, where `fetch_sub` would wrap.
            let atom = entry.value();
            loop {
                let current = atom.load(Ordering::Relaxed);
                if current == 0 {
                    return;
                }
                if atom
                    .compare_exchange_weak(
                        current,
                        current - 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
            }
        }
    }

    /// Current in-flight count for an endpoint; 0 if never seen.
    pub fn get(&self, endpoint: &str) -> u32 {
        self.counts
            .get(endpoint)
            .map(|e| e.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// One outstanding dispatch. `remaining` shrinks as `on_stored` calls
/// arrive, and the dispatch is dropped once it empties.
///
/// `uncached_tokens` counts the tokens that the engine had to prefill. It is
/// stored since the radix tree will have moved on by the time it's needed.
/// `input_tokens` is the full prompt's token count, for the quadratic
/// prefill cost model.
#[derive(Debug, Clone)]
struct InflightDispatch {
    endpoint: String,
    remaining: HashSet<u64>,
    dispatched_at: Instant,
    uncached_tokens: u32,
    input_tokens: u32,
}

/// One outstanding dispatch, without its hashes, for the picker's queue-cost
/// model.
#[derive(Debug, Clone, Copy)]
pub struct InflightSummary {
    pub dispatched_at: Instant,
    pub uncached_tokens: u32,
    pub input_tokens: u32,
}

/// Full per-dispatch snapshot, so prefix overlap and progress are computed
/// from one consistent read.
#[derive(Debug, Clone)]
pub struct DispatchSnapshot {
    pub remaining: HashSet<u64>,
    pub dispatched_at: Instant,
    pub uncached_tokens: u32,
    pub input_tokens: u32,
}

impl DispatchSnapshot {
    /// Project to the `InflightSummary` view, without the hash set.
    pub fn summary(&self) -> InflightSummary {
        InflightSummary {
            dispatched_at: self.dispatched_at,
            uncached_tokens: self.uncached_tokens,
            input_tokens: self.input_tokens,
        }
    }
}

/// Per-endpoint tracker of dispatched prompt prefixes whose `BlockStored`
/// events haven't landed yet, so the picker can credit a worker for blocks
/// about to be in its KV cache.
///
/// The radix tree only reflects emitted `BlockStored` events, so it
/// under-reports overlap while a prior turn is still queued or prefilling.
/// See `picker::compute_layered_overlap`.
///
/// Same lifecycle and endpoint key (`score_addr`) as `InflightRegistry`,
/// which is just a count.
#[derive(Default)]
pub struct InflightPrefixes {
    /// Keyed by `request_id` so `forget(id)` is O(1). Per-endpoint order is
    /// recovered from `dispatched_at` at query time.
    dispatches: DashMap<u64, InflightDispatch>,
}

impl InflightPrefixes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every outstanding dispatch on `endpoint`, oldest first. Callers derive
    /// layered overlap, queue cost and progress from this one snapshot.
    pub fn dispatches_for(&self, endpoint: &str) -> Vec<DispatchSnapshot> {
        let mut matching: Vec<DispatchSnapshot> = self
            .dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| DispatchSnapshot {
                remaining: e.value().remaining.clone(),
                dispatched_at: e.value().dispatched_at,
                uncached_tokens: e.value().uncached_tokens,
                input_tokens: e.value().input_tokens,
            })
            .collect();
        matching.sort_by_key(|d| d.dispatched_at);
        matching
    }

    /// Record a dispatch. Called inside the `pick_lock` critical section so
    /// the next concurrent pick sees it.
    pub fn insert(
        &self,
        request_id: u64,
        endpoint: String,
        hashes: HashSet<u64>,
        uncached_tokens: u32,
        input_tokens: u32,
    ) {
        self.dispatches.insert(
            request_id,
            InflightDispatch {
                endpoint,
                remaining: hashes,
                dispatched_at: Instant::now(),
                uncached_tokens,
                input_tokens,
            },
        );
    }

    /// Remove stored hashes from every dispatch on `endpoint`, dropping
    /// drained ones.
    pub fn on_stored(&self, endpoint: &str, stored: &[u64]) {
        if stored.is_empty() {
            return;
        }
        let mut drained: Vec<u64> = Vec::new();
        for mut entry in self.dispatches.iter_mut() {
            if entry.value().endpoint != endpoint {
                continue;
            }
            let remaining = &mut entry.value_mut().remaining;
            for h in stored {
                remaining.remove(h);
            }
            if remaining.is_empty() {
                drained.push(*entry.key());
            }
        }
        // Removed after iterating.
        for id in drained {
            self.dispatches.remove(&id);
        }
    }

    /// Drop the dispatch for `request_id`. Idempotent. Called when the pick's
    /// observation is released, like `InflightRegistry::dec`.
    pub fn forget(&self, request_id: u64) {
        self.dispatches.remove(&request_id);
    }

    /// Drop dispatches older than `max_age`. Backstop against lifecycle
    /// leaks (lost `BlockStored`, stream dropped without observation).
    pub fn sweep(&self, max_age: Duration) {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .dispatches
            .iter()
            .filter(|e| now.duration_since(e.value().dispatched_at) > max_age)
            .map(|e| *e.key())
            .collect();
        for id in stale {
            self.dispatches.remove(&id);
        }
    }

    /// Pending dispatch count across all endpoints. Diagnostic only.
    pub fn len(&self) -> usize {
        self.dispatches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dispatches.is_empty()
    }
}

#[cfg(test)]
mod tests;
