use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use dashmap::DashMap;
use serde::Deserialize;

/// Verdict returned by [`WorkerRegistry::set_served_models`].
///
/// The canonical model set catches two different fleets merged into one pool,
/// which would corrupt the prefix model since block hashes are pool-scoped. It
/// doesn't freeze the model list: workers gaining or lacking models (LoRA
/// adapters loaded at runtime) are distinguished from a genuine conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServedModelsVerdict {
    /// Models matched canonical exactly (or canonical was empty and this report
    /// set it). `by_model` was updated for this worker.
    Accepted,
    /// This worker reports a superset of canonical: it gained models. Canonical
    /// has been widened to match and `by_model` updated. A LoRA adapter being
    /// loaded, or an engine finishing a lazy load.
    Widened { added: Vec<String> },
    /// This worker reports a subset of canonical: it is missing some. Canonical
    /// is left alone and `by_model` reflects only what this worker actually
    /// serves, so nothing routes to it for the missing models. An adapter not
    /// loaded here yet, mid-rollout, or unloaded.
    Narrowed { missing: Vec<String> },
    /// `new_models` was empty: the engine isn't ready yet. Nothing was updated.
    Empty,
    /// `new_models` conflicts with canonical: each has something the other
    /// lacks, so this is a different fleet. `by_model` was not updated; the
    /// caller should evict.
    Mismatch {
        expected: Vec<String>,
        observed: Vec<String>,
    },
}

use crate::discovery::WorkerConfig;
use crate::sync::lock;

/// Extract `host:port` from an engine HTTP base URL.
/// `http://127.0.0.1:8000/` → `127.0.0.1:8000`. Returns `None` if the
/// authority can't be isolated.
fn host_port_from_http_url(url: &str) -> Option<String> {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    let authority = after_scheme.split('/').next().unwrap_or("");
    (!authority.is_empty()).then(|| authority.to_string())
}

/// A pool's workers, keyed by the numeric `WorkerId` `dynamo-kv-router` uses.
///
/// `by_model` is populated from each worker's `/v1/models` scrape (via
/// [`Self::set_served_models`]); a worker may serve several models (LoRA
/// adapters). A non-empty `WorkerConfig.model_name` is an initial hint,
/// routable until the first scrape replaces it.
#[derive(Debug, Default)]
pub struct WorkerRegistry {
    by_num: DashMap<u64, WorkerEntry>,
    /// `model_name -> {worker_id_num}`. One mutex; updates are rare and
    /// reads are cheap. A caller that holds it with a `by_num` guard takes it
    /// first, so the two can't deadlock.
    by_model: Mutex<HashMap<String, HashSet<u64>>>,
    /// Canonical set of model names this fleet serves; see
    /// [`ServedModelsVerdict`]. Pinned by the first non-empty report, and
    /// cleared when the registry becomes empty so the next report re-pins it.
    canonical_models: Mutex<Option<BTreeSet<String>>>,
}

#[derive(Debug, Clone)]
pub struct WorkerEntry {
    pub worker_id_str: String,
    /// Bootstrap hint from `WorkerConfig.model_name`,
    /// superseded by `current_models` once `/v1/models` lands.
    pub model_name_hint: Option<String>,
    /// Models this worker is currently serving, as reported by the latest
    /// `/v1/models` scrape. Empty until the first successful scrape.
    pub current_models: Vec<String>,
    /// HTTP base URL for the engine's OpenAI-compatible `/tokenize`. `None`
    /// if not configured.
    pub http_url: Option<String>,
    /// KV-cache hash block size, the worker set's `kv_block_size`. A worker
    /// whose events report a different value is evicted (see
    /// `ingest::BlockSizeGuard`).
    pub block_size: u32,
    /// True once the ZMQ listener has finished its initial replay-drain and is
    /// receiving live. Part of [`Self::is_ready`]. Sticky across libzmq
    /// reconnects.
    pub initial_sync_complete: bool,
    /// True once kinference has decoded a KV cache event from this worker (see
    /// [`WorkerRegistry::note_kv_events_observed`]). Reported in
    /// `status.kvEvents.subscribedEndpoints`; not part of readiness.
    pub kv_events_observed: bool,
    /// The endpoint returned to callers. Must be `ip:port`, since gateways
    /// route to it verbatim without resolving hostnames:
    ///
    /// 1. `WorkerConfig.endpoint` if set, for when `http_url` is a hostname.
    /// 2. Else `host:port` from `http_url` (the pod IP).
    /// 3. Else `worker_id_str`, to keep it non-empty.
    pub score_addr: String,
    /// Engine identity, from `/v1/models` `owned_by`. Diagnostics only.
    pub engine: Option<Engine>,
    // --- Placement metadata, loaded but not yet consumed by routing. ---
    pub pod_name: Option<String>,
    pub namespace: Option<String>,
    pub node_name: Option<String>,
    pub node_ip: Option<String>,
    pub pod_ip: Option<String>,
    pub availability_zone: Option<String>,
    pub instance_type: Option<String>,
}

impl WorkerEntry {
    /// True when this worker can be picked: `/v1/models` has been scraped and
    /// the ZMQ listener has finished its initial sync. `score_workers` only
    /// considers ready workers, and `/readyz` passes when any worker is ready.
    ///
    /// A block-size mismatch evicts the worker rather than failing readiness.
    pub fn is_ready(&self) -> bool {
        self.initial_sync_complete && !self.current_models.is_empty()
    }

    /// Build a `WorkerEntry` from a `WorkerConfig`, with `model_name` as the
    /// hint and `block_size` from the worker set.
    pub fn from_config(w: &WorkerConfig, block_size: u32) -> Self {
        // See the `score_addr` field doc.
        let score_addr = w
            .endpoint
            .clone()
            .or_else(|| w.http_url.as_deref().and_then(host_port_from_http_url))
            .unwrap_or_else(|| w.worker_id_str.clone());
        let hint = (!w.model_name.is_empty()).then(|| w.model_name.clone());
        Self {
            worker_id_str: w.worker_id_str.clone(),
            model_name_hint: hint,
            current_models: Vec::new(),
            http_url: w.http_url.clone(),
            block_size,
            initial_sync_complete: false,
            kv_events_observed: false,
            score_addr,
            engine: None,
            pod_name: w.pod_name.clone(),
            namespace: w.namespace.clone(),
            node_name: w.node_name.clone(),
            node_ip: w.node_ip.clone(),
            pod_ip: w.pod_ip.clone(),
            availability_zone: w.availability_zone.clone(),
            instance_type: w.instance_type.clone(),
        }
    }
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a worker. Returns `false` if `worker_id_num` is already
    /// registered.
    pub fn register(&self, worker_id_num: u64, entry: WorkerEntry) -> bool {
        if self.by_num.contains_key(&worker_id_num) {
            return false;
        }
        // Seed by_model from the hint until the first scrape.
        if entry.current_models.is_empty() {
            if let Some(hint) = entry.model_name_hint.as_deref() {
                let mut by_model = lock(&self.by_model);
                by_model
                    .entry(hint.to_string())
                    .or_default()
                    .insert(worker_id_num);
            }
        } else {
            let mut by_model = lock(&self.by_model);
            for m in &entry.current_models {
                by_model.entry(m.clone()).or_default().insert(worker_id_num);
            }
        }
        self.by_num.insert(worker_id_num, entry);
        true
    }

    /// Remove a worker from the registry and drop its membership from every
    /// model bucket. Safe to call on a worker that was never registered
    /// (returns `false`).
    ///
    /// If the registry becomes empty, also clears the canonical model set, so
    /// a rolling model swap converges once the last old-model worker leaves.
    pub fn unregister(&self, worker_id_num: u64) -> bool {
        let removed = self.by_num.remove(&worker_id_num);
        if removed.is_none() {
            return false;
        }
        {
            let mut by_model = lock(&self.by_model);
            // Cheap: a fleet serves few distinct models.
            by_model.retain(|_model, set| {
                set.remove(&worker_id_num);
                !set.is_empty()
            });
        }
        if self.by_num.is_empty() {
            let mut canonical = lock(&self.canonical_models);
            *canonical = None;
        }
        true
    }

    /// Update the worker's served models after a `/v1/models` scrape, checked
    /// against the canonical set (see [`ServedModelsVerdict`]). Idempotent.
    ///
    /// A bootstrap hint the live models don't include is dropped silently.
    pub fn set_served_models(
        &self,
        worker_id_num: u64,
        new_models: &[String],
    ) -> ServedModelsVerdict {
        let mut by_model = lock(&self.by_model);
        let Some(mut entry_ref) = self.by_num.get_mut(&worker_id_num) else {
            // Unregistered between scrape and update.
            return ServedModelsVerdict::Empty;
        };

        // The engine hasn't loaded its models yet.
        if new_models.is_empty() {
            return ServedModelsVerdict::Empty;
        }

        let new_set: BTreeSet<String> = new_models.iter().cloned().collect();
        // Only a genuine conflict returns early.
        let relation = {
            let mut canonical = lock(&self.canonical_models);
            match canonical.as_ref() {
                None => {
                    *canonical = Some(new_set.clone());
                    ServedModelsVerdict::Accepted
                }
                Some(pinned) if pinned == &new_set => ServedModelsVerdict::Accepted,
                Some(pinned) if new_set.is_superset(pinned) => {
                    // Widen, so the next worker reporting the same set isn't a
                    // conflict.
                    let added: Vec<String> = new_set.difference(pinned).cloned().collect();
                    *canonical = Some(new_set.clone());
                    ServedModelsVerdict::Widened { added }
                }
                Some(pinned) if new_set.is_subset(pinned) => {
                    // Don't narrow canonical: the other workers still serve the
                    // rest.
                    let missing: Vec<String> = pinned.difference(&new_set).cloned().collect();
                    ServedModelsVerdict::Narrowed { missing }
                }
                Some(pinned) => {
                    let expected: Vec<String> = pinned.iter().cloned().collect();
                    let observed: Vec<String> = new_set.iter().cloned().collect();
                    return ServedModelsVerdict::Mismatch { expected, observed };
                }
            }
        };

        let entry = entry_ref.value_mut();

        // First scrape: replace the hint with live data.
        if entry.current_models.is_empty()
            && let Some(hint) = entry.model_name_hint.as_deref()
            && let Some(set) = by_model.get_mut(hint)
        {
            set.remove(&worker_id_num);
            if set.is_empty() {
                by_model.remove(hint);
            }
        }

        // Diff current vs new.
        let old: HashSet<&String> = entry.current_models.iter().collect();
        let new: HashSet<&String> = new_models.iter().collect();

        let to_remove: Vec<String> = old.difference(&new).map(|s| (*s).clone()).collect();
        let to_add: Vec<String> = new.difference(&old).map(|s| (*s).clone()).collect();

        if !to_remove.is_empty() || !to_add.is_empty() {
            for m in &to_remove {
                if let Some(set) = by_model.get_mut(m) {
                    set.remove(&worker_id_num);
                    if set.is_empty() {
                        by_model.remove(m);
                    }
                }
            }
            for m in &to_add {
                by_model.entry(m.clone()).or_default().insert(worker_id_num);
            }
        }

        entry.current_models = new_models.to_vec();
        // `by_model` reflects what this worker serves, whatever the relation.
        relation
    }

    /// How many workers currently serve `model`.
    #[cfg(test)]
    pub fn by_model_len(&self, model: &str) -> usize {
        lock(&self.by_model).get(model).map_or(0, |s| s.len())
    }

    /// Re-apply a declared model list to every worker in this set, replacing
    /// the canonical pin.
    ///
    /// For `modelDiscovery.mode: Static`: an edit to `spec.models` keeps this
    /// registry (and the prefix cache), so the pin and each worker's
    /// `current_models` must be updated here. The pin is replaced, not
    /// cleared, so the next worker to report can't decide the set.
    ///
    /// `None` means back to endpoint discovery: drop the pin.
    pub fn reset_declared_models(&self, models: Option<&[String]>) -> usize {
        let Some(models) = models else {
            *lock(&self.canonical_models) = None;
            return 0;
        };
        let pinned: BTreeSet<String> = models.iter().cloned().collect();
        // Pin first, so a concurrent scrape is checked against the new list.
        *lock(&self.canonical_models) = Some(pinned);

        let mut by_model = lock(&self.by_model);
        let mut touched = 0;
        for mut entry in self.by_num.iter_mut() {
            let num = *entry.key();
            for m in entry.value().current_models.iter() {
                if let Some(set) = by_model.get_mut(m) {
                    set.remove(&num);
                }
            }
            entry.value_mut().current_models = models.to_vec();
            for m in models {
                by_model.entry(m.clone()).or_default().insert(num);
            }
            touched += 1;
        }
        by_model.retain(|_, set| !set.is_empty());
        touched
    }

    /// Update the worker's detected engine. Called once per worker after
    /// `/v1/models` returns a recognized `owned_by`. No-op if the worker
    /// has been unregistered.
    pub fn set_engine(&self, worker_id_num: u64, engine: Engine) {
        if let Some(mut entry) = self.by_num.get_mut(&worker_id_num) {
            entry.value_mut().engine = Some(engine);
        }
    }

    /// Mark the worker's initial ZMQ sync as complete. Sticky. Returns `true`
    /// if the entry existed.
    pub fn set_initial_sync_complete(&self, worker_id_num: u64) -> bool {
        match self.by_num.get_mut(&worker_id_num) {
            Some(mut entry) => {
                entry.value_mut().initial_sync_complete = true;
                true
            }
            None => false,
        }
    }

    /// Record that a KV cache event actually arrived from this worker.
    ///
    /// Unlike [`Self::set_initial_sync_complete`] (a ZMQ `connect()` succeeds
    /// even with no publisher), this is only set by a decoded frame.
    ///
    /// Sticky, since an idle fleet sends no events. Callers should gate this on
    /// a local flag so it is written once per listener, not per event.
    pub fn note_kv_events_observed(&self, worker_id_num: u64) -> bool {
        match self.by_num.get_mut(&worker_id_num) {
            Some(mut entry) => {
                entry.value_mut().kv_events_observed = true;
                true
            }
            None => false,
        }
    }

    /// True when any worker is ready; see [`WorkerEntry::is_ready`]. Drives
    /// `/readyz`.
    pub fn any_ready(&self) -> bool {
        self.by_num.iter().any(|e| e.value().is_ready())
    }

    /// Snapshot read of a single worker. Returns an owned clone so the
    /// caller doesn't hold a DashMap guard across awaits.
    pub fn lookup(&self, worker_id_num: u64) -> Option<WorkerEntry> {
        self.by_num.get(&worker_id_num).map(|e| e.value().clone())
    }

    /// Does any worker serve `model_name`?
    pub fn has_model(&self, model_name: &str) -> bool {
        let by_model = lock(&self.by_model);
        by_model
            .get(model_name)
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    }

    /// Snapshot of `(worker_id_num, WorkerEntry)` pairs for every worker
    /// that currently serves `model_name`. Entries are cloned to release
    /// the DashMap guard before the caller might `.await` over them. Order
    /// is unspecified (HashSet → Vec).
    pub fn workers_for_model(&self, model_name: &str) -> Vec<(u64, WorkerEntry)> {
        let nums: Vec<u64> = {
            let by_model = lock(&self.by_model);
            match by_model.get(model_name) {
                None => return Vec::new(),
                Some(set) => set.iter().copied().collect(),
            }
        };
        nums.into_iter()
            .filter_map(|n| self.by_num.get(&n).map(|e| (n, e.value().clone())))
            .collect()
    }

    /// Distinct model names this registry currently serves.
    pub fn known_models(&self) -> Vec<String> {
        lock(&self.by_model).keys().cloned().collect()
    }

    /// Snapshot of every `(worker_id_num, WorkerEntry)`, as owned clones.
    pub fn iter_snapshot(&self) -> Vec<(u64, WorkerEntry)> {
        self.by_num
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }

    /// Just the ids, without cloning every entry.
    ///
    /// For sweeps that don't need the entries, which `iter_snapshot` clones.
    pub fn ids(&self) -> Vec<u64> {
        self.by_num.iter().map(|e| *e.key()).collect()
    }

    pub fn len(&self) -> usize {
        self.by_num.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_num.is_empty()
    }
}

/// A worker's inference engine, detected from the `owned_by` field of
/// `/v1/models`. Only used for display.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    #[default]
    Vllm,
    Sglang,
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Engine::Vllm => "vllm",
            Engine::Sglang => "sglang",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests;
