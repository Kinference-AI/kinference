//! A `Pool` is the runtime state of exactly one worker set: what a scoring
//! decision consults and commits into.
//!
//! A worker set is declared as an `InferenceWorkerSet` or in the config, both
//! resolve to a `PoolSpec`, and a `Pool` is built from that: a radix tree at
//! one block size, the worker registry that populates it, the inflight
//! bookkeeping that must stay consistent with both, and the lock that
//! serializes commits into them.
//!
//! Pools are independent, 1:1 with worker sets, with no shared radix trees.
//! A single `Pick` may score against several, but they are never merged.
//!
//! Per pool:
//! - `predictor` / `picker`: coefficients and routing policy.
//! - `block_size`: KV hash granularity.
//! - `indexer`: a `KvIndexer` built with `block_size`.
//! - `registry`: workers, plus homogeneity enforcement (`canonical_models`).
//! - `inflight_prefixes` / `inflight`: shadow tracking of
//!   dispatched-but-unobserved work.
//! - `pick_lock`: so picks in different pools run concurrently.
//!
//! The metrics registry, observations, tokenization and response-id
//! minting are shared, on `KinferenceService`.
//!
//! [`PoolManager`] owns the pool lifecycle: create, update, retire, reap.

use std::collections::{BTreeMap, HashMap};
use std::error::Error as StdError;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use dynamo_kv_router::indexer::{KvIndexer, KvIndexerMetrics};
use kinference_policy::{PoolId, PoolKey};
use tokio_util::sync::CancellationToken;

use crate::discovery::{DiscoveryConfig, WorkerConfig};
use crate::error::Error;
use crate::eviction::EvictionTx;
use crate::inflight::{InflightPrefixes, InflightRegistry};
use crate::metrics::FleetMetrics;
use crate::models::ModelIndex;
use crate::profile::{PickerConfig, PredictorConfig, TokenizeMode};
use crate::sync::{read, write};
use crate::workers::WorkerRegistry;

/// A worker set with its profile resolved: everything needed to build a
/// `pool::Pool`, whether declared in the config or by an `InferenceWorkerSet`.
///
/// Reconciles compare specs with `PartialEq` to decide whether a pool needs
/// updating, and to decide whether a retired pool can be resurrected.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolSpec {
    pub key: PoolKey,
    pub name: String,
    pub profile: String,
    pub kv_block_size: u32,
    pub predictor: PredictorConfig,
    pub picker: PickerConfig,
    pub tokenize_mode: TokenizeMode,
    pub render_url: Option<String>,
    pub discovery: DiscoveryConfig,
    /// Per-set `/v1/models` refresh interval
    /// (`spec.modelDiscovery.interval`). `None` uses the fleet-wide
    /// `models_refresh_secs`.
    pub models_refresh_secs: Option<u64>,
    /// A fixed model list (`spec.modelDiscovery.mode: Static`); `/v1/models`
    /// is not polled. Lets engines that don't serve `/v1/models` be routable.
    pub static_models: Option<Vec<String>>,
}

/// An immutable view of the fleet's pools.
///
/// A snapshot rather than a concurrent map, so consumers that iterate (the
/// fleet gauges, `gather_facts`, `/readyz`) see one coherent pool set.
#[derive(Default)]
pub struct PoolMap {
    by_id: HashMap<PoolId, Arc<Pool>>,
    ordered: Vec<Arc<Pool>>,
}

impl PoolMap {
    pub fn from_pools(pools: Vec<Arc<Pool>>) -> Self {
        Self {
            by_id: pools.iter().map(|p| (p.id, p.clone())).collect(),
            ordered: pools,
        }
    }

    /// `None` once a pool is gone. Callers must degrade rather than panic, since
    /// an id can outlive its pool (a pending observation, a queued eviction).
    pub fn get(&self, id: PoolId) -> Option<&Arc<Pool>> {
        self.by_id.get(&id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Pool>> {
        self.ordered.iter()
    }

    pub fn as_slice(&self) -> &[Arc<Pool>] {
        &self.ordered
    }

    pub fn len(&self) -> usize {
        self.ordered.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }
}

/// The published pool set. Read on the pick hot path: take the read lock, clone
/// the `Arc`, release.
pub struct PoolRegistry {
    current: RwLock<Arc<PoolMap>>,
    /// Bumped on every publish. Policy resolution depends on which pools exist
    /// as well as on CRD events, so it re-resolves on this.
    generation: tokio::sync::watch::Sender<u64>,
}

impl Default for PoolRegistry {
    fn default() -> Self {
        Self {
            current: RwLock::new(Arc::new(PoolMap::default())),
            generation: tokio::sync::watch::channel(0).0,
        }
    }
}

impl PoolRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Wake up whenever the pool set changes.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.generation.subscribe()
    }

    pub fn from_pools(pools: Vec<Arc<Pool>>) -> Arc<Self> {
        let r = Self::new();
        r.publish(Arc::new(PoolMap::from_pools(pools)));
        r
    }

    pub fn snapshot(&self) -> Arc<PoolMap> {
        read(&self.current).clone()
    }

    pub fn publish(&self, map: Arc<PoolMap>) {
        *write(&self.current) = map;
        // After the write, so a woken consumer reads the new set rather than
        // racing the one it was told about.
        self.generation.send_modify(|g| *g = g.wrapping_add(1));
    }
}

/// The self-contained routing world for one homogeneous worker pool.
///
/// Built from a [`PoolSpec`] by [`PoolManager`] and
/// published into the [`PoolRegistry`]. A dropped pool survives as long as
/// requests still hold it, which lets the reaper tell exactly when it is safe
/// to cancel the token.
pub struct Pool {
    /// This pool's opaque, never-reused handle. See [`PoolId`].
    pub id: PoolId,
    /// The declaration this pool came from, stable across restarts and matched
    /// on by reconciles. See [`PoolKey`].
    pub key: PoolKey,
    /// Operator-facing name (from the `workerset` entry's `name`, or the
    /// `InferenceWorkerSet`'s). Unique across the fleet.
    pub name: String,
    /// The profile this pool's tuning came from, logged on each observation
    /// event.
    pub profile: String,
    /// KV-cache hash block size for every worker in this pool.
    pub block_size: u32,
    /// Latency-model coefficients for this pool.
    pub predictor: PredictorConfig,
    /// Routing policy for this pool.
    pub picker: PickerConfig,
    /// This pool's worker set + `by_model` index + homogeneity guard.
    pub registry: Arc<WorkerRegistry>,
    /// This pool's KV radix-tree index, built with `block_size`.
    pub indexer: Arc<KvIndexer>,
    /// Dispatched-but-not-yet-stored prefix tracker for this pool.
    pub inflight_prefixes: Arc<InflightPrefixes>,
    /// Per-endpoint shadow pick counter for this pool.
    pub inflight: Arc<InflightRegistry>,
    /// Serializes the score → pick → record critical section for this
    /// pool. Per-pool so picks in different pools don't contend.
    pub pick_lock: Arc<Mutex<()>>,
    /// This pool's primary tokenization path (Render/Local/Compare).
    pub tokenize_mode: TokenizeMode,
    /// This pool's dedicated `/render` URL, if configured. When `None`,
    /// render/compare rotate across the pool's own worker `http_url`s.
    pub tokenize_render_url: Option<String>,
    /// `/v1/models` scrape cadence for this pool's workers; `None` uses the
    /// fleet-wide setting.
    pub models_refresh_secs: Option<u64>,
    /// `Some` when this pool's model list is declared rather than discovered.
    /// Its workers are seeded with these names and never scraped for
    /// `/v1/models`.
    pub static_models: Option<Vec<String>>,
    /// Scopes everything this pool owns, including its `KvIndexer`'s OS
    /// thread. A child of the process token; see [`Pool::from_spec`].
    ///
    /// Cancelled by the reaper, not at retirement: `KvIndexer` unwraps a
    /// channel send, so anything still reaching it after cancellation panics.
    /// Discovery uses a separate token that does stop at retirement.
    pub cancel: CancellationToken,
}

impl Pool {
    /// Build a `Pool` from its config. Constructs a fresh `KvIndexer` sized to
    /// this pool's `block_size`, plus empty per-pool registries.
    ///
    /// `parent` is the process-wide token; the pool takes a child of it.
    /// `KvIndexer`'s `Drop` cancels the token it was given, so handing it the
    /// process token would shut down the whole process when a pool is dropped.
    /// Process shutdown still reaches every pool.
    pub fn from_spec(id: PoolId, spec: &PoolSpec, parent: &CancellationToken) -> Self {
        let cancel = parent.child_token();
        let kv_metrics = Arc::new(KvIndexerMetrics::new_unregistered());
        let indexer = Arc::new(KvIndexer::new(
            cancel.clone(),
            spec.kv_block_size,
            kv_metrics,
        ));
        Self {
            id,
            cancel,
            key: spec.key.clone(),
            name: spec.name.clone(),
            profile: spec.profile.clone(),
            block_size: spec.kv_block_size,
            predictor: spec.predictor.clone(),
            picker: spec.picker.clone(),
            registry: Arc::new(WorkerRegistry::new()),
            indexer,
            inflight_prefixes: Arc::new(InflightPrefixes::new()),
            inflight: Arc::new(InflightRegistry::new()),
            pick_lock: Arc::new(Mutex::new(())),
            tokenize_mode: spec.tokenize_mode,
            tokenize_render_url: spec.render_url.clone(),
            models_refresh_secs: spec.models_refresh_secs,
            static_models: spec.static_models.clone(),
        }
    }

    /// Rebuild this pool with new tuning, sharing every piece of live state.
    ///
    /// Keeps the same `PoolId`, `KvIndexer` (and so its warm prefix cache),
    /// worker registry, inflight trackers and token; only the tuning values
    /// change. Readers holding the old snapshot finish against the old values.
    ///
    /// `block_size` is baked into the `KvIndexer`, so changing it requires
    /// retire-and-create instead (debug-asserted).
    pub fn with_spec(&self, spec: &PoolSpec) -> Self {
        debug_assert_eq!(
            self.block_size, spec.kv_block_size,
            "block_size is baked into the KvIndexer; a change needs retire + create"
        );
        Self {
            id: self.id,
            key: spec.key.clone(),
            name: spec.name.clone(),
            profile: spec.profile.clone(),
            block_size: self.block_size,
            predictor: spec.predictor.clone(),
            picker: spec.picker.clone(),
            tokenize_mode: spec.tokenize_mode,
            tokenize_render_url: spec.render_url.clone(),
            models_refresh_secs: spec.models_refresh_secs,
            static_models: spec.static_models.clone(),
            // Shared, so the update keeps all live state.
            registry: self.registry.clone(),
            indexer: self.indexer.clone(),
            inflight_prefixes: self.inflight_prefixes.clone(),
            inflight: self.inflight.clone(),
            pick_lock: self.pick_lock.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

/// What one worker's tasks (the ingest listener, the metrics poller, the
/// warm-up) need from the worker, its pool and the coordinator. Built once
/// per worker by the coordinator and cloned into each task.
///
/// Holds the pool's handles rather than the `Pool` itself: the reaper takes a
/// retired pool as idle once nothing else holds it, and a per-worker task
/// may outlive retirement by a few seconds.
#[derive(Clone)]
pub struct WorkerContext {
    pub cfg: WorkerConfig,
    /// The worker's `score_addr`, the key the pick path and the ingest
    /// listener share for inflight prefixes.
    pub endpoint_key: String,
    pub pool_id: PoolId,
    pub pool_name: String,
    pub block_size: u32,
    pub registry: Arc<WorkerRegistry>,
    pub indexer: Arc<KvIndexer>,
    pub inflight_prefixes: Arc<InflightPrefixes>,
    pub prom: Arc<FleetMetrics>,
    pub models: Arc<ModelIndex>,
    pub eviction_tx: EvictionTx,
    /// The worker set declares its models. A `/v1/models` scrape then only
    /// reads metadata: it never claims models or evicts the worker.
    pub declared_models: bool,
    /// The per-worker token; cancelled when the worker is removed or evicted.
    pub cancel: CancellationToken,
}

/// Side effects that belong to subsystems the manager doesn't depend on
/// (discovery, the coordinator channel). Implemented by
/// `fleet::coordinator::Coordinator`.
pub trait PoolHooks: Send + Sync {
    /// Start everything that feeds a new pool: its discovery task and the
    /// forwarder that tags its `WorkerChange`s onto the shared coordinator
    /// channel.
    ///
    /// On `Err` the pool is not published.
    fn on_create(&self, pool: &Arc<Pool>, spec: &PoolSpec) -> Result<(), Error>;

    /// Drop every fleet-wide trace of this pool's workers: metrics samples,
    /// per-worker tasks.
    ///
    /// MUST NOT call into the pool's `KvIndexer`: its subtree goes away with
    /// the whole tree at reap, and calling `remove_worker` here would race the
    /// cancellation the reaper is about to do. MUST NOT cancel the pool token.
    fn on_retire(&self, pool: &Arc<Pool>);
}

/// When a retired pool may be reaped.
#[derive(Debug, Clone, Copy)]
pub struct ReapPolicy {
    /// Grace period after retirement before reaping is even considered, so a
    /// worker set that flaps can be resurrected with its cache intact.
    pub linger: Duration,
    /// Hard backstop: past this the pool is reaped regardless, with a warning,
    /// so a leaked `Arc<Pool>` can't pin an OS thread forever.
    pub backstop: Duration,
}

impl Default for ReapPolicy {
    fn default() -> Self {
        Self {
            linger: Duration::from_secs(30),
            backstop: Duration::from_secs(600),
        }
    }
}

enum CreateOutcome {
    Created,
    /// A retired pool with an identical spec came back: same `PoolId`, same
    /// radix tree, no re-warm.
    Resurrected,
    /// `on_create` refused; nothing was published.
    Failed,
}

struct Retired {
    pool: Arc<Pool>,
    spec: PoolSpec,
    at: Instant,
}

/// What one reconcile did. Returned for logging and for tests; the manager
/// itself does not act on it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub created: Vec<PoolKey>,
    pub resurrected: Vec<PoolKey>,
    /// Tuning changed; same pool, same cache, new coefficients.
    pub updated: Vec<PoolKey>,
    /// `kv_block_size` changed, which needs retire and create, losing the
    /// cache.
    pub replaced: Vec<PoolKey>,
    pub retired: Vec<PoolKey>,
    /// `on_create` failed; nothing was published.
    pub failed: Vec<PoolKey>,
}

impl ReconcileReport {
    pub fn is_noop(&self) -> bool {
        self == &Self::default()
    }
}

/// Pool lifecycle: create, update, retire, reap.
///
/// `PoolManager` is owned by a single reconcile task and takes `&mut self`, so
/// the live map needs no lock. The rest of the process reads the
/// [`PoolRegistry`] snapshot it publishes.
///
/// Teardown is two-phase, because the vendored `KvIndexer::apply_event` and
/// `remove_worker` `.unwrap()` a channel send and panic once the indexer's
/// token is cancelled:
///
/// 1. Retire, synchronously: publish a snapshot without the pool, drop its
///    model claims, stop its discovery, evict its workers without
///    touching the indexer, and move it to `retired`. The token stays live.
/// 2. Reap, on a sweep: once nothing else holds the pool and a linger has
///    passed, cancel the token (the indexer thread exits) and drop it.
///
/// Nothing outlives `pick_and_record` holding an `Arc<Pool>` (a pending
/// observation holds the registries directly), so `strong_count == 1` means
/// no request is inside the pool.
///
/// A watch reconnect or `delete --all && apply` retires and recreates a worker
/// set within seconds, so create first looks in `retired` for the same
/// [`PoolKey`] with an identical spec and resurrects it in place, keeping its
/// `PoolId`, radix tree and registry.
pub struct PoolManager {
    registry: Arc<PoolRegistry>,
    /// The live set, ordered so replicas publish pools in the same order.
    live: BTreeMap<PoolKey, (Arc<Pool>, PoolSpec)>,
    retired: Vec<Retired>,
    hooks: Arc<dyn PoolHooks>,
    /// Process-wide token; every pool takes a child of it.
    parent: tokio_util::sync::CancellationToken,
    models: Arc<ModelIndex>,
    reap: ReapPolicy,
}

impl PoolManager {
    pub fn new(
        registry: Arc<PoolRegistry>,
        hooks: Arc<dyn PoolHooks>,
        parent: tokio_util::sync::CancellationToken,
        models: Arc<ModelIndex>,
        reap: ReapPolicy,
    ) -> Self {
        Self {
            registry,
            live: BTreeMap::new(),
            retired: Vec::new(),
            hooks,
            parent,
            models,
            reap,
        }
    }

    /// Bring the live set in line with `desired`.
    ///
    /// An empty `desired` retires everything, so the caller must not reconcile
    /// before its watch has listed.
    pub fn reconcile(&mut self, desired: Vec<PoolSpec>) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let wanted: BTreeMap<PoolKey, PoolSpec> =
            desired.into_iter().map(|s| (s.key.clone(), s)).collect();

        // Retire first, so a replace frees its workers' fleet-wide state before
        // the replacement claims the same ids.
        let going: Vec<PoolKey> = self
            .live
            .keys()
            .filter(|k| !wanted.contains_key(*k))
            .cloned()
            .collect();
        for key in going {
            self.retire(&key);
            report.retired.push(key);
        }

        for (key, spec) in wanted {
            match self.live.get(&key) {
                Some((_, live_spec)) if *live_spec == spec => {}
                Some((pool, _)) if pool.block_size != spec.kv_block_size => {
                    // The indexer can't change block size, so this discards
                    // the set's prefix cache.
                    tracing::info!(
                        workerset = %key,
                        from = pool.block_size,
                        to = spec.kv_block_size,
                        "kvBlockSize changed; replacing the worker set and its prefix cache"
                    );
                    self.retire(&key);
                    match self.create(spec) {
                        CreateOutcome::Failed => report.failed.push(key),
                        // May resurrect a matching retired entry.
                        _ => report.replaced.push(key),
                    }
                }
                Some((pool, live_spec)) => {
                    // A declared model list is per-worker state in the shared
                    // registry, so push a change into the existing workers.
                    if live_spec.static_models != spec.static_models {
                        let touched = pool
                            .registry
                            .reset_declared_models(spec.static_models.as_deref());
                        self.models.release(pool.id, &pool.name);
                        if let Some(models) = spec.static_models.as_deref() {
                            self.models.associate(pool.id, &pool.name, models);
                        }
                        tracing::info!(
                            workerset = %key,
                            workers = touched,
                            models = ?spec.static_models,
                            "declared models changed"
                        );
                    }
                    // Copy-on-write: new values, same id, same warm cache.
                    let next = Arc::new(pool.with_spec(&spec));
                    tracing::info!(
                        workerset = %key,
                        profile = %spec.profile,
                        "worker set updated"
                    );
                    self.live.insert(key.clone(), (next, spec));
                    self.publish();
                    report.updated.push(key);
                }
                None => match self.create(spec) {
                    CreateOutcome::Created => report.created.push(key),
                    CreateOutcome::Resurrected => report.resurrected.push(key),
                    CreateOutcome::Failed => report.failed.push(key),
                },
            }
        }
        report
    }

    /// Reap retired pools that nothing is using any more.
    ///
    /// Returns how many were reaped. Only touches the retired list, normally
    /// empty.
    pub fn sweep(&mut self) -> usize {
        let (linger, backstop) = (self.reap.linger, self.reap.backstop);
        let mut reaped = 0;
        self.retired.retain(|r| {
            let age = r.at.elapsed();
            // `strong_count == 1` means this list holds the only reference:
            // no pick is inside the pool and no snapshot still lists it.
            let idle = Arc::strong_count(&r.pool) == 1;
            if age < linger || (!idle && age < backstop) {
                return true;
            }
            if !idle {
                tracing::warn!(
                    workerset = %r.pool.key,
                    strong_count = Arc::strong_count(&r.pool),
                    age_secs = age.as_secs(),
                    "reaping a retired worker set that still has handles"
                );
            }
            // Cancelled here, not at retire; see the module docs.
            r.pool.cancel.cancel();
            tracing::debug!(workerset = %r.pool.key, age_secs = age.as_secs(), "worker set reaped");
            reaped += 1;
            false
        });
        reaped
    }

    /// Live pools, for logging and tests.
    pub fn live_keys(&self) -> Vec<PoolKey> {
        self.live.keys().cloned().collect()
    }

    #[cfg(test)]
    fn retired_keys(&self) -> Vec<PoolKey> {
        self.retired.iter().map(|r| r.pool.key.clone()).collect()
    }

    #[cfg(test)]
    fn live_pool(&self, key: &PoolKey) -> Option<&Arc<Pool>> {
        self.live.get(key).map(|(p, _)| p)
    }

    /// Bring a worker set into service, reusing a retired pool when one matches
    /// exactly.
    fn create(&mut self, spec: PoolSpec) -> CreateOutcome {
        let (pool, resurrected) = match self.take_resurrectable(&spec) {
            Some(pool) => (pool, true),
            None => (
                Arc::new(Pool::from_spec(PoolId::next(), &spec, &self.parent)),
                false,
            ),
        };
        // Hooks before publishing, so a published pool is fully wired.
        if let Err(err) = self.hooks.on_create(&pool, &spec) {
            tracing::error!(
                workerset = %spec.key,
                error = &err as &dyn StdError,
                "failed to start the worker set's discovery"
            );
            if resurrected {
                // Put it back so it still gets reaped.
                self.retired.push(Retired {
                    pool,
                    spec,
                    at: Instant::now(),
                });
            }
            return CreateOutcome::Failed;
        }
        tracing::info!(
            workerset = %spec.key,
            profile = %spec.profile,
            block_size = spec.kv_block_size,
            resurrected,
            "worker set created"
        );
        self.live.insert(spec.key.clone(), (pool, spec));
        self.publish();
        if resurrected {
            CreateOutcome::Resurrected
        } else {
            CreateOutcome::Created
        }
    }

    /// Pull a retired pool back if its spec is unchanged.
    ///
    /// A changed spec doesn't resurrect: the retired pool's indexer and
    /// registry belong to the old one.
    fn take_resurrectable(&mut self, spec: &PoolSpec) -> Option<Arc<Pool>> {
        let idx = self
            .retired
            .iter()
            .position(|r| r.pool.key == spec.key && r.spec == *spec)?;
        Some(self.retired.remove(idx).pool)
    }

    fn retire(&mut self, key: &PoolKey) {
        let Some((pool, spec)) = self.live.remove(key) else {
            return;
        };
        // 1. Stop routing to it, so no new pick enters during teardown.
        self.publish();
        // 2. Drop its model claims.
        self.models.release(pool.id, &pool.name);
        // 3. Fleet-wide worker state (metrics, per-worker tasks), without
        //    touching the indexer; see `PoolHooks::on_retire`.
        self.hooks.on_retire(&pool);
        tracing::info!(
            workerset = %key,
            strong_count = Arc::strong_count(&pool),
            "worker set retired"
        );
        self.retired.push(Retired {
            pool,
            spec,
            at: Instant::now(),
        });
    }

    fn publish(&self) {
        let pools: Vec<Arc<Pool>> = self.live.values().map(|(p, _)| p.clone()).collect();
        self.registry.publish(Arc::new(PoolMap::from_pools(pools)));
    }
}

/// One `Pool` for tests, from a minimal static spec.
#[cfg(any(test, feature = "test-util"))]
pub fn test_pool(name: &str) -> Arc<Pool> {
    test_pool_with_block_size(name, 16)
}

/// A test pool with a given block size, which sets `total_blocks` and so the
/// prefill term, making pools predict differently for one prompt.
#[cfg(any(test, feature = "test-util"))]
pub fn test_pool_with_block_size(name: &str, block_size: u32) -> Arc<Pool> {
    Arc::new(Pool::from_spec(
        PoolId::next(),
        &test_spec(name, block_size),
        &CancellationToken::new(),
    ))
}

/// A minimal spec shared by tests that need a `Pool`.
#[cfg(any(test, feature = "test-util"))]
pub fn test_spec(name: &str, kv_block_size: u32) -> PoolSpec {
    PoolSpec {
        key: PoolKey::local(name),
        name: name.to_string(),
        profile: "test-profile".to_string(),
        kv_block_size,
        predictor: PredictorConfig::default(),
        picker: PickerConfig::default(),
        tokenize_mode: TokenizeMode::default(),
        render_url: None,
        discovery: DiscoveryConfig::default(),
        models_refresh_secs: None,
        static_models: None,
    }
}

#[cfg(test)]
mod tests;
