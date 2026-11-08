//! The worker lifecycle: the loop that takes discovery and eviction events
//! and adds or drops workers, and the hooks that start and stop a pool's
//! discovery.
//!
//! Fleet-wide state (the scrape samples, the worker-to-pool index) is keyed
//! by `worker_id_num` alone, so the first pool to register a worker owns it
//! and only the owner may drop it.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use dynamo_kv_router::indexer::KvIndexerInterface;
use kinference_policy::PoolId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::discovery::{DiscoveryFactory, WorkerChange, WorkerConfig};
use crate::error::Error;
use crate::eviction::{self, EvictionReason};
use crate::ingest::{self, IngestConfig};
use crate::metrics::FleetMetrics;
use crate::models::ModelIndex;
use crate::pool::{Pool, PoolHooks, PoolSpec, WorkerContext};
use crate::scrape::{MetricsRegistry, spawn_metrics_poller, spawn_one_shot_warmup};
use crate::workers::{ServedModelsVerdict, WorkerEntry};

/// Shared handles, built by `Fleet::new`.
pub struct Handles {
    /// Process token. Pool and discovery tokens are its children.
    pub cancel: CancellationToken,
    /// Builds a pool's Kubernetes discovery; `None` when no worker set
    /// needs it or credentials were unavailable at startup.
    pub kubernetes: Option<DiscoveryFactory>,
    pub metrics: Arc<MetricsRegistry>,
    pub prom: Arc<FleetMetrics>,
    pub models: Arc<ModelIndex>,
}

/// Settings for the per-worker tasks, from `Config`.
#[derive(Debug, Clone, Copy)]
pub struct WorkerTaskSettings {
    pub poll_ms: u64,
    pub models_refresh_secs: u64,
    pub ingest: IngestConfig,
    /// TTL of a pick's in-flight bookkeeping; bounds the per-pool
    /// inflight-prefix sweep.
    pub observation_ttl: Duration,
}

/// The receiving ends of the coordinator's two channels, drained by
/// [`Coordinator::run`].
pub struct Inbox {
    changes: mpsc::Receiver<PoolChange>,
    evictions: eviction::EvictionRx,
}

/// A discovery event, tagged with its pool and with the discovery token of
/// the run that produced it. `run` drops the event once that token is
/// cancelled, so a retired pool's queued events can't re-register workers.
type PoolChange = (Arc<Pool>, CancellationToken, WorkerChange);

/// Owns the worker lifecycle: registers and tears down workers as discovery
/// and eviction events arrive, and implements [`PoolHooks`] so a pool's
/// discovery starts when it is created and stops when it retires.
pub struct Coordinator {
    cancel: CancellationToken,
    kubernetes: Option<DiscoveryFactory>,
    metrics: Arc<MetricsRegistry>,
    prom: Arc<FleetMetrics>,
    models: Arc<ModelIndex>,
    settings: WorkerTaskSettings,
    /// Each pool runs its own discovery; a per-pool forwarder tags its
    /// events with the pool and sends them here.
    coord_tx: mpsc::Sender<PoolChange>,
    /// Ingest and the scrape request a worker's eviction here on a
    /// homogeneity violation; `run` does the same teardown as for `Removed`.
    eviction_tx: eviction::EvictionTx,
    /// Per-worker cancellation tokens, so `Removed` stops one worker's tasks
    /// without disturbing the rest of the fleet. Keyed by (pool, worker), so
    /// when a worker moves between worker sets the old pool's `Removed`
    /// doesn't cancel the new pool's listeners.
    task_tokens: DashMap<(PoolId, u64), CancellationToken>,
    /// Which pool owns each worker, for eviction and the observation
    /// lifecycle.
    worker_pool: DashMap<u64, Arc<Pool>>,
    /// Per-pool token scoping that pool's discovery, forwarder, and prefix
    /// sweep.
    ///
    /// Not `pool.cancel`, which survives retirement until the reaper fires
    /// (the `KvIndexer` panics if used after cancellation). Discovery must
    /// stop the moment the pool retires.
    discovery_tokens: DashMap<PoolId, CancellationToken>,
}

impl Coordinator {
    pub fn new(handles: Handles, settings: WorkerTaskSettings) -> (Arc<Self>, Inbox) {
        let (coord_tx, changes) = mpsc::channel(64);
        let (eviction_tx, evictions) = eviction::channel();
        let coordinator = Arc::new(Self {
            cancel: handles.cancel,
            kubernetes: handles.kubernetes,
            metrics: handles.metrics,
            prom: handles.prom,
            models: handles.models,
            settings,
            coord_tx,
            eviction_tx,
            task_tokens: DashMap::new(),
            worker_pool: DashMap::new(),
            discovery_tokens: DashMap::new(),
        });
        (coordinator, Inbox { changes, evictions })
    }

    /// Dispatch each event to its pool's state stores. The coordinator holds
    /// a sender for each channel, so neither closes while this runs; the
    /// per-worker tasks stop through their tokens.
    pub async fn run(self: Arc<Self>, mut inbox: Inbox) {
        loop {
            tokio::select! {
                biased;
                Some(req) = inbox.evictions.recv() => {
                    // Resolve the worker's pool; a worker we no longer track
                    // (already evicted) is skipped.
                    let owner = self
                        .worker_pool
                        .get(&req.worker_id_num)
                        .map(|e| e.value().clone());
                    if let Some(pool) = owner {
                        // Counted here, the only path with a reason, and
                        // after the lookup so a gone worker isn't counted.
                        self.prom.record_worker_eviction(req.reason.as_label());
                        self.evict_worker(&pool, req.worker_id_num, Some(req.reason))
                            .await;
                    }
                }
                change = inbox.changes.recv() => {
                    let Some((pool, discovery, change)) = change else {
                        unreachable!("the coordinator holds a discovery sender");
                    };
                    // Queued before the pool retired. `on_retire` already
                    // dropped the pool's workers, and the indexer may be
                    // reaped soon.
                    if discovery.is_cancelled() {
                        tracing::debug!(
                            workerset = pool.name,
                            "dropping a retired worker set's discovery event"
                        );
                        continue;
                    }
                    match change {
                        WorkerChange::Added(w) => self.add_worker(&pool, w),
                        WorkerChange::Removed(num) => self.evict_worker(&pool, num, None).await,
                    }
                }
            }
        }
    }

    /// Handle a `WorkerChange::Added`: register the worker across all state
    /// stores, spawn its ZMQ listener and metrics poller under a per-worker
    /// cancel token, and kick a one-shot `/v1/models` scrape so the worker
    /// is routable before the next periodic tick.
    fn add_worker(&self, pool: &Arc<Pool>, w: WorkerConfig) {
        let block_size = pool.block_size;
        let registry = &pool.registry;
        tracing::info!(
            workerset = pool.name,
            worker_id = w.worker_id_str,
            block_size,
            zmq_endpoint = w.zmq_endpoint.as_deref().unwrap_or("-"),
            http_url = w.http_url.as_deref().unwrap_or("-"),
            "worker added"
        );

        // Two worker sets selecting one pod. Must bail: `worker_id_num` keys
        // fleet-wide state the first registration owns. The fix is a Service
        // selector.
        if let Some(owner) = self.worker_pool.get(&w.worker_id_num)
            && owner.value().id != pool.id
        {
            tracing::warn!(
                workerset = pool.name,
                worker_id = w.worker_id_str,
                owner = owner.value().name,
                "worker already belongs to another worker set; skipping"
            );
            return;
        }

        // Provisionally route the model_name hint, before the first
        // /v1/models scrape.
        self.worker_pool.insert(w.worker_id_num, pool.clone());
        if !w.model_name.is_empty() {
            self.models
                .associate(pool.id, &pool.name, std::slice::from_ref(&w.model_name));
        }

        // Seeded from config.model_name; `by_model` is updated by the
        // metrics poller and warmup.
        let entry = WorkerEntry::from_config(&w, block_size);
        // The key the pick path uses for inflight state, which the ZMQ
        // listener also keys `on_stored` by.
        let endpoint_key = entry.score_addr.clone();
        if !registry.register(w.worker_id_num, entry) {
            tracing::warn!(
                workerset = pool.name,
                worker_id = w.worker_id_str,
                "worker id already registered; skipping"
            );
            self.worker_pool.remove(&w.worker_id_num);
            return;
        }

        let worker_token = self.cancel.child_token();
        self.task_tokens
            .insert((pool.id, w.worker_id_num), worker_token.clone());

        // A declared model list is authoritative: seed it and skip
        // `/v1/models`, for engines that don't serve it.
        if let Some(declared) = pool.static_models.as_deref() {
            if declared.is_empty() {
                tracing::warn!(
                    workerset = pool.name,
                    worker_id = w.worker_id_str,
                    "modelDiscovery.mode is Static but spec.models is empty"
                );
            } else {
                // `set_served_models` refuses on a mismatch, so only claim
                // the models when it accepted them.
                match registry.set_served_models(w.worker_id_num, declared) {
                    // Every worker gets the same declared list, so these mean
                    // the pin outlived a `spec.models` edit
                    // (`reset_declared_models` didn't run). Still routable.
                    ServedModelsVerdict::Widened { .. } | ServedModelsVerdict::Narrowed { .. } => {
                        tracing::warn!(
                            workerset = pool.name,
                            worker_id = w.worker_id_str,
                            "declared models differ from the worker set's; routing on the declared models"
                        );
                        self.models.associate(pool.id, &pool.name, declared);
                    }
                    ServedModelsVerdict::Accepted | ServedModelsVerdict::Empty => {
                        self.models.associate(pool.id, &pool.name, declared);
                    }
                    ServedModelsVerdict::Mismatch { expected, observed } => {
                        // As above, but not routable: skip the worker rather
                        // than half-register it.
                        tracing::error!(
                            workerset = pool.name,
                            worker_id = w.worker_id_str,
                            ?expected,
                            ?observed,
                            "declared models conflict with the worker set's; not routing to the worker"
                        );
                    }
                }
            }
            // Readiness normally waits on a `/v1/models` scrape. With a
            // declared list there is nothing to wait for.
            registry.set_initial_sync_complete(w.worker_id_num);
        }

        let ctx = WorkerContext {
            cfg: w,
            endpoint_key,
            pool_id: pool.id,
            pool_name: pool.name.clone(),
            block_size,
            registry: registry.clone(),
            indexer: pool.indexer.clone(),
            inflight_prefixes: pool.inflight_prefixes.clone(),
            prom: self.prom.clone(),
            models: self.models.clone(),
            eviction_tx: self.eviction_tx.clone(),
            cancel: worker_token,
        };

        // The metrics poller and warmup update model routing after a
        // /v1/models scrape.
        ingest::spawn_worker_listener(ctx.clone(), self.settings.ingest);
        // With a declared model list, push the models refresh out of the
        // way; the poller still scrapes load metrics.
        let refresh_secs = if pool.static_models.is_some() {
            u64::MAX / 2000
        } else {
            pool.models_refresh_secs
                .unwrap_or(self.settings.models_refresh_secs)
        };
        spawn_metrics_poller(
            ctx.clone(),
            self.metrics.clone(),
            self.settings.poll_ms,
            refresh_secs,
        );

        // An immediate /v1/models scrape, so the worker is routable without
        // waiting for the periodic tick.
        spawn_one_shot_warmup(ctx, self.metrics.clone());
    }

    /// Teardown for both discovery removals and evictions: cancel the
    /// worker's tasks, drop its state, and drop its radix-tree subtree.
    ///
    /// `reason` is `None` for discovery removals and `Some` for evictions,
    /// which are logged with the reason.
    async fn evict_worker(
        &self,
        pool: &Arc<Pool>,
        worker_id_num: u64,
        reason: Option<EvictionReason>,
    ) {
        self.drop_worker_state(pool, worker_id_num, reason.as_ref());
        pool.indexer.remove_worker(worker_id_num).await;
    }

    /// Everything `evict_worker` does except the indexer call, for retiring
    /// pools: `KvIndexer::remove_worker` panics if it races cancellation,
    /// and the whole tree is dropped anyway.
    fn drop_worker_state(
        &self,
        pool: &Arc<Pool>,
        worker_id_num: u64,
        reason: Option<&EvictionReason>,
    ) {
        // For the log line, before the registry entry is dropped.
        let worker_id = pool
            .registry
            .lookup(worker_id_num)
            .map(|e| e.worker_id_str)
            .unwrap_or_else(|| format!("<unregistered:{worker_id_num}>"));
        match reason {
            Some(r) => {
                tracing::warn!(workerset = pool.name, worker_id, reason = %r, "Evicting worker")
            }
            None => tracing::info!(workerset = pool.name, worker_id, "Removed worker"),
        }

        // Stop the per-worker tasks, for this pool only.
        if let Some((_, token)) = self.task_tokens.remove(&(pool.id, worker_id_num)) {
            token.cancel();
        }

        // Per-pool state: unconditionally ours to drop.
        pool.registry.unregister(worker_id_num);

        // Fleet-wide state is keyed by `worker_id_num` alone, so only drop it
        // while this pool is the owner; blanked metrics would make the worker
        // look idle.
        //
        // The model index is left alone: other workers may still serve the
        // model.
        let still_ours = self
            .worker_pool
            .get(&worker_id_num)
            .is_none_or(|e| e.value().id == pool.id);
        if still_ours {
            self.metrics.forget(worker_id_num);
            self.worker_pool.remove(&worker_id_num);
        } else {
            tracing::debug!(
                workerset = pool.name,
                worker_id,
                "worker now belongs to another worker set; leaving fleet-wide state alone"
            );
        }
    }
}

impl PoolHooks for Coordinator {
    fn on_create(&self, pool: &Arc<Pool>, spec: &PoolSpec) -> Result<(), Error> {
        let build = self
            .kubernetes
            .as_ref()
            .ok_or(Error::KubeDiscoveryUnavailable)?;

        let token = self.cancel.child_token();
        self.discovery_tokens.insert(pool.id, token.clone());

        let (pool_tx, mut pool_rx) = mpsc::channel::<WorkerChange>(64);
        tokio::spawn(build(spec.discovery.clone(), pool_tx, token.clone()));
        // Forwarder: tag this pool's WorkerChange events with the pool. It
        // stops at retirement, and `run` drops the events already queued.
        {
            let coord_tx = self.coord_tx.clone();
            let pool = pool.clone();
            let token = token.clone();
            tokio::spawn(async move {
                loop {
                    let wc = tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        wc = pool_rx.recv() => match wc {
                            Some(wc) => wc,
                            None => break,
                        },
                    };
                    if coord_tx
                        .send((pool.clone(), token.clone(), wc))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        // Per-pool inflight-prefix TTL sweep, a backstop against lifecycle
        // leaks.
        {
            let inflight = pool.inflight_prefixes.clone();
            let ttl = self.settings.observation_ttl;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = interval.tick() => inflight.sweep(ttl),
                    }
                }
            });
        }
        Ok(())
    }

    fn on_retire(&self, pool: &Arc<Pool>) {
        // Stop discovery first, or it re-announces the workers we are about
        // to drop and the coordinator resurrects them into a retired pool.
        if let Some((_, token)) = self.discovery_tokens.remove(&pool.id) {
            token.cancel();
        }
        // Drop every worker's fleet-wide state. Synchronous and indexer-free:
        // the whole radix tree goes at reap, and touching it here would race
        // the cancellation.
        let workers: Vec<u64> = pool
            .registry
            .iter_snapshot()
            .into_iter()
            .map(|(num, _)| num)
            .collect();
        let count = workers.len();
        for num in workers {
            self.drop_worker_state(pool, num, None);
        }
        tracing::debug!(
            workerset = %pool.key,
            workers = count,
            "released this worker set's workers"
        );
    }
}

#[cfg(test)]
mod tests;
