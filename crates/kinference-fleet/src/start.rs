//! `Fleet`: the one place the server builds the fleet from and starts it.

use std::sync::Arc;

use prometheus::Registry;
use tokio_util::sync::CancellationToken;

use crate::coordinator::{Coordinator, Handles, Inbox, WorkerTaskSettings};
use crate::discovery::DiscoveryFactory;
use crate::error::Error;
use crate::metrics::FleetMetrics;
use crate::models::ModelIndex;
use crate::pool::{PoolManager, PoolRegistry, PoolSpec, ReapPolicy};
use crate::scrape::MetricsRegistry;

/// The fleet's settings, from the server's config.
pub struct FleetConfig {
    /// The config-declared worker sets, materialized by [`Fleet::start`].
    pub specs: Vec<PoolSpec>,
    pub workers: WorkerTaskSettings,
    pub reap: ReapPolicy,
}

/// What the router and the control plane read from the fleet.
#[derive(Clone)]
pub struct FleetHandles {
    pub pools: Arc<PoolRegistry>,
    pub models: Arc<ModelIndex>,
    pub scrape: Arc<MetricsRegistry>,
}

/// The fleet before it runs: the handles exist and the coordinator and the
/// pool manager are built, but no pool exists and no loop runs.
pub struct Fleet {
    pub handles: FleetHandles,
    specs: Vec<PoolSpec>,
    manager: PoolManager,
    coordinator: Arc<Coordinator>,
    inbox: Inbox,
}

impl Fleet {
    /// Build the shared state, the coordinator and the pool manager.
    /// Registers the fleet's metric families in `registry`. Nothing runs
    /// until [`Fleet::start`].
    pub fn new(
        cfg: FleetConfig,
        kubernetes: Option<DiscoveryFactory>,
        registry: &Registry,
        cancel: CancellationToken,
    ) -> Self {
        let handles = FleetHandles {
            pools: PoolRegistry::new(),
            models: Arc::new(ModelIndex::new()),
            scrape: Arc::new(MetricsRegistry::new()),
        };
        let (coordinator, inbox) = Coordinator::new(
            Handles {
                cancel: cancel.clone(),
                kubernetes,
                metrics: handles.scrape.clone(),
                prom: Arc::new(FleetMetrics::new(registry)),
                models: handles.models.clone(),
            },
            cfg.workers,
        );
        let manager = PoolManager::new(
            handles.pools.clone(),
            coordinator.clone(),
            cancel,
            handles.models.clone(),
            cfg.reap,
        );
        Self {
            handles,
            specs: cfg.specs,
            manager,
            coordinator,
            inbox,
        }
    }

    /// Materialize the config-declared worker sets and run the coordinator.
    /// A set that fails to start is fatal here, unlike at runtime, so a
    /// misconfigured set can't silently shrink the fleet. Returns the pool
    /// manager, which the control plane owns from here on.
    ///
    /// Call it after the router has started, so its tokenizer warm-up is
    /// subscribed before the first model claim.
    pub fn start(self) -> Result<PoolManager, Error> {
        let Self {
            specs,
            mut manager,
            coordinator,
            inbox,
            ..
        } = self;
        // Pinned, so that the worker-set watch's reconciles keep them.
        manager.pin(specs);
        let report = manager.reconcile(Vec::new());
        if !report.failed.is_empty() {
            return Err(Error::WorkerSetsFailed(report.failed));
        }
        tokio::spawn(coordinator.run(inbox));
        Ok(manager)
    }
}

#[cfg(test)]
mod tests;
