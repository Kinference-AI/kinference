//! `start`: the one call the server makes to run the control plane.

use std::error::Error as StdError;
use std::sync::Arc;

use kinference_fleet::pool::{PoolManager, PoolRegistry};
use kinference_fleet::profile::ProfileConfig;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_policy::provider_health::ProviderHealthStore;
use kinference_policy::store::PolicyStore;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Error;
use super::api::KubeApi;
use super::leader::{self, LeaderElectionConfig};
use super::metrics::ControllerMetrics;
use super::policy::PolicyConfig;
use super::policy::binding::BindingWriter;
use super::policy::status::{StatusHandles, StatusWriter};
use super::policy::store::PolicyWatcher;
use super::policy::worker_sets::{KubernetesConfig, RejectionStore, WorkerSetWatcher};

/// The control plane's settings, from the server's config.
pub struct ControllerConfig {
    pub kubernetes: KubernetesConfig,
    pub policy: PolicyConfig,
    pub leader_election: LeaderElectionConfig,
    pub profiles: Vec<ProfileConfig>,
}

/// What the control plane drives and reads, built by the server.
pub struct ControllerHandles {
    /// Owned from here on: the worker-set watch reconciles through it, or
    /// the reap sweep runs here when `kubernetes.enabled` is off.
    pub manager: PoolManager,
    pub pools: Arc<PoolRegistry>,
    pub scrape: Arc<MetricsRegistry>,
    /// `None` when policy is off: no CRD watch, no writers, no election.
    pub store: Option<Arc<PolicyStore>>,
    pub provider_health: Arc<ProviderHealthStore>,
    pub worker_sets_ready: watch::Sender<bool>,
}

/// The running control plane. `cancel` stops it; `shutdown` then waits for
/// the elector, the one task with work left after cancellation.
pub struct Controller {
    leader: Option<JoinHandle<()>>,
}

impl Controller {
    /// Wait a bounded moment for the elector to vacate the Lease, so a slow
    /// API server can't hold the pod past its grace period. Otherwise the
    /// handover waits out the lease, as on SIGKILL.
    pub async fn shutdown(self) {
        const VACATE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let Some(task) = self.leader else {
            return;
        };
        match tokio::time::timeout(VACATE_BUDGET, task).await {
            Ok(Ok(())) => tracing::debug!("leader elector stopped; lease released if held"),
            Ok(Err(e)) => tracing::warn!(error = &e as &dyn StdError, "leader elector task failed"),
            Err(_) => tracing::warn!(
                budget = ?VACATE_BUDGET,
                "leader elector did not stop in time; a peer takes over when the lease expires"
            ),
        }
    }
}

/// Connect to the API server when `kubernetes.enabled` or policy is on, then
/// start what runs: the worker-set watch or the reap sweep, and with policy
/// on the CRD watch, the elector, the status and binding writers. Registers
/// the controller's metric families in `registry`.
pub async fn start(
    cfg: ControllerConfig,
    handles: ControllerHandles,
    registry: &prometheus::Registry,
    cancel: CancellationToken,
) -> Result<Controller, Error> {
    let ControllerHandles {
        mut manager,
        pools,
        scrape,
        store,
        provider_health,
        worker_sets_ready,
    } = handles;
    let prom = Arc::new(ControllerMetrics::new(registry));
    let api = if cfg.kubernetes.enabled || store.is_some() {
        Some(KubeApi::infer().await?)
    } else {
        None
    };
    // Written by the worker-set watch, read by the status writer. Empty
    // with `kubernetes.enabled` off.
    let rejections = RejectionStore::new();

    // The manager moves into one task, so it needs no lock. With
    // `kubernetes.enabled`, that task also watches `InferenceWorkerSet`s;
    // otherwise it only reaps.
    match api.clone().filter(|_| cfg.kubernetes.enabled) {
        Some(api) => {
            let watcher = WorkerSetWatcher::new(
                &cfg.kubernetes,
                cfg.profiles.clone(),
                manager,
                rejections.clone(),
                prom.clone(),
                worker_sets_ready,
            );
            let cancel = cancel.clone();
            tokio::spawn(async move { watcher.run(api, cancel).await });
        }
        None => {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tick.tick() => { manager.sweep(); }
                    }
                }
            });
        }
    }

    // Optional CRD-driven routing policy, sharing the snapshot cell the pick
    // path reads. When disabled, a pool-naming Pick falls back to model-based
    // routing.
    let (Some(store), Some(api)) = (store, api) else {
        return Ok(Controller { leader: None });
    };
    if !cfg.kubernetes.enabled {
        tracing::warn!(
            "policy.enabled needs kubernetes.enabled; every destination reports NoMatchingPool"
        );
    }
    let watcher = PolicyWatcher::new(
        api.clone(),
        cfg.policy.namespace.clone(),
        pools.clone(),
        rejections.clone(),
        store.clone(),
    );
    tokio::spawn({
        let cancel = cancel.clone();
        async move { watcher.run(cancel).await }
    });

    // Only one replica writes status; everything else runs on every replica.
    // With election disabled the channel is set to `true` and no elector is
    // spawned.
    let (leader_rx, leader) = if cfg.leader_election.enabled {
        let (tx, rx) = watch::channel(false);
        let ns = cfg
            .leader_election
            .namespace
            .clone()
            .or_else(leader::pod_namespace)
            .unwrap_or_else(|| "default".to_string());
        let identity = cfg.leader_election.identity.clone();
        let params = leader::LeaseParams::from(&cfg.leader_election);
        tracing::debug!(
            lease = %params.name,
            namespace = %ns,
            identity = %identity,
            lease_duration_secs = params.lease_duration.as_secs(),
            "leader election enabled"
        );
        let shutdown = {
            let cancel = cancel.clone();
            async move { cancel.cancelled().await }
        };
        let api = api.clone();
        let task = tokio::spawn(async move {
            leader::run(api, &ns, identity, params, tx, shutdown).await;
        });
        (rx, Some(task))
    } else {
        // Always leader. The sender is leaked so the channel never closes,
        // which the writers would log as an error.
        let (tx, rx) = watch::channel(true);
        std::mem::forget(tx);
        (rx, None)
    };

    // A conforming gateway won't program a route until the pool reports
    // Accepted=True, so nothing routes until this runs.
    let status = StatusWriter::new(
        api.clone(),
        StatusHandles {
            store: store.clone(),
            pools,
            rejections,
            metrics: scrape,
            provider_health,
            leader: leader_rx.clone(),
            prom,
        },
    );
    tokio::spawn({
        let cancel = cancel.clone();
        async move { status.run(cancel).await }
    });

    // `BackendBinding`s let a gateway resolve a route naming an
    // `InferencePool`. Leader-gated like status.
    if cfg.policy.backend_bindings {
        let binding = BindingWriter::new(
            api,
            cfg.policy.namespace.clone(),
            store,
            cfg.policy.binding_extension(),
            leader_rx,
        );
        tokio::spawn(async move { binding.run(cancel).await });
    }

    Ok(Controller { leader })
}
