use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::*;
use crate::discovery::DiscoveryTask;
use crate::pool::test_spec;

fn config(name: &str) -> FleetConfig {
    FleetConfig {
        specs: vec![test_spec(name)],
        workers: WorkerTaskSettings {
            poll_ms: 1000,
            models_refresh_secs: 30,
            ingest: crate::ingest::IngestConfig {
                disabled: true,
                replay: false,
                replay_timeout: std::time::Duration::from_secs(1),
            },
            observation_ttl: std::time::Duration::from_secs(60),
        },
        reap: ReapPolicy::default(),
    }
}

/// Announces nothing and waits for cancellation.
fn parked_discovery() -> DiscoveryFactory {
    Arc::new(|_, _, cancel: CancellationToken| {
        Box::pin(async move { cancel.cancelled().await }) as DiscoveryTask
    })
}

/// `start` publishes each config-declared set and hands back the manager.
#[tokio::test]
async fn start_materializes_the_config_declared_sets() {
    let cfg = config("ws-a");
    let key = cfg.specs[0].key.clone();
    let fleet = Fleet::new(
        cfg,
        Some(parked_discovery()),
        &prometheus::Registry::new(),
        CancellationToken::new(),
    );
    let handles = fleet.handles.clone();
    let manager = fleet.start().expect("every set starts");
    assert_eq!(manager.live_keys(), vec![key]);
    assert_eq!(handles.pools.snapshot().len(), 1);
}

/// A set whose pool cannot start is fatal, and nothing is published.
#[tokio::test]
async fn start_fails_when_a_set_cannot_start() {
    let cfg = config("ws-a");
    let key = cfg.specs[0].key.clone();
    let fleet = Fleet::new(
        cfg,
        None,
        &prometheus::Registry::new(),
        CancellationToken::new(),
    );
    let handles = fleet.handles.clone();
    let err = fleet.start().err().expect("the set cannot start");
    match err {
        Error::WorkerSetsFailed(failed) => assert_eq!(failed, vec![key]),
        other => panic!("expected WorkerSetsFailed, got {other:?}"),
    }
    assert!(handles.pools.snapshot().is_empty());
}
