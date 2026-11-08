use super::*;

use crate::discovery::DiscoveryTask;
use crate::pool::{test_pool, test_pool_with_block_size, test_spec};
use crate::scrape::MetricsSample;

struct Harness {
    coordinator: Arc<Coordinator>,
    metrics: Arc<MetricsRegistry>,
    models: Arc<ModelIndex>,
    process: CancellationToken,
    inbox: Inbox,
}

fn harness() -> Harness {
    let metrics = Arc::new(MetricsRegistry::new());
    let models = Arc::new(ModelIndex::new());
    let process = CancellationToken::new();
    let (coordinator, inbox) = Coordinator::new(
        Handles {
            cancel: process.clone(),
            // Announces nothing and waits for cancellation.
            kubernetes: Some(Arc::new(|_, _, cancel: CancellationToken| {
                Box::pin(async move { cancel.cancelled().await }) as DiscoveryTask
            })),
            metrics: metrics.clone(),
            prom: Arc::new(FleetMetrics::new(&prometheus::Registry::new())),
            models: models.clone(),
        },
        WorkerTaskSettings {
            poll_ms: 1000,
            models_refresh_secs: 30,
            ingest: IngestConfig::default(),
            observation_ttl: Duration::from_secs(600),
        },
    );
    Harness {
        coordinator,
        metrics,
        models,
        process,
        inbox,
    }
}

/// No endpoints, so the per-worker tasks return before any I/O.
fn worker(num: u64, hint: &str) -> WorkerConfig {
    WorkerConfig {
        worker_id_str: format!("w-{num}"),
        worker_id_num: num,
        model_name: hint.to_string(),
        ..Default::default()
    }
}

fn pool_with_declared_models(name: &str, models: &[&str]) -> Arc<Pool> {
    let mut spec = test_spec(name, 16);
    spec.static_models = Some(models.iter().map(|m| m.to_string()).collect());
    Arc::new(Pool::from_spec(
        PoolId::next(),
        &spec,
        &CancellationToken::new(),
    ))
}

fn token_for(c: &Coordinator, pool: &Pool, num: u64) -> Option<CancellationToken> {
    c.task_tokens
        .get(&(pool.id, num))
        .map(|t| t.value().clone())
}

fn owner_of(c: &Coordinator, num: u64) -> Option<PoolId> {
    c.worker_pool.get(&num).map(|p| p.value().id)
}

fn discovery_token(c: &Coordinator, pool: &Pool) -> CancellationToken {
    c.discovery_tokens
        .get(&pool.id)
        .map(|t| t.value().clone())
        .expect("discovery token stored")
}

/// Adding a worker makes it routable by its model hint before any scrape.
#[tokio::test]
async fn add_makes_the_worker_routable_by_its_hint() {
    let h = harness();
    let pool = test_pool("a");

    h.coordinator.add_worker(&pool, worker(1, "llama"));

    assert!(pool.registry.lookup(1).is_some());
    assert_eq!(h.models.pools_for("llama"), vec![pool.id]);
    assert_eq!(owner_of(&h.coordinator, 1), Some(pool.id));
    assert!(token_for(&h.coordinator, &pool, 1).is_some());
}

/// A worker id is fleet-wide state the first pool owns; a second pool's Add
/// for the same id is skipped and the owner's state is untouched.
#[tokio::test]
async fn a_second_pool_cannot_claim_an_owned_worker() {
    let h = harness();
    let a = test_pool("a");
    let b = test_pool("b");

    h.coordinator.add_worker(&a, worker(1, "llama"));
    h.coordinator.add_worker(&b, worker(1, "llama"));

    assert!(b.registry.lookup(1).is_none());
    assert!(token_for(&h.coordinator, &b, 1).is_none());
    assert!(a.registry.lookup(1).is_some());
    assert_eq!(owner_of(&h.coordinator, 1), Some(a.id));
    assert_eq!(h.models.pools_for("llama"), vec![a.id]);
}

/// A declared model list seeds the worker and marks it ready without a
/// `/v1/models` scrape. A list that conflicts with the registry's pinned
/// set leaves the worker registered but not routable for those models.
#[tokio::test]
async fn declared_models_seed_the_worker_and_mark_it_ready() {
    let h = harness();
    let pool = pool_with_declared_models("a", &["llama"]);

    h.coordinator.add_worker(&pool, worker(1, "hint"));

    let entry = pool.registry.lookup(1).expect("registered");
    assert!(entry.is_ready(), "a declared list needs no scrape");
    assert_eq!(h.models.pools_for("llama"), vec![pool.id]);

    // Pin a disjoint set, as a stale pin from a `spec.models` edit would.
    let conflicted = pool_with_declared_models("b", &["mistral"]);
    conflicted
        .registry
        .register(99, WorkerEntry::from_config(&worker(99, ""), 16));
    assert_eq!(
        conflicted
            .registry
            .set_served_models(99, &["other".to_string()]),
        ServedModelsVerdict::Accepted
    );

    h.coordinator.add_worker(&conflicted, worker(2, "hint"));

    assert!(conflicted.registry.lookup(2).is_some());
    assert!(h.models.pools_for("mistral").is_empty());
}

/// Eviction stops the worker's tasks and drops every trace of it: the
/// registry entry, the scrape sample and the pool mapping.
#[tokio::test]
async fn evict_cancels_the_tasks_and_drops_the_state() {
    let h = harness();
    let pool = test_pool("a");
    h.coordinator.add_worker(&pool, worker(1, "llama"));
    h.metrics
        .record_sample_for_test(1, MetricsSample::default());
    let token = token_for(&h.coordinator, &pool, 1).expect("token stored");

    h.coordinator
        .evict_worker(
            &pool,
            1,
            Some(EvictionReason::BlockSizeMismatch {
                expected: 16,
                observed: 32,
            }),
        )
        .await;

    assert!(token.is_cancelled());
    assert!(token_for(&h.coordinator, &pool, 1).is_none());
    assert!(pool.registry.lookup(1).is_none());
    assert!(h.metrics.sample(1).is_none());
    assert_eq!(owner_of(&h.coordinator, 1), None);
    assert!(!h.process.is_cancelled());
}

/// A removal from a pool that no longer owns the worker drops only that
/// pool's state; the new owner's sample and mapping survive.
#[tokio::test]
async fn a_stale_removal_leaves_a_moved_worker_alone() {
    let h = harness();
    let a = test_pool("a");
    let b = test_pool("b");
    h.coordinator.add_worker(&a, worker(1, "llama"));
    h.coordinator.evict_worker(&a, 1, None).await;
    h.coordinator.add_worker(&b, worker(1, "llama"));
    h.metrics
        .record_sample_for_test(1, MetricsSample::default());
    let b_token = token_for(&h.coordinator, &b, 1).expect("token stored");

    h.coordinator.drop_worker_state(&a, 1, None);

    assert!(h.metrics.sample(1).is_some());
    assert_eq!(owner_of(&h.coordinator, 1), Some(b.id));
    assert!(!b_token.is_cancelled());
    assert!(b.registry.lookup(1).is_some());
}

/// Retiring a pool stops its discovery and drops every worker's tasks,
/// sample and mapping.
#[tokio::test]
async fn retire_stops_discovery_and_drops_every_worker() {
    let h = harness();
    let pool = test_pool("a");
    h.coordinator
        .on_create(&pool, &test_spec("a", 16))
        .expect("discovery starts");
    let discovery = h
        .coordinator
        .discovery_tokens
        .get(&pool.id)
        .map(|t| t.value().clone())
        .expect("discovery token stored");
    for num in 1..=2 {
        h.coordinator.add_worker(&pool, worker(num, "llama"));
        h.metrics
            .record_sample_for_test(num, MetricsSample::default());
    }
    let tokens: Vec<_> = (1..=2)
        .map(|num| token_for(&h.coordinator, &pool, num).expect("token stored"))
        .collect();

    h.coordinator.on_retire(&pool);

    assert!(discovery.is_cancelled());
    assert!(h.coordinator.discovery_tokens.get(&pool.id).is_none());
    for (i, token) in tokens.iter().enumerate() {
        let num = i as u64 + 1;
        assert!(token.is_cancelled());
        assert!(pool.registry.lookup(num).is_none());
        assert!(h.metrics.sample(num).is_none());
        assert_eq!(owner_of(&h.coordinator, num), None);
    }
    assert!(!pool.cancel.is_cancelled(), "the reaper cancels the pool");
}

/// An Added that a pool queued before it retired doesn't register the
/// worker, so the replacement pool can claim it.
#[tokio::test]
async fn a_retired_pool_queued_add_does_not_claim_the_worker() {
    let h = harness();
    let old = test_pool_with_block_size("a", 16);
    let new = test_pool_with_block_size("a", 32);
    h.coordinator
        .on_create(&old, &test_spec("a", 16))
        .expect("discovery starts");
    let stale = discovery_token(&h.coordinator, &old);
    h.coordinator
        .coord_tx
        .send((old.clone(), stale, WorkerChange::Added(worker(1, "llama"))))
        .await
        .expect("inbox open");

    h.coordinator.on_retire(&old);
    h.coordinator
        .on_create(&new, &test_spec("a", 32))
        .expect("discovery starts");
    let live = discovery_token(&h.coordinator, &new);
    h.coordinator
        .coord_tx
        .send((new.clone(), live, WorkerChange::Added(worker(1, "llama"))))
        .await
        .expect("inbox open");
    let run = tokio::spawn(h.coordinator.clone().run(h.inbox));

    tokio::time::timeout(Duration::from_secs(5), async {
        while owner_of(&h.coordinator, 1).is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a pool claims the worker");
    run.abort();

    assert_eq!(owner_of(&h.coordinator, 1), Some(new.id));
    assert!(new.registry.lookup(1).is_some());
    assert!(old.registry.lookup(1).is_none());
    assert!(token_for(&h.coordinator, &old, 1).is_none());
}
