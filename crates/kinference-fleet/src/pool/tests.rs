use super::*;
use dynamo_kv_router::indexer::KvIndexerInterface;

fn cfg(name: &str, block_size: u32) -> PoolSpec {
    crate::pool::test_spec(name, block_size)
}

/// Dropping one pool doesn't disturb the process or its siblings, although
/// `KvIndexer` cancels its token in `Drop`.
#[tokio::test]
async fn dropping_a_pool_cancels_only_that_pool() {
    let process = CancellationToken::new();
    let a = Pool::from_spec(PoolId::next(), &cfg("a", 16), &process);
    let b = Pool::from_spec(PoolId::next(), &cfg("b", 16), &process);
    let b_cancel = b.cancel.clone();

    let a_token = a.cancel.clone();
    drop(a);

    assert!(
        !process.is_cancelled(),
        "dropping one pool must not cancel the process token"
    );
    assert!(!b_cancel.is_cancelled(), "sibling pool must be untouched");
    assert!(
        a_token.is_cancelled(),
        "the dropped pool's own token should be cancelled by KvIndexer::drop"
    );

    // The sibling's indexer still answers, not merely uncancelled.
    b.indexer
        .find_matches(Vec::new())
        .await
        .expect("sibling indexer still serves after a sibling pool was dropped");
}

/// Ids are never recycled, so a handle that outlives its pool resolves to
/// nothing rather than to whoever took its place.
///
#[tokio::test]
async fn a_stale_id_resolves_to_nothing_rather_than_to_another_pool() {
    let process = CancellationToken::new();
    let doomed = Arc::new(Pool::from_spec(PoolId::next(), &cfg("a", 16), &process));
    let doomed_id = doomed.id;

    let registry = PoolRegistry::from_pools(vec![doomed.clone()]);
    assert!(registry.snapshot().get(doomed_id).is_some());

    // The pool leaves the fleet, and a replacement arrives.
    let replacement = Arc::new(Pool::from_spec(PoolId::next(), &cfg("b", 16), &process));
    assert_ne!(
        replacement.id, doomed_id,
        "a fresh pool must never inherit a retired pool's id"
    );
    registry.publish(Arc::new(PoolMap::from_pools(vec![replacement.clone()])));

    assert!(
        registry.snapshot().get(doomed_id).is_none(),
        "a stale id must resolve to None, not to the replacement"
    );
    assert!(registry.snapshot().get(replacement.id).is_some());
}

/// Publishing wakes consumers, such as policy resolution, whose answer
/// depends on the pool set even when their own inputs didn't change.
#[tokio::test]
async fn publishing_wakes_subscribers() {
    let registry = PoolRegistry::new();
    let mut rx = registry.subscribe();
    assert!(!rx.has_changed().unwrap(), "no publish yet");

    registry.publish(Arc::new(PoolMap::from_pools(vec![test_pool("a")])));
    assert!(rx.has_changed().unwrap(), "a publish must be observable");
    rx.borrow_and_update();

    // Including a publish that empties the set.
    registry.publish(Arc::new(PoolMap::from_pools(Vec::new())));
    assert!(rx.has_changed().unwrap());
}

/// A snapshot taken before a change keeps serving the old view, so a reader
/// mid-request never sees the set shift.
#[tokio::test]
async fn an_existing_snapshot_is_unaffected_by_a_later_publish() {
    let process = CancellationToken::new();
    let a = Arc::new(Pool::from_spec(PoolId::next(), &cfg("a", 16), &process));
    let registry = PoolRegistry::from_pools(vec![a.clone()]);

    let held = registry.snapshot();
    registry.publish(Arc::new(PoolMap::default()));

    assert_eq!(held.len(), 1, "the held snapshot is immutable");
    assert!(held.get(a.id).is_some());
    assert!(registry.snapshot().is_empty(), "the new view is empty");
}

/// A pick's inflight bookkeeping survives its pool being destroyed.
///
/// `release_inflight_state` is private, so this checks what it relies on: the
/// handles on the observation record keep working after the fleet drops the
/// pool.
#[tokio::test]
async fn inflight_handles_outlive_their_pool() {
    let process = CancellationToken::new();
    let pool = Arc::new(Pool::from_spec(PoolId::next(), &cfg("a", 16), &process));
    let pool_token = pool.cancel.clone();

    // What a pick stashes on its observation record.
    let inflight = pool.inflight.clone();
    let prefixes = pool.inflight_prefixes.clone();
    pool.inflight.inc("10.0.0.1:8000");
    assert_eq!(pool.inflight.get("10.0.0.1:8000"), 1);

    // The fleet drops the pool while the request is still in flight.
    let registry = PoolRegistry::from_pools(vec![pool]);
    registry.publish(Arc::new(PoolMap::default()));
    assert!(registry.snapshot().is_empty());

    // The release still lands, on the right counter.
    inflight.dec("10.0.0.1:8000");
    assert_eq!(
        inflight.get("10.0.0.1:8000"),
        0,
        "the decrement must reach the pool the pick was made against"
    );
    // The handles hold the two registries, not the pool, so the pool itself
    // (indexer thread and radix tree) was reclaimed when the fleet dropped it.
    assert!(
        pool_token.is_cancelled(),
        "the pool is reclaimed promptly; only the inflight maps linger"
    );

    drop((inflight, prefixes));
}

/// Process shutdown still reaches every pool: `child_token` propagates from
/// parent to child.
#[tokio::test]
async fn cancelling_the_process_cancels_every_pool() {
    let process = CancellationToken::new();
    let p = Pool::from_spec(PoolId::next(), &cfg("a", 16), &process);
    assert!(!p.cancel.is_cancelled());
    process.cancel();
    assert!(p.cancel.is_cancelled(), "process shutdown must reach pools");
}

#[derive(Default)]
struct RecordingHooks {
    created: Mutex<Vec<String>>,
    retired: Mutex<Vec<String>>,
    fail_on: Mutex<Option<String>>,
}

impl PoolHooks for RecordingHooks {
    fn on_create(&self, pool: &Arc<Pool>, _spec: &PoolSpec) -> Result<(), Error> {
        if self.fail_on.lock().unwrap().as_deref() == Some(pool.name.as_str()) {
            return Err(Error::KubeDiscoveryUnavailable);
        }
        self.created.lock().unwrap().push(pool.name.clone());
        Ok(())
    }
    fn on_retire(&self, pool: &Arc<Pool>) {
        self.retired.lock().unwrap().push(pool.name.clone());
    }
}

struct Harness {
    mgr: PoolManager,
    registry: Arc<PoolRegistry>,
    hooks: Arc<RecordingHooks>,
    models: Arc<ModelIndex>,
    process: CancellationToken,
}

fn harness(reap: ReapPolicy) -> Harness {
    let registry = PoolRegistry::new();
    let hooks = Arc::new(RecordingHooks::default());
    let models = Arc::new(ModelIndex::new());
    let process = CancellationToken::new();
    let mgr = PoolManager::new(
        registry.clone(),
        hooks.clone(),
        process.clone(),
        models.clone(),
        reap,
    );
    Harness {
        mgr,
        registry,
        hooks,
        models,
        process,
    }
}

/// Reap as soon as the pool is idle; tests that need a linger set one.
fn eager() -> ReapPolicy {
    ReapPolicy {
        linger: Duration::ZERO,
        backstop: Duration::from_secs(600),
    }
}

fn spec(name: &str, block_size: u32) -> PoolSpec {
    crate::pool::test_spec(name, block_size)
}

fn worker_cfg(num: u64) -> crate::discovery::WorkerConfig {
    crate::discovery::WorkerConfig {
        worker_id_str: format!("w-{num}"),
        worker_id_num: num,
        model_name: "old".into(),
        http_url: Some(format!("http://10.0.0.{num}:8000")),
        ..Default::default()
    }
}

#[tokio::test]
async fn create_retire_reap() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    assert_eq!(h.registry.snapshot().len(), 1);
    let token = h
        .mgr
        .live_pool(&PoolKey::local("a"))
        .unwrap()
        .cancel
        .clone();

    let report = h.mgr.reconcile(Vec::new());
    assert_eq!(report.retired, vec![PoolKey::local("a")]);
    assert_eq!(
        h.registry.snapshot().len(),
        0,
        "retire must stop routing immediately"
    );
    assert!(
        !token.is_cancelled(),
        "retire must NOT cancel the token — the KvIndexer's channel send \
         unwraps, so anything still feeding it would panic"
    );
    assert_eq!(h.hooks.retired.lock().unwrap().as_slice(), ["a"]);

    assert_eq!(h.mgr.sweep(), 1);
    assert!(
        token.is_cancelled(),
        "reap must cancel so the indexer thread exits"
    );
    assert!(h.mgr.retired_keys().is_empty());
    assert!(
        !h.process.is_cancelled(),
        "a pool reap must not touch the process"
    );
}

#[tokio::test]
async fn a_pinned_spec_survives_reconciles_that_omit_it() {
    let mut h = harness(eager());
    h.mgr.pin(vec![spec("cfg", 16)]);
    h.mgr.reconcile(vec![spec("cr", 16)]);
    let report = h.mgr.reconcile(Vec::new());
    assert_eq!(report.retired, vec![PoolKey::local("cr")]);
    assert_eq!(h.mgr.live_keys(), vec![PoolKey::local("cfg")]);
}

/// A retired pool's indexer keeps answering until it is reaped, since
/// `KvIndexer` panics if reached after cancellation.
#[tokio::test]
async fn a_retired_pool_indexer_still_answers_until_reaped() {
    let mut h = harness(ReapPolicy {
        linger: Duration::from_secs(30),
        backstop: Duration::from_secs(600),
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let pool = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();

    h.mgr.reconcile(Vec::new());
    // Retired, not reaped: in-flight work still finds a working index.
    pool.indexer
        .find_matches(Vec::new())
        .await
        .expect("a retired pool's indexer must still answer");
    assert_eq!(h.mgr.sweep(), 0, "the handle above blocks the reap");
    assert!(!pool.cancel.is_cancelled());
}

/// A request inside the pool holds an `Arc<Pool>`, which blocks reaping.
#[tokio::test]
async fn a_pool_with_an_outstanding_handle_is_not_reaped() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let held = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    let token = held.cancel.clone();

    h.mgr.reconcile(Vec::new());
    assert_eq!(
        h.mgr.sweep(),
        0,
        "an outstanding handle must block the reap"
    );
    assert!(!token.is_cancelled());

    drop(held);
    assert_eq!(h.mgr.sweep(), 1);
    assert!(token.is_cancelled());
}

/// The backstop reaps a pool despite a leaked `Arc`.
#[tokio::test]
async fn the_backstop_reaps_past_the_deadline_even_when_held() {
    let mut h = harness(ReapPolicy {
        linger: Duration::ZERO,
        backstop: Duration::ZERO,
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let _leaked = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    h.mgr.reconcile(Vec::new());
    assert_eq!(
        h.mgr.sweep(),
        1,
        "the backstop must fire regardless of the count"
    );
}

/// Retiring and immediately recreating the same set resurrects it: same
/// `PoolId`, indexer and registry.
#[tokio::test]
async fn resurrection_preserves_the_pool_and_its_cache() {
    // Non-zero linger, so the sweeper can't reap before the recreate.
    let mut h = harness(ReapPolicy {
        linger: Duration::from_secs(30),
        backstop: Duration::from_secs(600),
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    let id_before = before.id;
    let indexer_before = Arc::as_ptr(&before.indexer);
    let registry_before = Arc::as_ptr(&before.registry);
    drop(before);

    h.mgr.reconcile(Vec::new());
    assert_eq!(h.mgr.sweep(), 0, "the linger keeps it resurrectable");

    let report = h.mgr.reconcile(vec![spec("a", 16)]);
    assert_eq!(report.resurrected, vec![PoolKey::local("a")]);
    assert!(report.created.is_empty());
    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_eq!(after.id, id_before, "same PoolId");
    assert_eq!(
        Arc::as_ptr(&after.indexer),
        indexer_before,
        "same KvIndexer — the whole point, or the fleet re-warms from cold"
    );
    assert_eq!(
        Arc::as_ptr(&after.registry),
        registry_before,
        "same worker registry"
    );
    assert!(!after.cancel.is_cancelled());
    assert!(
        h.mgr.retired_keys().is_empty(),
        "resurrection consumes the retired entry"
    );
}

/// A different spec builds a fresh pool rather than resurrecting.
#[tokio::test]
async fn a_changed_spec_does_not_resurrect() {
    let mut h = harness(ReapPolicy {
        linger: Duration::from_secs(30),
        backstop: Duration::from_secs(600),
    });
    h.mgr.reconcile(vec![spec("a", 16)]);
    let id_before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().id;
    h.mgr.reconcile(Vec::new());

    let report = h.mgr.reconcile(vec![spec("a", 64)]);
    assert_eq!(report.created, vec![PoolKey::local("a")]);
    assert!(report.resurrected.is_empty());
    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_ne!(after.id, id_before);
    assert_eq!(after.block_size, 64);
}

/// An unchanged spec is a no-op.
#[tokio::test]
async fn reconciling_an_unchanged_set_does_nothing() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16), spec("b", 16)]);
    let ids: Vec<PoolId> = [PoolKey::local("a"), PoolKey::local("b")]
        .iter()
        .map(|k| h.mgr.live_pool(k).unwrap().id)
        .collect();

    let report = h.mgr.reconcile(vec![spec("a", 16), spec("b", 16)]);
    assert!(
        report.is_noop(),
        "steady state must be a no-op, got {report:?}"
    );
    let after: Vec<PoolId> = [PoolKey::local("a"), PoolKey::local("b")]
        .iter()
        .map(|k| h.mgr.live_pool(k).unwrap().id)
        .collect();
    assert_eq!(ids, after);
    assert_eq!(h.hooks.created.lock().unwrap().len(), 2, "no re-create");
}

/// Retuning keeps the pool and its cache; only the values change.
#[tokio::test]
async fn a_tuning_change_is_copy_on_write() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    let snapshot_before = h.registry.snapshot();

    let mut next = spec("a", 16);
    next.predictor.base_ms = 999.0;
    let report = h.mgr.reconcile(vec![next]);
    assert_eq!(report.updated, vec![PoolKey::local("a")]);

    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_eq!(after.id, before.id, "same PoolId");
    assert_eq!(after.predictor.base_ms, 999.0);
    assert_eq!(Arc::as_ptr(&after.indexer), Arc::as_ptr(&before.indexer));
    // A reader mid-pick keeps the values it started with.
    assert_eq!(
        snapshot_before.get(before.id).unwrap().predictor.base_ms,
        before.predictor.base_ms
    );
    assert_eq!(
        h.hooks.retired.lock().unwrap().len(),
        0,
        "an update is not a retire"
    );
}

/// Editing a Static set's declared model list reaches the already-registered
/// workers, and the model index.
#[tokio::test]
async fn changing_the_declared_model_list_reseeds_the_live_workers() {
    let mut h = harness(eager());
    let mut first = spec("a", 16);
    first.static_models = Some(vec!["old".to_string()]);
    h.mgr.reconcile(vec![first]);

    let pool = h.mgr.live_pool(&PoolKey::local("a")).unwrap().clone();
    pool.registry.register(
        1,
        crate::workers::WorkerEntry::from_config(&worker_cfg(1), 16),
    );
    pool.registry.set_served_models(1, &["old".to_string()]);
    h.models.associate(pool.id, "a", &["old".to_string()]);

    let mut next = spec("a", 16);
    next.static_models = Some(vec!["new".to_string()]);
    let report = h.mgr.reconcile(vec![next]);
    assert_eq!(report.updated, vec![PoolKey::local("a")]);

    // The worker now serves the new name...
    assert_eq!(
        pool.registry.by_model_len("new"),
        1,
        "the live worker must be re-seeded, not left on the old name"
    );
    assert_eq!(pool.registry.by_model_len("old"), 0);
    // ...and the index agrees, so a request for it lands here.
    assert_eq!(h.models.pools_for("new"), vec![pool.id]);
    assert!(
        h.models.pools_for("old").is_empty(),
        "the stale claim must go, or requests route here for a model \
         nothing in this pool serves"
    );
}

/// A block-size change replaces the pool.
#[tokio::test]
async fn a_block_size_change_replaces_the_pool() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let id_before = h.mgr.live_pool(&PoolKey::local("a")).unwrap().id;

    let report = h.mgr.reconcile(vec![spec("a", 64)]);
    assert_eq!(report.replaced, vec![PoolKey::local("a")]);
    let after = h.mgr.live_pool(&PoolKey::local("a")).unwrap();
    assert_ne!(after.id, id_before);
    assert_eq!(after.block_size, 64);
    assert_eq!(h.hooks.retired.lock().unwrap().as_slice(), ["a"]);
}

/// A retired pool's model claims are released.
#[tokio::test]
async fn retiring_releases_the_model_claims() {
    let mut h = harness(eager());
    h.mgr.reconcile(vec![spec("a", 16)]);
    let id = h.mgr.live_pool(&PoolKey::local("a")).unwrap().id;
    h.models.associate(id, "a", &["gemma".to_string()]);
    assert_eq!(h.models.pools_for("gemma"), vec![id]);

    h.mgr.reconcile(Vec::new());
    assert!(
        h.models.pools_for("gemma").is_empty(),
        "a retired pool's claim must not outlive it"
    );
}

/// A pool whose discovery can't start isn't published.
#[tokio::test]
async fn a_failed_create_is_not_published() {
    let mut h = harness(eager());
    *h.hooks.fail_on.lock().unwrap() = Some("bad".to_string());
    let report = h.mgr.reconcile(vec![spec("good", 16), spec("bad", 16)]);
    assert_eq!(report.created, vec![PoolKey::local("good")]);
    assert_eq!(report.failed, vec![PoolKey::local("bad")]);
    assert_eq!(h.registry.snapshot().len(), 1);
    assert!(h.mgr.live_pool(&PoolKey::local("bad")).is_none());
}

/// Churn soak: every pool created ends up reaped with its token cancelled.
#[tokio::test]
async fn churn_leaves_nothing_behind() {
    let mut h = harness(eager());
    let mut tokens = Vec::new();
    for i in 0..200 {
        let name = format!("set-{i}");
        h.mgr.reconcile(vec![spec(&name, 16)]);
        tokens.push(
            h.mgr
                .live_pool(&PoolKey::local(&name))
                .unwrap()
                .cancel
                .clone(),
        );
        h.mgr.sweep();
    }
    h.mgr.reconcile(Vec::new());
    h.mgr.sweep();
    assert!(h.mgr.live_keys().is_empty());
    assert!(h.mgr.retired_keys().is_empty());
    assert_eq!(h.registry.snapshot().len(), 0);
    assert!(
        tokens.iter().all(|t| t.is_cancelled()),
        "every pool's token must be cancelled, or its indexer thread leaks"
    );
    assert!(!h.process.is_cancelled());
}
