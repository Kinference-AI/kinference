//! Event classification and the reflector stores that feed `resolve`.
use super::*;
use crate::api::recorded::Recorded;
use kinference_policy::crd::{Destination, InferencePoolSpec, InferenceWorkerSetSpec, ServiceRef};
use kinference_policy::resolve::RejectionIndex;
use kube::api::ObjectMeta;

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some("infra".into()),
        ..Default::default()
    }
}

fn worker_set(name: &str) -> InferenceWorkerSet {
    InferenceWorkerSet {
        metadata: meta(name),
        spec: InferenceWorkerSetSpec {
            service_ref: ServiceRef {
                name: name.into(),
                port: None,
            },
            profile: Some("h100".into()),
            ..Default::default()
        },
        status: None,
    }
}

fn pool(name: &str, dests: Vec<Destination>) -> InferencePool {
    InferencePool {
        metadata: meta(name),
        spec: InferencePoolSpec {
            destinations: dests,
            policy_ref: None,
        },
        status: None,
    }
}

/// The reflector-store path works end to end, not just `resolve`.
#[test]
fn seeding_a_reflector_store_feeds_resolve() {
    let (store, mut writer) = reflector::store::<InferenceWorkerSet>();
    writer.apply_watcher_event(&watcher::Event::Apply(worker_set("workers-h100")));
    let sets = store.state();
    assert_eq!(sets.len(), 1);
    let key = kinference_policy::PoolKey::namespaced("infra", "workers-h100");
    let id = kinference_policy::PoolId::from_raw(1);
    let idx = resolve(
        &[Arc::new(pool(
            "chat",
            vec![Destination {
                kind: Some("InferenceWorkerSet".into()),
                name: Some("workers-h100".into()),
                ..Default::default()
            }],
        ))],
        &[],
        &sets,
        &[],
        &[],
        &[],
        &PoolIndex::from([(key, id)]),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool resolved");
    assert_eq!(p.destinations.len(), 1);
    assert_eq!(
        p.destinations[0]
            .as_local()
            .expect("a local destination")
            .pool_id,
        id,
        "the destination resolves to the pool the set became"
    );
}

/// The run loop gates publishing on `InitDone`; misclassifying it would
/// publish a snapshot missing every pool, or never publish.
#[test]
fn only_init_done_reports_listed() {
    let ev = |e: watcher::Event<InferencePool>| {
        matches!(
            step::<InferencePool>(Ok(Some(e)), "InferencePool", &mut false),
            Step::Listed
        )
    };
    assert!(ev(watcher::Event::InitDone));
    assert!(!ev(watcher::Event::Init));
    assert!(!ev(watcher::Event::Apply(pool("chat", vec![]))));

    // An ended stream must be rebuildable, never terminal.
    assert!(matches!(
        step::<InferencePool>(Ok(None), "InferencePool", &mut false),
        Step::Ended
    ));
}

// --- the publish gate vs. an uninstalled CRD ---------------------------

fn api_err(code: u16) -> kube::Error {
    kube::Error::Api(Box::new(kube::core::Status {
        code,
        message: "the server could not find the requested resource".into(),
        ..Default::default()
    }))
}

/// A missing CRD must not hold the publish gate shut.
#[test]
fn a_404_on_the_initial_list_reads_as_an_empty_store() {
    assert!(crd_not_installed(&watcher::Error::InitialListFailed(
        api_err(404)
    )));
    assert!(crd_not_installed(&watcher::Error::WatchStartFailed(
        api_err(404)
    )));
}

/// Everything else is retried rather than read as "no objects".
#[test]
fn other_failures_are_still_transient() {
    for code in [401, 403, 409, 429, 500, 503] {
        assert!(
            !crd_not_installed(&watcher::Error::InitialListFailed(api_err(code))),
            "{code} must be retried, not treated as an absent CRD"
        );
    }
    assert!(!crd_not_installed(&watcher::Error::WatchFailed(api_err(
        404
    ))));
    assert!(!crd_not_installed(&watcher::Error::NoResourceVersion));
}

// --- the run loop, over a recorded API server --------------------------

/// Wait for the watcher to publish a listed snapshot.
async fn published(store: &PolicyStore) -> Arc<kinference_policy::resolve::PolicyIndex> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let snapshot = store.snapshot();
            if snapshot.listed() {
                break snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the watcher publishes once every kind has listed")
}

/// One generation of `run_once` over `api`, until it has published.
async fn run_once_over(api: Recorded) -> Arc<kinference_policy::resolve::PolicyIndex> {
    let store = PolicyStore::new();
    let watcher = PolicyWatcher::new(
        api,
        Some("infra".into()),
        kinference_fleet::pool::PoolRegistry::new(),
        crate::policy::worker_sets::RejectionStore::new(),
        store.clone(),
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let cancel = cancel.clone();
        async move { watcher.run_once(&cancel).await }
    });
    let snapshot = published(&store).await;
    cancel.cancel();
    task.await.expect("the watcher exits on cancel");
    snapshot
}

#[tokio::test]
async fn run_once_publishes_when_every_gated_kind_has_listed() {
    let api = Recorded::default()
        .with([pool(
            "chat",
            vec![Destination {
                kind: Some("InferenceWorkerSet".into()),
                name: Some("workers-h100".into()),
                ..Default::default()
            }],
        )])
        .with([worker_set("workers-h100")]);
    let snapshot = run_once_over(api).await;
    assert_eq!(snapshot.len(), 1);
    let chat = snapshot.lookup("infra", "chat").expect("the recorded pool");
    assert_eq!(
        chat.unresolved.len(),
        1,
        "no live pool for the set yet, so its destination is unresolved"
    );
    assert!(
        snapshot.services().listed(),
        "the Service list completed too"
    );
}

#[tokio::test]
async fn an_absent_crd_does_not_hold_the_gate() {
    let api = Recorded::default()
        .with([pool("chat", vec![])])
        .without::<HTTPRoute>()
        .without::<ReferenceGrant>();
    let snapshot = run_once_over(api).await;
    assert_eq!(
        snapshot.len(),
        1,
        "published with the two kinds read as empty"
    );
}
