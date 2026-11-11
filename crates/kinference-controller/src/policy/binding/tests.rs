use super::*;

use crate::api::recorded::{Recorded, Write, key};

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};

use kinference_policy::PoolId;
use kinference_policy::compile::CompiledPolicy;
use kinference_policy::crd::{Condition, IntOrString};
use kinference_policy::engine::ObjectKey;
use kinference_policy::resolve::{ResolvedProvider, ResolvedWorkerSet};

/// The default picker extension, in the binding's own namespace.
fn ext() -> Option<ExtensionReference> {
    Some(ext_ref("chat-picker", None))
}

fn ext_ref(name: &str, namespace: Option<&str>) -> ExtensionReference {
    ExtensionReference {
        name: name.into(),
        namespace: namespace.map(String::from),
    }
}

fn ws(name: &str, service: &str, namespace: &str) -> ResolvedDestination {
    ws_port(name, service, namespace, None)
}

fn ws_port(
    name: &str,
    service: &str,
    namespace: &str,
    port: Option<IntOrString>,
) -> ResolvedDestination {
    ResolvedDestination::Local(ResolvedWorkerSet {
        name: name.into(),
        namespace: namespace.into(),
        labels: BTreeMap::new(),
        pool_id: PoolId::from_raw(0),
        service_name: service.into(),
        service_port: port,
        kv_events_declared: false,
        aliases: Vec::new(),
    })
}

fn service(ns: &str, name: &str, ports: &[(&str, i32)]) -> Arc<Service> {
    Arc::new(Service {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(
                ports
                    .iter()
                    .map(|(n, p)| ServicePort {
                        name: (!n.is_empty()).then(|| n.to_string()),
                        port: *p,
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }),
        status: None,
    })
}

/// A single-port Service for every worker set the fixtures name. Single-port
/// because `ws` omits `serviceRef.port`, which only resolves in that case.
fn index() -> ServiceIndex {
    ServiceIndex::build(&[
        service("infra", "vllm-h100", &[("", 8080)]),
        service("infra", "vllm-a100", &[("", 8080)]),
        service("gpu", "vllm-a100", &[("", 8080)]),
    ])
}

/// `desired_binding` against that index, asserting nothing was omitted.
fn desired(pool: &ResolvedPool, extension: Option<&ExtensionReference>) -> BackendBinding {
    let d = desired_binding(pool, extension, &index());
    assert!(d.omitted.is_empty(), "fixture ports: {:?}", d.omitted);
    d.binding
}

fn provider(name: &str, backend_ns: &str, backend: &str) -> ResolvedDestination {
    ResolvedDestination::Provider(ResolvedProvider {
        name: name.into(),
        namespace: "infra".into(),
        uid: None,
        labels: BTreeMap::new(),
        backend_ref: ObjectKey::new(backend_ns, backend),
        path: None,
        models: Vec::new(),
    })
}

fn pool(dests: Vec<ResolvedDestination>) -> ResolvedPool {
    ResolvedPool {
        uid: Some("pool-uid".into()),
        namespace: "infra".into(),
        name: "chat".into(),
        resource_version: "100".into(),
        generation: Some(3),
        policy: Arc::new(CompiledPolicy::serve_as_asked("<implicit>")),
        dangling_policy: None,
        destinations: dests,
        unresolved: Vec::new(),
        parents: Vec::new(),
    }
}

/// The claim is what a route's `InferencePool` backendRef is matched against.
#[test]
fn claims_the_pool_by_group_kind_name() {
    let b = desired(&pool(vec![]), ext().as_ref());
    assert_eq!(b.spec.claim.group, "kinference.buoyant.io");
    assert_eq!(b.spec.claim.kind, "InferencePool");
    assert_eq!(b.spec.claim.name, "chat");
    // The gateway matches a claim against refs in the binding's namespace.
    assert_eq!(b.metadata.namespace.as_deref(), Some("infra"));
    assert_eq!(b.metadata.name.as_deref(), Some("chat"));
}

/// A worker set becomes its Service, on the port the Service exposes.
#[test]
fn worker_set_becomes_a_service_ref_on_its_service_port() {
    let b = desired(
        &pool(vec![ws("workers-h100", "vllm-h100", "infra")]),
        ext().as_ref(),
    );
    assert_eq!(b.spec.backend_refs.len(), 1);
    let r = &b.spec.backend_refs[0];
    // The entry name is the destination's.
    assert_eq!(r.name, "workers-h100");
    assert_eq!(r.target.name, "vllm-h100");
    // Unset group and kind mean a core Service.
    assert_eq!(r.target.group, None);
    assert_eq!(r.target.kind, None);
    // The Service's only port, since the fixture's `serviceRef` names none.
    assert_eq!(r.target.port, Some(8080));
    // Same namespace as the binding, so it is left to default.
    assert_eq!(r.target.namespace, None);
}

/// A port named by `serviceRef` is resolved to its number.
#[test]
fn a_named_service_ref_port_resolves_to_its_number() {
    let idx = ServiceIndex::build(&[service(
        "infra",
        "vllm-h100",
        &[("http", 80), ("metrics", 9090)],
    )]);
    let p = pool(vec![ws_port(
        "workers-h100",
        "vllm-h100",
        "infra",
        Some(IntOrString::Str("http".into())),
    )]);
    let d = desired_binding(&p, ext().as_ref(), &idx);
    assert!(d.omitted.is_empty());
    // 80, the Service port, not the pod's target port.
    assert_eq!(d.binding.spec.backend_refs[0].target.port, Some(80));
}

/// A Service in another namespace is looked up there, not in the pool's
/// namespace.
#[test]
fn a_cross_namespace_port_is_read_from_the_targets_own_namespace() {
    let idx = ServiceIndex::build(&[
        service("infra", "vllm-a100", &[("http", 80)]),
        service("gpu", "vllm-a100", &[("http", 9000)]),
    ]);
    let p = pool(vec![ws_port(
        "remote",
        "vllm-a100",
        "gpu",
        Some(IntOrString::Str("http".into())),
    )]);
    let d = desired_binding(&p, ext().as_ref(), &idx);
    let r = &d.binding.spec.backend_refs[0];
    assert_eq!(r.target.namespace.as_deref(), Some("gpu"));
    assert_eq!(r.target.port, Some(9000));
}

/// A ref whose port can't be resolved is left out and reported, never written
/// portless.
#[test]
fn a_destination_whose_port_does_not_resolve_is_omitted_with_its_reason() {
    let idx = ServiceIndex::build(&[service(
        "infra",
        "vllm-h100",
        &[("http", 80), ("metrics", 9090)],
    )]);

    // Two ports and no name: ambiguous, so kinference won't guess.
    let d = desired_binding(
        &pool(vec![ws("workers-h100", "vllm-h100", "infra")]),
        ext().as_ref(),
        &idx,
    );
    assert!(d.binding.spec.backend_refs.is_empty());
    assert_eq!(
        d.omitted,
        [("workers-h100".to_string(), UnresolvedPort::AmbiguousPort)]
    );

    // A typo'd port name, and a Service that is not there at all.
    let p = pool(vec![
        ws_port(
            "workers-h100",
            "vllm-h100",
            "infra",
            Some(IntOrString::Str("htpp".into())),
        ),
        ws_port(
            "workers-a100",
            "vllm-a100",
            "infra",
            Some(IntOrString::Str("http".into())),
        ),
    ]);
    let d = desired_binding(&p, ext().as_ref(), &idx);
    assert!(d.binding.spec.backend_refs.is_empty());
    assert_eq!(
        d.omitted,
        [
            ("workers-h100".to_string(), UnresolvedPort::UnknownPortName),
            ("workers-a100".to_string(), UnresolvedPort::ServiceNotFound),
        ]
    );
}

/// One destination's unresolvable port doesn't drop the pool's other refs.
#[test]
fn an_omitted_destination_leaves_the_rest_of_the_binding_intact() {
    let idx = ServiceIndex::build(&[
        service("infra", "vllm-h100", &[("", 8080)]),
        service("infra", "vllm-a100", &[("http", 80), ("metrics", 9090)]),
    ]);
    let p = pool(vec![
        ws("workers-h100", "vllm-h100", "infra"),
        ws("workers-a100", "vllm-a100", "infra"),
        provider("openai", "infra", "openai-backend"),
    ]);
    let d = desired_binding(&p, ext().as_ref(), &idx);
    let names: Vec<&str> = d
        .binding
        .spec
        .backend_refs
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(names, ["workers-h100", "openai"]);
    assert_eq!(d.omitted.len(), 1);
}

/// Against an index that has not listed, every worker set is omitted as
/// `NotListed` rather than written portless.
#[test]
fn an_unlisted_index_omits_every_service_ref() {
    let p = pool(vec![
        ws("workers-h100", "vllm-h100", "infra"),
        provider("openai", "infra", "openai-backend"),
    ]);
    let d = desired_binding(&p, ext().as_ref(), &ServiceIndex::unlisted());
    // Providers don't need a Service.
    assert_eq!(d.binding.spec.backend_refs.len(), 1);
    assert_eq!(d.binding.spec.backend_refs[0].name, "openai");
    assert_eq!(
        d.omitted,
        [("workers-h100".to_string(), UnresolvedPort::NotListed)]
    );
}

/// A provider becomes the `GatewayBackend` it names, without a port.
#[test]
fn provider_becomes_a_gateway_backend_ref() {
    let b = desired(
        &pool(vec![provider("openai", "infra", "openai-backend")]),
        ext().as_ref(),
    );
    let r = &b.spec.backend_refs[0];
    assert_eq!(r.name, "openai");
    assert_eq!(r.target.group.as_deref(), Some("gateway.linkerd.io"));
    assert_eq!(r.target.kind.as_deref(), Some("GatewayBackend"));
    assert_eq!(r.target.name, "openai-backend");
    assert_eq!(r.target.port, None);
}

/// A target's namespace is set only when it differs from the binding's.
#[test]
fn only_cross_namespace_targets_carry_a_namespace() {
    let b = desired(
        &pool(vec![
            ws("local", "vllm-h100", "infra"),
            ws("remote", "vllm-a100", "gpu"),
            provider("openai", "vendors", "openai-backend"),
        ]),
        ext().as_ref(),
    );
    assert_eq!(b.spec.backend_refs[0].target.namespace, None);
    assert_eq!(
        b.spec.backend_refs[1].target.namespace.as_deref(),
        Some("gpu")
    );
    assert_eq!(
        b.spec.backend_refs[2].target.namespace.as_deref(),
        Some("vendors")
    );
}

/// Destination order is the pool's, and both classes land in one list.
#[test]
fn both_classes_share_one_ordered_list() {
    let b = desired(
        &pool(vec![
            ws("workers-h100", "vllm-h100", "infra"),
            provider("openai", "infra", "openai-backend"),
            ws("workers-a100", "vllm-a100", "infra"),
        ]),
        ext().as_ref(),
    );
    let names: Vec<&str> = b
        .spec
        .backend_refs
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(names, ["workers-h100", "openai", "workers-a100"]);
}

/// A pool with no resolved destinations still gets an empty binding.
#[test]
fn a_pool_with_no_resolved_destination_still_gets_a_binding() {
    let b = desired(&pool(vec![]), ext().as_ref());
    assert!(b.spec.backend_refs.is_empty());
}

/// The binding is owned by its pool and labeled as ours.
#[test]
fn labels_and_owner_reference_mark_the_binding_as_ours() {
    let b = desired(&pool(vec![]), ext().as_ref());
    assert!(is_ours(&b));
    assert_eq!(b.labels().get(POOL_LABEL).map(String::as_str), Some("chat"));
    let owner = &b.metadata.owner_references.as_ref().unwrap()[0];
    assert_eq!(owner.kind, "InferencePool");
    assert_eq!(owner.api_version, "kinference.buoyant.io/v1alpha1");
    assert_eq!(owner.uid, "pool-uid");
    assert_eq!(owner.controller, Some(true));
}

/// A pool without a known UID still gets a binding, just an unowned one.
#[test]
fn a_pool_without_a_uid_gets_no_owner_reference() {
    let mut p = pool(vec![]);
    p.uid = None;
    let b = desired(&p, ext().as_ref());
    assert!(b.metadata.owner_references.is_none());
}

/// An object without our label is someone else's, whatever it claims.
#[test]
fn an_unlabelled_binding_is_not_ours() {
    let mut b = desired(&pool(vec![]), ext().as_ref());
    b.metadata.labels = None;
    assert!(!is_ours(&b));
}

/// Equal specs compare in sync, so the writer doesn't patch every tick.
#[test]
fn identical_bindings_are_in_sync() {
    let want = desired(
        &pool(vec![ws("workers-h100", "vllm-h100", "infra")]),
        ext().as_ref(),
    );
    assert!(in_sync(&want.clone(), &want));
}

/// ...and a drifted one doesn't, in either direction.
#[test]
fn drift_is_not_in_sync() {
    let want = desired(
        &pool(vec![ws("workers-h100", "vllm-h100", "infra")]),
        ext().as_ref(),
    );

    let fewer = desired(&pool(vec![]), ext().as_ref());
    assert!(!in_sync(&fewer, &want));

    let mut edited = want.clone();
    edited.spec.backend_refs[0].target.name = "someone-elses-service".into();
    assert!(!in_sync(&edited, &want));

    // An extension added out of band is drift too.
    let mut extended = want.clone();
    extended.spec.extensions = vec![ext_ref("kinference-selector", None)];
    assert!(!in_sync(&extended, &want));
}

/// Owners compare by UID, since a recreated pool keeps its name.
#[test]
fn an_owner_reference_to_a_dead_pool_is_not_in_sync() {
    let want = desired(&pool(vec![]), ext().as_ref());
    let mut stale = want.clone();
    stale.metadata.owner_references.as_mut().unwrap()[0].uid = "older-pool-uid".into();
    assert!(!in_sync(&stale, &want));
}

/// Fields kinference does not set are not compared.
#[test]
fn foreign_metadata_does_not_count_as_drift() {
    let want = desired(&pool(vec![]), ext().as_ref());
    let mut annotated = want.clone();
    annotated.metadata.annotations = Some(BTreeMap::from([(
        "example.com/owner".to_string(),
        "platform".to_string(),
    )]));
    annotated
        .metadata
        .labels
        .as_mut()
        .unwrap()
        .insert("example.com/team".into(), "platform".into());
    assert!(in_sync(&annotated, &want));
}

/// The binding lists the picker extension.
#[test]
fn the_configured_extension_is_listed() {
    let b = desired(&pool(vec![]), ext().as_ref());
    assert_eq!(b.spec.extensions, [ext_ref("chat-picker", None)]);

    let renamed = desired(&pool(vec![]), Some(&ext_ref("infra-picker", None)));
    assert_eq!(renamed.spec.extensions, [ext_ref("infra-picker", None)]);
}

/// An extension in another namespace is named with it; one in the pool's
/// namespace is written without, so equivalent bindings compare equal.
#[test]
fn the_extension_namespace_is_written_only_when_it_differs() {
    let elsewhere = desired(
        &pool(vec![]),
        Some(&ext_ref("chat-picker", Some("kinference"))),
    );
    assert_eq!(
        elsewhere.spec.extensions,
        [ext_ref("chat-picker", Some("kinference"))]
    );

    let beside = desired(&pool(vec![]), Some(&ext_ref("chat-picker", Some("infra"))));
    assert_eq!(beside.spec.extensions, [ext_ref("chat-picker", None)]);
}

/// A cleared extension lists none, for routes carrying their own
/// `extensionRef` filter.
#[test]
fn no_configured_extension_lists_none() {
    let b = desired(&pool(vec![]), None);
    assert!(b.spec.extensions.is_empty());
}

/// A blank `backend_binding_extension` reads as none.
#[test]
fn a_blank_configured_name_is_no_extension() {
    let cfg = crate::policy::PolicyConfig::default();
    assert_eq!(cfg.binding_extension(), ext());

    let cleared = crate::policy::PolicyConfig {
        backend_binding_extension: "  ".into(),
        ..Default::default()
    };
    assert_eq!(cleared.binding_extension(), None);
}

/// A configured namespace is carried; a blank one reads as the binding's own.
#[test]
fn the_configured_extension_namespace_is_carried() {
    let cfg = crate::policy::PolicyConfig {
        backend_binding_extension_namespace: Some("kinference".into()),
        ..Default::default()
    };
    assert_eq!(
        cfg.binding_extension(),
        Some(ext_ref("chat-picker", Some("kinference")))
    );

    let blank = crate::policy::PolicyConfig {
        backend_binding_extension_namespace: Some(" ".into()),
        ..Default::default()
    };
    assert_eq!(blank.binding_extension(), ext());
}

/// Renaming the extension registers as drift on existing bindings.
#[test]
fn a_changed_extension_is_drift() {
    let want = desired(&pool(vec![]), ext().as_ref());
    let stale = desired(&pool(vec![]), Some(&ext_ref("infra-picker", None)));
    assert!(!in_sync(&stale, &want));
    // And so does clearing it.
    let cleared = desired(&pool(vec![]), None);
    assert!(!in_sync(&cleared, &want));
}

fn condition(status: &str) -> Condition {
    Condition {
        r#type: "Accepted".into(),
        status: status.into(),
        observed_generation: None,
        last_transition_time: "2026-01-01T00:00:00Z".into(),
        reason: "NoNativeTarget".into(),
        message: "backendRefs is empty".into(),
    }
}

/// Only an explicit `Accepted=False` counts as a rejection.
#[test]
fn only_a_false_accepted_condition_reads_as_rejected() {
    let mut b = desired(&pool(vec![]), ext().as_ref());
    assert_eq!(
        rejected_reason(&b),
        None,
        "no status yet is not a rejection"
    );

    b.status = Some(kinference_policy::crd::BackendBindingStatus {
        conditions: vec![condition("True")],
    });
    assert_eq!(rejected_reason(&b), None);

    b.status = Some(kinference_policy::crd::BackendBindingStatus {
        conditions: vec![condition("False")],
    });
    let reason = rejected_reason(&b).expect("rejected");
    assert!(reason.contains("NoNativeTarget"), "{reason}");
    assert!(reason.contains("backendRefs is empty"), "{reason}");
}

// --- the reconcile loop, over a recorded API server ---------------------

/// A listed snapshot with one pool, `infra/chat`, whose one destination is
/// the worker set `infra/workers-h100` on Service `vllm-h100`.
fn listed_snapshot() -> Arc<kinference_policy::resolve::PolicyIndex> {
    use kinference_policy::crd::{
        Destination, InferencePoolSpec, InferenceWorkerSet, InferenceWorkerSetSpec, ServiceRef,
    };
    let meta = |name: &str| ObjectMeta {
        name: Some(name.into()),
        namespace: Some("infra".into()),
        uid: Some(format!("uid-{name}")),
        ..Default::default()
    };
    let pool_cr = InferencePool {
        metadata: meta("chat"),
        spec: InferencePoolSpec {
            destinations: vec![Destination {
                kind: Some("InferenceWorkerSet".into()),
                name: Some("workers-h100".into()),
                ..Default::default()
            }],
            policy_ref: None,
        },
        status: None,
    };
    let set = InferenceWorkerSet {
        metadata: meta("workers-h100"),
        spec: InferenceWorkerSetSpec {
            service_ref: ServiceRef {
                name: "vllm-h100".into(),
                port: None,
            },
            profile: Some("h100".into()),
            kv_block_size: Some(16),
            ..Default::default()
        },
        status: None,
    };
    let pools_by_key = kinference_policy::resolve::PoolIndex::from([(
        kinference_policy::PoolKey::namespaced("infra", "workers-h100"),
        PoolId::from_raw(1),
    )]);
    let index = kinference_policy::resolve::resolve(
        &[Arc::new(pool_cr)],
        &[],
        &[Arc::new(set)],
        &[],
        &[],
        &[],
        &pools_by_key,
        &Default::default(),
    )
    .with_services(index());
    assert_eq!(
        index.lookup("infra", "chat").unwrap().destinations.len(),
        1,
        "the fixture's destination resolves"
    );
    Arc::new(index)
}

fn writer_over(api: &Recorded) -> BindingWriter<Recorded> {
    let store = PolicyStore::new();
    store.publish(listed_snapshot());
    let (tx, rx) = watch::channel(true);
    std::mem::forget(tx);
    BindingWriter::new(api.clone(), Some("infra".into()), store, ext(), rx)
}

fn chat() -> crate::api::recorded::Key {
    key::<BackendBinding>("infra", "chat")
}

#[tokio::test]
async fn a_pool_with_no_binding_gets_one_applied() {
    let api = Recorded::default();
    writer_over(&api).reconcile_once().await.unwrap();
    assert_eq!(
        api.writes(),
        vec![Write::Apply {
            key: chat(),
            manager: FIELD_MANAGER.into()
        }]
    );
    let binding: BackendBinding = api.object("infra", "chat").unwrap();
    assert!(is_ours(&binding));
    assert_eq!(binding.spec.claim.name, "chat");
}

#[tokio::test]
async fn an_in_sync_binding_is_left_alone() {
    let api = Recorded::default();
    let w = writer_over(&api);
    w.reconcile_once().await.unwrap();
    let first = api.writes();
    w.reconcile_once().await.unwrap();
    assert_eq!(api.writes(), first, "what we applied is what is listed");
}

#[tokio::test]
async fn a_binding_that_is_not_ours_is_never_adopted() {
    let mut foreign = desired(
        listed_snapshot().lookup("infra", "chat").unwrap(),
        ext().as_ref(),
    );
    foreign.metadata.labels = None;
    foreign.spec.extensions.clear();
    let api = Recorded::default().with([foreign]);
    writer_over(&api).reconcile_once().await.unwrap();
    assert!(api.writes().is_empty(), "left alone, drift and all");
}

#[tokio::test]
async fn an_orphaned_binding_of_ours_is_deleted() {
    let mut orphan = desired(
        listed_snapshot().lookup("infra", "chat").unwrap(),
        ext().as_ref(),
    );
    orphan.metadata.name = Some("gone".into());
    let api = Recorded::default().with([orphan]);
    writer_over(&api).reconcile_once().await.unwrap();
    assert_eq!(
        api.writes(),
        vec![
            Write::Apply {
                key: chat(),
                manager: FIELD_MANAGER.into()
            },
            Write::Delete(key::<BackendBinding>("infra", "gone")),
        ]
    );
    assert!(api.object::<BackendBinding>("infra", "gone").is_none());
}

#[tokio::test]
async fn a_missing_binding_crd_publishes_nothing() {
    let api = Recorded::default().without::<BackendBinding>();
    writer_over(&api).reconcile_once().await.unwrap();
    assert!(api.writes().is_empty());
}

/// Counts ERROR events emitted on this thread.
struct CountErrors(Arc<std::sync::atomic::AtomicUsize>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountErrors {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::ERROR {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// A clean shutdown does not log the leader channel as lost.
#[tokio::test]
async fn shutdown_logs_no_error() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(CountErrors(errors.clone())),
    );
    // The race this guards against is random, so repeat it.
    for _ in 0..32 {
        let (tx, rx) = watch::channel(false);
        let w = BindingWriter::new(
            Recorded::default(),
            Some("infra".into()),
            PolicyStore::new(),
            ext(),
            rx,
        );
        let cancel = CancellationToken::new();
        // The elector's order on shutdown: cancel fires, then a last send and drop.
        cancel.cancel();
        tx.send(false).expect("the writer holds the receiver");
        drop(tx);
        // Awaited on this thread so the thread-local subscriber sees its events.
        tokio::time::timeout(std::time::Duration::from_secs(5), w.run(cancel))
            .await
            .expect("the writer exits on cancel");
    }
    assert_eq!(errors.load(std::sync::atomic::Ordering::Relaxed), 0);
}

/// A leader whose elector drops the leader channel stops writing.
#[tokio::test(start_paused = true)]
async fn a_closed_leader_channel_stops_writes() {
    let api = Recorded::default();
    let store = PolicyStore::new();
    store.publish(listed_snapshot());
    let (tx, rx) = watch::channel(true);
    let w = BindingWriter::new(api.clone(), Some("infra".into()), store, ext(), rx);
    drop(tx);

    let cancel = CancellationToken::new();
    let task = tokio::spawn(w.run(cancel.clone()));
    // Two ticks past the first.
    tokio::time::sleep(std::time::Duration::from_secs(25)).await;
    cancel.cancel();
    task.await.expect("the writer does not panic");

    assert!(api.writes().is_empty(), "wrote {:?}", api.writes());
}

/// A reconcile that starts after demotion neither applies nor prunes.
#[tokio::test]
async fn a_demoted_reconcile_writes_nothing() {
    let want = desired(
        listed_snapshot().lookup("infra", "chat").unwrap(),
        ext().as_ref(),
    );
    let mut orphan = want.clone();
    orphan.metadata.name = Some("gone".into());
    // The first cluster needs an apply, the second only a delete.
    for existing in [vec![orphan.clone()], vec![want, orphan]] {
        let api = Recorded::default().with(existing);
        let store = PolicyStore::new();
        store.publish(listed_snapshot());
        let (tx, rx) = watch::channel(true);
        let w = BindingWriter::new(api.clone(), Some("infra".into()), store, ext(), rx);
        tx.send(false).expect("the writer holds the receiver");

        w.reconcile_once().await.unwrap();

        assert!(api.writes().is_empty(), "wrote {:?}", api.writes());
    }
}
