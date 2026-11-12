use super::*;
use kinference_fleet::profile::{
    PickerConfig, PredictorConfig, ProfileTokenizeConfig, TokenizeMode,
};
use kinference_policy::crd::{InferenceWorkerSetSpec, KvEvents, ServiceRef};

fn profile(name: &str) -> ProfileConfig {
    ProfileConfig {
        name: name.to_string(),
        predictor: PredictorConfig::default(),
        picker: PickerConfig::default(),
        tokenize: ProfileTokenizeConfig {
            mode: TokenizeMode::Local,
        },
    }
}

fn iws(ns: &str, name: &str, spec: InferenceWorkerSetSpec) -> InferenceWorkerSet {
    let mut o = InferenceWorkerSet::new(name, spec);
    o.metadata.namespace = Some(ns.to_string());
    o
}

fn full_spec() -> InferenceWorkerSetSpec {
    InferenceWorkerSetSpec {
        service_ref: ServiceRef {
            name: "vllm".into(),
            port: None,
        },
        profile: Some("h100".into()),
        kv_block_size: Some(16),
        metrics_port: None,
        kv_events: None,
        model_discovery: None,
        models: Vec::new(),
    }
}

#[test]
fn a_complete_set_resolves_against_its_profile() {
    let (out, rejected) = desired_pools(
        &[iws("infra", "workers-h100", full_spec())],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty(), "{rejected:?}");
    let spec = out
        .get(&PoolKey::namespaced("infra", "workers-h100"))
        .unwrap();
    assert_eq!(spec.profile, "h100");
    assert_eq!(spec.kv_block_size, 16);
    assert_eq!(spec.tokenize_mode, TokenizeMode::Local);
    assert_eq!(spec.discovery.service_name.as_deref(), Some("vllm"));
    assert_eq!(
        spec.discovery.namespace.as_deref(),
        Some("infra"),
        "the slice watch must be scoped to the set's own namespace"
    );
}

/// Same-named Services in different namespaces are different fleets, and
/// must not be merged into one pool.
#[test]
fn same_named_sets_in_two_namespaces_stay_separate() {
    let (out, rejected) = desired_pools(
        &[
            iws("a", "workers", full_spec()),
            iws("b", "workers", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty());
    assert_eq!(out.len(), 2);
    let a = out.get(&PoolKey::namespaced("a", "workers")).unwrap();
    let b = out.get(&PoolKey::namespaced("b", "workers")).unwrap();
    assert_ne!(a.name, b.name, "names must not collide either");
    assert_eq!(a.discovery.namespace.as_deref(), Some("a"));
    assert_eq!(b.discovery.namespace.as_deref(), Some("b"));
}

/// An unknown profile (fixed in the router's config) and a missing one (fixed
/// in the CR) get different reasons. The unknown case lists what is defined.
#[test]
fn an_unknown_profile_is_rejected_and_lists_the_defined_ones() {
    let mut spec = full_spec();
    spec.profile = Some("h1OO".into());
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100"), profile("a100")],
        &DiscoveryDefaults::default(),
    );
    assert!(out.is_empty());
    assert_eq!(rejected.len(), 1);
    match &rejected[0].reason {
        RejectReason::UnknownProfile { defined } => {
            assert_eq!(defined, &["a100".to_string(), "h100".to_string()]);
        }
        other => panic!("expected UnknownProfile, got {other:?}"),
    }
    assert_eq!(rejected[0].reason.as_condition_reason(), "UnknownProfile");
}

/// An object missing required fields is rejected individually rather than
/// failing deserialization of the whole list page.
#[test]
fn a_set_missing_required_fields_is_rejected_without_taking_the_others_down() {
    let mut no_profile = full_spec();
    no_profile.profile = None;
    let mut no_block = full_spec();
    no_block.kv_block_size = None;
    let mut zero_block = full_spec();
    zero_block.kv_block_size = Some(0);
    let mut no_svc = full_spec();
    no_svc.service_ref.name = String::new();

    let (out, rejected) = desired_pools(
        &[
            iws("infra", "bad-profile", no_profile),
            iws("infra", "bad-block", no_block),
            iws("infra", "zero-block", zero_block),
            iws("infra", "bad-svc", no_svc),
            iws("infra", "good", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(out.len(), 1, "the healthy set must still materialize");
    assert!(out.contains_key(&PoolKey::namespaced("infra", "good")));
    let reasons: Vec<&str> = rejected
        .iter()
        .map(|r| r.reason.as_condition_reason())
        .collect();
    assert_eq!(
        reasons,
        [
            "MissingProfile",
            "MissingKvBlockSize",
            "InvalidKvBlockSize",
            "MissingServiceRef"
        ]
    );
}

/// A model row without a `name` must still parse, since `watcher`
/// deserializes list pages wholesale.
#[test]
fn a_models_row_missing_its_name_parses_rather_than_failing_the_page() {
    let spec: InferenceWorkerSetSpec = serde_json::from_value(serde_json::json!({
        "serviceRef": {"name": "vllm", "port": "http"},
        "profile": "h100",
        "kvBlockSize": 16,
        // What the API server leaves after pruning an undeclared field.
        "models": [{"aliases": ["qwen-2.5"]}]
    }))
    .expect("must parse: failing here fails every set in the list page");
    assert_eq!(spec.models.len(), 1);
    assert!(spec.models[0].name.is_none());
}

/// ...and is then refused, since its aliases would resolve to no model.
#[test]
fn a_models_row_missing_its_name_is_rejected() {
    let mut spec = full_spec();
    spec.models = vec![kinference_policy::crd::ModelAlias {
        name: None,
        aliases: vec!["qwen-2.5".into()],
    }];
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(out.is_empty(), "no pool: the set is not routable");
    assert_eq!(rejected[0].reason, RejectReason::ModelMissingName);
    assert_eq!(rejected[0].reason.as_condition_reason(), "ModelMissingName");
}

/// A row with a name and no aliases is routable.
#[test]
fn a_models_row_with_no_aliases_is_fine() {
    let mut spec = full_spec();
    spec.models = vec![kinference_policy::crd::ModelAlias {
        name: Some("meta/llama-3.3-70b".into()),
        aliases: vec![],
    }];
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty(), "{rejected:?}");
    assert_eq!(out.len(), 1);
}

/// `kvEvents: {}` must parse, since `watcher` deserializes list pages
/// wholesale.
#[test]
fn an_empty_kv_events_block_parses_rather_than_failing_the_page() {
    let spec: InferenceWorkerSetSpec = serde_json::from_value(serde_json::json!({
        "serviceRef": {"name": "vllm", "port": "http"},
        "profile": "h100",
        "kvBlockSize": 16,
        "kvEvents": {}
    }))
    .expect("must parse: failing here fails every set in the list page");
    assert!(spec.kv_events.is_some(), "the block is present");
    assert!(spec.kv_events.unwrap().port.is_none(), "but says no port");
}

/// ...and is then refused, rather than defaulted to a port or to "publishes
/// nothing", either of which yields a pool with an empty prefix model.
#[test]
fn a_kv_events_block_with_no_port_is_rejected() {
    let mut spec = full_spec();
    spec.kv_events = Some(KvEvents {
        port: None,
        replay_port: None,
    });
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(out.is_empty(), "no pool: the set is not routable");
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].reason, RejectReason::KvEventsMissingPort);
    assert_eq!(
        rejected[0].reason.as_condition_reason(),
        "KvEventsMissingPort"
    );
    assert!(
        rejected[0]
            .reason
            .to_string()
            .contains("omit the whole block"),
        "the message must name the fix: {}",
        rejected[0].reason
    );
}

/// End to end for the portless-`kvEvents` case: `desired_pools` refuses it,
/// `rejection_index` projects the reason, and `bind` returns `Incomplete`.
#[test]
fn a_portless_kv_events_block_binds_as_incomplete() {
    use kinference_policy::crd::{Destination, InferencePool, InferencePoolSpec};
    use kinference_policy::resolve::{PoolIndex, UnresolvedReason, rejection_index, resolve};

    let mut spec = full_spec();
    spec.kv_events = Some(KvEvents {
        port: None,
        replay_port: None,
    });
    let ws = iws("infra", "workers-h100", spec);
    let (pools_built, rejections) = desired_pools(
        std::slice::from_ref(&ws),
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(pools_built.is_empty(), "refused, so it produced no pool");

    let mut pool = InferencePool::new(
        "chat",
        InferencePoolSpec {
            destinations: vec![Destination {
                kind: Some("InferenceWorkerSet".into()),
                name: Some("workers-h100".into()),
                ..Default::default()
            }],
            policy_ref: None,
        },
    );
    pool.metadata.namespace = Some("infra".into());
    let idx = resolve(
        &[std::sync::Arc::new(pool)],
        &[],
        &[std::sync::Arc::new(ws)],
        &[],
        &[],
        &[],
        // What `desired_pools` built: empty.
        &PoolIndex::new(),
        &rejection_index(&rejections),
    );

    // Reported on the pool.
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved.len(), 1);
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::Incomplete);

    // And on the set's own entry.
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(e.bound.as_ref().err(), Some(&UnresolvedReason::Incomplete));
}

/// Omitting the block entirely is legal: it says these engines publish
/// nothing.
#[test]
fn an_omitted_kv_events_block_is_not_a_rejection() {
    let mut spec = full_spec();
    spec.kv_events = None;
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(rejected.len(), 0);
    assert_eq!(out.len(), 1);
    assert!(
        out.values()
            .next()
            .unwrap()
            .discovery
            .ports
            .kv_events_declared_absent
    );
}

#[test]
fn declared_ports_reach_discovery_by_name_or_number() {
    let mut spec = full_spec();
    spec.service_ref.port = Some(IntOrString::Str("openai".into()));
    spec.metrics_port = Some(IntOrString::Int(9090));
    spec.kv_events = Some(KvEvents {
        port: Some(IntOrString::Str("kv-pub".into())),
        replay_port: Some(IntOrString::Str("kv-replay".into())),
    });

    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let ports = &out.values().next().unwrap().discovery.ports;
    assert_eq!(ports.http, Some(PortRef::Name("openai".into())));
    assert_eq!(ports.metrics, Some(PortRef::Number(9090)));
    assert_eq!(ports.replay, Some(PortRef::Name("kv-replay".into())));
    assert_eq!(ports.zmq, Some(PortRef::Name("kv-pub".into())));
}

/// Unset ports stay unset, so discovery falls back to port names and then
/// defaults.
#[test]
fn unset_ports_leave_the_conventional_resolution_alone() {
    let mut spec = full_spec();
    spec.kv_events = Some(KvEvents {
        port: Some(IntOrString::Str("kv-pub".into())),
        replay_port: None,
    });
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let ports = &out.values().next().unwrap().discovery.ports;
    assert_eq!(ports.http, None);
    assert_eq!(ports.metrics, None);
    assert_eq!(ports.replay, None);
    assert!(!ports.kv_events_declared_absent);
}

/// An absent `kvEvents` declares that these engines don't publish, rather
/// than falling back to a default port. ZMQ `connect` always succeeds, so a
/// silent socket would go unnoticed.
#[test]
fn an_absent_kv_events_block_is_honored_rather_than_defaulted() {
    let (out, _) = desired_pools(
        &[iws("infra", "ws", full_spec())],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let ports = &out.values().next().unwrap().discovery.ports;
    assert!(ports.kv_events_declared_absent);
    assert_eq!(ports.zmq, None);
}

#[test]
fn model_discovery_sets_the_interval_and_the_static_list() {
    let mut spec = full_spec();
    // Two upstream models sharing one alias.
    spec.models = vec![
        kinference_policy::crd::ModelAlias {
            name: Some("meta-llama/Llama-3.3-70B-Instruct".into()),
            aliases: vec!["chat".into()],
        },
        kinference_policy::crd::ModelAlias {
            name: Some("neuralmagic/Llama-3.3-70B-Instruct-FP8".into()),
            aliases: vec!["chat".into()],
        },
    ];
    spec.model_discovery = Some(kinference_policy::crd::ModelDiscovery {
        mode: ModelDiscoveryMode::Static,
        interval: "2m".into(),
    });
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let resolved = out.values().next().unwrap();
    assert_eq!(resolved.models_refresh_secs, Some(120));
    assert_eq!(
        resolved.static_models.as_deref(),
        Some(
            [
                "meta-llama/Llama-3.3-70B-Instruct".to_string(),
                "neuralmagic/Llama-3.3-70B-Instruct-FP8".to_string()
            ]
            .as_slice()
        ),
        "Static mode seeds the UPSTREAM names, which is what the engine serves"
    );
}

/// Endpoint mode is the default and produces no static list.
#[test]
fn endpoint_mode_leaves_discovery_on() {
    let mut spec = full_spec();
    spec.model_discovery = Some(kinference_policy::crd::ModelDiscovery::default());
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(out.values().next().unwrap().static_models, None);
}

/// A bad interval falls back to the fleet default rather than rejecting the
/// set.
#[test]
fn an_unparseable_interval_falls_back_instead_of_rejecting() {
    let mut spec = full_spec();
    spec.model_discovery = Some(kinference_policy::crd::ModelDiscovery {
        mode: ModelDiscoveryMode::Endpoint,
        interval: "soon".into(),
    });
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty(), "a bad interval must not kill the set");
    assert_eq!(out.values().next().unwrap().models_refresh_secs, None);
}

/// An out-of-range port number is dropped, not truncated.
#[test]
fn an_out_of_range_port_is_dropped_not_truncated() {
    let mut spec = full_spec();
    spec.metrics_port = Some(IntOrString::Int(70000));
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(out.values().next().unwrap().discovery.ports.metrics, None);
}

/// The desired set doesn't depend on the order the watch delivered objects
/// in.
#[test]
fn the_desired_set_is_order_independent() {
    let forward = desired_pools(
        &[
            iws("infra", "a", full_spec()),
            iws("infra", "b", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let reverse = desired_pools(
        &[
            iws("infra", "b", full_spec()),
            iws("infra", "a", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(
        forward.0.keys().collect::<Vec<_>>(),
        reverse.0.keys().collect::<Vec<_>>()
    );
}

// --- watch to reconcile, against hand-seeded reflector stores ---

fn watcher_with(
    sets: &[InferenceWorkerSet],
    profiles: Vec<ProfileConfig>,
) -> (
    WorkerSetWatcher,
    std::sync::Arc<kinference_fleet::pool::PoolRegistry>,
) {
    let (store, mut writer) = reflector::store::<InferenceWorkerSet>();
    for s in sets {
        writer.apply_watcher_event(&watcher::Event::Apply(s.clone()));
    }
    let registry = kinference_fleet::pool::PoolRegistry::new();
    let manager = kinference_fleet::pool::PoolManager::new(
        registry.clone(),
        std::sync::Arc::new(NoopHooks),
        CancellationToken::new(),
        std::sync::Arc::new(kinference_fleet::models::ModelIndex::new()),
        kinference_fleet::pool::ReapPolicy {
            linger: Duration::ZERO,
            backstop: Duration::from_secs(600),
        },
    );
    let cfg = KubernetesConfig {
        enabled: true,
        namespace: Some("infra".into()),
        selector: None,
        discovery_defaults: DiscoveryDefaults::default(),
    };
    (
        WorkerSetWatcher::new(
            &cfg,
            profiles,
            manager,
            RejectionStore::new(),
            std::sync::Arc::new(crate::metrics::ControllerMetrics::new(
                &prometheus::Registry::new(),
            )),
            tokio::sync::watch::channel(false).0,
        )
        .with_store(store),
        registry,
    )
}

struct NoopHooks;
impl kinference_fleet::pool::PoolHooks for NoopHooks {
    fn on_create(
        &self,
        _pool: &std::sync::Arc<kinference_fleet::pool::Pool>,
        _spec: &PoolSpec,
    ) -> Result<(), kinference_fleet::error::Error> {
        Ok(())
    }
    fn on_retire(&self, _pool: &std::sync::Arc<kinference_fleet::pool::Pool>) {}
}

#[tokio::test]
async fn initial_list_publishes_readiness_after_materializing_worker_sets() {
    use crate::api::recorded::Recorded;

    for sets in [vec![], vec![iws("infra", "a", full_spec())]] {
        let expected = sets.len();
        let (watcher, registry) = watcher_with(&[], vec![profile("h100")]);
        let mut ready = watcher.ready.subscribe();
        assert!(!*ready.borrow());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watcher.run(Recorded::default().with(sets), cancel.clone()));
        tokio::time::timeout(Duration::from_secs(2), ready.wait_for(|ready| *ready))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(registry.snapshot().len(), expected);
        cancel.cancel();
        task.await.unwrap();
        assert!(*ready.borrow());
    }
}

#[tokio::test]
async fn initial_list_failure_does_not_publish_readiness() {
    use crate::api::recorded::Recorded;

    let (watcher, registry) = watcher_with(&[], vec![profile("h100")]);
    let mut ready = watcher.ready.subscribe();
    let cancel = CancellationToken::new();
    let api = Recorded::default().without::<InferenceWorkerSet>();
    let task = tokio::spawn(watcher.run(api, cancel.clone()));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), ready.changed())
            .await
            .is_err()
    );
    assert!(!*ready.borrow());
    assert!(registry.snapshot().is_empty());
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn a_listed_store_materializes_its_sets() {
    let (mut w, registry) = watcher_with(&[iws("infra", "a", full_spec())], vec![profile("h100")]);
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().len(), 1);
    assert_eq!(registry.snapshot().iter().next().unwrap().name, "infra/a");
}

/// `desired_pools` is total, so reconciling an empty store retracts every
/// pool. This is why `run_once` doesn't reconcile before `InitDone`.
#[tokio::test]
async fn reconciling_against_an_empty_store_would_retract_every_pool() {
    let (mut w, registry) = watcher_with(
        &[
            iws("infra", "a", full_spec()),
            iws("infra", "b", full_spec()),
        ],
        vec![profile("h100")],
    );
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().len(), 2);

    let (empty, _) = reflector::store::<InferenceWorkerSet>();
    w.store = Some(empty);
    w.reconcile_from_store();
    assert_eq!(
        registry.snapshot().len(),
        0,
        "an empty store retracts everything — which is exactly why the \
         reconcile is gated on InitDone"
    );
}

/// A set losing its profile must not take the healthy ones with it.
#[tokio::test]
async fn one_rejected_set_does_not_disturb_the_others() {
    let mut bad = full_spec();
    bad.profile = Some("nope".into());
    let (mut w, registry) = watcher_with(
        &[iws("infra", "good", full_spec()), iws("infra", "bad", bad)],
        vec![profile("h100")],
    );
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().len(), 1);
    assert_eq!(
        registry.snapshot().iter().next().unwrap().name,
        "infra/good"
    );
}

/// Re-reconciling an unchanged store is a no-op.
#[tokio::test]
async fn re_reconciling_an_unchanged_store_keeps_the_same_pools() {
    let (mut w, registry) = watcher_with(&[iws("infra", "a", full_spec())], vec![profile("h100")]);
    w.reconcile_from_store();
    let id = registry.snapshot().iter().next().unwrap().id;
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().iter().next().unwrap().id, id);
}

/// The watch over a recorded API server lists the set and materializes it.
#[tokio::test]
async fn a_recorded_set_materializes_through_the_watch() {
    use crate::api::recorded::Recorded;

    let (w, registry) = watcher_with(&[], vec![profile("h100")]);
    let api = Recorded::default().with([iws("infra", "a", full_spec())]);
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let cancel = cancel.clone();
        async move { w.run(api, cancel).await }
    });
    let live = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pools = registry.snapshot();
            if !pools.is_empty() {
                break pools;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the set materializes once the watch has listed");
    cancel.cancel();
    task.await.expect("the watcher exits on cancel");
    assert_eq!(live.iter().next().unwrap().name, "infra/a");
}
