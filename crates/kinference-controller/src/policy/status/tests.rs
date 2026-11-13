use super::*;
use crate::api::recorded::{Key, Recorded, Write, key};
use kinference_policy::compile::CompiledPolicy;
use kinference_policy::crd::{InferencePolicySpec, ModelAlias, ParentRef};
use kinference_policy::resolve::{Unresolved, UnresolvedReason};

fn dest(name: &str, aliases: &[(&str, &str)]) -> ResolvedWorkerSet {
    ResolvedWorkerSet {
        name: name.into(),
        namespace: "infra".into(),
        labels: BTreeMap::new(),
        pool_id: PoolId::from_raw(0),
        service_name: "vllm-h100".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures pass (canonical, upstream); the canonical name becomes
            // an alias of the upstream.
            .map(|(n, u)| ModelAlias {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
            })
            .collect(),
    }
}

fn facts(
    ready: i64,
    models: &[(&str, i64, Option<i64>, Option<i64>)],
) -> HashMap<PoolId, PoolFacts> {
    let mut m = HashMap::new();
    m.insert(
        PoolId::from_raw(0),
        PoolFacts {
            ready_endpoints: ready,
            event_endpoints: ready,
            models: models
                .iter()
                .map(|(u, n, lo, hi)| ModelFact {
                    upstream: u.to_string(),
                    ready_endpoints: *n,
                    ctx_min: *lo,
                    ctx_max: *hi,
                })
                .collect(),
        },
    );
    m
}

/// A `ResolvedPolicy` wrapper for the status tests, referenced by one pool.
fn rp(policy: CompiledPolicy) -> ResolvedPolicy {
    ResolvedPolicy {
        uid: None,
        namespace: "infra".into(),
        name: "chat-policy".into(),
        generation: Some(7),
        policy: Arc::new(policy),
        // Status never reads the raw spec.
        spec: Default::default(),
        referenced_by: vec!["chat".into()],
        reference_warnings: Vec::new(),
    }
}

/// The same policy with nothing pointing at it.
fn rp_unreferenced(policy: CompiledPolicy) -> ResolvedPolicy {
    ResolvedPolicy {
        referenced_by: Vec::new(),
        ..rp(policy)
    }
}

fn resolved(dests: Vec<ResolvedWorkerSet>, unresolved: Vec<Unresolved>) -> ResolvedPool {
    ResolvedPool {
        uid: None,
        namespace: "infra".into(),
        name: "chat".into(),
        resource_version: "100".into(),
        generation: Some(3),
        policy: Arc::new(CompiledPolicy::serve_as_asked("<implicit>")),
        dangling_policy: None,
        destinations: dests
            .into_iter()
            .map(kinference_policy::resolve::ResolvedDestination::Local)
            .collect(),
        unresolved,
        parents: vec![ParentRef {
            kind: Some("Gateway".into()),
            name: "inference-gateway".into(),
            namespace: Some("infra".into()),
            ..Default::default()
        }],
    }
}

/// A rejected worker set reports the rejection on its own object, with
/// pool-derived fields cleared.
fn pcond<'a>(s: &'a InferenceProviderStatus, ty: &str) -> &'a Condition {
    s.conditions
        .iter()
        .find(|c| c.r#type == ty)
        .expect("condition present")
}

/// No condition message may carry a run of spaces, as left by a broken line
/// continuation in a string literal.
#[test]
fn no_condition_message_has_a_double_space() {
    let d = dest("workers-h100", &[]);
    let health = |suppressed, fail| ProviderHealth {
        suppressed,
        last_failure: fail,
    };
    let mut msgs: Vec<String> = Vec::new();
    let mut collect = |conds: &[Condition]| {
        msgs.extend(conds.iter().map(|c| c.message.clone()));
    };
    collect(&desired_worker_set_status(&d, &facts(0, &[]), &[], None, None).conditions);
    collect(
        &desired_worker_set_status(&d, &facts(2, &[]), &["p".to_string()], None, None).conditions,
    );
    collect(
        &rejected_worker_set_status(
            &kinference_policy::rejection::RejectReason::MissingKvBlockSize,
            &[],
            None,
        )
        .conditions,
    );
    collect(
        &unbound_worker_set_status(UnresolvedReason::NoMatchingPool, &[], None, None).conditions,
    );
    collect(
        &desired_pool_status(
            &resolved(vec![dest("w", &[])], vec![]),
            &facts(1, &[]),
            None,
        )
        .conditions,
    );
    collect(&desired_provider_status(&health(false, None), None).conditions);
    collect(
        &desired_provider_status(
            &health(true, Some((FailureKind::Unauthorized, "401".into()))),
            None,
        )
        .conditions,
    );
    for r in [
        UnresolvedReason::UnsupportedApiFormat,
        UnresolvedReason::Incomplete,
        UnresolvedReason::CrossNamespaceNotGranted,
        UnresolvedReason::AmbiguousName,
    ] {
        collect(&rejected_provider_status(r, None).conditions);
    }
    collect(&desired_policy_status(&rp_unreferenced(one_rule()), None).conditions);

    assert!(
        !msgs.is_empty(),
        "the harness must actually collect messages"
    );
    for m in &msgs {
        assert!(
            !m.contains("  "),
            "condition message has a run of spaces: {m:?}"
        );
    }
}

/// Every kind's `state` is the primary verdict, then comma-separated
/// qualifiers.
#[test]
fn state_tokens_compose_across_the_kinds() {
    let d = dest("workers-h100", &[]);

    // Worker set: accepted, but idle and in no pool.
    let idle = desired_worker_set_status(&d, &facts(0, &[]), &[], None, None);
    assert_eq!(
        idle.state.as_deref(),
        Some("Accepted,NotReady,Unreferenced")
    );

    let serving = desired_worker_set_status(&d, &facts(3, &[]), &["chat".to_string()], None, None);
    assert_eq!(serving.state.as_deref(), Some("Accepted"));

    // A rejection stands alone, without `NotReady`.
    let broken = rejected_worker_set_status(
        &kinference_policy::rejection::RejectReason::MissingKvBlockSize,
        &[],
        None,
    );
    assert_eq!(broken.state.as_deref(), Some("MissingKvBlockSize"));

    // Pool: a destination dangles and the policyRef names the wrong kind.
    // Both are qualifiers since it still serves.
    let mut r = resolved(vec![dest("workers-h100", &[])], vec![]);
    r.unresolved = vec![Unresolved {
        kind: "InferenceWorkerSet".into(),
        name: "gone".into(),
        uid: None,
        reason: UnresolvedReason::NotFound,
    }];
    r.dangling_policy = Some(DanglingPolicy::WrongKind {
        name: "p".into(),
        group: "kinference.buoyant.io".into(),
        kind: "ConfigMap".into(),
    });
    let pool = desired_pool_status(&r, &facts(1, &[]), None);
    assert_eq!(
        pool.state.as_deref(),
        Some("Accepted,UnresolvedDestinations,PolicyRefWrongKind")
    );

    // Provider: well-formed but producing nothing.
    let suppressed = desired_provider_status(
        &ProviderHealth {
            suppressed: true,
            last_failure: Some((FailureKind::Unauthorized, "401".into())),
        },
        None,
    );
    assert_eq!(
        suppressed.state.as_deref(),
        Some("Accepted,BackendUnauthorized,Suppressed")
    );
}

/// Never empty on any kind, so a blank column means no status was written.
#[test]
fn state_always_carries_at_least_one_token() {
    let d = dest("workers-h100", &[]);
    for st in [
        desired_worker_set_status(&d, &facts(1, &[]), &["p".to_string()], None, None)
            .state
            .clone(),
        desired_pool_status(
            &resolved(vec![dest("w", &[])], vec![]),
            &facts(1, &[]),
            None,
        )
        .state
        .clone(),
        desired_provider_status(
            &ProviderHealth {
                suppressed: false,
                last_failure: None,
            },
            None,
        )
        .state
        .clone(),
    ] {
        let st = st.expect("state is always written");
        assert!(!st.is_empty(), "and never the empty string");
    }
}

/// A healthy provider carries `Accepted=True`.
#[test]
fn a_healthy_provider_reports_accepted() {
    let s = desired_provider_status(
        &ProviderHealth {
            suppressed: false,
            last_failure: None,
        },
        None,
    );
    assert_eq!(pcond(&s, "Accepted").status, "True");
    assert_eq!(pcond(&s, "Accepted").reason, "Accepted");
    assert_eq!(
        s.conditions[0].r#type, "Accepted",
        "first, like every other kind, since a printer column reads it"
    );
    assert_eq!(pcond(&s, "ResolvedRefs").status, "True");
    assert_eq!(pcond(&s, "Ready").status, "True");
}

/// Rejected then fixed lands on True, not on a missing condition.
#[test]
fn a_provider_that_recovers_flips_accepted_to_true() {
    let broken = rejected_provider_status(UnresolvedReason::Incomplete, None);
    assert_eq!(pcond(&broken, "Accepted").status, "False");
    let fixed = desired_provider_status(
        &ProviderHealth {
            suppressed: false,
            last_failure: None,
        },
        Some(&broken),
    );
    assert_eq!(pcond(&fixed, "Accepted").status, "True");
}

fn cond<'a>(s: &'a InferenceWorkerSetStatus, ty: &str) -> &'a Condition {
    s.conditions
        .iter()
        .find(|c| c.r#type == ty)
        .expect("condition present")
}

/// A set no pool names is accepted, since it still serves every `Pick` that
/// names no pool. Unlike an unreferenced policy.
#[test]
fn an_unreferenced_worker_set_is_accepted_but_not_referenced() {
    let d = dest("workers-h100", &[]);
    let s = desired_worker_set_status(&d, &facts(2, &[]), &[], None, None);

    assert_eq!(cond(&s, "Accepted").status, "True");
    assert_eq!(cond(&s, "Accepted").reason, "Accepted");
    assert_eq!(cond(&s, "Ready").status, "True", "2 ready endpoints");

    let r = cond(&s, "Referenced");
    assert_eq!(r.status, "False");
    assert_eq!(r.reason, "NoPoolNamesIt");
    assert!(r.message.contains("no pool-scoped policy"));
    assert!(
        r.message.contains("still serves a Pick that names no pool"),
        "must not read as unreachable"
    );
    assert!(
        r.message.contains("patch infpool"),
        "the message carries the fix: {}",
        r.message
    );
    assert!(s.referenced_by.is_empty());
}

#[test]
fn a_referenced_worker_set_names_the_pools_routing_to_it() {
    let d = dest("workers-h100", &[]);
    let refs = vec!["chat".to_string(), "batch".to_string()];
    let s = desired_worker_set_status(&d, &facts(1, &[]), &refs, None, None);

    let r = cond(&s, "Referenced");
    assert_eq!(r.status, "True");
    assert_eq!(r.reason, "InPool");
    assert!(r.message.contains("chat, batch"), "{}", r.message);
    assert_eq!(
        s.referenced_by, refs,
        "reported as a field, not only in prose"
    );
}

/// Unreferenced and unready are independent.
#[test]
fn referenced_and_ready_are_independent() {
    let d = dest("workers-h100", &[]);
    let unref_ready = desired_worker_set_status(&d, &facts(3, &[]), &[], None, None);
    assert_eq!(cond(&unref_ready, "Ready").status, "True");
    assert_eq!(cond(&unref_ready, "Referenced").status, "False");

    let refd_unready =
        desired_worker_set_status(&d, &facts(0, &[]), &["chat".to_string()], None, None);
    assert_eq!(cond(&refd_unready, "Ready").status, "False");
    assert_eq!(cond(&refd_unready, "Referenced").status, "True");
}

#[test]
fn an_unbound_worker_set_reports_the_reason_it_did_not_bind() {
    let s = unbound_worker_set_status(UnresolvedReason::NoMatchingPool, &[], Some(7), None);
    assert_eq!(cond(&s, "Accepted").status, "False");
    assert_eq!(cond(&s, "Accepted").reason, "NoMatchingPool");
    assert_eq!(cond(&s, "Accepted").observed_generation, Some(7));
    assert_eq!(s.endpoints.as_ref().unwrap().ready, Some(0));
    assert!(s.models.is_empty());
}

/// A rejected set keeps `referencedBy`.
#[test]
fn a_rejected_worker_set_still_reports_who_named_it() {
    let reason = kinference_policy::rejection::RejectReason::MissingKvBlockSize;
    let s = rejected_worker_set_status(&reason, &["chat".to_string()], None);
    assert_eq!(s.referenced_by, vec!["chat".to_string()]);
    assert_eq!(cond(&s, "Accepted").status, "False");
}

/// Same inputs, same bytes, or the writer's last-written comparison never
/// breaks the patch loop.
#[test]
fn unreferenced_status_is_stable_across_reconciles() {
    let d = dest("workers-h100", &[]);
    let a = desired_worker_set_status(&d, &facts(1, &[]), &[], None, None);
    let b = desired_worker_set_status(&d, &facts(1, &[]), &[], None, Some(&a));
    assert_eq!(
        serde_json::to_value(&a).unwrap(),
        serde_json::to_value(&b).unwrap()
    );
}

#[test]
fn a_rejected_worker_set_reports_why_and_clears_its_counts() {
    let reason = kinference_policy::rejection::RejectReason::UnknownProfile {
        defined: vec!["h100".into(), "a100".into()],
    };
    let st = rejected_worker_set_status(&reason, &[], None);
    let accepted = st
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert_eq!(accepted.status, "False");
    assert_eq!(accepted.reason, "UnknownProfile");
    assert!(
        accepted.message.contains("h100"),
        "the message must list the defined profiles: {}",
        accepted.message
    );
    assert_eq!(
        st.endpoints.as_ref().unwrap().ready,
        Some(0),
        "ready must be CLEARED, not left stale"
    );
    assert!(st.models.is_empty());
}

/// A persistent rejection keeps its `lastTransitionTime`.
#[test]
fn a_persistent_rejection_does_not_rewrite_its_transition_time() {
    let reason = kinference_policy::rejection::RejectReason::MissingKvBlockSize;
    let first = rejected_worker_set_status(&reason, &[], None);
    let second = rejected_worker_set_status(&reason, &[], Some(&first));
    assert_eq!(
        first.conditions[0].last_transition_time,
        second.conditions[0].last_transition_time
    );
}

#[test]
fn pool_status_reports_count_models_and_accepted() {
    let r = resolved(vec![dest("workers-h100", &[])], vec![]);
    let s = desired_pool_status(
        &r,
        &facts(4, &[("gemma", 4, Some(32768), Some(131072))]),
        None,
    );
    assert_eq!(s.destination_count, Some(1));
    assert_eq!(s.models.len(), 1);
    assert_eq!(s.models[0].name, "gemma");
    assert_eq!(s.models[0].served_by, vec!["workers-h100"]);
    let cw = s.models[0].context_window.as_ref().unwrap();
    assert_eq!((cw.min, cw.max), (Some(32768), Some(131072)));
    // The handshake the gateway waits on.
    let accepted = s.parents[0]
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert_eq!(accepted.status, "True");
    assert_eq!(
        s.parents[0].controller_name.as_deref(),
        Some(CONTROLLER_NAME)
    );
    assert_eq!(accepted.observed_generation, Some(3));
}

#[test]
fn pool_status_reports_models_under_their_canonical_names() {
    // A client asks for `bar`; the engine reports the upstream model.
    let r = resolved(
        vec![dest(
            "workers-h100",
            &[("bar", "meta-llama/Llama-3.3-70B-Instruct")],
        )],
        vec![],
    );
    let s = desired_pool_status(
        &r,
        &facts(2, &[("meta-llama/Llama-3.3-70B-Instruct", 2, None, None)]),
        None,
    );
    assert_eq!(s.models[0].name, "bar");
}

/// Every alias gets its own pool model entry, with `servedBy` duplicated
/// across them.
#[test]
fn a_pool_lists_every_alias_as_its_own_askable_model() {
    let r = resolved(
        vec![dest(
            "qwen-2-5-32b",
            &[
                ("qwen-2.5", "Qwen/Qwen2.5-32B"),
                ("qwen-simple", "Qwen/Qwen2.5-32B"),
            ],
        )],
        vec![],
    );
    let s = desired_pool_status(&r, &facts(2, &[("Qwen/Qwen2.5-32B", 2, None, None)]), None);
    let names: Vec<&str> = s.models.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["qwen-2.5", "qwen-simple"]);
    for m in &s.models {
        assert_eq!(
            m.served_by,
            vec!["qwen-2-5-32b"],
            "same fleet under both names"
        );
    }
}

/// Unaliased models report under the upstream name.
#[test]
fn a_pool_falls_back_to_the_upstream_name_when_nothing_is_aliased() {
    let r = resolved(vec![dest("workers-h100", &[])], vec![]);
    let s = desired_pool_status(
        &r,
        &facts(1, &[("google/gemma-4-12B-it", 1, None, None)]),
        None,
    );
    assert_eq!(s.models[0].name, "google/gemma-4-12B-it");
}

#[test]
fn unresolved_destination_flips_resolved_refs_but_not_accepted() {
    // A pool with one good destination still routes.
    let r = resolved(
        vec![dest("good", &[])],
        vec![Unresolved {
            kind: "InferenceWorkerSet".into(),
            name: "gone".into(),
            uid: None,
            reason: UnresolvedReason::NotFound,
        }],
    );
    let s = desired_pool_status(&r, &facts(1, &[]), None);
    let c = &s.parents[0].conditions;
    assert_eq!(
        c.iter().find(|c| c.r#type == "Accepted").unwrap().status,
        "True"
    );
    let refs = c.iter().find(|c| c.r#type == "ResolvedRefs").unwrap();
    assert_eq!(refs.status, "False");
    assert_eq!(refs.reason, "UnresolvedDestinations");
    assert_eq!(
        s.unresolved_destinations[0].reason.as_deref(),
        Some("NotFound")
    );
}

#[test]
fn dangling_policy_ref_flips_resolved_refs() {
    let mut r = resolved(vec![dest("good", &[])], vec![]);
    r.dangling_policy = Some(DanglingPolicy::NotFound {
        name: "nope".into(),
    });
    let s = desired_pool_status(&r, &facts(1, &[]), None);
    let refs = s.parents[0]
        .conditions
        .iter()
        .find(|c| c.r#type == "ResolvedRefs")
        .unwrap();
    assert_eq!(refs.status, "False");
    assert_eq!(refs.reason, "PolicyNotFound");
    assert!(refs.message.contains("nope"));
}

#[test]
fn no_destinations_means_not_accepted() {
    let r = resolved(vec![], vec![]);
    let s = desired_pool_status(&r, &HashMap::new(), None);
    let accepted = s.parents[0]
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert_eq!(accepted.status, "False");
    assert_eq!(accepted.reason, "NoDestinations");
}

#[test]
fn last_transition_time_is_stable_while_the_condition_holds() {
    let r = resolved(vec![dest("good", &[])], vec![]);
    let first = desired_pool_status(&r, &facts(1, &[]), None);
    let t0 = first.parents[0].conditions[0].last_transition_time.clone();
    let second = desired_pool_status(&r, &facts(1, &[]), Some(&first));
    assert_eq!(second.parents[0].conditions[0].last_transition_time, t0);
}

#[test]
fn last_transition_time_moves_when_the_condition_flips() {
    // A fixed past timestamp, since `metav1.Time` has second granularity.
    const OLD: &str = "2020-01-01T00:00:00Z";
    let good = resolved(vec![dest("good", &[])], vec![]);
    let mut first = desired_pool_status(&good, &facts(1, &[]), None);
    for c in &mut first.parents[0].conditions {
        c.last_transition_time = OLD.to_string();
    }

    // Unchanged condition: the old timestamp is carried forward.
    let same = desired_pool_status(&good, &facts(1, &[]), Some(&first));
    assert_eq!(same.parents[0].conditions[0].last_transition_time, OLD);

    // Flipped condition: a fresh timestamp replaces it.
    let empty = resolved(vec![], vec![]);
    let flipped = desired_pool_status(&empty, &HashMap::new(), Some(&first));
    assert_eq!(flipped.parents[0].conditions[0].status, "False");
    assert_ne!(
        flipped.parents[0].conditions[0].last_transition_time, OLD,
        "a real transition must restamp the time"
    );
}

/// A `StatusWriter` over an empty recording.
fn writer_with_leader(leader: bool) -> (StatusWriter<Recorded>, watch::Sender<bool>) {
    writer_with_leader_and_registry(leader).0
}

/// As [`writer_with_leader`], plus the registry its metrics render through.
fn writer_with_leader_and_registry(
    leader: bool,
) -> (
    (StatusWriter<Recorded>, watch::Sender<bool>),
    prometheus::Registry,
) {
    writer_over(Recorded::default(), leader)
}

/// A leader-gated `StatusWriter` over `api`, reading an empty fleet.
fn writer_over(
    api: Recorded,
    leader: bool,
) -> (
    (StatusWriter<Recorded>, watch::Sender<bool>),
    prometheus::Registry,
) {
    let (tx, rx) = watch::channel(leader);
    let registry = prometheus::Registry::new();
    let handles = StatusHandles {
        store: kinference_policy::store::PolicyStore::new(),
        pools: kinference_fleet::pool::PoolRegistry::new(),
        rejections: crate::policy::worker_sets::RejectionStore::new(),
        metrics: Arc::new(MetricsRegistry::new()),
        provider_health: Arc::new(kinference_policy::provider_health::ProviderHealthStore::new()),
        leader: rx,
        prom: Arc::new(crate::metrics::ControllerMetrics::new(&registry)),
    };
    let w = StatusWriter::new(api, handles);
    ((w, tx), registry)
}

// --- the reconcile loop, over a recorded API server ---------------------

fn cr_meta(name: &str) -> kube::api::ObjectMeta {
    kube::api::ObjectMeta {
        name: Some(name.into()),
        namespace: Some("infra".into()),
        uid: Some(format!("uid-{name}")),
        ..Default::default()
    }
}

/// One of each kind the writer patches: a pool naming a worker set, a
/// provider and a policy.
fn recorded_cluster() -> (Recorded, Arc<kinference_policy::resolve::PolicyIndex>) {
    use kinference_policy::crd::{
        BackendObjectRef, Destination, InferencePoolSpec, InferenceProviderSpec,
        InferenceWorkerSetSpec, PolicyRef, ServiceRef,
    };
    let pool = InferencePool {
        metadata: cr_meta("chat"),
        spec: InferencePoolSpec {
            destinations: vec![
                Destination {
                    kind: Some("InferenceWorkerSet".into()),
                    name: Some("workers-h100".into()),
                    ..Default::default()
                },
                Destination {
                    kind: Some("InferenceProvider".into()),
                    name: Some("openai".into()),
                    ..Default::default()
                },
            ],
            policy_ref: Some(PolicyRef {
                group: None,
                kind: None,
                name: "chat-policy".into(),
            }),
        },
        status: None,
    };
    let set = InferenceWorkerSet {
        metadata: cr_meta("workers-h100"),
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
    let provider = InferenceProvider {
        metadata: cr_meta("openai"),
        spec: InferenceProviderSpec {
            backend_ref: Some(BackendObjectRef {
                name: Some("openai-backend".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        status: None,
    };
    let policy = InferencePolicy {
        metadata: cr_meta("chat-policy"),
        spec: Default::default(),
        status: None,
    };
    let index = kinference_policy::resolve::resolve(
        &[Arc::new(pool.clone())],
        &[Arc::new(policy.clone())],
        &[Arc::new(set.clone())],
        &[Arc::new(provider.clone())],
        &[],
        &[],
        &Default::default(),
        &Default::default(),
    );
    let api = Recorded::default()
        .with([pool])
        .with([set])
        .with([provider])
        .with([policy]);
    (api, Arc::new(index))
}

fn patched_keys(api: &Recorded) -> Vec<Key> {
    api.writes()
        .into_iter()
        .map(|w| match w {
            Write::PatchStatus { key, .. } => key,
            other => panic!("the status writer only patches status, got {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn a_listed_snapshot_patches_every_object_it_describes() {
    let (api, index) = recorded_cluster();
    let ((w, _tx), _) = writer_over(api.clone(), true);
    w.store.publish(index);

    w.reconcile_once().await;

    assert_eq!(
        patched_keys(&api),
        vec![
            key::<InferencePool>("infra", "chat"),
            key::<InferenceProvider>("infra", "openai"),
            key::<InferenceWorkerSet>("infra", "workers-h100"),
            key::<InferencePolicy>("infra", "chat-policy"),
        ]
    );
    let pool: InferencePool = api.object("infra", "chat").unwrap();
    assert!(
        pool.status.is_some(),
        "the recording holds what was patched"
    );
}

#[tokio::test]
async fn a_worker_set_clears_kv_events_when_rejected_and_restores_them_on_recovery() {
    let (api, _) = recorded_cluster();
    let ((writer, _tx), _) = writer_over(api.clone(), true);
    let worker_set: InferenceWorkerSet = api.object("infra", "workers-h100").unwrap();
    let mut destination = dest("workers-h100", &[]);
    destination.kv_events_declared = true;
    let stats = facts(2, &[("m", 2, None, None)]);
    let referenced_by = vec!["infra/chat".into()];
    let bound = desired_worker_set_status(&destination, &stats, &referenced_by, None, None);
    let rejected = rejected_worker_set_status(
        &kinference_policy::rejection::RejectReason::MissingKvBlockSize,
        &referenced_by,
        Some(&bound),
    );
    let recovered =
        desired_worker_set_status(&destination, &stats, &referenced_by, None, Some(&rejected));

    for (desired, has_kv_events) in [(&bound, true), (&rejected, false), (&recovered, true)] {
        writer
            .patch::<InferenceWorkerSet>(
                "InferenceWorkerSet",
                "infra",
                "workers-h100",
                worker_set.metadata.uid.as_deref(),
                desired,
            )
            .await;

        let patched: InferenceWorkerSet = api.object("infra", "workers-h100").unwrap();
        let status = patched.status.unwrap();
        assert_eq!(status.kv_events.is_some(), has_kv_events);
        assert_eq!(
            status.endpoints.unwrap().ready,
            desired.endpoints.unwrap().ready
        );
        assert_eq!(status.models.len(), desired.models.len());

        let writes = api.writes();
        writer
            .patch::<InferenceWorkerSet>(
                "InferenceWorkerSet",
                "infra",
                "workers-h100",
                worker_set.metadata.uid.as_deref(),
                desired,
            )
            .await;
        assert_eq!(api.writes(), writes);
    }
}

#[tokio::test]
async fn an_unchanged_snapshot_issues_no_second_patch() {
    let (api, index) = recorded_cluster();
    let ((w, _tx), _) = writer_over(api.clone(), true);
    w.store.publish(index);

    w.reconcile_once().await;
    let first = api.writes();
    w.reconcile_once().await;
    assert_eq!(
        api.writes(),
        first,
        "nothing changed, so nothing is written"
    );
}

#[tokio::test]
async fn a_failed_patch_is_retried_on_the_next_tick() {
    let (api, index) = recorded_cluster();
    let ((w, _tx), _) = writer_over(api.clone(), true);
    w.store.publish(index);
    api.failing::<InferencePool>("infra", "chat");

    w.reconcile_once().await;
    assert_eq!(
        patched_keys(&api).len(),
        3,
        "the failed patch is not written and not cached"
    );

    api.heal();
    w.reconcile_once().await;
    assert_eq!(
        patched_keys(&api).last(),
        Some(&key::<InferencePool>("infra", "chat")),
        "only the failed one is retried"
    );
    assert_eq!(patched_keys(&api).len(), 4);
}

fn render(registry: &prometheus::Registry) -> String {
    use prometheus::Encoder as _;
    let mut buf = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&registry.gather(), &mut buf)
        .unwrap();
    String::from_utf8(buf).unwrap()
}

/// A recreated object (new UID) misses the cache, even with identical status.
#[tokio::test]
async fn a_recreated_object_is_a_cache_miss_even_with_identical_status() {
    let (w, _tx) = writer_with_leader(true);
    let status = serde_json::json!({"destinationCount": 1});

    // What the writer wrote for the first incarnation.
    w.last.lock().unwrap().insert(
        (
            "InferencePool",
            "infra".into(),
            "chat".into(),
            Some("uid-1".into()),
        ),
        status.clone(),
    );
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", Some("uid-1"))
            .is_some(),
        "the original is still cached"
    );

    // Same namespace, name and status, but a different object.
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", Some("uid-2"))
            .is_none(),
        "a recreated object must MISS, or its patch is skipped and it keeps \
         the empty status it was created with"
    );

    // And an object whose UID we could not determine must not collide with
    // either incarnation.
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", None)
            .is_none()
    );
}

/// Promotion drops the last-written cache, which may be stale after a
/// failover.
#[tokio::test]
async fn promotion_drops_the_last_written_cache_so_status_is_re_asserted() {
    let (w, _tx) = writer_with_leader(false);

    // Stand in for "we wrote this while we were previously leader".
    w.last.lock().unwrap().insert(
        ("InferencePool", "infra".into(), "chat".into(), None),
        serde_json::json!({"destinationCount": 1}),
    );
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", None)
            .is_some(),
        "cache should be warm before promotion"
    );

    w.on_promotion();

    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", None)
            .is_none(),
        "after promotion the cache must be empty, so the next reconcile \
         re-asserts rather than trusting a baseline a different leader has \
         since overwritten"
    );
}

/// Demotion withdraws `kinference_policy_warnings`.
///
/// Driven through `run`, so it also checks the loop calls `on_demotion`. The
/// store is never listed, so no reconcile clears the gauge instead.
#[tokio::test]
async fn demotion_withdraws_the_policy_warning_counts() {
    let ((w, tx), registry) = writer_with_leader_and_registry(true);
    let prom = w.prom.clone();
    // Stand in for "we published these while we were leader".
    prom.set_policy_warnings(&[("infra/routing".into(), "UnknownBackend", 2)]);
    assert!(render(&registry).contains("kinference_policy_warnings{"));

    let cancel = CancellationToken::new();
    let task = tokio::spawn(w.run(cancel.clone()));
    tx.send(false).expect("the writer holds the receiver");

    let cleared = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while render(&registry).contains("kinference_policy_warnings") {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    cancel.cancel();
    task.await.expect("the writer exits on cancel");
    assert!(
        cleared.is_ok(),
        "a demoted replica must stop exporting policy warning counts"
    );
}

/// Emptying a status list must be expressible on the wire.
///
/// Status is patched with `Patch::Merge`, where an omitted key is left
/// alone, so an empty list must serialize as `[]` rather than be skipped.
#[test]
fn emptied_status_lists_serialize_so_a_merge_patch_can_clear_them() {
    // A pool that was unresolved and is now fully resolved.
    let r = resolved(vec![dest("workers-h100", &[])], vec![]);
    let s = desired_pool_status(&r, &facts(1, &[]), None);
    assert!(
        s.unresolved_destinations.is_empty(),
        "fixture should have nothing unresolved"
    );
    let json = serde_json::to_value(&s).unwrap();
    for key in ["unresolvedDestinations", "models"] {
        assert_eq!(
            json.get(key),
            Some(&serde_json::json!([])),
            "`{key}` must appear as [] so the merge patch clears it; \
             omitting it leaves the previous value in place"
        );
    }

    // Same contract on the other two status types.
    let ws = desired_worker_set_status(&dest("workers-h100", &[]), &facts(0, &[]), &[], None, None);
    let wj = serde_json::to_value(&ws).unwrap();
    assert_eq!(wj.get("models"), Some(&serde_json::json!([])));
    // A set just removed from every pool must be able to clear
    // `referencedBy`.
    assert_eq!(
        wj.get("referencedBy"),
        Some(&serde_json::json!([])),
        "`referencedBy` must appear as [] so dropping the last pool clears it"
    );

    let pol = kinference_policy::compile::CompiledPolicy::serve_as_asked("p");
    let pj = serde_json::to_value(desired_policy_status(&rp(pol), None)).unwrap();
    assert_eq!(
        pj.get("warnings"),
        Some(&serde_json::json!([])),
        "`warnings` must be clearable — one outliving the mistake it describes \
         misleads whoever just fixed it"
    );
}

#[test]
fn identical_inputs_produce_identical_status_so_no_patch_is_issued() {
    // This equality is what stops the write/watch/rebuild loop.
    let r = resolved(vec![dest("good", &[])], vec![]);
    let a = desired_pool_status(&r, &facts(2, &[("m", 2, Some(8), Some(8))]), None);
    let b = desired_pool_status(&r, &facts(2, &[("m", 2, Some(8), Some(8))]), Some(&a));
    assert_eq!(
        serde_json::to_value(&a).unwrap(),
        serde_json::to_value(&b).unwrap()
    );
}

/// Entries are keyed on the upstream model, so all of its aliases are
/// reported.
#[test]
fn every_alias_for_a_model_is_reported() {
    let d = dest(
        "workers-h100",
        &[
            ("qwen-2.5", "Qwen/Qwen2.5-32B"),
            ("qwen-simple", "Qwen/Qwen2.5-32B"),
        ],
    );
    let s = desired_worker_set_status(
        &d,
        &facts(2, &[("Qwen/Qwen2.5-32B", 2, None, None)]),
        &[],
        None,
        None,
    );
    assert_eq!(s.models.len(), 1, "one entry per OBSERVED model");
    assert_eq!(s.models[0].upstream_name, "Qwen/Qwen2.5-32B");
    assert_eq!(
        s.models[0].aliases,
        ["qwen-2.5", "qwen-simple"],
        "sorted, both present"
    );
}

/// A model with no aliases reports just its upstream name; `aliases` is
/// skipped when empty.
#[test]
fn an_undeclared_model_reports_no_aliases() {
    let d = dest("workers-h100", &[]);
    let s = desired_worker_set_status(
        &d,
        &facts(1, &[("google/gemma-4-12B-it", 1, None, None)]),
        &[],
        None,
        None,
    );
    assert_eq!(s.models[0].upstream_name, "google/gemma-4-12B-it");
    assert!(s.models[0].aliases.is_empty());
    let json = serde_json::to_value(&s.models[0]).unwrap();
    assert!(
        json.get("aliases").is_none(),
        "empty aliases must not serialize"
    );
}

/// Several models on one fleet (LoRA adapters, or several
/// `--served-model-name` values) get an entry each, and `readyEndpoints`
/// shows a partial rollout.
#[test]
fn each_observed_model_gets_its_own_entry_with_its_own_endpoint_count() {
    let d = dest("workers-h100", &[("base", "Qwen/Qwen2.5-32B")]);
    let s = desired_worker_set_status(
        &d,
        &facts(
            2,
            &[
                ("Qwen/Qwen2.5-32B", 2, Some(32768), Some(32768)),
                ("my-adapter", 1, Some(32768), Some(32768)),
            ],
        ),
        &[],
        None,
        None,
    );
    assert_eq!(s.models.len(), 2);
    let adapter = s
        .models
        .iter()
        .find(|m| m.upstream_name == "my-adapter")
        .unwrap();
    assert_eq!(adapter.ready_endpoints, Some(1), "loaded on one pod of two");
    assert!(adapter.aliases.is_empty());
    let base = s
        .models
        .iter()
        .find(|m| m.upstream_name == "Qwen/Qwen2.5-32B")
        .unwrap();
    assert_eq!(base.ready_endpoints, Some(2));
    assert_eq!(base.aliases, ["base"]);
}

#[test]
fn worker_set_status_counts_ready_endpoints_and_models() {
    let d = dest("workers-h100", &[("bar", "upstream/bar-fp8")]);
    let s = desired_worker_set_status(
        &d,
        &facts(3, &[("upstream/bar-fp8", 3, Some(32768), Some(32768))]),
        &[],
        None,
        None,
    );
    assert_eq!(s.endpoints.unwrap().ready, Some(3));
    // Keyed on the observed model, with declared aliases alongside.
    assert_eq!(s.models[0].upstream_name, "upstream/bar-fp8");
    assert_eq!(s.models[0].aliases, ["bar"]);
    assert_eq!(s.models[0].ready_endpoints, Some(3));
    assert_eq!(
        s.conditions
            .iter()
            .find(|c| c.r#type == "Ready")
            .unwrap()
            .status,
        "True"
    );
}

/// `status.kvEvents` exposes a worker set that is ready and serving while its
/// prefix model is empty, which readiness can't detect since ZMQ connects
/// whether or not a publisher exists.
#[test]
fn kv_events_status_separates_declared_from_actually_flowing() {
    // Declared, and events arriving from all three: the healthy case.
    let mut d = dest("workers-h100", &[]);
    d.kv_events_declared = true;
    let mut f = facts(3, &[("m", 3, None, None)]);
    let s = desired_worker_set_status(&d, &f, &[], None, None);
    let kv = s.kv_events.unwrap();
    assert_eq!(kv.configured, Some(true));
    assert_eq!(kv.subscribed_endpoints, Some(3));

    // Declared and ready, but no endpoint has delivered an event: likely a
    // misconfigured publisher port.
    f.get_mut(&PoolId::from_raw(0)).unwrap().event_endpoints = 0;
    let s = desired_worker_set_status(&d, &f, &[], None, None);
    assert_eq!(s.kv_events.unwrap().subscribed_endpoints, Some(0));
    assert_eq!(
        s.conditions
            .iter()
            .find(|c| c.r#type == "Ready")
            .unwrap()
            .status,
        "True",
        "no prefix model degrades ranking; it does not take the set out of service"
    );

    // Not declared: still reported, as `configured: false`.
    let plain = dest("workers-a100", &[]);
    let s = desired_worker_set_status(&plain, &facts(1, &[]), &[], None, None);
    assert_eq!(s.kv_events.unwrap().configured, Some(false));
}

#[test]
fn worker_set_with_no_ready_endpoints_is_not_ready_but_still_accepted() {
    let d = dest("workers-h100", &[]);
    let s = desired_worker_set_status(&d, &facts(0, &[]), &[], None, None);
    assert_eq!(
        s.conditions
            .iter()
            .find(|c| c.r#type == "Accepted")
            .unwrap()
            .status,
        "True",
        "it bound to a pool; it just has no ready pods"
    );
    let ready = s.conditions.iter().find(|c| c.r#type == "Ready").unwrap();
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "NoReadyEndpoints");
}

#[test]
fn policy_status_composes_state_tokens_and_reports_an_inert_rule() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [
            {"name": "big", "matches": [{"inputTokens": "big"}],
             "route": {"select": [{"minimize": "PredictedTTFT"}]}},
            {"name": "plain", "route": {"select": [{"minimize": "PredictedTTFT"}]}}
        ]
    }))
    .unwrap();
    let p = CompiledPolicy::compile("chat-policy", &spec);
    let s = desired_policy_status(&rp(p), None);
    // In force and carrying warnings. Warnings don't affect `Accepted`.
    assert_eq!(s.state.as_deref(), Some("InForce,HasWarnings"));
    let inert = s
        .warnings
        .iter()
        .find(|w| w.contains("never matches"))
        .unwrap_or_else(|| panic!("inert-rule warning missing: {:?}", s.warnings));
    assert!(
        inert.contains("inputTokens"),
        "the warning must name the offending term: {inert}"
    );
    // Warnings never make a policy unusable.
    assert_eq!(s.conditions[0].status, "True");
    assert_eq!(s.conditions[0].observed_generation, Some(7));
}

#[test]
fn clean_policy_has_no_warnings() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }))
    .unwrap();
    let s = desired_policy_status(&rp(CompiledPolicy::compile("p", &spec)), None);
    assert!(s.warnings.is_empty());
    assert!(s.conditions[0].message.contains("1 rule(s) compiled"));
}

#[test]
fn two_destinations_serving_one_model_both_appear_in_served_by() {
    let mut d2 = dest("workers-a100", &[]);
    d2.pool_id = PoolId::from_raw(1);
    let r = resolved(vec![dest("workers-h100", &[]), d2], vec![]);
    let mut f = facts(2, &[("gemma", 2, Some(8192), Some(8192))]);
    f.insert(
        PoolId::from_raw(1),
        PoolFacts {
            ready_endpoints: 1,
            event_endpoints: 1,
            models: vec![ModelFact {
                upstream: "gemma".into(),
                ready_endpoints: 1,
                ctx_min: Some(4096),
                ctx_max: Some(4096),
            }],
        },
    );
    let s = desired_pool_status(&r, &f, None);
    assert_eq!(s.models.len(), 1);
    assert_eq!(s.models[0].served_by, vec!["workers-h100", "workers-a100"]);
    // The range spans both destinations.
    let cw = s.models[0].context_window.as_ref().unwrap();
    assert_eq!((cw.min, cw.max), (Some(4096), Some(8192)));
}

// --- referenced vs not --------------------------------------------------

fn one_rule() -> CompiledPolicy {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{
            "name": "r",
            "route": {"select": [{"type": "Local", "minimize": "PredictedTTFT"}]}
        }]
    }))
    .expect("valid fixture");
    CompiledPolicy::compile("p", &spec)
}

/// An unattached policy still gets status, with `Accepted=False`.
#[test]
fn an_unreferenced_policy_reports_why_rather_than_nothing() {
    let s = desired_policy_status(&rp_unreferenced(one_rule()), None);

    // `state` and warnings are published before the policy is attached. A
    // clean draft carries exactly one token.
    assert_eq!(s.state.as_deref(), Some("NotReferenced"));
    assert!(s.warnings.is_empty(), "so no HasWarnings token");

    let c = &s.conditions[0];
    assert_eq!(c.r#type, "Accepted");
    assert_eq!(c.status, "False");
    assert_eq!(c.reason, "NotReferenced");
    assert!(
        c.message.contains("no InferencePool names it"),
        "the message must say what is wrong: {}",
        c.message
    );
    // And how to attach it.
    assert!(c.message.contains("policyRef"), "{}", c.message);
}

#[test]
fn a_referenced_policy_names_the_pools_it_serves() {
    let s = desired_policy_status(&rp(one_rule()), None);
    let c = &s.conditions[0];
    assert_eq!(c.status, "True");
    assert_eq!(c.reason, "Accepted");
    assert!(
        c.message.contains("in force on pool(s) chat"),
        "{}",
        c.message
    );
}

/// Detaching flips the condition rather than leaving `Accepted=True`.
#[test]
fn detaching_flips_accepted_rather_than_freezing_it() {
    let mut attached = desired_policy_status(&rp(one_rule()), None);
    assert_eq!(attached.conditions[0].status, "True");

    // Backdated, since `lastTransitionTime` has second resolution.
    attached.conditions[0].last_transition_time = "2020-01-01T00:00:00Z".to_string();

    let detached = desired_policy_status(&rp_unreferenced(one_rule()), Some(&attached));
    assert_eq!(detached.conditions[0].status, "False");
    assert_eq!(detached.conditions[0].reason, "NotReferenced");
    // A real state change, so the timestamp is not carried forward.
    assert_ne!(
        detached.conditions[0].last_transition_time,
        "2020-01-01T00:00:00Z"
    );

    // An unchanged condition keeps its timestamp.
    let still_attached = desired_policy_status(&rp(one_rule()), Some(&attached));
    assert_eq!(
        still_attached.conditions[0].last_transition_time,
        "2020-01-01T00:00:00Z"
    );
}

/// Policy conditions carry `observedGeneration`.
#[test]
fn the_policy_status_reports_the_generation_it_describes() {
    let s = desired_policy_status(&rp(one_rule()), None);
    assert_eq!(s.conditions[0].observed_generation, Some(7));
}

/// Unchanged state produces a byte-identical status.
#[test]
fn an_unchanged_policy_produces_an_identical_status() {
    let first = desired_policy_status(&rp_unreferenced(one_rule()), None);
    let again = desired_policy_status(&rp_unreferenced(one_rule()), Some(&first));
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&again).unwrap()
    );
}

/// A refused provider reports the refusal on its own object.
#[test]
fn a_refused_provider_reports_the_reason_and_the_fix() {
    let s = rejected_provider_status(
        kinference_policy::resolve::UnresolvedReason::UnsupportedApiFormat,
        None,
    );
    let accepted = &s.conditions[0];
    assert_eq!(accepted.r#type, "Accepted");
    assert_eq!(accepted.status, "False");
    assert_eq!(accepted.reason, "UnsupportedApiFormat");
    // The message says how to fix it.
    assert!(
        accepted.message.contains("Only OpenAI"),
        "{}",
        accepted.message
    );
    assert!(
        accepted.message.contains("OpenAI-compatible endpoint"),
        "{}",
        accepted.message
    );

    assert_eq!(s.conditions[1].r#type, "Ready");
    assert_eq!(s.conditions[1].status, "False");

    // A refused provider's status carries nothing beyond its verdict. Checked
    // by key set, so any new field leaking through fails here.
    let keys: Vec<String> = serde_json::to_value(&s)
        .unwrap()
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, ["conditions", "state"], "unexpected key on a refusal");
}

#[test]
fn every_refusal_reason_gets_its_own_message() {
    use kinference_policy::resolve::UnresolvedReason as R;
    let msgs: Vec<String> = [
        R::UnsupportedApiFormat,
        R::Incomplete,
        R::CrossNamespaceNotGranted,
        R::AmbiguousName,
    ]
    .into_iter()
    .map(|r| {
        rejected_provider_status(r, None).conditions[0]
            .message
            .clone()
    })
    .collect();
    // Distinct guidance per reason.
    let unique: std::collections::BTreeSet<&String> = msgs.iter().collect();
    assert_eq!(unique.len(), msgs.len(), "{msgs:?}");
}

// --- policy warnings in status ------------------------------------------------

use kinference_policy::warning::{PolicyWarning, WarningCode};

fn warning(i: usize) -> PolicyWarning {
    PolicyWarning::new(
        WarningCode::EmptyDrop,
        format!("spec.rules[{i}].route.exclude[0].drop"),
        "removes nothing",
    )
}

/// The CRD caps `status.warnings` at 32 entries of 2048 characters, and going
/// over rejects the whole patch. A long list is cut to 31 plus a pointer to the
/// rest; an over-long entry is clipped.
#[test]
fn status_warnings_fit_the_crd_limits() {
    let mut compiled = CompiledPolicy::serve_as_asked("p");
    compiled.warnings = (0..40).map(warning).collect();
    compiled.warnings.push(PolicyWarning::new(
        WarningCode::EmptyDrop,
        "spec",
        "ü".repeat(5000),
    ));
    let s = desired_policy_status(&rp(compiled), None);
    assert_eq!(s.warnings.len(), 32);
    assert!(
        s.warnings[31].contains("and 10 more warning(s)"),
        "{}",
        s.warnings[31]
    );
    assert!(
        s.warnings[31].contains("/api/policy/context"),
        "{}",
        s.warnings[31]
    );
    assert!(s.warnings.iter().all(|w| w.chars().count() <= 2048));
    // The full count still reaches the condition, so nothing is understated.
    let accepted = s
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert!(
        accepted.message.contains("41 warning(s)"),
        "{}",
        accepted.message
    );

    let mut long = CompiledPolicy::serve_as_asked("p");
    long.warnings = vec![PolicyWarning::new(
        WarningCode::EmptyDrop,
        "spec",
        "ü".repeat(5000),
    )];
    let s = desired_policy_status(&rp(long), None);
    assert_eq!(
        s.warnings[0].chars().count(),
        2048,
        "clipped on a char boundary"
    );
    assert!(s.warnings[0].ends_with('…'));
}

/// Status renders each warning as `Code: path: message`, so `kubectl get -o
/// yaml` shows the same code an agent matches on.
#[test]
fn status_warnings_carry_code_and_path() {
    let mut compiled = CompiledPolicy::serve_as_asked("p");
    compiled.warnings = vec![warning(0)];
    let s = desired_policy_status(&rp(compiled), None);
    assert_eq!(
        s.warnings,
        ["EmptyDrop: spec.rules[0].route.exclude[0].drop: removes nothing"]
    );
}

/// A name warning alone is enough for `HasWarnings`.
#[test]
fn reference_warnings_alone_raise_has_warnings() {
    let mut r = rp(CompiledPolicy::serve_as_asked("p"));
    r.reference_warnings = vec![PolicyWarning::new(
        WarningCode::UnknownBackend,
        "spec.rules[0].route.select[0].order[0]",
        "`databriks` is not a destination",
    )];
    let s = desired_policy_status(&r, None);
    assert_eq!(s.state.as_deref(), Some("InForce,HasWarnings"));
    assert_eq!(s.warnings.len(), 1);
    assert!(s.warnings[0].starts_with("UnknownBackend: "));
}

/// A clean shutdown does not count as a leader election error.
#[tokio::test]
async fn shutdown_does_not_count_a_leader_election_error() {
    // The race this guards against is random, so repeat it.
    for _ in 0..32 {
        let ((w, tx), registry) = writer_with_leader_and_registry(false);
        let cancel = CancellationToken::new();
        // The elector's order on shutdown: cancel fires, then a last send and drop.
        cancel.cancel();
        tx.send(false).expect("the writer holds the receiver");
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), w.run(cancel))
            .await
            .expect("the writer exits on cancel");
        assert!(
            render(&registry).contains("kinference_leader_election_errors_total 0"),
            "shutdown must not bump the leader election error counter"
        );
    }
}
