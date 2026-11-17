//! Resolution is driven directly against hand-seeded reflector stores.
use super::*;
use crate::crd::{
    BackendObjectRef, Destination, HTTPRouteRule, InferencePolicySpec, InferencePoolSpec,
    InferenceProviderSpec, InferenceWorkerSetSpec, LabelSelector, PolicyRef, ProviderModel,
    ReferenceGrantFrom, ReferenceGrantSpec, ReferenceGrantTo, ServiceRef,
};
use kube::core::ObjectMeta;

/// The Nth destination as a worker set; panics if it is a provider.
fn local(p: &ResolvedPool, i: usize) -> &ResolvedWorkerSet {
    p.destinations[i]
        .as_local()
        .expect("test expects a local destination")
}

/// `resolve` with no providers and no grants.
fn resolve_local(
    pools: &[Arc<InferencePool>],
    policies: &[Arc<InferencePolicy>],
    worker_sets: &[Arc<InferenceWorkerSet>],
    routes: &[Arc<HTTPRoute>],
    pools_by_key: &PoolIndex,
    rejections: &RejectionIndex,
) -> PolicyIndex {
    resolve(
        pools,
        policies,
        worker_sets,
        &[],
        routes,
        &[],
        pools_by_key,
        rejections,
    )
}

fn meta(name: &str, ns: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(ns.into()),
        resource_version: Some("100".into()),
        ..Default::default()
    }
}

fn worker_set(name: &str, service: &str, labels: &[(&str, &str)]) -> Arc<InferenceWorkerSet> {
    let mut m = meta(name, "infra");
    if !labels.is_empty() {
        m.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
    }
    Arc::new(InferenceWorkerSet {
        metadata: m,
        spec: InferenceWorkerSetSpec {
            service_ref: ServiceRef {
                name: service.into(),
                port: None,
            },
            // Required by the CRD, so set by default.
            profile: Some("h100".into()),
            kv_block_size: Some(16),
            ..Default::default()
        },
        status: None,
    })
}

fn pool(name: &str, dests: Vec<Destination>, policy: Option<&str>) -> Arc<InferencePool> {
    Arc::new(InferencePool {
        metadata: meta(name, "infra"),
        spec: InferencePoolSpec {
            destinations: dests,
            policy_ref: policy.map(|p| PolicyRef {
                group: None,
                kind: None,
                name: p.into(),
            }),
        },
        status: None,
    })
}

fn provider(
    name: &str,
    ns: &str,
    backend: &str,
    backend_ns: Option<&str>,
    models: Vec<(&str, &str, Option<u32>)>,
) -> Arc<InferenceProvider> {
    Arc::new(InferenceProvider {
        metadata: meta(name, ns),
        spec: InferenceProviderSpec {
            backend_ref: Some(BackendObjectRef {
                group: None,
                kind: None,
                name: Some(backend.into()),
                namespace: backend_ns.map(str::to_string),
            }),
            api: Some(ApiFormat::OpenAI),
            // Set per-test.
            path: None,
            models: models
                .into_iter()
                // (canonical, upstream, ctx); the canonical becomes an alias
                // of the upstream.
                .map(|(n, up, ctx)| ProviderModel {
                    name: Some(up.into()),
                    aliases: if n == up { vec![] } else { vec![n.into()] },
                    context_window: ctx,
                })
                .collect(),
        },
        status: None,
    })
}

fn dest_provider(name: &str, ns: Option<&str>) -> Destination {
    Destination {
        kind: Some("InferenceProvider".into()),
        name: Some(name.into()),
        namespace: ns.map(str::to_string),
        ..Default::default()
    }
}

/// A grant in `to_ns` letting `from_ns`'s InferenceProviders reach
/// GatewayBackends: what a cross-namespace `backendRef` needs.
fn grant(to_ns: &str, from_ns: &str, name: Option<&str>) -> Arc<ReferenceGrant> {
    grant_from(
        "InferenceProvider",
        from_ns,
        to_ns,
        (GATEWAY_BACKEND_GROUP, GATEWAY_BACKEND_KIND),
        name,
    )
}

/// A grant in `to_ns` letting `from_kind` objects in `from_ns` reach the
/// `to` group and kind there, by `name` or all of them.
fn grant_from(
    from_kind: &str,
    from_ns: &str,
    to_ns: &str,
    to: (&str, &str),
    name: Option<&str>,
) -> Arc<ReferenceGrant> {
    Arc::new(ReferenceGrant {
        metadata: meta("g", to_ns),
        spec: ReferenceGrantSpec {
            from: vec![ReferenceGrantFrom {
                group: Some("kinference.buoyant.io".into()),
                kind: Some(from_kind.into()),
                namespace: Some(from_ns.into()),
            }],
            to: vec![ReferenceGrantTo {
                group: Some(to.0.into()),
                kind: Some(to.1.into()),
                name: name.map(str::to_string),
            }],
        },
    })
}

/// A one-rule InferencePolicy. `type: Local` on the tier because the CRD
/// requires it for `PredictedTTFT`.
fn policy(name: &str) -> Arc<InferencePolicy> {
    Arc::new(InferencePolicy {
        metadata: meta(name, "infra"),
        spec: serde_json::from_value::<InferencePolicySpec>(serde_json::json!({
            "rules": [{
                "name": "r",
                "route": {"select": [{"type": "Local", "minimize": "PredictedTTFT"}]}
            }]
        }))
        .unwrap(),
        status: None,
    })
}

fn dest_named(name: &str) -> Destination {
    Destination {
        kind: Some("InferenceWorkerSet".into()),
        name: Some(name.into()),
        ..Default::default()
    }
}

/// The identity index the resolver consults, (namespace, name) to pool, with
/// every name these tests use. Tests that want a miss build their own.
fn pools() -> PoolIndex {
    [
        "a",
        "b",
        "private",
        "shared-1",
        "workers-h100",
        "workers-a100",
    ]
    .into_iter()
    .enumerate()
    .map(|(i, name)| {
        (
            crate::PoolKey::namespaced("infra", name),
            PoolId::from_raw(i as u64),
        )
    })
    .collect()
}

/// Only a snapshot built from listed stores is marked listed, so a `lookup`
/// miss can be told apart from "not listed yet".
#[test]
fn the_default_snapshot_is_not_listed_but_a_resolved_one_is() {
    assert!(
        !PolicyIndex::default().listed(),
        "before the first list, a miss must not be read as 'no such pool'"
    );
    let idx = resolve_local(&[], &[], &[], &[], &pools(), &RejectionIndex::new());
    assert!(
        idx.listed(),
        "anything resolve_local() builds came from listed stores"
    );
    assert!(idx.is_empty(), "listed and empty are independent facts");
}

#[test]
fn worker_set_binds_to_the_pool_with_the_matching_service() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(local(p, 0).service_name, "vllm-h100");
    assert!(p.unresolved.is_empty());
}

/// A worker set the router has not materialized into a pool is reported as
/// `NoMatchingPool` rather than dropped.
#[test]
fn a_worker_set_with_no_pool_yet_is_unresolved() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-a100")], None)],
        &[],
        &[worker_set("workers-a100", "vllm-a100", &[])],
        &[],
        // The set is valid; the router just has not created its pool.
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert!(p.destinations.is_empty());
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::NoMatchingPool);
    assert_eq!(p.unresolved[0].name, "workers-a100");
}

#[test]
fn missing_destination_is_not_found() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("gone")], None)],
        &[],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::NotFound);
}

#[test]
fn empty_destinations_means_every_worker_set_in_the_namespace() {
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[
            worker_set("a", "vllm-h100", &[]),
            worker_set("b", "vllm-h100", &[]),
        ],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    assert_eq!(idx.lookup("infra", "chat").unwrap().destinations.len(), 2);
}

/// The snapshot doesn't depend on the order of the inputs.
#[test]
fn input_order_does_not_change_the_snapshot() {
    let route = |name: &str, gateway: &str| {
        Arc::new(HTTPRoute {
            metadata: meta(name, "infra"),
            spec: serde_json::from_value(serde_json::json!({
                "parentRefs": [{"kind": "Gateway", "name": gateway}],
                "rules": [{"backendRefs": [
                    {"group": "kinference.buoyant.io", "kind": "InferencePool", "name": "chat"}]}]
            }))
            .unwrap(),
        })
    };
    let pools_in = [pool("chat", vec![], None)];
    let mut sets = vec![
        worker_set("a", "vllm-h100", &[]),
        worker_set("b", "vllm-h100", &[]),
        worker_set("unbound", "nothing-serves-this", &[]),
        worker_set("unbound-too", "nothing-serves-this", &[]),
    ];
    let mut routes = vec![route("r1", "gw-1"), route("r2", "gw-2")];
    let snapshot = |sets: &[Arc<InferenceWorkerSet>], routes: &[Arc<HTTPRoute>]| {
        let idx = resolve_local(
            &pools_in,
            &[],
            sets,
            routes,
            &pools(),
            &RejectionIndex::new(),
        );
        let p = idx.lookup("infra", "chat").unwrap();
        (
            p.destinations
                .iter()
                .map(|d| d.name().to_string())
                .collect::<Vec<_>>(),
            p.unresolved
                .iter()
                .map(|u| u.name.clone())
                .collect::<Vec<_>>(),
            p.parents.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
        )
    };
    let forward = snapshot(&sets, &routes);
    sets.reverse();
    routes.reverse();
    assert_eq!(snapshot(&sets, &routes), forward);
    assert_eq!(forward.0, ["a", "b"]);
    assert_eq!(forward.1, ["unbound", "unbound-too"]);
    assert_eq!(forward.2, ["gw-1", "gw-2"]);
}

#[test]
fn selector_destination_matches_by_label() {
    let dest = Destination {
        kind: Some("InferenceWorkerSet".into()),
        selector: Some(LabelSelector {
            match_labels: [("tier".to_string(), "shared".to_string())]
                .into_iter()
                .collect(),
        }),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest], None)],
        &[],
        &[
            worker_set("shared-1", "vllm-h100", &[("tier", "shared")]),
            worker_set("private", "vllm-h100", &[("tier", "private")]),
        ],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.destinations.len(), 1);
    assert_eq!(local(p, 0).name, "shared-1");
}

#[test]
fn a_set_matched_twice_is_only_added_once() {
    // A name and a selector can both cover the same worker set.
    let by_label = Destination {
        kind: Some("InferenceWorkerSet".into()),
        selector: Some(LabelSelector {
            match_labels: [("tier".to_string(), "shared".to_string())]
                .into_iter()
                .collect(),
        }),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("shared-1"), by_label], None)],
        &[],
        &[worker_set("shared-1", "vllm-h100", &[("tier", "shared")])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    assert_eq!(
        idx.lookup("infra", "chat").unwrap().destinations.len(),
        1,
        "destinationCount must not double-count"
    );
}

/// An `InferenceWorkerSet` no `InferencePool` names still appears in the
/// index, so it gets status.
#[test]
fn a_worker_set_no_pool_names_is_still_published() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[
            worker_set("workers-h100", "vllm-h100", &[]),
            worker_set("workers-a100", "vllm-a100", &[]),
        ],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let sets: Vec<_> = idx.worker_sets().collect();
    assert_eq!(
        sets.len(),
        2,
        "both sets published, not just the referenced one"
    );

    let named = sets.iter().find(|e| e.name == "workers-h100").unwrap();
    assert_eq!(named.referenced_by, vec!["chat".to_string()]);
    assert!(named.bound.is_ok());

    let orphan = sets.iter().find(|e| e.name == "workers-a100").unwrap();
    assert!(
        orphan.referenced_by.is_empty(),
        "no pool names it, so nothing to report"
    );
    assert!(
        orphan.bound.is_ok(),
        "it bound to a live pool — being unreferenced is not being broken"
    );
}

/// Two pools routing to one set are both named.
#[test]
fn a_worker_set_reached_by_two_pools_lists_both_sorted() {
    let idx = resolve_local(
        &[
            pool("zeta", vec![dest_named("workers-h100")], None),
            pool("alpha", vec![dest_named("workers-h100")], None),
        ],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(
        e.referenced_by,
        vec!["alpha".to_string(), "zeta".to_string()]
    );
}

/// A set reached through an empty `destinations` ("every set in this
/// namespace") counts as referenced.
#[test]
fn an_implicitly_reached_worker_set_counts_as_referenced() {
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(e.referenced_by, vec!["chat".to_string()]);
}

/// A set kinference has no pool for is published too, carrying the reason.
#[test]
fn a_worker_set_with_no_pool_is_published_with_its_reason() {
    let idx = resolve_local(
        &[],
        &[],
        &[worker_set("no-such-pool", "vllm-missing", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(
        e.bound.as_ref().err(),
        Some(&UnresolvedReason::NoMatchingPool)
    );
}

/// A `policyRef` naming another kind, like `{kind: ConfigMap, name: x}`,
/// doesn't resolve to the InferencePolicy named `x`.
#[test]
fn a_policy_ref_naming_the_wrong_kind_dangles() {
    let mut p = pool("chat", vec![dest_named("workers-h100")], Some("real"));
    Arc::get_mut(&mut p).unwrap().spec.policy_ref = Some(PolicyRef {
        group: None,
        kind: Some("ConfigMap".into()),
        name: "real".into(),
    });
    let idx = resolve_local(
        std::slice::from_ref(&p),
        &[policy("real")],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let r = idx.lookup("infra", "chat").unwrap();
    assert_eq!(
        r.dangling_policy,
        Some(DanglingPolicy::WrongKind {
            name: "real".into(),
            group: "kinference.buoyant.io".into(),
            kind: "ConfigMap".into(),
        }),
        "the policy EXISTS; the ref is what is wrong"
    );
    assert!(r.policy.rules.is_empty(), "fails open, serving as asked");
    // And the policy it wrongly named must not count as referenced.
    let pol = idx.policies().next().unwrap();
    assert!(pol.referenced_by.is_empty());
}

/// The default group/kind are the right ones, so omitting them still resolves.
#[test]
fn an_omitted_group_and_kind_still_resolve() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], Some("real"))],
        &[policy("real")],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let r = idx.lookup("infra", "chat").unwrap();
    assert_eq!(r.dangling_policy, None);
}

#[test]
fn pool_without_policy_ref_serves_as_asked() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert!(p.policy.rules.is_empty());
    assert!(p.dangling_policy.is_none());
}

#[test]
fn dangling_policy_ref_fails_open_and_is_reported() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], Some("nope"))],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.dangling_policy.as_ref().map(|d| d.name()), Some("nope"));
    // Fail open: still routable, just unfiltered.
    assert_eq!(p.destinations.len(), 1);
    assert!(p.policy.rules.is_empty());
}

#[test]
fn policy_ref_resolves_and_compiles() {
    let policy = Arc::new(InferencePolicy {
        metadata: meta("chat-policy", "infra"),
        spec: serde_json::from_value::<InferencePolicySpec>(serde_json::json!({
            "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
        }))
        .unwrap(),
        status: None,
    });
    let idx = resolve_local(
        &[pool(
            "chat",
            vec![dest_named("workers-h100")],
            Some("chat-policy"),
        )],
        &[policy],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert!(p.dangling_policy.is_none());
    assert_eq!(p.policy.rules.len(), 1);
    assert_eq!(p.policy.rules[0].name, "r");
}

#[test]
fn parents_come_from_httproutes_naming_the_pool() {
    let route = Arc::new(HTTPRoute {
        metadata: meta("llm", "infra"),
        spec: serde_json::from_value(serde_json::json!({
            "parentRefs": [{"kind": "Gateway", "name": "inference-gateway"}],
            "rules": [{"backendRefs": [
                {"group": "kinference.buoyant.io", "kind": "InferencePool", "name": "chat"}]}]
        }))
        .unwrap(),
    });
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[],
        &[route],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.parents.len(), 1);
    assert_eq!(p.parents[0].name, "inference-gateway");
    // The namespace defaults to the route's, so status names the Gateway
    // unambiguously.
    assert_eq!(p.parents[0].namespace.as_deref(), Some("infra"));
}

#[test]
fn a_route_targeting_a_different_pool_is_not_a_parent() {
    let route = Arc::new(HTTPRoute {
        metadata: meta("other", "infra"),
        spec: serde_json::from_value(serde_json::json!({
            "parentRefs": [{"name": "gw"}],
            "rules": [{"backendRefs": [
                {"group": "kinference.buoyant.io", "kind": "InferencePool", "name": "elsewhere"}]}]
        }))
        .unwrap(),
    });
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[],
        &[route],
        &pools(),
        &RejectionIndex::new(),
    );
    assert!(idx.lookup("infra", "chat").unwrap().parents.is_empty());
}

#[test]
fn a_service_backend_ref_is_not_a_parent() {
    // Only kinference.buoyant.io InferencePool refs count.
    let route = Arc::new(HTTPRoute {
        metadata: meta("svc-route", "infra"),
        spec: HTTPRouteSpecForTest::service_backend("chat"),
    });
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[],
        &[route],
        &pools(),
        &RejectionIndex::new(),
    );
    assert!(idx.lookup("infra", "chat").unwrap().parents.is_empty());
}

struct HTTPRouteSpecForTest;
impl HTTPRouteSpecForTest {
    fn service_backend(name: &str) -> crate::crd::HTTPRouteSpec {
        crate::crd::HTTPRouteSpec {
            parent_refs: vec![ParentRef {
                name: "gw".into(),
                ..Default::default()
            }],
            rules: vec![HTTPRouteRule {
                backend_refs: vec![crate::crd::BackendRef {
                    // No group/kind → a core Service, not our pool.
                    name: name.into(),
                    ..Default::default()
                }],
            }],
        }
    }
}

#[test]
fn unsupported_destination_kind_is_reported() {
    let dest = Destination {
        kind: Some("Service".into()),
        name: Some("svc".into()),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest], None)],
        &[],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::UnsupportedKind);
}

/// Identity carries the namespace, so a same-named set in another namespace
/// is a different pool.
#[test]
fn a_pool_in_another_namespace_does_not_claim_this_worker_set() {
    let elsewhere: PoolIndex = [(
        crate::PoolKey::namespaced("other", "workers-h100"),
        PoolId::from_raw(0),
    )]
    .into_iter()
    .collect();
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &elsewhere,
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(
        p.unresolved[0].reason,
        UnresolvedReason::NoMatchingPool,
        "a pool in another namespace must not claim this worker set"
    );
}

/// A pool reports the recorded rejection reason for an unusable destination,
/// not a generic `NoMatchingPool`.
#[test]
fn an_unresolved_destination_reports_the_resolvers_reason() {
    use crate::rejection::{RejectReason, Rejection};
    let sets = [
        worker_set("workers-h100", "vllm-h100", &[]),
        worker_set("workers-a100", "vllm-a100", &[]),
    ];
    let rejections = rejection_index(&[
        Rejection {
            key: crate::PoolKey::namespaced("infra", "workers-h100"),
            reason: RejectReason::UnknownProfile {
                defined: vec!["h100".into()],
            },
        },
        Rejection {
            key: crate::PoolKey::namespaced("infra", "workers-a100"),
            reason: RejectReason::MissingKvBlockSize,
        },
    ]);
    let idx = resolve_local(
        &[pool(
            "chat",
            vec![dest_named("workers-h100"), dest_named("workers-a100")],
            None,
        )],
        &[],
        &sets,
        &[],
        // Neither produced a pool, which is why they were rejected.
        &PoolIndex::new(),
        &rejections,
    );
    let p = idx.lookup("infra", "chat").unwrap();
    let reasons: Vec<UnresolvedReason> = p.unresolved.iter().map(|u| u.reason).collect();
    assert!(
        reasons.contains(&UnresolvedReason::UnknownProfile),
        "got {reasons:?}"
    );
    assert!(
        reasons.contains(&UnresolvedReason::InvalidKvBlockSize),
        "got {reasons:?}"
    );
}

/// With no rejection recorded, a set with no pool is `NoMatchingPool`.
#[test]
fn an_unrecorded_miss_stays_no_matching_pool() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::NoMatchingPool);
}

// --- alias resolution -------------------------------------------------

fn set_with_aliases(aliases: &[(&str, &str)]) -> ResolvedWorkerSet {
    ResolvedWorkerSet {
        name: "ws".into(),
        namespace: "infra".into(),
        labels: BTreeMap::new(),
        pool_id: PoolId::from_raw(0),
        service_name: "svc".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures pass (canonical, upstream); the canonical becomes an
            // alias of the upstream.
            .map(|(n, u)| ModelAlias {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
            })
            .collect(),
    }
}

#[test]
fn two_models_sharing_an_alias_yield_two_upstreams() {
    let ws = set_with_aliases(&[
        ("bar", "meta-llama/Llama-3.3-70B-Instruct"),
        ("bar", "neuralmagic/Llama-3.3-70B-Instruct-FP8"),
    ]);
    let up = ws.upstreams_for("bar");
    assert!(up.contains(&"meta-llama/Llama-3.3-70B-Instruct".to_string()));
    assert!(up.contains(&"neuralmagic/Llama-3.3-70B-Instruct-FP8".to_string()));
    // Plus the canonical name itself, in case it is also served directly.
    assert!(up.contains(&"bar".to_string()));
}

#[test]
fn no_aliases_means_identity() {
    let ws = set_with_aliases(&[]);
    assert_eq!(ws.upstreams_for("gemma"), vec!["gemma".to_string()]);
}

#[test]
fn aliases_are_additive_not_a_whitelist() {
    // Declaring an alias for one model must not hide the others.
    let ws = set_with_aliases(&[("foo", "meta-llama/Llama-3.1-8B-Instruct")]);
    assert_eq!(ws.upstreams_for("qwen"), vec!["qwen".to_string()]);
}

#[test]
fn canonical_for_inverts_the_alias_table() {
    let ws = set_with_aliases(&[("bar", "neuralmagic/Llama-3.3-70B-Instruct-FP8")]);
    assert_eq!(
        ws.canonical_for("neuralmagic/Llama-3.3-70B-Instruct-FP8"),
        "bar"
    );
    assert_eq!(
        ws.canonical_for("unlisted"),
        "unlisted",
        "identity fallback"
    );
}

// --- providers ---------------------------------------------------------

#[test]
fn a_provider_destination_resolves_to_a_backend_reference() {
    let prov = provider(
        "together",
        "infra",
        "together-backend",
        None,
        vec![("bar", "acme-llama-70b", Some(131072))],
    );
    let idx = resolve(
        &[pool("chat", vec![dest_provider("together", None)], None)],
        &[],
        &[],
        &[prov],
        &[],
        &[],
        // Empty: a provider resolves without any pool.
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert!(p.unresolved.is_empty(), "{:?}", p.unresolved);
    let got = p.destinations[0].as_provider().expect("provider");
    assert_eq!(got.name, "together");
    // The namespace is defaulted, so the pick response is fully qualified.
    assert_eq!(got.backend_ref, ObjectKey::new("infra", "together-backend"));
    assert_eq!(
        got.upstreams_for("bar"),
        vec![("acme-llama-70b".to_string(), Some(131072))]
    );
}

/// The path is carried, not interpreted. Absent and empty both become `None`.
#[test]
fn a_provider_path_travels_verbatim_and_empty_means_absent() {
    let mut prov = provider("together", "infra", "b", None, vec![("bar", "up", None)]);
    Arc::get_mut(&mut prov).unwrap().spec.path =
        Some("/serving-endpoints/chat/completions?api-version=2024-10-21".into());
    let r = resolve_provider(&prov, &[]).expect("resolves");
    assert_eq!(
        r.path.as_deref(),
        Some("/serving-endpoints/chat/completions?api-version=2024-10-21"),
        "the query string is part of the path the caller dispatches on"
    );

    // Whitespace is not a path either.
    for blank in ["", "   "] {
        Arc::get_mut(&mut prov).unwrap().spec.path = Some(blank.into());
        assert_eq!(resolve_provider(&prov, &[]).expect("resolves").path, None);
    }

    Arc::get_mut(&mut prov).unwrap().spec.path = None;
    assert_eq!(resolve_provider(&prov, &[]).expect("resolves").path, None);
}

/// A path is never a reason to refuse a destination; the schema's `^/` is the
/// only check.
#[test]
fn a_malformed_provider_path_does_not_unresolve_the_provider() {
    let mut prov = provider("together", "infra", "b", None, vec![("bar", "up", None)]);
    Arc::get_mut(&mut prov).unwrap().spec.path = Some("no-leading-slash".into());
    let r = resolve_provider(&prov, &[]).expect("still resolves");
    assert_eq!(r.path.as_deref(), Some("no-leading-slash"));
}

#[test]
fn a_provider_missing_a_required_field_is_incomplete_not_dropped() {
    // Required by the schema but `Option` in Rust, so enforced here.
    let mut prov = provider("together", "infra", "b", None, vec![("bar", "up", None)]);
    Arc::get_mut(&mut prov).unwrap().spec.api = None;
    let idx = resolve(
        &[pool("chat", vec![dest_provider("together", None)], None)],
        &[],
        &[],
        &[prov],
        &[],
        &[],
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert!(p.destinations.is_empty());
    assert_eq!(p.unresolved.len(), 1);
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::Incomplete);
    assert_eq!(p.unresolved[0].kind, "InferenceProvider");
}

#[test]
fn a_provider_with_no_usable_model_row_is_incomplete() {
    let prov = provider("together", "infra", "b", None, vec![]);
    let idx = resolve(
        &[pool("chat", vec![dest_provider("together", None)], None)],
        &[],
        &[],
        &[prov],
        &[],
        &[],
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::Incomplete);
}

#[test]
fn a_destination_with_no_kind_is_rejected_rather_than_assumed_local() {
    // `kind` has no default.
    let dest = Destination {
        kind: None,
        name: Some("a".into()),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest], None)],
        &[],
        &[worker_set("a", "vllm-a", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert!(p.destinations.is_empty());
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::UnsupportedKind);
    assert_eq!(p.unresolved[0].kind, "<unset>");
}

#[test]
fn a_name_shared_across_kinds_is_ambiguous_and_neither_is_admitted() {
    // A policy names destinations by bare string, so both would be ambiguous.
    let idx = resolve(
        &[pool(
            "chat",
            vec![dest_named("a"), dest_provider("a", None)],
            None,
        )],
        &[],
        &[worker_set("a", "vllm-a", &[])],
        &[provider("a", "infra", "b", None, vec![("bar", "up", None)])],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.destinations.len(), 1, "the first one stands");
    assert_eq!(p.unresolved.len(), 1);
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::AmbiguousName);
}

#[test]
fn a_cross_namespace_backend_ref_needs_a_grant_and_fails_closed() {
    let prov = provider(
        "together",
        "infra",
        "shared-backend",
        Some("vendors"),
        vec![("bar", "up", None)],
    );
    let call = |grants: &[Arc<ReferenceGrant>]| {
        resolve(
            &[pool("chat", vec![dest_provider("together", None)], None)],
            &[],
            &[],
            std::slice::from_ref(&prov),
            &[],
            grants,
            &PoolIndex::new(),
            &RejectionIndex::new(),
        )
    };

    // No grant: refused.
    let p = call(&[]);
    let p = p.lookup("infra", "chat").expect("pool");
    assert!(p.destinations.is_empty());
    assert_eq!(
        p.unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // A grant in the wrong namespace does not help either.
    let p = call(&[grant("elsewhere", "infra", None)]);
    assert_eq!(
        p.lookup("infra", "chat").unwrap().unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // A grant naming a different backend does not help.
    let p = call(&[grant("vendors", "infra", Some("other-backend"))]);
    assert_eq!(
        p.lookup("infra", "chat").unwrap().unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // Nor does one for InferencePools: `from` names the kind holding the
    // reference, here the provider.
    let p = call(&[grant_from(
        "InferencePool",
        "infra",
        "vendors",
        (GATEWAY_BACKEND_GROUP, GATEWAY_BACKEND_KIND),
        None,
    )]);
    assert_eq!(
        p.lookup("infra", "chat").unwrap().unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // The grant that actually covers it, both wildcard and by name.
    for g in [
        grant("vendors", "infra", None),
        grant("vendors", "infra", Some("shared-backend")),
    ] {
        let p = call(&[g]);
        let p = p.lookup("infra", "chat").expect("pool");
        assert!(p.unresolved.is_empty(), "{:?}", p.unresolved);
        assert_eq!(
            p.destinations[0].as_provider().unwrap().backend_ref,
            ObjectKey::new("vendors", "shared-backend")
        );
    }
}

/// A pool's destination in another namespace needs a grant there from
/// InferencePools: the pool holds that reference. One from InferenceProviders,
/// as a provider's `backendRef` needs, does not cover it.
#[test]
fn a_cross_namespace_destination_needs_a_grant_for_pools() {
    // Provider and its backend both live in `vendors`; only the pool crosses.
    let prov = provider(
        "together",
        "vendors",
        "shared-backend",
        None,
        vec![("bar", "up", None)],
    );
    let call = |grants: &[Arc<ReferenceGrant>]| {
        resolve(
            &[pool(
                "chat",
                vec![dest_provider("together", Some("vendors"))],
                None,
            )],
            &[],
            &[],
            std::slice::from_ref(&prov),
            &[],
            grants,
            &PoolIndex::new(),
            &RejectionIndex::new(),
        )
    };
    let to_providers = ("kinference.buoyant.io", "InferenceProvider");
    let reason = |grants: &[Arc<ReferenceGrant>]| {
        call(grants)
            .lookup("infra", "chat")
            .expect("pool")
            .unresolved
            .first()
            .map(|u| u.reason)
    };

    assert_eq!(
        reason(&[]),
        Some(UnresolvedReason::CrossNamespaceNotGranted)
    );
    assert_eq!(
        reason(&[grant_from(
            "InferenceProvider",
            "infra",
            "vendors",
            to_providers,
            None
        )]),
        Some(UnresolvedReason::CrossNamespaceNotGranted)
    );
    assert_eq!(
        reason(&[grant_from(
            "InferencePool",
            "infra",
            "vendors",
            to_providers,
            Some("together")
        )]),
        None
    );
}

#[test]
fn an_omitted_destinations_list_never_picks_up_a_provider() {
    // An empty `destinations` covers worker sets only; providers must be
    // named.
    let idx = resolve(
        &[pool("chat", vec![], None)],
        &[],
        &[worker_set("a", "vllm-a", &[])],
        &[provider(
            "together",
            "infra",
            "b",
            None,
            vec![("bar", "up", None)],
        )],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.destinations.len(), 1);
    assert!(p.destinations[0].as_local().is_some());
}

/// `name` is the vendor's spelling and matches on its own; `aliases` add
/// client-facing names on top.
#[test]
fn a_provider_model_matches_its_own_name_and_every_alias() {
    use crate::crd::ProviderModel;
    let mut prov = provider("vendor", "infra", "b", None, vec![]);
    Arc::get_mut(&mut prov).unwrap().spec.models = vec![
        ProviderModel {
            name: Some("Qwen/Qwen2.5-32B-Instruct-Turbo".into()),
            aliases: vec!["qwen-2.5".into(), "qwen-simple".into()],
            context_window: Some(32768),
        },
        // No aliases at all.
        ProviderModel {
            name: Some("meta/llama-3.3-70b".into()),
            aliases: vec![],
            context_window: None,
        },
    ];
    let r = resolve_provider(&prov, &[]).expect("resolves");

    let upstream =
        |n: &str| -> Vec<String> { r.upstreams_for(n).into_iter().map(|(u, _)| u).collect() };
    // Every alias reaches the vendor's model, and so does its own name.
    for n in ["qwen-2.5", "qwen-simple", "Qwen/Qwen2.5-32B-Instruct-Turbo"] {
        assert_eq!(
            upstream(n),
            vec!["Qwen/Qwen2.5-32B-Instruct-Turbo".to_string()],
            "{n} must resolve to the vendor's spelling"
        );
    }
    // An aliasless row is reachable under its own name.
    assert_eq!(
        upstream("meta/llama-3.3-70b"),
        vec!["meta/llama-3.3-70b".to_string()]
    );
    // And nothing else is: there is no discovery for a provider.
    assert!(upstream("gpt-4").is_empty());

    // The context window travels with the row, so every alias inherits it.
    assert_eq!(r.upstreams_for("qwen-simple")[0].1, Some(32768));

    assert_eq!(
        r.served_names(),
        vec![
            "Qwen/Qwen2.5-32B-Instruct-Turbo",
            "meta/llama-3.3-70b",
            "qwen-2.5",
            "qwen-simple",
        ]
    );
}

#[test]
fn provider_served_names_report_what_is_declared() {
    let prov = provider(
        "together",
        "infra",
        "b",
        None,
        vec![
            ("bar", "up-1", None),
            ("foo", "up-2", None),
            ("bar", "up-3", None),
        ],
    );
    let r = resolve_provider(&prov, &[]).expect("resolves");
    // The vendor's own name matches as well as its aliases.
    assert_eq!(r.served_names(), vec!["bar", "foo", "up-1", "up-2", "up-3"]);
    // Two rows sharing one alias are two candidates.
    assert_eq!(r.upstreams_for("bar").len(), 2);
    // And the upstream name resolves to itself.
    assert_eq!(
        r.upstreams_for("up-2"),
        vec![("up-2".to_string(), None)],
        "a request naming the vendor's own model must reach it"
    );
}

/// With no grants visible (including when the ReferenceGrant CRD is absent),
/// no cross-namespace reference is permitted.
#[test]
fn an_absent_grant_store_permits_nothing() {
    assert!(!grant_permits(
        &[],
        "vendors",
        GATEWAY_BACKEND_GROUP,
        GATEWAY_BACKEND_KIND,
        Some("shared-backend"),
        "infra",
        "InferenceProvider",
    ));
}

// --- every policy is published, referenced or not -----------------------

#[test]
fn a_policy_no_pool_references_is_still_published() {
    let idx = resolve_local(
        // The pool names `attached`; `orphan` exists and nothing points at it.
        &[pool("chat", vec![], Some("attached"))],
        &[policy("attached"), policy("orphan")],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let got: Vec<(&str, usize)> = idx
        .policies()
        .map(|p| (p.name.as_str(), p.referenced_by.len()))
        .collect();
    // Both appear.
    assert_eq!(got, vec![("attached", 1), ("orphan", 0)]);
}

#[test]
fn referenced_by_names_the_pools() {
    let idx = resolve_local(
        &[
            pool("chat", vec![], Some("shared")),
            pool("batch", vec![], Some("shared")),
        ],
        &[policy("shared")],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.policies().next().expect("policy");
    let mut by = p.referenced_by.clone();
    by.sort();
    // A policy reached by two pools names both.
    assert_eq!(by, vec!["batch", "chat"]);
}

#[test]
fn a_dangling_policy_ref_leaves_no_policy_to_publish() {
    let idx = resolve_local(
        &[pool("chat", vec![], Some("typo"))],
        &[],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    // Nothing to publish: there is no object named `typo`. The pool reports
    // it instead.
    assert_eq!(idx.policies().count(), 0);
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.dangling_policy.as_ref().map(|d| d.name()), Some("typo"));
}

#[test]
fn the_published_order_is_stable() {
    // Stable order, for the status writer's last-written comparison.
    let call = || {
        resolve_local(
            &[pool("chat", vec![], Some("b"))],
            &[policy("c"), policy("a"), policy("b")],
            &[],
            &[],
            &pools(),
            &RejectionIndex::new(),
        )
    };
    let names = |i: &PolicyIndex| -> Vec<String> { i.policies().map(|p| p.name.clone()).collect() };
    assert_eq!(names(&call()), vec!["a", "b", "c"]);
    assert_eq!(names(&call()), names(&call()));
}

#[test]
fn a_policy_shared_by_two_pools_is_compiled_once() {
    let idx = resolve_local(
        &[
            pool("chat", vec![], Some("shared")),
            pool("batch", vec![], Some("shared")),
        ],
        &[policy("shared")],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let a = idx.lookup("infra", "chat").unwrap().policy.clone();
    let b = idx.lookup("infra", "batch").unwrap().policy.clone();
    assert!(
        Arc::ptr_eq(&a, &b),
        "both pools should share one compilation"
    );
    assert!(Arc::ptr_eq(&a, &idx.policies().next().unwrap().policy));
}

// --- OpenAI-compatible only --------------------------------------------

/// A format nothing translates to doesn't become a destination.
#[test]
fn a_non_openai_api_is_refused() {
    for api in [
        ApiFormat::Anthropic,
        ApiFormat::Bedrock,
        ApiFormat::VertexAI,
        ApiFormat::AzureOpenAI,
    ] {
        let mut prov = provider("p", "infra", "b", None, vec![("bar", "up", None)]);
        Arc::get_mut(&mut prov).unwrap().spec.api = Some(api);
        let idx = resolve(
            &[pool("chat", vec![dest_provider("p", None)], None)],
            &[],
            &[],
            std::slice::from_ref(&prov),
            &[],
            &[],
            &PoolIndex::new(),
            &RejectionIndex::new(),
        );
        let p = idx.lookup("infra", "chat").expect("pool");
        assert!(p.destinations.is_empty(), "{api:?} must not resolve");
        assert_eq!(
            p.unresolved[0].reason,
            UnresolvedReason::UnsupportedApiFormat,
            "{api:?}"
        );
    }
}

/// Enforced at resolve, not only in the CRD enum: an object that bypassed
/// the schema still deserializes, and is then refused.
#[test]
fn a_bypassed_object_still_parses_before_it_is_refused() {
    let spec: crate::crd::InferenceProviderSpec = serde_json::from_value(serde_json::json!({
        "backendRef": {"name": "b"},
        "api": "Anthropic",
        "models": [{"name": "up", "aliases": ["bar"]}]
    }))
    .expect("a value the CRD no longer admits must still DESERIALIZE");
    assert_eq!(spec.api, Some(ApiFormat::Anthropic));

    let prov = Arc::new(InferenceProvider {
        metadata: meta("p", "infra"),
        spec,
        status: None,
    });
    assert_eq!(
        resolve_provider(&prov, &[]).unwrap_err(),
        UnresolvedReason::UnsupportedApiFormat
    );
}

/// `api` is validated and then discarded; the wire carries no format.
#[test]
fn openai_still_resolves() {
    let prov = provider("p", "infra", "b", None, vec![("bar", "up", None)]);
    let r = resolve_provider(&prov, &[]).expect("resolves");
    assert_eq!(r.name, "p");
    assert_eq!(r.backend_ref, ObjectKey::new("infra", "b"));
}

/// A bare `resolve` carries an unlisted `Service` view; the watcher attaches
/// the real one.
#[test]
fn a_resolved_snapshot_carries_no_services_until_the_watcher_attaches_them() {
    let idx = resolve_local(&[], &[], &[], &[], &pools(), &RejectionIndex::new());
    assert!(idx.listed(), "the CRD side of this snapshot is complete");
    assert!(
        !idx.services().listed(),
        "and the Service side is not, because nothing here read one"
    );

    let attached = resolve_local(&[], &[], &[], &[], &pools(), &RejectionIndex::new())
        .with_services(crate::services::ServiceIndex::build(&[]));
    assert!(attached.services().listed());
    assert!(attached.services().is_empty());
}
