use super::*;
use kinference_fleet::workers::WorkerEntry;

use kinference_fleet::pool::test_pool as pool;

fn ready_entry() -> WorkerEntry {
    WorkerEntry {
        worker_id_str: "w-1".into(),
        model_name_hint: Some("gemma".into()),
        current_models: vec!["gemma".into()],
        http_url: Some("http://10.0.0.1:8000/".into()),
        block_size: 16,
        initial_sync_complete: true,
        kv_events_observed: true,
        score_addr: "10.0.0.1:8000".into(),
        engine: None,
        pod_name: None,
        namespace: None,
        node_name: None,
        node_ip: None,
        pod_ip: None,
        availability_zone: None,
        instance_type: None,
    }
}

async fn status_and_body(
    pools: Arc<PoolRegistry>,
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
) -> (StatusCode, String) {
    status_with_worker_sets(pools, policy, tokio::sync::watch::channel(true).1).await
}

async fn status_with_worker_sets(
    pools: Arc<PoolRegistry>,
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
    worker_sets_ready: tokio::sync::watch::Receiver<bool>,
) -> (StatusCode, String) {
    let resp = readyz(State(ReadyzState {
        pools,
        policy,
        worker_sets_ready,
    }))
    .await;
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .expect("readyz body is small");
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Readiness doesn't wait for anywhere to route to: a Pick for a model
/// nothing serves answers `NO_WORKERS`, and a provider-only deployment has no
/// workers.
#[tokio::test]
async fn ready_with_no_pools_at_all() {
    let (status, body) = status_and_body(PoolRegistry::new(), None).await;
    assert_eq!(status, StatusCode::OK, "got: {body:?}");
    assert!(body.contains("worker sets: 0"), "got: {body:?}");
}

#[tokio::test]
async fn readiness_waits_for_the_initial_worker_set_list_even_when_empty() {
    let (ready, receiver) = tokio::sync::watch::channel(false);
    let pools = PoolRegistry::new();
    let (status, body) = status_with_worker_sets(pools.clone(), None, receiver.clone()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("InferenceWorkerSet watch"));

    ready.send_replace(true);
    let (status, body) = status_with_worker_sets(pools, None, receiver).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("worker sets: 0"));
}

#[tokio::test]
async fn policy_and_worker_set_initialization_are_independent_readiness_gates() {
    for (workers_listed, policy_listed) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let policy = PolicyStore::new();
        if policy_listed {
            policy.publish(Arc::new(
                kinference_policy::resolve::PolicyIndex::listed_empty(),
            ));
        }
        let receiver = tokio::sync::watch::channel(workers_listed).1;
        let (status, _) =
            status_with_worker_sets(PoolRegistry::new(), Some(policy), receiver).await;
        assert_eq!(status == StatusCode::OK, workers_listed && policy_listed);
    }
}

/// Cold workers are ready too; the body reports the fleet state.
#[tokio::test]
async fn ready_with_pools_whose_workers_are_still_cold() {
    let registry = PoolRegistry::new();
    registry.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        pool("cold"),
    ])));
    let (status, body) = status_and_body(registry, None).await;
    assert_eq!(status, StatusCode::OK, "got: {body:?}");
    assert!(body.contains("worker sets: 1"), "got: {body:?}");
    assert!(body.contains("ready endpoints: 0"), "got: {body:?}");
}

#[tokio::test]
async fn the_body_counts_pools_with_ready_endpoints() {
    let registry = PoolRegistry::new();
    let p = pool("warm");
    p.registry.register(1, ready_entry());
    registry.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        p,
    ])));
    let (status, body) = status_and_body(registry, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ready endpoints: 1"), "got: {body:?}");
}

/// `/metrics` computes the fleet gauges in the scrape itself: with no task
/// running, the first scrape after an ingest already shows the index size
/// and the worker health.
#[tokio::test]
async fn the_first_scrape_shows_the_fleet_gauges_with_no_task_running() {
    use dynamo_kv_router::indexer::KvIndexerInterface;

    let registry = PoolRegistry::new();
    let p = pool("warm");
    p.registry.register(1, ready_entry());
    p.indexer
        .apply_event(kinference_fleet::gauges::test_stored_event(1, &[1, 2]))
        .await;
    registry.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        p,
    ])));
    let (app, _) = app_with(None, registry);

    let (status, _, body) = call(&app, "GET", "/metrics", None, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    let out = String::from_utf8(body).unwrap();
    assert!(
        out.lines().any(|l| l == "kinference_index_blocks 2"),
        "{out}"
    );
    // Never scraped, so its age counts from the registry's startup: just now.
    assert!(
        out.lines()
            .any(|l| l == r#"kinference_workers{state="ok"} 1"#),
        "{out}"
    );
    assert!(
        out.lines()
            .any(|l| l == r#"kinference_workers_by_model{model="gemma",state="ok"} 1"#),
        "{out}"
    );
}

/// The one gate: before the policy watch lists, a Pick naming a pool would
/// route on load alone, bypassing the policy.
#[tokio::test]
async fn not_ready_until_the_policy_watch_has_listed() {
    let store = kinference_policy::store::PolicyStore::new();
    let (status, body) = status_and_body(PoolRegistry::new(), Some(store.clone())).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("silently bypass"), "got: {body:?}");

    // A listed-but-empty snapshot is enough.
    store.publish(Arc::new(
        kinference_policy::resolve::PolicyIndex::listed_empty(),
    ));
    let (status, body) = status_and_body(PoolRegistry::new(), Some(store)).await;
    assert_eq!(status, StatusCode::OK, "got: {body:?}");
}

/// Policy off means nothing to wait for.
#[tokio::test]
async fn ready_immediately_when_policy_is_disabled() {
    let (status, _) = status_and_body(PoolRegistry::new(), None).await;
    assert_eq!(status, StatusCode::OK);
}

// ── Policy API ────────────────────────────────────────────────────────────

use kinference_policy::resolve::PolicyIndex;

fn api_state(
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
) -> policy_api::PolicyApiState {
    policy_api::PolicyApiState::new(
        policy,
        PoolRegistry::new(),
        Arc::new(kinference_fleet::scrape::MetricsRegistry::new()),
    )
}

async fn body_of(resp: Response) -> (StatusCode, serde_json::Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .expect("explorer bodies are small");
    let v = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
                .chars()
                .take(200)
                .collect::<String>()
        )
    });
    (status, v)
}

/// `policy.enabled = false` is reported as such, not as "no policies".
#[tokio::test]
async fn context_says_disabled_rather_than_returning_an_empty_list() {
    let (status, body) = body_of(policy_api::context(State(api_state(None))).await).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["error"]["code"], "PolicyDisabled", "{body}");
    assert!(body["policies"].is_null(), "no list at all: {body}");
}

/// Before the watch has listed, the policy set is unknown, not empty.
#[tokio::test]
async fn context_says_not_listed_before_the_first_list_completes() {
    // A default `PolicyStore` has never published, so `listed()` is false.
    let store = kinference_policy::store::PolicyStore::new();
    let (status, body) = body_of(policy_api::context(State(api_state(Some(store)))).await).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "NotListed", "{body}");
}

/// A listed-but-empty cluster is an empty list.
#[tokio::test]
async fn context_returns_an_empty_list_once_listed_with_no_policies() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(PolicyIndex::listed_empty()));
    let (status, body) = body_of(policy_api::context(State(api_state(Some(store)))).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "ok");
    assert_eq!(body["policies"].as_array().unwrap().len(), 0);
    // The vocabulary comes from the code.
    let qos: Vec<&str> = body["qos"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_str().unwrap())
        .collect();
    assert_eq!(qos, vec!["interactive", "batch", "deferrable"]);
}

/// A `namespace/name` that does not resolve is a 404, not an empty explanation.
#[tokio::test]
async fn simulate_404s_on_an_unknown_policy() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(PolicyIndex::listed_empty()));
    let req = serde_json::from_value(serde_json::json!({
        "policy": "default/nope", "model": ["m"], "candidates": []
    }))
    .unwrap();
    let (status, body) = body_of(
        policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NotFound", "{body}");
    assert_eq!(body["error"]["field"], "policy", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("default/nope")
    );
}

/// `policy` must be namespaced.
#[tokio::test]
async fn simulate_rejects_an_unnamespaced_policy() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(PolicyIndex::listed_empty()));
    let req = serde_json::from_value(serde_json::json!({
        "policy": "policy-5", "candidates": []
    }))
    .unwrap();
    let (status, _) = body_of(
        policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// An index holding one unattached policy, which needs no pool or worker set.
fn index_with_policy(name: &str, spec: serde_json::Value) -> Arc<PolicyIndex> {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kinference_policy::crd::{InferencePolicy, InferencePolicySpec};
    let p = Arc::new(InferencePolicy {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: serde_json::from_value::<InferencePolicySpec>(spec).expect("valid spec"),
        status: None,
    });
    Arc::new(kinference_policy::resolve::resolve(
        &[],
        &[p],
        &[],
        &[],
        &[],
        &[],
        &std::collections::BTreeMap::new(),
        &std::collections::BTreeMap::new(),
    ))
}

fn policy_5() -> serde_json::Value {
    // policy-5 of the demo fleet in `crates/kinference-policy/src/check/tests.rs`.
    serde_json::json!({
        "rules": [{
            "name": "batch-only-where-nothing-is-queued",
            "matches": [{ "qos": ["batch"] }],
            "route": {
                "consider": [{ "type": "Local" }],
                "exclude": [{
                    "name": "require-an-empty-queue",
                    "drop": { "backend": { "type": "Local" }, "queuedPrompts": { "greaterThan": 0 } }
                }],
                "select": [{ "minimize": "PredictedTTFT" }],
                "onEmpty": "Reject"
            }
        }]
    })
}

/// An unattached policy is still listed.
#[tokio::test]
async fn context_lists_an_unattached_policy_and_flags_it() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy("policy-5", policy_5()));
    let (status, body) = body_of(policy_api::context(State(api_state(Some(store)))).await).await;
    assert_eq!(status, StatusCode::OK);

    let p = &body["policies"][0];
    assert_eq!(p["key"], "default/policy-5");
    assert_eq!(p["attached"], false, "no pool names it: {p}");
    assert_eq!(p["referencedBy"].as_array().unwrap().len(), 0);
    assert!(
        p["state"].as_str().unwrap().contains("NotReferenced"),
        "state should say so: {p}"
    );
    assert_eq!(p["rules"][0]["name"], "batch-only-where-nothing-is-queued");
    assert_eq!(p["rules"][0]["onEmpty"], "Reject");
}

/// End to end: the same request flips verdict on an edited candidate,
/// covering matching (`qos: batch`), an exclusion on a candidate attribute,
/// `onEmpty: Reject`, and the override plumbing through `build_candidate`.
#[tokio::test]
async fn simulate_flips_verdict_when_a_candidate_override_is_edited() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy("policy-5", policy_5()));

    let run = |queued: i64| {
        let store = store.clone();
        async move {
            let req = serde_json::from_value(serde_json::json!({
                "policy": "default/policy-5",
                "model": ["qwen-2.5"],
                "qos": "batch",
                "inputTokens": 4096,
                "maxTokens": 256,
                "candidates": [{
                    "backendName": "ws-qwen",
                    "backendType": "Local",
                    "canonicalModel": "qwen-2.5",
                    "endpoint": "10.0.1.100:8000",
                    "queuedPrompts": queued,
                    "predictedTtftMs": 400.0
                }]
            }))
            .unwrap();
            body_of(
                policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
            )
            .await
        }
    };

    let (s0, idle) = run(0).await;
    assert_eq!(s0, StatusCode::OK);
    assert_eq!(
        idle["verdict"], "chosen",
        "an idle worker is allowed: {idle}"
    );
    assert!(idle["winner"].as_str().unwrap().contains("ws-qwen"));

    let (s1, busy) = run(1).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(
        busy["verdict"], "rejected_on_empty",
        "one queued prompt is enough: {busy}"
    );
    assert!(busy["winner"].is_null());

    // The exclusion is named, and the candidate's attributes come back.
    let elim = &busy["steps"][0]["trace"]["eliminated"][0];
    assert!(
        elim["why"]
            .as_str()
            .unwrap()
            .contains("require-an-empty-queue"),
        "{elim}"
    );
    // A float, since `queuedPrompts` is a windowed mean.
    assert_eq!(busy["effectiveCandidates"][0]["queuedPrompts"], 1.0);
    assert_eq!(
        busy["effectiveCandidates"][0]["predictedTtftMs"], 400.0,
        "an explicit override is used verbatim, not recomputed"
    );
}

/// Simulate routes on the `tags` it is given.
#[tokio::test]
async fn simulate_routes_on_the_given_tags() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy(
        "restricted-only",
        serde_json::json!({
            "unmatched": "Reject",
            "rules": [{
                "name": "restricted-stays-local",
                "matches": [{ "tag": "restricted" }],
                "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
            }]
        }),
    ));

    let run = |tags: serde_json::Value| {
        let store = store.clone();
        async move {
            let req = serde_json::from_value(serde_json::json!({
                "policy": "default/restricted-only",
                "model": ["qwen-2.5"],
                "tags": tags,
                "candidates": [{
                    "backendName": "ws-qwen",
                    "backendType": "Local",
                    "canonicalModel": "qwen-2.5",
                    "endpoint": "10.0.1.100:8000",
                    "predictedTtftMs": 400.0
                }]
            }))
            .unwrap();
            body_of(
                policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
            )
            .await
        }
    };

    let (status, tagged) = run(serde_json::json!(["internal", "restricted"])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tagged["verdict"], "chosen", "{tagged}");

    let (status, untagged) = run(serde_json::json!([])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(untagged["verdict"], "rejected_unmatched", "{untagged}");
}

/// `tags` is bounded like `model`.
#[tokio::test]
async fn simulate_rejects_too_many_tags() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy("policy-5", policy_5()));
    let tags: Vec<String> = (0..65).map(|i| format!("t{i}")).collect();
    let req = serde_json::from_value(serde_json::json!({
        "policy": "default/policy-5", "tags": tags, "candidates": []
    }))
    .unwrap();
    let (status, body) = body_of(
        policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["field"], "tags", "{body}");
}

/// A candidate with no prediction is ranked at 0.0 ms, so `predictionSource`
/// must say so.
#[tokio::test]
async fn an_uncomputable_prediction_is_disclosed_rather_than_shown_as_zero() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy("policy-5", policy_5()));

    // No `pool`, so there is no predictor to borrow and no override given.
    let req = serde_json::from_value(serde_json::json!({
        "policy": "default/policy-5",
        "model": ["qwen-2.5"],
        "qos": "batch",
        "inputTokens": 4096,
        "candidates": [
            { "backendName": "ws-a", "backendType": "Local",
              "canonicalModel": "qwen-2.5", "endpoint": "10.0.1.1:8000",
              "queuedPrompts": 0 },
            { "backendName": "ws-b", "backendType": "Local",
              "canonicalModel": "qwen-2.5", "endpoint": "10.0.1.2:8000",
              "queuedPrompts": 0, "predictedTtftMs": 250.0 },
            { "backendName": "together", "backendType": "Provider",
              "canonicalModel": "qwen-2.5" }
        ]
    }))
    .unwrap();

    let (status, body) = body_of(
        policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cands = &body["effectiveCandidates"];

    // Each of the three reasons is distinct.
    assert_eq!(
        cands[0]["predictionSource"], "unavailable",
        "no pool and no override means no prediction: {cands}"
    );
    assert!(
        cands[0]["predictionNote"]
            .as_str()
            .unwrap()
            .contains("ranked as 0.0 ms"),
        "the note must say what the missing prediction did to the ranking: {cands}"
    );
    assert!(cands[0]["predictedTtftMs"].is_null(), "{cands}");
    assert_eq!(cands[1]["predictionSource"], "override");
    assert_eq!(cands[1]["predictedTtftMs"], 250.0);
    assert_eq!(
        cands[2]["predictionSource"], "notApplicable",
        "a provider has an aggregate observed TTFT, never a per-request one: {cands}"
    );

    // `ws-a` won on 0.0 ms although `ws-b` has the only real number.
    assert_eq!(body["verdict"], "chosen", "{body}");
    assert!(
        body["winner"].as_str().unwrap().contains("ws-a"),
        "the unpredicted candidate ranks FIRST, at a fabricated zero: {body}"
    );
    assert_eq!(body["steps"][0]["trace"]["winnerObjectiveMs"], 0.0);
}

/// `spec.models` aliases must reach the candidate table.
///
/// Both the upstream and the alias are seeded, so a `consider` naming the
/// alias admits it.
#[tokio::test]
async fn a_local_worker_is_seeded_under_every_declared_alias() {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kinference_policy::crd::{
        InferencePool, InferencePoolSpec, InferenceWorkerSet, InferenceWorkerSetSpec,
    };

    let meta = |name: &str| ObjectMeta {
        name: Some(name.into()),
        namespace: Some("default".into()),
        ..Default::default()
    };

    let ws = Arc::new(InferenceWorkerSet {
        metadata: meta("gemma-4-12b"),
        spec: serde_json::from_value::<InferenceWorkerSetSpec>(serde_json::json!({
            "serviceRef": { "name": "gemma-4-12b-vllm", "port": "http" },
            "profile": "default",
            // the gemma-4 worker set of the demo fleet in `crates/kinference-policy/src/check/tests.rs`.
            "models": [{ "name": "google/gemma-4-12B-it", "aliases": ["gemma-4"] }],
        }))
        .expect("valid worker set spec"),
        status: None,
    });
    let ip = Arc::new(InferencePool {
        metadata: meta("inference-local"),
        spec: serde_json::from_value::<InferencePoolSpec>(serde_json::json!({
            "destinations": [{ "kind": "InferenceWorkerSet", "name": "gemma-4-12b" }],
        }))
        .expect("valid pool spec"),
        status: None,
    });

    // The worker reports only the upstream name; the alias is on the CR.
    let live = pool("gemma-4-12b");
    let pool_id = live.id;
    let mut entry = ready_entry();
    entry.current_models = vec!["google/gemma-4-12B-it".into()];
    entry.model_name_hint = Some("google/gemma-4-12B-it".into());
    live.registry.register(1, entry);
    let pools = PoolRegistry::new();
    pools.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        live,
    ])));

    let mut by_key = std::collections::BTreeMap::new();
    by_key.insert(
        kinference_policy::PoolKey {
            namespace: Some("default".into()),
            name: "gemma-4-12b".into(),
        },
        pool_id,
    );

    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(kinference_policy::resolve::resolve(
        &[ip],
        &[],
        &[ws],
        &[],
        &[],
        &[],
        &by_key,
        &std::collections::BTreeMap::new(),
    )));

    let st = policy_api::PolicyApiState::new(
        Some(store),
        pools,
        Arc::new(kinference_fleet::scrape::MetricsRegistry::new()),
    );
    let (status, body) = body_of(policy_api::context(State(st)).await).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let cands = body["pools"][0]["candidates"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("no candidates array: {body}");
        });
    let names: Vec<&str> = cands
        .iter()
        .map(|c| c["canonicalModel"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"gemma-4"),
        "the alias a rule would name is missing: {names:?}"
    );
    assert!(
        names.contains(&"google/gemma-4-12B-it"),
        "aliases ADD names — the upstream stays routable under itself: {names:?}"
    );
    // Both point at the same wire string, which is what the body will carry.
    for c in cands {
        assert_eq!(c["upstreamModel"], "google/gemma-4-12B-it", "{c}");
        assert_eq!(c["backendType"], "Local", "{c}");
    }
}

/// A `matches: {model: qwen-2.5}` rule must fire for a request naming
/// `qwen-simple`.
///
/// `RequestAttrs::model` is the resolved upstream plus every alias declared
/// for it, as in production.
#[tokio::test]
async fn a_model_term_matches_any_sibling_alias_of_the_same_upstream_model() {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kinference_policy::crd::{
        InferencePool, InferencePoolSpec, InferenceWorkerSet, InferenceWorkerSetSpec,
    };

    let meta = |name: &str| ObjectMeta {
        name: Some(name.into()),
        namespace: Some("default".into()),
        ..Default::default()
    };
    let ws = Arc::new(InferenceWorkerSet {
        metadata: meta("qwen-2-5-32b"),
        spec: serde_json::from_value::<InferenceWorkerSetSpec>(serde_json::json!({
            "serviceRef": { "name": "qwen-vllm", "port": "http" },
            "profile": "default",
            // the qwen-2.5 worker set of the demo fleet in `crates/kinference-policy/src/check/tests.rs`.
            "models": [{
                "name": "Qwen/Qwen2.5-32B-Instruct-AWQ",
                "aliases": ["qwen-2.5", "qwen-simple"],
            }],
        }))
        .expect("valid worker set spec"),
        status: None,
    });
    let ip = Arc::new(InferencePool {
        metadata: meta("inference-local"),
        spec: serde_json::from_value::<InferencePoolSpec>(serde_json::json!({
            "destinations": [{ "kind": "InferenceWorkerSet", "name": "qwen-2-5-32b" }],
            "policyRef": { "name": "alias-policy" },
        }))
        .expect("valid pool spec"),
        status: None,
    });
    let pol = Arc::new(kinference_policy::crd::InferencePolicy {
        metadata: meta("alias-policy"),
        spec: serde_json::from_value::<kinference_policy::crd::InferencePolicySpec>(
            serde_json::json!({
                "rules": [{
                    "name": "only-qwen-2-5",
                    // Names ONE alias of the upstream model.
                    "matches": [{ "model": "qwen-2.5" }],
                    "route": {
                        "consider": [{ "type": "Local" }],
                        "select": [{ "minimize": "PredictedTTFT" }],
                        "onEmpty": "Reject"
                    }
                }],
                "unmatched": "Reject"
            }),
        )
        .expect("valid policy spec"),
        status: None,
    });

    let live = pool("qwen-2-5-32b");
    let pool_id = live.id;
    let mut entry = ready_entry();
    entry.current_models = vec!["Qwen/Qwen2.5-32B-Instruct-AWQ".into()];
    live.registry.register(1, entry);
    let pools = PoolRegistry::new();
    pools.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        live,
    ])));

    let mut by_key = std::collections::BTreeMap::new();
    by_key.insert(
        kinference_policy::PoolKey {
            namespace: Some("default".into()),
            name: "qwen-2-5-32b".into(),
        },
        pool_id,
    );
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(kinference_policy::resolve::resolve(
        &[ip],
        &[pol],
        &[ws],
        &[],
        &[],
        &[],
        &by_key,
        &std::collections::BTreeMap::new(),
    )));

    let run = |requested: &'static str| {
        let store = store.clone();
        let pools = pools.clone();
        async move {
            let req = serde_json::from_value(serde_json::json!({
                "policy": "default/alias-policy",
                "pool": "default/inference-local",
                "requestedModel": requested,
                "inputTokens": 1024,
                // One candidate per name the endpoint is routable under.
                // A `consider` with no `models:` targets the requested name, so
                // the policy admits only that row.
                "candidates": [
                    { "backendName": "qwen-2-5-32b", "backendType": "Local",
                      "canonicalModel": "Qwen/Qwen2.5-32B-Instruct-AWQ",
                      "endpoint": "10.0.1.100:8000", "queuedPrompts": 0 },
                    { "backendName": "qwen-2-5-32b", "backendType": "Local",
                      "canonicalModel": "qwen-2.5",
                      "endpoint": "10.0.1.100:8000", "queuedPrompts": 0 },
                    { "backendName": "qwen-2-5-32b", "backendType": "Local",
                      "canonicalModel": "qwen-simple",
                      "endpoint": "10.0.1.100:8000", "queuedPrompts": 0 }
                ]
            }))
            .unwrap();
            let st = policy_api::PolicyApiState::new(
                Some(store),
                pools,
                Arc::new(kinference_fleet::scrape::MetricsRegistry::new()),
            );
            body_of(policy_api::simulate(State(st), policy_api::ApiJson(req)).await).await
        }
    };

    // The name the rule literally spells.
    let (_, direct) = run("qwen-2.5").await;
    assert_eq!(direct["verdict"], "chosen", "{direct}");

    // The sibling alias of the same upstream, so the same rule fires.
    let (_, sibling) = run("qwen-simple").await;
    assert_eq!(
        sibling["steps"][0]["matched"], true,
        "`model: qwen-2.5` must fire for a sibling alias of the same upstream: {sibling}"
    );
    assert_eq!(sibling["verdict"], "chosen", "{sibling}");

    // And the expansion is reported.
    let names: Vec<&str> = sibling["resolvedModels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["Qwen/Qwen2.5-32B-Instruct-AWQ", "qwen-2.5", "qwen-simple"],
        "upstream first, then aliases sorted — the order `lifecycle.rs` builds"
    );

    // An unrelated model still misses, so the expansion has not made `model:`
    // match everything.
    let (_, other) = run("gemma-4").await;
    assert_eq!(other["steps"][0]["matched"], false, "{other}");
    assert_eq!(other["verdict"], "rejected_unmatched", "{other}");
}

/// Each candidate is predicted with its own worker set's model, since
/// coefficients and block size differ per set.
#[tokio::test]
async fn each_candidate_is_predicted_with_its_own_worker_sets_model() {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kinference_policy::crd::{
        InferencePool, InferencePoolSpec, InferenceWorkerSet, InferenceWorkerSetSpec,
    };

    let meta = |name: &str| ObjectMeta {
        name: Some(name.into()),
        namespace: Some("default".into()),
        ..Default::default()
    };
    let ws = |name: &str, block: u32| {
        Arc::new(InferenceWorkerSet {
            metadata: meta(name),
            spec: serde_json::from_value::<InferenceWorkerSetSpec>(serde_json::json!({
                "serviceRef": { "name": format!("{name}-svc"), "port": "http" },
                "profile": "default",
                "kvBlockSize": block,
            }))
            .expect("valid worker set spec"),
            status: None,
        })
    };
    let ip = Arc::new(InferencePool {
        metadata: meta("both"),
        spec: serde_json::from_value::<InferencePoolSpec>(serde_json::json!({
            "destinations": [
                { "kind": "InferenceWorkerSet", "name": "big-blocks" },
                { "kind": "InferenceWorkerSet", "name": "small-blocks" },
            ],
        }))
        .expect("valid pool spec"),
        status: None,
    });

    // Two live pools with different block sizes, so `total_blocks` and the
    // prefill term differ for one prompt.
    let big = kinference_fleet::pool::test_pool_with_block_size("big-blocks", 256);
    let small = kinference_fleet::pool::test_pool_with_block_size("small-blocks", 16);
    let (big_id, small_id) = (big.id, small.id);
    let pools = PoolRegistry::new();
    pools.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        big, small,
    ])));

    let mut by_key = std::collections::BTreeMap::new();
    for (name, id) in [("big-blocks", big_id), ("small-blocks", small_id)] {
        by_key.insert(
            kinference_policy::PoolKey {
                namespace: Some("default".into()),
                name: name.into(),
            },
            id,
        );
    }
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(kinference_policy::resolve::resolve(
        &[ip],
        &[Arc::new(kinference_policy::crd::InferencePolicy {
            metadata: meta("passthrough"),
            spec: serde_json::from_value(serde_json::json!({
                "rules": [], "unmatched": "ServeLocal"
            }))
            .expect("valid policy spec"),
            status: None,
        })],
        &[ws("big-blocks", 256), ws("small-blocks", 16)],
        &[],
        &[],
        &[],
        &by_key,
        &std::collections::BTreeMap::new(),
    )));

    let req = serde_json::from_value(serde_json::json!({
        "policy": "default/passthrough",
        "pool": "default/both",
        "requestedModel": "m",
        "inputTokens": 4096,
        "candidates": [
            { "backendName": "big-blocks", "backendType": "Local",
              "canonicalModel": "m", "endpoint": "10.0.0.1:8000", "queuedPrompts": 0 },
            { "backendName": "small-blocks", "backendType": "Local",
              "canonicalModel": "m", "endpoint": "10.0.0.2:8000", "queuedPrompts": 0 },
            // Belongs to no set.
            { "backendName": "invented", "backendType": "Local",
              "canonicalModel": "m", "endpoint": "10.0.0.3:8000", "queuedPrompts": 0 },
        ]
    }))
    .unwrap();
    let st = policy_api::PolicyApiState::new(
        Some(store),
        pools,
        Arc::new(kinference_fleet::scrape::MetricsRegistry::new()),
    );
    let (status, body) =
        body_of(policy_api::simulate(State(st), policy_api::ApiJson(req)).await).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let c = body["effectiveCandidates"].as_array().unwrap();
    let ttft = |i: usize| c[i]["predictedTtftMs"].as_f64().unwrap();
    assert_ne!(
        ttft(0),
        ttft(1),
        "two sets with different block sizes must not predict identically: {body}"
    );
    assert_eq!(c[0]["predictionSource"], "predictor");
    assert_eq!(c[1]["predictionSource"], "predictor");

    // The borrowed case is reported rather than passed off as this worker's
    // own number.
    assert_eq!(c[2]["predictionSource"], "borrowed", "{body}");
    let note = c[2]["predictionNote"].as_str().unwrap();
    assert!(
        note.contains("no worker set named `invented`"),
        "a synthesized worker borrows a predictor and must say whose: {note}"
    );
}

/// `kvCacheUsage` filters end to end, including where it disagrees with
/// `queuedPrompts`.
#[tokio::test]
async fn kv_cache_usage_excludes_the_saturated_worker_the_queue_ranks_backwards() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy(
        "kv-guard",
        serde_json::json!({
            "rules": [{
                "name": "keep-off-full-gpus",
                "matches": [{}],
                "route": {
                    "consider": [{ "type": "Local" }],
                    "exclude": [{
                        "name": "require-kv-headroom",
                        "drop": { "backend": { "type": "Local" }, "kvCacheUsage": { "greaterThan": 0.9 } }
                    }],
                    "select": [{ "minimize": "PredictedTTFT" }],
                    "onEmpty": "Reject"
                }
            }]
        }),
    ));

    // `full` is saturated, yet has the shorter queue.
    let req = serde_json::from_value(serde_json::json!({
        "policy": "default/kv-guard",
        "requestedModel": "m",
        "inputTokens": 1024,
        "candidates": [
            { "backendName": "full", "backendType": "Local", "canonicalModel": "m",
              "endpoint": "10.0.0.1:8000", "queuedPrompts": 1.0, "kvCacheUsage": 0.93,
              "predictedTtftMs": 100.0 },
            { "backendName": "spiky", "backendType": "Local", "canonicalModel": "m",
              "endpoint": "10.0.0.2:8000", "queuedPrompts": 5.0, "kvCacheUsage": 0.31,
              "predictedTtftMs": 200.0 }
        ]
    }))
    .unwrap();
    let (status, body) = body_of(
        policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["verdict"], "chosen", "{body}");
    assert!(
        body["winner"].as_str().unwrap().contains("spiky"),
        "the 93%-KV worker must be excluded even though its queue is shorter: {body}"
    );

    let elim = &body["steps"][0]["trace"]["eliminated"][0];
    assert!(
        elim["candidate"].as_str().unwrap().contains("full"),
        "{elim}"
    );
    assert!(
        elim["why"]
            .as_str()
            .unwrap()
            .contains("require-kv-headroom"),
        "{elim}"
    );

    // Both signals are echoed.
    let c = &body["effectiveCandidates"][0];
    assert_eq!(c["kvCacheUsage"], 0.93, "{c}");
    assert_eq!(c["queuedPrompts"], 1.0, "{c}");
}

/// A backed-up local worker fails over to the provider.
///
/// `policy-3` has no `consider`, exclusions that shed a queued local endpoint,
/// and a second `select` tier of `type: Provider`. Each destination serves its
/// own build of the model under the alias that the client asked for, so the
/// policy considers the provider alongside the local set.
#[tokio::test]
async fn a_backed_up_local_worker_fails_over_to_the_provider() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy(
        "policy-3",
        serde_json::json!({
            "rules": [{
                "name": "local-first-with-provider-failover",
                "route": {
                    "exclude": [{
                        "name": "drop-backed-up-endpoints",
                        "drop": { "backend": {"type": "Local"}, "queuedPrompts": {"greaterThan": 4} }
                    }],
                    "select": [
                        {"type": "Local", "minimize": "PredictedTTFT"},
                        {"type": "Provider", "order": ["databricks"]}
                    ],
                    "onEmpty": "Reject"
                }
            }],
            "unmatched": "ServeLocal"
        }),
    ));

    let run = |queued: f64| {
        let store = store.clone();
        async move {
            let req = serde_json::from_value(serde_json::json!({
                "policy": "default/policy-3",
                // The client asks for the alias that both destinations declare.
                "requestedModel": "qwen-2.5",
                "inputTokens": 4096,
                "candidates": [
                    { "backendName": "qwen-2-5-32b", "backendType": "Local",
                      "canonicalModel": "qwen-2.5",
                      "upstreamModel": "Qwen/Qwen2.5-32B-Instruct-AWQ",
                      "endpoint": "10.0.1.55:8000", "queuedPrompts": queued,
                      "predictedTtftMs": 120.0 },
                    // The provider knows the model only by the ALIAS.
                    { "backendName": "databricks", "backendType": "Provider",
                      "canonicalModel": "qwen-2.5",
                      "upstreamModel": "Qwen/Qwen2.5-32B-Instruct-Turbo",
                      "observedTtftP95Ms": 300.0 }
                ]
            }))
            .unwrap();
            body_of(
                policy_api::simulate(State(api_state(Some(store))), policy_api::ApiJson(req)).await,
            )
            .await
        }
    };

    // Idle local worker: the first tier wins outright, provider untouched.
    let (_, idle) = run(0.0).await;
    assert_eq!(idle["verdict"], "chosen", "{idle}");
    assert!(
        idle["winner"].as_str().unwrap().contains("qwen-2-5-32b"),
        "local is tier 1 and must win while healthy: {idle}"
    );

    // Backed up: the exclusion sheds it, and tier 2 takes over.
    let (_, busy) = run(9.0).await;
    assert_eq!(
        busy["verdict"], "chosen",
        "the provider must be reachable, not `rejected_on_empty`: {busy}"
    );
    assert!(
        busy["winner"].as_str().unwrap().contains("databricks"),
        "failover must reach the provider that knows the model by its alias: {busy}"
    );

    // And the provider was admitted at consider.
    let admitted = busy["steps"][0]["consider"]["admitted"].as_array().unwrap();
    assert!(
        admitted
            .iter()
            .any(|a| a.as_str().unwrap().contains("databricks")),
        "the provider must survive `consider`: {admitted:?}"
    );
}

// ── The /api contract, through the real router ───────────────────────────────
//
// Through `router()`, to cover routing, JSON rejections, the body limit and
// method fallbacks.

use axum::body::Body;
use axum::http::{HeaderMap, Request as HttpRequest};
use tower::ServiceExt;

/// The whole admin router over `policy`, plus the Prometheus registry it
/// exposes, so a test can read `/metrics` the way a scraper does.
fn admin_router(
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
) -> (axum::Router, prometheus::Registry) {
    app_with(policy, PoolRegistry::new())
}

fn app_with(
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
    pools: Arc<PoolRegistry>,
) -> (axum::Router, prometheus::Registry) {
    let metrics = Arc::new(kinference_fleet::scrape::MetricsRegistry::new());
    let registry = metrics::registry();
    let state = AppState {
        worker_sets_ready: tokio::sync::watch::channel(true).1,
        pools: pools.clone(),
        policy: policy.clone(),
        registry: registry.clone(),
        gauges: FleetGauges::new(pools.clone(), metrics.clone(), &registry),
        policy_api: policy_api::PolicyApiState::new(policy, pools, metrics),
    };
    (router(state), registry)
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: impl Into<Body>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut req = HttpRequest::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    let resp = app
        .clone()
        .oneshot(req.body(body.into()).unwrap())
        .await
        .expect("the router is infallible");
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), 4 << 20)
        .await
        .unwrap();
    (status, headers, bytes.to_vec())
}

/// POST a JSON body to simulate; the response, parsed.
async fn post_sim(
    app: &axum::Router,
    body: serde_json::Value,
) -> (StatusCode, HeaderMap, serde_json::Value) {
    let (s, h, b) = call(
        app,
        "POST",
        "/api/policy/simulate",
        Some("application/json"),
        body.to_string(),
    )
    .await;
    let v = serde_json::from_slice(&b)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&b)));
    (s, h, v)
}

fn json_body(h: &HeaderMap, b: &[u8]) -> serde_json::Value {
    assert!(
        h["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json"),
        "every /api response is JSON: {h:?}"
    );
    serde_json::from_slice(b).unwrap()
}

fn listed_store() -> Arc<kinference_policy::store::PolicyStore> {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy("policy-5", policy_5()));
    store
}

/// A typo'd endpoint is a JSON 404 that names the real endpoints.
#[tokio::test]
async fn an_unknown_api_path_is_a_json_404_listing_the_endpoints() {
    let (app, _) = admin_router(Some(listed_store()));
    for path in [
        "/api/policy/simulat",
        "/api/policy/context/",
        "/api",
        "/api/v1/things",
    ] {
        let (s, h, b) = call(&app, "GET", path, None, Body::empty()).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{path}");
        let v = json_body(&h, &b);
        assert_eq!(v["error"]["code"], "NotFound", "{path}: {v}");
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap()
                .contains("POST /api/policy/simulate"),
            "{path}: {v}"
        );
    }
    // Nothing is served outside `/api`, `/metrics` and `/readyz`: the root and
    // any other path get the same JSON 404.
    for path in ["/", "/some/other/route"] {
        let (s, h, b) = call(&app, "GET", path, None, Body::empty()).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(json_body(&h, &b)["error"]["code"], "NotFound", "{path}");
    }
}

#[tokio::test]
async fn a_wrong_method_is_a_json_405_naming_the_right_one() {
    let (app, _) = admin_router(Some(listed_store()));
    let (s, h, b) = call(&app, "GET", "/api/policy/simulate", None, Body::empty()).await;
    assert_eq!(s, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(h["allow"], "POST");
    assert_eq!(json_body(&h, &b)["error"]["code"], "MethodNotAllowed");

    let (s, h, b) = call(&app, "POST", "/api/policy/context", None, Body::empty()).await;
    assert_eq!(s, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(h["allow"], "GET, HEAD");
    assert_eq!(json_body(&h, &b)["error"]["code"], "MethodNotAllowed");
}

#[tokio::test]
async fn body_problems_come_back_in_the_envelope() {
    let (app, _) = admin_router(Some(listed_store()));

    let (s, h, b) = call(
        &app,
        "POST",
        "/api/policy/simulate",
        Some("text/plain"),
        "{}",
    )
    .await;
    assert_eq!(s, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(json_body(&h, &b)["error"]["code"], "UnsupportedMediaType");

    let huge = format!(
        r#"{{"policy": "default/policy-5", "tenant": "{}"}}"#,
        "x".repeat(300 * 1024)
    );
    let (s, h, b) = call(
        &app,
        "POST",
        "/api/policy/simulate",
        Some("application/json"),
        huge,
    )
    .await;
    assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(json_body(&h, &b)["error"]["code"], "PayloadTooLarge");

    let (s, h, b) = call(
        &app,
        "POST",
        "/api/policy/simulate",
        Some("application/json"),
        "{not json",
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&h, &b)["error"]["code"], "InvalidRequest");
}

/// A misspelled field is rejected, by name.
#[tokio::test]
async fn an_unknown_request_field_is_rejected_by_name() {
    let (app, _) = admin_router(Some(listed_store()));
    for (body, field) in [
        (
            serde_json::json!({"policy": "default/policy-5", "inputToken": 4096}),
            "inputToken",
        ),
        (
            serde_json::json!({"policy": "default/policy-5", "candidates": [
                {"backendName": "a", "backendType": "Local", "canonicalModel": "m", "queuedPrompt": 3}
            ]}),
            "queuedPrompt",
        ),
    ] {
        let (s, _, v) = post_sim(&app, body).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(v["error"]["code"], "InvalidRequest");
        assert!(
            v["error"]["message"].as_str().unwrap().contains(field),
            "{v}"
        );
    }
}

/// Values outside their domain are rejected with the field that holds them.
#[tokio::test]
async fn out_of_range_values_name_their_field() {
    let (app, _) = admin_router(Some(listed_store()));
    let cand = |extra: serde_json::Value| {
        let mut c =
            serde_json::json!({"backendName": "a", "backendType": "Local", "canonicalModel": "m"});
        c.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        c
    };
    for (body, field, status) in [
        (
            serde_json::json!({"policy": "default/policy-5", "runs": 501}),
            "runs",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5", "runs": 0}),
            "runs",
            400,
        ),
        (serde_json::json!({"policy": "policy-5"}), "policy", 400),
        (
            serde_json::json!({"policy": "default/policy-5", "qos": "realtime"}),
            "qos",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5", "inputTokens": -1}),
            "inputTokens",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5", "inputTokens": i64::MAX}),
            "inputTokens",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5", "requestedModel": "a", "model": ["b"]}),
            "model",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5",
                               "candidates": [cand(serde_json::json!({"backendType": "local"}))]}),
            "candidates[0].backendType",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5",
                               "candidates": [cand(serde_json::json!({"kvCacheUsage": 80}))]}),
            "candidates[0].kvCacheUsage",
            400,
        ),
        (
            serde_json::json!({"policy": "default/policy-5", "pool": "default/nope"}),
            "pool",
            404,
        ),
        (serde_json::json!({"policy": "default/nope"}), "policy", 404),
    ] {
        let (s, _, v) = post_sim(&app, body.clone()).await;
        assert_eq!(s.as_u16(), status, "{body} → {v}");
        assert_eq!(v["error"]["field"], field, "{body} → {v}");
    }
}

/// Saturated, the API says so and when to come back, instead of queueing.
#[tokio::test]
async fn simulate_is_busy_rather_than_queued_when_saturated() {
    let st = api_state(Some(listed_store()));
    let held = st
        .sim_permits
        .clone()
        .try_acquire_many_owned(st.sim_permits.available_permits() as u32)
        .unwrap();
    let req = serde_json::from_value(serde_json::json!({"policy": "default/policy-5"})).unwrap();
    let resp = policy_api::simulate(State(st.clone()), policy_api::ApiJson(req)).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(resp.headers().contains_key("retry-after"));
    let (_, v) = body_of(resp).await;
    assert_eq!(v["error"]["code"], "Busy");
    drop(held);
    let req = serde_json::from_value(serde_json::json!({"policy": "default/policy-5"})).unwrap();
    let resp = policy_api::simulate(State(st), policy_api::ApiJson(req)).await;
    assert_eq!(resp.status(), StatusCode::OK, "permits come back");
}

/// A fleet with one live gemma worker behind `inference-local`, attached to
/// `policy`, served through the full router.
fn gemma_fleet(policy: serde_json::Value) -> axum::Router {
    gemma_fleet_labelled(policy, &[])
}

/// [`gemma_fleet`], with labels on the worker set.
fn gemma_fleet_labelled(policy: serde_json::Value, labels: &[(&str, &str)]) -> axum::Router {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kinference_policy::crd::{
        InferencePolicy, InferencePolicySpec, InferencePool, InferencePoolSpec, InferenceWorkerSet,
        InferenceWorkerSetSpec,
    };
    let meta = |name: &str| ObjectMeta {
        name: Some(name.into()),
        namespace: Some("default".into()),
        ..Default::default()
    };
    let ws = Arc::new(InferenceWorkerSet {
        metadata: ObjectMeta {
            labels: Some(
                labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            ..meta("gemma-4-12b")
        },
        spec: serde_json::from_value::<InferenceWorkerSetSpec>(serde_json::json!({
            "serviceRef": { "name": "gemma-4-12b-vllm", "port": "http" },
            "profile": "default",
            "models": [{ "name": "google/gemma-4-12B-it", "aliases": ["gemma-4"] }],
        }))
        .unwrap(),
        status: None,
    });
    let ip = Arc::new(InferencePool {
        metadata: meta("inference-local"),
        spec: serde_json::from_value::<InferencePoolSpec>(serde_json::json!({
            "destinations": [{ "kind": "InferenceWorkerSet", "name": "gemma-4-12b" }],
            "policyRef": { "name": "p" },
        }))
        .unwrap(),
        status: None,
    });
    let pol = Arc::new(InferencePolicy {
        metadata: meta("p"),
        spec: serde_json::from_value::<InferencePolicySpec>(policy).unwrap(),
        status: None,
    });
    let live = pool("gemma-4-12b");
    let pool_id = live.id;
    let mut entry = ready_entry();
    entry.current_models = vec!["google/gemma-4-12B-it".into()];
    entry.model_name_hint = Some("google/gemma-4-12B-it".into());
    live.registry.register(1, entry);
    let pools = PoolRegistry::new();
    pools.publish(Arc::new(kinference_fleet::pool::PoolMap::from_pools(vec![
        live,
    ])));
    let mut by_key = std::collections::BTreeMap::new();
    by_key.insert(
        kinference_policy::PoolKey::namespaced("default", "gemma-4-12b"),
        pool_id,
    );
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(Arc::new(kinference_policy::resolve::resolve(
        &[ip],
        &[pol],
        &[ws],
        &[],
        &[],
        &[],
        &by_key,
        &std::collections::BTreeMap::new(),
    )));
    app_with(Some(store), pools).0
}

/// Read `context`, edit a seed, post it back: every key a seed carries is
/// one `simulate` accepts.
#[tokio::test]
async fn context_seeds_post_back_to_simulate_unchanged() {
    let app = gemma_fleet(serde_json::json!({
        "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    let (s, h, b) = call(&app, "GET", "/api/policy/context", None, Body::empty()).await;
    assert_eq!(s, StatusCode::OK);
    let ctx = json_body(&h, &b);
    let pool = &ctx["pools"][0];
    assert_eq!(
        pool["policyRef"],
        serde_json::json!({"name": "p", "resolved": true})
    );
    assert_eq!(pool["destinations"][0]["name"], "gemma-4-12b");
    assert_eq!(pool["destinations"][0]["type"], "Local");
    assert!(pool["destinations"][0]["blockSize"].is_u64(), "{pool}");
    assert_eq!(
        pool["destinations"][0]["declaredModels"],
        serde_json::json!(["gemma-4", "google/gemma-4-12B-it"])
    );
    let seeds = pool["candidates"].clone();
    assert!(seeds.as_array().unwrap().len() >= 2, "{seeds}");

    let (s, _, v) = post_sim(
        &app,
        serde_json::json!({
            "policy": "default/p",
            "pool": "default/inference-local",
            "requestedModel": "gemma-4",
            "inputTokens": 2048,
            "candidates": seeds,
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["verdict"], "chosen", "{v}");
    let c = &v["effectiveCandidates"][0];
    assert_eq!(c["predictionSource"], "predictor", "{v}");
    assert_eq!(c["readyEndpoints"], 1, "derived as production does: {v}");
    assert_eq!(c["prefixCacheHitRatio"], 0.0, "a cold prompt: {v}");
}

/// Derived signals (`prefixCacheHitRatio`, `backend.readyEndpoints`) reach
/// `drop:` predicates as they do in production.
#[tokio::test]
async fn derived_signals_reach_exclusions_as_in_production() {
    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy(
        "thin",
        serde_json::json!({"rules": [{"name": "r", "route": {
            "exclude": [{"name": "too-thin", "drop": {
                "backend": {"type": "Local", "readyEndpoints": {"lessThan": 2}}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
    ));
    let (app, _) = admin_router(Some(store));
    let local = |name: &str, ep: &str| {
        serde_json::json!({"backendName": name, "backendType": "Local", "canonicalModel": "m",
                           "endpoint": ep, "predictedTtftMs": 100.0})
    };
    let (s, _, v) = post_sim(
        &app,
        serde_json::json!({"policy": "default/thin", "model": ["m"], "candidates": [
            local("lonely", "10.0.0.1:8000"),
            local("paired", "10.0.0.2:8000"),
            local("paired", "10.0.0.3:8000"),
        ]}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let winner = v["winner"].as_str().unwrap();
    assert!(
        winner.starts_with("paired"),
        "the one-endpoint set is excluded: {v}"
    );

    let store = kinference_policy::store::PolicyStore::new();
    store.publish(index_with_policy(
        "warm",
        serde_json::json!({"rules": [{"name": "r", "route": {
            "exclude": [{"name": "cold", "drop": {
                "backend": {"type": "Local"}, "prefixCacheHitRatio": {"lessThan": 0.5}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
    ));
    let (app, _) = admin_router(Some(store));
    let (s, _, v) = post_sim(
        &app,
        serde_json::json!({"policy": "default/warm", "model": ["m"], "candidates": [
            {"backendName": "cold", "backendType": "Local", "canonicalModel": "m",
             "predictedTtftMs": 10.0, "prefixCacheHitRatio": 0.1},
            {"backendName": "warm", "backendType": "Local", "canonicalModel": "m",
             "predictedTtftMs": 90.0, "prefixCacheHitRatio": 0.9},
        ]}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v["winner"].as_str().unwrap().starts_with("warm"), "{v}");
}

/// The read-only guarantee: `/metrics` renders identically before and after a
/// burst of API calls, including a 500-run simulation.
#[tokio::test]
async fn the_policy_api_changes_no_metric() {
    let app = gemma_fleet(serde_json::json!({
        "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}],
                                         "tieBreak": ["Random"]}}]
    }));
    let scrape = |app: axum::Router| async move {
        let (s, _, b) = call(&app, "GET", "/metrics", None, Body::empty()).await;
        assert_eq!(s, StatusCode::OK);
        String::from_utf8(b).unwrap()
    };
    let before = scrape(app.clone()).await;
    assert_eq!(
        before,
        scrape(app.clone()).await,
        "control: a scrape changes nothing"
    );

    let (_, h, b) = call(&app, "GET", "/api/policy/context", None, Body::empty()).await;
    let seeds = json_body(&h, &b)["pools"][0]["candidates"].clone();
    for runs in [1, 500] {
        let (s, _, v) = post_sim(
            &app,
            serde_json::json!({"policy": "default/p", "pool": "default/inference-local",
                               "requestedModel": "gemma-4", "inputTokens": 512,
                               "runs": runs, "candidates": seeds}),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert_eq!(v["verdict"], "chosen", "the burst must really run: {v}");
    }
    let _ = call(&app, "GET", "/api/nope", None, Body::empty()).await;
    assert_eq!(
        before,
        scrape(app).await,
        "the policy API must not move a metric"
    );
}

/// A `select` tier that subsets by label sees the destination's labels in
/// simulation too, as in the router.
#[tokio::test]
async fn simulation_gives_candidates_their_destination_labels() {
    let app = gemma_fleet_labelled(
        serde_json::json!({"rules": [{"name": "r", "route": {"select": [
            {"backends": {"selector": {"matchLabels": {"accelerator": "l40s"}}},
             "minimize": "PredictedTTFT"}]}}]}),
        &[("accelerator", "l40s")],
    );
    let (_, h, b) = call(&app, "GET", "/api/policy/context", None, Body::empty()).await;
    let seeds = json_body(&h, &b)["pools"][0]["candidates"].clone();
    let (s, _, v) = post_sim(
        &app,
        serde_json::json!({"policy": "default/p", "pool": "default/inference-local",
                           "requestedModel": "gemma-4", "candidates": seeds}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        v["verdict"], "chosen",
        "the label tier must admit the set: {v}"
    );
}
