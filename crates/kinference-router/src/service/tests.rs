use super::*;
use crate::service::test_support::{Render, ServiceBuilder, TestService};
use std::time::Duration;

use kinference_fleet::pool::{PoolRegistry, test_pool};

fn score(endpoint: &str, queue_waiting: u32) -> WorkerScore {
    WorkerScore {
        name: endpoint.to_string(),
        endpoint: endpoint.to_string(),
        matched_blocks: 0,
        completion_rate_per_sec: 10.0,
        queue_waiting,
        queue_running: 0,
        queue_swapped: 0,
        kv_cache_usage: 0.0,
        block_size: 16,
        total_blocks: 4,
    }
}

fn group(pool: Arc<Pool>, scores: Vec<WorkerScore>) -> ScoredGroup {
    ScoredGroup {
        pool,
        model: "gemma".to_string(),
        scores,
        side: ScoreSideData {
            hashes_by_block_size: HashMap::new(),
            tokens: Vec::new(),
        },
    }
}

/// With two worker sets serving one model, the no-policy path chooses on
/// predicted wait, so the deeper queue loses even when scored first.
#[tokio::test]
async fn the_less_loaded_pool_wins() {
    let busy = group(test_pool("busy"), vec![score("10.0.0.1:8000", 64)]);
    let idle = group(test_pool("idle"), vec![score("10.0.0.2:8000", 0)]);
    let winner = best_group(vec![busy, idle], None).expect("a pool must win");
    assert_eq!(winner.pool.name, "idle");
}

/// A group is ranked by its BEST endpoint, not its average: a pool with one
/// free worker and three saturated ones is a fine place to send a request.
#[tokio::test]
async fn a_pool_is_ranked_by_its_best_endpoint() {
    let mixed = group(
        test_pool("mixed"),
        vec![
            score("10.0.0.1:8000", 512),
            score("10.0.0.2:8000", 0),
            score("10.0.0.3:8000", 512),
        ],
    );
    let steady = group(test_pool("steady"), vec![score("10.0.0.4:8000", 8)]);
    // `steady` goes first, so returning the head or ranking on the mean
    // would both pick it.
    let winner = best_group(vec![steady, mixed], None).expect("a pool must win");
    assert_eq!(winner.pool.name, "mixed");
}

/// A pool that scored no workers cannot win, and doesn't shadow one that did.
#[tokio::test]
async fn a_pool_with_no_scores_is_skipped() {
    let empty = group(test_pool("empty"), Vec::new());
    let live = group(test_pool("live"), vec![score("10.0.0.1:8000", 4)]);
    let winner = best_group(vec![empty, live], None).expect("a pool must win");
    assert_eq!(winner.pool.name, "live");
    assert!(best_group(vec![group(test_pool("empty"), Vec::new())], None).is_none());
}

/// Ties keep the first group, and `resolve_pools` yields pool-id order, so
/// replicas agree.
#[tokio::test]
async fn a_tie_keeps_the_first_group() {
    let first = group(test_pool("first"), vec![score("10.0.0.1:8000", 3)]);
    let second = group(test_pool("second"), vec![score("10.0.0.2:8000", 3)]);
    let winner = best_group(vec![first, second], None).expect("a pool must win");
    assert_eq!(winner.pool.name, "first");
}

/// The two degradations have distinct labels and messages.
#[test]
fn the_two_degradations_are_told_apart() {
    assert_ne!(
        Degraded::Tokenize.as_label(),
        Degraded::PrefixIndex.as_label()
    );
    assert_ne!(Degraded::Tokenize.reason(), Degraded::PrefixIndex.reason());
    // Labels become a Prometheus label value; keep them metric-shaped.
    for d in [Degraded::Tokenize, Degraded::PrefixIndex] {
        assert!(
            d.as_label()
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '_'),
            "{} is not a usable metric label",
            d.as_label()
        );
    }
}

// --- alias fallback -------------------------------------------------
//
// The model index only holds names engines report, so a declared alias is
// translated before the "nothing serves this model" check.

fn ws(aliases: &[(&str, &str)]) -> kinference_policy::resolve::ResolvedWorkerSet {
    kinference_policy::resolve::ResolvedWorkerSet {
        name: "set-a".into(),
        namespace: "default".into(),
        labels: std::collections::BTreeMap::new(),
        pool_id: kinference_policy::PoolId::from_raw(0),
        service_name: "svc".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures pass (canonical, upstream); the canonical becomes an
            // alias of the upstream.
            .map(|(n, u)| kinference_policy::crd::ModelAlias {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
            })
            .collect(),
    }
}

#[test]
fn an_alias_resolves_to_the_upstream_an_engine_actually_serves() {
    let d = [ws(&[("qwen-simple", "Qwen/Qwen2.5-32B-Instruct-AWQ")])];
    let got = first_served_upstream(&[&d[0]], "qwen-simple", |m| {
        m == "Qwen/Qwen2.5-32B-Instruct-AWQ"
    });
    assert_eq!(got.as_deref(), Some("Qwen/Qwen2.5-32B-Instruct-AWQ"));
}

/// A declared alias whose upstream nothing serves is not rewritten.
#[test]
fn an_alias_whose_upstream_is_unserved_does_not_resolve() {
    let d = [ws(&[("qwen-simple", "Qwen/Qwen2.5-32B-Instruct-AWQ")])];
    assert_eq!(
        first_served_upstream(&[&d[0]], "qwen-simple", |_| false),
        None
    );
}

/// The canonical name itself, which `upstreams_for` appends, is never
/// returned.
#[test]
fn the_canonical_name_is_never_returned_as_its_own_upstream() {
    let d = [ws(&[])];
    assert_eq!(
        first_served_upstream(&[&d[0]], "some-model", |_| true),
        None
    );
}

/// Two upstream models sharing one alias resolve to one fixed choice, not
/// candidates the policy ranks.
#[test]
fn several_upstreams_take_the_first_that_is_served() {
    let d = [ws(&[("bar", "llama-70B"), ("bar", "llama-70B-FP8")])];
    // Only the FP8 build is up, so the first declared one is skipped: first
    // served, not first declared.
    assert_eq!(
        first_served_upstream(&[&d[0]], "bar", |m| m == "llama-70B-FP8").as_deref(),
        Some("llama-70B-FP8")
    );
    // Both up: the earlier alias wins, deterministically.
    assert_eq!(
        first_served_upstream(&[&d[0]], "bar", |_| true).as_deref(),
        Some("llama-70B")
    );
}

/// Aliases live per worker set, so a pool spanning several sets searches all
/// of them.
#[test]
fn aliases_on_a_later_destination_are_found() {
    let d = [ws(&[]), ws(&[("short", "vendor/long-name")])];
    assert_eq!(
        first_served_upstream(&[&d[0], &d[1]], "short", |m| m == "vendor/long-name").as_deref(),
        Some("vendor/long-name")
    );
}

// --- provider candidates and the health gate ---------------------------

fn resolved_provider(
    name: &str,
    models: &[(&str, &str, Option<u32>)],
) -> kinference_policy::resolve::ResolvedProvider {
    kinference_policy::resolve::ResolvedProvider {
        uid: None,
        name: name.into(),
        namespace: "default".into(),
        labels: std::collections::BTreeMap::new(),
        backend_ref: kinference_policy::engine::ObjectKey::new("default", format!("{name}-be")),
        path: None,
        models: models
            .iter()
            // (canonical, upstream, ctx); the canonical becomes an alias of
            // the upstream.
            .map(|(n, u, ctx)| kinference_policy::crd::ProviderModel {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
                context_window: *ctx,
            })
            .collect(),
    }
}

/// Every per-endpoint signal is `None`, not a default.
#[test]
fn a_provider_candidate_carries_no_per_endpoint_signal() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "acme/llama-70b", Some(131072))]);
    let cands = provider_candidates_for(&health, &prov, "bar");
    assert_eq!(cands.len(), 1);
    let c = &cands[0];

    assert_eq!(c.canonical_model, "bar");
    // The vendor's own spelling.
    assert_eq!(c.upstream_model, "acme/llama-70b");
    assert_eq!(c.backend, "together");
    assert_eq!(
        c.backend_type(),
        kinference_policy::crd::BackendType::Provider
    );

    assert_eq!(c.attrs.predicted_ttft_ms, None);
    assert_eq!(c.attrs.prefix_cache_hit_ratio, None);
    assert_eq!(c.attrs.queued_prompts, None);
    assert_eq!(c.attrs.backend_ready_endpoints, None);
    // Declared rather than observed.
    assert_eq!(c.attrs.context_window, Some(131072));
}

/// The path is carried on the candidate, since `selection` only sees the
/// `CandidateTarget`.
#[test]
fn a_provider_candidate_carries_the_dispatch_path() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let mut prov = resolved_provider("together", &[("bar", "up", None)]);
    prov.path = Some("/serving-endpoints/chat/completions".into());

    let target = &provider_candidates_for(&health, &prov, "bar")[0].target;
    let kinference_policy::engine::CandidateTarget::Provider { path, .. } = target else {
        panic!("a provider candidate");
    };
    assert_eq!(path.as_deref(), Some("/serving-endpoints/chat/completions"));

    // A provider that declares none leaves the path alone.
    prov.path = None;
    let target = &provider_candidates_for(&health, &prov, "bar")[0].target;
    let kinference_policy::engine::CandidateTarget::Provider { path, .. } = target else {
        panic!("a provider candidate");
    };
    assert_eq!(*path, None);
}

#[test]
fn a_declaration_the_crd_would_reject_yields_no_candidate() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    // A canonical name nobody declared.
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    assert!(provider_candidates_for(&health, &prov, "other").is_empty());
}

#[test]
fn two_rows_under_one_name_are_two_candidates() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider(
        "together",
        &[("bar", "acme/v1", None), ("bar", "acme/v2", None)],
    );
    let cands = provider_candidates_for(&health, &prov, "bar");
    // Interchangeable upstream models become separate candidates.
    assert_eq!(cands.len(), 2);
}

/// A provider the caller keeps failing to reach stops being offered.
#[test]
fn a_suppressed_provider_produces_no_candidates_at_all() {
    use kinference_policy::provider_health::FailureKind;
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    let key = kinference_policy::engine::ObjectKey::new("default", "together");

    assert_eq!(provider_candidates_for(&health, &prov, "bar").len(), 1);

    for _ in 0..3 {
        health.record_failure(&key, FailureKind::Unresolved, "no such GatewayBackend");
    }
    assert!(
        provider_candidates_for(&health, &prov, "bar").is_empty(),
        "a provider the caller cannot dispatch to must not be a candidate"
    );

    // One success brings it back.
    health.record_success(&key, 200.0);
    assert_eq!(provider_candidates_for(&health, &prov, "bar").len(), 1);
}

#[test]
fn an_observed_provider_carries_its_p95_as_both_signal_and_objective() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    let key = kinference_policy::engine::ObjectKey::new("default", "together");
    health.record_success(&key, 840.0);

    let c = &provider_candidates_for(&health, &prov, "bar")[0];
    // Once as the `observedTTFTP95` a predicate compares against...
    assert_eq!(c.attrs.backend_observed_ttft_ms, Some(840.0));
    // ...and once as the value EstimatedTTFT ranks on, which must agree.
    assert_eq!(
        c.objective_ms(&kinference_policy::compile::Rank::MinimizeEstimatedTtft),
        Some(840.0)
    );
    // But never as PredictedTTFT, which is Local-only by construction.
    assert_eq!(
        c.objective_ms(&kinference_policy::compile::Rank::MinimizePredictedTtft),
        None
    );
}

/// `worker_score` maps every input to the field it names.
///
/// Five of its parameters are `u32`, so a transposition would type-check.
#[test]
fn worker_score_maps_each_input_to_its_own_field() {
    let entry = kinference_fleet::workers::WorkerEntry {
        worker_id_str: "w-7".into(),
        model_name_hint: None,
        current_models: vec![],
        http_url: None,
        block_size: 999, // must NOT be what lands in the score
        initial_sync_complete: true,
        kv_events_observed: true,
        score_addr: "10.0.0.7:8000".into(),
        engine: None,
        pod_name: None,
        namespace: None,
        node_name: None,
        node_ip: None,
        pod_ip: None,
        availability_zone: None,
        instance_type: None,
    };
    // Distinct values throughout, so any swap changes the assertion that fails.
    let sample = kinference_fleet::scrape::MetricsSample {
        completion_rate_per_sec: 1.5,
        waiting: 11,
        running: 22,
        swapped: 33,
        kv_cache_usage: 0.75,
        ..Default::default()
    };

    let s = worker_score(&entry, &sample, 16, 44, 55);

    assert_eq!(s.name, "w-7");
    assert_eq!(s.endpoint, "10.0.0.7:8000");
    assert_eq!(s.block_size, 16, "block_size comes from the argument");
    assert_eq!(s.matched_blocks, 44);
    assert_eq!(s.total_blocks, 55);
    assert_eq!(s.queue_waiting, 11);
    assert_eq!(s.queue_running, 22);
    assert_eq!(s.queue_swapped, 33);
    assert!((s.completion_rate_per_sec - 1.5).abs() < f32::EPSILON);
    assert!((s.kv_cache_usage - 0.75).abs() < f32::EPSILON);
}

// --- policy_select, end to end ----------------------------------------------
//
// Through the real service with live pools; the builder's fake `/render`
// tokenizes, so scoring yields a score per worker.

/// The scored group a pick starts with: `model` on `pool`, one score per
/// `(address, queued)`.
fn primary(pool: &Arc<Pool>, model: &str, workers: &[(&str, u32)]) -> ScoredGroup {
    ScoredGroup {
        pool: pool.clone(),
        model: model.into(),
        scores: workers
            .iter()
            .map(|(addr, queued)| crate::picker::WorkerScore {
                name: addr.to_string(),
                endpoint: addr.to_string(),
                matched_blocks: 0,
                completion_rate_per_sec: 0.0,
                queue_waiting: *queued,
                queue_running: 0,
                queue_swapped: 0,
                kv_cache_usage: 0.0,
                block_size: 16,
                total_blocks: 4,
            })
            .collect(),
        side: ScoreSideData {
            hashes_by_block_size: HashMap::new(),
            tokens: Vec::new(),
        },
    }
}

async fn select(
    svc: &KinferenceService,
    store: &Arc<kinference_policy::store::PolicyStore>,
    start: ScoredGroup,
    model: &str,
) -> Result<PolicySelection, PickAndRecordOutcome> {
    let pref = kinference_policy::PoolRef {
        namespace: "default".into(),
        name: "pool".into(),
        resource_version: String::new(),
    };
    let spare = Some(ScoreInput {
        input: Input::Messages(vec![serde_json::json!({"role": "user", "content": "hi"})]),
        chat_extras: ChatExtras::default(),
        raw_body: Some(b"{}".to_vec()),
    });
    let attrs = kinference_policy::predicate::RequestAttrs {
        model: vec![model.into()],
        requested_model: model.into(),
        ..Default::default()
    };
    svc.policy_select(&pref, store, Some(start), model, spare, attrs)
        .await
}

/// When every matching rule falls through, the unmatched route serves the
/// requested model, not a group an earlier rule scored (here, `gemma`).
#[tokio::test]
async fn a_fall_through_serves_the_requested_model_only() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .pool("gemma-ws", "gemma", &["10.0.0.2:8000"])
        .policy(
            serde_json::json!({"rules": [{"name": "try-gemma", "route": {
            "consider": [{"models": {"names": ["gemma"]}}],
            "exclude": [{"name": "not-gemma", "drop": {"backend": {"name": "gemma-ws"}}}],
            "select": [{"minimize": "PredictedTTFT"}],
            "onEmpty": "FallThrough"}}]}),
        )
        .build()
        .await;
    // A deep queue on qwen, so gemma would win any comparison it was let into.
    let start = primary(&t.pools[0], "qwen", &[("10.0.0.1:8000", 60)]);
    let Ok(PolicySelection::Local { group, endpoint }) =
        select(&t.svc, t.store(), start, "qwen").await
    else {
        panic!("expected a local pick");
    };
    assert_eq!(group.model, "qwen");
    assert_eq!(endpoint, "10.0.0.1:8000");
}

/// A worker never scraped has no `queuedPrompts`, so a `drop` on it can't
/// fire. The scraped idle worker beside it is dropped.
#[tokio::test]
async fn an_unscraped_worker_has_no_saturation_signals() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000", "10.0.0.2:8000"])
        .policy(
            serde_json::json!({"rules": [{"name": "busy-only", "route": {
            "exclude": [{"name": "idle", "drop": {
                "backend": {"type": "Local"}, "queuedPrompts": {"lessThan": 1}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
        )
        .build()
        .await;
    // Worker 2 was scraped and is idle; worker 1 never was.
    t.svc.scoring.scrape.record_sample_for_test(
        2,
        kinference_fleet::scrape::MetricsSample {
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );
    let start = primary(
        &t.pools[0],
        "qwen",
        &[("10.0.0.1:8000", 0), ("10.0.0.2:8000", 0)],
    );
    let Ok(PolicySelection::Local { endpoint, .. }) =
        select(&t.svc, t.store(), start, "qwen").await
    else {
        panic!("the unscraped worker must survive the exclusion");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
}

/// `policy_select` for `qwen` on worker set `qwen-ws`, which declares it with
/// aliases `q` and `qwen-latest`. The request's `model` contains all three
/// names, as when `pick_and_record` builds it.
async fn select_aliased(
    spec: serde_json::Value,
) -> (TestService, Result<PolicySelection, PickAndRecordOutcome>) {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000", "10.0.0.2:8000"])
        .alias("qwen-ws", "qwen", &["q", "qwen-latest"])
        .policy(spec)
        .build()
        .await;
    let pref = TestService::pool_ref();
    let mut names = vec!["qwen".to_string()];
    names.extend(t.svc.declared_aliases(Some(&pref), "qwen"));
    assert_eq!(names, ["qwen", "q", "qwen-latest"], "the fixture's aliases");
    let start = primary(
        &t.pools[0],
        "qwen",
        &[("10.0.0.1:8000", 0), ("10.0.0.2:8000", 5)],
    );
    let spare = Some(ScoreInput {
        input: Input::Messages(vec![serde_json::json!({"role": "user", "content": "hi"})]),
        chat_extras: ChatExtras::default(),
        raw_body: Some(b"{}".to_vec()),
    });
    let attrs = kinference_policy::predicate::RequestAttrs {
        model: names,
        requested_model: "qwen".into(),
        ..Default::default()
    };
    let out = t
        .svc
        .policy_select(&pref, t.store(), Some(start), "qwen", spare, attrs)
        .await;
    (t, out)
}

/// A model that has several names yields one candidate per endpoint, not one
/// per name, so the router records each endpoint's prediction once.
#[tokio::test]
async fn an_aliased_model_is_one_candidate_per_endpoint() {
    let (t, out) = select_aliased(serde_json::json!({"rules": []})).await;
    let Ok(PolicySelection::Local { endpoint, .. }) = out else {
        panic!("expected a local pick");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
    let metrics = t.metrics();
    for worker in ["10.0.0.1:8000", "10.0.0.2:8000"] {
        let line =
            format!(r#"kinference_predicted_ttft_ms_count{{model="qwen",worker="{worker}"}} 1"#);
        assert!(metrics.contains(&line), "missing `{line}`:\n{metrics}");
    }
}

/// A `drop` on one alias removes the model under every name.
#[tokio::test]
async fn dropping_an_alias_drops_the_model_under_every_name() {
    let (_, out) = select_aliased(serde_json::json!({"rules": [{"name": "no-q", "route": {
        "exclude": [{"name": "q", "drop": {"model": {"name": "q"}}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}))
    .await;
    assert!(
        !matches!(out, Ok(PolicySelection::Local { .. })),
        "every endpoint answers to `q`, so none is left to pick"
    );
}

/// A provider that declares a model under two names yields one candidate, so a
/// `drop` on either name removes the provider.
#[tokio::test]
async fn dropping_an_alias_drops_a_provider_model_under_every_name() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .provider(
            "cloud",
            serde_json::json!([{"name": "vendor/qwen", "aliases": ["qwen", "q"]}]),
        )
        .policy(
            serde_json::json!({"rules": [{"name": "cloud-but-not-q", "route": {
            "consider": [{"type": "Provider"}],
            "exclude": [{"name": "q", "drop": {"model": {"name": "q"}}}],
            "select": [{"order": ["cloud"]}]}}]}),
        )
        .build()
        .await;
    let start = primary(&t.pools[0], "qwen", &[("10.0.0.1:8000", 0)]);
    let attrs = kinference_policy::predicate::RequestAttrs {
        model: vec!["qwen".into(), "q".into()],
        requested_model: "qwen".into(),
        ..Default::default()
    };
    let out = t
        .svc
        .policy_select(
            &TestService::pool_ref(),
            t.store(),
            Some(start),
            "qwen",
            None,
            attrs,
        )
        .await;
    assert!(
        !matches!(out, Ok(PolicySelection::Provider(_))),
        "the provider's one candidate answers to `q`, so it is dropped"
    );
}

/// Two worker sets that share the alias `coding`: `glm-ws` serves `glm-up` as
/// `glm`, `coding` and `coding-high`; `qwen-ws` serves `qwen-up` as `qwen`,
/// `coding` and `coding-low`. The policy has no rules, so every request takes
/// the unmatched route. `busy` is the set that gets a deep queue.
async fn shared_alias_fleet(busy: &str) -> TestService {
    let t = ServiceBuilder::new()
        .pool("glm-ws", "glm-up", &["10.0.0.1:8000"])
        .alias("glm-ws", "glm-up", &["glm", "coding", "coding-high"])
        .pool("qwen-ws", "qwen-up", &["10.0.0.2:8000"])
        .alias("qwen-ws", "qwen-up", &["qwen", "coding", "coding-low"])
        .policy(serde_json::json!({"rules": []}))
        .build()
        .await;
    let busy_worker = match busy {
        "glm-ws" => 1,
        "qwen-ws" => 2,
        other => panic!("no worker set {other}"),
    };
    t.svc.scoring.scrape.record_sample_for_test(
        busy_worker,
        kinference_fleet::scrape::MetricsSample {
            waiting: 60,
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );
    t
}

/// Pick `model` in a [`shared_alias_fleet`]: the endpoint that the router
/// chose, and the model in the forwarded body.
async fn pick_by_name(t: &TestService, model: &str) -> (String, String) {
    let body = serde_json::json!({"model": model, "messages": [{"role": "user", "content": "hi"}]})
        .to_string();
    let out = t
        .pick(
            body.as_bytes(),
            1,
            Qos::Interactive,
            Some(&TestService::pool_ref()),
        )
        .await;
    let (endpoint, wire) = match out {
        PickAndRecordOutcome::Ok { endpoint, body, .. }
        | PickAndRecordOutcome::Degraded { endpoint, body, .. } => (endpoint, body),
        other => panic!("{model}: expected a local pick, got {other:?}"),
    };
    // Empty when the router left the body unchanged.
    let sent = if wire.is_empty() {
        model_in(body.as_bytes())
    } else {
        model_in(&wire)
    };
    (endpoint, sent)
}

/// When only one worker set declares a name, the router sends requests for that
/// name to that set alone, even when the other set is faster and shares a
/// different alias with it.
#[tokio::test]
async fn a_name_one_set_declares_is_served_only_by_that_set() {
    let glm = ("10.0.0.1:8000".to_string(), "glm-up".to_string());
    let qwen = ("10.0.0.2:8000".to_string(), "qwen-up".to_string());

    let qwen_faster = shared_alias_fleet("glm-ws").await;
    for name in ["coding-high", "glm", "glm-up"] {
        assert_eq!(pick_by_name(&qwen_faster, name).await, glm, "{name}");
    }
    let glm_faster = shared_alias_fleet("qwen-ws").await;
    for name in ["coding-low", "qwen", "qwen-up"] {
        assert_eq!(pick_by_name(&glm_faster, name).await, qwen, "{name}");
    }
}

/// When both sets declare a name, both are eligible: the faster set wins, and
/// the router rewrites the body to that set's model, whichever upstream the
/// alias resolved to first.
#[tokio::test]
async fn a_name_both_sets_declare_is_served_by_either() {
    assert_eq!(
        pick_by_name(&shared_alias_fleet("glm-ws").await, "coding").await,
        ("10.0.0.2:8000".to_string(), "qwen-up".to_string())
    );
    assert_eq!(
        pick_by_name(&shared_alias_fleet("qwen-ws").await, "coding").await,
        ("10.0.0.1:8000".to_string(), "glm-up".to_string())
    );
}

/// If a provider serves its own build of a model under the alias that the
/// client asked for, the router can still send requests to it, even though the
/// alias resolved to the local build before any rule ran.
#[tokio::test]
async fn an_alias_reaches_a_provider_serving_its_own_build() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen-awq", &["10.0.0.1:8000"])
        .alias("qwen-ws", "qwen-awq", &["qwen-2.5"])
        .provider(
            "cloud",
            serde_json::json!([{"name": "vendor/qwen-turbo", "aliases": ["qwen-2.5"]}]),
        )
        .policy(serde_json::json!({"rules": [{"name": "to-cloud", "route": {
            "consider": [{"type": "Provider"}],
            "select": [{"order": ["cloud"]}]}}]}))
        .build()
        .await;
    let body = br#"{"model":"qwen-2.5","messages":[{"role":"user","content":"hi"}]}"#;
    let out = t
        .pick(body, 1, Qos::Interactive, Some(&TestService::pool_ref()))
        .await;
    let PickAndRecordOutcome::Provider { pick, body, .. } = out else {
        panic!("expected the provider, got {out:?}");
    };
    assert_eq!(pick.upstream_model, "vendor/qwen-turbo");
    assert_eq!(model_in(&body), "vendor/qwen-turbo");
}

/// A service where `qwen` is served by worker set `qwen-ws` and by provider
/// `cloud`, whose row names it `vendor/qwen`, after two picks for tenant
/// `acme`: response 1 interactive, which is unmatched and stays local, and
/// response 2 batch, which a rule sends to the provider. Returns the two
/// outcomes in that order.
async fn local_and_cloud_picks() -> (TestService, [PickAndRecordOutcome; 2]) {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .provider(
            "cloud",
            serde_json::json!([{"name": "vendor/qwen", "aliases": ["qwen"]}]),
        )
        .policy(serde_json::json!({"rules": [{"name": "batch-to-cloud",
            "matches": [{"qos": ["batch"]}],
            "route": {"consider": [{"type": "Provider"}], "select": [{"order": ["cloud"]}]}}]}))
        .build()
        .await;
    let pool = TestService::pool_ref();

    let local = t.pick(BODY, 1, Qos::Interactive, Some(&pool)).await;
    let cloud = t.pick(BODY, 2, Qos::Batch, Some(&pool)).await;
    (t, [local, cloud])
}

/// Every pick that routes is counted by tenant and destination.
#[tokio::test]
async fn a_pick_is_counted_by_tenant_and_destination() {
    let (t, _) = local_and_cloud_picks().await;
    let out = t.metrics();
    for line in [
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
        r#"kinference_routed_requests_total{destination="cloud",tenant="default",type="Provider"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
}

/// The token families' `model` is the name each body was SENT with: the
/// worker's model on a local pick, the provider row's `name` on a provider pick.
/// Not the alias the client asked for, nor what the report says.
#[tokio::test]
async fn token_metrics_name_the_model_each_body_was_sent_with() {
    use crate::proto::picker_server::Picker;

    let (t, _) = local_and_cloud_picks().await;
    for (response_id, prompt_tokens) in [(1, 10), (2, 7)] {
        t.svc
            .report_final_token(tonic::Request::new(crate::proto::ReportFinalTokenRequest {
                response_id,
                model: "what-the-vendor-said".into(),
                prompt_tokens,
                completion_tokens: 1,
                ..Default::default()
            }))
            .await
            .expect("report accepted");
    }
    let out = t.metrics();
    for line in [
        r#"kinference_prompt_tokens_total{model="qwen",qos="interactive",tenant="default"} 10"#,
        r#"kinference_prompt_tokens_total{model="vendor/qwen",qos="batch",tenant="default"} 7"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(!out.contains("what-the-vendor-said"), "{out}");
}

// --- the pick path, end to end ----------------------------------------------

/// An OpenAI chat request for `qwen`.
const BODY: &[u8] = br#"{"model":"qwen","messages":[{"role":"user","content":"hi"}]}"#;

/// The samples of counter `family` in `out` that are not zero. Families are
/// registered with every label up front, so absence must be checked by value.
fn nonzero<'a>(out: &'a str, family: &str) -> Vec<&'a str> {
    out.lines()
        .filter(|l| l.starts_with(&format!("{family}{{")) && !l.ends_with(" 0"))
        .collect()
}

/// The `model` a body names.
fn model_in(body: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(body).expect("a JSON body")["model"]
        .as_str()
        .expect("a string model")
        .to_string()
}

/// A good pick names the worker and reserves its in-flight state.
#[tokio::test]
async fn a_good_pick_returns_the_worker_and_reserves_its_state() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;

    let out = t.pick(BODY, 7, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Ok {
        endpoint,
        response_id,
        body,
        model,
    } = out
    else {
        panic!("{out:?}");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
    assert_eq!(response_id, 7);
    assert!(body.is_empty(), "an unchanged body comes back empty");
    assert_eq!(model, "qwen");
    assert_eq!(t.render_calls(), 1);

    // What ReportFirstToken and ReportFinalToken will find.
    assert_eq!(t.svc.observations.pending.len(), 1);
    let meta = t
        .svc
        .observations
        .labels
        .take(7)
        .expect("labels for the final report");
    assert_eq!(meta.model, "qwen");
    assert_eq!(meta.qos, Qos::Interactive);
    assert_eq!(meta.tenant, "acme");
    assert!(!meta.streaming);
    assert!(meta.provider.is_none());
    // What the next pick on this pool sees.
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 1);
    let dispatches = t.pools[0].inflight_prefixes.dispatches_for("10.0.0.1:8000");
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        dispatches[0].total_at_dispatch, 2,
        "32 tokens in blocks of 16"
    );

    let out = t.metrics();
    for line in [
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
        r#"kinference_prefix_requested_blocks_total{model="qwen",qos="interactive"} 2"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(nonzero(&out, "kinference_score_degraded_total").is_empty());
    assert!(nonzero(&out, "kinference_policy_decisions_total").is_empty());
}

/// A streaming request comes back rewritten with `stream_options.include_usage`
/// even though its model did not change.
#[tokio::test]
async fn a_streaming_pick_returns_the_rewritten_body() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;
    let body = br#"{"model":"qwen","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

    let out = t.pick(body, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Ok { body, .. } = out else {
        panic!("{out:?}");
    };
    let rewritten: serde_json::Value = serde_json::from_slice(&body).expect("a JSON body");
    assert_eq!(rewritten["stream_options"]["include_usage"], true);
    assert_eq!(model_in(&body), "qwen");
    assert!(
        t.svc.observations.labels.take(1).expect("labels").streaming,
        "the labels remember that the request streams"
    );
}

/// A model nothing serves is refused without scoring.
#[tokio::test]
async fn an_unknown_model_yields_no_workers() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;
    let body = br#"{"model":"llama","messages":[{"role":"user","content":"hi"}]}"#;

    let out = t.pick(body, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::NoWorkers { model } = out else {
        panic!("{out:?}");
    };
    assert_eq!(model, "llama");
    assert_eq!(t.render_calls(), 0);
    assert_eq!(t.svc.observations.pending.len(), 0);
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 0);
    assert!(nonzero(&t.metrics(), "kinference_routed_requests_total").is_empty());
}

/// A body that is not an OpenAI chat request is refused with the reason, and
/// nothing is reserved.
#[tokio::test]
async fn a_malformed_body_is_a_bad_request() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;
    let cases: [(&[u8], &str); 4] = [
        (
            b"{not json",
            "body is not a valid OpenAI ChatCompletionRequest: ",
        ),
        (
            b"[1,2]",
            "body is not a valid OpenAI ChatCompletionRequest: expected a JSON object",
        ),
        (
            br#"{"messages":[]}"#,
            "request body has an empty or absent `model`",
        ),
        (
            br#"{"model":"","messages":[]}"#,
            "request body has an empty or absent `model`",
        ),
    ];
    for (body, expected) in cases {
        let out = t.pick(body, 1, Qos::Interactive, None).await;
        let PickAndRecordOutcome::BadRequest { reason, model } = out else {
            panic!("{out:?}");
        };
        assert!(reason.starts_with(expected), "{reason}");
        assert_eq!(model, "");
    }
    assert_eq!(t.render_calls(), 0);
    assert_eq!(t.svc.observations.pending.len(), 0);
}

/// A policy that rejects the request leaves no state behind, though a worker
/// was available and scored.
#[tokio::test]
async fn a_policy_rejection_leaves_no_state() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .policy(serde_json::json!({"unmatched": "Reject"}))
        .build()
        .await;

    let out = t
        .pick(BODY, 1, Qos::Interactive, Some(&TestService::pool_ref()))
        .await;
    let PickAndRecordOutcome::PolicyRejected { reason, model } = out else {
        panic!("{out:?}");
    };
    assert_eq!(reason, "no rule matched and unmatched=Reject");
    assert_eq!(model, "qwen");
    // Scoring precedes the policy, so the worker was tokenized against.
    assert_eq!(t.render_calls(), 1);
    assert_eq!(t.svc.observations.pending.len(), 0);
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 0);

    let out = t.metrics();
    assert!(out.contains(r#"kinference_policy_decisions_total{outcome="rejected_unmatched"} 1"#));
    assert!(nonzero(&out, "kinference_routed_requests_total").is_empty());
}

/// `auto` resolves to the one model the fleet serves, and the body is
/// rewritten to name it.
#[tokio::test]
async fn auto_resolves_to_the_only_model_and_rewrites_the_body() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;
    let body = br#"{"model":"auto","messages":[{"role":"user","content":"hi"}]}"#;

    let out = t.pick(body, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Ok { model, body, .. } = out else {
        panic!("{out:?}");
    };
    assert_eq!(model, "qwen");
    assert_eq!(model_in(&body), "qwen");
    let rewritten: serde_json::Value = serde_json::from_slice(&body).expect("a JSON body");
    assert_eq!(rewritten["messages"][0]["content"], "hi");
    assert_eq!(
        t.svc.observations.labels.take(1).expect("labels").model,
        "qwen"
    );
    assert!(t.metrics().contains(
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#
    ));
}

/// Before any model is known, `auto` has nothing to resolve to.
#[tokio::test]
async fn auto_with_no_models_yields_no_workers() {
    let t = ServiceBuilder::new().build().await;
    let body = br#"{"model":"auto","messages":[{"role":"user","content":"hi"}]}"#;

    let out = t.pick(body, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::NoWorkers { model } = out else {
        panic!("{out:?}");
    };
    assert_eq!(model, "auto");
}

/// A declared alias resolves to the upstream an engine serves, and the body is
/// rewritten to the upstream. Without the named pool there is no alias table.
#[tokio::test]
async fn an_alias_resolves_to_the_upstream_and_rewrites_the_body() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .alias("qwen-ws", "qwen", &["qwen-alias"])
        .policy(serde_json::json!({}))
        .build()
        .await;
    let body = br#"{"model":"qwen-alias","messages":[{"role":"user","content":"hi"}]}"#;

    let out = t
        .pick(body, 1, Qos::Interactive, Some(&TestService::pool_ref()))
        .await;
    let PickAndRecordOutcome::Ok {
        model,
        endpoint,
        body: rewritten,
        ..
    } = out
    else {
        panic!("{out:?}");
    };
    assert_eq!(model, "qwen");
    assert_eq!(endpoint, "10.0.0.1:8000");
    assert_eq!(model_in(&rewritten), "qwen");
    let out = t.metrics();
    assert!(out.contains(r#"kinference_policy_decisions_total{outcome="unmatched_served"} 1"#));
    assert!(out.contains(
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#
    ));

    let out = t.pick(body, 2, Qos::Interactive, None).await;
    let PickAndRecordOutcome::NoWorkers { model } = out else {
        panic!("{out:?}");
    };
    assert_eq!(model, "qwen-alias");
}

/// A pick whose tokenize failed: chosen on load alone, with the same state
/// reserved as a good pick, minus the prefix blocks.
async fn degraded_pick(render: Render) {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .render(render)
        .build()
        .await;

    let out = t.pick(BODY, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Degraded {
        endpoint,
        response_id,
        reason,
        body,
        model,
    } = out
    else {
        panic!("{out:?}");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
    assert_eq!(response_id, 1);
    assert_eq!(reason, "tokenize unavailable across the fleet");
    assert!(body.is_empty());
    assert_eq!(model, "qwen");

    assert_eq!(t.svc.observations.pending.len(), 1);
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 1);
    let dispatches = t.pools[0].inflight_prefixes.dispatches_for("10.0.0.1:8000");
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        dispatches[0].total_at_dispatch, 0,
        "no tokens, so no blocks"
    );

    let out = t.metrics();
    for line in [
        r#"kinference_score_degraded_total{reason="tokenize"} 1"#,
        r#"kinference_tokenize_total{path="unavailable"} 1"#,
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(nonzero(&out, "kinference_prefix_requested_blocks_total").is_empty());
}

/// `/render` answering with an error degrades the pick.
#[tokio::test]
async fn a_failing_render_degrades_to_a_load_only_pick() {
    degraded_pick(Render::Failing).await;
}

/// No worker to tokenize against degrades the pick the same way.
#[tokio::test]
async fn no_render_endpoint_degrades_to_a_load_only_pick() {
    degraded_pick(Render::Absent).await;
}

/// The rule for batch sends the request to the provider, with the body naming
/// the vendor's model and no pool state touched. The interactive request
/// before it stayed local.
#[tokio::test]
async fn a_batch_request_fails_over_to_the_provider() {
    let (t, [local, cloud]) = local_and_cloud_picks().await;
    assert!(
        matches!(local, PickAndRecordOutcome::Ok { .. }),
        "{local:?}"
    );
    let PickAndRecordOutcome::Provider {
        pick,
        response_id,
        body,
    } = cloud
    else {
        panic!("{cloud:?}");
    };
    assert_eq!(pick.provider.name, "cloud");
    assert_eq!(pick.upstream_model, "vendor/qwen");
    assert_eq!(pick.canonical_model, "qwen");
    assert_eq!(response_id, 2);
    assert_eq!(model_in(&body), "vendor/qwen");

    // Only the local pick reserved pool state.
    assert_eq!(t.svc.observations.pending.len(), 1);
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 1);
    let labels = t
        .svc
        .observations
        .labels
        .provider_pick_for(2)
        .expect("provider labels for the reports");
    assert_eq!(labels.provider.name, "cloud");
    assert_eq!(labels.model, "vendor/qwen");

    let out = t.metrics();
    for line in [
        r#"kinference_routed_requests_total{destination="cloud",tenant="default",type="Provider"} 1"#,
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
        r#"kinference_policy_decisions_total{outcome="chosen"} 1"#,
        r#"kinference_policy_decisions_total{outcome="unmatched_served"} 1"#,
        r#"kinference_policy_rule_matches_total{rule="batch-to-cloud"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
}

/// A named pool the snapshot does not hold routes on load alone.
#[tokio::test]
async fn a_named_pool_the_snapshot_lacks_fails_open() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;

    let out = t
        .pick(BODY, 1, Qos::Interactive, Some(&TestService::pool_ref()))
        .await;
    assert!(matches!(out, PickAndRecordOutcome::Ok { .. }), "{out:?}");
    let out = t.metrics();
    assert!(out.contains(r#"kinference_policy_decisions_total{outcome="unknown_pool"} 1"#));
    assert!(out.contains(
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#
    ));
}

/// With policy off, a named pool is ignored and no decision is recorded.
#[tokio::test]
async fn with_policy_off_a_named_pool_is_ignored() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .without_policy()
        .build()
        .await;

    let out = t
        .pick(BODY, 1, Qos::Interactive, Some(&TestService::pool_ref()))
        .await;
    assert!(matches!(out, PickAndRecordOutcome::Ok { .. }), "{out:?}");
    assert!(nonzero(&t.metrics(), "kinference_policy_decisions_total").is_empty());
}

/// A worker that has not finished its initial sync is not picked, though its
/// pool claims the model.
#[tokio::test]
async fn a_worker_still_syncing_is_not_picked() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .still_syncing()
        .build()
        .await;

    let out = t.pick(BODY, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::NoWorkers { model } = out else {
        panic!("{out:?}");
    };
    assert_eq!(model, "qwen");
    assert_eq!(t.render_calls(), 0);
}

/// Two worker sets serving one model: the pick goes to the one with the
/// shorter predicted wait, tokenizing once for both.
#[tokio::test]
async fn two_pools_serving_one_model_pick_the_less_loaded() {
    let t = ServiceBuilder::new()
        .pool("busy-ws", "qwen", &["10.0.0.1:8000"])
        .pool("idle-ws", "qwen", &["10.0.0.2:8000"])
        .build()
        .await;
    // Worker 1 was scraped with a deep queue; worker 2 reads as idle.
    t.svc.scoring.scrape.record_sample_for_test(
        1,
        kinference_fleet::scrape::MetricsSample {
            waiting: 64,
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );

    let out = t.pick(BODY, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Ok { endpoint, .. } = out else {
        panic!("{out:?}");
    };
    assert_eq!(endpoint, "10.0.0.2:8000");
    assert_eq!(t.render_calls(), 1, "the second pool reuses the tokens");
    assert!(t.metrics().contains(
        r#"kinference_routed_requests_total{destination="idle-ws",tenant="default",type="Local"} 1"#
    ));
}

/// `start` builds the service from empty handles, and `grpc::server`
/// serves it.
#[tokio::test]
async fn start_builds_from_empty_handles() {
    let registry = prometheus::Registry::new();
    let config = RouterConfig {
        tokenize: TokenizeConfig::default(),
        metrics: MetricsConfig::default(),
        bodies: BodyLimits {
            max_total_bytes: 1024,
            max_request_bytes: 512,
            ttl: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(1),
        },
        observation_ttl: Duration::from_secs(1),
        observations_csv: None,
    };
    let fleet = FleetHandles {
        pools: PoolRegistry::new(),
        models: Arc::new(ModelIndex::new()),
        scrape: Arc::new(MetricsRegistry::new()),
    };
    let policy = PolicyHandles {
        store: None,
        provider_health: Arc::new(ProviderHealthStore::new()),
    };
    let service =
        KinferenceService::start(config, fleet, policy, &registry, CancellationToken::new())
            .expect("a service");
    assert!(service.policy.is_none());
    let _server = crate::grpc::server(service);
}
