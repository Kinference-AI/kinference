//! The policy API: `GET /api/policy/context` and `POST /api/policy/simulate`.
//!
//! The listener is plaintext and unauthenticated, so:
//!
//! - Read-only. Both handlers evaluate against an immutable snapshot and change
//!   nothing (no metric, counter, cache or id allocation). [`PolicyApiState`]
//!   holds only snapshot readers, and `the_policy_api_changes_no_metric`
//!   checks `/metrics` is unchanged by a burst of calls.
//! - Bounded. Bodies, lists and `runs` are capped, and simulation runs on the
//!   blocking pool behind a small semaphore, never on the runtime serving
//!   `Pick`. Excess simulations get `429 Busy`.
//! - JSON, always, errors included, in one envelope:
//!   `{"error": {"code", "message", "field"?}}`. Unknown request fields are
//!   rejected by name.
//!
//! The decision comes from [`kinference_policy::explain`]; this module seeds
//! candidates from live state, applies the caller's overrides, and projects
//! the result to JSON by hand, so API naming doesn't leak Rust field names and
//! the engine types needn't derive `Serialize`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    extract::{FromRef, FromRequest, Request, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use kinference_policy::PoolId;
use kinference_policy::crd::{OnEmpty, Unmatched};
use kinference_policy::engine::{Candidate, CandidateTarget, ObjectKey};
use kinference_policy::explain::{self, Explanation, RuleStep};
use kinference_policy::predicate::{CandidateAttrs, QOS_VALUES, RequestAttrs};
use kinference_policy::resolve::{PolicyIndex, ResolvedPool};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use kinference_fleet::pool::PoolRegistry;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_policy::store::PolicyStore;
use kinference_router::picker::{self, WorkerScore};

// ── limits ───────────────────────────────────────────────────────────────────

/// Largest request body any `/api` route accepts; a simulation is a few
/// kilobytes.
pub(super) const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_CANDIDATES: usize = 256;
const MAX_MODELS: usize = 32;
const MAX_TAGS: usize = 64;
const MAX_RUNS: u32 = 500;
/// Bounds token counts so the block arithmetic in [`predict`] can't overflow.
const MAX_TOKENS: i64 = 10_000_000;
/// Simulations that may run at once. Each is CPU-bound for up to `MAX_RUNS`
/// evaluations.
const SIM_CONCURRENCY: usize = 4;

/// The endpoints that exist, for the 404 body.
const ENDPOINTS: &[(&str, &str)] = &[
    ("GET", "/api/policy/context"),
    ("POST", "/api/policy/simulate"),
];

// ── the error envelope ───────────────────────────────────────────────────────

/// Every `/api` failure, as `{"error": {"code", "message", "field"?}}`.
///
/// `code` is stable and meant to be matched on; `message` is for a person;
/// `field`, when present, names the request field at fault, in the request's
/// own spelling (`candidates[2].backendType`).
#[derive(Debug)]
pub(super) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    field: Option<String>,
    retry_after_secs: Option<u64>,
    allow: Option<&'static str>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            field: None,
            retry_after_secs: None,
            allow: None,
        }
    }

    fn invalid(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            ..Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
        }
    }

    fn not_found(field: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            field: field.map(str::to_string),
            ..Self::new(StatusCode::NOT_FOUND, "NotFound", message)
        }
    }

    /// Fold axum's plain-text JSON rejections into the envelope, all as
    /// `InvalidRequest`.
    fn from_rejection(rej: JsonRejection) -> Self {
        let message = rej.body_text();
        match rej.status() {
            StatusCode::UNSUPPORTED_MEDIA_TYPE => Self::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UnsupportedMediaType",
                "send the body as JSON, with `Content-Type: application/json`",
            ),
            StatusCode::PAYLOAD_TOO_LARGE => Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "PayloadTooLarge",
                format!("request body exceeds {MAX_BODY_BYTES} bytes"),
            ),
            _ => Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut error = json!({"code": self.code, "message": self.message});
        if let Some(f) = self.field {
            error["field"] = json!(f);
        }
        let mut resp = (self.status, Json(json!({ "error": error }))).into_response();
        if let Some(s) = self.retry_after_secs {
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(s));
        }
        if let Some(a) = self.allow {
            resp.headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static(a));
        }
        resp
    }
}

/// `Json<T>`, but a rejection comes back in the envelope rather than as
/// axum's plain text.
pub(super) struct ApiJson<T>(pub(super) T);

#[axum::async_trait]
impl<T, S> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(ApiJson(v)),
            Err(rej) => Err(ApiError::from_rejection(rej)),
        }
    }
}

/// Any `/api` path that is not an endpoint. Lists the ones that are.
pub(super) async fn not_found(uri: Uri) -> ApiError {
    let known = ENDPOINTS
        .iter()
        .map(|(m, p)| format!("{m} {p}"))
        .collect::<Vec<_>>()
        .join(", ");
    ApiError::not_found(
        None,
        format!("no endpoint at {}. Available: {known}", uri.path()),
    )
}

/// The right path with the wrong method. `allow` lists the methods that work.
pub(super) fn method_not_allowed(allow: &'static str) -> ApiError {
    ApiError {
        allow: Some(allow),
        ..ApiError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            format!("this endpoint accepts {allow}"),
        )
    }
}

// ── state ────────────────────────────────────────────────────────────────────

/// What both handlers need, as one `State` extractor.
///
/// Snapshot readers only: no Prometheus registry, body store, inflight registry, or
/// HTTP or kube client, which makes the API read-only by construction. The
/// semaphore only bounds this API's own load.
#[derive(Clone)]
pub(super) struct PolicyApiState {
    pub(super) policy: Option<Arc<PolicyStore>>,
    pub(super) pools: Arc<PoolRegistry>,
    pub(super) metrics: Arc<MetricsRegistry>,
    pub(super) sim_permits: Arc<Semaphore>,
}

impl PolicyApiState {
    pub(super) fn new(
        policy: Option<Arc<PolicyStore>>,
        pools: Arc<PoolRegistry>,
        metrics: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            policy,
            pools,
            metrics,
            sim_permits: Arc::new(Semaphore::new(SIM_CONCURRENCY)),
        }
    }
}

impl FromRef<super::AppState> for PolicyApiState {
    fn from_ref(state: &super::AppState) -> Self {
        state.policy_api.clone()
    }
}

/// The snapshot, or why there is none: policy disabled, or not listed yet.
/// These are reported distinctly rather than as an empty list.
fn snapshot_or_reason(st: &PolicyApiState) -> Result<Arc<PolicyIndex>, ApiError> {
    let Some(store) = st.policy.as_ref() else {
        return Err(ApiError::new(
            StatusCode::NOT_IMPLEMENTED,
            "PolicyDisabled",
            "`policy.enabled = false`, so no InferencePolicy is watched. There is nothing to \
             evaluate.",
        ));
    };
    let snap = store.snapshot();
    if !snap.listed() {
        return Err(ApiError {
            retry_after_secs: Some(2),
            ..ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "NotListed",
                "the CRD watch has not completed its first list, so an empty policy set would \
                 mean `unknown`, not `none`. Retry shortly.",
            )
        });
    }
    Ok(snap)
}

/// `namespace/name`, or `None`.
fn split_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('/')
        .filter(|(ns, n)| !ns.is_empty() && !n.is_empty() && !n.contains('/'))
}

/// The CRD spelling of `spec.unmatched`.
fn unmatched_str(u: Unmatched) -> &'static str {
    match u {
        Unmatched::ServeLocal => "ServeLocal",
        Unmatched::Reject => "Reject",
    }
}

/// The CRD spelling of `onEmpty`.
fn on_empty_str(o: OnEmpty) -> &'static str {
    match o {
        OnEmpty::Reject => "Reject",
        OnEmpty::FallThrough => "FallThrough",
    }
}

// ── context ──────────────────────────────────────────────────────────────────

/// A candidate as the caller sees it: live values, editable, and postable back
/// to `simulate` verbatim as one of its `candidates`.
///
/// A struct rather than positional arguments, several of which share a type.
struct Seed<'a> {
    pool_key: &'a str,
    name: &'a str,
    backend_type: &'a str,
    canonical: &'a str,
    upstream: &'a str,
    endpoint: Option<&'a str>,
    /// The windowed mean, which is what a policy thresholds.
    queued: Option<f64>,
    kv_cache_usage: Option<f64>,
    context_window: Option<i64>,
}

fn seed_json(s: Seed<'_>) -> Value {
    json!({
        "pool": s.pool_key,
        "backendName": s.name,
        "backendType": s.backend_type,
        "canonicalModel": s.canonical,
        "upstreamModel": s.upstream,
        "endpoint": s.endpoint,
        "queuedPrompts": s.queued,
        "kvCacheUsage": s.kv_cache_usage,
        "contextWindow": s.context_window,
    })
}

/// Live candidate seeds for one pool, sorted.
///
/// Not the driver's candidate generation, which needs the tokenizer and the
/// radix index: just which destinations exist, their endpoints, and current
/// queue depth. `predictedTTFT` is computed at simulate time.
///
/// Sorted by pool, backend, canonical model, upstream model and endpoint, so
/// responses are stable across processes.
fn seeds_for_pool(st: &PolicyApiState, pool: &ResolvedPool) -> Vec<Value> {
    let key = format!("{}/{}", pool.namespace, pool.name);
    let mut out = Vec::new();
    let map = st.pools.snapshot();

    for dest in &pool.destinations {
        match dest.as_local() {
            Some(ws) => {
                let Some(p) = map.get(ws.pool_id) else {
                    continue;
                };
                for id in p.registry.ids() {
                    let entry = match p.registry.lookup(id) {
                        Some(e) => e,
                        None => continue,
                    };
                    // `None` until the first scrape, rather than zeros that
                    // would read as idle.
                    let s = st.metrics.sample(id);
                    let models = st.metrics.models(id);
                    // One seed per served model, as the driver produces.
                    let served: Vec<(String, Option<i64>)> = if models.models.is_empty() {
                        // No `/v1/models` refresh yet: fall back to what the
                        // registry knows, so a cold worker still appears.
                        entry
                            .current_models
                            .iter()
                            .cloned()
                            .map(|m| (m, None))
                            .collect()
                    } else {
                        models
                            .models
                            .iter()
                            .map(|m| (m.name.clone(), m.max_model_len.map(|v| v as i64)))
                            .collect()
                    };
                    for (upstream, ctx) in served {
                        if upstream.is_empty() {
                            continue;
                        }
                        // One candidate per client-facing name, as the driver
                        // produces: the upstream itself plus one per alias, so
                        // a `consider` naming an alias admits it.
                        for canonical in
                            std::iter::once(upstream.clone()).chain(ws.aliases_for(&upstream))
                        {
                            out.push(seed_json(Seed {
                                pool_key: &key,
                                name: &ws.name,
                                backend_type: "Local",
                                canonical: &canonical,
                                upstream: &upstream,
                                endpoint: entry.http_url.as_deref(),
                                queued: s.map(|s| s.waiting_mean as f64),
                                kv_cache_usage: s.map(|s| s.kv_cache_usage_mean as f64),
                                context_window: ctx,
                            }));
                        }
                    }
                }
            }
            None => {
                let Some(prov) = dest.as_provider() else {
                    continue;
                };
                for m in &prov.models {
                    let Some(upstream) = m.name.as_deref() else {
                        continue;
                    };
                    // The row's own name plus each alias.
                    for canonical in
                        std::iter::once(upstream).chain(m.aliases.iter().map(|a| a.as_str()))
                    {
                        out.push(seed_json(Seed {
                            pool_key: &key,
                            name: &prov.name,
                            backend_type: "Provider",
                            canonical,
                            upstream,
                            endpoint: None,
                            queued: None,
                            kv_cache_usage: None,
                            context_window: m.context_window.map(|v| v as i64),
                        }));
                    }
                }
            }
        }
    }
    let sort_key = |v: &Value| {
        [
            "pool",
            "backendName",
            "canonicalModel",
            "upstreamModel",
            "endpoint",
        ]
        .map(|k| v[k].as_str().unwrap_or_default().to_string())
    };
    out.sort_by_key(sort_key);
    out
}

/// The pool's destinations as declared, with what an author needs to write a
/// policy against them: the name a policy uses, the class, the labels a
/// selector matches, the models declared, and, for a local set, the KV block
/// size `matchedBlocks` is counted in.
fn destinations_json(st: &PolicyApiState, pool: &ResolvedPool) -> Vec<Value> {
    let map = st.pools.snapshot();
    let mut out: Vec<Value> = pool
        .destinations
        .iter()
        .map(|d| match d {
            kinference_policy::resolve::ResolvedDestination::Local(w) => {
                let mut models: Vec<&str> = w
                    .aliases
                    .iter()
                    .flat_map(|r| {
                        r.name
                            .as_deref()
                            .into_iter()
                            .chain(r.aliases.iter().map(String::as_str))
                    })
                    .collect();
                models.sort_unstable();
                models.dedup();
                json!({
                    "name": w.name,
                    "type": "Local",
                    "labels": w.labels,
                    "blockSize": map.get(w.pool_id).map(|p| p.block_size),
                    "declaredModels": models,
                })
            }
            kinference_policy::resolve::ResolvedDestination::Provider(p) => {
                let mut models: Vec<&str> = p
                    .models
                    .iter()
                    .flat_map(|r| {
                        r.name
                            .as_deref()
                            .into_iter()
                            .chain(r.aliases.iter().map(String::as_str))
                    })
                    .collect();
                models.sort_unstable();
                models.dedup();
                json!({
                    "name": p.name,
                    "type": "Provider",
                    "labels": p.labels,
                    "blockSize": Value::Null,
                    "declaredModels": models,
                })
            }
        })
        .collect();
    out.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    out
}

/// The policy a pool names, as `{name, resolved}`, or `null` when it names
/// none.
fn policy_ref_json(pool: &ResolvedPool) -> Value {
    use kinference_policy::resolve::DanglingPolicy;
    match &pool.dangling_policy {
        Some(DanglingPolicy::NotFound { name }) => {
            json!({"name": name, "resolved": false, "problem": "NotFound"})
        }
        Some(DanglingPolicy::WrongKind { name, .. }) => {
            json!({"name": name, "resolved": false, "problem": "WrongKind"})
        }
        None if pool.policy.name.starts_with('<') => Value::Null,
        None => json!({"name": pool.policy.name, "resolved": true}),
    }
}

/// `GET /api/policy/context`: everything a caller needs to write a simulation:
/// every policy with its findings, every pool with its destinations and live
/// candidates, and the closed `qos` vocabulary.
pub(super) async fn context(State(st): State<PolicyApiState>) -> Response {
    let snap = match snapshot_or_reason(&st) {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    tracing::debug!(
        policies = snap.policies().count(),
        "served the policy context"
    );

    let policies: Vec<Value> = snap
        .policies()
        .map(|p| {
            let status = kinference_controller::policy::status::desired_policy_status(p, None);
            let warnings: Vec<_> = p.all_warnings().collect();
            json!({
                "namespace": p.namespace,
                "name": p.name,
                "key": format!("{}/{}", p.namespace, p.name),
                // False means nothing routes through it.
                "attached": !p.referenced_by.is_empty(),
                "referencedBy": p.referenced_by,
                // The kubectl `state` column.
                "state": status.state,
                "inForce": !p.referenced_by.is_empty(),
                "hasWarnings": !warnings.is_empty(),
                // Compile and name warnings, untruncated, unlike
                // `status.warnings`. A policy with warnings still routes.
                "warnings": warnings,
                "unmatched": unmatched_str(p.policy.unmatched),
                "rules": p.policy.rules.iter().enumerate().map(|(i, r)| json!({
                    "index": i,
                    "name": r.name,
                    // Entries in `matches`, not requests matched. Zero means
                    // a catch-all.
                    "matchEntries": r.matches.len(),
                    "onEmpty": on_empty_str(r.route.on_empty),
                })).collect::<Vec<_>>(),
                "nondeterminism": explain::nondeterminism(&p.policy),
                // The spec as applied, not the compiled form.
                "spec": serde_json::to_value(p.spec.as_ref()).unwrap_or(Value::Null),
            })
        })
        .collect();

    // Sorted, since `PolicyIndex::pools()` iterates a `HashMap`.
    let mut by_key: Vec<&Arc<ResolvedPool>> = snap.pools().collect();
    by_key.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    let pools: Vec<Value> = by_key
        .into_iter()
        .map(|p| {
            json!({
                "key": format!("{}/{}", p.namespace, p.name),
                "name": p.name,
                "policyRef": policy_ref_json(p),
                "destinations": destinations_json(&st, p),
                "candidates": seeds_for_pool(&st, p),
            })
        })
        .collect();

    Json(json!({
        "state": "ok",
        "policies": policies,
        "pools": pools,
        "qos": QOS_VALUES,
    }))
    .into_response()
}

// ── simulate: request ────────────────────────────────────────────────────────

/// One candidate the caller wants in the simulation, with its overrides.
///
/// `deny_unknown_fields`, so a misspelled override is rejected rather than
/// ignored. Every field a `context` seed carries is accepted, so seeds can be
/// posted back unchanged.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CandidateOverride {
    /// Echo of the seed's pool. Must match the request's `pool` when both are
    /// set: candidates are simulated against one pool's predictor.
    #[serde(default)]
    pool: Option<String>,
    backend_name: String,
    /// `Local` or `Provider`, exactly.
    backend_type: String,
    canonical_model: String,
    #[serde(default)]
    upstream_model: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    /// Mean queued prompts over the window, so fractional.
    #[serde(default)]
    queued_prompts: Option<f64>,
    /// Mean KV-cache utilization, 0.0..1.0.
    #[serde(default)]
    kv_cache_usage: Option<f64>,
    #[serde(default)]
    context_window: Option<i64>,
    /// Override for the computed prediction, in milliseconds. When absent the
    /// predictor runs.
    #[serde(default)]
    predicted_ttft_ms: Option<f64>,
    /// Provider-only: the provider's observed TTFT p95, in milliseconds.
    #[serde(default, rename = "observedTtftP95Ms")]
    observed_ttft_p95_ms: Option<f64>,
    /// Cached prefix blocks, in the candidate's worker set's `blockSize`
    /// (`pools[].destinations[].blockSize` in `context`). Absent means a cold
    /// prompt.
    #[serde(default)]
    matched_blocks: Option<u32>,
    /// Override for `prefixCacheHitRatio`, 0.0..1.0. Derived from
    /// `matchedBlocks` and `inputTokens` when absent, as production does.
    #[serde(default)]
    prefix_cache_hit_ratio: Option<f64>,
    /// Override for `backend.readyEndpoints`. Derived from the number of local
    /// endpoints given for this backend when absent.
    #[serde(default)]
    ready_endpoints: Option<i64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SimRequest {
    /// `namespace/name` of the policy to evaluate.
    policy: String,
    /// `namespace/name` of the pool whose predictor and alias tables to use.
    #[serde(default)]
    pool: Option<String>,
    /// The name the client asked for. Expanded server-side into the alias set
    /// the engine would see. Preferred over `model`; set one or the other.
    #[serde(default)]
    requested_model: Option<String>,
    /// The raw `model` attribute set, for asserting on an exact set. The
    /// simulation takes its first entry as the name that the client asked for.
    #[serde(default)]
    model: Vec<String>,
    #[serde(default)]
    qos: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    input_tokens: Option<i64>,
    #[serde(default)]
    max_tokens: Option<i64>,
    #[serde(default)]
    candidates: Vec<CandidateOverride>,
    /// Repeat the evaluation and return a distribution of outcomes, 1..=500.
    /// Meaningful for a policy `nondeterminism` flags.
    #[serde(default)]
    runs: Option<u32>,
}

impl SimRequest {
    /// Everything that can be judged without the snapshot. Each failure names
    /// the field, so a caller fixes the request without guessing.
    fn validate(&self) -> Result<(), ApiError> {
        if split_key(&self.policy).is_none() {
            return Err(ApiError::invalid(
                "policy",
                "`policy` must be `namespace/name`",
            ));
        }
        if let Some(p) = &self.pool
            && split_key(p).is_none()
        {
            return Err(ApiError::invalid("pool", "`pool` must be `namespace/name`"));
        }
        if self.requested_model.is_some() && !self.model.is_empty() {
            return Err(ApiError::invalid(
                "model",
                "set `requestedModel` (expanded through the alias tables, like a real request) \
                 or `model` (the exact attribute set), not both",
            ));
        }
        if self.model.len() > MAX_MODELS {
            return Err(ApiError::invalid(
                "model",
                format!("at most {MAX_MODELS} names"),
            ));
        }
        if self.tags.len() > MAX_TAGS {
            return Err(ApiError::invalid(
                "tags",
                format!("at most {MAX_TAGS} tags"),
            ));
        }
        if self.candidates.len() > MAX_CANDIDATES {
            return Err(ApiError::invalid(
                "candidates",
                format!("at most {MAX_CANDIDATES} candidates"),
            ));
        }
        if let Some(q) = &self.qos
            && !QOS_VALUES.contains(&q.as_str())
        {
            return Err(ApiError::invalid(
                "qos",
                format!("unknown qos `{q}`; one of {QOS_VALUES:?}, or omit it"),
            ));
        }
        for (field, v) in [
            ("inputTokens", self.input_tokens),
            ("maxTokens", self.max_tokens),
        ] {
            if let Some(n) = v
                && !(0..=MAX_TOKENS).contains(&n)
            {
                return Err(ApiError::invalid(
                    field,
                    format!("must be 0..={MAX_TOKENS}"),
                ));
            }
        }
        if let Some(r) = self.runs
            && !(1..=MAX_RUNS).contains(&r)
        {
            return Err(ApiError::invalid("runs", format!("must be 1..={MAX_RUNS}")));
        }
        for (i, c) in self.candidates.iter().enumerate() {
            let at = |f: &str| format!("candidates[{i}].{f}");
            if c.backend_type != "Local" && c.backend_type != "Provider" {
                return Err(ApiError::invalid(
                    at("backendType"),
                    format!("`{}` is not `Local` or `Provider`", c.backend_type),
                ));
            }
            if let (Some(own), Some(req)) = (&c.pool, &self.pool)
                && own != req
            {
                return Err(ApiError::invalid(
                    at("pool"),
                    format!("`{own}` is not the request's pool `{req}`; one simulation, one pool"),
                ));
            }
            let fraction = |f: &str, v: Option<f64>| match v {
                Some(x) if !(0.0..=1.0).contains(&x) => {
                    Err(ApiError::invalid(at(f), "a 0–1 fraction (0.8 means 80%)"))
                }
                _ => Ok(()),
            };
            fraction("kvCacheUsage", c.kv_cache_usage)?;
            fraction("prefixCacheHitRatio", c.prefix_cache_hit_ratio)?;
            for (f, v) in [
                ("queuedPrompts", c.queued_prompts),
                ("predictedTtftMs", c.predicted_ttft_ms),
                ("observedTtftP95Ms", c.observed_ttft_p95_ms),
            ] {
                if v.is_some_and(|x| x < 0.0) {
                    return Err(ApiError::invalid(at(f), "must not be negative"));
                }
            }
            for (f, v) in [
                ("contextWindow", c.context_window),
                ("readyEndpoints", c.ready_endpoints),
            ] {
                if v.is_some_and(|x| x < 0) {
                    return Err(ApiError::invalid(at(f), "must not be negative"));
                }
            }
        }
        Ok(())
    }
}

// ── simulate: candidates ─────────────────────────────────────────────────────

/// Where a candidate's `predictedTtftMs` came from. A closed set a program can
/// branch on; the prose is in `predictionNote`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PredictionSource {
    /// The real predictor, with this candidate's own worker set's model.
    Predictor,
    /// The real predictor, with another set's model, since the candidate
    /// names no set in the pool.
    Borrowed,
    /// The caller supplied `predictedTtftMs`.
    Override,
    /// No prediction could be made. The candidate is still ranked, at 0.0 ms.
    Unavailable,
    /// A provider: no per-request prediction exists.
    NotApplicable,
}

impl PredictionSource {
    fn as_str(self) -> &'static str {
        match self {
            PredictionSource::Predictor => "predictor",
            PredictionSource::Borrowed => "borrowed",
            PredictionSource::Override => "override",
            PredictionSource::Unavailable => "unavailable",
            PredictionSource::NotApplicable => "notApplicable",
        }
    }
}

/// What the predictor said about one candidate, and the block arithmetic
/// `prefixCacheHitRatio` is derived from.
struct Prediction {
    ms: Option<f64>,
    source: PredictionSource,
    note: Option<String>,
    /// Blocks the prompt spans in this candidate's set, and how many of them
    /// the caller said are cached. `None` without a set to count in.
    blocks: Option<(u32, u32)>,
}

impl Prediction {
    fn bare(source: PredictionSource, note: Option<&str>) -> Self {
        Self {
            ms: None,
            source,
            note: note.map(str::to_string),
            blocks: None,
        }
    }
}

/// The single candidate constructor.
///
/// `predicted_ttft_ms`, `observed_ttft_ms` and the backend type each live in
/// both `target` (for ranking and tier filtering) and `attrs` (for
/// predicates); building candidates in one place keeps them agreeing.
fn build_candidate(
    o: &CandidateOverride,
    p: &Prediction,
    ready_endpoints: Option<i64>,
    labels: Arc<BTreeMap<String, String>>,
    pool_id: PoolId,
    input_tokens: Option<i64>,
) -> Candidate {
    let upstream = o
        .upstream_model
        .clone()
        .unwrap_or_else(|| o.canonical_model.clone());
    let is_local = o.backend_type == "Local";
    let ttft = p.ms.or(o.predicted_ttft_ms);
    // As `policy_driver` derives it: matched over total blocks, and 0.0 (not
    // absent) when the prompt spans no blocks.
    let hit_ratio = o.prefix_cache_hit_ratio.or(match p.blocks {
        Some((total, matched)) if total > 0 => Some(matched as f64 / total as f64),
        Some(_) => Some(0.0),
        None => None,
    });

    let target = if is_local {
        CandidateTarget::Local {
            pool_id,
            endpoint: o
                .endpoint
                .clone()
                .unwrap_or_else(|| "simulated".to_string()),
            score_idx: 0,
            predicted_ttft_ms: ttft.unwrap_or(0.0) as f32,
        }
    } else {
        CandidateTarget::Provider {
            provider: ObjectKey::new("default", &o.backend_name),
            backend_ref: ObjectKey::new("default", &o.backend_name),
            // Nothing is dispatched.
            path: None,
            observed_ttft_ms: o.observed_ttft_p95_ms.map(|v| v as f32),
        }
    };

    Candidate {
        canonical_model: o.canonical_model.clone(),
        upstream_model: upstream,
        backend: o.backend_name.clone(),
        labels,
        target,
        attrs: CandidateAttrs {
            context_window: o.context_window,
            // The simulated request's count, as if this candidate's render
            // agreed with it. Providers skip the fit check.
            input_tokens: if is_local { input_tokens } else { None },
            // Written from the same `ttft` the target got, never separately.
            predicted_ttft_ms: if is_local { ttft } else { None },
            prefix_cache_hit_ratio: if is_local { hit_ratio } else { None },
            queued_prompts: if is_local { o.queued_prompts } else { None },
            kv_cache_usage: if is_local { o.kv_cache_usage } else { None },
            model_names: vec![o.canonical_model.clone()],
            backend_name: Some(o.backend_name.clone()),
            backend_type: Some(if is_local { "Local" } else { "Provider" }.to_string()),
            backend_ready_endpoints: if is_local { ready_endpoints } else { None },
            backend_observed_ttft_ms: if is_local {
                None
            } else {
                o.observed_ttft_p95_ms
            },
            backend_budget_exhausted: None,
        },
    }
}

/// Every name a request for `requested` answers to.
///
/// As in production, `RequestAttrs::model` is the resolved upstream plus every
/// client-facing alias declared for it, resolved from the same tables.
fn model_attr_set(
    st: &PolicyApiState,
    pool: Option<&ResolvedPool>,
    requested: &str,
) -> Vec<String> {
    if requested.is_empty() {
        return Vec::new();
    }
    let refs: Vec<&kinference_policy::resolve::ResolvedWorkerSet> = pool
        .map(|p| p.local_destinations().collect())
        .unwrap_or_default();

    // `resolve_alias`'s ordering rule. When nothing is served, fall back to
    // the raw table so simulation works against a cold fleet.
    let map = st.pools.snapshot();
    let upstream = kinference_router::service::first_served_upstream(&refs, requested, |u| {
        map.iter().any(|p| {
            p.registry
                .iter_snapshot()
                .iter()
                .any(|(_, e)| e.current_models.iter().any(|m| m == u))
        })
    })
    .or_else(|| kinference_router::service::first_served_upstream(&refs, requested, |_| true))
    .unwrap_or_else(|| requested.to_string());

    // Upstream first, then aliases sorted, as `lifecycle.rs` builds it.
    let mut aliases: Vec<String> = refs
        .iter()
        .flat_map(|d| d.aliases_for(&upstream))
        .filter(|a| *a != upstream)
        .collect();
    aliases.sort();
    aliases.dedup();
    std::iter::once(upstream).chain(aliases).collect()
}

/// Run the real predictor for a hypothetical prompt on one candidate.
///
/// The source is reported because a candidate with no prediction is ranked at
/// 0.0 ms, the fastest possible.
///
/// The predictor is given no running batch, decode contention or KV pressure,
/// so it is optimistic compared to real picks under load.
fn predict(
    st: &PolicyApiState,
    pool: Option<&ResolvedPool>,
    o: &CandidateOverride,
    input_tokens: Option<i64>,
) -> Prediction {
    if o.backend_type == "Provider" {
        return Prediction::bare(
            PredictionSource::NotApplicable,
            Some("providers have no per-request prediction"),
        );
    }
    // This candidate's worker set's predictor and live pool, or why there is
    // none. Coefficients and block size differ per set. A synthesized worker
    // belongs to no set, so it borrows one, reported as `Borrowed`.
    let map = st.pools.snapshot();
    let found: Result<(Option<String>, Arc<kinference_fleet::pool::Pool>), &'static str> =
        match pool {
            None => Err("no pool selected, so no predictor to run; ranked as 0.0 ms"),
            Some(resolved) => {
                let own = resolved
                    .local_destinations()
                    .find(|d| d.name == o.backend_name);
                match own.map(|d| (d, None)).or_else(|| {
                    resolved
                        .local_destinations()
                        .next()
                        .map(|d| (d, Some(d.name.clone())))
                }) {
                    None => Err(
                        "the pool has no local worker set to borrow a predictor from; ranked as 0.0 ms",
                    ),
                    Some((ws, borrowed)) => map
                        .get(ws.pool_id)
                        .map(|live| (borrowed, live.clone()))
                        .ok_or("the worker set has no live pool yet; ranked as 0.0 ms"),
                }
            }
        };

    // Ceiling division: a partial block still has to be prefilled. `validate`
    // bounds the token count, so this can't overflow.
    let blocks = found.as_ref().ok().map(|(_, live)| {
        let bs = live.block_size.max(1) as i64;
        let tokens = input_tokens.unwrap_or(0).max(0);
        let total = ((tokens + bs - 1) / bs) as u32;
        (total, o.matched_blocks.unwrap_or(0).min(total))
    });

    // An explicit override wins. The block count still feeds the hit ratio.
    if o.predicted_ttft_ms.is_some() {
        return Prediction {
            ms: None,
            source: PredictionSource::Override,
            note: None,
            blocks,
        };
    }
    let (borrowed, live) = match found {
        Ok(f) => f,
        Err(why) => return Prediction::bare(PredictionSource::Unavailable, Some(why)),
    };
    let block_size = live.block_size.max(1);
    let (total_blocks, matched) = blocks.expect("a live pool yields a block count");

    let score = WorkerScore {
        name: o.backend_name.clone(),
        endpoint: o.endpoint.clone().unwrap_or_default(),
        matched_blocks: matched,
        completion_rate_per_sec: 0.0,
        // The predictor takes an instantaneous depth, so round the mean.
        queue_waiting: o.queued_prompts.unwrap_or(0.0).max(0.0).round() as u32,
        queue_running: 0,
        queue_swapped: 0,
        kv_cache_usage: 0.0,
        block_size,
        total_blocks,
    };
    // No inflight summaries for a hypothetical request.
    let ms = picker::predicted_wait_ms(&score, &live.predictor, &[], Instant::now()).total_ms();
    match borrowed {
        None => Prediction {
            ms: Some(ms as f64),
            source: PredictionSource::Predictor,
            note: None,
            blocks,
        },
        Some(from) => Prediction {
            ms: Some(ms as f64),
            source: PredictionSource::Borrowed,
            note: Some(format!(
                "no worker set named `{}` in the pool; predicted with `{from}`'s model",
                o.backend_name
            )),
            blocks,
        },
    }
}

fn step_json(s: &RuleStep) -> Value {
    json!({
        "index": if s.index == usize::MAX { Value::Null } else { json!(s.index) },
        "name": s.name,
        "matched": s.matched,
        "fellThrough": s.fell_through,
        "winner": s.winner,
        "tierRank": s.tier_rank,
        "tierSubsetDrops": s.tier_subset_drops.iter()
            .map(|(c, w)| json!({"candidate": c, "why": w})).collect::<Vec<_>>(),
        "predicates": s.predicates.iter().map(|p| json!({
            "matched": p.matched,
            "terms": p.terms.iter()
                .map(|t| json!({"attr": t.attr, "matched": t.matched})).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "consider": s.consider.as_ref().map(|c| json!({
            "weighted": c.weighted,
            "targets": c.targets.iter()
                .map(|(m, b)| json!({"model": m, "backends": b})).collect::<Vec<_>>(),
            "admitted": c.admitted,
            "rejected": c.rejected.iter()
                .map(|(cd, w)| json!({"candidate": cd, "why": w})).collect::<Vec<_>>(),
        })),
        "trace": s.trace.as_ref().map(|t| json!({
            "considered": t.considered,
            "eliminated": t.eliminated.iter()
                .map(|(c, w)| json!({"candidate": c, "why": w})).collect::<Vec<_>>(),
            "survivors": t.survivors,
            "tier": t.tier,
            "winnerObjectiveMs": t.winner_ttft_ms,
            "tied": t.tied,
        })),
    })
}

fn explanation_json(e: &Explanation) -> Value {
    json!({
        "verdict": e.verdict.as_label(),
        "winner": e.winner,
        "nondeterminism": e.nondeterminism,
        "steps": e.steps.iter().map(step_json).collect::<Vec<_>>(),
    })
}

// ── simulate: the handler ────────────────────────────────────────────────────

/// `POST /api/policy/simulate`
pub(super) async fn simulate(
    State(st): State<PolicyApiState>,
    ApiJson(req): ApiJson<SimRequest>,
) -> Response {
    match simulate_inner(st, req).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn simulate_inner(st: PolicyApiState, req: SimRequest) -> Result<Value, ApiError> {
    req.validate()?;
    let snap = snapshot_or_reason(&st)?;
    let (ns, name) = split_key(&req.policy).expect("validated");
    if !snap.policies().any(|p| p.namespace == ns && p.name == name) {
        return Err(ApiError::not_found(
            Some("policy"),
            format!("no InferencePolicy `{}`", req.policy),
        ));
    }
    if let Some(p) = &req.pool {
        let (pns, pname) = split_key(p).expect("validated");
        if snap.lookup(pns, pname).is_none() {
            return Err(ApiError::not_found(
                Some("pool"),
                format!("no InferencePool `{p}`"),
            ));
        }
    }

    // CPU-bound, so on the blocking pool, behind a semaphore. `try_acquire`,
    // so excess requests get 429 rather than queueing.
    let permit = st
        .sim_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError {
            retry_after_secs: Some(1),
            ..ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "Busy",
                format!("{SIM_CONCURRENCY} simulations are already running; retry shortly"),
            )
        })?;
    let started = Instant::now();
    let policy_key = req.policy.clone();
    let pool_key = req.pool.clone();
    let runs = req.runs.unwrap_or(1);
    let candidates = req.candidates.len();
    let body = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        run_simulation(&st, &snap, &req)
    })
    .await
    .map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal",
            format!("simulation failed: {e}"),
        )
    })?;
    // INFO, as an audit trail. Context reads are DEBUG, since they are polled.
    tracing::info!(
        policy = %policy_key,
        inference_pool = pool_key.as_deref().unwrap_or("-"),
        runs,
        candidates,
        verdict = body["verdict"].as_str().unwrap_or("?"),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "policy simulated"
    );
    Ok(body)
}

/// The simulation proper. Pure over its inputs.
fn run_simulation(st: &PolicyApiState, snap: &PolicyIndex, req: &SimRequest) -> Value {
    let (ns, name) = split_key(&req.policy).expect("validated");
    let resolved = snap
        .policies()
        .find(|p| p.namespace == ns && p.name == name)
        .expect("checked before spawning");
    let policy = resolved.policy.clone();
    let pool: Option<&ResolvedPool> = req
        .pool
        .as_deref()
        .and_then(split_key)
        .and_then(|(pns, pname)| snap.lookup(pns, pname))
        .map(Arc::as_ref);

    let attrs = RequestAttrs {
        model: match req.requested_model.as_deref() {
            Some(m) => model_attr_set(st, pool, m),
            None => req.model.clone(),
        },
        requested_model: req
            .requested_model
            .clone()
            .or_else(|| req.model.first().cloned())
            .unwrap_or_default(),
        input_tokens: req.input_tokens,
        max_tokens: req.max_tokens,
        tenant: req.tenant.clone(),
        qos: req.qos.clone(),
        tag: req.tags.clone(),
    };

    // `evaluate` never dereferences the pool id. A fixed placeholder, since
    // `PoolId::next()` would advance a process-wide counter.
    let sim_pool_id = pool
        .and_then(|p| p.local_destinations().next().map(|ws| ws.pool_id))
        .unwrap_or(PoolId::UNSCORED);

    // `backend.readyEndpoints`: the distinct local endpoints given per
    // backend.
    let mut endpoints: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for c in req.candidates.iter().filter(|c| c.backend_type == "Local") {
        endpoints
            .entry(c.backend_name.as_str())
            .or_default()
            .insert(c.endpoint.as_deref().unwrap_or("simulated"));
    }

    let built: Vec<(Candidate, Prediction)> = req
        .candidates
        .iter()
        .map(|o| {
            let p = predict(st, pool, o, req.input_tokens);
            let ready = o.ready_endpoints.or_else(|| {
                endpoints
                    .get(o.backend_name.as_str())
                    .map(|e| e.len() as i64)
            });
            // The destination's labels, for `select` tiers that subset by
            // label.
            let labels = pool
                .and_then(|p| p.backend(&o.backend_name))
                .map(|d| Arc::new(d.labels().clone()))
                .unwrap_or_default();
            (
                build_candidate(o, &p, ready, labels, sim_pool_id, req.input_tokens),
                p,
            )
        })
        .collect();
    let candidates: Vec<Candidate> = built.iter().map(|(c, _)| c.clone()).collect();

    // The candidate set as evaluated, since `Trace` carries no per-candidate
    // attributes.
    let effective: Vec<Value> = built
        .iter()
        .map(|(c, p)| {
            json!({
                "predictionSource": p.source.as_str(),
                "predictionNote": p.note,
                "label": c.label(),
                "backendName": c.backend,
                "backendType": c.backend_type().as_str(),
                "canonicalModel": c.canonical_model,
                "upstreamModel": c.upstream_model,
                "predictedTtftMs": c.attrs.predicted_ttft_ms,
                "observedTtftP95Ms": c.attrs.backend_observed_ttft_ms,
                "queuedPrompts": c.attrs.queued_prompts,
                "kvCacheUsage": c.attrs.kv_cache_usage,
                "prefixCacheHitRatio": c.attrs.prefix_cache_hit_ratio,
                "readyEndpoints": c.attrs.backend_ready_endpoints,
                "contextWindow": c.attrs.context_window,
            })
        })
        .collect();

    let first = explain::explain(&policy, &attrs, &candidates);

    // A nondeterministic policy can be run repeatedly for a distribution.
    let runs = req.runs.unwrap_or(1);
    let histogram = if runs > 1 {
        let mut counts: BTreeMap<String, u32> = BTreeMap::new();
        for _ in 0..runs {
            let e = explain::explain(&policy, &attrs, &candidates);
            let k = e
                .winner
                .clone()
                .unwrap_or_else(|| format!("<{}>", e.verdict.as_label()));
            *counts.entry(k).or_insert(0) += 1;
        }
        Some(json!({
            "runs": runs,
            "outcomes": counts.iter().map(|(k, v)| json!({"outcome": k, "count": v}))
                .collect::<Vec<_>>(),
        }))
    } else {
        None
    };

    let mut body = explanation_json(&first);
    body["policy"] = json!(req.policy);
    // Compile AND name warnings, as `context` and status report them.
    body["warnings"] = json!(resolved.all_warnings().collect::<Vec<_>>());
    body["effectiveCandidates"] = json!(effective);
    // What the requested name expanded to, which explains `model:` matches.
    body["resolvedModels"] = json!(attrs.model);
    body["distribution"] = histogram.unwrap_or(Value::Null);
    body
}
