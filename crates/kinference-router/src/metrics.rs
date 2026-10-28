//! The router's own Prometheus families: the request path, labeled by
//! `model` and `qos`, plus a `tenant` resolved per `metrics.tenant_labels`.
//! Registered in the registry the server lends.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

use prometheus::{
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, Registry, exponential_buckets,
};
use serde::Deserialize;

use kinference_fleet::metrics::{
    histogram, histogram_vec, int_counter, int_counter_vec, int_gauge,
};

/// `metrics`: labelling of the metrics kinference publishes.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsConfig {
    /// Which tenants get their own `tenant` label value. Off by default because
    /// tenants are caller-supplied and therefore unbounded.
    #[serde(default)]
    pub tenant_labels: TenantLabels,
}

/// The label value unattributed requests are bucketed under. A tenant literally
/// named `default` shares this series under an allowlist.
pub const DEFAULT_TENANT_LABEL: &str = "default";

/// Which tenants get their own `tenant` label value:
///
/// ```json
/// "tenant_labels": "none"              // default; everything buckets under "default"
/// "tenant_labels": "all"               // every tenant gets its own label
/// "tenant_labels": ["acme", "globex"]  // these keep their identity; the rest bucket
/// ```
///
/// `[]` is equivalent to `"none"`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TenantLabels {
    #[default]
    None,
    /// Unbounded cardinality: about 91 series per (model, qos, tenant).
    /// [`RouterMetrics::new`] warns at startup when set.
    All,
    Allow(Vec<String>),
}

// Hand-written instead of `#[serde(untagged)]` so a typo like `"sure"` gets an
// error naming what was expected, rather than untagged's generic message.
impl<'de> serde::Deserialize<'de> for TenantLabels {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = TenantLabels;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(r#""none", "all", or a list of tenant ids"#)
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    "none" => Ok(TenantLabels::None),
                    "all" => Ok(TenantLabels::All),
                    other => Err(E::invalid_value(serde::de::Unexpected::Str(other), &self)),
                }
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(s) = seq.next_element::<String>()? {
                    out.push(s);
                }
                Ok(if out.is_empty() {
                    TenantLabels::None
                } else {
                    TenantLabels::Allow(out)
                })
            }
        }
        d.deserialize_any(V)
    }
}

/// Inter-token-latency bucket layout version. MUST match
/// `ReportFinalTokenRequest.itl_bucket_layout` in `proto/kinference.proto`; bump
/// both together if the edges below change. Reports carrying a different
/// layout are skipped rather than mis-bucketed.
const ITL_BUCKET_LAYOUT: u32 = 1;

/// Prometheus `le` labels for ITL Layout 1, index-aligned to the client's
/// `itl_bucket_counts`. Upper edges (ms) `[5,10,15,20,30,40,50,75,100,150,
/// 200,300,500,1000]` plus `+Inf`. Must stay in sync with the proto's Layout 1.
const ITL_LE_LABELS: [&str; 15] = [
    "5", "10", "15", "20", "30", "40", "50", "75", "100", "150", "200", "300", "500", "1000",
    "+Inf",
];

// ── Closed label sets, pre-initialized at construction ────────────────────
//
// A `*Vec` child series doesn't exist until first touched, and `rate()` of an
// absent series returns nothing, so alerts never fire. These closed, small sets
// are seeded at zero (see `preinit`). Open-valued families like
// `{model,qos,tenant}` are not, to avoid inventing cardinality.

/// `kinference_picks_total{outcome}` and `kinference_pick_duration_ms{outcome}`.
/// Authoritative source: [`PickOutcome::as_label`].
const PICK_OUTCOMES: [&str; 3] = ["ok", "bad_request", "no_workers"];

/// `kinference_score_degraded_total{reason}`. Source: [`crate::service::Degraded::as_label`].
const DEGRADE_REASONS: [&str; 2] = ["tokenize", "prefix_index"];

/// `kinference_tokenize_total{path}`. Source: `crate::tokenizer`; exactly one
/// per tokenize attempt.
const TOKENIZE_PATHS: [&str; 3] = ["local", "http_fallback", "unavailable"];

/// `kinference_tokenize_compare_total{result}`. Source: `crate::tokenizer`.
/// Only moves in compare mode.
const TOKENIZE_COMPARE_RESULTS: [&str; 3] = ["match", "divergence", "error"];

/// `kinference_body_rejected_total{reason}`.
/// Source: [`crate::body_store::RejectReason::as_label`].
const BODY_REJECT_REASONS: [&str; 3] = ["declared_too_large", "request_too_large", "server_full"];

/// `kinference_body_aborted_total{reason}`. Source: `crate::grpc::abort_reason_label`.
const BODY_ABORT_REASONS: [&str; 5] = [
    "unspecified",
    "not_dispatched",
    "pull_failed",
    "backend_unresolved",
    "backend_unauthorized",
];

/// `kinference_policy_decisions_total{outcome}`.
///
/// No enum behind this one: call sites in `service::policy_driver` and
/// `service::lifecycle` pass string literals, so add new outcomes here too.
const POLICY_DECISION_OUTCOMES: [&str; 9] = [
    "chosen",
    "unmatched_served",
    "unmatched_empty",
    "rejected_unmatched",
    "rejected_on_empty",
    "unknown_pool",
    "not_listed",
    "no_destinations",
    "forced_endpoint_missing",
];

/// Outcome of one pick, as the `outcome` label on `kinference_picks_total` and
/// `kinference_pick_duration_ms`.
///
/// Distinct from the WIRE outcome (`pick_response::Outcome` in the proto):
/// `DEGRADED` counts as a success here, because a worker was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickOutcome {
    /// Picker returned a worker. `outcome=OK` or `DEGRADED` in the response.
    Success,
    /// The body did not parse, or carried nothing to route on.
    /// `outcome=BAD_REQUEST`.
    NoOpinion,
    /// No workers serve the requested model. `outcome=NO_WORKERS`.
    NotFound,
}

impl PickOutcome {
    /// Named for the WIRE outcome rather than the variant, since that is what
    /// an operator sees in a response and in the proto.
    pub fn as_label(self) -> &'static str {
        match self {
            PickOutcome::Success => "ok",
            PickOutcome::NoOpinion => "bad_request",
            PickOutcome::NotFound => "no_workers",
        }
    }
}

/// The router's metric families. Clone-free: share via `Arc`.
///
/// The per-request families are labeled by `model` and `qos`, plus a `tenant`
/// resolved through [`Self::tenant_label`].
pub struct RouterMetrics {
    /// How to label the `tenant` dimension. Resolved once at construction from
    /// `metrics.tenant_labels`.
    tenants: TenantPolicy,
    /// Whether an ITL report with the wrong bucket count has been logged.
    itl_length_warned: AtomicBool,
    /// `kinference_prompt_tokens_total{model,qos}`: prompt tokens, from ReportFinalToken.
    prompt_tokens: IntCounterVec,
    /// `kinference_completion_tokens_total{model,qos}`: completion tokens, from ReportFinalToken.
    completion_tokens: IntCounterVec,
    /// `kinference_cached_prompt_tokens_total{model,qos,tenant}`: prompt tokens
    /// the engine served from its prefix cache.
    cached_prompt_tokens: IntCounterVec,
    /// `kinference_cached_tokens_absent_total`: reports that omitted the field.
    cached_tokens_absent: IntCounter,
    /// `kinference_cached_tokens_reported_total`: reports that carried it.
    cached_tokens_reported: IntCounter,
    /// `kinference_orphaned_reports_total`: final-token reports kinference
    /// could not attribute to a pick.
    orphaned_reports: IntCounter,
    /// `kinference_prompt_length_tokens{model,qos}`: distribution of
    /// per-request prompt token counts.
    prompt_length: HistogramVec,
    /// `kinference_completion_length_tokens{model,qos}`: distribution of
    /// per-request completion token counts. Same buckets as `prompt_length`.
    completion_length: HistogramVec,
    /// `kinference_request_duration_ms{model,qos,tenant}`: pick to
    /// ReportFinalToken.
    request_duration_ms: HistogramVec,
    /// `kinference_ttft_ms{model,qos}`: time-to-first-token in milliseconds,
    /// from `ReportFirstToken`. Streaming requests only; for non-streaming the
    /// reported time is total latency.
    ttft_ms: HistogramVec,
    /// `kinference_provider_ttft_ms{provider,model}`: time-to-first-token for
    /// off-cluster picks, in milliseconds, from the caller's `ReportFirstToken`.
    /// A separate family because provider picks never reach
    /// `Observations::finalize`, where `record_ttft` is called.
    ///
    /// The router's own p95 (used by `EstimatedTTFT` and
    /// `backend.observedTTFTP95`) is not published; `histogram_quantile` over
    /// this approximates it.
    ///
    /// Successful reports only, since a failure's time is time-to-error. No
    /// `qos` label.
    provider_ttft_ms: HistogramVec,
    /// `kinference_itl_ms_bucket{model,qos,le}`: cumulative inter-token-latency
    /// bucket counts (ms), summed from each request's per-bucket counts (see
    /// `record_itl`). Hand-aggregated as an `IntCounterVec` with an `le` label,
    /// since the native `Histogram` can't ingest pre-bucketed counts.
    /// `histogram_quantile` works on it; there is no `_sum`.
    itl_bucket: IntCounterVec,
    /// `kinference_itl_ms_count{model,qos}`: total ITL gap samples (equals the
    /// `le="+Inf"` bucket). Companion to `itl_bucket`.
    itl_count: IntCounterVec,
    // --- Body buffer (PushBody, Pick, PullBody). ---
    //
    // Not labeled by {model,qos}: no model is known at push time.
    /// `kinference_body_buffer_bytes`: bytes currently held (uploads in progress
    /// and bodies awaiting a pull). Compare against `body_max_total_kb`, which
    /// is in KB.
    body_buffer_bytes: IntGauge,
    /// `kinference_body_buffer_entries`: buffered bodies currently tracked.
    body_buffer_entries: IntGauge,
    /// `kinference_body_pushed_total` / `..._bytes_total`: completed pushes.
    body_pushed: IntCounter,
    body_pushed_bytes: IntCounter,
    /// `kinference_body_rejected_total{reason}`: pushes refused over budget,
    /// a sign the buffer is under-provisioned.
    body_rejected: IntCounterVec,
    /// `kinference_body_reaped_total`: bodies reclaimed by the TTL sweep, i.e.
    /// callers that vanished without aborting.
    body_reaped: IntCounter,
    /// `kinference_body_aborted_total{reason}`: explicit `Abort` calls.
    body_aborted: IntCounterVec,
    // --- CRD routing policy. Not {model,qos}-labeled: these describe the
    // policy machinery, and `rule` is already the useful dimension.
    /// `kinference_picks_total{outcome}`: every Pick() by wire outcome.
    picks: IntCounterVec,
    /// `kinference_pick_duration_ms{outcome}`: the router's own decision latency.
    pick_duration_ms: HistogramVec,
    /// `kinference_tokenize_total{path}`: how each request got tokenized.
    tokenize_path: IntCounterVec,
    /// `kinference_tokenize_duration_ms`: tokenize latency, all paths.
    tokenize_duration_ms: Histogram,
    /// `kinference_tokenize_compare_total{result}`: compare-mode agreement.
    tokenize_compare: IntCounterVec,
    /// `kinference_prefix_requested_blocks_total{model,qos}`: prompt blocks scored.
    prefix_requested_blocks: IntCounterVec,
    /// `kinference_prefix_matched_blocks_total{model,qos}`: layered overlap.
    prefix_matched_blocks: IntCounterVec,
    /// `kinference_prefix_radix_matched_blocks_total{model,qos}`: raw radix match.
    prefix_radix_matched_blocks: IntCounterVec,
    /// `kinference_prefix_match_ratio{model,qos}`: layered / requested, per pick.
    prefix_match_ratio: HistogramVec,
    /// `kinference_prefix_overmatch_total{model,qos}`: picks where matched > requested.
    prefix_overmatch: IntCounterVec,
    /// `kinference_score_degraded_total{reason}`.
    score_degraded: IntCounterVec,
    /// `kinference_policy_decisions_total{outcome}`.
    policy_decisions: IntCounterVec,
    /// `kinference_routed_requests_total{tenant,destination,type}`: which worker
    /// set or provider each tenant's traffic went to.
    routed_requests: IntCounterVec,
    /// `kinference_policy_rule_matches_total{rule}`: which rules are actually live.
    policy_rule_matches: IntCounterVec,
    /// `kinference_policy_revision_mismatch_total`: the gateway routed on a pool
    /// revision we don't hold. Rising means our watch lags the gateway.
    policy_revision_mismatch: IntCounter,
    /// `kinference_predicted_ttft_ms{worker,model}`: the values `predictedTTFT`
    /// compares against, for every candidate scored (winners alone would be
    /// biased low).
    ///
    /// Recorded by exactly one ranker per request: `policy_driver`'s
    /// `candidates_for` when a policy is in play, `service::best_group`
    /// otherwise.
    predicted_ttft_ms: HistogramVec,
}

/// Build a per-(model,qos) token-count histogram (a `length` distribution)
/// with the shared bucket layout: exponential factor-2 from 8 to ~262k tokens.
/// Shared by prompt and completion lengths so they are comparable.
fn length_histogram(registry: &Registry, name: &str, help: &str) -> HistogramVec {
    histogram_vec(
        registry,
        name,
        help,
        exponential_buckets(8.0, 2.0, 16).expect("length histogram buckets are valid"),
        &["model", "qos", "tenant"],
    )
}

/// The resolved form of `metrics.tenant_labels`.
///
/// Like [`TenantLabels`], but with a `HashSet` allowlist, since
/// `tenant_label` is on the hot path.
enum TenantPolicy {
    /// Everything buckets under [`DEFAULT_TENANT_LABEL`].
    None,
    /// Every tenant seen becomes its own label value.
    All,
    Allow(HashSet<String>),
}

impl From<&TenantLabels> for TenantPolicy {
    fn from(l: &TenantLabels) -> Self {
        match l {
            TenantLabels::None => Self::None,
            TenantLabels::All => Self::All,
            TenantLabels::Allow(v) => Self::Allow(v.iter().cloned().collect()),
        }
    }
}

impl RouterMetrics {
    /// Build the families from `metrics`, register them in `registry`, and
    /// seed every closed label set at zero. Warns once if tenant labelling is
    /// unbounded.
    pub fn new(registry: &Registry, cfg: &MetricsConfig) -> Self {
        if matches!(cfg.tenant_labels, TenantLabels::All) {
            tracing::warn!(
                "tenant label values are unbounded; set metrics.tenant_labels to an allowlist"
            );
        }
        let tenants = TenantPolicy::from(&cfg.tenant_labels);
        let prompt_tokens = int_counter_vec(
            registry,
            "kinference_prompt_tokens_total",
            "Prompt (input) tokens by served model and QoS class",
            &["model", "qos", "tenant"],
        );
        let completion_tokens = int_counter_vec(
            registry,
            "kinference_completion_tokens_total",
            "Completion (output) tokens by served model and QoS class",
            &["model", "qos", "tenant"],
        );
        let cached_prompt_tokens = int_counter_vec(
            registry,
            "kinference_cached_prompt_tokens_total",
            "Prompt tokens the ENGINE reports it served from its own prefix \
                 cache, from ReportFinalToken. Measured by the engine, not \
                 predicted by the router: subtract from kinference_prompt_tokens_total \
                 for the prompt tokens that actually cost prefill compute",
            &["model", "qos", "tenant"],
        );
        let cached_tokens_absent = int_counter(
            registry,
            "kinference_cached_tokens_absent_total",
            "Final-token reports that carried no cached_tokens field. Those \
             requests contribute nothing to kinference_cached_prompt_tokens_total, so \
             a non-zero value means the cached figure UNDERSTATES the true cache \
             hit and any billing derived from it OVERCHARGES. Unlabelled on \
             purpose: it answers whether the basis is complete, not whose",
        );
        let cached_tokens_reported = int_counter(
            registry,
            "kinference_cached_tokens_reported_total",
            "Final-token reports that DID carry cached_tokens. The companion \
             denominator to kinference_cached_tokens_absent_total: the two partition \
             every report, so absent/(absent+reported) is the exact share of \
             traffic missing from the cached-token basis. Both are unlabelled \
             single series on purpose — a ratio built from a labelled family \
             instead is a sum-of-rates over many sparse children, which does not \
             equal the rate of their sum and drifts above 100%",
        );
        let orphaned_reports = int_counter(
            registry,
            "kinference_orphaned_reports_total",
            "Final-token reports whose response_id kinference never issued, or whose \
             labels aged out of the 600s TTL. Their model comes from the request \
             field rather than the pick, their tenant falls back to the default \
             bucket, and their qos is reported as `unknown` — so a non-zero value \
             means that much of every per-request family is misattributed. \
             Unlabelled: it answers whether attribution is intact, not whose",
        );
        // Prompt and completion lengths share one bucket layout (see
        // `length_histogram`).
        let prompt_length = length_histogram(
            registry,
            "kinference_prompt_length_tokens",
            "Distribution of per-request prompt (input) token counts, by model and QoS",
        );
        let completion_length = length_histogram(
            registry,
            "kinference_completion_length_tokens",
            "Distribution of per-request completion (output) token counts, by model and QoS",
        );
        // The `ttft_ms` edges, so the two are comparable, plus three longer
        // buckets for full generations.
        let request_duration_ms = histogram_vec(
            registry,
            "kinference_request_duration_ms",
            "Router-observed end-to-end request duration in milliseconds: the \
                 interval from the pick to the caller's ReportFinalToken. Includes \
                 the caller's own handling and the report's network hop, so it is \
                 NOT client-observed latency. Only requests that report are counted",
            vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0, 160000.0, 300000.0, 600000.0,
            ],
            &["model", "qos", "tenant"],
        );
        // Milliseconds, as `observed_ttft_ms`, with buckets from 10ms to 80s.
        let ttft_ms = histogram_vec(
            registry,
            "kinference_ttft_ms",
            "Time-to-first-token in milliseconds, by model and QoS (streaming requests only)",
            vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0,
            ],
            &["model", "qos", "tenant"],
        );
        // Same buckets as `ttft_ms`, so the two are comparable.
        let provider_ttft_ms = histogram_vec(
            registry,
            "kinference_provider_ttft_ms",
            "Time-to-first-token for off-cluster provider picks, in milliseconds, \
                 by provider and model (successful reports only)",
            vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0,
            ],
            &["provider", "model"],
        );
        // ITL: a hand-aggregated histogram fed from the client's per-request
        // bucket counts. Counters, so `rate()` and `histogram_quantile` work.
        let itl_bucket = int_counter_vec(
            registry,
            "kinference_itl_ms_bucket",
            "Cumulative inter-token-latency bucket counts (ms), by model and QoS",
            &["model", "qos", "le", "tenant"],
        );
        let itl_count = int_counter_vec(
            registry,
            "kinference_itl_ms_count",
            "Total inter-token-latency gap samples, by model and QoS",
            &["model", "qos", "tenant"],
        );
        // Body-buffer stats.
        let body_buffer_bytes = int_gauge(
            registry,
            "kinference_body_buffer_bytes",
            "Request-body bytes currently buffered (uploads in progress + bodies awaiting PullBody)",
        );
        let body_buffer_entries = int_gauge(
            registry,
            "kinference_body_buffer_entries",
            "Request bodies currently tracked by the body store",
        );
        let body_pushed = int_counter(
            registry,
            "kinference_body_pushed_total",
            "Request bodies successfully pushed and buffered",
        );
        let body_pushed_bytes = int_counter(
            registry,
            "kinference_body_pushed_bytes_total",
            "Total request-body bytes accepted by PushBody",
        );
        let body_rejected = int_counter_vec(
            registry,
            "kinference_body_rejected_total",
            "PushBody calls refused because a buffer budget was exceeded",
            &["reason"],
        );
        let body_reaped = int_counter(
            registry,
            "kinference_body_reaped_total",
            "Buffered bodies reclaimed by the TTL sweep (caller vanished without aborting)",
        );
        let body_aborted = int_counter_vec(
            registry,
            "kinference_body_aborted_total",
            "Abort calls, by reason",
            &["reason"],
        );
        let picks = int_counter_vec(
            registry,
            "kinference_picks_total",
            "Pick() calls by wire outcome: ok, bad_request (the body did not \
                 parse or carried nothing to route on), no_workers (nothing serves \
                 the requested model). The router's top-line request outcome — \
                 distinct from kinference_policy_decisions_total, which is about CRD \
                 policy resolution rather than the result returned to the caller",
            &["outcome"],
        );
        let pick_duration_ms = histogram_vec(
            registry,
            "kinference_pick_duration_ms",
            "How long the router took to answer a Pick(), in milliseconds. This \
                 is kinference's OWN service latency, not the model's — it covers \
                 scoring and any tokenize round-trip, and ends when the response is \
                 built. Labeled by outcome because a rejected pick is fast and \
                 would otherwise flatter the distribution",
            vec![
                0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0,
            ],
            &["outcome"],
        );
        let tokenize_path = int_counter_vec(
            registry,
            "kinference_tokenize_total",
            "How each request got tokenized: local (the in-process tokenizer \
                 served it), http_fallback (local failed, the engine's /render \
                 saved it), unavailable (both failed — the pick is DEGRADED and \
                 routes on load alone). Exactly one per tokenize attempt, so the \
                 three sum to the total",
            &["path"],
        );
        let tokenize_duration_ms = histogram(
            registry,
            "kinference_tokenize_duration_ms",
            "Tokenize latency in milliseconds, across every path. On the Pick \
                 critical path, so it is a component of kinference_pick_duration_ms",
            vec![
                0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
            ],
        );
        let tokenize_compare = int_counter_vec(
            registry,
            "kinference_tokenize_compare_total",
            "Compare-mode agreement between the local tokenizer and the engine's \
                 /render: match, divergence, error. A DIVERGENCE means every prefix \
                 block hash is wrong, so cache-affinity routing silently stops \
                 working while the prefix metrics still report plausible numbers. \
                 Only ever non-zero when tokenize.mode = compare",
            &["result"],
        );
        let prefix_requested_blocks = int_counter_vec(
            registry,
            "kinference_prefix_requested_blocks_total",
            "Prompt blocks scored for prefix reuse, by served model and QoS. The \
                 shared denominator for both matched-block counters",
            &["model", "qos"],
        );
        let prefix_matched_blocks = int_counter_vec(
            registry,
            "kinference_prefix_matched_blocks_total",
            "Prompt blocks the router expected the CHOSEN worker to already hold: \
                 the layered overlap it actually scored on, radix match plus credit \
                 for blocks in-flight requests are still prefilling",
            &["model", "qos"],
        );
        let prefix_radix_matched_blocks = int_counter_vec(
            registry,
            "kinference_prefix_radix_matched_blocks_total",
            "Prompt blocks the radix tree alone reported cached on the chosen \
                 worker. Same picks as the layered counter, so the difference between \
                 them is the credit given to in-flight prefill",
            &["model", "qos"],
        );
        // Linear: the interesting structure is at both ends (cold misses
        // and full hits).
        let prefix_match_ratio = histogram_vec(
            registry,
            "kinference_prefix_match_ratio",
            "Per-pick layered prefix overlap as a fraction of the prompt. The \
                 counters give the volume-weighted rate; this gives the SHAPE, which \
                 is the actionable part — a fleet averaging 0.45 may be uniformly \
                 mediocre or bimodal, and those want opposite responses",
            vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
            &["model", "qos"],
        );
        let prefix_overmatch = int_counter_vec(
            registry,
            "kinference_prefix_overmatch_total",
            "Picks where matched blocks exceeded the prompt's block count. Real, \
                 not defensive: overlap scores are summed across data-parallel ranks, \
                 so a DP>1 worker publishing one prefix from two ranks double-counts. \
                 The ratio is clamped to 1.0; this counts how often that happened",
            &["model", "qos"],
        );
        let score_degraded = int_counter_vec(
            registry,
            "kinference_score_degraded_total",
            "Picks that fell back to load-only scoring, by which subsystem failed",
            &["reason"],
        );
        let policy_decisions = int_counter_vec(
            registry,
            "kinference_policy_decisions_total",
            "CRD routing-policy decisions, by outcome",
            &["outcome"],
        );
        let routed_requests = int_counter_vec(
            registry,
            "kinference_routed_requests_total",
            "Picks that chose a destination, by tenant, destination (worker set or \
                 provider) and its type (Local or Provider)",
            &["tenant", "destination", "type"],
        );
        let policy_rule_matches = int_counter_vec(
            registry,
            "kinference_policy_rule_matches_total",
            "InferencePolicy rules that produced a routing decision, by rule name",
            &["rule"],
        );
        // Same buckets as `kinference_ttft_ms`, so predicted and observed
        // are comparable.
        let predicted_ttft_ms = histogram_vec(
            registry,
            "kinference_predicted_ttft_ms",
            "Predicted time-to-first-token in milliseconds, per candidate scored by \
                 an InferencePolicy, by worker and model. The exact values a \
                 `drop: {predictedTTFT: ...}` clause compares against — every candidate, \
                 not just the winner. Recorded whether or not a policy is attached",
            vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0,
            ],
            &["worker", "model"],
        );
        let policy_revision_mismatch = int_counter(
            registry,
            "kinference_policy_revision_mismatch_total",
            "Picks whose named InferencePool revision differed from the watched one",
        );
        let m = Self {
            tenants,
            itl_length_warned: AtomicBool::new(false),
            picks,
            pick_duration_ms,
            tokenize_path,
            tokenize_duration_ms,
            tokenize_compare,
            prefix_requested_blocks,
            prefix_matched_blocks,
            prefix_radix_matched_blocks,
            prefix_match_ratio,
            prefix_overmatch,
            request_duration_ms,
            cached_prompt_tokens,
            cached_tokens_absent,
            cached_tokens_reported,
            orphaned_reports,
            prompt_tokens,
            completion_tokens,
            prompt_length,
            completion_length,
            ttft_ms,
            provider_ttft_ms,
            itl_bucket,
            itl_count,
            body_buffer_bytes,
            body_buffer_entries,
            body_pushed,
            body_pushed_bytes,
            body_rejected,
            body_reaped,
            body_aborted,
            score_degraded,
            policy_decisions,
            routed_requests,
            policy_rule_matches,
            predicted_ttft_ms,
            policy_revision_mismatch,
        };
        m.preinit();
        m
    }

    /// Create every closed-set child series at zero, so a dashboard can tell
    /// "has not happened" from "is not happening".
    ///
    /// `with_label_values` materializes the child as a side effect.
    ///
    /// Counters only: `histogram_quantile` over an all-zero histogram returns
    /// nothing, so seeding `pick_duration_ms` would cost ~45 series for no
    /// benefit. `policy_rule_matches{rule}` has no bounded set to seed.
    fn preinit(&self) {
        for outcome in PICK_OUTCOMES {
            self.picks.with_label_values(&[outcome]);
        }
        for reason in DEGRADE_REASONS {
            self.score_degraded.with_label_values(&[reason]);
        }
        for path in TOKENIZE_PATHS {
            self.tokenize_path.with_label_values(&[path]);
        }
        for result in TOKENIZE_COMPARE_RESULTS {
            self.tokenize_compare.with_label_values(&[result]);
        }
        for reason in BODY_REJECT_REASONS {
            self.body_rejected.with_label_values(&[reason]);
        }
        for reason in BODY_ABORT_REASONS {
            self.body_aborted.with_label_values(&[reason]);
        }
        for outcome in POLICY_DECISION_OUTCOMES {
            self.policy_decisions.with_label_values(&[outcome]);
        }
    }

    /// One body accepted by `PushBody`.
    pub fn record_body_pushed(&self, bytes: u64) {
        self.body_pushed.inc();
        self.body_pushed_bytes.inc_by(bytes);
    }

    /// One `PushBody` refused over budget. `reason` comes from
    /// [`crate::body_store::RejectReason::as_label`] so the label and the
    /// caller-facing message can't drift.
    pub fn record_body_rejected(&self, reason: &str) {
        self.body_rejected.with_label_values(&[reason]).inc();
    }

    /// One `Abort`, by reason.
    pub fn record_body_aborted(&self, reason: &str) {
        self.body_aborted.with_label_values(&[reason]).inc();
    }

    /// Bodies reclaimed by the TTL sweep.
    pub fn record_bodies_reaped(&self, count: u64) {
        self.body_reaped.inc_by(count);
    }

    /// One completed `Pick()`: its wire outcome and how long the router took.
    ///
    /// `outcome` is the result handed back to the caller, distinct from the
    /// policy decision in `record_policy_decision`.
    pub fn record_pick(&self, outcome: &str, latency_ms: f64) {
        self.picks.with_label_values(&[outcome]).inc();
        self.pick_duration_ms
            .with_label_values(&[outcome])
            .observe(latency_ms);
    }

    /// Which path tokenized one request: `local`, `http_fallback`, or
    /// `unavailable`. Exactly one per attempt.
    pub fn record_tokenize_path(&self, path: &str) {
        self.tokenize_path.with_label_values(&[path]).inc();
    }

    /// Tokenize latency, all paths.
    pub fn record_tokenize_duration(&self, latency_ms: f64) {
        self.tokenize_duration_ms.observe(latency_ms);
    }

    /// Compare-mode local-vs-`/render` agreement: `match`, `divergence`, or
    /// `error`.
    ///
    /// A divergence means kinference's block hashes don't match the engine's,
    /// so prefix routing silently stops working.
    pub fn record_tokenize_compare(&self, result: &str) {
        self.tokenize_compare.with_label_values(&[result]).inc();
    }

    /// One final-token report that could not be attributed to a pick.
    ///
    /// A direct counter, since deriving it from the difference of two families
    /// with different label sets is unreliable.
    pub fn record_orphaned_report(&self) {
        self.orphaned_reports.inc();
    }

    /// One pick's prefix-cache outcome for the CHOSEN worker.
    ///
    /// `layered` is the overlap the picker scored on (radix match plus
    /// in-flight prefill credit); `radix` is the raw tree match. With one
    /// denominator, `layered - radix` reads as the in-flight credit.
    ///
    /// Do not call this for a degraded pick, which would read as a cache miss;
    /// those are counted by [`Self::record_score_degraded`]. The
    /// `requested == 0` guard below is only a backstop.
    ///
    /// Counts picks, not requests, so totals won't line up with the token and
    /// duration families.
    pub fn record_prefix_match(
        &self,
        model: &str,
        qos: &str,
        requested: u32,
        layered: u32,
        radix: u32,
    ) {
        if model.is_empty() || requested == 0 {
            return;
        }
        let labels = [model, qos];
        self.prefix_requested_blocks
            .with_label_values(&labels)
            .inc_by(requested as u64);
        self.prefix_matched_blocks
            .with_label_values(&labels)
            .inc_by(layered as u64);
        self.prefix_radix_matched_blocks
            .with_label_values(&labels)
            .inc_by(radix as u64);
        // A DP>1 worker publishing one prefix from two ranks can report more
        // matched blocks than the prompt has. Clamp the ratio, and count it.
        if layered > requested {
            self.prefix_overmatch.with_label_values(&labels).inc();
        }
        let ratio = (layered as f64 / requested as f64).clamp(0.0, 1.0);
        self.prefix_match_ratio
            .with_label_values(&labels)
            .observe(ratio);
    }

    /// A pick that lost its prefix-cache signal. `reason` names the subsystem
    /// that failed (tokenize, or kinference's own prefix index).
    pub fn record_score_degraded(&self, reason: &str) {
        self.score_degraded.with_label_values(&[reason]).inc();
    }

    /// One CRD-policy decision, by outcome (`chosen`, `rejected_on_empty`,
    /// `unmatched_served`, `unknown_pool`, …).
    pub fn record_policy_decision(&self, outcome: &str) {
        self.policy_decisions.with_label_values(&[outcome]).inc();
    }

    /// One pick that chose a destination: the worker set or provider, by the
    /// name a policy uses for it, and its type.
    ///
    /// Counted at the pick, so it says where requests were sent, whether or
    /// not they succeeded. Retries count again.
    pub fn record_routed(
        &self,
        tenant: &str,
        destination: &str,
        kind: kinference_policy::crd::BackendType,
    ) {
        self.routed_requests
            .with_label_values(&[self.tenant_label(tenant), destination, kind.as_str()])
            .inc();
    }

    /// A rule fired.
    pub fn record_policy_rule_match(&self, rule: &str) {
        self.policy_rule_matches.with_label_values(&[rule]).inc();
    }

    /// One candidate's predicted TTFT, as scored by a policy.
    pub fn record_predicted_ttft(&self, worker: &str, model: &str, ms: f64) {
        self.predicted_ttft_ms
            .with_label_values(&[worker, model])
            .observe(ms);
    }

    /// The pool revision the gateway routed on differs from the one we hold.
    /// A rising count means the watch is lagging the gateway.
    pub fn record_policy_revision_mismatch(&self) {
        self.policy_revision_mismatch.inc();
    }

    /// Publish the body store's current occupancy. Called from the sweep task.
    pub fn set_body_buffer(&self, entries: u64, bytes: u64) {
        self.body_buffer_entries.set(entries as i64);
        self.body_buffer_bytes.set(bytes as i64);
    }

    /// Prompt tokens the ENGINE served from its prefix cache, if it said.
    ///
    /// `None` means the caller sent no `cached_tokens`, which is not zero; it
    /// is counted separately.
    ///
    /// Unlike `kinference_prefix_matched_blocks_total` (the router's
    /// prediction), this is what the engine actually did.
    pub fn record_cached_prompt_tokens(
        &self,
        model: &str,
        qos: &str,
        tenant: &str,
        cached_tokens: Option<u32>,
    ) {
        if model.is_empty() {
            return;
        }
        match cached_tokens {
            Some(n) => {
                self.cached_prompt_tokens
                    .with_label_values(&[model, qos, self.tenant_label(tenant)])
                    .inc_by(n as u64);
                // Separately, since `n` can legitimately be zero.
                self.cached_tokens_reported.inc();
            }
            None => self.cached_tokens_absent.inc(),
        }
    }

    /// Record one `ReportFinalToken`: add the token counts to the per-model/qos
    /// counters and observe the lengths into the per-model/qos histograms. A
    /// blank `model` (undetermined by the caller) is skipped rather than
    /// bucketed under an empty label.
    pub fn record_usage(
        &self,
        model: &str,
        qos: &str,
        tenant: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
    ) {
        if model.is_empty() {
            return;
        }
        let tenant = self.tenant_label(tenant);
        self.prompt_tokens
            .with_label_values(&[model, qos, tenant])
            .inc_by(prompt_tokens as u64);
        self.completion_tokens
            .with_label_values(&[model, qos, tenant])
            .inc_by(completion_tokens as u64);
        // Exactly one observation per request, so each histogram's `_count`
        // is the request count and its `_sum` mirrors the matching counter.
        self.prompt_length
            .with_label_values(&[model, qos, tenant])
            .observe(prompt_tokens as f64);
        self.completion_length
            .with_label_values(&[model, qos, tenant])
            .observe(completion_tokens as f64);
    }

    /// One completed request, from the pick to `ReportFinalToken`.
    ///
    /// The router's view, not the client's: it includes the caller's own
    /// handling and the report's network hop. Only requests that report are
    /// counted.
    pub fn record_request_duration(&self, model: &str, qos: &str, tenant: &str, ms: u64) {
        if model.is_empty() {
            return;
        }
        self.request_duration_ms
            .with_label_values(&[model, qos, self.tenant_label(tenant)])
            .observe(ms as f64);
    }

    /// Record one streaming request's observed time-to-first-token, in
    /// milliseconds. Only for streaming requests, since for non-streaming the
    /// first-byte time is the whole response.
    pub fn record_ttft(&self, model: &str, qos: &str, tenant: &str, ttft_ms: u32) {
        if model.is_empty() {
            return;
        }
        self.ttft_ms
            .with_label_values(&[model, qos, self.tenant_label(tenant)])
            .observe(ttft_ms as f64);
    }

    /// Observe a provider pick's time-to-first-token, in milliseconds.
    ///
    /// Call only for a successful first-token report. A blank `provider` or
    /// `model` is skipped.
    pub fn record_provider_ttft(&self, provider: &str, model: &str, ttft_ms: u32) {
        if provider.is_empty() || model.is_empty() {
            return;
        }
        self.provider_ttft_ms
            .with_label_values(&[provider, model])
            .observe(ttft_ms as f64);
    }

    /// Merge one request's inter-token-latency bucket counts (from
    /// `ReportFinalToken.itl_bucket_counts`) into the fleet-wide per-(model,qos)
    /// histogram. `counts[i]` is the number of gaps in Layout-1 bucket `i`
    /// (non-cumulative on the wire); we accumulate the cumulative count into
    /// each `le` bucket, which is additive across requests. Skips on empty
    /// `model`, a `layout` that doesn't match [`ITL_BUCKET_LAYOUT`], or a
    /// `counts` length that doesn't match the layout, as the proto requires.
    pub fn record_itl(&self, model: &str, qos: &str, tenant: &str, layout: u32, counts: &[u32]) {
        if model.is_empty() {
            return;
        }
        if layout != ITL_BUCKET_LAYOUT {
            tracing::debug!(
                layout,
                expected = ITL_BUCKET_LAYOUT,
                "skipping an ITL report with an unknown bucket layout"
            );
            return;
        }
        // Empty is valid: the proto sends it for responses with fewer than two
        // chunks.
        if counts.is_empty() {
            return;
        }
        if counts.len() != ITL_LE_LABELS.len() {
            if !self.itl_length_warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    got = counts.len(),
                    expected = ITL_LE_LABELS.len(),
                    "skipping an ITL report with the wrong bucket count"
                );
            }
            return;
        }
        let tenant = self.tenant_label(tenant);
        let mut cum: u64 = 0;
        for (i, &c) in counts.iter().enumerate() {
            cum += c as u64;
            // Positional, matching the declared order
            // `["model", "qos", "le", "tenant"]`; all are `&str`, so a swap
            // would compile.
            self.itl_bucket
                .with_label_values(&[model, qos, ITL_LE_LABELS[i], tenant])
                .inc_by(cum);
        }
        // `cum` is now the total gaps this request (== the +Inf bucket).
        self.itl_count
            .with_label_values(&[model, qos, tenant])
            .inc_by(cum);
    }

    /// The label value for `tenant`, per `metrics.tenant_labels`.
    ///
    /// An empty tenant always resolves to [`DEFAULT_TENANT_LABEL`], unlike in
    /// policy, where `tenant: ""` is a distinct matchable value.
    fn tenant_label<'a>(&'a self, tenant: &'a str) -> &'a str {
        if tenant.is_empty() {
            return DEFAULT_TENANT_LABEL;
        }
        match &self.tenants {
            TenantPolicy::None => DEFAULT_TENANT_LABEL,
            TenantPolicy::All => tenant,
            TenantPolicy::Allow(set) => {
                if set.contains(tenant) {
                    tenant
                } else {
                    DEFAULT_TENANT_LABEL
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
