//! What a pick leaves behind, kept until the caller reports.
//!
//! `Pick()` stashes a `PendingObservation` keyed by the server-minted
//! `request_id`. When the caller reports the observed TTFT via
//! `ReportFirstToken()`, `Observations::finalize` takes it back, logs the
//! predicted-versus-observed event and releases the pick's inflight state;
//! `abort` releases without an event, and `sweep_expired` is the backstop
//! for callers that never report.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::metrics::RouterMetrics;
use crate::picker::WaitPrediction;
use crate::picker::WorkerScore;
use kinference_fleet::profile::{PickerConfig, PredictorConfig};

/// Quality-of-service class for a request, mapped from the proto
/// `PickRequest.QosClass`. Used as the `qos` label on `/metrics` series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qos {
    /// Latency-sensitive / interactive (the proto zero value / default).
    Interactive,
    /// Throughput-oriented; tolerant of higher TTFT.
    Batch,
    /// Lowest priority; may be deferred under load.
    Deferrable,
    /// The class could not be determined, so none is asserted.
    ///
    /// Only on the terminal-report path, when `report_final_token` gets a
    /// `response_id` it never issued or whose labels expired. Never from
    /// [`Qos::from_proto`], since policy matches on QoS at pick time.
    Unknown,
}

impl Qos {
    /// Map the proto `QosClass` i32 (INTERACTIVE=0, BATCH=1, DEFERRABLE=2).
    /// Unknown values fall back to `Interactive` (the proto zero value).
    pub(crate) fn from_proto(v: i32) -> Self {
        match v {
            1 => Qos::Batch,
            2 => Qos::Deferrable,
            _ => Qos::Interactive,
        }
    }

    /// Stable lowercase value for the Prometheus `qos=…` label.
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Qos::Interactive => "interactive",
            Qos::Batch => "batch",
            Qos::Deferrable => "deferrable",
            Qos::Unknown => "unknown",
        }
    }
}

/// The per-pool state a completed pick must release: decrement the endpoint's
/// shadow count, and forget its in-flight prefix.
///
/// Carried on the record rather than looked up by `PoolId` at release time, so
/// the release path needs no lookup and works whether or not the pool still
/// exists.
///
/// The two registries rather than an `Arc<Pool>`, so a pool torn down
/// mid-request (indexer thread, radix tree) is reclaimed promptly; only these
/// two small maps outlive it, for as long as an observation needs them.
#[derive(Clone)]
pub struct InflightHandles {
    pub inflight: std::sync::Arc<kinference_fleet::inflight::InflightRegistry>,
    pub prefixes: std::sync::Arc<kinference_fleet::inflight::InflightPrefixes>,
}

impl InflightHandles {
    pub fn of(pool: &kinference_fleet::pool::Pool) -> Self {
        Self {
            inflight: pool.inflight.clone(),
            prefixes: pool.inflight_prefixes.clone(),
        }
    }
}

impl std::fmt::Debug for InflightHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The registries are interior-mutable maps; their contents are not
        // useful here and printing them would take locks on a Debug call.
        f.write_str("InflightHandles(..)")
    }
}

/// Everything needed to log a per-request observation event when the
/// corresponding `ReportFirstToken()` arrives.
///
/// Built by `KinferenceService::pick_and_record` at the end of the pick
/// critical section; consumed by [`Observations::finalize`].
#[derive(Debug, Clone)]
pub struct PendingObservation {
    /// Server-minted identifier for this pick (the `response_id` on the
    /// wire), also the `InflightPrefixes` key.
    pub request_id: u64,
    /// The worker set this pick was routed to. A label for the logs only;
    /// the release path uses `inflight`.
    pub workerset: String,
    /// The profile whose coefficients produced this row's prediction.
    pub profile: String,
    /// The two things the release path actually touches, carried directly.
    pub inflight: InflightHandles,
    /// When the pick was made. Drives TTL eviction in `take_expired`.
    pub picked_at: Instant,
    /// The worker selected by the picker. Carries the layered
    /// expected-overlap value (`matched_blocks` after layering), block
    /// size, and live load metrics that the observation event logs.
    pub chosen_worker: WorkerScore,
    /// Per-term breakdown of the predicted wait at pick time.
    pub prediction: WaitPrediction,
    /// Shadow `inflight_picks` count for the chosen worker at pick
    /// time, BEFORE the post-pick `inc()`. Zero when inflight
    /// tracking is disabled.
    pub q_inflight: u32,
    /// Raw radix-tree match for the chosen worker at pick time.
    /// `chosen_worker.matched_blocks` carries the layered value the
    /// picker scored on; this is the underlying snapshot.
    pub kv_blocks: u32,
    /// Sum of `uncached_at_dispatch` across inflight dispatches on
    /// the chosen worker at pick time.
    pub inflight_workload_blocks: u32,
    /// Constant-model queue cost (`q_rate × q_waiting`).
    pub q_ms_constant: f32,
    /// Workload-model queue cost (`Σ uncached × prefill_rate +
    /// unknown × q_rate`).
    pub q_ms_workload: f32,
    /// Per-dispatch overlap counts (`|P_new ∩ Pᵢ|`) against the
    /// chosen worker's inflight dispatches, oldest first.
    pub inflight_overlap_blocks: Vec<u32>,
    /// Snapshot of `expected_overlap_alpha` at pick time.
    pub expected_overlap_alpha: f32,
    /// Snapshot of the latency-model coefficients at pick time, so each
    /// observation event logs the predictor state that produced it.
    pub predictor_cfg: PredictorConfig,
    /// Snapshot of the picker knobs at pick time, likewise.
    pub picker_cfg: PickerConfig,
    /// The caller's `client_request_id`, for log correlation.
    pub external_request_id: Option<String>,
    /// True when the cache-replication override swapped the picker's
    /// normal pick for a cold-idle alternative (see
    /// `picker::cache_replication_override`), so offline analysis can split
    /// residuals by it.
    pub replication_fired: bool,
    /// The model this request resolved to (post-`auto`). The label for
    /// per-model observation metrics (e.g. the TTFT histogram); may differ
    /// from the client-supplied model string when `auto` was resolved.
    pub model: String,
    /// Whether the request asked for a streaming response (`stream: true`).
    /// For non-streaming requests the reported "TTFT" is total latency, so
    /// `Observations::finalize` logs no observation event, but still
    /// releases state.
    pub streaming: bool,
    /// QoS class from the Pick request. The `qos` label on the TTFT
    /// histogram, recorded from this record at `ReportFirstToken`.
    pub qos: Qos,
    /// Tenant from the Pick request, raw: the `tenant` label on the TTFT
    /// histogram. Carried here rather than read from `RequestLabels`, whose
    /// entry an abort or TTL sweep may already have consumed.
    pub tenant: String,
}

/// In-memory stash of pending observations. Inserted by `pick_and_record`,
/// consumed by `Observations::finalize` or `abort`, and TTL-swept
/// as a backstop.
pub struct PendingObservations {
    by_id: DashMap<u64, PendingObservation>,
    ttl: Duration,
}

impl PendingObservations {
    /// Construct with the given TTL, which must exceed the longest response
    /// time a caller may take to report (see `observation_ttl_secs`).
    pub fn new(ttl: Duration) -> Self {
        Self {
            by_id: DashMap::new(),
            ttl,
        }
    }

    /// Stash a pending observation keyed by `request_id`, overwriting any
    /// existing entry (ids are random 64-bit, so collisions don't happen).
    pub fn insert(&self, obs: PendingObservation) {
        self.by_id.insert(obs.request_id, obs);
    }

    /// Remove and return the entry for `request_id`. `None` means it was
    /// unknown (swept, never inserted, or already consumed).
    pub fn take(&self, request_id: u64) -> Option<PendingObservation> {
        self.by_id.remove(&request_id).map(|(_, v)| v)
    }

    /// Remove and return every entry older than `ttl`, so the caller can
    /// release each one's inflight state (see [`Observations::sweep_expired`]).
    pub fn take_expired(&self) -> Vec<PendingObservation> {
        let cutoff = Instant::now() - self.ttl;
        let expired: Vec<u64> = self
            .by_id
            .iter()
            .filter(|e| e.value().picked_at < cutoff)
            .map(|e| *e.key())
            .collect();
        expired
            .into_iter()
            .filter_map(|id| self.by_id.remove(&id).map(|(_, v)| v))
            .collect()
    }

    /// Number of currently-pending observations. Test-only diagnostic.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Per-request metric labels needed at `ReportFinalToken`. The
/// `PendingObservation` is consumed earlier, at `ReportFirstToken`, so this
/// side map survives until `ReportFinalToken` or the TTL sweep. Nothing
/// inflight is tied to it, so eviction is a plain drop.
#[derive(Debug, Clone)]
pub struct RequestMeta {
    pub model: String,
    pub qos: Qos,
    /// Tenant from the `Pick` request, raw; `crate::metrics` applies the
    /// allowlist. Empty buckets under the default label.
    pub tenant: String,
    /// Whether the request asked for a streaming response.
    ///
    /// Needed by the provider path, which has no `PendingObservation`, to tell
    /// a real TTFT from a non-streaming response's total latency.
    pub streaming: bool,
    pub picked_at: Instant,
    /// The `InferenceProvider` this pick went to, when it went off-cluster.
    ///
    /// Here rather than in `PendingObservation`, which is built from
    /// per-endpoint scoring state a provider doesn't have, and because this
    /// map lives until `ReportFinalToken`.
    pub provider: Option<kinference_policy::engine::ObjectKey>,
}

/// What `RequestLabels` knows about a pick that went off-cluster.
pub struct ProviderPickLabels {
    pub provider: kinference_policy::engine::ObjectKey,
    pub model: String,
    /// Whether the caller asked for streaming. Decides whether its reported
    /// first-token time is a real TTFT or the whole response.
    pub streaming: bool,
}

pub struct RequestLabels {
    by_id: DashMap<u64, RequestMeta>,
    ttl: Duration,
}

impl RequestLabels {
    pub fn new(ttl: Duration) -> Self {
        Self {
            by_id: DashMap::new(),
            ttl,
        }
    }

    /// Stash labels for a pick, keyed by the server-minted response id.
    pub fn insert(&self, response_id: u64, meta: RequestMeta) {
        self.by_id.insert(response_id, meta);
    }

    /// Remove and return the labels for `response_id`, if present. `None`
    /// means unknown (already taken, TTL-swept, or never picked).
    pub fn take(&self, response_id: u64) -> Option<RequestMeta> {
        self.by_id.remove(&response_id).map(|(_, v)| v)
    }

    /// What a provider pick's feedback needs, without consuming the entry.
    ///
    /// A peek rather than a take, since provider feedback (`ReportFirstToken`,
    /// `Abort`) may arrive before the terminal report that clears this. One
    /// lookup, so all fields come from the same map state.
    pub fn provider_pick_for(&self, response_id: u64) -> Option<ProviderPickLabels> {
        self.by_id.get(&response_id).and_then(|m| {
            m.provider.clone().map(|provider| ProviderPickLabels {
                provider,
                model: m.model.clone(),
                streaming: m.streaming,
            })
        })
    }

    /// Drop every entry older than `ttl`. Backstop for requests that never
    /// reach `ReportFinalToken`; nothing is tied to these so they're dropped
    /// silently. Returns the number dropped.
    pub fn take_expired(&self) -> usize {
        let cutoff = Instant::now() - self.ttl;
        let before = self.by_id.len();
        self.by_id.retain(|_, v| v.picked_at >= cutoff);
        before - self.by_id.len()
    }
}

/// The service's part for what a pick leaves behind: the pending stash, the
/// request labels, and what callers report about providers.
///
/// kinference never talks to a provider; the caller does, and reports back
/// over the feedback RPCs. `record_provider_pick` writes the labels the
/// report handlers read back.
pub(crate) struct Observations {
    /// Inserted by `pick_and_record`, consumed by [`Self::finalize`] or
    /// [`Self::abort`], and TTL-swept.
    pub(crate) pending: PendingObservations,
    /// Per-request metric labels (model + qos), keyed by `response_id`.
    /// Inserted at pick; taken at `ReportFinalToken` to label the token and
    /// ITL metrics, when the `PendingObservation` is already gone. TTL-swept.
    pub(crate) labels: RequestLabels,
    /// What the caller has reported back about off-cluster destinations.
    /// Shared with the status writer, which publishes it. Always present,
    /// even with policy disabled, so the feedback handlers needn't check.
    pub(crate) provider_health: Arc<kinference_policy::provider_health::ProviderHealthStore>,
    prom: Arc<RouterMetrics>,
}

impl Observations {
    /// Both maps sweep at `ttl`.
    pub(crate) fn new(
        ttl: Duration,
        provider_health: Arc<kinference_policy::provider_health::ProviderHealthStore>,
        prom: Arc<RouterMetrics>,
    ) -> Self {
        Self {
            pending: PendingObservations::new(ttl),
            labels: RequestLabels::new(ttl),
            provider_health,
            prom,
        }
    }

    /// Stash what a provider pick needs for its feedback to land.
    ///
    /// Only the labels map: there is no `PendingObservation` (built from
    /// per-endpoint scoring state) and nothing pool-scoped for a provider.
    ///
    /// `qos`, `tenant` and `streaming` describe the request, not the
    /// provider. `streaming` lets [`Self::record_provider_first_token`] tell
    /// a real TTFT from a non-streaming response's total latency.
    pub(crate) fn record_provider_pick(
        &self,
        response_id: u64,
        provider: &kinference_policy::engine::ObjectKey,
        upstream_model: &str,
        qos: Qos,
        tenant: &str,
        streaming: bool,
    ) {
        self.labels.insert(
            response_id,
            RequestMeta {
                // The model the body names, as for a local pick.
                model: upstream_model.to_string(),
                qos,
                tenant: tenant.to_string(),
                streaming,
                picked_at: Instant::now(),
                provider: Some(provider.clone()),
            },
        );
    }

    /// Fold a first-token report into a provider's health, when the pick went
    /// off-cluster.
    ///
    /// `upstream_status` is how kinference learns about failures like a revoked
    /// API key, which it can't see otherwise.
    ///
    /// Returns whether `response_id` was a known provider pick, so the reply
    /// can acknowledge it despite there being no `PendingObservation`.
    pub(crate) fn record_provider_first_token(
        &self,
        response_id: u64,
        observed_ttft_ms: u32,
        upstream_status: i32,
    ) -> bool {
        let Some(labels) = self.labels.provider_pick_for(response_id) else {
            return false;
        };
        let (provider, model) = (labels.provider, labels.model);
        use kinference_policy::provider_health::FailureKind;
        let failure = match upstream_status {
            // No response at all.
            0 => Some((FailureKind::Upstream, "no response".to_string())),
            401 | 403 => Some((
                FailureKind::Unauthorized,
                format!("vendor returned {upstream_status}"),
            )),
            s if s >= 500 => Some((FailureKind::Upstream, format!("vendor returned {s}"))),
            _ => None,
        };
        match failure {
            Some((kind, detail)) => self.note_provider_failure(&provider, kind, &detail),
            None if labels.streaming => {
                self.provider_health
                    .record_success(&provider, observed_ttft_ms as f32);
                // Success only: a failure's time is time-to-error.
                self.prom
                    .record_provider_ttft(&provider.label(), &model, observed_ttft_ms);
            }
            None => {
                // Non-streaming: `observed_ttft_ms` is total latency, so record
                // the success without a sample.
                self.provider_health.record_success_untimed(&provider);
            }
        }
        true
    }

    /// Fold an `Abort` that reports a provider failure into that provider's
    /// health, when the pick went off-cluster. The caller maps the wire
    /// reason to `kind` and skips the reasons that are not provider failures.
    pub(crate) fn record_provider_abort(
        &self,
        response_id: u64,
        kind: kinference_policy::provider_health::FailureKind,
        detail: &str,
    ) {
        let Some(labels) = self.labels.provider_pick_for(response_id) else {
            return;
        };
        self.note_provider_failure(&labels.provider, kind, detail);
    }

    /// Consume the labels for a terminal report and record the per-(model,
    /// qos, tenant) usage, duration and ITL families.
    ///
    /// Returns whether `response_id` was known. When it was not (swept or
    /// never picked), the families are still recorded, under the model
    /// `unknown` and `Qos::Unknown`, and the orphan is counted.
    pub(crate) fn record_final_token(&self, response_id: u64, report: &FinalTokenReport) -> bool {
        let meta = self.labels.take(response_id);
        // An unknown or expired `response_id` leaves nothing to attribute this
        // to. The empty tenant maps to the default bucket.
        let (model, qos, tenant) = match &meta {
            Some(m) => (m.model.clone(), m.qos, m.tenant.clone()),
            // `unknown`, rather than a guess. The report's model is whatever
            // the caller sent, so as a label it could grow without bound.
            None => ("unknown".to_string(), Qos::Unknown, String::new()),
        };
        if meta.is_none() {
            self.prom.record_orphaned_report();
        }
        let qos_label = qos.as_label();
        tracing::debug!(
            response_id,
            model = %model,
            reported_model = %report.model,
            qos = qos_label,
            prompt_tokens = report.prompt_tokens,
            completion_tokens = report.completion_tokens,
            itl_layout = report.itl_bucket_layout,
            itl_buckets = report.itl_bucket_counts.len(),
            known = meta.is_some(),
            "final token reported"
        );
        // End-to-end duration, when we know when the pick happened.
        if let Some(m) = &meta {
            self.prom.record_request_duration(
                &model,
                qos_label,
                &tenant,
                m.picked_at.elapsed().as_millis() as u64,
            );
        }
        self.prom.record_usage(
            &model,
            qos_label,
            &tenant,
            report.prompt_tokens,
            report.completion_tokens,
        );
        // Engine-measured prefix-cache hit; `None` is counted as unknown.
        self.prom
            .record_cached_prompt_tokens(&model, qos_label, &tenant, report.cached_tokens);
        self.prom.record_itl(
            &model,
            qos_label,
            &tenant,
            report.itl_bucket_layout,
            &report.itl_bucket_counts,
        );
        meta.is_some()
    }

    /// Record a failure and log the moment a provider stops being dispatched to.
    ///
    /// Logged on the transition only.
    fn note_provider_failure(
        &self,
        provider: &kinference_policy::engine::ObjectKey,
        kind: kinference_policy::provider_health::FailureKind,
        detail: &str,
    ) {
        if self.provider_health.record_failure(provider, kind, detail) {
            tracing::warn!(
                provider = %provider.label(),
                reason = kind.as_str(),
                detail,
            "suppressing a provider after repeated dispatch failures"
            );
        }
    }

    /// Close an observation: release the pick's inflight state and, for a
    /// streaming request, log the observation event. Called from `ReportFirstToken()`
    /// with the caller's observed TTFT.
    ///
    /// Returns `false`, changing nothing, when `request_id` is unknown (swept,
    /// never inserted, or already consumed).
    pub(crate) fn finalize(
        &self,
        request_id: u64,
        observed_ttft_ms: u32,
        upstream_status: i32,
    ) -> bool {
        let Some(record) = self.pending.take(request_id) else {
            return false;
        };
        // Always release in-flight bookkeeping, streaming or not.
        release_inflight_state(&record);

        // For non-streaming responses `observed_ttft_ms` is the full response
        // time, not TTFT, so skip the observation event.
        if !record.streaming {
            return true;
        }

        // Streaming: a real TTFT.
        self.prom.record_ttft(
            &record.model,
            record.qos.as_label(),
            &record.tenant,
            observed_ttft_ms,
        );

        let predicted_ms = record.prediction.total_ms();
        let residual_ms = observed_ttft_ms as f32 - predicted_ms;
        log_observation(&record, observed_ttft_ms, residual_ms, upstream_status);
        true
    }

    /// Drop a `PendingObservation` without logging an observation event.
    ///
    /// For a request that died before its first token: release the inflight
    /// state, but log no event. Returns `true` if a record was found.
    pub(crate) fn abort(&self, request_id: u64) -> bool {
        let Some(record) = self.pending.take(request_id) else {
            return false;
        };
        release_inflight_state(&record);
        true
    }

    /// Evict pending observations past the TTL, releasing each one's inflight
    /// state as `abort` would. The backstop for callers that never
    /// report. Returns the number evicted.
    pub(crate) fn sweep_expired(&self) -> usize {
        let evicted = self.pending.take_expired();
        for record in &evicted {
            release_inflight_state(record);
        }
        // Request labels whose ReportFinalToken never arrived.
        self.labels.take_expired();
        evicted.len()
    }
}

/// What `ReportFinalToken` carries: the served model, the token counts and
/// the inter-token-latency buckets, free of the proto.
pub(crate) struct FinalTokenReport {
    pub model: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    /// Engine-measured prefix-cache hit; `None` when the engine did not
    /// report it.
    pub cached_tokens: Option<u32>,
    pub itl_bucket_layout: u32,
    pub itl_bucket_counts: Vec<u32>,
}

/// The tracing target of the predicted-versus-observed event, one per
/// streaming observation. Enable it with `kinference::observation=debug`.
const OBSERVATION_TARGET: &str = "kinference::observation";

/// Emit the predicted-versus-observed event for `record`, the input for
/// refitting the predictor. The event includes the profile's coefficients, so
/// each event is self-contained.
fn log_observation(
    record: &PendingObservation,
    observed_ms: u32,
    residual_ms: f32,
    upstream_status: i32,
) {
    let w = &record.chosen_worker;
    let p = &record.prediction;
    let pred = &record.predictor_cfg;
    let pkr = &record.picker_cfg;
    tracing::debug!(
        target: OBSERVATION_TARGET,
        response_id = record.request_id,
        req = record.external_request_id.as_deref(),
        model = record.model.as_str(),
        workerset = record.workerset.as_str(),
        profile = record.profile.as_str(),
        worker = w.name.as_str(),
        endpoint = w.endpoint.as_str(),
        upstream_status,
        predicted_ms = p.total_ms(),
        observed_ms,
        residual_ms,
        base_ms = p.base_ms,
        q_ms = p.q_ms,
        q_rate_ms_per_entry = p.q_rate_ms_per_entry,
        prefill_ms = p.prefill_ms,
        cache_ms = p.cache_ms,
        q_running_ms = p.q_running_ms,
        decode_contention_ms = p.decode_contention_ms,
        overlap_blocks = w.matched_blocks,
        total_blocks = w.total_blocks,
        kv_blocks = record.kv_blocks,
        q_waiting = w.queue_waiting,
        q_inflight = record.q_inflight,
        q_running = w.queue_running,
        q_swapped = w.queue_swapped,
        kv_cache_usage = w.kv_cache_usage,
        compl_per_s = w.completion_rate_per_sec,
        q_workload = record.inflight_workload_blocks,
        q_ms_constant = record.q_ms_constant,
        q_ms_workload = record.q_ms_workload,
        inflight_dispatches = record.inflight_overlap_blocks.len(),
        inflight_overlap_blocks = %JsonList(&record.inflight_overlap_blocks),
        expected_overlap_alpha = record.expected_overlap_alpha,
        replication_fired = record.replication_fired,
        queue_ms_per_entry = pred.queue_ms_per_entry,
        cache_pressure_penalty_ms = pred.cache_pressure_penalty_ms,
        saturated_cold_ms_per_entry = pred.saturated_cold_ms_per_entry,
        cache_pressure_saturated_threshold = pred.cache_pressure_saturated_threshold,
        attention_uncached_block_ms = pred.attention_uncached_block_ms,
        attention_uncached_per_every_block_ms = pred.attention_uncached_per_every_block_ms,
        q_running_penalty_peak_ms = pred.q_running_penalty_peak_ms,
        q_running_penalty_peak_qr = pred.q_running_penalty_peak_qr,
        q_running_penalty_falloff_qr = pred.q_running_penalty_falloff_qr,
        inflight_concurrent_capacity = pred.inflight_concurrent_capacity,
        forward_pass_ms = pred.forward_pass_ms,
        top_k_min = pkr.top_k_min,
        top_k_fraction = pkr.top_k_fraction,
        inflight_tracking = pkr.inflight_tracking,
        tie_tolerance_ms = pkr.tie_tolerance_ms,
        replication_threshold_pct = pkr.replication_threshold_pct,
        replication_warm_target = pkr.replication_warm_target,
        replication_warm_threshold_fraction = pkr.replication_warm_threshold_fraction,
        "first token observed"
    );
}

/// Formats a list as a compact JSON array, such as `[5,3]`. A `tracing` field
/// can't hold an array, so a JSON log line holds the array as a string.
struct JsonList<'a>(&'a [u32]);

impl std::fmt::Display for JsonList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[")?;
        for (i, n) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{n}")?;
        }
        f.write_str("]")
    }
}

/// Release the inflight counter and inflight-prefix entry a pick reserved
/// for `record`. The inflight counter has no sweep of its own, so every
/// terminal path for a pick (`finalize`, `abort`, `sweep_expired`) must come
/// through here.
///
/// The pool handles ride on the record, so a pool torn down mid-request
/// stays alive until its pending observations are released.
fn release_inflight_state(record: &PendingObservation) {
    // Pick-time config, from the record.
    if record.picker_cfg.inflight_tracking {
        record.inflight.inflight.dec(&record.chosen_worker.endpoint);
    }
    record.inflight.prefixes.forget(record.request_id);
}

#[cfg(test)]
mod tests;
