//! Wait-time predictor and top-K-with-tie-extension endpoint picker.
//!
//! Pure arithmetic, no I/O.

use std::fmt::Write;
use std::time::Instant;

use kinference_fleet::inflight::InflightSummary;
use kinference_fleet::profile::{PickerConfig, PredictorConfig};

/// One worker's inputs to the picker, projected from `metrics::MetricsSample`
/// by `service::scoring::worker_score`.
#[derive(Debug, Clone)]
pub struct WorkerScore {
    pub name: String,
    pub endpoint: String,
    pub matched_blocks: u32,
    pub completion_rate_per_sec: f32,
    pub queue_waiting: u32,
    pub queue_running: u32,
    pub queue_swapped: u32,
    pub kv_cache_usage: f32,
    pub block_size: u32,
    pub total_blocks: u32,
}

/// Per-term breakdown of the predicted wait, in milliseconds. Kept
/// alongside the total so the log can show what drove a decision.
#[derive(Debug, Clone)]
pub struct WaitPrediction {
    pub base_ms: f32,
    pub q_ms: f32,
    /// The effective per-entry queue rate used to derive `q_ms`. Diagnostic
    /// only: shows whether the cold-saturation fallback applied.
    pub q_rate_ms_per_entry: f32,
    pub prefill_ms: f32,
    pub cache_ms: f32,
    /// U-shaped penalty over the worker's running request count. See
    /// [`q_running_penalty_ms`].
    pub q_running_ms: f32,
    /// Flat per-active-decode cost: `(q_running + q_inflight) ×
    /// forward_pass_ms`, the cost of joining a batch with N decodes in
    /// progress. Inert when `forward_pass_ms == 0`.
    pub decode_contention_ms: f32,
}

impl WaitPrediction {
    pub fn total_ms(&self) -> f32 {
        self.base_ms
            + self.q_ms
            + self.prefill_ms
            + self.cache_ms
            + self.q_running_ms
            + self.decode_contention_ms
    }
}

/// Quadratic prefill-time estimator:
/// ```text
/// prefill_ms = a × n_uncached + c × n_uncached × n_total
/// ```
/// where `a = predictor.attention_uncached_block_ms` (linear per-block
/// setup cost: weight fetch, KV write, etc.) and
/// `c = predictor.attention_uncached_per_every_block_ms` (attention
/// cost: each uncached query attends to every token in the prompt,
/// so scales with prompt length).
///
/// Used for the new request's prefill in `predicted_wait_ms`, and for each
/// inflight's (with its own `total_at_dispatch`) in `compute_queue_cost`.
pub fn prefill_ms_for(predictor: &PredictorConfig, uncached: u32, total: u32) -> f32 {
    let u = uncached as f32;
    let t = total as f32;
    predictor.attention_uncached_block_ms * u
        + predictor.attention_uncached_per_every_block_ms * u * t
}

/// Expected block overlap between our prompt and a worker's KV cache
/// *at the time our prompt actually runs*, given the worker's queue
/// of pending dispatches we've already routed to it.
///
/// `per_dispatch` is the per-dispatch overlap count `|P_new ∩ Pᵢ|`,
/// in dispatch order (oldest first). `kv_match` is the radix-tree
/// match count `|P_new ∩ KV|`, the current snapshot. `alpha` is
/// the eviction-decay factor: each layer "one request older" gets
/// multiplied by an additional `alpha`.
///
/// The model, with layers indexed newest-first (L₀ = newest pending,
/// L_k = the radix-tree KV layer):
///
/// ```text
/// overlap = α⁰ · max(0, o₀ − M₀)
///         + α¹ · max(0, o₁ − M₁)
///         + α² · max(0, o₂ − M₂)
///         + …
///         + αᵏ · max(0, o_k − M_k)
///
/// where oᵢ = |P_new ∩ Lᵢ|  and  Mᵢ = max(o₀, o₁, …, oᵢ₋₁) ,  M₀ = 0
/// ```
///
/// Each block of `P_new` is counted exactly once, at the weight of
/// its *most-recent source*. `Mᵢ` is the longest prefix already
/// covered by every layer strictly newer than `Lᵢ`; the
/// `max(0, oᵢ − Mᵢ)` clamp ensures a layer can only *extend* the
/// covered prefix, never remove blocks.
///
/// Limit behavior:
/// - `α = 1` (no eviction): each layer contributes its incremental
///   prefix beyond all newer layers, so the sum equals the longest
///   matched prefix across all sources, i.e.
///   `max(o₀, o₁, …, o_k, kv_match)`.
/// - `α = 0`: only the L₀ term survives, so the sum collapses to
///   the most-recent dispatch's overlap (or `kv_match` if there are
///   no pending dispatches).
/// - `α ∈ (0, 1)`: each "new" block (one extending the covered
///   prefix) is weighted by `αⁱ` where `i` is its layer index from
///   newest. Older layers progressively discounted. Sum is
///   monotonic in α (raising α never decreases overlap).
pub fn compute_layered_overlap(per_dispatch: &[f32], kv_match: f32, alpha: f32) -> f32 {
    // Newest-first, so the freshest dispatch is L₀ (weight 1). `cum_max`
    // is Mᵢ.
    let mut total = 0.0_f32;
    let mut cum_max = 0.0_f32;
    let mut weight = 1.0_f32;
    for &overlap in per_dispatch.iter().rev() {
        let new_blocks = (overlap - cum_max).max(0.0);
        total += weight * new_blocks;
        if overlap > cum_max {
            cum_max = overlap;
        }
        weight *= alpha;
    }
    // Final layer: radix-tree KV cache.
    let new_blocks = (kv_match - cum_max).max(0.0);
    total += weight * new_blocks;
    total
}

/// Scale each dispatch's raw overlap by its `progress` so the layered
/// expected-overlap reflects only blocks that have actually been
/// committed to the worker's prefix cache by the time our new request
/// arrives. The remaining (uncomputed) portion of each inflight's
/// prefix doesn't give us a cache hit.
///
/// `progress[i] ∈ [0, 1]`:
/// - `0` for inflight items vLLM hasn't started prefilling yet
///   (overflow past `inflight_concurrent_capacity`, or freshly
///   dispatched with elapsed=0)
/// - `1` for items that have finished prefilling (elapsed ≥
///   estimated_prefill); all their blocks should be in cache.
///
/// The caller computes `progress[i]` per inflight using the
/// same `elapsed / estimated_prefill_ms` formula the queue-cost model
/// uses, and zeroes out positions beyond `cfg.inflight_concurrent_capacity`.
/// Length of `per_dispatch_progress` must equal `per_dispatch_overlap`.
pub fn scale_per_dispatch_by_progress(
    per_dispatch_overlap: &[f32],
    per_dispatch_progress: &[f32],
) -> Vec<f32> {
    debug_assert_eq!(
        per_dispatch_overlap.len(),
        per_dispatch_progress.len(),
        "progress must be parallel to overlap"
    );
    per_dispatch_overlap
        .iter()
        .zip(per_dispatch_progress.iter())
        .map(|(o, p)| o * p.clamp(0.0, 1.0))
        .collect()
}

/// Predicted wait (ms) for sending this prompt to worker `w`; see
/// [`WaitPrediction`] for the terms:
///
///   base_ms              = constant per-request intercept
///   q_ms                 = time-progressed inflight cost + unknown-queue cost
///   prefill_ms           = prefill_ms_for(uncached_blocks, total_blocks)
///   cache_ms             = cache_pressure_penalty_ms * kv_cache_usage
///   q_running_ms         = q_running_penalty_ms(...)
///   decode_contention_ms = effective_qr * forward_pass_ms
///
/// Queue cost (see `compute_queue_cost`) is the sum of:
///
/// - Remaining-prefill inflight cost: each in-flight dispatch we have on the
///   worker contributes
///   `(1 - progress) × prefill_ms_for(uncached_at_dispatch, total_at_dispatch)`,
///   with progress `elapsed / estimated_prefill` clamped to [0, 1]. Inflights
///   past position N (`inflight_concurrent_capacity`) haven't been admitted,
///   so contribute their full prefill cost.
///
/// - Unknown-queue cost: queue entries vLLM reports as waiting
///   that we have no inflight-dispatch record for
///   (`max(0, queue_waiting - inflight_count)`) at
///   `q_rate_ms_per_entry`. Cold-saturation can promote that rate to
///   `cfg.saturated_cold_ms_per_entry`.
///
/// `now` is passed in so every worker is scored against the same instant.
///
/// `total_blocks == 0` (metric unavailable, or prompt smaller than one
/// block) treats uncached_blocks as 0 rather than producing NaN.
pub fn predicted_wait_ms(
    w: &WorkerScore,
    predictor: &PredictorConfig,
    inflight_summaries: &[InflightSummary],
    now: Instant,
) -> WaitPrediction {
    let q = compute_queue_cost(w, predictor, inflight_summaries, now);

    let uncached_blocks = w.total_blocks.saturating_sub(w.matched_blocks);
    let prefill_ms = prefill_ms_for(predictor, uncached_blocks, w.total_blocks);

    let cache_ms = predictor.cache_pressure_penalty_ms * w.kv_cache_usage;

    // Picks on this worker not yet visible in scraped metrics; see
    // `q_running_penalty_ms`.
    let q_inflight = inflight_summaries.len() as u32;
    let q_running_ms = q_running_penalty_ms(w.queue_running, q_inflight, predictor);

    // Includes just-dispatched requests, like the U-shape penalty.
    let effective_qr = w.queue_running.saturating_add(q_inflight) as f32;
    let decode_contention_ms = effective_qr * predictor.forward_pass_ms;

    WaitPrediction {
        base_ms: predictor.base_ms,
        q_ms: q.q_ms,
        q_rate_ms_per_entry: q.q_rate_ms_per_entry,
        prefill_ms,
        cache_ms,
        q_running_ms,
        decode_contention_ms,
    }
}

/// U-shaped latency penalty over the worker's effective running count:
/// vLLM's scraped `num_requests_running` plus our picks the engine hasn't
/// reflected yet. Without the latter, concurrent picks within one metrics
/// refresh would all see an idle worker and pile onto it.
///
/// With one or two running requests, a new prefill contends for the GPU but
/// can't join the running batch; at five or more it rides along nearly free.
/// Modeled as a tent: 0 at `effective_qr = 0`, `peak` at `peak_qr`, back to 0
/// at `falloff_qr` and beyond. `peak <= 0.0` disables it.
fn q_running_penalty_ms(qr: u32, q_inflight: u32, predictor: &PredictorConfig) -> f32 {
    let peak = predictor.q_running_penalty_peak_ms;
    let effective_qr = qr.saturating_add(q_inflight);
    if peak <= 0.0 || effective_qr == 0 {
        return 0.0;
    }
    // Keep both denominators positive: peak_qr >= 1, falloff > peak_qr.
    let peak_qr = predictor.q_running_penalty_peak_qr.max(1) as f32;
    let falloff = predictor
        .q_running_penalty_falloff_qr
        .max(predictor.q_running_penalty_peak_qr.saturating_add(1)) as f32;
    let qr_f = effective_qr as f32;
    if qr_f >= falloff {
        return 0.0;
    }
    if qr_f <= peak_qr {
        // Rising edge: 0 at effective_qr=0, peak at peak_qr.
        peak * qr_f / peak_qr
    } else {
        // Falling edge: peak at peak_qr, 0 at falloff_qr.
        peak * (falloff - qr_f) / (falloff - peak_qr)
    }
}

/// The intermediate values of the queue-cost calculation, for
/// `predicted_wait_ms` and for logging and the observation CSV.
#[derive(Debug, Clone)]
pub struct QueueCostBreakdown {
    /// Per-entry queue rate after the cold-saturation check. Equals
    /// `cfg.queue_ms_per_entry` in the common case; promoted to
    /// `cfg.saturated_cold_ms_per_entry` when the worker has no
    /// completion history AND looks cache-saturated AND has waiting
    /// queue depth.
    pub q_rate_ms_per_entry: f32,
    /// `q_rate_ms_per_entry * w.queue_waiting`. Diagnostic only, kept in
    /// the CSV.
    pub q_ms_constant: f32,
    /// Inflight position costs plus `unknown_q_count * q_rate`. Same as
    /// `q_ms`; named to match the CSV column.
    pub q_ms_workload: f32,
    /// q_ms used by the predictor.
    pub q_ms: f32,
    /// Total uncached blocks across our inflight dispatches, for the CSV.
    pub inflight_workload_blocks: u32,
}

/// `(progress, discount)` for one inflight slot, where `discount` is the
/// fraction of its prefill cost still in the future (1 at progress 0, 0 once
/// prefill is done; decode cost is `decode_contention_ms`).
///
/// `position` is 1-indexed, oldest first. Positions beyond `n` (batch
/// capacity) haven't been admitted, so their elapsed time is queueing, not
/// progress: discount 1. With nothing to prefill, discount is 0.
fn inflight_discount(
    position: u32,
    n: u32,
    elapsed_ms: f32,
    estimated_prefill_ms: f32,
) -> (f32, f32) {
    if position > n {
        return (0.0, 1.0);
    }
    if estimated_prefill_ms <= 0.0 {
        return (1.0, 0.0);
    }
    let progress = (elapsed_ms / estimated_prefill_ms).clamp(0.0, 1.0);
    let discount = (1.0 - progress).clamp(0.0, 1.0);
    (progress, discount)
}

pub fn compute_queue_cost(
    w: &WorkerScore,
    predictor: &PredictorConfig,
    inflight_summaries: &[InflightSummary],
    now: Instant,
) -> QueueCostBreakdown {
    let mut q_rate_ms_per_entry = predictor.queue_ms_per_entry;
    if w.completion_rate_per_sec == 0.0
        && w.kv_cache_usage >= predictor.cache_pressure_saturated_threshold
        && w.queue_waiting > 0
        && predictor.saturated_cold_ms_per_entry > q_rate_ms_per_entry
    {
        // Cold-saturation fallback: no completions, a saturated cache, and
        // a queue.
        q_rate_ms_per_entry = predictor.saturated_cold_ms_per_entry;
    }

    let q_ms_constant = q_rate_ms_per_entry * w.queue_waiting as f32;

    let inflight_count = inflight_summaries.len() as u32;
    let inflight_workload_blocks: u32 = inflight_summaries
        .iter()
        .map(|s| s.uncached_at_dispatch as u64)
        .sum::<u64>()
        .min(u32::MAX as u64) as u32;
    let unknown_q_count = w.queue_waiting.saturating_sub(inflight_count);

    // A nonzero queue means the batch is at capacity, so treat every
    // inflight as not yet admitted.
    let capacity = if w.queue_waiting > 0 {
        0
    } else {
        predictor.inflight_concurrent_capacity
    };

    let mut position_cost_ms = 0.0_f32;
    for (idx, summary) in inflight_summaries.iter().enumerate() {
        let position = (idx as u32).saturating_add(1);
        // Uses the inflight's own prompt length, not ours.
        let estimated_prefill_ms = prefill_ms_for(
            predictor,
            summary.uncached_at_dispatch,
            summary.total_at_dispatch,
        );
        let elapsed_ms = now
            .checked_duration_since(summary.dispatched_at)
            .map(|d| d.as_millis() as f32)
            .unwrap_or(0.0);
        let (_progress, discount) =
            inflight_discount(position, capacity, elapsed_ms, estimated_prefill_ms);
        position_cost_ms += discount * estimated_prefill_ms;
    }

    let unknown_cost_ms = unknown_q_count as f32 * q_rate_ms_per_entry;
    let q_ms_workload = position_cost_ms + unknown_cost_ms;
    let q_ms = q_ms_workload;

    QueueCostBreakdown {
        q_rate_ms_per_entry,
        q_ms_constant,
        q_ms_workload,
        q_ms,
        inflight_workload_blocks,
    }
}

/// Outcome of a successful pick. `replication_fired` is true when the
/// cache-replication override replaced the normal pick (see
/// [`cache_replication_override`]); recorded in the CSV.
pub struct PickResult<'a> {
    /// The position of `worker` in the `scores` the pick ran over.
    pub index: usize,
    pub worker: &'a WorkerScore,
    pub replication_fired: bool,
}

/// Everything one pick scores over. The three slices are parallel to
/// `scores`.
#[derive(Clone, Copy)]
pub struct PickInputs<'a> {
    pub scores: &'a [WorkerScore],
    pub predictor: &'a PredictorConfig,
    pub picker: &'a PickerConfig,
    /// Per-worker in-flight pick counts, used only as a tie-breaker within
    /// the top-K pool; zeros when in-flight tracking is disabled.
    pub inflight: &'a [u32],
    pub inflight_workload: &'a [Vec<InflightSummary>],
    /// The raw radix-tree match per worker, before in-flight credit.
    pub kv_blocks: &'a [u32],
    pub now: Instant,
}

/// Pick a worker, or `None` when `scores` is empty.
pub fn pick_endpoint<'a>(inputs: &PickInputs<'a>) -> Option<PickResult<'a>> {
    let PickInputs {
        scores,
        predictor,
        picker,
        inflight,
        inflight_workload,
        kv_blocks,
        now,
    } = *inputs;
    if scores.is_empty() {
        return None;
    }
    debug_assert_eq!(
        scores.len(),
        inflight.len(),
        "inflight must be parallel to scores"
    );
    debug_assert_eq!(
        scores.len(),
        inflight_workload.len(),
        "inflight_workload must be parallel to scores"
    );

    // (index, predicted wait_ms). Every worker here already serves the
    // requested model.
    let mut scored: Vec<(usize, f32)> = scores
        .iter()
        .enumerate()
        .map(|(i, w)| {
            (
                i,
                predicted_wait_ms(w, predictor, &inflight_workload[i], now).total_ms(),
            )
        })
        .collect();

    // Top-K random, against dogpiling: sort by predicted wait, take the best
    // `effective_k` plus anything tied at the cutoff, and pick uniformly.
    // Without the tie extension, a cold fleet with identical predictions
    // would send every request to the first worker.
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let n = scored.len();
    let effective_k = std::cmp::max(
        picker.top_k_min as usize,
        ((n as f32) * picker.top_k_fraction).ceil() as usize,
    )
    .max(1)
    .min(n);
    // Cutoff: the larger of the K-th best predicted_ms and the best plus
    // `tie_tolerance_ms`.
    let cutoff_wait = scored[effective_k - 1]
        .1
        .max(scored[0].1 + picker.tie_tolerance_ms);
    let pool_end = scored
        .iter()
        .position(|(_, w)| *w > cutoff_wait)
        .unwrap_or(n);
    let pool = &scored[..pool_end];

    // Within the pool, prefer the lowest in-flight count, to break up
    // bursts. A no-op when in-flight tracking is disabled.
    let min_inflight_in_pool = pool.iter().map(|(i, _)| inflight[*i]).min().unwrap_or(0);
    let eligible: Vec<(usize, f32)> = pool
        .iter()
        .filter(|(i, _)| inflight[*i] == min_inflight_in_pool)
        .copied()
        .collect();

    // Uniform random pick within the eligible set.
    let chosen_idx = if eligible.len() == 1 {
        eligible[0].0
    } else {
        let mut buf = [0u8; 8];
        match getrandom::getrandom(&mut buf) {
            Ok(()) => {
                let r = u64::from_le_bytes(buf) as usize;
                eligible[r % eligible.len()].0
            }
            Err(err) => {
                tracing::warn!(
                    error = &err as &dyn std::error::Error,
                    "getrandom failed; picking the first eligible worker"
                );
                eligible[0].0
            }
        }
    };

    let final_idx = cache_replication_override(scores, picker, kv_blocks, chosen_idx, &scored);

    Some(PickResult {
        index: final_idx,
        worker: &scores[final_idx],
        replication_fired: final_idx != chosen_idx,
    })
}

/// Apply the cache-replication override. Given `chosen_idx` (the
/// picker's normal pick after top-K + inflight tie-break), decide
/// whether to swap it for a cold-idle worker so the fleet's prefix
/// cache spreads to a second worker. Returns the (possibly-replaced)
/// index into `scores`.
///
/// The override fires only when *all* of:
/// 1. `cfg.replication_threshold_pct > 0` and
///    `cfg.replication_warm_target > 1` (feature enabled).
/// 2. The picker's chosen worker is committed-warm: its raw
///    radix-tree match (`kv_blocks`) covers at least the fraction
///    of `total_blocks` configured by
///    `cfg.replication_warm_threshold_fraction`.
/// 3. Fewer than `cfg.replication_warm_target` workers are
///    committed-warm in the current snapshot.
/// 4. There's at least one candidate that's neither committed-warm
///    (raw `kv_blocks` below the threshold) nor about to be warm
///    (post-cache-credit `matched_blocks` below the threshold), so we
///    don't replicate to a worker already prefilling the same prefix.
/// 5. The cheapest such candidate's predicted total is within
///    `best_ms * (1 + cfg.replication_threshold_pct/100)` of the
///    originally-picked worker's predicted total. A percentage, so it
///    scales with prompt size.
///
/// Otherwise returns `chosen_idx` unchanged. A swap is logged at DEBUG.
fn cache_replication_override(
    scores: &[WorkerScore],
    picker: &PickerConfig,
    kv_blocks_per_worker: &[u32],
    chosen_idx: usize,
    scored: &[(usize, f32)],
) -> usize {
    if picker.replication_threshold_pct <= 0.0 || picker.replication_warm_target <= 1 {
        return chosen_idx;
    }
    debug_assert_eq!(
        scores.len(),
        kv_blocks_per_worker.len(),
        "kv_blocks_per_worker must be parallel to scores"
    );
    let best = &scores[chosen_idx];
    let total = best.total_blocks as f32;
    if total <= 0.0 {
        return chosen_idx;
    }
    let fraction = picker.replication_warm_threshold_fraction.clamp(0.0, 1.0);
    let warm_blocks = (total * fraction).round() as u32;
    let raw_kv = |idx: usize| kv_blocks_per_worker[idx];
    if raw_kv(chosen_idx) < warm_blocks {
        // Best is a cold pick; nothing to replicate.
        return chosen_idx;
    }
    let warm_count = (0..scores.len())
        .filter(|i| raw_kv(*i) >= warm_blocks)
        .count();
    if warm_count >= picker.replication_warm_target as usize {
        // Already replicated enough.
        return chosen_idx;
    }
    // Reuse the predictions from `scored` rather than re-running the
    // predictor.
    let predicted_of = |idx: usize| -> f32 {
        scored
            .iter()
            .find(|(i, _)| *i == idx)
            .map(|(_, p)| *p)
            .unwrap_or(f32::INFINITY)
    };
    // Candidates: neither committed-warm nor warming up (condition 4).
    let cheapest_candidate = scores
        .iter()
        .enumerate()
        .filter(|(i, w)| raw_kv(*i) < warm_blocks && w.matched_blocks < warm_blocks)
        .map(|(i, _)| (i, predicted_of(i)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let Some((cand_idx, cand_ms)) = cheapest_candidate else {
        return chosen_idx;
    };
    let best_ms = predicted_of(chosen_idx);
    let extra_cost = cand_ms - best_ms;
    // `replication_threshold_pct` is in percentage points.
    let max_extra = best_ms.max(0.0) * (picker.replication_threshold_pct / 100.0);
    if extra_cost > max_extra {
        return chosen_idx;
    }
    let cand = &scores[cand_idx];
    tracing::debug!(
        from_endpoint = %best.endpoint,
        from_predicted_ms = best_ms,
        to_endpoint = %cand.endpoint,
        to_predicted_ms = cand_ms,
        extra_cost_ms = extra_cost,
        max_extra_ms = max_extra,
        replication_threshold_pct = picker.replication_threshold_pct,
        warm_count,
        target = picker.replication_warm_target,
        warm_blocks_threshold = warm_blocks,
        total_blocks = best.total_blocks,
        warm_threshold_fraction = fraction,
        "redirecting to replicate the prefix cache"
    );
    cand_idx
}

/// Render a table of every scored worker with the predictor's per-term
/// breakdown, for the pick path's TRACE log. Rows are sorted by `name` so
/// consecutive tables line up; the `chosen_endpoint` row is marked with `*`.
/// A leading `weights:` line shows the configuration shared by all rows.
///
/// Columns:
/// - `expect_ms` = the predicted TTFT, the sum of the `_ms` columns
/// - `overlap/total` shows the layered expected-overlap value the
///   picker scored on; `kv_blocks` shows the raw radix-tree match for
///   comparison. At α=1 with no inflight overlap they're identical.
/// - `q_wait` is the worker's scraped `queue_waiting`; `q_inflight`
///   is our shadow count of picks not-yet-observed-as-TTFT;
///   `q_workload` is the sum of uncached blocks across our inflight
///   dispatches (the workload model's input).
///
/// `q_inflight` only breaks ties in `pick_endpoint`; it isn't part of
/// `expect_ms`.
pub fn format_score_table(
    inputs: &PickInputs<'_>,
    expected_overlap_alpha: f32,
    chosen_endpoint: Option<&str>,
) -> String {
    let PickInputs {
        scores,
        predictor,
        picker,
        inflight,
        inflight_workload,
        kv_blocks,
        now,
    } = *inputs;
    if scores.is_empty() {
        return String::from("(no workers scored)");
    }
    debug_assert_eq!(
        scores.len(),
        inflight.len(),
        "inflight must be parallel to scores"
    );
    debug_assert_eq!(
        scores.len(),
        kv_blocks.len(),
        "kv_blocks must be parallel to scores"
    );
    debug_assert_eq!(
        scores.len(),
        inflight_workload.len(),
        "inflight_workload must be parallel to scores"
    );

    // Sort by `name`, carrying the parallel values along.
    let mut sorted: Vec<(&WorkerScore, WaitPrediction, u32, u32, u32)> = scores
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let workload = &inflight_workload[i];
            let workload_blocks: u32 = workload
                .iter()
                .map(|s| s.uncached_at_dispatch as u64)
                .sum::<u64>()
                .min(u32::MAX as u64) as u32;
            (
                w,
                predicted_wait_ms(w, predictor, workload, now),
                inflight[i],
                kv_blocks[i],
                workload_blocks,
            )
        })
        .collect();
    sorted.sort_by(|a, b| a.0.name.cmp(&b.0.name));

    let mut out = String::with_capacity(256 + scores.len() * 200);
    let _ = writeln!(
        out,
        "weights: expected_overlap_alpha={}  inflight_concurrent_capacity={}  forward_pass_ms={}  inflight_tracking={}  tie_tolerance_ms={}",
        expected_overlap_alpha,
        predictor.inflight_concurrent_capacity,
        predictor.forward_pass_ms,
        picker.inflight_tracking,
        picker.tie_tolerance_ms,
    );
    let _ = writeln!(
        out,
        "{:<2} {:<20} {:<22} {:>10} {:>8} {:>8} {:>11} {:>9} {:>10} {:>10} {:>13} {:>10} {:>7} {:>11} {:>6} {:>11} {:>10} {:>8}",
        "",
        "name",
        "endpoint",
        "expect_ms",
        "base_ms",
        "q_ms",
        "prefill_ms",
        "cache_ms",
        "q_run_ms",
        "dec_cont",
        "overlap/total",
        "kv_blocks",
        "q_wait",
        "q_inflight",
        "q_run",
        "q_workload",
        "kv_cache%",
        "compl/s",
    );
    for (w, pred, q_inflight, kv, q_workload) in &sorted {
        let marker = if Some(w.endpoint.as_str()) == chosen_endpoint {
            "*"
        } else {
            " "
        };
        let _ = writeln!(
            out,
            "{:<2} {:<20} {:<22} {:>10.1} {:>8.1} {:>8.1} {:>11.1} {:>9.1} {:>10.1} {:>10.1} {:>13} {:>10} {:>7} {:>11} {:>6} {:>11} {:>9.1}% {:>8.2}",
            marker,
            truncate(&w.name, 20),
            truncate(&w.endpoint, 22),
            pred.total_ms(),
            pred.base_ms,
            pred.q_ms,
            pred.prefill_ms,
            pred.cache_ms,
            pred.q_running_ms,
            pred.decode_contention_ms,
            format!("{}/{}", w.matched_blocks, w.total_blocks),
            kv,
            w.queue_waiting,
            q_inflight,
            w.queue_running,
            q_workload,
            w.kv_cache_usage * 100.0,
            w.completion_rate_per_sec,
        );
    }
    // Trim trailing newline so tracing's own newline doesn't double up.
    if out.ends_with('\n') {
        out.pop();
    }
    out
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max { s } else { &s[..max] }
}

#[cfg(test)]
mod tests;
