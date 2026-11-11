use std::error::Error as StdError;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::pool::WorkerContext;
use crate::workers::Engine;
use dashmap::DashMap;

// vLLM and SGLang prefix metric names with `<engine>:`, which
// prometheus-parse 0.2.5 rejects. The scraped body is rewritten to `vllm_` /
// `sglang_` before parsing, and the names below use that form.

/// Which Prometheus metric names to read for each field of `MetricsSample`.
struct MetricNames {
    /// Candidate names for the "requests waiting in queue" gauge. First match
    /// wins, so put preferred names first.
    waiting: &'static [&'static str],
    /// Candidate names for the "currently running" gauge.
    running: &'static [&'static str],
    /// Candidate names for "swapped to CPU" (vLLM only).
    swapped: &'static [&'static str],
    /// Candidate names for KV-cache utilization (0.0..1.0).
    kv_cache_usage: &'static [&'static str],
    /// If `Some(name)`, only accept samples whose label `name` equals the
    /// worker's configured `model_name`. If `None`, take the first matching
    /// sample regardless of labels.
    model_label: Option<&'static str>,
    /// Candidate counter names for "successfully completed requests."
    /// Summed across all label combinations (e.g. `finished_reason`) matching
    /// the worker's model_label. First match wins, so put preferred names
    /// first. Used to derive `completion_rate_per_sec` as a delta over the
    /// scrape interval.
    completions: &'static [&'static str],
    /// Candidate counter names for "prefix-cache lookups" (denominator).
    /// vLLM ≥ 0.6 exposes this as a monotonic counter. Pair with
    /// `prefix_cache_hits` to derive a windowed hit rate.
    prefix_cache_queries: &'static [&'static str],
    /// Candidate counter names for "prefix-cache lookups that hit"
    /// (numerator). Paired with `prefix_cache_queries`.
    prefix_cache_hits: &'static [&'static str],
    /// Candidate gauge names that express the hit rate directly as a
    /// 0..1 ratio. SGLang exposes `sglang:cache_hit_rate` this way.
    /// Consulted only when the counter pair above misses.
    prefix_cache_hit_rate_gauge: &'static [&'static str],
}

// Union of vLLM and SGLang candidate names, which don't collide; the scraper
// picks whichever the engine emits.
const ALL_NAMES: MetricNames = MetricNames {
    waiting: &["vllm_num_requests_waiting", "sglang_num_queue_reqs"],
    running: &["vllm_num_requests_running", "sglang_num_running_reqs"],
    // SGLang has no CPU-swap concept; lookup just fails silently for it.
    swapped: &["vllm_num_requests_swapped"],
    // vLLM v0 named this `gpu_cache_usage_perc`; v1 renamed it to
    // `kv_cache_usage_perc`. Accept either, prefer v1. SGLang uses
    // `sglang_token_usage`.
    kv_cache_usage: &[
        "vllm_kv_cache_usage_perc",
        "vllm_gpu_cache_usage_perc",
        "sglang_token_usage",
    ],
    // Both engines label samples with `model_name`.
    model_label: Some("model_name"),
    completions: &[
        // Prefer direct success counters; fall back to histogram observation
        // counts (`_count` is a counter of completed requests).
        "vllm_request_success_total",
        "vllm_e2e_request_latency_seconds_count",
        "sglang_num_requests_total",
        "sglang_e2e_request_latency_seconds_count",
    ],
    // vLLM ≥ 0.6. Defensively include the singular form some
    // intermediate versions emitted.
    prefix_cache_queries: &[
        "vllm_prefix_cache_queries_total",
        "vllm_prefix_cache_query_total",
    ],
    prefix_cache_hits: &["vllm_prefix_cache_hits_total"],
    // SGLang publishes the rate directly as a gauge (0..1). Only used
    // when the vLLM counter pair above isn't present.
    prefix_cache_hit_rate_gauge: &["sglang_cache_hit_rate"],
};

/// Best-effort detection of which engine a `/v1/models` response was served
/// by, from `data[*].owned_by`. `None` if neither matches; informational
/// only.
fn engine_from_owned_by(owned_by: &str) -> Option<Engine> {
    match owned_by {
        "vllm" => Some(Engine::Vllm),
        "sglang" => Some(Engine::Sglang),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsSample {
    pub waiting: u32,
    pub running: u32,
    pub swapped: u32,
    pub kv_cache_usage: f32,
    /// `waiting`, averaged over `COMPLETION_RATE_WINDOW`.
    ///
    /// What the policy's `queuedPrompts` predicate reads. The picker uses the
    /// instantaneous `waiting`, since it predicts a dispatch happening now.
    pub waiting_mean: f32,
    /// `kv_cache_usage`, averaged over the same window. What `kvCacheUsage`
    /// reads. Still a 0.0..1.0 ratio.
    pub kv_cache_usage_mean: f32,
    /// Cumulative successful-completion counter at the time of this scrape
    /// (sum across all label combinations matching `model_label`). The
    /// per-sec rate is derived from the delta against the previous sample.
    pub completions_total: u64,
    /// Successfully-completed requests per second, averaged over a sliding
    /// window (see `COMPLETION_RATE_WINDOW`), since a single-tick delta is
    /// usually 0 under low load. `0.0` until there are two samples, or when
    /// the engine lacks the counter.
    pub completion_rate_per_sec: f32,
    /// Cumulative prefix-cache hit counter (vLLM
    /// `prefix_cache_hits_total`). `0` when the engine doesn't expose it
    /// (older vLLM, SGLang).
    pub prefix_cache_hits_total: u64,
    /// Cumulative prefix-cache query counter (vLLM
    /// `prefix_cache_queries_total`). Pair with `prefix_cache_hits_total`
    /// to derive the windowed rate. `0` when the engine doesn't expose
    /// this metric.
    pub prefix_cache_queries_total: u64,
    /// Engine-reported prefix-cache hit rate, 0..1. Sourced from:
    ///   - vLLM counter pair: `Δhits / Δqueries` over the same window as
    ///     `completion_rate_per_sec`;
    ///   - SGLang gauge `sglang:cache_hit_rate`, verbatim.
    ///
    /// `0.0` when neither source is present (older engine, cold start
    /// before two samples accumulated, or zero queries in the window).
    pub prefix_cache_hit_rate: f32,
    pub scraped_at: Option<Instant>,
}

/// One entry from `/v1/models`'s `data[]`, with only the fields we use.
#[derive(Clone, Debug, Default)]
pub struct ModelEntry {
    /// `data[*].id`.
    pub name: String,
    /// `data[*].max_model_len`. `None` when the engine omitted it (some
    /// embedding-only models don't carry it).
    pub max_model_len: Option<u32>,
}

/// Worker-scoped cache of the engine's `/v1/models` response.
#[derive(Clone, Debug, Default)]
pub struct ModelsInfo {
    pub models: Vec<ModelEntry>,
}

/// Sliding-window samples of `completions_total` per worker, for
/// `completion_rate_per_sec`.
#[derive(Clone, Debug, Default)]
struct CompletionHistory {
    /// `(scraped_at, completions_total)` from each successful scrape. Bounded
    /// by both count and age; see `COMPLETION_RATE_WINDOW`.
    samples: std::collections::VecDeque<(Instant, u64)>,
}

/// Per-worker sliding-window history of vLLM's prefix-cache counters, like
/// `CompletionHistory`, so the hit rate is `Δhits / Δqueries` over the window.
#[derive(Clone, Debug, Default)]
struct PrefixCacheHistory {
    /// `(scraped_at, hits_total, queries_total)` from each scrape that
    /// found the vLLM counter pair. Empty for SGLang and pre-0.6 vLLM.
    samples: std::collections::VecDeque<(Instant, u64, u64)>,
}

/// Per-worker sliding-window history of the two saturation gauges, which are
/// averaged rather than differentiated.
///
/// `num_requests_waiting` is bursty, so a policy threshold on the
/// instantaneous value would react to transients; the mean makes it mean
/// sustained pressure. `kv_cache_usage` shares the window for consistency.
#[derive(Clone, Debug, Default)]
struct GaugeHistory {
    /// `(scraped_at, waiting, kv_cache_usage)`, over `COMPLETION_RATE_WINDOW`.
    samples: std::collections::VecDeque<(Instant, u32, f32)>,
}

/// Sliding window for the windowed rates and means. Long enough that 1 req
/// every 10s registers (~0.1 req/s), short enough to track load changes.
const COMPLETION_RATE_WINDOW: Duration = Duration::from_secs(30);

pub struct MetricsRegistry {
    by_worker: DashMap<u64, MetricsSample>,
    /// `/v1/models` data, refreshed less often than `by_worker`. Cloned out on
    /// read so no DashMap ref is held across an await.
    models_by_worker: DashMap<u64, ModelsInfo>,
    /// Completion-counter history, for the windowed rate.
    completion_history: DashMap<u64, CompletionHistory>,
    /// vLLM prefix-cache counter history, for the windowed hit rate.
    prefix_cache_history: DashMap<u64, PrefixCacheHistory>,
    /// Saturation gauge history, for the windowed means.
    gauge_history: DashMap<u64, GaugeHistory>,
    startup: Instant,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            by_worker: DashMap::new(),
            models_by_worker: DashMap::new(),
            completion_history: DashMap::new(),
            prefix_cache_history: DashMap::new(),
            gauge_history: DashMap::new(),
            startup: Instant::now(),
        }
    }

    /// Push the latest scrape sample into the per-worker history and return
    /// the windowed `completion_rate_per_sec`: the slope over the samples in
    /// the window, so a single sample yields 0.0 and two samples 30s apart
    /// yield `(c2 - c1) / 30`.
    fn record_and_compute_rate(&self, worker_id_num: u64, now: Instant, counter: u64) -> f32 {
        let mut entry = self.completion_history.entry(worker_id_num).or_default();
        let hist = entry.value_mut();
        hist.samples.push_back((now, counter));
        // Evict anything older than the window (always keep at least one
        // sample so an idle worker doesn't end up with an empty deque).
        while hist.samples.len() > 1
            && hist
                .samples
                .front()
                .is_some_and(|s| now.saturating_duration_since(s.0) > COMPLETION_RATE_WINDOW)
        {
            hist.samples.pop_front();
        }
        if hist.samples.len() < 2 {
            return 0.0;
        }
        let (Some(&(t0, c0)), Some(&(t1, c1))) = (hist.samples.front(), hist.samples.back()) else {
            return 0.0;
        };
        let dt = t1.saturating_duration_since(t0).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        // saturating_sub handles counter resets on engine restart.
        let delta = c1.saturating_sub(c0);
        (delta as f64 / dt) as f32
    }

    /// Push the latest scrape into the per-worker prefix-cache history
    /// and return the windowed hit rate, `Δhits / Δqueries`. `0.0` until there
    /// are two samples, or when no queries occurred in the window.
    fn record_and_compute_hit_rate(
        &self,
        worker_id_num: u64,
        now: Instant,
        hits: u64,
        queries: u64,
    ) -> f32 {
        let mut entry = self.prefix_cache_history.entry(worker_id_num).or_default();
        let hist = entry.value_mut();
        hist.samples.push_back((now, hits, queries));
        while hist.samples.len() > 1
            && hist
                .samples
                .front()
                .is_some_and(|s| now.saturating_duration_since(s.0) > COMPLETION_RATE_WINDOW)
        {
            hist.samples.pop_front();
        }
        if hist.samples.len() < 2 {
            return 0.0;
        }
        let (Some(&(_, h0, q0)), Some(&(_, h1, q1))) = (hist.samples.front(), hist.samples.back())
        else {
            return 0.0;
        };
        let dh = h1.saturating_sub(h0);
        let dq = q1.saturating_sub(q0);
        if dq == 0 {
            return 0.0;
        }
        // Clamp to [0, 1] against counter quirks.
        ((dh as f64 / dq as f64) as f32).clamp(0.0, 1.0)
    }

    /// Push this scrape's saturation gauges into the per-worker history and
    /// return their means over the window.
    ///
    /// A mean rather than a max (which would extend a blip across the
    /// window) or a median (which would erase a real but short spike).
    ///
    /// A single sample is a valid mean, so there is no `0.0` warm-up that
    /// would make a new worker look idle.
    fn record_and_compute_means(
        &self,
        worker_id_num: u64,
        now: Instant,
        waiting: u32,
        kv_cache_usage: f32,
    ) -> (f32, f32) {
        let mut entry = self.gauge_history.entry(worker_id_num).or_default();
        let hist = entry.value_mut();
        hist.samples.push_back((now, waiting, kv_cache_usage));
        while hist.samples.len() > 1
            && hist
                .samples
                .front()
                .is_some_and(|s| now.saturating_duration_since(s.0) > COMPLETION_RATE_WINDOW)
        {
            hist.samples.pop_front();
        }
        let n = hist.samples.len() as f64;
        let (qs, ks) = hist.samples.iter().fold((0.0f64, 0.0f64), |(q, k), s| {
            (q + s.1 as f64, k + s.2 as f64)
        });
        ((qs / n) as f32, ((ks / n) as f32).clamp(0.0, 1.0))
    }

    pub fn latest(&self, worker_id_num: u64) -> MetricsSample {
        self.sample(worker_id_num).unwrap_or_default()
    }

    /// The last successful scrape, or `None` if this worker has never been
    /// scraped. Unlike [`Self::latest`], which returns an all-zero (idle)
    /// sample, so use this for decisions.
    pub fn sample(&self, worker_id_num: u64) -> Option<MetricsSample> {
        self.by_worker.get(&worker_id_num).map(|e| *e.value())
    }

    /// Record `sample` as this worker's latest scrape. Test-only.
    #[cfg(any(test, feature = "test-util"))]
    pub fn record_sample_for_test(&self, worker_id_num: u64, sample: MetricsSample) {
        self.by_worker.insert(worker_id_num, sample);
    }

    /// Snapshot of the worker's served-models list. Empty `Default` when
    /// the worker isn't being scraped for /v1/models (no http_url) or no
    /// refresh has yet succeeded.
    pub fn models(&self, worker_id_num: u64) -> ModelsInfo {
        self.models_by_worker
            .get(&worker_id_num)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    /// Drop all per-worker state for a removed worker, so a replacement
    /// doesn't inherit its history.
    pub fn forget(&self, worker_id_num: u64) {
        self.by_worker.remove(&worker_id_num);
        self.gauge_history.remove(&worker_id_num);
        self.models_by_worker.remove(&worker_id_num);
        self.completion_history.remove(&worker_id_num);
        self.prefix_cache_history.remove(&worker_id_num);
    }

    /// Milliseconds since the last successful scrape for this worker. If the
    /// worker has never been scraped (no `metrics_url`, or every attempt has
    /// failed), returns the time elapsed since kinference started.
    pub fn age_ms_for(&self, worker_id_num: u64) -> u64 {
        let now = Instant::now();
        match self
            .by_worker
            .get(&worker_id_num)
            .and_then(|e| e.value().scraped_at)
        {
            Some(t) => now.saturating_duration_since(t).as_millis() as u64,
            None => now.saturating_duration_since(self.startup).as_millis() as u64,
        }
    }
}

/// Kick a one-shot `/v1/models` fetch as soon as a worker is registered, so
/// it is routable before the poller's first periodic refresh. Records the
/// served models and the detected engine.
///
/// Fire-and-forget: on failure, the periodic refresh catches up.
pub fn spawn_one_shot_warmup(ctx: WorkerContext, samples: Arc<MetricsRegistry>) {
    let WorkerContext {
        cfg,
        registry: workers,
        eviction_tx,
        models: index,
        pool_id,
        pool_name,
        ..
    } = ctx;
    let Some(http_url) = cfg.http_url.clone() else {
        return; // can't warm up without /v1/models reachability
    };
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    worker_id = cfg.worker_id_str,
                    error = &err as &dyn StdError,
                    "failed to build the warmup HTTP client"
                );
                return;
            }
        };
        match fetch_models(&client, &http_url).await {
            Ok((models, detected_engine)) => {
                let names: Vec<String> = models.iter().map(|m| m.name.clone()).collect();
                let engine_str = detected_engine
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                tracing::debug!(
                    worker_id = cfg.worker_id_str,
                    model_count = names.len(),
                    engine = %engine_str,
                    "fetched /v1/models"
                );
                match workers.set_served_models(cfg.worker_id_num, &names) {
                    // Widened/Narrowed are routable, e.g. a LoRA adapter
                    // rollout: index what this worker serves rather than evict.
                    crate::workers::ServedModelsVerdict::Widened { added } => {
                        tracing::info!(
                            worker_id = cfg.worker_id_str,
                            ?added,
                            "worker serves models new to its worker set"
                        );
                        index.associate(pool_id, &pool_name, &names);
                    }
                    crate::workers::ServedModelsVerdict::Narrowed { missing } => {
                        tracing::info!(
                            worker_id = cfg.worker_id_str,
                            ?missing,
                            "worker lacks models that its worker set serves"
                        );
                        index.associate(pool_id, &pool_name, &names);
                    }
                    crate::workers::ServedModelsVerdict::Accepted
                    | crate::workers::ServedModelsVerdict::Empty => {
                        index.associate(pool_id, &pool_name, &names);
                    }
                    crate::workers::ServedModelsVerdict::Mismatch { expected, observed } => {
                        let _ = eviction_tx.send(crate::eviction::EvictionRequest {
                            worker_id_num: cfg.worker_id_num,
                            reason: crate::eviction::EvictionReason::ModelMismatch {
                                expected,
                                observed,
                            },
                        });
                        return;
                    }
                }
                if let Some(eng) = detected_engine {
                    workers.set_engine(cfg.worker_id_num, eng);
                }
                samples
                    .models_by_worker
                    .insert(cfg.worker_id_num, ModelsInfo { models });
            }
            Err(err) => {
                tracing::debug!(
                    worker_id = cfg.worker_id_str,
                    http_url,
                    error = &err as &dyn StdError,
                    "/v1/models fetch failed"
                );
            }
        }
    });
}

/// Scrape the worker's `/metrics` every `poll_ms` into `samples`, and
/// refresh its `/v1/models` every `models_refresh_secs`.
pub fn spawn_metrics_poller(
    ctx: WorkerContext,
    samples: Arc<MetricsRegistry>,
    poll_ms: u64,
    models_refresh_secs: u64,
) {
    let WorkerContext {
        cfg,
        registry: workers,
        eviction_tx,
        models: index,
        pool_id,
        pool_name,
        cancel,
        ..
    } = ctx;
    let Some(metrics_url) = cfg.metrics_url.clone() else {
        tracing::info!(
            worker_id = cfg.worker_id_str,
            "no metrics URL; load signals stay at zero"
        );
        return;
    };

    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(err) => {
                tracing::error!(
                    worker_id = cfg.worker_id_str,
                    error = &err as &dyn StdError,
                    "failed to build the metrics HTTP client"
                );
                return;
            }
        };

        // At least 1ms, or the loop would spin.
        let poll_ms = poll_ms.max(1);
        let mut tick = tokio::time::interval(Duration::from_millis(poll_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut first_scrape = true;
        let mut warned_about_label = false;

        // The model_name label filter for /metrics: from config at first,
        // then from each /v1/models refresh (empty for multi-model workers,
        // which aggregates across models).
        let mut current_model_name: String = cfg.model_name.clone();

        // /v1/models refresh cadence in poll ticks: on the first tick and
        // every N after.
        let refresh_every: u64 = ((models_refresh_secs * 1000) / poll_ms).max(1);
        let mut n: u64 = 0;
        // Whether the current run of failures has been logged; reset on
        // success, so a failure after a recovery warns again.
        let mut warned_about_models = false;
        let mut warned_about_scrape = false;

        tracing::debug!(
            worker_id = cfg.worker_id_str,
            url = metrics_url,
            poll_ms,
            models_refresh_secs,
            "metrics poller started"
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {}
            }

            // /v1/models refresh, if http_url is set, every N ticks. Bounded by
            // the client timeout.
            if let Some(http_url) = cfg.http_url.as_deref()
                && n.is_multiple_of(refresh_every)
            {
                match fetch_models(&client, http_url).await {
                    Ok((models, detected_engine)) => {
                        // Index the served models so requests route here.
                        let names: Vec<String> = models.iter().map(|m| m.name.clone()).collect();
                        match workers.set_served_models(cfg.worker_id_num, &names) {
                            // As in the warmup path: only a conflict is fatal.
                            crate::workers::ServedModelsVerdict::Widened { added } => {
                                tracing::info!(
                                    worker_id = cfg.worker_id_str,
                                    ?added,
                                    "worker serves models new to its worker set"
                                );
                                index.associate(pool_id, &pool_name, &names);
                            }
                            crate::workers::ServedModelsVerdict::Narrowed { missing } => {
                                tracing::info!(
                                    worker_id = cfg.worker_id_str,
                                    ?missing,
                                    "worker lacks models that its worker set serves"
                                );
                                index.associate(pool_id, &pool_name, &names);
                            }
                            crate::workers::ServedModelsVerdict::Accepted
                            | crate::workers::ServedModelsVerdict::Empty => {
                                index.associate(pool_id, &pool_name, &names);
                            }
                            crate::workers::ServedModelsVerdict::Mismatch {
                                expected,
                                observed,
                            } => {
                                let _ = eviction_tx.send(crate::eviction::EvictionRequest {
                                    worker_id_num: cfg.worker_id_num,
                                    reason: crate::eviction::EvictionReason::ModelMismatch {
                                        expected,
                                        observed,
                                    },
                                });
                                return;
                            }
                        }
                        if let Some(eng) = detected_engine {
                            workers.set_engine(cfg.worker_id_num, eng);
                        }
                        // Single model: filter by its name. Otherwise no
                        // filter.
                        current_model_name = if names.len() == 1 {
                            names[0].clone()
                        } else {
                            String::new()
                        };
                        samples
                            .models_by_worker
                            .insert(cfg.worker_id_num, ModelsInfo { models });
                        warned_about_models = false;
                    }
                    Err(err) => {
                        if !warned_about_models {
                            tracing::warn!(
                                worker_id = cfg.worker_id_str,
                                http_url,
                                error = &err as &dyn StdError,
                                "/v1/models fetch failed"
                            );
                            warned_about_models = true;
                        }
                    }
                }
            }
            n = n.wrapping_add(1);

            match scrape_once(
                &client,
                &metrics_url,
                &ALL_NAMES,
                &current_model_name,
                &cfg.worker_id_str,
                first_scrape,
                &mut warned_about_label,
            )
            .await
            {
                Ok(mut sample) => {
                    first_scrape = false;
                    // Record this scrape and read back the windowed values.
                    if let Some(now) = sample.scraped_at {
                        let (qm, km) = samples.record_and_compute_means(
                            cfg.worker_id_num,
                            now,
                            sample.waiting,
                            sample.kv_cache_usage,
                        );
                        sample.waiting_mean = qm;
                        sample.kv_cache_usage_mean = km;
                        sample.completion_rate_per_sec = samples.record_and_compute_rate(
                            cfg.worker_id_num,
                            now,
                            sample.completions_total,
                        );
                        // The vLLM counter pair, when present, takes
                        // precedence over a gauge value, since it uses our
                        // window.
                        if sample.prefix_cache_queries_total > 0 {
                            sample.prefix_cache_hit_rate = samples.record_and_compute_hit_rate(
                                cfg.worker_id_num,
                                now,
                                sample.prefix_cache_hits_total,
                                sample.prefix_cache_queries_total,
                            );
                        }
                    }
                    samples.by_worker.insert(cfg.worker_id_num, sample);
                    warned_about_scrape = false;
                }
                Err(err) => {
                    if !std::mem::replace(&mut warned_about_scrape, true) {
                        tracing::warn!(
                            worker_id = cfg.worker_id_str,
                            error = &err as &dyn StdError,
                            "metrics scrape failed"
                        );
                    }
                }
            }
        }
    });
}

async fn scrape_once(
    client: &reqwest::Client,
    url: &str,
    names: &MetricNames,
    model_name: &str,
    worker_id_str: &str,
    first_scrape: bool,
    warned_about_label: &mut bool,
) -> Result<MetricsSample, Error> {
    let http = |source| Error::Http {
        url: url.to_string(),
        source,
    };
    let resp = client.get(url).send().await.map_err(http)?;
    if !resp.status().is_success() {
        return Err(Error::HttpStatus {
            url: url.to_string(),
            status: resp.status(),
        });
    }
    let body = resp.text().await.map_err(http)?;
    // prometheus-parse rejects colons in metric names.
    let body = body.replace("vllm:", "vllm_").replace("sglang:", "sglang_");
    let scrape = prometheus_parse::Scrape::parse(body.lines().map(|l| Ok(l.to_owned()))).map_err(
        |source| Error::MetricsParse {
            url: url.to_string(),
            source,
        },
    )?;

    if first_scrape {
        let mut names: Vec<&str> = scrape.samples.iter().map(|s| s.metric.as_str()).collect();
        names.sort();
        names.dedup();
        let mut label_keys: std::collections::BTreeSet<String> = Default::default();
        for s in &scrape.samples {
            for k in s.labels.iter() {
                label_keys.insert(k.0.to_string());
            }
        }
        tracing::debug!(
            worker_id = worker_id_str,
            metric_count = names.len(),
            ?label_keys,
            "first metrics scrape parsed"
        );
        tracing::trace!(
            worker_id = worker_id_str,
            metrics = %names.join(", "),
            "metrics on the first scrape"
        );
    }

    // First-match-wins lookup. If the engine has a `model_label`, filter by it;
    // otherwise accept the first matching sample regardless of labels.
    let pick_first = |candidates: &[&'static str]| -> Option<f64> {
        for name in candidates {
            let found = scrape
                .samples
                .iter()
                .filter(|s| s.metric == *name)
                .find(|s| match names.model_label {
                    // No filter without a model_name (before the first
                    // /v1/models refresh, or for multi-model workers).
                    Some(label) if !model_name.is_empty() => {
                        s.labels.get(label) == Some(model_name)
                    }
                    _ => true,
                })
                .and_then(|s| match s.value {
                    prometheus_parse::Value::Gauge(v)
                    | prometheus_parse::Value::Counter(v)
                    | prometheus_parse::Value::Untyped(v) => Some(v),
                    _ => None,
                });
            if found.is_some() {
                return found;
            }
        }
        None
    };

    let waiting = pick_first(names.waiting);
    let running = pick_first(names.running);
    let swapped = pick_first(names.swapped);
    let kv_cache = pick_first(names.kv_cache_usage);

    // Completion counter: sum across all label combinations (e.g.
    // finished_reason=stop|length|...) that match this worker's model_label.
    // Histogram `_count` series are exposed as Counter values by
    // prometheus-parse, so the same iter handles both candidate names.
    let pick_sum = |candidates: &[&'static str]| -> Option<f64> {
        for name in candidates {
            let mut sum = 0.0;
            let mut found = false;
            for s in scrape.samples.iter().filter(|s| s.metric == *name) {
                // As above: no filter without a model_name.
                if let Some(label) = names.model_label
                    && !model_name.is_empty()
                    && s.labels.get(label) != Some(model_name)
                {
                    continue;
                }
                match s.value {
                    prometheus_parse::Value::Counter(v)
                    | prometheus_parse::Value::Gauge(v)
                    | prometheus_parse::Value::Untyped(v) => {
                        sum += v;
                        found = true;
                    }
                    _ => {}
                }
            }
            if found {
                return Some(sum);
            }
        }
        None
    };
    let completions_total = pick_sum(names.completions).unwrap_or(0.0).max(0.0) as u64;

    // Prefix-cache hit-rate inputs: vLLM (≥ 0.6) emits a counter pair, from
    // which the poller derives the windowed ratio. SGLang's gauge is read
    // below.
    let prefix_cache_hits_total = pick_sum(names.prefix_cache_hits).unwrap_or(0.0).max(0.0) as u64;
    let prefix_cache_queries_total =
        pick_sum(names.prefix_cache_queries).unwrap_or(0.0).max(0.0) as u64;

    // Warn about a model_name label mismatch only when there is a filter to
    // mismatch.
    if !*warned_about_label
        && names.model_label.is_some()
        && !model_name.is_empty()
        && waiting.is_none()
        && running.is_none()
        && swapped.is_none()
        && kv_cache.is_none()
    {
        tracing::warn!(
            worker_id = worker_id_str,
            model_name,
            "no metrics match the model_name label"
        );
        *warned_about_label = true;
    }

    // SGLang's hit-rate gauge. The poller overrides it with the windowed
    // counter rate when the vLLM pair is present.
    let sglang_hit_rate_gauge = names
        .prefix_cache_hit_rate_gauge
        .iter()
        .find_map(|name| {
            scrape.samples.iter().find_map(|s| {
                if &s.metric == name {
                    match s.value {
                        prometheus_parse::Value::Gauge(v)
                        | prometheus_parse::Value::Counter(v)
                        | prometheus_parse::Value::Untyped(v) => Some(v.clamp(0.0, 1.0) as f32),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        })
        .unwrap_or(0.0);

    Ok(MetricsSample {
        waiting: waiting.unwrap_or(0.0).max(0.0) as u32,
        running: running.unwrap_or(0.0).max(0.0) as u32,
        swapped: swapped.unwrap_or(0.0).max(0.0) as u32,
        // A fraction. An `+Inf` would turn the predictor's `penalty * usage`
        // into NaN.
        kv_cache_usage: kv_cache
            .filter(|v| !v.is_nan())
            .unwrap_or(0.0)
            .clamp(0.0, 1.0) as f32,
        completions_total,
        // Filled in by the poller.
        completion_rate_per_sec: 0.0,
        // Likewise the windowed means.
        waiting_mean: 0.0,
        kv_cache_usage_mean: 0.0,
        prefix_cache_hits_total,
        prefix_cache_queries_total,
        // SGLang's rate; the poller may override it (see above).
        prefix_cache_hit_rate: sglang_hit_rate_gauge,
        scraped_at: Some(Instant::now()),
    })
}

/// Fetch `/v1/models` from the engine and parse `id`, `max_model_len` and
/// `owned_by` per entry. `max_model_len` may be missing. The engine is taken
/// from the first non-empty `owned_by`.
async fn fetch_models(
    client: &reqwest::Client,
    http_url: &str,
) -> Result<(Vec<ModelEntry>, Option<Engine>), Error> {
    let url = format!("{}/v1/models", http_url.trim_end_matches('/'));
    let http = |source| Error::Http {
        url: url.clone(),
        source,
    };
    let resp = client.get(&url).send().await.map_err(http)?;
    if !resp.status().is_success() {
        return Err(Error::HttpStatus {
            url,
            status: resp.status(),
        });
    }
    #[derive(serde::Deserialize)]
    struct ModelsResponse {
        data: Vec<ModelData>,
    }
    #[derive(serde::Deserialize)]
    struct ModelData {
        id: String,
        max_model_len: Option<u32>,
        #[serde(default)]
        owned_by: Option<String>,
    }
    let body: ModelsResponse = resp.json().await.map_err(http)?;
    let detected_engine = body
        .data
        .iter()
        .find_map(|m| m.owned_by.as_deref().and_then(engine_from_owned_by));
    let entries: Vec<ModelEntry> = body
        .data
        .into_iter()
        .map(|m| ModelEntry {
            name: m.id,
            max_model_len: m.max_model_len,
        })
        .collect();
    Ok((entries, detected_engine))
}

#[cfg(test)]
mod tests;
