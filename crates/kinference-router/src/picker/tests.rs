use super::*;

fn make_worker(
    endpoint: &str,
    matched: u32,
    total: u32,
    q_wait: u32,
    completion: f32,
    kv: f32,
) -> WorkerScore {
    WorkerScore {
        name: endpoint.to_string(),
        endpoint: endpoint.to_string(),
        matched_blocks: matched,
        total_blocks: total,
        queue_waiting: q_wait,
        queue_running: 0,
        queue_swapped: 0,
        kv_cache_usage: kv,
        completion_rate_per_sec: completion,
        block_size: 16,
    }
}

/// A zero-filled inflight slice parallel to `workers`.
fn zero_inflight(workers: &[WorkerScore]) -> Vec<u32> {
    vec![0; workers.len()]
}

/// An empty pending-workload slice parallel to `workers`.
fn zero_workload(workers: &[WorkerScore]) -> Vec<Vec<InflightSummary>> {
    vec![Vec::new(); workers.len()]
}

/// A no-match KV slice parallel to `workers`.
fn zero_kv_blocks(workers: &[WorkerScore]) -> Vec<u32> {
    vec![0; workers.len()]
}

/// The pick inputs over parallel slices a test built, as of now.
fn inputs<'a>(
    scores: &'a [WorkerScore],
    predictor: &'a PredictorConfig,
    picker: &'a PickerConfig,
    inflight: &'a [u32],
    inflight_workload: &'a [Vec<InflightSummary>],
    kv_blocks: &'a [u32],
) -> PickInputs<'a> {
    PickInputs {
        scores,
        predictor,
        picker,
        inflight,
        inflight_workload,
        kv_blocks,
        now: Instant::now(),
    }
}

/// Build a single InflightSummary for use in tests. `dispatched_at`
/// is `age_ms` before now. `total_at_dispatch` equals `uncached` (fully cold);
/// see `summary_with_total`.
fn summary(uncached: u32, age_ms: u64) -> InflightSummary {
    summary_with_total(uncached, uncached, age_ms)
}

fn summary_with_total(uncached: u32, total: u32, age_ms: u64) -> InflightSummary {
    InflightSummary {
        dispatched_at: std::time::Instant::now() - std::time::Duration::from_millis(age_ms),
        uncached_at_dispatch: uncached,
        total_at_dispatch: total,
    }
}

#[test]
fn single_worker_wins_trivially() {
    let predictor = PredictorConfig::default();
    let picker = PickerConfig::default();
    let workers = vec![make_worker("a:1", 0, 100, 0, 0.0, 0.0)];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("a single worker is always pickable");
    assert_eq!(picked.worker.endpoint, "a:1");
}

#[test]
fn higher_overlap_wins() {
    // With strong prefill term + zero queue, the worker with more
    // matched blocks should win deterministically (top_k_min=1).
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let workers = vec![
        make_worker("cold:1", 0, 1000, 0, 0.0, 0.0), // 1000 uncached blocks
        make_worker("hot:1", 999, 1000, 0, 0.0, 0.0), // 1 uncached block
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty input picks");
    assert_eq!(picked.worker.endpoint, "hot:1");
}

/// A worker whose predicted wait is NaN never wins.
#[test]
fn a_nan_prediction_never_wins() {
    // A zero penalty times an infinite usage is NaN.
    let predictor = PredictorConfig {
        cache_pressure_penalty_ms: 0.0,
        ..Default::default()
    };
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let mut workers: Vec<WorkerScore> = (0..40)
        .map(|i| make_worker(&format!("nan:{i}"), 0, 10, 0, 0.0, f32::INFINITY))
        .collect();
    workers.insert(20, make_worker("ok:1", 0, 10, 0, 0.0, 0.0));
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty input picks");
    assert_eq!(picked.worker.endpoint, "ok:1");
}

#[test]
fn cold_tie_with_top_k_min_1_still_spreads() {
    // With top_k_min=1 and identical workers, plain argmin would always
    // pick index 0. The tie extension makes the pick uniform across ties,
    // so over many runs every endpoint is chosen.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let workers = vec![
        make_worker("a:1", 0, 0, 0, 0.0, 0.0),
        make_worker("b:1", 0, 0, 0, 0.0, 0.0),
        make_worker("c:1", 0, 0, 0, 0.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    let mut seen = std::collections::HashSet::new();
    // 200 samples × 3-way uniform ≈ vanishingly small chance of one being missed.
    for _ in 0..200 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        seen.insert(picked.worker.endpoint.clone());
    }
    assert_eq!(
        seen.len(),
        3,
        "tie-extension should spread across all 3 tied workers; got {:?}",
        seen
    );
}

#[test]
fn inflight_breaks_predicted_wait_ties() {
    // Three workers tied on predicted_ms with different in-flight counts:
    // the lowest in-flight always wins.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let workers = vec![
        make_worker("loaded:1", 0, 100, 0, 0.0, 0.0), // inflight = 5
        make_worker("medium:1", 0, 100, 0, 0.0, 0.0), // inflight = 2
        make_worker("empty:1", 0, 100, 0, 0.0, 0.0),  // inflight = 0
    ];
    let inflight = vec![5, 2, 0];
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    for _ in 0..50 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        assert_eq!(
            picked.worker.endpoint, "empty:1",
            "tie-break must always pick lowest-inflight; saw {}",
            picked.worker.endpoint
        );
    }
}

#[test]
fn tie_tolerance_widens_pool_to_include_near_ties() {
    // One worker has matched=0 and the others matched=1 (a chat-template
    // prefix), a gap of a few ms. With tie_tolerance_ms=50 the pool includes
    // the empty worker, and its lower inflight count wins.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        tie_tolerance_ms: 50.0,
        ..Default::default()
    };
    let workers = vec![
        // Empty worker; 0 of 100 blocks "matched" (no template cached).
        make_worker("empty:1", 0, 100, 0, 0.0, 0.0),
        // Warm workers; 1 of 100 blocks matched (template only).
        make_worker("warm:1", 1, 100, 0, 0.0, 0.0),
        make_worker("warm:2", 1, 100, 0, 0.0, 0.0),
        make_worker("warm:3", 1, 100, 0, 0.0, 0.0),
    ];
    // :empty:1 has the fewest inflight picks. The other three
    // workers are busier.
    let inflight = vec![0, 1, 1, 2];
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    for _ in 0..50 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        assert_eq!(
            picked.worker.endpoint, "empty:1",
            "tie tolerance should bring the empty worker into the pool, \
             then inflight tie-break should pick it deterministically; saw {}",
            picked.worker.endpoint
        );
    }
}

#[test]
fn tie_tolerance_zero_keeps_strict_predicted_wait_ordering() {
    // With tie_tolerance_ms=0, :empty:1 is not in the pool, so a warm
    // worker wins despite higher inflight.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        tie_tolerance_ms: 0.0,
        ..Default::default()
    };
    let workers = vec![
        make_worker("empty:1", 0, 100, 0, 0.0, 0.0),
        make_worker("warm:1", 1, 100, 0, 0.0, 0.0),
        make_worker("warm:2", 1, 100, 0, 0.0, 0.0),
    ];
    let inflight = vec![0, 1, 1];
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    for _ in 0..50 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        assert_ne!(
            picked.worker.endpoint, "empty:1",
            "with tolerance=0 the empty worker must be excluded; saw {}",
            picked.worker.endpoint
        );
    }
}

#[test]
fn tie_tolerance_does_not_pull_in_meaningfully_worse_workers() {
    // Even with a generous tolerance, a worker far above the minimum stays
    // out of the pool.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        tie_tolerance_ms: 50.0,
        ..Default::default()
    };
    let workers = vec![
        // Truly slow worker: a real waiting queue (10 entries).
        make_worker("slow:1", 0, 100, 10, 1.0, 0.0),
        // Fast workers, no queue.
        make_worker("fast:1", 0, 100, 0, 100.0, 0.0),
        make_worker("fast:2", 0, 100, 0, 100.0, 0.0),
    ];
    let inflight = vec![0, 5, 5];
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    for _ in 0..50 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        assert_ne!(
            picked.worker.endpoint, "slow:1",
            "slow:1 has a real queue cost — tolerance must not pull it in; saw {}",
            picked.worker.endpoint
        );
    }
}

#[test]
fn inflight_tie_within_a_pool_falls_through_to_random() {
    // Tied on predicted_ms and inflight: random across all three.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let workers = vec![
        make_worker("a:1", 0, 0, 0, 0.0, 0.0),
        make_worker("b:1", 0, 0, 0, 0.0, 0.0),
        make_worker("c:1", 0, 0, 0, 0.0, 0.0),
    ];
    let inflight = vec![0, 0, 0];
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    let mut seen = std::collections::HashSet::new();
    for _ in 0..200 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        seen.insert(picked.worker.endpoint.clone());
    }
    assert_eq!(
        seen.len(),
        3,
        "with all-zero inflight, tie-break is a no-op → should still spread; got {:?}",
        seen
    );
}

#[test]
fn inflight_does_not_override_predicted_wait() {
    // Inflight only breaks ties within the top-K pool; it never beats a
    // better predicted_ms.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let workers = vec![
        // Bigger prefill → much worse predicted_ms
        make_worker("slow:1", 0, 1000, 0, 0.0, 0.0),
        // Better prefill → much better predicted_ms
        make_worker("fast:1", 999, 1000, 0, 0.0, 0.0),
    ];
    // slow has 0 inflight; fast has 10 inflight. Picker should
    // still pick fast because its predicted_ms is much better.
    let inflight = vec![0, 10];
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    for _ in 0..50 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        assert_eq!(
            picked.worker.endpoint, "fast:1",
            "predicted_ms must dominate inflight; saw {}",
            picked.worker.endpoint
        );
    }
}

#[test]
fn pure_argmin_deterministic() {
    // top_k_min=1, top_k_fraction=0.0 + a *strict* ordering means
    // pure argmin and we always pick the winner.
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    let workers = vec![
        make_worker("loaded:1", 0, 100, 10, 1.0, 0.0), // big queue, slow decode
        make_worker("free:1", 0, 100, 0, 100.0, 0.0),  // no queue, fast decode
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = zero_kv_blocks(&workers);
    for _ in 0..50 {
        let picked = pick_endpoint(&inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv,
        ))
        .expect("non-empty input picks");
        assert_eq!(picked.worker.endpoint, "free:1");
    }
}

#[test]
fn saturated_cold_fallback_kicks_in() {
    // No completions, cache above threshold and a queue: the saturated-cold
    // rate applies.
    let predictor = PredictorConfig::default();
    // 5 queued entries, no completion history, cache 95% full.
    let w = make_worker("stuck:1", 0, 0, 5, 0.0, 0.95);
    let pred = predicted_wait_ms(&w, &predictor, &[], Instant::now());
    assert_eq!(
        pred.q_rate_ms_per_entry, predictor.saturated_cold_ms_per_entry,
        "expected cold-saturation fallback rate ({}), got {}",
        predictor.saturated_cold_ms_per_entry, pred.q_rate_ms_per_entry
    );
    let expected_queue = predictor.saturated_cold_ms_per_entry * 5.0;
    assert!(
        (pred.q_ms - expected_queue).abs() < 0.01,
        "queue_ms should be 5 * saturated_cold rate; got {}",
        pred.q_ms
    );
}

#[test]
fn empty_input_returns_none() {
    let predictor = PredictorConfig::default();
    let picker = PickerConfig::default();
    let workers: Vec<WorkerScore> = vec![];
    assert!(pick_endpoint(&inputs(&workers, &predictor, &picker, &[], &[], &[])).is_none());
}

#[test]
fn format_score_table_sorts_by_name_and_marks_chosen() {
    let predictor = PredictorConfig::default();
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        ..Default::default()
    };
    // Out of alphabetical order, and the chosen worker has the higher
    // predicted wait, so the marker follows `chosen_endpoint`, not the order.
    let workers = vec![
        make_worker("zeta:1", 0, 100, 10, 1.0, 0.0),   // big wait
        make_worker("alpha:1", 0, 100, 0, 100.0, 0.0), // small wait
    ];
    let inflight = vec![0, 0];
    let kv_match = vec![0, 0];
    let workload: Vec<Vec<InflightSummary>> = vec![Vec::new(); 2];
    let table = format_score_table(
        &inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv_match,
        ),
        1.0,
        Some("zeta:1"),
    );
    let lines: Vec<&str> = table.lines().collect();
    assert_eq!(
        lines.len(),
        4,
        "weights + header + 2 data rows; got: {table}"
    );
    assert!(
        lines[0].starts_with("weights:"),
        "first line should be weights summary: {table}"
    );
    assert!(
        lines[0].contains("expected_overlap_alpha"),
        "weights line must spell out the config field name: {table}"
    );
    assert!(
        lines[0].contains("inflight_concurrent_capacity"),
        "weights line must spell out the config field name: {table}"
    );
    assert!(
        lines[1].contains("expect_ms"),
        "second line should be column header: {table}"
    );
    // Sorted by name, so alpha:1 first.
    assert!(
        lines[2].contains("alpha:1"),
        "first data row should be alpha:1: {table}"
    );
    assert!(
        !lines[2].starts_with('*'),
        "alpha:1 was NOT chosen: {table}"
    );
    assert!(
        lines[3].contains("zeta:1"),
        "second data row should be zeta:1: {table}"
    );
    assert!(
        lines[3].starts_with('*'),
        "zeta:1 was the chosen endpoint: {table}"
    );
}

#[test]
fn format_score_table_renders_q_wait_and_q_inflight() {
    let predictor = PredictorConfig::default();
    let picker = PickerConfig::default();
    let workers = vec![make_worker("a:1", 0, 100, 5, 1.0, 0.0)];
    let inflight = vec![3];
    let kv_match = vec![0];
    let workload: Vec<Vec<InflightSummary>> = vec![vec![summary(42, 100)]];
    let table = format_score_table(
        &inputs(
            &workers, &predictor, &picker, &inflight, &workload, &kv_match,
        ),
        1.0,
        Some("a:1"),
    );
    assert!(
        table.contains("q_wait"),
        "header must include q_wait (scraped queue depth): {table}"
    );
    assert!(
        table.contains("q_inflight"),
        "header must include q_inflight (shadow count): {table}"
    );
    assert!(
        table.contains("q_run"),
        "header must include q_run (engine-reported running batch size): {table}"
    );
    assert!(
        table.contains("q_run_ms"),
        "header must include q_run_ms (predicted U-shape penalty): {table}"
    );
    assert!(
        table.contains("kv_blocks"),
        "header must include kv_blocks (raw radix-tree match): {table}"
    );
    assert!(
        table.contains("q_workload"),
        "header must include q_workload (inflight workload blocks): {table}"
    );
    assert!(
        table.contains("overlap/total"),
        "header must include overlap/total: {table}"
    );
}

// -------- q_running U-shape penalty -------------------------------

/// Default config knobs (peak_ms=700, peak_qr=2, falloff_qr=5).
/// 0 at effective_qr=0 and at effective_qr ≥ falloff_qr. q_inflight is 0
/// here; it is covered below.
#[test]
fn q_running_penalty_is_zero_at_qr_zero_and_falloff() {
    let predictor = PredictorConfig::default();
    assert_eq!(q_running_penalty_ms(0, 0, &predictor), 0.0, "qr=0 → 0");
    assert_eq!(
        q_running_penalty_ms(5, 0, &predictor),
        0.0,
        "qr=falloff_qr → 0"
    );
    assert_eq!(
        q_running_penalty_ms(6, 0, &predictor),
        0.0,
        "qr > falloff_qr → 0"
    );
    assert_eq!(
        q_running_penalty_ms(100, 0, &predictor),
        0.0,
        "qr ≫ falloff_qr → 0"
    );
}

/// At effective_qr == peak_qr the penalty hits the configured peak.
#[test]
fn q_running_penalty_peaks_at_configured_qr() {
    let predictor = PredictorConfig::default();
    assert!(
        (q_running_penalty_ms(2, 0, &predictor) - 700.0).abs() < 1e-4,
        "peak should be 700ms at qr=peak_qr=2, got {}",
        q_running_penalty_ms(2, 0, &predictor)
    );
}

/// Default cfg: 700ms peak at qr=2, 0 at qr=0 and qr=5.
/// Rising edge (0→2): linear ramp slope = 350 ms/step.
/// Falling edge (2→5): linear decline slope = -700/3 ms/step.
#[test]
fn q_running_penalty_ramps_linearly_on_both_sides() {
    let predictor = PredictorConfig::default();
    // Rising edge: qr=1 → 350ms (half of peak).
    approx(q_running_penalty_ms(1, 0, &predictor), 350.0);
    // Falling edge: qr=3 → 700 * (5-3)/(5-2) = 700 * 2/3 ≈ 466.67ms.
    approx(q_running_penalty_ms(3, 0, &predictor), 466.6667);
    // Falling edge: qr=4 → 700 * (5-4)/(5-2) = 700 * 1/3 ≈ 233.33ms.
    approx(q_running_penalty_ms(4, 0, &predictor), 233.3333);
}

/// `peak_ms <= 0` disables the term.
#[test]
fn q_running_penalty_disabled_when_peak_is_zero() {
    let predictor = PredictorConfig {
        q_running_penalty_peak_ms: 0.0,
        ..Default::default()
    };
    for qr in 0..=10 {
        for q_inflight in 0..=5 {
            assert_eq!(
                q_running_penalty_ms(qr, q_inflight, &predictor),
                0.0,
                "peak=0 must disable for all (qr, q_inflight) (failed at qr={qr}, qi={q_inflight})"
            );
        }
    }
}

/// Degenerate config: peak_qr=0 is treated as 1, so there is no NaN or
/// panic.
#[test]
fn q_running_penalty_degenerate_peak_qr_zero_does_not_panic() {
    let predictor = PredictorConfig {
        q_running_penalty_peak_qr: 0,
        ..Default::default()
    };
    let v = q_running_penalty_ms(1, 0, &predictor);
    assert!(
        v.is_finite() && v >= 0.0,
        "must produce a finite, non-negative value; got {v}"
    );
}

/// The penalty is over `qr + q_inflight`, so (qr=0, q_inflight=k) matches
/// (qr=k, q_inflight=0).
#[test]
fn q_running_penalty_inflight_combines_with_scraped_qr() {
    let predictor = PredictorConfig::default();
    for k in 0..=5 {
        let only_scraped = q_running_penalty_ms(k, 0, &predictor);
        let only_inflight = q_running_penalty_ms(0, k, &predictor);
        let split = q_running_penalty_ms(k / 2, k - k / 2, &predictor);
        approx(only_inflight, only_scraped);
        approx(split, only_scraped);
    }
}

/// At effective_qr ≥ falloff_qr the penalty is 0 however the count is split,
/// e.g. qr=3 plus q_inflight=2.
#[test]
fn q_running_penalty_inflight_can_push_past_falloff() {
    let predictor = PredictorConfig::default();
    assert_eq!(
        q_running_penalty_ms(3, 2, &predictor),
        0.0,
        "qr=3 + q_inflight=2 should reach falloff_qr=5 and produce 0"
    );
    assert_eq!(
        q_running_penalty_ms(0, 10, &predictor),
        0.0,
        "q_inflight alone can carry effective_qr past falloff"
    );
}

/// A huge q_inflight added to a near-max qr doesn't panic or wrap.
#[test]
fn q_running_penalty_saturating_add_does_not_panic() {
    let predictor = PredictorConfig::default();
    let v = q_running_penalty_ms(u32::MAX, u32::MAX, &predictor);
    assert!(
        v.is_finite() && v >= 0.0,
        "must produce a finite value; got {v}"
    );
    assert_eq!(v, 0.0, "max value lands well past falloff → 0");
}

/// Two workers differing only in `queue_running`: the qr=2 one (peak
/// penalty) predicts about `peak` more than the qr=0 one.
#[test]
fn predicted_wait_ms_includes_q_running_term() {
    let predictor = PredictorConfig::default();
    let mut idle = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    idle.queue_running = 0;
    let mut contended = make_worker("b:1", 0, 100, 0, 1.0, 0.0);
    contended.queue_running = 2; // peak of the U-shape
    let now = Instant::now();
    let p_idle = predicted_wait_ms(&idle, &predictor, &[], now);
    let p_cont = predicted_wait_ms(&contended, &predictor, &[], now);
    // The contended worker should be roughly 700ms slower in total,
    // with everything else equal.
    let delta = p_cont.total_ms() - p_idle.total_ms();
    assert!(
        (delta - 700.0).abs() < 1e-3,
        "qr=2 worker should be ~700ms slower than qr=0 (everything else equal); got delta={delta}"
    );
    // And the q_running_ms component should be visible on the contended worker.
    assert_eq!(p_idle.q_running_ms, 0.0);
    approx(p_cont.q_running_ms, 700.0);
}

/// At qr ≥ falloff_qr, q_running_ms is back to 0.
#[test]
fn predicted_wait_ms_q_running_term_disappears_past_falloff() {
    let predictor = PredictorConfig::default();
    let mut idle = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    idle.queue_running = 0;
    let mut busy = make_worker("b:1", 0, 100, 0, 1.0, 0.0);
    busy.queue_running = 7; // well past falloff_qr=5
    let now = Instant::now();
    let p_idle = predicted_wait_ms(&idle, &predictor, &[], now);
    let p_busy = predicted_wait_ms(&busy, &predictor, &[], now);
    // The penalty is 0 on both, and the workers are otherwise identical.
    assert_eq!(
        p_busy.q_running_ms, 0.0,
        "qr=7 (past falloff) must produce 0 penalty"
    );
    assert!(
        (p_busy.total_ms() - p_idle.total_ms()).abs() < 1e-4,
        "qr=7 and qr=0 must produce identical total_ms (q_running term suppressed)"
    );
}

#[test]
fn format_score_table_empty() {
    let predictor = PredictorConfig::default();
    let picker = PickerConfig::default();
    let table = format_score_table(&inputs(&[], &predictor, &picker, &[], &[], &[]), 1.0, None);
    assert_eq!(table, "(no workers scored)");
}

// -------- compute_layered_overlap ---------------------------------

fn approx(a: f32, b: f32) {
    assert!(
        (a - b).abs() < 1e-4,
        "expected {b}, got {a} (diff {})",
        (a - b).abs()
    );
}

#[test]
fn layered_overlap_alpha_one_yields_max_across_all_sources() {
    // α=1 (no eviction): the sum equals the longest matched prefix
    // across all sources (pending dispatches + kv).
    //
    // Empty pending: trivially kv_match.
    approx(compute_layered_overlap(&[], 17.0, 1.0), 17.0);
    // Monotonic with kv largest: max = kv = 20.
    approx(compute_layered_overlap(&[5.0, 8.0, 12.0], 20.0, 1.0), 20.0);
    // Non-monotonic with kv largest: max = 25.
    approx(compute_layered_overlap(&[10.0, 3.0, 7.0], 25.0, 1.0), 25.0);
    // Pending bigger than kv: max from pending wins.
    approx(compute_layered_overlap(&[8.0], 5.0, 1.0), 8.0);
    // Newest dispatch has biggest overlap (chat-continuation):
    // max = newest = 6.
    approx(compute_layered_overlap(&[3.0, 6.0], 0.0, 1.0), 6.0);
    // Middle pending is biggest: still picks max.
    approx(compute_layered_overlap(&[3.0, 12.0, 5.0], 4.0, 1.0), 12.0);
}

#[test]
fn layered_overlap_alpha_zero_keeps_only_most_recent_layer() {
    // With pending: only the most-recent dispatch (last element)
    // contributes; α discounts everything else to zero.
    approx(compute_layered_overlap(&[10.0, 3.0, 7.0], 25.0, 0.0), 7.0);
    // No pending: the "most recent layer" is kv_match.
    approx(compute_layered_overlap(&[], 25.0, 0.0), 25.0);
}

#[test]
fn layered_overlap_empty_pending_returns_kv_for_any_alpha() {
    for &alpha in &[0.0_f32, 0.25, 0.5, 0.8, 1.0] {
        approx(compute_layered_overlap(&[], 13.0, alpha), 13.0);
    }
}

// ─── Quadratic prefill formula ──────────────────────────────────

#[test]
fn prefill_ms_quadratic_matches_short_prompt_anchor() {
    // a=2.3, c=0.009: 520-block prompt fully cold should produce
    // ~7 ms/block × 525 ≈ 3675 ms. (Synthetic-test calibration.)
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 2.3,
        attention_uncached_per_every_block_ms: 0.009,
        ..Default::default()
    };
    let got = prefill_ms_for(&predictor, 525, 520);
    // expected: 2.3 * 525 + 0.009 * 525 * 520 = 1207.5 + 2457 = 3664.5
    assert!(
        (got - 3664.5).abs() < 0.1,
        "short prompt prefill ms: got {got}, expected ≈ 3664.5"
    );
}

#[test]
fn prefill_ms_quadratic_matches_long_prompt_anchor() {
    // Long prompt (1649 blocks): ~17 ms/block ≈ 28000 ms total.
    // (Mooncake-anchor calibration.)
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 2.3,
        attention_uncached_per_every_block_ms: 0.009,
        ..Default::default()
    };
    let got = prefill_ms_for(&predictor, 1616, 1649);
    // expected: 2.3 * 1616 + 0.009 * 1616 * 1649 = 3716.8 + 23983.06 ≈ 27700
    assert!(
        (got - 27700.0).abs() < 5.0,
        "long prompt prefill ms: got {got}, expected ≈ 27700"
    );
}

#[test]
fn prefill_ms_zero_uncached_is_zero() {
    // A pure warm hit has 0 prefill cost regardless of prompt length.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 2.3,
        attention_uncached_per_every_block_ms: 0.009,
        ..Default::default()
    };
    assert_eq!(prefill_ms_for(&predictor, 0, 1000), 0.0);
    assert_eq!(prefill_ms_for(&predictor, 0, 100_000), 0.0);
}

#[test]
fn prefill_ms_linear_only_matches_legacy_behavior() {
    // A zero quadratic coefficient reduces to `a × uncached`.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 7.0,
        attention_uncached_per_every_block_ms: 0.0,
        ..Default::default()
    };
    for &total in &[100u32, 500, 2000] {
        assert_eq!(
            prefill_ms_for(&predictor, 100, total),
            700.0,
            "total={total} should not affect cost when c=0"
        );
    }
}

// ─── Cache-credit refinement: scale overlap by progress ─────────

#[test]
fn scale_per_dispatch_by_progress_zeroes_unstarted() {
    // Two inflights, both with the same raw overlap. Progress 0
    // (just admitted) yields no cache credit; progress 1 yields full.
    let scaled = scale_per_dispatch_by_progress(&[100.0, 100.0], &[0.0, 1.0]);
    assert_eq!(scaled, vec![0.0, 100.0]);
}

#[test]
fn scale_per_dispatch_by_progress_clamps_to_unit_range() {
    // Out-of-range progress is clamped to [0, 1].
    let scaled = scale_per_dispatch_by_progress(&[100.0, 100.0], &[1.5, -0.3]);
    assert_eq!(scaled, vec![100.0, 0.0]);
}

#[test]
fn scale_per_dispatch_by_progress_partial_credit() {
    // Half-done inflight with 100 overlap blocks → 50 cached.
    let scaled = scale_per_dispatch_by_progress(&[100.0], &[0.5]);
    assert_eq!(scaled, vec![50.0]);
}

#[test]
fn layered_overlap_with_cache_credit_zeros_fresh_inflights() {
    // Two inflights, both with 100 overlap blocks. The newest is
    // fresh (progress=0); the older is done (progress=1).
    // Without scaling: layered overlap would credit 100 from the
    // newest (taking it over the kv_match floor of 0).
    // With scaling:    the newest contributes 0; the older
    //                  contributes its full 100 at α-weight.
    let scaled = scale_per_dispatch_by_progress(&[100.0, 100.0], &[1.0, 0.0]);
    // α=1, kv_match=0: max-with-clamp formula → 100 (from older).
    approx(compute_layered_overlap(&scaled, 0.0, 1.0), 100.0);
    // α=0.5: only the newest (L0) gets weight 1, older gets 0.5.
    // L0 contributes 0 × 1 = 0; L1 contributes max(0, 100-0) × 0.5 = 50.
    approx(compute_layered_overlap(&scaled, 0.0, 0.5), 50.0);
}

#[test]
fn layered_overlap_monotonic_in_alpha() {
    // Raising α never decreases overlap.
    let prev = compute_layered_overlap(&[3.0, 6.0, 4.0], 7.0, 0.0);
    let mut last = prev;
    for step in 1..=10 {
        let alpha = step as f32 / 10.0;
        let value = compute_layered_overlap(&[3.0, 6.0, 4.0], 7.0, alpha);
        assert!(
            value + 1e-4 >= last,
            "overlap must be monotonic in α: at α={alpha} got {value} < prior {last}"
        );
        last = value;
    }
}

#[test]
fn layered_overlap_redundant_pending_dispatches_collapse() {
    // Two pending dispatches that overlap P_new identically. The older one
    // adds nothing, so the result doesn't depend on α.
    //
    //   queue=[abcde, abcdf], P_new=abcdg, kv empty
    //   per_dispatch (oldest first) = [4, 4]
    // newest (P2=abcdf) contributes 4; P1 is fully redundant;
    // kv empty contributes nothing.
    for &alpha in &[0.0_f32, 0.25, 0.5, 0.8, 1.0] {
        approx(compute_layered_overlap(&[4.0, 4.0], 0.0, alpha), 4.0);
    }
}

#[test]
fn layered_overlap_unrelated_newest_with_helpful_older() {
    //   queue=[abcde, abcdf, awxyz], P_new=abcdg, kv empty
    //   per_dispatch (oldest first) = [4, 4, 1]
    // newest L0 (awxyz) covers 1 block ("a"); L1 (abcdf) adds 3
    // new blocks (bcd) beyond what L0 covered; L2 (abcde) is now
    // fully redundant with L1; KV empty.
    //   overlap = 1 + 3α
    approx(compute_layered_overlap(&[4.0, 4.0, 1.0], 0.0, 1.0), 4.0);
    approx(
        compute_layered_overlap(&[4.0, 4.0, 1.0], 0.0, 0.9),
        1.0 + 3.0 * 0.9,
    );
    approx(
        compute_layered_overlap(&[4.0, 4.0, 1.0], 0.0, 0.5),
        1.0 + 3.0 * 0.5,
    );
    approx(compute_layered_overlap(&[4.0, 4.0, 1.0], 0.0, 0.0), 1.0);
}

#[test]
fn layered_overlap_never_debits_when_newer_is_larger() {
    // The newest dispatch has more overlap than older ones; no layer may
    // subtract.
    //
    // per_dispatch=[3, 6] (oldest first). At α=1 the answer is
    // max(3, 6, 0) = 6.
    approx(compute_layered_overlap(&[3.0, 6.0], 0.0, 1.0), 6.0);
    // At α=0.5: newest contributes 6, older covered by newest
    // (no new blocks), KV no contribution.
    approx(compute_layered_overlap(&[3.0, 6.0], 0.0, 0.5), 6.0);
    // Confirm even with negative-looking inputs (older > newer
    // followed by a smaller kv), the sum is non-negative.
    let result = compute_layered_overlap(&[100.0, 0.0], 0.0, 0.5);
    assert!(result >= 0.0, "expected non-negative, got {result}");
}

// -------- predicted_wait_ms queue-workload blend -------------------

// ─── Time-progressed inflight discount ──────────────────────────

#[test]
fn fresh_inflight_charged_near_full_prefill() {
    // Inflight dispatched 10ms ago; estimated prefill = 100 × 10 =
    // 1000ms. Progress ≈ 1% → discount = (1 - 0.01) ≈ 0.99. Almost
    // the full prefill cost contends with us.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        inflight_concurrent_capacity: 5,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    let pred = predicted_wait_ms(&w, &predictor, &[summary(100, 10)], Instant::now());
    // Tolerance for the few ms between creating the summary and
    // `Instant::now()`.
    assert!(
        pred.q_ms >= 0.95 * 100.0 * 10.0 && pred.q_ms <= 1.01 * 100.0 * 10.0,
        "fresh inflight should be charged near full prefill: got {}",
        pred.q_ms
    );
}

#[test]
fn done_inflight_contributes_nothing_to_queue_cost() {
    // Inflight dispatched 5000ms ago, estimated prefill = 100 × 10 =
    // 1000ms. Progress clamped to 1.0 → discount = (1 - 1.0) = 0.0.
    // No prefill ahead of this inflight → no contribution. Decode
    // contention is priced separately via `forward_pass_ms`.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        inflight_concurrent_capacity: 5,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    let pred = predicted_wait_ms(&w, &predictor, &[summary(100, 5000)], Instant::now());
    assert!(
        pred.q_ms.abs() < 0.01,
        "done inflight should contribute nothing to q_ms: got {}",
        pred.q_ms
    );
}

#[test]
fn half_done_inflight_charged_at_half() {
    // Inflight dispatched 500ms ago, estimated prefill = 100 × 10 =
    // 1000ms. Progress = 0.5 → discount = (1 - 0.5) = 0.5.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        inflight_concurrent_capacity: 5,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    let pred = predicted_wait_ms(&w, &predictor, &[summary(100, 500)], Instant::now());
    // expected ≈ 0.5 × 100 × 10 = 500ms. Tolerance for clock drift.
    assert!(
        pred.q_ms >= 480.0 && pred.q_ms <= 510.0,
        "half-done inflight ≈ half-prefill discount: got {} (expected ≈ 500)",
        pred.q_ms
    );
}

#[test]
fn warm_hit_inflight_contributes_nothing_regardless_of_age() {
    // An inflight with 0 uncached blocks contributes nothing to q_ms, however
    // recently it was dispatched.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        inflight_concurrent_capacity: 5,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    let pred = predicted_wait_ms(&w, &predictor, &[summary(0, 0)], Instant::now());
    assert!(
        pred.q_ms.abs() < 0.01,
        "warm-hit inflight has no prefill cost to charge: got {}",
        pred.q_ms
    );
}

#[test]
fn overflow_position_charged_at_full_prefill_regardless_of_age() {
    // 3 inflights with capacity 2. Position 3 is queued behind the
    // batch and gets full prefill cost regardless of how old it
    // appears.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        inflight_concurrent_capacity: 2,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    // All three appear old enough that their elapsed >> estimated
    // prefill. Positions 1 and 2 → discount=0 (no prefill ahead).
    // Position 3 → discount=1.0 (full prefill ahead) because it
    // overflowed batch capacity.
    let summaries = vec![summary(100, 5000), summary(100, 4000), summary(100, 3000)];
    let pred = predicted_wait_ms(&w, &predictor, &summaries, Instant::now());
    let expected = 1.0 * 100.0 * 10.0;
    assert!(
        (pred.q_ms - expected).abs() < 0.01,
        "position > N charged at full prefill: got {} (expected {})",
        pred.q_ms,
        expected
    );
}

#[test]
fn queue_waiting_disables_time_discount() {
    // With q_waiting > 0 the batch is at capacity, so every inflight is
    // charged its full prefill cost.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        queue_ms_per_entry: 500.0,
        inflight_concurrent_capacity: 5,
        ..Default::default()
    };
    // queue_waiting=2, both are accounted for in our inflight set
    // (so unknown_q = 0). Two inflights that are old enough to
    // have been long-done; absent q_waiting their discount would
    // collapse to 0, but q_waiting=2 forces every position past
    // the effective capacity → discount=1.0 (full prefill).
    let w = make_worker("a:1", 0, 100, 2, 1.0, 0.0);
    let pred = predicted_wait_ms(
        &w,
        &predictor,
        &[summary(100, 5000), summary(100, 4000)],
        Instant::now(),
    );
    let expected = 2.0 * 1.0 * 100.0 * 10.0; // 2 inflights × full × blocks × prefill_ms
    assert!(
        (pred.q_ms - expected).abs() < 0.01,
        "q_waiting>0 should charge every inflight at full prefill: \
         got {} (expected {})",
        pred.q_ms,
        expected
    );
}

#[test]
fn unknown_q_count_still_charged_at_baseline_rate() {
    // queue_waiting=5, only 1 inflight known. unknown_q_count=4,
    // each charged at q_rate_ms_per_entry.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        queue_ms_per_entry: 500.0,
        inflight_concurrent_capacity: 5,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 5, 1.0, 0.0);
    let pred = predicted_wait_ms(&w, &predictor, &[summary(200, 5000)], Instant::now());
    // q_waiting > 0 ⇒ no discount on the single inflight (full prefill).
    // Inflight cost = 1.0 * 200 * 10 = 2000.
    // Unknown cost = 4 * 500 = 2000.
    let expected = 2000.0 + 2000.0;
    assert!(
        (pred.q_ms - expected).abs() < 0.01,
        "unknown_q_count at baseline rate: got {} (expected {})",
        pred.q_ms,
        expected
    );
}

#[test]
fn capacity_one_charges_fresh_inflight_near_full_prefill() {
    // N=1: position 1 > 1 is false → time-progressed. With
    // elapsed=10ms vs estimated 100×10=1000ms, progress=1% →
    // discount = (1 - 0.01) ≈ 0.99 → near full prefill.
    let predictor = PredictorConfig {
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        inflight_concurrent_capacity: 1,
        ..Default::default()
    };
    let w = make_worker("a:1", 0, 100, 0, 1.0, 0.0);
    let pred = predicted_wait_ms(&w, &predictor, &[summary(100, 10)], Instant::now());
    // Just-dispatched inflight near full prefill: ≈ 1000ms.
    assert!(
        pred.q_ms >= 0.95 * 1000.0,
        "N=1 just-dispatched should be near full prefill: got {}",
        pred.q_ms
    );
}

// ─── Cache-replication override ─────────────────────────────────

/// Config for replication tests: zero queue/cache penalties and a flat
/// 10ms-per-uncached-block prefill.
fn replication_cfg() -> (PredictorConfig, PickerConfig) {
    let predictor = PredictorConfig {
        base_ms: 0.0,
        queue_ms_per_entry: 0.0,
        cache_pressure_penalty_ms: 0.0,
        saturated_cold_ms_per_entry: 0.0,
        cache_pressure_saturated_threshold: 1.0,
        inflight_concurrent_capacity: 5,
        attention_uncached_block_ms: 10.0,
        attention_uncached_per_every_block_ms: 0.0,
        // No U-shape penalty, to keep the arithmetic flat.
        q_running_penalty_peak_ms: 0.0,
        q_running_penalty_peak_qr: 2,
        q_running_penalty_falloff_qr: 5,
        // No decode contention either.
        forward_pass_ms: 0.0,
        expected_overlap_alpha: 1.0,
    };
    let picker = PickerConfig {
        top_k_min: 1,
        top_k_fraction: 0.0,
        inflight_tracking: false,
        tie_tolerance_ms: 0.0,
        // 500% slowdown allowed; tests that need tighter set their own.
        replication_threshold_pct: 500.0,
        replication_warm_target: 2,
        replication_warm_threshold_fraction: 0.5,
    };
    (predictor, picker)
}

#[test]
fn replication_disabled_by_threshold_zero() {
    // Threshold=0 disables the feature entirely; pick is unchanged.
    let (predictor, mut picker) = replication_cfg();
    picker.replication_threshold_pct = 0.0;
    // total=100, warm=80 (above 50% threshold), cold workers at 0
    let workers = vec![
        make_worker("warm:1", 80, 100, 0, 1.0, 0.0),
        make_worker("cold:1", 0, 100, 0, 1.0, 0.0),
        make_worker("cold:2", 0, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = vec![80_u32, 0, 0];
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    // Should pick warm:1 (cheapest) and *not* swap.
    assert_eq!(picked.worker.endpoint, "warm:1");
}

#[test]
fn replication_skipped_when_best_is_cold() {
    // All workers cold → there's nothing to replicate. Pick is
    // determined by normal picker logic; replication does not fire.
    let (predictor, picker) = replication_cfg();
    let workers = vec![
        make_worker("w:1", 0, 100, 0, 1.0, 0.0),
        make_worker("w:2", 0, 100, 0, 1.0, 0.0),
        make_worker("w:3", 0, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = vec![0_u32, 0, 0];
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    // No replication; pick is just the normal one. All are tied;
    // any one is fine.
    assert!(["w:1", "w:2", "w:3"].contains(&picked.worker.endpoint.as_str()));
}

#[test]
fn replication_skipped_when_already_at_target() {
    // Two warm workers + replication_warm_target=2 → already
    // replicated enough; picker doesn't redirect to the cold one.
    let (predictor, picker) = replication_cfg();
    let workers = vec![
        make_worker("warm:1", 80, 100, 0, 1.0, 0.0),
        make_worker("warm:2", 80, 100, 0, 1.0, 0.0),
        make_worker("cold:1", 0, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = vec![80_u32, 80, 0];
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    // One of the warm workers; not the cold one.
    assert!(
        picked.worker.endpoint.starts_with("warm:"),
        "expected a warm worker; got {}",
        picked.worker.endpoint
    );
}

#[test]
fn replication_fires_within_threshold() {
    // warm:1 wins normally (predicted 200ms vs cold:1 1000ms).
    // Δ = 800ms. max_extra = 200 × 500% = 1000ms. 800 ≤ 1000
    // → swap to cold:1.
    let (predictor, picker) = replication_cfg();
    let workers = vec![
        make_worker("warm:1", 80, 100, 0, 1.0, 0.0),
        make_worker("cold:1", 0, 100, 0, 1.0, 0.0),
        make_worker("cold:2", 0, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = vec![80_u32, 0, 0];
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    // Replication redirected us to a cold worker.
    assert!(
        picked.worker.endpoint.starts_with("cold:"),
        "expected replication to swap to cold; got {}",
        picked.worker.endpoint
    );
}

#[test]
fn replication_skipped_above_threshold() {
    // warm:1 is mostly-warm (matched 80/100 → predicted 200ms).
    // cold:1 is fully cold (1000ms). Δ = 800ms. With pct=300%,
    // max_extra = 200 × 3 = 600ms; 800 > 600 → no replication.
    let (predictor, mut picker) = replication_cfg();
    picker.replication_threshold_pct = 300.0;
    let workers = vec![
        make_worker("warm:1", 80, 100, 0, 1.0, 0.0),
        make_worker("cold:1", 0, 100, 0, 1.0, 0.0),
        make_worker("cold:2", 0, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = vec![80_u32, 0, 0];
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    assert_eq!(
        picked.worker.endpoint, "warm:1",
        "Δ > threshold should keep warm pick"
    );
}

#[test]
fn replication_skips_about_to_be_warm_candidates() {
    // warm:1 (matched=80, predicted=200ms) wins on lowest
    // predicted_ms over cold:1 (matched=70, predicted=300ms).
    // cold:1 has raw kv_blocks=0 BUT post-credit matched=70
    // (in-flight prefill warming it), so it's excluded as a
    // replication candidate; cold:2 (truly cold, matched=0) is
    // the only candidate. (warm:1 deliberately at matched=80
    // not 100 so best_ms is non-zero; otherwise the percentage
    // threshold would reject every candidate.)
    let (predictor, picker) = replication_cfg();
    let workers = vec![
        make_worker("warm:1", 80, 100, 0, 1.0, 0.0),
        make_worker("cold:1", 70, 100, 0, 1.0, 0.0), // matched (credit) high, kv low
        make_worker("cold:2", 0, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    let kv = vec![80_u32, 0, 0]; // cold:1 raw kv = 0 (not committed)
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    // warm:1 (predicted 200ms) wins normally. Replication:
    // - warm_count = 1 (just warm:1; cold:1 has raw kv=0)
    // - target = 2 → consider replication
    // - candidate filter excludes cold:1 (matched=70 ≥ 50 → already
    //   warming up) and accepts cold:2 (matched=0 < 50)
    // - cold:2 predicted = 1000ms; Δ = 800ms; max_extra = 200×5 =
    //   1000ms; 800 ≤ 1000 → fires
    assert_eq!(
        picked.worker.endpoint, "cold:2",
        "should swap to truly-cold cold:2, NOT to already-warming cold:1"
    );
}

#[test]
fn replication_picks_cheapest_candidate() {
    // Two cold candidates with different inflight costs. Replication
    // should pick the cheaper one.
    let (predictor, picker) = replication_cfg();
    let workers = vec![
        make_worker("warm:1", 80, 100, 0, 1.0, 0.0),
        // Both cold; differentiate via matched-blocks visible only
        // to the picker. (cold:1 has slightly more matched to make
        // its predicted_ms smaller.)
        make_worker("cold:1", 30, 100, 0, 1.0, 0.0),
        make_worker("cold:2", 10, 100, 0, 1.0, 0.0),
    ];
    let inflight = zero_inflight(&workers);
    let workload = zero_workload(&workers);
    // raw kv_blocks: warm:1 committed (80), others below threshold.
    let kv = vec![80_u32, 30, 10];
    let picked = pick_endpoint(&inputs(
        &workers, &predictor, &picker, &inflight, &workload, &kv,
    ))
    .expect("non-empty");
    // Candidates: cold:1 (700ms predicted) and cold:2 (900ms).
    // cold:1 is cheaper → replication picks it.
    assert_eq!(picked.worker.endpoint, "cold:1");
}

#[test]
fn replication_threshold_pct_is_scale_invariant() {
    // The percentage threshold gives the same decision regardless of prompt
    // size. Small (100-block) and large (1000-block) prompts both have
    // warm:1 at 60% matched and cold:1 fully cold, so Δ:best is 1.5x in
    // both.
    let (base_predictor, base_picker) = replication_cfg();

    let run = |predictor: &PredictorConfig, picker: &PickerConfig, scale: u32| -> String {
        let warm_matched = 60 * scale;
        let total = 100 * scale;
        let workers = vec![
            make_worker("warm:1", warm_matched, total, 0, 1.0, 0.0),
            make_worker("cold:1", 0, total, 0, 1.0, 0.0),
            make_worker("cold:2", 0, total, 0, 1.0, 0.0),
        ];
        let kv = vec![warm_matched, 0, 0];
        pick_endpoint(&inputs(
            &workers,
            predictor,
            picker,
            &zero_inflight(&workers),
            &zero_workload(&workers),
            &kv,
        ))
        .expect("non-empty")
        .worker
        .endpoint
        .clone()
    };

    // 200% slowdown allowed. Δ:best = 1.5x = 150% slowdown in
    // both cases → both should fire.
    let mut picker = base_picker.clone();
    picker.replication_threshold_pct = 200.0;
    let small = run(&base_predictor, &picker, 1); // 100-block prompt
    let big = run(&base_predictor, &picker, 10); // 1000-block prompt
    assert!(
        small.starts_with("cold:"),
        "small prompt should replicate at pct=200; got {small}"
    );
    assert!(
        big.starts_with("cold:"),
        "large prompt should ALSO replicate at pct=200 (scale-invariant); got {big}"
    );

    // Sanity-check the inverse direction too: a tighter threshold
    // skips BOTH sizes uniformly. Δ:best = 150%, so pct=100
    // (max_extra = 100% of best, i.e. allow doubling) skips both.
    let mut picker = base_picker;
    picker.replication_threshold_pct = 100.0;
    let small = run(&base_predictor, &picker, 1);
    let big = run(&base_predictor, &picker, 10);
    assert_eq!(
        small, "warm:1",
        "small prompt should skip at pct=100 (Δ=150% > 100%); got {small}"
    );
    assert_eq!(
        big, "warm:1",
        "large prompt should ALSO skip at pct=100; got {big}"
    );
}
