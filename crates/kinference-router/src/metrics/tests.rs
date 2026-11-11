use super::*;

use std::ops::Deref;

use prometheus::{Encoder, TextEncoder};

/// A `RouterMetrics` with the registry it renders through.
struct Metrics {
    m: RouterMetrics,
    registry: Registry,
}

impl Deref for Metrics {
    type Target = RouterMetrics;
    fn deref(&self) -> &RouterMetrics {
        &self.m
    }
}

impl Metrics {
    fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }
}

/// Tenant labelling off, the `metrics` default.
fn metrics() -> Metrics {
    with(TenantLabels::None)
}

#[test]
fn records_and_accumulates_per_model_and_qos() {
    let m = metrics();
    m.record_usage("m-a", "interactive", "", 100, 40);
    m.record_usage("m-b", "batch", "", 7, 3);
    m.record_usage("m-a", "interactive", "", 50, 10); // accumulates onto m-a/interactive

    let out = m.render();
    assert!(
        out.contains(
            "kinference_prompt_tokens_total{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 150"
        ),
        "prompt m-a/interactive should be 100+50=150:\n{out}"
    );
    assert!(
        out.contains(
            "kinference_completion_tokens_total{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 50"
        ),
        "completion m-a/interactive should be 40+10=50:\n{out}"
    );
    // A different qos is a distinct series.
    assert!(
        out.contains(
            "kinference_prompt_tokens_total{model=\"m-b\",qos=\"batch\",tenant=\"default\"} 7"
        ),
        "prompt m-b/batch should be 7:\n{out}"
    );
    assert!(out.contains("# TYPE kinference_prompt_tokens_total counter"));
}

/// Provider latency, which never reaches `record_ttft`.
#[test]
fn provider_ttft_is_recorded_by_provider_and_model() {
    let m = metrics();
    m.record_provider_ttft("default/databricks", "qwen-2.5", 420);
    m.record_provider_ttft("default/databricks", "qwen-2.5", 1500);
    m.record_provider_ttft("default/together", "llama-3", 90);
    let out = m.render();

    assert!(
        out.contains("# TYPE kinference_provider_ttft_ms histogram"),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_count{model=\"qwen-2.5\",provider=\"default/databricks\"} 2"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_sum{model=\"qwen-2.5\",provider=\"default/databricks\"} 1920"
        ),
        "{out}"
    );
    // A different provider is a distinct series.
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_count{model=\"llama-3\",provider=\"default/together\"} 1"
        ),
        "{out}"
    );
    // Same bucket edges as `kinference_ttft_ms`.
    assert!(
        out.contains(
            "kinference_provider_ttft_ms_bucket{model=\"qwen-2.5\",provider=\"default/databricks\",le=\"500\"} 1"
        ),
        "420ms falls in le=500, 1500ms does not:\n{out}"
    );
}

/// An empty provider or model is skipped.
#[test]
fn provider_ttft_skips_blank_labels() {
    let m = metrics();
    m.record_provider_ttft("", "qwen-2.5", 100);
    m.record_provider_ttft("default/databricks", "", 100);
    let out = m.render();
    assert!(
        !out.contains("kinference_provider_ttft_ms_count"),
        "neither should have been recorded:\n{out}"
    );
}

#[test]
fn length_histograms_carry_model_and_qos() {
    let m = metrics();
    m.record_usage("m-a", "interactive", "", 100, 40); // prompt ≤128, completion ≤64
    m.record_usage("m-a", "interactive", "", 5000, 300); // prompt ≤8192, completion ≤512
    let out = m.render();

    assert!(
        out.contains("# TYPE kinference_prompt_length_tokens histogram"),
        "{out}"
    );
    assert!(
        out.contains("kinference_prompt_length_tokens_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 2"),
        "{out}"
    );
    assert!(
        out.contains("kinference_prompt_length_tokens_sum{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 5100"),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_prompt_length_tokens_bucket{model=\"m-a\",qos=\"interactive\",tenant=\"default\",le=\"128\"} 1"
        ),
        "{out}"
    );
    assert!(
        out.contains("kinference_completion_length_tokens_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 2"),
        "{out}"
    );
}

#[test]
fn ttft_histogram_carries_model_and_qos() {
    let m = metrics();
    m.record_ttft("m-a", "interactive", "", 250);
    m.record_ttft("m-a", "interactive", "", 1500);
    let out = m.render();

    assert!(out.contains("# TYPE kinference_ttft_ms histogram"), "{out}");
    assert!(
        out.contains(
            "kinference_ttft_ms_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 2"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "kinference_ttft_ms_sum{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 1750"
        ),
        "{out}"
    );
    assert!(
        out.contains("kinference_ttft_ms_bucket{model=\"m-a\",qos=\"interactive\",tenant=\"default\",le=\"250\"} 1"),
        "{out}"
    );
}

#[test]
fn itl_buckets_accumulate_cumulatively() {
    let m = metrics();
    // Layout 1: 1 gap in bucket 0 (≤5ms), 2 gaps in bucket 7 (≤75ms).
    let mut counts = vec![0u32; 15];
    counts[0] = 1;
    counts[7] = 2;
    m.record_itl("m-a", "interactive", "", 1, &counts);
    let out = m.render();

    // Cumulative: le="5" sees only bucket 0; le="75" sees buckets 0..=7.
    // (Labels sort alphabetically, so `le` renders first here, unlike native
    // histograms, which append `le` last.)
    assert!(
        out.contains(
            "kinference_itl_ms_bucket{le=\"5\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 1"
        ),
        "le=5 should be 1:\n{out}"
    );
    assert!(
        out.contains(
            "kinference_itl_ms_bucket{le=\"50\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 1"
        ),
        "le=50 should still be 1 (bucket 7 is >50):\n{out}"
    );
    assert!(
        out.contains(
            "kinference_itl_ms_bucket{le=\"75\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 3"
        ),
        "le=75 should be 1+2=3:\n{out}"
    );
    assert!(
        out.contains("kinference_itl_ms_bucket{le=\"+Inf\",model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 3"),
        "+Inf should be total 3:\n{out}"
    );
    assert!(
        out.contains(
            "kinference_itl_ms_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 3"
        ),
        "count should be 3:\n{out}"
    );
}

#[test]
fn itl_skips_on_layout_or_length_mismatch() {
    let m = metrics();
    let good = vec![1u32; 15];
    m.record_itl("m-a", "interactive", "", 1, &good); // total 15
    m.record_itl("m-a", "interactive", "", 2, &good); // wrong layout → skip
    m.record_itl("m-a", "interactive", "", 1, &[0, 1, 2]); // wrong length → skip
    let out = m.render();
    // Only the first (valid) report counts: 15 samples.
    assert!(
        out.contains(
            "kinference_itl_ms_count{model=\"m-a\",qos=\"interactive\",tenant=\"default\"} 15"
        ),
        "only the layout-1, length-15 report should count:\n{out}"
    );
}

#[test]
fn empty_model_is_skipped() {
    let m = metrics();
    m.record_usage("", "interactive", "", 100, 40);
    m.record_ttft("", "interactive", "", 100);
    m.record_itl("", "interactive", "", 1, &[1u32; 15]);
    let out = m.render();
    assert!(
        !out.contains("model=\"\""),
        "empty model should be skipped:\n{out}"
    );
}

/// Build a registry with an explicit tenant policy.
fn with(labels: TenantLabels) -> Metrics {
    let registry = Registry::new();
    let m = RouterMetrics::new(
        &registry,
        &MetricsConfig {
            tenant_labels: labels,
        },
    );
    Metrics { m, registry }
}

/// The allowlist decides which tenants keep their identity.
#[test]
fn tenant_labels_bucket_per_the_allowlist() {
    // "none": every tenant collapses, including one that looks real.
    let m = with(TenantLabels::None);
    m.record_usage("m", "batch", "acme", 10, 5);
    let out = m.render();
    assert!(
        out.contains(
            r#"kinference_prompt_tokens_total{model="m",qos="batch",tenant="default"} 10"#
        ),
        "none must collapse every tenant:\n{out}"
    );
    assert!(
        !out.contains(r#"tenant="acme""#),
        "acme must not appear:\n{out}"
    );

    // "all": every tenant keeps its identity.
    let m = with(TenantLabels::All);
    m.record_usage("m", "batch", "acme", 10, 5);
    m.record_usage("m", "batch", "globex", 7, 3);
    let out = m.render();
    assert!(out.contains(r#"tenant="acme"} 10"#), "{out}");
    assert!(out.contains(r#"tenant="globex"} 7"#), "{out}");

    // A list: a hit keeps its identity, a miss buckets.
    let m = with(TenantLabels::Allow(vec!["acme".into()]));
    m.record_usage("m", "batch", "acme", 10, 5);
    m.record_usage("m", "batch", "globex", 7, 3);
    let out = m.render();
    assert!(
        out.contains(r#"tenant="acme"} 10"#),
        "allowlist hit:\n{out}"
    );
    assert!(
        out.contains(r#"tenant="default"} 7"#),
        "allowlist miss must bucket:\n{out}"
    );
    assert!(!out.contains(r#"tenant="globex""#), "{out}");
}

/// An empty tenant always buckets, whatever the policy says.
///
/// Unlike policy, where `tenant: ""` is a distinct matchable value.
#[test]
fn an_empty_tenant_always_buckets_under_default() {
    for labels in [
        TenantLabels::None,
        TenantLabels::All,
        TenantLabels::Allow(vec![String::new()]),
    ] {
        let m = with(labels.clone());
        m.record_usage("m", "batch", "", 10, 5);
        let out = m.render();
        assert!(
            out.contains(
                r#"kinference_prompt_tokens_total{model="m",qos="batch",tenant="default"} 10"#
            ),
            "{labels:?} must bucket an empty tenant:\n{out}"
        );
    }
}

/// Where each tenant's requests went, by destination and type, under the
/// same tenant bucketing as every other tenant family.
#[test]
fn routed_requests_count_by_tenant_destination_and_type() {
    use kinference_policy::crd::BackendType::{Local, Provider};

    let m = with(TenantLabels::Allow(vec!["acme".into()]));
    m.record_routed("acme", "qwen-ws", Local);
    m.record_routed("acme", "qwen-ws", Local);
    m.record_routed("acme", "databricks", Provider);
    m.record_routed("globex", "qwen-ws", Local);
    let out = m.render();
    for line in [
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="acme",type="Local"} 2"#,
        r#"kinference_routed_requests_total{destination="databricks",tenant="acme",type="Provider"} 1"#,
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(
        !out.contains(r#"tenant="globex""#),
        "not allowlisted: {out}"
    );
}

/// A tenant literally named `default` shares the fallback bucket.
///
/// Under an allowlist, an allowlisted `default` and every non-allowlisted
/// tenant land on one series.
#[test]
fn a_tenant_named_default_collides_with_the_fallback_bucket() {
    let m = with(TenantLabels::Allow(vec!["default".into()]));
    m.record_usage("m", "batch", "default", 10, 5); // allowlisted
    m.record_usage("m", "batch", "someone-else", 7, 3); // bucketed
    let out = m.render();
    assert!(
        out.contains(
            r#"kinference_prompt_tokens_total{model="m",qos="batch",tenant="default"} 17"#
        ),
        "both land on the same series (10 + 7):\n{out}"
    );
}

/// Every label lands in the position its declaration names.
///
/// `with_label_values` matches by order, and on `itl_bucket` all four values
/// are `&str`, so a transposition would compile. Self-identifying values make
/// it visible.
#[test]
fn every_label_lands_in_its_declared_position() {
    let m = with(TenantLabels::All);
    let mut counts = vec![0u32; 15];
    counts[0] = 1;
    m.record_usage("MODEL", "batch", "TENANT", 10, 5);
    m.record_ttft("MODEL", "batch", "TENANT", 250);
    m.record_request_duration("MODEL", "batch", "TENANT", 4_000);
    m.record_itl("MODEL", "batch", "TENANT", 1, &counts);
    let out = m.render();

    // Declared labels sort alphabetically; a native histogram appends `le`
    // after them, while `itl_bucket` declares `le` so it sorts first.
    for expect in [
        r#"kinference_prompt_tokens_total{model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_completion_tokens_total{model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_prompt_length_tokens_bucket{model="MODEL",qos="batch",tenant="TENANT",le="8"}"#,
        r#"kinference_completion_length_tokens_count{model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_ttft_ms_bucket{model="MODEL",qos="batch",tenant="TENANT",le="250"}"#,
        r#"kinference_request_duration_ms_bucket{model="MODEL",qos="batch",tenant="TENANT",le="5000"}"#,
        r#"kinference_itl_ms_bucket{le="5",model="MODEL",qos="batch",tenant="TENANT"}"#,
        r#"kinference_itl_ms_count{model="MODEL",qos="batch",tenant="TENANT"}"#,
    ] {
        assert!(
            out.contains(expect),
            "missing or transposed:\n  {expect}\n{out}"
        );
    }
    // The transposition this guards against, stated as its own assertion.
    assert!(
        !out.contains(r#"tenant="5""#) && !out.contains(r#"le="TENANT""#),
        "le and tenant are transposed:\n{out}"
    );
}

/// The duration histogram shares `ttft_ms`'s bucket edges and extends past
/// them.
#[test]
fn request_duration_is_comparable_to_ttft_and_has_a_longer_tail() {
    let m = metrics();
    m.record_ttft("m", "batch", "", 250);
    m.record_request_duration("m", "batch", "", 250);
    let out = m.render();

    // Same edge, same bucket: an equal observation lands identically in both.
    for fam in ["kinference_ttft_ms", "kinference_request_duration_ms"] {
        assert!(
            out.contains(&format!(
                r#"{fam}_bucket{{model="m",qos="batch",tenant="default",le="250"}} 1"#
            )),
            "{fam} must bucket 250 at le=250:\n{out}"
        );
    }

    // The tail exists only on the duration family, so a 5-minute generation
    // doesn't land in +Inf.
    assert!(
        out.contains(r#"kinference_request_duration_ms_bucket{model="m",qos="batch",tenant="default",le="300000"}"#),
        "duration needs buckets past ttft's 80s ceiling:\n{out}"
    );
    assert!(
        !out.contains(
            r#"kinference_ttft_ms_bucket{model="m",qos="batch",tenant="default",le="300000"}"#
        ),
        "ttft should NOT have grown a 300s bucket:\n{out}"
    );
}

/// A blank model is skipped, matching every other per-request recorder.
#[test]
fn request_duration_skips_a_blank_model() {
    let m = metrics();
    m.record_request_duration("", "batch", "", 1_000);
    assert!(
        !m.render().contains("kinference_request_duration_ms"),
        "a blank model must not be bucketed under an empty label"
    );
}

/// Assert no SERIES line exists for a family.
///
/// Skips `#` lines, since a family's name can appear in other families' HELP
/// text.
fn assert_no_series(out: &str, name: &str) {
    let found: Vec<&str> = out
        .lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(name))
        .collect();
    assert!(found.is_empty(), "expected no {name} series, got {found:?}");
}

/// Assert on a WHOLE rendered line, not a substring.
///
/// A substring check would let `... 200` pass for `... 20`.
fn assert_line(out: &str, expect: &str) {
    let name = expect.split('{').next().unwrap();
    let got = out
        .lines()
        .find(|l| l.starts_with(name) && l.starts_with(&expect[..expect.rfind(' ').unwrap()]));
    assert_eq!(
        got,
        Some(expect),
        "\nexpected line: {expect}\nrendered:\n{out}"
    );
}

/// The counters accumulate blocks and the histogram observes the ratio.
#[test]
fn prefix_match_records_both_numerators_against_one_denominator() {
    let m = metrics();
    // Two picks: 8/10 layered with 6 from the radix tree, then 5/10 with 5.
    m.record_prefix_match("m", "batch", 10, 8, 6);
    m.record_prefix_match("m", "batch", 10, 5, 5);
    let out = m.render();

    assert_line(
        &out,
        r#"kinference_prefix_requested_blocks_total{model="m",qos="batch"} 20"#,
    );
    assert_line(
        &out,
        r#"kinference_prefix_matched_blocks_total{model="m",qos="batch"} 13"#,
    );
    assert_line(
        &out,
        r#"kinference_prefix_radix_matched_blocks_total{model="m",qos="batch"} 11"#,
    );
    // 13/20 layered vs 11/20 radix: the difference is the in-flight credit.

    assert_line(
        &out,
        r#"kinference_prefix_match_ratio_count{model="m",qos="batch"} 2"#,
    );
    // 0.8 and 0.5: both at or below le=0.8, only one at or below le=0.5.
    assert_line(
        &out,
        r#"kinference_prefix_match_ratio_bucket{model="m",qos="batch",le="0.5"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_prefix_match_ratio_bucket{model="m",qos="batch",le="0.8"} 2"#,
    );
}

/// A degraded pick records nothing, in both of its shapes: a tokenize failure
/// (`requested == 0, layered == 0`) and a prefix-index failure (`layered > 0`
/// against a zero-length prompt). This pins the recorder's own backstop.
#[test]
fn a_degraded_pick_records_no_prefix_metrics() {
    for (label, layered) in [("tokenize", 0), ("prefix_index", 4)] {
        let m = metrics();
        m.record_prefix_match("m", "batch", 0, layered, 0);
        assert!(
            !m.render().contains("kinference_prefix_"),
            "{label}-degraded picks must not reach the prefix metrics"
        );
    }
}

/// Matched blocks exceeding the prompt is clamped, and counted.
///
/// A DP>1 worker publishing one prefix from two ranks reports roughly double.
#[test]
fn an_overmatch_is_clamped_to_one_and_counted() {
    let m = metrics();
    m.record_prefix_match("m", "batch", 10, 18, 18);
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_prefix_overmatch_total{model="m",qos="batch"} 1"#,
    );
    // Whole-line, since an unclamped `1.8` contains `1` as a prefix.
    let sum_line = out
        .lines()
        .find(|l| l.starts_with("kinference_prefix_match_ratio_sum{"))
        .expect("the histogram must have a _sum line");
    assert_eq!(
        sum_line, r#"kinference_prefix_match_ratio_sum{model="m",qos="batch"} 1"#,
        "the observation must be clamped to 1.0, not 1.8"
    );
    // The counters keep the raw values; only the ratio is clamped.
    assert_line(
        &out,
        r#"kinference_prefix_matched_blocks_total{model="m",qos="batch"} 18"#,
    );
}

/// A blank model is skipped, matching every other per-request recorder.
#[test]
fn prefix_match_skips_a_blank_model() {
    let m = metrics();
    m.record_prefix_match("", "batch", 10, 8, 6);
    assert!(!m.render().contains("kinference_prefix_"));
}

/// Pick outcomes and latency are labelled by the wire outcome.
#[test]
fn picks_are_counted_and_timed_by_outcome() {
    let m = metrics();
    m.record_pick("ok", 3.0);
    m.record_pick("ok", 30.0);
    m.record_pick("no_workers", 0.6);
    let out = m.render();

    assert_line(&out, r#"kinference_picks_total{outcome="ok"} 2"#);
    assert_line(&out, r#"kinference_picks_total{outcome="no_workers"} 1"#);
    // Latency is labelled by outcome because a rejected pick is fast and would
    // otherwise flatter the successful distribution.
    assert_line(
        &out,
        r#"kinference_pick_duration_ms_bucket{outcome="ok",le="5"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_pick_duration_ms_bucket{outcome="ok",le="50"} 2"#,
    );
    assert_line(
        &out,
        r#"kinference_pick_duration_ms_count{outcome="no_workers"} 1"#,
    );
}

/// `PickOutcome` maps to the wire names an operator actually sees, not the
/// Rust variant names.
#[test]
fn pick_outcome_labels_match_the_wire_names() {
    assert_eq!(PickOutcome::Success.as_label(), "ok");
    assert_eq!(PickOutcome::NoOpinion.as_label(), "bad_request");
    assert_eq!(PickOutcome::NotFound.as_label(), "no_workers");
}

/// The three tokenize paths are exhaustive: they sum to the number of attempts.
#[test]
fn tokenize_paths_partition_every_attempt() {
    let m = metrics();
    for _ in 0..3 {
        m.record_tokenize_path("local");
        m.record_tokenize_duration(2.0);
    }
    m.record_tokenize_path("http_fallback");
    m.record_tokenize_duration(40.0);
    m.record_tokenize_path("unavailable");
    m.record_tokenize_duration(900.0);
    let out = m.render();

    assert_line(&out, r#"kinference_tokenize_total{path="local"} 3"#);
    assert_line(&out, r#"kinference_tokenize_total{path="http_fallback"} 1"#);
    assert_line(&out, r#"kinference_tokenize_total{path="unavailable"} 1"#);
    // One duration per attempt, so the histogram count is the path total.
    assert_line(&out, "kinference_tokenize_duration_ms_count 5");
}

/// Compare-mode agreement, including the divergence that silently breaks all
/// prefix hashing.
#[test]
fn tokenize_compare_counts_divergences_separately() {
    let m = metrics();
    m.record_tokenize_compare("match");
    m.record_tokenize_compare("divergence");
    m.record_tokenize_compare("error");
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_tokenize_compare_total{result="match"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_tokenize_compare_total{result="divergence"} 1"#,
    );
    assert_line(
        &out,
        r#"kinference_tokenize_compare_total{result="error"} 1"#,
    );
}

/// An empty ITL report records nothing, and is not an error.
///
/// The proto sends empty `itl_bucket_counts` for responses with fewer than two
/// chunks. Only behavior is checked; the log level isn't, since tests install
/// no tracing subscriber.
#[test]
fn an_empty_itl_report_is_not_an_error() {
    let m = metrics();
    m.record_itl("m", "batch", "", 1, &[]);
    assert!(
        !m.render().contains("kinference_itl_ms"),
        "an empty report has nothing to bucket"
    );
    // A genuinely wrong length is still refused.
    m.record_itl("m", "batch", "", 1, &[1, 2, 3]);
    assert!(!m.render().contains("kinference_itl_ms"));
    // And a correct one still lands.
    m.record_itl("m", "batch", "", 1, &[1u32; 15]);
    assert!(m.render().contains("kinference_itl_ms_count"));
}

/// Engine-measured cached prompt tokens accumulate per tenant.
#[test]
fn cached_prompt_tokens_accumulate_per_tenant() {
    let m = with(TenantLabels::All);
    m.record_cached_prompt_tokens("qwen", "batch", "acme", Some(900));
    m.record_cached_prompt_tokens("qwen", "batch", "acme", Some(100));
    m.record_cached_prompt_tokens("qwen", "batch", "globex", Some(5));
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="qwen",qos="batch",tenant="acme"} 1000"#,
    );
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="qwen",qos="batch",tenant="globex"} 5"#,
    );
}

/// A missing cached_tokens is not a zero, which would understate the cache
/// hit.
#[test]
fn an_absent_cached_tokens_is_counted_separately_from_zero() {
    let m = metrics();

    // Explicit zero: a real report of no cache hit. It contributes an
    // observation, and nothing to the absent counter.
    m.record_cached_prompt_tokens("m", "batch", "", Some(0));
    let out = m.render();
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="m",qos="batch",tenant="default"} 0"#,
    );
    assert_line(&out, "kinference_cached_tokens_absent_total 0");

    // Absent: the caller said nothing.
    m.record_cached_prompt_tokens("m", "batch", "", None);
    m.record_cached_prompt_tokens("m", "batch", "", None);
    let out = m.render();
    assert_line(&out, "kinference_cached_tokens_absent_total 2");
    // ...and it must not have moved the token counter.
    assert_line(
        &out,
        r#"kinference_cached_prompt_tokens_total{model="m",qos="batch",tenant="default"} 0"#,
    );
}

/// A blank model is skipped, matching every other per-request recorder.
#[test]
fn cached_prompt_tokens_skips_a_blank_model() {
    let m = metrics();
    m.record_cached_prompt_tokens("", "batch", "", Some(10));
    assert_no_series(&m.render(), "kinference_cached_prompt_tokens_total");
}

// ── Pre-initialization of closed label sets ───────────────────────────────

/// Whole-line match, so `... 0` doesn't match `0.5`.
fn has_line(out: &str, line: &str) -> bool {
    out.lines().any(|l| l == line)
}

/// Every closed-set child exists at zero on a registry that has seen no
/// traffic.
#[test]
fn preinit_creates_every_closed_set_series_at_zero() {
    let out = metrics().render();

    let counters: [(&str, &[&str]); 6] = [
        ("kinference_picks_total{outcome=", &PICK_OUTCOMES),
        ("kinference_score_degraded_total{reason=", &DEGRADE_REASONS),
        ("kinference_tokenize_total{path=", &TOKENIZE_PATHS),
        (
            "kinference_tokenize_compare_total{result=",
            &TOKENIZE_COMPARE_RESULTS,
        ),
        (
            "kinference_body_rejected_total{reason=",
            &BODY_REJECT_REASONS,
        ),
        ("kinference_body_aborted_total{reason=", &BODY_ABORT_REASONS),
    ];
    for (prefix, values) in counters {
        for v in values {
            let line = format!("{prefix}\"{v}\"}} 0");
            assert!(
                has_line(&out, &line),
                "missing seeded series `{line}`:\n{out}"
            );
        }
    }
    for v in POLICY_DECISION_OUTCOMES {
        let line = format!("kinference_policy_decisions_total{{outcome=\"{v}\"}} 0");
        assert!(has_line(&out, &line), "missing `{line}`:\n{out}");
    }
    // The histogram beside `picks` is not seeded; see `preinit`.
    assert!(
        !out.lines()
            .any(|l| l.starts_with("kinference_pick_duration_ms_count{")),
        "pick_duration_ms must NOT be seeded — a zero histogram yields no \
         quantile, so the series would be pure cost:\n{out}"
    );
}

/// Open label sets stay empty: `{model,qos,tenant}` families and
/// `policy_rule_matches` are not seeded.
#[test]
fn preinit_does_not_seed_open_label_sets() {
    let out = metrics().render();
    for family in [
        "kinference_prompt_tokens_total",
        "kinference_completion_tokens_total",
        "kinference_cached_prompt_tokens_total",
        "kinference_request_duration_ms_count",
        "kinference_ttft_ms_count",
        "kinference_itl_ms_count",
        "kinference_provider_ttft_ms_count",
        "kinference_prefix_requested_blocks_total",
        "kinference_policy_rule_matches_total",
    ] {
        let opened = format!("{family}{{");
        assert!(
            !out.lines().any(|l| l.starts_with(&opened)),
            "`{family}` must have NO series before traffic; seeding it would \
             invent cardinality:\n{out}"
        );
    }
}

/// Ties [`PICK_OUTCOMES`] to its source of truth via an exhaustive `match`.
#[test]
fn pick_outcome_labels_match_the_enum() {
    use PickOutcome as P;
    let all = [P::Success, P::NoOpinion, P::NotFound];
    for v in all {
        match v {
            P::Success | P::NoOpinion | P::NotFound => {}
        }
    }
    let from_enum: Vec<&str> = all.into_iter().map(|o| o.as_label()).collect();
    assert_eq!(from_enum, PICK_OUTCOMES);
}

#[test]
fn degrade_reason_labels_match_the_enum() {
    use crate::service::Degraded as D;
    let all = [D::Tokenize, D::PrefixIndex];
    for v in all {
        match v {
            D::Tokenize | D::PrefixIndex => {}
        }
    }
    let from_enum: Vec<&str> = all.into_iter().map(|r| r.as_label()).collect();
    assert_eq!(from_enum, DEGRADE_REASONS);
}

#[test]
fn body_reject_reason_labels_match_the_enum() {
    use crate::body_store::RejectReason as R;
    let all = [
        R::DeclaredTooLarge,
        R::RequestTooLarge,
        R::ChunkTooLarge,
        R::ServerFull,
    ];
    for v in all {
        match v {
            R::DeclaredTooLarge | R::RequestTooLarge | R::ChunkTooLarge | R::ServerFull => {}
        }
    }
    let from_enum: Vec<&str> = all.into_iter().map(|r| r.as_label()).collect();
    assert_eq!(from_enum, BODY_REJECT_REASONS);
}

/// `cached_tokens_absent` and `cached_tokens_reported` partition every
/// report: exactly one fires per call, so `absent / (absent + reported)` is an
/// exact share.
#[test]
fn cached_tokens_reported_and_absent_partition_every_report() {
    let m = metrics();
    m.record_cached_prompt_tokens("m", "interactive", "", Some(64));
    m.record_cached_prompt_tokens("m", "interactive", "", Some(0)); // a real zero still COUNTS
    m.record_cached_prompt_tokens("m", "interactive", "", None);
    m.record_cached_prompt_tokens("m", "interactive", "", None);
    // Skipped entirely: no model, so neither counter moves.
    m.record_cached_prompt_tokens("", "interactive", "", Some(5));

    let out = m.render();
    assert!(
        has_line(&out, "kinference_cached_tokens_reported_total 2"),
        "{out}"
    );
    assert!(
        has_line(&out, "kinference_cached_tokens_absent_total 2"),
        "{out}"
    );
    // A reported zero adds nothing to the token sum, hence the separate count.
    assert!(
        has_line(
            &out,
            "kinference_cached_prompt_tokens_total{model=\"m\",qos=\"interactive\",tenant=\"default\"} 64"
        ),
        "{out}"
    );
}

/// Orphaned reports get their own counter rather than only being visible as a
/// difference between two others.
#[test]
fn orphaned_reports_are_counted_directly() {
    let m = metrics();
    let out = m.render();
    assert!(
        has_line(&out, "kinference_orphaned_reports_total 0"),
        "unlabelled, so it exists from startup:\n{out}"
    );
    m.record_orphaned_report();
    m.record_orphaned_report();
    let out = m.render();
    assert!(
        has_line(&out, "kinference_orphaned_reports_total 2"),
        "{out}"
    );
}

/// An orphan is labelled `unknown`, not folded into `interactive`.
#[test]
fn unknown_qos_is_its_own_label_value() {
    use crate::observations::Qos;
    assert_eq!(Qos::Unknown.as_label(), "unknown");
    // The four are distinct: nothing silently aliases onto another class.
    let labels =
        [Qos::Interactive, Qos::Batch, Qos::Deferrable, Qos::Unknown].map(|q| q.as_label());
    let uniq: std::collections::HashSet<&str> = labels.into_iter().collect();
    assert_eq!(uniq.len(), 4, "qos labels must be distinct: {labels:?}");

    // And it renders as a real series, not a dropped one.
    let m = metrics();
    m.record_usage("m", Qos::Unknown.as_label(), "", 10, 5);
    let out = m.render();
    assert!(
        has_line(
            &out,
            "kinference_prompt_tokens_total{model=\"m\",qos=\"unknown\",tenant=\"default\"} 10"
        ),
        "{out}"
    );
}

/// The PICK path must keep mapping an unrecognized wire value to `Interactive`.
///
/// Policy matches on QoS, so declassifying there would change routing.
#[test]
fn from_proto_never_yields_unknown() {
    use crate::observations::Qos;
    for wire in [-1i32, 0, 1, 2, 3, 99] {
        assert_ne!(
            Qos::from_proto(wire).as_label(),
            "unknown",
            "wire value {wire} must not declassify a pick"
        );
    }
}

/// The prediction histogram is labelled per worker and shares `kinference_ttft_ms`'s
/// bucket edges.
///
/// Shared edges, so predicted and observed distributions are comparable.
#[test]
fn predicted_ttft_is_per_worker_and_shares_the_observed_buckets() {
    let p = metrics();
    p.record_predicted_ttft("10.0.0.1:8000", "gemma-4", 120.0);
    p.record_predicted_ttft("10.0.0.1:8000", "gemma-4", 3000.0);
    p.record_predicted_ttft("10.0.0.2:8000", "gemma-4", 30.0);
    let out = p.render();

    // Both workers get their own series.
    assert!(
        has_line(
            &out,
            "kinference_predicted_ttft_ms_count{model=\"gemma-4\",worker=\"10.0.0.1:8000\"} 2"
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            "kinference_predicted_ttft_ms_count{model=\"gemma-4\",worker=\"10.0.0.2:8000\"} 1"
        ),
        "{out}"
    );

    // Both families are populated first, since a histogram with no
    // observations emits no buckets and the comparison would pass vacuously.
    p.record_ttft("gemma-4", "interactive", "acme", 120);
    let out = p.render();
    let edges = |metric: &str| -> Vec<String> {
        out.lines()
            .filter(|l| l.starts_with(&format!("{metric}_bucket{{")))
            .filter_map(|l| {
                let i = l.find("le=\"")? + 4;
                let j = l[i..].find('"')? + i;
                Some(l[i..j].to_string())
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let predicted = edges("kinference_predicted_ttft_ms");
    let observed = edges("kinference_ttft_ms");
    assert!(
        !predicted.is_empty() && !observed.is_empty(),
        "both families must have observations, else this compares empty sets"
    );
    assert_eq!(
        predicted, observed,
        "predicted and observed TTFT must share bucket edges to be comparable"
    );
}
