use super::*;

use prometheus::{Encoder, TextEncoder};

fn metrics() -> (FleetMetrics, Registry) {
    let registry = Registry::new();
    let m = FleetMetrics::new(&registry);
    (m, registry)
}

fn render(registry: &Registry) -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut buf)
        .unwrap();
    String::from_utf8(buf).unwrap()
}

/// Whole-line match, so `... 0` doesn't match `0.5`.
fn has_line(out: &str, line: &str) -> bool {
    out.lines().any(|l| l == line)
}

/// Every closed-set child exists at zero on a registry that has seen no
/// traffic.
#[test]
fn closed_label_sets_are_seeded_at_zero() {
    let (_, registry) = metrics();
    let out = render(&registry);
    for kind in INDEX_EVENT_KINDS {
        let line = format!("kinference_index_events_total{{kind=\"{kind}\"}} 0");
        assert!(has_line(&out, &line), "missing `{line}`:\n{out}");
    }
    for reason in EVICTION_REASONS {
        let line = format!("kinference_worker_evictions_total{{reason=\"{reason}\"}} 0");
        assert!(has_line(&out, &line), "missing `{line}`:\n{out}");
    }
}

/// Evictions count by reason.
#[test]
fn worker_evictions_are_counted_by_reason() {
    let (m, registry) = metrics();
    m.record_worker_eviction("model_mismatch");
    m.record_worker_eviction("model_mismatch");
    let out = render(&registry);
    assert!(
        has_line(
            &out,
            "kinference_worker_evictions_total{reason=\"model_mismatch\"} 2"
        ),
        "{out}"
    );
}

/// Skipped KV events count by worker set and reason.
#[test]
fn skipped_kv_events_are_counted_by_workerset_and_reason() {
    let (m, registry) = metrics();
    m.record_kv_event_skipped("set-a", "block_size_mismatch");
    m.record_kv_event_skipped("set-a", "block_size_mismatch");
    m.record_kv_event_skipped("set-b", "unknown_kind");
    let out = render(&registry);
    for line in [
        "kinference_kv_events_skipped_total{reason=\"block_size_mismatch\",workerset=\"set-a\"} 2",
        "kinference_kv_events_skipped_total{reason=\"unknown_kind\",workerset=\"set-b\"} 1",
    ] {
        assert!(has_line(&out, line), "missing `{line}`:\n{out}");
    }
}

/// Each event kind counts once; only stored and removed move the block totals.
#[test]
fn index_events_count_by_kind_and_blocks_by_direction() {
    let (m, registry) = metrics();
    m.record_index_event(IngestEvent::Stored, 4);
    m.record_index_event(IngestEvent::Stored, 2);
    m.record_index_event(IngestEvent::Removed, 3);
    m.record_index_event(IngestEvent::Cleared, 99);
    let out = render(&registry);
    for line in [
        "kinference_index_events_total{kind=\"stored\"} 2",
        "kinference_index_events_total{kind=\"removed\"} 1",
        "kinference_index_events_total{kind=\"cleared\"} 1",
        "kinference_index_blocks_stored_total 6",
        "kinference_index_blocks_removed_total 3",
    ] {
        assert!(has_line(&out, line), "missing `{line}`:\n{out}");
    }
}

/// The label list matches the enum, and doesn't carry the mismatched values,
/// which are unbounded.
#[test]
fn eviction_reason_labels_match_the_enum() {
    use crate::eviction::EvictionReason as E;
    let all = [E::ModelMismatch {
        expected: vec!["a".into()],
        observed: vec!["b".into()],
    }];
    for v in &all {
        match v {
            E::ModelMismatch { .. } => {}
        }
    }
    let from_enum: Vec<&str> = all.iter().map(|r| r.as_label()).collect();
    assert_eq!(from_enum, EVICTION_REASONS);
    // Display names the models; the label must not.
    let d = all[0].to_string();
    assert!(
        d.contains("\"a\"") && d.contains("\"b\""),
        "Display should name both: {d}"
    );
    assert_eq!(all[0].as_label(), "model_mismatch");
}

/// Ties [`INDEX_EVENT_KINDS`] to its enum via an exhaustive `match`.
#[test]
fn index_event_labels_match_the_enum() {
    let all = [
        IngestEvent::Stored,
        IngestEvent::Removed,
        IngestEvent::Cleared,
    ];
    for v in all {
        match v {
            IngestEvent::Stored | IngestEvent::Removed | IngestEvent::Cleared => {}
        }
    }
    let from_enum: Vec<&str> = all.iter().map(|k| k.as_label()).collect();
    assert_eq!(from_enum, INDEX_EVENT_KINDS);
}
