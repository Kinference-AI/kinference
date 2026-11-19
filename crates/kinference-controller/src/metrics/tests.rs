use super::*;

use prometheus::{Encoder, TextEncoder};

fn metrics() -> (ControllerMetrics, Registry) {
    let registry = Registry::new();
    let m = ControllerMetrics::new(&registry);
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

/// Every rejection reason exists at zero on a registry that has seen no
/// traffic.
#[test]
fn rejection_reasons_are_seeded_at_zero() {
    let (_, registry) = metrics();
    let out = render(&registry);
    for v in WORKERSET_REJECT_REASONS {
        let line = format!("kinference_workerset_rejected_total{{reason=\"{v}\"}} 0");
        assert!(has_line(&out, &line), "missing `{line}`:\n{out}");
    }
}

/// The seeded reasons are the condition reasons the policy crate emits.
#[test]
fn rejection_reason_labels_match_the_enum() {
    use kinference_policy::rejection::RejectReason as R;
    let all = [
        R::MissingProfile,
        R::UnknownProfile { defined: vec![] },
        R::MissingServiceRef,
        R::ModelMissingName,
        R::KvEventsMissingPort,
    ];
    let from_enum: Vec<&str> = all.iter().map(|r| r.as_condition_reason()).collect();
    assert_eq!(from_enum, WORKERSET_REJECT_REASONS);
}

/// A follower's leader gauge is present at 0, not absent, and the transition
/// counter moves only on a change.
#[test]
fn leader_gauge_distinguishes_follower_from_absent() {
    let (m, registry) = metrics();
    m.set_leader(false);
    let out = render(&registry);
    assert!(
        has_line(&out, "kinference_leader 0"),
        "gauge must be present at 0:\n{out}"
    );
    assert!(
        has_line(&out, "kinference_leader_transitions_total 0"),
        "{out}"
    );
    m.set_leader(true);
    m.set_leader(true);
    let out = render(&registry);
    assert!(has_line(&out, "kinference_leader 1"), "{out}");
    assert!(
        has_line(&out, "kinference_leader_transitions_total 1"),
        "{out}"
    );
}

/// A new pass replaces the previous rows, so fixed policies drop out.
#[test]
fn policy_warnings_are_replaced_per_pass() {
    let (m, registry) = metrics();
    m.set_policy_warnings(&[("infra/routing".into(), "UnknownBackend", 2)]);
    let out = render(&registry);
    assert!(
        has_line(
            &out,
            "kinference_policy_warnings{code=\"UnknownBackend\",policy=\"infra/routing\"} 2"
        ),
        "{out}"
    );
    m.set_policy_warnings(&[("infra/other".into(), "EmptyRule", 1)]);
    let out = render(&registry);
    assert!(!out.contains("infra/routing"), "{out}");
    assert!(
        has_line(
            &out,
            "kinference_policy_warnings{code=\"EmptyRule\",policy=\"infra/other\"} 1"
        ),
        "{out}"
    );
}
