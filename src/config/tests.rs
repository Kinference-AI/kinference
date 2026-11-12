use serde_json::{Value, json};

use super::*;
use kinference_fleet::profile::{PickerConfig, PredictorConfig, TokenizeMode};
use kinference_router::metrics::TenantLabels;

fn parse(v: Value) -> serde_json::Result<Config> {
    serde_json::from_value(v)
}

fn workerset(name: &str, profile: &str, kv_block_size: u32, service: &str) -> Value {
    json!({
        "name": name,
        "profile": profile,
        "kv_block_size": kv_block_size,
        "discovery": {"service_name": service},
    })
}

#[test]
fn config_without_worker_sets_fails_validate() {
    let cfg =
        parse(json!({"listen": "127.0.0.1:50051"})).expect("parses (pools defaults to empty)");
    let err = cfg
        .validate()
        .expect_err("empty worker_sets should fail validate");
    assert!(err.to_string().contains("at least one"));
}

#[test]
fn workerset_with_zero_block_size_fails_validate() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "t"}],
        "workerset": [workerset("p", "t", 0, "svc")],
    }))
    .expect("parses");
    let err = cfg
        .validate()
        .expect_err("zero kv_block_size should fail validate");
    assert!(err.to_string().contains("kv_block_size"));
}

#[test]
fn duplicate_workerset_names_fail_validate() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "t"}],
        "workerset": [workerset("dup", "t", 16, "a"), workerset("dup", "t", 16, "b")],
    }))
    .expect("parses");
    let err = cfg
        .validate()
        .expect_err("duplicate workerset names should fail");
    assert!(err.to_string().contains("duplicate workerset name"));
}

#[test]
fn workerset_without_service_name_fails_validate() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "t"}],
        "workerset": [{"name": "p", "profile": "t", "kv_block_size": 16}],
    }))
    .expect("parses");
    let err = cfg
        .validate()
        .expect_err("workerset without service_name should fail");
    assert!(err.to_string().contains("service_name"));
}

/// Build a minimal valid config with the given `leader_election` section.
fn cfg_with_leader_election(section: Value) -> Config {
    parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "t"}],
        "workerset": [workerset("p", "t", 16, "svc")],
        "leader_election": section,
    }))
    .expect("parses")
}

/// `renew_grace >= lease_duration` makes the holder permanently due for
/// renewal, so the elector never sleeps and rewrites the Lease as fast as
/// the apiserver will accept it. Caught at load.
#[test]
fn renew_grace_at_or_above_lease_duration_is_rejected() {
    for grace in [15, 16, 100] {
        let cfg = cfg_with_leader_election(json!({
            "enabled": true,
            "lease_duration_secs": 15,
            "renew_grace_secs": grace,
        }));
        let err = cfg
            .validate()
            .expect_err("grace >= duration must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("renew_grace_secs"), "got: {msg}");
        assert!(
            msg.contains("lease_duration_secs"),
            "the message must name both values so the fix is obvious: {msg}"
        );
    }
    // The shipped default pair is valid.
    cfg_with_leader_election(json!({"enabled": true}))
        .validate()
        .expect("10 < 15 is fine");
}

#[test]
fn zero_and_empty_leader_election_settings_are_rejected() {
    for section in [
        json!({"enabled": true, "lease_duration_secs": 0}),
        json!({"enabled": true, "renew_grace_secs": 0}),
        json!({"enabled": true, "retry_secs": 0}),
        json!({"enabled": true, "lease_name": "  "}),
    ] {
        cfg_with_leader_election(section).validate().unwrap_err();
    }
}

/// While disabled (the default), the timings are not validated.
#[test]
fn leader_election_defaults_off_and_is_not_validated_when_off() {
    let cfg = cfg_with_leader_election(json!({}));
    assert!(!cfg.leader_election.enabled);
    assert_eq!(cfg.leader_election.lease_name, "kinference");
    assert_eq!(cfg.leader_election.lease_duration_secs, 15);
    assert_eq!(cfg.leader_election.renew_grace_secs, 10);

    cfg_with_leader_election(json!({"lease_duration_secs": 1, "renew_grace_secs": 99}))
        .validate()
        .expect("timings are inert while disabled");
}

#[test]
fn a_profile_that_omits_everything_gets_the_struct_defaults() {
    // A profile with no predictor / picker sections still resolves;
    // expected_overlap_alpha defaults to 1.0.
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "t"}],
        "workerset": [workerset("p", "t", 64, "svc")],
    }))
    .expect("parses");
    cfg.validate().expect("validates");
    let spec = &cfg.pool_specs().expect("resolves")[0];
    assert_eq!(spec.profile, "t");
    assert_eq!(spec.predictor.expected_overlap_alpha, 1.0);
    assert_eq!(spec.predictor.base_ms, PredictorConfig::default().base_ms);
    assert_eq!(spec.picker.top_k_min, PickerConfig::default().top_k_min);
    assert_eq!(spec.tokenize_mode, TokenizeMode::default());
}

/// An undefined profile error names both the worker set and the profile,
/// and lists the defined profiles.
#[test]
fn an_undefined_profile_is_rejected_and_names_both_sides() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "h100"}, {"name": "a100"}],
        "workerset": [workerset("ws", "typo-h100", 16, "svc")],
    }))
    .expect("parses");
    let err = cfg.validate().expect_err("unknown profile must fail");
    let msg = err.to_string();
    assert!(msg.contains("ws"), "names the worker set: {msg}");
    assert!(
        msg.contains("typo-h100"),
        "names the missing profile: {msg}"
    );
    assert!(
        msg.contains("a100") && msg.contains("h100"),
        "lists the defined profiles so the typo is obvious: {msg}"
    );
}

/// Two sets sharing a profile get identical tuning.
#[test]
fn two_worker_sets_sharing_a_profile_get_identical_tuning() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{
            "name": "shared",
            "predictor": {"base_ms": 777.0, "expected_overlap_alpha": 0.8},
            "picker": {"top_k_min": 3},
            "tokenize": {"mode": "local"},
        }],
        "workerset": [
            workerset("a", "shared", 16, "svc-a"),
            workerset("b", "shared", 64, "svc-b"),
        ],
    }))
    .expect("parses");
    cfg.validate().expect("validates");
    let specs = cfg.pool_specs().expect("resolves");
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].predictor.base_ms, 777.0);
    assert_eq!(specs[0].predictor.expected_overlap_alpha, 0.8);
    assert_eq!(specs[0].picker.top_k_min, 3);
    assert_eq!(specs[0].tokenize_mode, TokenizeMode::Local);
    assert_eq!(specs[0].predictor, specs[1].predictor);
    assert_eq!(specs[0].picker, specs[1].picker);
    // ...but block size stays per set, since it depends on the engines.
    assert_eq!(specs[0].kv_block_size, 16);
    assert_eq!(specs[1].kv_block_size, 64);
}

#[test]
fn duplicate_profile_names_fail_validate() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "dup"}, {"name": "dup"}],
        "workerset": [workerset("ws", "dup", 16, "svc")],
    }))
    .expect("parses");
    let err = cfg
        .validate()
        .expect_err("duplicate profile names must fail");
    assert!(err.to_string().contains("duplicate profile name"));
}

/// `profile` has no default: a worker set that omits it fails to parse.
#[test]
fn a_workerset_without_a_profile_does_not_parse() {
    let err = parse(json!({
        "listen": "127.0.0.1:50051",
        "workerset": [{"name": "ws", "kv_block_size": 16, "discovery": {"service_name": "svc"}}],
    }))
    .expect_err("missing profile must not parse");
    assert!(err.to_string().contains("profile"), "got: {err}");
}

/// `metrics.tenant_labels` accepts its three forms, and an absent section
/// means `none`.
#[test]
fn tenant_labels_accepts_its_three_forms() {
    let labels = |metrics: Option<Value>| -> TenantLabels {
        let mut v = json!({"listen": "127.0.0.1:50051"});
        if let Some(m) = metrics {
            v["metrics"] = m;
        }
        parse(v).expect("parses").metrics.tenant_labels
    };
    assert_eq!(
        labels(None),
        TenantLabels::None,
        "absent section means none"
    );
    assert_eq!(
        labels(Some(json!({}))),
        TenantLabels::None,
        "absent key too"
    );
    assert_eq!(
        labels(Some(json!({"tenant_labels": "none"}))),
        TenantLabels::None
    );
    assert_eq!(
        labels(Some(json!({"tenant_labels": "all"}))),
        TenantLabels::All
    );
    assert_eq!(
        labels(Some(json!({"tenant_labels": ["acme", "globex"]}))),
        TenantLabels::Allow(vec!["acme".into(), "globex".into()])
    );
    // An empty list is how a templating tool spells "none".
    assert_eq!(
        labels(Some(json!({"tenant_labels": []}))),
        TenantLabels::None
    );
}

/// A mistyped value fails the parse, with an error naming the accepted
/// forms, rather than falling back to the default.
#[test]
fn a_mistyped_tenant_labels_value_is_rejected_rather_than_defaulted() {
    let err = parse(json!({
        "listen": "127.0.0.1:50051",
        "metrics": {"tenant_labels": "sure"},
    }))
    .expect_err("a bogus tenant_labels value must not parse");
    let msg = err.to_string();
    assert!(
        msg.contains("none") && msg.contains("all"),
        "the error must name the accepted forms, got: {msg}"
    );
    assert!(
        msg.contains("sure"),
        "the error must quote what was actually written, got: {msg}"
    );
}

/// The body caps are configured in KB and parse as written.
///
/// The default is the KB figure, and the old `_bytes` keys are rejected by
/// `deny_unknown_fields`, so a byte value can't be read as KB.
#[test]
fn body_caps_are_configured_in_kb() {
    let cfg = parse(json!({
        "listen": "127.0.0.1:50051",
        "body_max_total_kb": 1024,
        "body_max_request_kb": 64,
    }))
    .expect("parses");
    assert_eq!(cfg.body_max_total_kb, Some(1024));
    assert_eq!(cfg.body_max_request_kb, Some(64));

    // The old spelling is a startup error.
    let err = parse(json!({
        "listen": "127.0.0.1:50051",
        "body_max_total_bytes": 268435456,
    }))
    .expect_err("the old key must not parse");
    let msg = err.to_string();
    assert!(
        msg.contains("body_max_total_bytes"),
        "the error must name the offending key, got: {msg}"
    );
    assert!(
        msg.contains("body_max_total_kb"),
        "and list the accepted ones, got: {msg}"
    );
}

/// Helm's `toJson` writes integral numbers without a fraction (`400`, not
/// `400.0`); float fields accept them.
#[test]
fn integral_numbers_load_into_float_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.json");
    std::fs::write(
        &path,
        r#"{
  "listen": "127.0.0.1:50051",
  "metrics_poll_ms": 1000,
  "profile": [{"name": "t", "predictor": {"base_ms": 400}}],
  "kubernetes": {"enabled": true}
}"#,
    )
    .expect("write json");
    let cfg = Config::load(&path).expect("loads");
    assert_eq!(cfg.metrics_poll_ms, Some(1000));
    assert_eq!(cfg.profiles[0].predictor.base_ms, 400.0);
}

/// A mistyped top-level key is a startup error, not a silent default.
#[test]
fn unknown_top_level_key_fails_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.json");
    std::fs::write(
        &path,
        r#"{"listen": "127.0.0.1:50051", "dashboard_listen": "0.0.0.0:9001"}"#,
    )
    .expect("write json");
    let err = Config::load(&path).expect_err("unknown key must not load");
    assert!(
        format!("{err:#}").contains("dashboard_listen"),
        "the error must name the offending key, got: {err:#}"
    );
}

fn minimal() -> Config {
    parse(json!({
        "listen": "127.0.0.1:50051",
        "profile": [{"name": "t"}],
        "workerset": [workerset("p", "t", 16, "svc")],
    }))
    .expect("parses")
}

#[test]
fn invalid_kubernetes_selector_fails_at_config_load() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    std::fs::write(
        &path,
        json!({
            "listen": "127.0.0.1:50051",
            "profile": [{"name": "t"}],
            "kubernetes": {"enabled": true, "selector": "app in ("}
        })
        .to_string(),
    )
    .unwrap();
    let error = Config::load(path).unwrap_err();
    assert!(error.to_string().contains("invalid kubernetes.selector"));
}

#[test]
fn kubernetes_selectors_accept_equality_sets_and_empty_values() {
    for selector in ["", "app=router", "app=", "app in (router,worker),!retired"] {
        let config = parse(json!({
            "listen": "127.0.0.1:50051",
            "profile": [{"name": "t"}],
            "kubernetes": {"enabled": true, "selector": selector}
        }))
        .unwrap();
        config.validate().unwrap();
    }
}

#[test]
fn disabled_kubernetes_ignores_its_unused_selector() {
    let mut config = minimal();
    config.kubernetes.selector = Some("app in (".into());
    config.validate().unwrap();
}

/// The router's defaults: 256 MiB and 8 MiB in bytes, 60 s, 30 s, 600 s.
#[test]
fn router_settings_default() {
    let router = minimal().router();
    assert_eq!(router.bodies.max_total_bytes, 256 * 1024 * 1024);
    assert_eq!(router.bodies.max_request_bytes, 8 * 1024 * 1024);
    assert_eq!(router.bodies.ttl, Duration::from_secs(60));
    assert_eq!(router.bodies.idle_timeout, Duration::from_secs(30));
    assert_eq!(router.observation_ttl, Duration::from_secs(600));
}

/// An absurd KB value clamps at `u64::MAX` instead of wrapping to a tiny cap.
#[test]
fn body_limits_saturate_instead_of_wrapping() {
    let mut cfg = minimal();
    cfg.body_max_total_kb = Some(u64::MAX);
    cfg.body_max_request_kb = Some(u64::MAX / 2);
    let bodies = cfg.router().bodies;
    assert_eq!(bodies.max_total_bytes, u64::MAX);
    assert_eq!(bodies.max_request_bytes, u64::MAX);
}

/// The fleet's defaults: one spec per workerset, 1000 ms, 30 s, a 30 s
/// linger and a 600 s backstop.
#[test]
fn fleet_settings_default() {
    let fleet = minimal().fleet().expect("resolves");
    assert_eq!(fleet.specs.len(), 1);
    assert_eq!(fleet.specs[0].name, "p");
    assert_eq!(fleet.workers.poll_ms, 1000);
    assert_eq!(fleet.workers.models_refresh_secs, 30);
    assert_eq!(fleet.workers.observation_ttl, Duration::from_secs(600));
    assert_eq!(fleet.reap.linger, Duration::from_secs(30));
    assert_eq!(fleet.reap.backstop, Duration::from_secs(600));
}

/// A retired pool must outlive observations, so a longer TTL raises the
/// reap backstop with it.
#[test]
fn reap_backstop_follows_a_longer_observation_ttl() {
    let mut cfg = minimal();
    cfg.observation_ttl_secs = Some(900);
    let fleet = cfg.fleet().expect("resolves");
    assert_eq!(fleet.workers.observation_ttl, Duration::from_secs(900));
    assert_eq!(fleet.reap.backstop, Duration::from_secs(900));
    assert_eq!(cfg.router().observation_ttl, Duration::from_secs(900));
}
