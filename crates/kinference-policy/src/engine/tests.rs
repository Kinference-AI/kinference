use super::*;
use crate::crd::InferencePolicySpec;
use serde_json::json;
use std::collections::BTreeMap;

fn policy(v: serde_json::Value) -> CompiledPolicy {
    let spec: InferencePolicySpec = serde_json::from_value(v).expect("valid fixture");
    CompiledPolicy::compile("p", &spec)
}

fn cand(ws: &str, model: &str, ttft: f32) -> Candidate {
    Candidate {
        canonical_model: model.to_string(),
        upstream_model: model.to_string(),
        backend: ws.to_string(),
        labels: Default::default(),
        target: CandidateTarget::Local {
            pool_id: PoolId::from_raw(0),
            endpoint: format!("{ws}-ep:8000"),
            score_idx: 0,
            predicted_ttft_ms: ttft,
        },
        attrs: CandidateAttrs {
            model_names: vec![model.to_string()],
            backend_name: Some(ws.to_string()),
            backend_type: Some("Local".to_string()),
            predicted_ttft_ms: Some(ttft as f64),
            ..Default::default()
        },
    }
}

/// An off-cluster candidate. `observed` is `None` for a provider nothing has
/// been reported about yet, which is the state that has to stay eligible.
fn prov(name: &str, model: &str, observed: Option<f32>) -> Candidate {
    Candidate {
        canonical_model: model.to_string(),
        upstream_model: format!("vendor-{model}"),
        backend: name.to_string(),
        labels: Default::default(),
        target: CandidateTarget::Provider {
            provider: ObjectKey::new("infra", name),
            backend_ref: ObjectKey::new("infra", format!("{name}-backend")),
            path: None,
            observed_ttft_ms: observed,
        },
        attrs: CandidateAttrs {
            model_names: vec![model.to_string()],
            backend_name: Some(name.to_string()),
            backend_type: Some("Provider".to_string()),
            backend_observed_ttft_ms: observed.map(|ms| ms as f64),
            ..Default::default()
        },
    }
}

fn req() -> RequestAttrs {
    RequestAttrs::default()
}

fn route_of(p: &CompiledPolicy) -> &CompiledRoute {
    &p.rules[0].route
}

// --- rule selection ---------------------------------------------------

#[test]
fn matching_rules_preserves_list_order() {
    let p = policy(json!({"rules": [
        {"name": "a", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}},
        {"name": "b", "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    let attrs = RequestAttrs {
        qos: Some("batch".into()),
        ..Default::default()
    };
    let names: Vec<&str> = matching_rules(&p, &attrs)
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(names, vec!["a", "b"], "both match; order is the policy's");
}

#[test]
fn non_matching_rules_are_skipped() {
    let p = policy(json!({"rules": [
        {"name": "batch-only", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    assert!(matching_rules(&p, &req()).is_empty());
}

#[test]
fn possibly_matching_rules_take_input_tokens_terms_as_true() {
    let p = policy(json!({"rules": [
        {"name": "long", "matches": [{"inputTokens": {"greaterThan": 32000}}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}},
        {"name": "long-batch",
         "matches": [{"inputTokens": {"greaterThan": 32000}, "qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}},
        {"name": "rest", "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    let names = |rules: Vec<&CompiledRule>| -> Vec<String> {
        rules.iter().map(|r| r.name.clone()).collect()
    };
    // No count, interactive: `long-batch` fails on `qos` whatever the count.
    let attrs = RequestAttrs {
        qos: Some("interactive".into()),
        ..Default::default()
    };
    assert_eq!(
        names(possibly_matching_rules(&p, &attrs)),
        vec!["long", "rest"]
    );
    assert_eq!(names(matching_rules(&p, &attrs)), vec!["rest"]);
}

#[test]
fn uses_input_tokens_sees_matches_but_not_guards() {
    let route = json!({"select": [{"minimize": "PredictedTTFT"}]});
    let in_matches = policy(json!({"rules": [{"name": "r",
        "matches": [{"inputTokens": {"greaterThan": 10}}], "route": route}]}));
    assert!(in_matches.uses_input_tokens());

    let in_guard = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "x", "if": {"inputTokens": {"greaterThan": 10}},
                     "drop": {"backend": {"name": "a100"}}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert!(
        !in_guard.uses_input_tokens(),
        "a guard reads each candidate's own count"
    );

    let neither = policy(json!({"rules": [{"name": "r",
        "matches": [{"qos": ["batch"]}], "route": route}]}));
    assert!(!neither.uses_input_tokens());
    assert!(!CompiledPolicy::serve_as_asked("p").uses_input_tokens());
}

// --- consider ---------------------------------------------------------

#[test]
fn default_consider_is_the_requested_model_everywhere() {
    let p = policy(json!({"rules": [
        {"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    let targets = models_to_score(route_of(&p), "gemma");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].model, "gemma");
    assert!(
        targets[0]
            .backends
            .matches(BackendType::Local, "anything", &BTreeMap::new()),
        "no subset = every destination the pool allows"
    );
}

#[test]
fn named_models_add_one_the_client_did_not_ask_for() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "consider": [{"models": {"names": ["foo", "bar"]}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let targets = models_to_score(route_of(&p), "gemma");
    let models: Vec<&str> = targets.iter().map(|t| t.model.as_str()).collect();
    assert_eq!(
        models,
        vec!["foo", "bar"],
        "the request's own model is not implied"
    );
}

#[test]
fn unweighted_groups_union() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "consider": [
            {"models": {"names": ["foo"]}},
            {"models": {"names": ["bar"]}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert_eq!(models_to_score(route_of(&p), "x").len(), 2);
}

#[test]
fn weighted_groups_sample_exactly_one() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "consider": [
            {"name": "h", "models": {"names": ["bar"]}, "backends": {"names": ["h100"]}, "weight": 70},
            {"name": "a", "models": {"names": ["bar"]}, "backends": {"names": ["a100"]}, "weight": 30}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let route = route_of(&p);
    assert!(route.weighted());
    for _ in 0..20 {
        let t = models_to_score(route, "bar");
        assert_eq!(t.len(), 1, "a partition picks one group, not the union");
    }
}

#[test]
fn weighted_sampling_hits_both_groups_over_many_draws() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "consider": [
            {"models": {"names": ["a"]}, "weight": 50},
            {"models": {"names": ["b"]}, "weight": 50}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let route = route_of(&p);
    let mut seen = std::collections::HashSet::new();
    for _ in 0..200 {
        seen.insert(models_to_score(route, "x")[0].model.clone());
    }
    assert_eq!(seen.len(), 2, "50/50 must reach both groups: {seen:?}");
}

#[test]
fn duplicate_targets_are_deduped() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "consider": [
            {"models": {"names": ["bar"]}},
            {"models": {"names": ["bar"]}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert_eq!(
        models_to_score(route_of(&p), "x").len(),
        1,
        "don't score twice"
    );
}

/// A Local group and a Provider group for one model are two subsets, not one
/// twice.
#[test]
fn groups_differing_only_by_class_are_both_scored() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "consider": [{"type": "Local"}, {"type": "Provider"}],
        "select": [{"minimize": "EstimatedTTFT"}]}}]}));
    let classes: Vec<_> = models_to_score(route_of(&p), "bar")
        .iter()
        .map(|t| t.backends.backend_type)
        .collect();
    assert_eq!(
        classes,
        [Some(BackendType::Local), Some(BackendType::Provider)]
    );
}

/// A `select` tier that subsets by label sees the destination's labels.
#[test]
fn a_select_tier_can_subset_by_label() {
    let p = policy(json!({"rules": [{"name": "r", "route": {"select": [
        {"backends": {"selector": {"matchLabels": {"accelerator": "h100"}}},
         "minimize": "PredictedTTFT"},
        {"minimize": "PredictedTTFT"}
    ]}}]}));
    let labelled = |ws: &str, ttft: f32, accel: &str| {
        let mut c = cand(ws, "bar", ttft);
        c.labels = Arc::new(BTreeMap::from([(
            "accelerator".to_string(),
            accel.to_string(),
        )]));
        c
    };
    // The a100 is faster; the label tier must still pick the h100.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![
            labelled("fast-a100", 10.0, "a100"),
            labelled("slow-h100", 90.0, "h100"),
        ],
    );
    let Outcome::Chosen { candidate, trace } = out else {
        panic!("expected a pick");
    };
    assert_eq!(candidate.backend, "slow-h100");
    assert_eq!(trace.tier, Some(0), "decided by the label tier");
}

// --- context-window fit (built in, before exclusions) -----------------

#[test]
fn context_window_fit_runs_before_exclusions() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let mut small = cand("h100", "bar", 100.0);
    small.attrs.context_window = Some(4096);
    small.attrs.input_tokens = Some(64000);
    let mut big = cand("a100", "bar", 500.0);
    big.attrs.context_window = Some(131072);
    big.attrs.input_tokens = Some(64000);
    let out = evaluate(route_of(&p), Some("r"), &req(), vec![small, big]);
    match out {
        Outcome::Chosen { candidate, trace } => {
            assert_eq!(candidate.backend, "a100", "the 4k endpoint cannot fit 64k");
            assert!(trace.eliminated[0].1.contains("context-window fit"));
        }
        other => panic!("expected a choice, got {other:?}"),
    }
}

#[test]
fn context_fit_accounts_for_max_tokens() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let mut c = cand("h100", "bar", 100.0);
    c.attrs.context_window = Some(8192);
    // 8000 prompt fits, but not with 1000 more tokens of completion.
    c.attrs.input_tokens = Some(8000);
    let attrs = RequestAttrs {
        max_tokens: Some(1000),
        ..Default::default()
    };
    assert!(matches!(
        evaluate(route_of(&p), Some("r"), &attrs, vec![c]),
        Outcome::Empty { .. }
    ));
}

#[test]
fn unknown_context_window_does_not_eliminate() {
    // A candidate that doesn't carry the signal isn't dropped by fitting.
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let mut c = cand("h100", "bar", 100.0); // context_window: None
    c.attrs.input_tokens = Some(999_999);
    assert!(matches!(
        evaluate(route_of(&p), Some("r"), &req(), vec![c]),
        Outcome::Chosen { .. }
    ));
}

#[test]
fn a_candidate_with_no_count_is_taken_to_fit() {
    // A failed render leaves no count; the request's own count is not used.
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let mut c = cand("h100", "bar", 100.0);
    c.attrs.context_window = Some(4096);
    let attrs = RequestAttrs {
        input_tokens: Some(64000),
        ..Default::default()
    };
    assert!(matches!(
        evaluate(route_of(&p), Some("r"), &attrs, vec![c]),
        Outcome::Chosen { .. }
    ));
}

// --- exclusions -------------------------------------------------------

#[test]
fn exclusion_drops_matching_candidates() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "no-a100", "drop": {"backend": {"name": "a100"}}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("a100", "bar", 10.0), cand("h100", "bar", 900.0)],
    );
    match out {
        Outcome::Chosen { candidate, trace } => {
            assert_eq!(candidate.backend, "h100", "excluded despite being faster");
            assert!(trace.eliminated[0].1.contains("no-a100"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn guarded_exclusion_sits_out_when_the_guard_misses() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "long-prompts",
                     "if": {"inputTokens": {"greaterThan": 32000}},
                     "drop": {"backend": {"name": "a100"}}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let with_count = |ws: &str, ttft: f32, n: i64| {
        let mut c = cand(ws, "bar", ttft);
        c.attrs.input_tokens = Some(n);
        c
    };
    // Short prompt: the guard misses, so a100 stays eligible and wins.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![
            with_count("a100", 10.0, 100),
            with_count("h100", 900.0, 100),
        ],
    );
    assert!(matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "a100"));

    // Long prompt: the guard fires and a100 is dropped.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![
            with_count("a100", 10.0, 64000),
            with_count("h100", 900.0, 64000),
        ],
    );
    assert!(matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"));
}

#[test]
fn a_guard_reads_each_candidates_own_count() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "long-prompts",
                     "if": {"inputTokens": {"greaterThan": 32000}},
                     "drop": {"backend": {"type": "Local"}}}],
        "select": [{"type": "Local", "minimize": "PredictedTTFT"}]}}]}));
    // One prompt, two tokenizers: only `long`'s rendering trips the guard.
    let mut short = cand("short", "a", 900.0);
    short.attrs.input_tokens = Some(20000);
    let mut long = cand("long", "b", 10.0);
    long.attrs.input_tokens = Some(40000);
    // The request's count is not consulted.
    let attrs = RequestAttrs {
        input_tokens: Some(1),
        ..Default::default()
    };
    let out = evaluate(route_of(&p), Some("r"), &attrs, vec![long, short]);
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "short"),
        "{out:?}"
    );
}

#[test]
fn a_guard_on_input_tokens_misses_a_candidate_with_no_count() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "long-to-cloud",
                     "if": {"inputTokens": {"greaterThan": 1000}},
                     "drop": {"backend": {"type": "Provider"}}}],
        "select": [{"order": ["cloud"]}]}}]}));
    let attrs = RequestAttrs {
        input_tokens: Some(5000),
        ..Default::default()
    };
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &attrs,
        vec![prov("cloud", "bar", Some(10.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "cloud"),
        "a provider has no count, so the guard misses: {out:?}"
    );
}

#[test]
fn two_guarded_exclusions_in_one_rule_compose() {
    // Two first-match-wins rules couldn't express this.
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [
            {"name": "eu", "if": {"tenant": "health-eu"}, "drop": {"backend": {"name": "us"}}},
            {"name": "long", "if": {"inputTokens": {"greaterThan": 1000}},
             "drop": {"backend": {"name": "small"}}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let attrs = RequestAttrs {
        tenant: "health-eu".into(),
        ..Default::default()
    };
    let candidates = [("us", 1.0), ("small", 2.0), ("eu-big", 3.0)]
        .into_iter()
        .map(|(ws, ttft)| {
            let mut c = cand(ws, "bar", ttft);
            c.attrs.input_tokens = Some(5000);
            c
        })
        .collect();
    let out = evaluate(route_of(&p), Some("r"), &attrs, candidates);
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "eu-big"),
        "both exclusions applied: {out:?}"
    );
}

#[test]
fn queued_prompts_exclusion_sheds_by_emptying_the_set() {
    // This sheds load rather than routing around it.
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "shed", "drop": {"queuedPrompts": {"greaterThan": 8}}}],
        "select": [{"minimize": "PredictedTTFT"}],
        "onEmpty": "Reject"}}]}));
    let mut busy = cand("h100", "bar", 100.0);
    busy.attrs.queued_prompts = Some(20.0);
    let out = evaluate(route_of(&p), Some("r"), &req(), vec![busy]);
    assert!(matches!(out, Outcome::Empty { .. }));
    assert!(!falls_through(route_of(&p)), "Reject is the default");
}

// --- select tiers -----------------------------------------------------

#[test]
fn minimize_picks_the_lowest_objective() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![
            cand("a", "m", 500.0),
            cand("b", "m", 120.0),
            cand("c", "m", 900.0),
        ],
    );
    match out {
        Outcome::Chosen { candidate, trace } => {
            assert_eq!(candidate.backend, "b");
            assert_eq!(trace.winner_ttft_ms, Some(120.0));
            assert_eq!(trace.tier, Some(0));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn first_tier_with_a_survivor_wins_outright() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [
            {"backends": {"names": ["h100"]}, "minimize": "PredictedTTFT"},
            {"backends": {"names": ["a100"]}, "minimize": "PredictedTTFT"}]}}]}));
    // h100 present but much slower; tier order beats the objective.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("a100", "m", 10.0), cand("h100", "m", 5000.0)],
    );
    assert!(matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"));
}

#[test]
fn later_tier_runs_only_when_the_earlier_one_is_empty() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "exclude": [{"name": "drop-h100", "drop": {"backend": {"name": "h100"}}}],
        "select": [
            {"backends": {"names": ["h100"]}, "minimize": "PredictedTTFT"},
            {"backends": {"names": ["a100"]}, "minimize": "PredictedTTFT"}]}}]}));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("h100", "m", 10.0), cand("a100", "m", 900.0)],
    );
    match out {
        Outcome::Chosen { candidate, trace } => {
            assert_eq!(candidate.backend, "a100");
            assert_eq!(trace.tier, Some(1), "fell through to the second tier");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn all_tiers_subsetted_away_is_empty() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"backends": {"names": ["nonexistent"]}, "minimize": "PredictedTTFT"}]}}]}));
    assert!(matches!(
        evaluate(
            route_of(&p),
            Some("r"),
            &req(),
            vec![cand("h100", "m", 1.0)]
        ),
        Outcome::Empty { .. }
    ));
}

#[test]
fn order_tier_prefers_earlier_worker_sets() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"order": ["h100", "a100"]}]}}]}));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("a100", "m", 1.0), cand("h100", "m", 9999.0)],
    );
    assert!(matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"));
}

#[test]
fn order_ranks_unlisted_sets_last_rather_than_dropping_them() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"order": ["h100"]}]}}]}));
    // Only an unlisted set exists: `order` is a preference, not a filter.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("other", "m", 1.0)],
    );
    assert!(matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "other"));
}

// --- tie break --------------------------------------------------------

#[test]
fn random_tie_break_reaches_every_tied_candidate() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}], "tieBreak": ["Random"]}}]}));
    let mut seen = std::collections::HashSet::new();
    for _ in 0..200 {
        let out = evaluate(
            route_of(&p),
            Some("r"),
            &req(),
            vec![
                cand("a", "m", 100.0),
                cand("b", "m", 100.0),
                cand("c", "m", 100.0),
            ],
        );
        if let Outcome::Chosen { candidate, trace } = out {
            assert_eq!(trace.tied, 3);
            seen.insert(candidate.backend);
        }
    }
    assert_eq!(seen.len(), 3, "Random must spread across ties: {seen:?}");
}

#[test]
fn without_random_ties_are_deterministic() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let first = |_| match evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("a", "m", 100.0), cand("b", "m", 100.0)],
    ) {
        Outcome::Chosen { candidate, .. } => candidate.backend,
        other => panic!("{other:?}"),
    };
    let picks: std::collections::HashSet<String> = (0..20).map(first).collect();
    assert_eq!(picks.len(), 1, "no tieBreak → stable choice");
}

// --- onEmpty / unmatched ---------------------------------------------

#[test]
fn fall_through_is_opt_in() {
    let reject = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert!(!falls_through(route_of(&reject)));
    let ft = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}], "onEmpty": "FallThrough"}}]}));
    assert!(falls_through(route_of(&ft)));
}

#[test]
fn unmatched_serve_local_yields_the_default_route() {
    let p = policy(json!({"unmatched": "ServeLocal", "rules": []}));
    let route = unmatched_route(&p).expect("ServeLocal serves");
    assert!(
        route.exclude.is_empty(),
        "no exclusions, not even a queue threshold"
    );
}

#[test]
fn unmatched_reject_refuses() {
    let p = policy(json!({"unmatched": "Reject", "rules": []}));
    assert_eq!(unmatched_route(&p).unwrap_err(), RejectReason::Unmatched);
}

// --- trace ------------------------------------------------------------

#[test]
fn trace_records_survivors_the_killer_and_the_objective() {
    let p = policy(json!({"rules": [{"name": "cheap", "route": {
        "exclude": [{"name": "no-a100", "drop": {"backend": {"name": "a100"}}}],
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let out = evaluate(
        route_of(&p),
        Some("cheap"),
        &req(),
        vec![cand("a100", "bar", 10.0), cand("h100", "bar", 250.0)],
    );
    let Outcome::Chosen { trace, .. } = out else {
        panic!("expected a choice")
    };
    assert_eq!(trace.considered.len(), 2);
    assert_eq!(trace.survivors, vec!["h100/bar@h100-ep:8000"]);
    assert_eq!(trace.eliminated.len(), 1);
    assert_eq!(trace.winner_ttft_ms, Some(250.0));
    let s = trace.summary();
    assert!(s.contains("rule=cheap"), "{s}");
    assert!(s.contains("no-a100"), "the killing exclusion is named: {s}");
    assert!(s.contains("objective_ms=250.0"), "{s}");
}

#[test]
fn empty_candidate_set_traces_the_rule() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"minimize": "PredictedTTFT"}]}}]}));
    let Outcome::Empty { trace } = evaluate(route_of(&p), Some("r"), &req(), vec![]) else {
        panic!("expected empty")
    };
    assert_eq!(trace.rule.as_deref(), Some("r"));
    assert!(trace.considered.is_empty());
}

// --- provider candidates ----------------------------------------------

/// A provider has no `PredictedTTFT`, and must not be scored as if it were
/// `0.0`, the best possible value.
#[test]
fn an_unrankable_candidate_is_dropped_from_the_tier_not_scored_as_zero() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{ "minimize": "PredictedTTFT" }] }
        }]
    }));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        // The local one is SLOW. A provider defaulted to zero would beat it.
        vec![cand("h100", "m", 900.0), prov("together", "m", None)],
    );
    match out {
        Outcome::Chosen { candidate, trace } => {
            assert_eq!(candidate.backend, "h100");
            assert!(
                trace
                    .eliminated
                    .iter()
                    .any(|(c, why)| c.contains("together") && why.contains("objective")),
                "the trace has to say why it went, not drop it silently: {:?}",
                trace.eliminated
            );
        }
        other => panic!("expected the local candidate to win, got {other:?}"),
    }
}

#[test]
fn estimated_ttft_ranks_a_prediction_against_an_observation() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{ "minimize": "EstimatedTTFT" }] }
        }]
    }));
    // The provider is faster, so it wins.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("h100", "m", 900.0), prov("together", "m", Some(200.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "together"),
        "{out:?}"
    );

    // And loses when it is slower.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("h100", "m", 200.0), prov("together", "m", Some(900.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"),
        "{out:?}"
    );
}

/// An unobserved provider stays eligible, or it could never get an
/// observation.
#[test]
fn an_unobserved_provider_sorts_last_but_still_wins_when_alone() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{ "minimize": "EstimatedTTFT" }] }
        }]
    }));
    let out = evaluate(route_of(&p), Some("r"), &req(), vec![prov("t", "m", None)]);
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "t"),
        "an unobserved provider alone in its tier must be chosen, not deadlocked: {out:?}"
    );

    // But it defers to anything with a real figure.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![prov("t", "m", None), cand("h100", "m", 5000.0)],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"),
        "{out:?}"
    );
}

#[test]
fn a_tier_can_select_by_backend_class() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [
                { "type": "Local", "minimize": "PredictedTTFT" },
                { "type": "Provider", "order": ["together"] }
            ]}
        }]
    }));
    // Tier 1 has a local candidate, so it wins outright; the provider tier is
    // only reached once nothing local survives.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("h100", "m", 900.0), prov("together", "m", Some(10.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"),
        "{out:?}"
    );

    // With the local candidate gone, tier 2 takes over.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![prov("together", "m", Some(10.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "together"),
        "{out:?}"
    );
}

#[test]
fn dropping_by_backend_type_keeps_a_request_in_the_cluster() {
    let p = policy(json!({
        "rules": [{
            "name": "local-only",
            "route": {
                "exclude": [{
                    "name": "stays-local",
                    "drop": { "backend": { "type": "Provider" } }
                }],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }]
            }
        }]
    }));
    let out = evaluate(
        route_of(&p),
        Some("local-only"),
        &req(),
        vec![cand("h100", "m", 900.0), prov("together", "m", Some(1.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"),
        "a provider must not win a request whose rule dropped it: {out:?}"
    );

    // And with only the provider standing, the request is refused rather than
    // leaving the cluster.
    let out = evaluate(
        route_of(&p),
        Some("local-only"),
        &req(),
        vec![prov("together", "m", Some(1.0))],
    );
    assert!(matches!(out, Outcome::Empty { .. }), "{out:?}");
}

/// A Local-only signal doesn't drop a provider, since absence doesn't match.
#[test]
fn a_local_only_signal_never_drops_a_provider_candidate() {
    let p = policy(json!({
        "rules": [{
            "name": "shed",
            "route": {
                "exclude": [{
                    "name": "busy",
                    "drop": { "queuedPrompts": { "greaterThan": 1 } }
                }],
                "select": [{ "minimize": "EstimatedTTFT" }]
            }
        }]
    }));
    let mut busy = cand("h100", "m", 10.0);
    busy.attrs.queued_prompts = Some(9.0);
    let out = evaluate(
        route_of(&p),
        Some("shed"),
        &req(),
        vec![busy, prov("together", "m", Some(900.0))],
    );
    // The busy local endpoint goes; the provider survives, carrying no
    // `queuedPrompts` at all, so load spills off-cluster.
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "together"),
        "{out:?}"
    );
}

#[test]
fn prefer_local_breaks_a_tie_toward_the_cluster() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": {
                "select": [{ "minimize": "EstimatedTTFT" }],
                "tieBreak": ["PreferLocal"]
            }
        }]
    }));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![prov("together", "m", Some(300.0)), cand("h100", "m", 300.0)],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"),
        "an exact tie should stay in the cluster: {out:?}"
    );
}

#[test]
fn order_ranks_provider_names_unchanged() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{
                "type": "Provider",
                "order": ["bedrock", "together"]
            }]}
        }]
    }));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        // Listed second in the candidate set, first in the order.
        vec![
            prov("together", "m", Some(10.0)),
            prov("bedrock", "m", Some(9000.0)),
        ],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "bedrock"),
        "`order` is a preference list, not a latency ranking: {out:?}"
    );
}

#[test]
fn the_context_window_check_skips_providers() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{ "type": "Provider", "order": ["small", "big"] }] }
        }]
    }));
    let mut small = prov("small", "m", Some(10.0));
    small.attrs.context_window = Some(8192);
    small.attrs.input_tokens = Some(100_000);
    let mut big = prov("big", "m", Some(9000.0));
    big.attrs.context_window = Some(200_000);
    let attrs = RequestAttrs {
        input_tokens: Some(100_000),
        ..Default::default()
    };
    let out = evaluate(route_of(&p), Some("r"), &attrs, vec![small, big]);
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "small"),
        "a provider's declared contextWindow does not gate it: {out:?}"
    );
}

#[test]
fn the_context_window_check_uses_each_candidates_own_count() {
    let p = policy(json!({"rules": [{"name": "r", "route": {
        "select": [{"order": ["short", "long"]}]}}]}));
    // One window, two tokenizers: only the shorter rendering fits.
    let mut short = cand("short", "a", 100.0);
    short.attrs.context_window = Some(8192);
    short.attrs.input_tokens = Some(6000);
    let mut long = cand("long", "b", 100.0);
    long.attrs.context_window = Some(8192);
    long.attrs.input_tokens = Some(9000);
    // The request's count would let both through.
    let attrs = RequestAttrs {
        input_tokens: Some(100),
        ..Default::default()
    };
    let out = evaluate(route_of(&p), Some("r"), &attrs, vec![long, short]);
    match out {
        Outcome::Chosen { candidate, trace } => {
            assert_eq!(candidate.backend, "short");
            assert_eq!(trace.eliminated.len(), 1);
            assert!(trace.eliminated[0].0.starts_with("long/"));
        }
        other => panic!("expected a choice, got {other:?}"),
    }
}

/// `type:` on a select tier filters candidates, including on an `order` tier,
/// which ranks by name and so has no objective to drop them by.
#[test]
fn a_tier_type_actually_filters_by_class() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{ "type": "Provider", "order": ["together"] }] }
        }]
    }));
    // A local candidate is not admitted to a Provider-only tier, even when it
    // is the only thing standing.
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("h100", "m", 10.0)],
    );
    assert!(
        matches!(out, Outcome::Empty { .. }),
        "a local candidate slipped into a Provider tier: {out:?}"
    );
}

/// The mirror image.
#[test]
fn a_local_tier_excludes_providers() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{ "type": "Local", "order": ["together"] }] }
        }]
    }));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![prov("together", "m", None)],
    );
    assert!(matches!(out, Outcome::Empty { .. }), "{out:?}");
}

/// `backends.type` is the long form of the same thing and must agree.
#[test]
fn the_long_form_of_the_tier_type_behaves_identically() {
    let p = policy(json!({
        "rules": [{
            "name": "r",
            "route": { "select": [{
                "backends": { "type": "Provider" },
                "order": ["together"]
            }]}
        }]
    }));
    let out = evaluate(
        route_of(&p),
        Some("r"),
        &req(),
        vec![cand("h100", "m", 10.0)],
    );
    assert!(matches!(out, Outcome::Empty { .. }), "{out:?}");
}

/// Unmatched traffic never leaves the cluster, regardless of which objective
/// the implicit tier uses.
#[test]
fn the_unmatched_route_never_reaches_a_provider() {
    let route = CompiledPolicy::default_route();
    let out = evaluate(&route, None, &req(), vec![prov("together", "m", Some(5.0))]);
    assert!(
        matches!(out, Outcome::Empty { .. }),
        "a provider served unmatched traffic: {out:?}"
    );

    // A local candidate is still served, even a slow one, since the implicit
    // route carries no exclusions.
    let out = evaluate(
        &route,
        None,
        &req(),
        vec![cand("h100", "m", 9000.0), prov("together", "m", Some(5.0))],
    );
    assert!(
        matches!(&out, Outcome::Chosen { candidate, .. } if candidate.backend == "h100"),
        "{out:?}"
    );
}

/// The provider is excluded at generation, not by failing to rank, so the
/// trace doesn't suggest a missing metric.
#[test]
fn the_unmatched_route_rejects_a_provider_as_ineligible_not_unmeasurable() {
    let route = CompiledPolicy::default_route();
    assert!(
        !route.select[0].backends.matches(
            crate::crd::BackendType::Provider,
            "together",
            &BTreeMap::new()
        ),
        "the implicit tier must not admit a provider in the first place"
    );
    assert!(route.select[0].backends.matches(
        crate::crd::BackendType::Local,
        "h100",
        &BTreeMap::new()
    ));
}

/// `models_to_score` carries the same filter, so no provider candidate is
/// even built for unmatched traffic.
#[test]
fn unmatched_generation_does_not_target_providers() {
    let route = CompiledPolicy::default_route();
    let targets = models_to_score(&route, "m");
    assert_eq!(targets.len(), 1);
    assert!(!targets[0].backends.matches(
        crate::crd::BackendType::Provider,
        "together",
        &BTreeMap::new()
    ));
}

/// A defaulted `consider` targets only the name that the client asked for;
/// each destination then resolves that name through its own alias table.
#[test]
fn a_defaulted_consider_targets_the_name_the_client_asked_for() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "local-first-with-provider-failover",
            "route": {
                "select": [
                    {"type": "Local", "minimize": "PredictedTTFT"},
                    {"type": "Provider", "order": ["databricks"]}
                ],
                "onEmpty": "Reject"
            }
        }]
    }));
    let targets = models_to_score(route_of(&p), "qwen-2.5");
    let names: Vec<&str> = targets.iter().map(|t| t.model.as_str()).collect();
    assert_eq!(names, ["qwen-2.5"]);
}

/// One name in, one target out.
#[test]
fn a_defaulted_consider_with_a_single_name_yields_a_single_target() {
    let p = policy(serde_json::json!({
        "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    let targets = models_to_score(route_of(&p), "only-one");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].model, "only-one");
}
