use super::*;

#[test]
fn absent_status_fields_serialize_as_null_for_merge_patches() {
    fn assert_null_fields(status: impl serde::Serialize, fields: &[&str]) {
        let json = serde_json::to_value(status).unwrap();
        for field in fields {
            assert_eq!(
                json.get(*field),
                Some(&serde_json::Value::Null),
                "{field} must serialize as null to remove its previous value"
            );
        }
    }

    assert_null_fields(
        InferenceWorkerSetStatus::default(),
        &["state", "endpoints", "kvEvents"],
    );
    assert_null_fields(
        KvEventsStatus::default(),
        &["configured", "subscribedEndpoints"],
    );
    assert_null_fields(EndpointCounts::default(), &["ready"]);
    assert_null_fields(
        InferencePoolStatus::default(),
        &["state", "destinationCount"],
    );
    assert_null_fields(InferenceProviderStatus::default(), &["state"]);
    assert_null_fields(InferencePolicyStatus::default(), &["state"]);
}

/// The YAML in `charts/kinference-crds/templates/` is the contract; these check
/// that our types deserialize its examples.
#[test]
fn worker_set_from_guide_example() {
    let y = serde_yaml_lite(
        r#"{"serviceRef":{"name":"vllm-h100","port":"openai"},
                "modelDiscovery":{"mode":"Endpoint","interval":"30s"},
                "models":[{"name":"meta-llama/Llama-3.3-70B-Instruct","aliases":["bar"]},
                          {"name":"neuralmagic/Llama-3.3-70B-Instruct-FP8","aliases":["bar"]}]}"#,
    );
    let spec: InferenceWorkerSetSpec = serde_json::from_value(y).unwrap();
    assert_eq!(spec.service_ref.name, "vllm-h100");
    assert_eq!(
        spec.service_ref.port.as_ref().unwrap().as_name(),
        Some("openai")
    );
    // Two upstream models sharing an alias are interchangeable.
    assert_eq!(spec.models.len(), 2);
    assert_ne!(spec.models[0].name, spec.models[1].name);
    assert_eq!(spec.models[0].aliases, spec.models[1].aliases);
    assert_eq!(spec.models[0].aliases, vec!["bar".to_string()]);
}

#[test]
fn numeric_port_parses_as_number() {
    let spec: InferenceWorkerSetSpec = serde_json::from_value(serde_yaml_lite(
        r#"{"serviceRef":{"name":"s","port":8000}}"#,
    ))
    .unwrap();
    let port = spec.service_ref.port.unwrap();
    assert_eq!(port.as_number(), Some(8000));
    assert_eq!(port.as_name(), None, "a numeric port has no name");
}

#[test]
fn pool_with_policy_ref_and_mixed_destinations() {
    let spec: InferencePoolSpec = serde_json::from_value(serde_yaml_lite(
            r#"{"destinations":[{"kind":"InferenceWorkerSet","name":"workers-h100"},
                                {"kind":"InferenceWorkerSet","selector":{"matchLabels":{"tier":"shared"}}}],
                "policyRef":{"name":"chat-policy"}}"#,
        ))
        .unwrap();
    assert_eq!(spec.destinations.len(), 2);
    assert_eq!(spec.destinations[0].name.as_deref(), Some("workers-h100"));
    assert!(spec.destinations[1].selector.is_some());
    assert_eq!(spec.policy_ref.unwrap().name, "chat-policy");
}

#[test]
fn pool_without_policy_ref_is_valid() {
    // "Omitting it means serve every request as asked."
    let spec: InferencePoolSpec = serde_json::from_value(serde_yaml_lite("{}")).unwrap();
    assert!(spec.policy_ref.is_none());
    assert!(
        spec.destinations.is_empty(),
        "empty = every IWS in the namespace"
    );
}

#[test]
fn policy_from_guide_example() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_yaml_lite(
        r#"{"unmatched":"ServeLocal",
                "rules":[{"name":"cheap-when-simple",
                          "matches":[{"model":"bar","qos":["batch"]}],
                          "route":{"consider":[{"models":{"names":["foo","bar"]}}],
                                   "exclude":[{"name":"keep-long-prompts-off-a100",
                                               "if":{"inputTokens":{"greaterThan":32000}},
                                               "drop":{"backend":{"name":"workers-a100"}}}],
                                   "select":[{"minimize":"PredictedTTFT"}],
                                   "tieBreak":["Random"],
                                   "onEmpty":"Reject"}}]}"#,
    ))
    .unwrap();
    assert_eq!(spec.unmatched, Unmatched::ServeLocal);
    let rule = &spec.rules[0];
    assert_eq!(rule.name, "cheap-when-simple");
    // Predicates stay untyped here; `predicate.rs` compiles them.
    assert_eq!(
        rule.matches[0].model.as_ref().unwrap(),
        &serde_json::json!("bar")
    );
    assert_eq!(
        rule.matches[0].qos.as_ref().unwrap(),
        &serde_json::json!(["batch"])
    );
    let ex = &rule.route.exclude[0];
    assert_eq!(ex.name, "keep-long-prompts-off-a100");
    assert!(ex.if_.is_some(), "the `if` keyword must map to if_");
    assert_eq!(
        ex.drop.backend.as_ref().unwrap().name.as_ref().unwrap(),
        &serde_json::json!("workers-a100")
    );
    assert_eq!(rule.route.select[0].minimize, Some(Minimize::PredictedTTFT));
    assert_eq!(rule.route.tie_break, vec![TieBreak::Random]);
    assert_eq!(rule.route.on_empty, OnEmpty::Reject);
}

#[test]
fn weighted_partition_parses() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_yaml_lite(
        r#"{"rules":[{"name":"split","route":{
                 "consider":[{"name":"h100-share","models":{"names":["bar"]},
                              "backends":{"names":["workers-h100"]},"weight":70},
                             {"name":"a100-share","models":{"names":["bar"]},
                              "backends":{"names":["workers-a100"]},"weight":30}],
                 "select":[{"minimize":"PredictedTTFT"}]}}]}"#,
    ))
    .unwrap();
    let consider = &spec.rules[0].route.consider;
    assert_eq!(consider[0].weight, Some(70));
    assert_eq!(consider[1].weight, Some(30));
}

#[test]
fn defaults_match_the_crd_schema() {
    // Route omitting tieBreak/onEmpty/consider/exclude.
    let spec: InferencePolicySpec = serde_json::from_value(serde_yaml_lite(
        r#"{"rules":[{"name":"r","route":{"select":[{"minimize":"PredictedTTFT"}]}}]}"#,
    ))
    .unwrap();
    // `unmatched` defaults to ServeLocal, `onEmpty` to Reject.
    assert_eq!(spec.unmatched, Unmatched::ServeLocal);
    assert_eq!(spec.rules[0].route.on_empty, OnEmpty::Reject);
    assert!(
        spec.rules[0].route.consider.is_empty(),
        "empty consider = fromRequest default"
    );
    assert!(
        spec.rules[0].matches.is_empty(),
        "no matches = matches everything"
    );
    let md = ModelDiscovery::default();
    assert_eq!(md.mode, ModelDiscoveryMode::Endpoint);
    assert_eq!(md.interval, "30s");
    assert_eq!(md.interval, "30s");
}

#[test]
fn order_tier_parses_as_alternative_to_minimize() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_yaml_lite(
        r#"{"rules":[{"name":"r","route":{"select":[{"order":["a","b"]}]}}]}"#,
    ))
    .unwrap();
    let tier = &spec.rules[0].route.select[0];
    assert_eq!(tier.order, vec!["a", "b"]);
    assert_eq!(tier.minimize, None, "exactly one of minimize or order");
}

/// An absent provider `path` means "dispatch on whatever the client sent", so
/// it stays `None`.
#[test]
fn provider_path_is_optional() {
    let with: InferenceProviderSpec = serde_json::from_value(serde_yaml_lite(
        r#"{"backendRef":{"name":"databricks"},"api":"OpenAI",
            "path":"/serving-endpoints/chat/completions",
            "models":[{"name":"vendor/model"}]}"#,
    ))
    .unwrap();
    assert_eq!(
        with.path.as_deref(),
        Some("/serving-endpoints/chat/completions")
    );

    let without: InferenceProviderSpec = serde_json::from_value(serde_yaml_lite(
        r#"{"backendRef":{"name":"databricks"},"api":"OpenAI",
            "models":[{"name":"vendor/model"}]}"#,
    ))
    .unwrap();
    assert_eq!(without.path, None, "an omitted path rewrites nothing");
}

/// Lets tests be written in the CRD's shape without a YAML dependency.
fn serde_yaml_lite(s: &str) -> serde_json::Value {
    serde_json::from_str(s).expect("test fixture is valid JSON")
}
