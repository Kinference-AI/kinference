use super::*;
use serde_json::json;

/// A policy can pick a different model after the body was serialized, and
/// the body must then name the new one.
#[test]
fn rewrite_model_replaces_the_model_and_preserves_everything_else() {
    let body = br#"{"model":"Qwen/Qwen2.5-32B-Instruct-AWQ","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#;
    let out = rewrite_model(body, "google/gemma-4-12B-it").expect("a JSON object rewrites");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["model"], "google/gemma-4-12B-it");
    // Every other field the caller sent survives.
    assert_eq!(v["max_tokens"], 16);
    assert_eq!(v["stream"], true);
    assert_eq!(v["messages"][0]["content"], "hi");
}

/// The rewrite keeps the client's key order, including inside a JSON schema.
#[test]
fn rewrite_model_preserves_key_order() {
    let body = br#"{"stream":false,"model":"a","response_format":{"type":"json_schema","json_schema":{"schema":{"properties":{"reasoning":{},"answer":{}}}}}}"#;
    let out = rewrite_model(body, "b").expect("a JSON object rewrites");
    assert_eq!(
        std::str::from_utf8(&out).unwrap(),
        r#"{"stream":false,"model":"b","response_format":{"type":"json_schema","json_schema":{"schema":{"properties":{"reasoning":{},"answer":{}}}}}}"#
    );
}

/// The pick-time mutations keep existing keys in place and append new ones.
#[test]
fn pick_body_mutations_preserve_key_order() {
    let mut obj: Value =
        serde_json::from_str(r#"{"stream":true,"zeta":1,"model":"auto","alpha":2}"#).unwrap();
    assert!(apply_pick_body_mutations(&mut obj, Some("m")));
    assert_eq!(
        serde_json::to_string(&obj).unwrap(),
        r#"{"stream":true,"zeta":1,"model":"m","alpha":2,"stream_options":{"include_usage":true}}"#
    );
}

/// A body we cannot parse is left alone; the caller refuses the request.
#[test]
fn rewrite_model_refuses_a_non_object_body() {
    assert!(rewrite_model(b"not json", "m").is_none());
    assert!(
        rewrite_model(b"[1,2,3]", "m").is_none(),
        "a JSON array is not a request body"
    );
}

#[test]
fn model_rewrite_sets_model_and_reports_mutated() {
    let mut obj = json!({ "model": "auto", "messages": [] });
    assert!(apply_pick_body_mutations(
        &mut obj,
        Some("google/gemma-4-12B-it")
    ));
    assert_eq!(obj["model"], json!("google/gemma-4-12B-it"));
}

#[test]
fn no_force_model_and_no_stream_is_a_noop() {
    let mut obj = json!({ "model": "m", "messages": [], "temperature": 0.5 });
    let before = obj.clone();
    assert!(!apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj, before);
}

#[test]
fn stream_true_adds_include_usage() {
    let mut obj = json!({ "model": "m", "stream": true });
    assert!(apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
}

#[test]
fn stream_true_preserves_existing_stream_options() {
    let mut obj = json!({
        "model": "m",
        "stream": true,
        "stream_options": { "continuous_usage_stats": true }
    });
    assert!(apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
    // Pre-existing sub-key must survive.
    assert_eq!(obj["stream_options"]["continuous_usage_stats"], json!(true));
}

#[test]
fn include_usage_already_true_is_a_noop() {
    let mut obj = json!({
        "model": "m",
        "stream": true,
        "stream_options": { "include_usage": true }
    });
    assert!(!apply_pick_body_mutations(&mut obj, None));
}

#[test]
fn malformed_stream_options_is_replaced() {
    // A non-object stream_options is coerced to a proper object.
    let mut obj = json!({ "model": "m", "stream": true, "stream_options": "garbage" });
    assert!(apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
}

#[test]
fn stream_false_adds_nothing() {
    let mut obj = json!({ "model": "m", "stream": false });
    assert!(!apply_pick_body_mutations(&mut obj, None));
    assert!(obj.get("stream_options").is_none());
}

#[test]
fn stream_absent_adds_nothing() {
    let mut obj = json!({ "model": "m" });
    assert!(!apply_pick_body_mutations(&mut obj, None));
    assert!(obj.get("stream_options").is_none());
}

#[test]
fn both_mutations_apply_together() {
    let mut obj = json!({ "model": "auto", "stream": true });
    assert!(apply_pick_body_mutations(&mut obj, Some("qwen")));
    assert_eq!(obj["model"], json!("qwen"));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
}

#[test]
fn unknown_fields_survive_parse_mutate_serialize() {
    let original = json!({
        "model": "auto",
        "messages": [{ "role": "user", "content": "hi" }],
        "temperature": 0.7,
        "max_tokens": 100,
        "stream": true,
        "some_vendor_ext": { "nested": [1, 2, 3] }
    });
    let mut obj = original.clone();
    assert!(apply_pick_body_mutations(&mut obj, Some("concrete-model")));
    // Round-trip through bytes the way pick_and_record will.
    let bytes = serde_json::to_vec(&obj).unwrap();
    let roundtripped: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(roundtripped["model"], json!("concrete-model"));
    assert_eq!(roundtripped["temperature"], json!(0.7));
    assert_eq!(roundtripped["max_tokens"], json!(100));
    assert_eq!(roundtripped["messages"], original["messages"]);
    assert_eq!(roundtripped["some_vendor_ext"], original["some_vendor_ext"]);
    assert_eq!(roundtripped["stream_options"]["include_usage"], json!(true));
}

#[test]
fn non_object_body_is_left_unchanged() {
    let mut obj = json!([1, 2, 3]);
    assert!(!apply_pick_body_mutations(&mut obj, Some("m")));
    assert_eq!(obj, json!([1, 2, 3]));
}

/// `stream_flag` matches Pydantic's lax bool, as the engine does.
#[test]
fn stream_flag_matches_pydantic_lax_bool() {
    let t = |raw: &str| stream_flag(&serde_json::from_str::<Value>(raw).unwrap());

    // Truthy, exactly Pydantic's set.
    for raw in [
        r#"{"stream":true}"#,
        r#"{"stream":"true"}"#,
        r#"{"stream":"True"}"#,
        r#"{"stream":"TRUE"}"#,
        r#"{"stream":"  true  "}"#,
        r#"{"stream":"t"}"#,
        r#"{"stream":"y"}"#,
        r#"{"stream":"yes"}"#,
        r#"{"stream":"on"}"#,
        r#"{"stream":"1"}"#,
        r#"{"stream":1}"#,
    ] {
        assert!(t(raw), "{raw} should read as streaming");
    }

    // Falsy, absent, and malformed. `2` is not truthy: Pydantic rejects
    // integers other than 0 and 1.
    for raw in [
        r#"{"stream":false}"#,
        r#"{"stream":"false"}"#,
        r#"{"stream":"False"}"#,
        r#"{"stream":"f"}"#,
        r#"{"stream":"n"}"#,
        r#"{"stream":"no"}"#,
        r#"{"stream":"off"}"#,
        r#"{"stream":"0"}"#,
        r#"{"stream":0}"#,
        r#"{"stream":2}"#,
        r#"{"stream":null}"#,
        r#"{"stream":[]}"#,
        r#"{"stream":"maybe"}"#,
        r#"{"messages":[]}"#,
    ] {
        assert!(!t(raw), "{raw} should NOT read as streaming");
    }
}

/// A string-valued `stream` still gets `include_usage` injected.
#[test]
fn string_valued_stream_still_gets_include_usage() {
    for raw in [
        r#"{"stream":"true"}"#,
        r#"{"stream":"yes"}"#,
        r#"{"stream":1}"#,
    ] {
        let mut obj: Value = serde_json::from_str(raw).unwrap();
        assert!(
            apply_pick_body_mutations(&mut obj, None),
            "{raw} should mutate"
        );
        assert_eq!(
            obj["stream_options"]["include_usage"],
            json!(true),
            "{raw} should have had include_usage forced on"
        );
    }
}
