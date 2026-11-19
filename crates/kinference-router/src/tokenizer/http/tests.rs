use std::collections::HashMap;

use super::*;
use kinference_fleet::workers::WorkerEntry;

#[test]
fn parse_render_response_single_object_extracts_token_ids() {
    let body = br#"{"request_id":"x","token_ids":[1,2,3],"sampling_params":{}}"#;
    let got = parse_render_response(body).expect("single-object parse");
    assert_eq!(got, vec![1, 2, 3]);
}

#[test]
fn parse_render_response_list_takes_first() {
    let body = br#"[{"token_ids":[1,2,3]},{"token_ids":[9,9]}]"#;
    let got = parse_render_response(body).expect("list parse");
    assert_eq!(got, vec![1, 2, 3], "first element's token_ids wins");
}

#[test]
fn parse_render_response_missing_token_ids_errors() {
    let body = br#"{"request_id":"x"}"#;
    let err = parse_render_response(body).expect_err("must error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("token_ids"),
        "error should mention token_ids: {msg}"
    );
}

#[test]
fn parse_render_response_empty_array_errors() {
    let err = parse_render_response(b"[]").expect_err("must error");
    let msg = format!("{err:#}");
    assert!(msg.contains("empty"), "error should mention empty: {msg}");
}

#[test]
fn build_chat_render_body_includes_extras_when_present() {
    let messages = vec![serde_json::json!({"role": "user", "content": "hi"})];
    let mut kwargs = HashMap::new();
    kwargs.insert("enable_thinking".to_string(), "false".to_string());
    let tools = vec![serde_json::json!({"type": "function", "function": {"name": "foo"}})];
    let tool_choice = serde_json::json!("required");

    let body = build_chat_render_body(
        "qwen",
        &messages,
        &kwargs,
        &tools,
        Some(&tool_choice),
        false,
    );

    assert_eq!(body["model"], "qwen");
    assert!(body["messages"].is_array());
    assert_eq!(body["add_generation_prompt"], false);
    assert!(body["tools"].is_array(), "tools present → field included");
    assert_eq!(body["tool_choice"], "required");
    // `enable_thinking` was string "false"; should JSON-parse to bool false.
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
}

#[test]
fn build_chat_render_body_omits_extras_when_empty() {
    let messages = vec![serde_json::json!({"role": "user", "content": "hi"})];
    let body = build_chat_render_body("qwen", &messages, &HashMap::new(), &[], None, true);

    assert_eq!(body["model"], "qwen");
    assert_eq!(body["add_generation_prompt"], true);
    assert!(
        body.get("tools").is_none(),
        "empty tools → field omitted (don't emit `tools: []`): {body}"
    );
    assert!(
        body.get("tool_choice").is_none(),
        "no tool_choice → field omitted: {body}"
    );
    assert!(
        body.get("chat_template_kwargs").is_none(),
        "no kwargs → field omitted: {body}"
    );
}

// --- the backend, against a fake /render ------------------------------------

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::tokenizer::test_support::{FakeRender, Render, request, worker};
use kinference_fleet::scrape::{MetricsRegistry, MetricsSample};

fn backend(scrape: &Arc<MetricsRegistry>) -> HttpRender {
    HttpRender::new(16, scrape.clone()).expect("an HTTP backend")
}

/// Record that `worker` was scraped `age` ago.
fn scraped(scrape: &MetricsRegistry, worker: u64, age: Duration) {
    scrape.record_sample_for_test(
        worker,
        MetricsSample {
            scraped_at: Some(Instant::now().checked_sub(age).expect("a recent instant")),
            ..Default::default()
        },
    );
}

fn at(fake: &FakeRender, addr: &str) -> WorkerEntry {
    worker("m", addr, true, Some(fake.base().to_string()))
}

fn raw() -> TokenizeInput {
    TokenizeInput::Raw("hello".into())
}

/// A worker's `/render` answers with the token ids, on the completions
/// path for a raw prompt.
#[tokio::test]
async fn renders_through_a_worker() {
    let fake = FakeRender::start(Render::Tokens(vec![1, 2, 3])).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [(1, at(&fake, "10.0.0.1:8000"))];

    let out = backend(&scrape)
        .render(&request(&workers, "m", raw()))
        .await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![1, 2, 3]));
    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/completions/render");
}

/// A raw body reaches the chat render path byte for byte.
#[tokio::test]
async fn a_raw_body_is_forwarded_verbatim() {
    let fake = FakeRender::start(Render::Tokens(vec![1])).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [(1, at(&fake, "10.0.0.1:8000"))];
    let body = br#"{"model":"m","messages":[],"x":1}"#;
    let mut req = request(&workers, "m", raw());
    req.raw_body = Some(body);

    let out = backend(&scrape).render(&req).await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![1]));
    let requests = fake.requests();
    assert_eq!(requests[0].path, "/v1/chat/completions/render");
    assert_eq!(requests[0].body, body);
}

/// A dedicated `render_url` is tried once; its failure never falls back to
/// the workers.
#[tokio::test]
async fn a_render_url_fails_fast() {
    let failing = FakeRender::start(Render::Failing).await;
    let fine = FakeRender::start(Render::Tokens(vec![1])).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [(1, at(&fine, "10.0.0.1:8000"))];
    let url = format!("{}/render", failing.base());
    let mut req = request(&workers, "m", raw());
    req.render_url = Some(&url);

    let out = backend(&scrape).render(&req).await;
    let TokenizeOutcome::Unavailable { reason, attempts } = out else {
        panic!("{out:?}");
    };
    assert_eq!(attempts, 1);
    assert!(reason.contains(&url), "{reason}");
    assert_eq!(failing.calls(), 1);
    assert_eq!(fine.calls(), 0);
}

/// A failing worker is skipped for the next one.
#[tokio::test]
async fn rotation_skips_a_failing_worker() {
    let failing = FakeRender::start(Render::Failing).await;
    let fine = FakeRender::start(Render::Tokens(vec![2])).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [
        (1, at(&failing, "10.0.0.1:8000")),
        (2, at(&fine, "10.0.0.2:8000")),
    ];
    // Worker 1 is the fresher scrape, so it goes first.
    scraped(&scrape, 1, Duration::ZERO);
    scraped(&scrape, 2, Duration::from_secs(10));

    let out = backend(&scrape)
        .render(&request(&workers, "m", raw()))
        .await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![2]));
    assert_eq!(failing.calls(), 1);
    assert_eq!(fine.calls(), 1);
}

/// The worker scraped most recently is tried first.
#[tokio::test]
async fn the_freshest_scrape_is_tried_first() {
    let stale = FakeRender::start(Render::Tokens(vec![1])).await;
    let fresh = FakeRender::start(Render::Tokens(vec![2])).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [
        (1, at(&stale, "10.0.0.1:8000")),
        (2, at(&fresh, "10.0.0.2:8000")),
    ];
    scraped(&scrape, 1, Duration::from_secs(10));
    scraped(&scrape, 2, Duration::ZERO);

    let out = backend(&scrape)
        .render(&request(&workers, "m", raw()))
        .await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![2]));
    assert_eq!(stale.calls(), 0);
    assert_eq!(fresh.calls(), 1);
}

/// An outage stops after three workers, not the whole pool.
#[tokio::test]
async fn at_most_three_workers_are_tried() {
    let failing = FakeRender::start(Render::Failing).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers: Vec<(u64, WorkerEntry)> = (1..=4)
        .map(|i| (i, at(&failing, &format!("10.0.0.{i}:8000"))))
        .collect();

    let out = backend(&scrape)
        .render(&request(&workers, "m", raw()))
        .await;
    let TokenizeOutcome::Unavailable { attempts, .. } = out else {
        panic!("{out:?}");
    };
    assert_eq!(attempts, 3);
    assert_eq!(failing.calls(), 3);
}

/// Without an `http_url` there is nothing to try.
#[tokio::test]
async fn no_http_url_is_unavailable_without_an_attempt() {
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [(1, worker("m", "10.0.0.1:8000", true, None))];

    let out = backend(&scrape)
        .render(&request(&workers, "m", raw()))
        .await;
    let TokenizeOutcome::Unavailable { reason, attempts } = out else {
        panic!("{out:?}");
    };
    assert_eq!(attempts, 0);
    assert!(reason.contains("http_url"), "{reason}");
}

/// A status that rejects the body is not retried on another worker.
#[tokio::test]
async fn a_rejected_body_is_not_retried() {
    for code in [400, 413, 422] {
        let rejecting = FakeRender::start(Render::Status(code)).await;
        let scrape = Arc::new(MetricsRegistry::new());
        let workers = [
            (1, at(&rejecting, "10.0.0.1:8000")),
            (2, at(&rejecting, "10.0.0.2:8000")),
        ];

        let out = backend(&scrape)
            .render(&request(&workers, "m", raw()))
            .await;
        let TokenizeOutcome::Unavailable { attempts, .. } = out else {
            panic!("{code}: {out:?}");
        };
        assert_eq!(attempts, 1, "{code}");
        assert_eq!(rejecting.calls(), 1, "{code}");
    }
}

/// A status that may be particular to one worker moves on to the next.
#[tokio::test]
async fn a_worker_status_moves_to_the_next_worker() {
    for code in [404, 429, 503] {
        let failing = FakeRender::start(Render::Status(code)).await;
        let fine = FakeRender::start(Render::Tokens(vec![2])).await;
        let scrape = Arc::new(MetricsRegistry::new());
        let workers = [
            (1, at(&failing, "10.0.0.1:8000")),
            (2, at(&fine, "10.0.0.2:8000")),
        ];
        scraped(&scrape, 1, Duration::ZERO);
        scraped(&scrape, 2, Duration::from_secs(10));

        let out = backend(&scrape)
            .render(&request(&workers, "m", raw()))
            .await;
        assert_eq!(out, TokenizeOutcome::Ok(vec![2]), "{code}");
        assert_eq!(failing.calls(), 1, "{code}");
        assert_eq!(fine.calls(), 1, "{code}");
    }
}

/// The reason for a failed render leaves out the response body, which may
/// echo the prompt.
#[tokio::test]
async fn the_reason_leaves_out_the_response_body() {
    let rejecting = FakeRender::start(Render::Status(400)).await;
    let scrape = Arc::new(MetricsRegistry::new());
    let workers = [(1, at(&rejecting, "10.0.0.1:8000"))];
    let input = TokenizeInput::Raw("a secret prompt".into());

    let out = backend(&scrape)
        .render(&request(&workers, "m", input))
        .await;
    let TokenizeOutcome::Unavailable { reason, .. } = out else {
        panic!("{out:?}");
    };
    assert!(reason.contains("400"), "{reason}");
    assert!(!reason.contains("secret"), "{reason}");
}
