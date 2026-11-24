use super::*;

/// A provider that declares no path sends the empty string, meaning "leave
/// the path alone".
#[test]
fn a_provider_destination_carries_the_path_and_empty_means_unchanged() {
    let pick = |path: Option<&str>| crate::service::ProviderPick {
        provider: kinference_policy::engine::ObjectKey::new("infra", "together"),
        backend_ref: kinference_policy::engine::ObjectKey::new("infra", "together-backend"),
        path: path.map(str::to_string),
        upstream_model: "acme/llama-70b".into(),
        canonical_model: "bar".into(),
    };

    let proto::pick_response::Destination::Provider(t) =
        provider_destination(&pick(Some("/serving-endpoints/chat/completions")))
    else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "/serving-endpoints/chat/completions");
    // The backend is still named by reference and fully qualified; the path
    // adds to that rather than replacing any part of it.
    let backend = t.backend_ref.expect("backendRef");
    assert_eq!(backend.namespace, "infra");
    assert_eq!(backend.name, "together-backend");

    let proto::pick_response::Destination::Provider(t) = provider_destination(&pick(None)) else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "", "no declared path sends no path");
}

/// A `Pick` with `api_call` for a pushed chat body.
async fn pick_with_api_call(
    api_call: i32,
) -> (
    crate::service::test_support::TestService,
    Result<Response<PickResponse>, Status>,
) {
    let t = crate::service::test_support::ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .without_policy()
        .build()
        .await;
    let body = br#"{"model":"qwen","messages":[{"role":"user","content":"hi"}]}"#;
    let store = &t.svc.bodies.store;
    let mut reservation = store.reservation();
    reservation
        .charge(body.len() as u64)
        .expect("within budget");
    let body_stream_id = store.commit(reservation, body.to_vec());
    let out = t
        .svc
        .pick(Request::new(PickRequest {
            body_stream_id,
            api_call,
            ..Default::default()
        }))
        .await;
    (t, out)
}

/// A pick without `api_call` counts as a chat-completion pick.
#[tokio::test]
async fn a_pick_without_an_api_call_is_a_chat_completion_pick() {
    let (t, out) = pick_with_api_call(0).await;
    let response = out.expect("a response").into_inner();
    assert_ne!(response.response_id, 0, "{response:?}");

    let out = t.metrics();
    for line in [
        r#"kinference_picks_total{api="chat_completions",outcome="ok"} 1"#,
        r#"kinference_pick_duration_ms_count{api="chat_completions",outcome="ok"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
}

/// A pick with `MESSAGES` counts as a Messages pick.
#[tokio::test]
async fn a_pick_with_messages_is_a_messages_pick() {
    let (t, out) = pick_with_api_call(proto::ApiCall::Messages as i32).await;
    let response = out.expect("a response").into_inner();
    assert_ne!(response.response_id, 0, "{response:?}");

    let out = t.metrics();
    for line in [
        r#"kinference_picks_total{api="messages",outcome="ok"} 1"#,
        r#"kinference_pick_duration_ms_count{api="messages",outcome="ok"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
}

/// A pick with `MESSAGES_COUNT_TOKENS` counts as a token-count pick, and
/// its first-token report finds no record.
#[tokio::test]
async fn a_pick_with_messages_count_tokens_is_a_token_count_pick() {
    let (t, out) = pick_with_api_call(proto::ApiCall::MessagesCountTokens as i32).await;
    let response = out.expect("a response").into_inner();
    assert_ne!(response.response_id, 0, "{response:?}");
    assert_eq!(response.outcome, proto::pick_response::Outcome::Ok as i32);
    assert_eq!(t.render_calls(), 0);
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 0);

    let out = t.metrics();
    for line in [
        r#"kinference_picks_total{api="messages_count_tokens",outcome="ok"} 1"#,
        r#"kinference_pick_duration_ms_count{api="messages_count_tokens",outcome="ok"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }

    let report = t
        .svc
        .report_first_token(Request::new(ReportFirstTokenRequest {
            response_id: response.response_id,
            observed_ttft_ms: 40,
            upstream_status: 200,
        }))
        .await
        .expect("a response")
        .into_inner();
    assert!(!report.acknowledged);
}

/// An `Abort` after a token-count pick releases the body.
#[tokio::test]
async fn an_abort_after_a_token_count_pick_releases_the_body() {
    let (t, out) = pick_with_api_call(proto::ApiCall::MessagesCountTokens as i32).await;
    let response = out.expect("a response").into_inner();

    let out = t
        .svc
        .abort(Request::new(AbortRequest {
            response_id: response.response_id,
            ..Default::default()
        }))
        .await
        .expect("a response")
        .into_inner();
    assert!(out.released);
    assert_eq!(t.svc.bodies.store.stats().1, 0, "the body is released");
}

/// An unknown value fails with `INVALID_ARGUMENT`.
#[tokio::test]
async fn a_pick_with_an_unknown_api_call_is_invalid() {
    let (t, out) = pick_with_api_call(99).await;
    let status = out.expect_err("a status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert_eq!(t.svc.bodies.store.stats().1, 0, "the body is released");
    assert!(t.svc.observations.pending.is_empty());
    let out = t.metrics();
    assert!(
        !out.lines()
            .any(|l| l.starts_with("kinference_picks_total{") && !l.ends_with(" 0")),
        "{out}"
    );
}
