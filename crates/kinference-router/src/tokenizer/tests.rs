use std::path::Path;

use super::*;
use crate::tokenizer::local::LocalTokenizeConfig;
use crate::tokenizer::test_support::{
    FakeRender, Render, WORDLEVEL_CHAT_TEMPLATE, exposition, mount_wordlevel, request,
    router_metrics, worker,
};

/// Every backend over a mount holding model `m`, and the registry the
/// dispatcher's metrics land in.
fn by_mode(dir: &Path) -> (ByMode, prometheus::Registry) {
    mount_wordlevel(dir, "m", Some(WORDLEVEL_CHAT_TEMPLATE));
    let (registry, prom) = router_metrics();
    let http = Arc::new(HttpRender::new(16, Arc::new(MetricsRegistry::new())).expect("http"));
    let config = LocalTokenizeConfig {
        mount_dir: Some(dir.to_path_buf()),
        warmup: false,
        hub_token: None,
    };
    let local = Arc::new(LocalTokenizer::new(config, http.clone(), prom.clone()).expect("local"));
    let compare = Arc::new(Compare::new(http.clone(), local.clone(), prom.clone()));
    (ByMode::new(http, local, compare, prom), registry)
}

fn raw() -> TokenizeInput {
    TokenizeInput::Raw("hello world".into())
}

/// Render mode goes to `/render`; the call is timed and no path is counted.
#[tokio::test]
async fn render_mode_renders() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (tokenizer, registry) = by_mode(dir.path());
    let fake = FakeRender::start(Render::Tokens(vec![1])).await;
    let workers = [(
        1,
        worker("m", "10.0.0.1:8000", true, Some(fake.base().to_string())),
    )];

    let out = tokenizer.tokenize(request(&workers, "m", raw())).await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![1]));
    let out = exposition(&registry);
    assert!(
        out.contains(r#"kinference_tokenize_duration_ms_count{api="chat_completions"} 1"#),
        "{out}"
    );
    for path in ["local", "http_fallback", "unavailable"] {
        assert!(
            out.contains(&format!(
                r#"kinference_tokenize_total{{api="chat_completions",path="{path}"}} 0"#
            )),
            "{out}"
        );
    }
}

/// An unavailable outcome is counted once, and still timed.
#[tokio::test]
async fn an_unavailable_outcome_is_counted_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (tokenizer, registry) = by_mode(dir.path());
    let workers = [(1, worker("m", "10.0.0.1:8000", true, None))];

    let out = tokenizer.tokenize(request(&workers, "m", raw())).await;
    assert!(
        matches!(out, TokenizeOutcome::Unavailable { .. }),
        "{out:?}"
    );
    let out = exposition(&registry);
    assert!(
        out.contains(r#"kinference_tokenize_total{api="chat_completions",path="unavailable"} 1"#),
        "{out}"
    );
    assert!(
        out.contains(r#"kinference_tokenize_duration_ms_count{api="chat_completions"} 1"#),
        "{out}"
    );
}

/// Local mode tokenizes in process.
#[tokio::test]
async fn local_mode_uses_the_local_backend() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (tokenizer, registry) = by_mode(dir.path());
    let mut req = request(&[], "m", raw());
    req.mode = TokenizeMode::Local;

    let out = tokenizer.tokenize(req).await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![99, 0, 1]));
    let out = exposition(&registry);
    assert!(
        out.contains(r#"kinference_tokenize_total{api="chat_completions",path="local"} 1"#),
        "{out}"
    );
    assert!(
        out.contains(r#"kinference_tokenize_duration_ms_count{api="chat_completions"} 1"#),
        "{out}"
    );
}

/// Compare mode returns what `/render` said.
#[tokio::test]
async fn compare_mode_returns_render() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (tokenizer, _) = by_mode(dir.path());
    let fake = FakeRender::start(Render::Tokens(vec![5])).await;
    let workers = [(
        1,
        worker("m", "10.0.0.1:8000", true, Some(fake.base().to_string())),
    )];
    let mut req = request(&workers, "m", raw());
    req.mode = TokenizeMode::Compare;

    let out = tokenizer.tokenize(req).await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![5]));
}
