use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::tokenizer::http::HttpRender;
use crate::tokenizer::local::{LocalTokenizeConfig, LocalTokenizer};
use crate::tokenizer::test_support::{
    FakeRender, Render, WORDLEVEL_CHAT_TEMPLATE, chat_input, exposition, mount_wordlevel, request,
    router_metrics, worker,
};
use kinference_fleet::scrape::MetricsRegistry;

/// A compare backend over a mount holding model `m` with `template`, and
/// the registry its metrics land in.
fn compare_over(dir: &Path, template: Option<&str>) -> (Compare, prometheus::Registry) {
    mount_wordlevel(dir, "m", template);
    let (registry, prom) = router_metrics();
    let http = Arc::new(HttpRender::new(16, Arc::new(MetricsRegistry::new())).expect("http"));
    let config = LocalTokenizeConfig {
        mount_dir: Some(dir.to_path_buf()),
        warmup: false,
        hub_token: None,
    };
    let local = Arc::new(LocalTokenizer::new(config, http.clone(), prom.clone()).expect("local"));
    (Compare::new(http, local, prom), registry)
}

fn raw() -> TokenizeInput {
    TokenizeInput::Raw("hello world".into())
}

/// Equal tokens count as a match.
#[tokio::test]
async fn matching_tokens_count_a_match() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (compare, registry) = compare_over(dir.path(), Some(WORDLEVEL_CHAT_TEMPLATE));

    compare.compare("m", &raw(), &[99, 0, 1]).await;
    assert!(
        exposition(&registry).contains(r#"kinference_tokenize_compare_total{result="match"} 1"#)
    );
}

/// A local failure counts as an error, not a divergence.
#[tokio::test]
async fn a_local_failure_counts_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (compare, registry) = compare_over(dir.path(), None);

    compare
        .compare("m", &chat_input("hello world"), &[99, 0, 1])
        .await;
    assert!(
        exposition(&registry).contains(r#"kinference_tokenize_compare_total{result="error"} 1"#)
    );
}

/// The `/render` tokens are what the caller gets, whatever the local
/// tokenizer would say.
#[tokio::test]
async fn render_is_returned_unchanged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (compare, _) = compare_over(dir.path(), Some(WORDLEVEL_CHAT_TEMPLATE));
    let fake = FakeRender::start(Render::Tokens(vec![5])).await;
    let workers = [(
        1,
        worker("m", "10.0.0.1:8000", true, Some(fake.base().to_string())),
    )];

    let out = compare.tokenize(request(&workers, "m", raw())).await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![5]));
}

/// Collects what a subscriber writes.
#[derive(Clone, Default)]
struct LogBuf(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Different tokens count as a divergence, and the log names token ids,
/// never the prompt's text.
#[tokio::test]
async fn different_tokens_count_a_divergence_without_logging_the_prompt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (compare, registry) = compare_over(dir.path(), Some(WORDLEVEL_CHAT_TEMPLATE));
    let buf = LogBuf::default();
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::TRACE)
        .finish();

    let _guard = tracing::subscriber::set_default(subscriber);
    compare.compare("m", &raw(), &[99, 1, 0]).await;
    assert!(
        exposition(&registry)
            .contains(r#"kinference_tokenize_compare_total{result="divergence"} 1"#)
    );
    let logs = String::from_utf8(buf.0.lock().expect("log buffer").clone()).expect("utf-8");
    assert!(logs.contains("local tokens differ"), "{logs}");
    assert!(!logs.contains("hello") && !logs.contains("world"), "{logs}");
}
