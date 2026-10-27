//! The compare backend: `/render` is returned; the local result is only
//! compared against it, for the divergence counters.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::http::HttpRender;
use super::local::LocalTokenizer;
use super::{TokenizeInput, TokenizeOutcome, TokenizeRequest, Tokenizer};
use crate::metrics::RouterMetrics;
use crate::warn_once::WarnOnce;

#[derive(Clone)]
pub(crate) struct Compare {
    http: Arc<HttpRender>,
    local: Arc<LocalTokenizer>,
    prom: Arc<RouterMetrics>,
    /// Models that have diverged, so each logs its first divergence only.
    diverged: Arc<WarnOnce<String>>,
    /// Models that have failed locally, so each logs its first failure only.
    failed: Arc<WarnOnce<String>>,
}

impl Compare {
    pub(crate) fn new(
        http: Arc<HttpRender>,
        local: Arc<LocalTokenizer>,
        prom: Arc<RouterMetrics>,
    ) -> Self {
        Self {
            http,
            local,
            prom,
            diverged: Arc::default(),
            failed: Arc::default(),
        }
    }

    /// `/render`, then the comparison in the background so local template
    /// rendering doesn't add to pick latency.
    pub(crate) async fn render_then_compare(&self, req: &TokenizeRequest<'_>) -> TokenizeOutcome {
        let outcome = self.http.render(req).await;
        if let TokenizeOutcome::Ok(http_tokens) = &outcome {
            let this = self.clone();
            let model = req.model.to_string();
            let input = req.input.clone();
            let http_tokens = http_tokens.clone();
            tokio::spawn(async move {
                this.compare(&model, &input, &http_tokens).await;
            });
        }
        outcome
    }

    /// Tokenize in process and count and log divergence from the `/render`
    /// tokens. Errors are counted, never returned.
    pub(crate) async fn compare(&self, model: &str, input: &TokenizeInput, http_tokens: &[u32]) {
        match self.local.encode(model, input).await {
            Ok(local_tokens) if local_tokens.as_slice() == http_tokens => {
                self.prom.record_tokenize_compare("match");
            }
            Ok(local_tokens) => {
                self.prom.record_tokenize_compare("divergence");
                if !self.diverged.first(model) {
                    return;
                }
                let first_diff = http_tokens
                    .iter()
                    .zip(local_tokens.iter())
                    .position(|(a, b)| a != b)
                    .unwrap_or_else(|| http_tokens.len().min(local_tokens.len()));
                // 8 tokens before to 16 after the divergence, on both sides.
                let win_start = first_diff.saturating_sub(8);
                let http_win: Vec<u32> = http_tokens
                    .iter()
                    .skip(win_start)
                    .take(24)
                    .copied()
                    .collect();
                let local_win: Vec<u32> = local_tokens
                    .iter()
                    .skip(win_start)
                    .take(24)
                    .copied()
                    .collect();
                let http_text = self
                    .local
                    .decode(model, &http_win)
                    .unwrap_or_else(|| "<decode-unavailable>".to_string());
                let local_text = self
                    .local
                    .decode(model, &local_win)
                    .unwrap_or_else(|| "<decode-unavailable>".to_string());
                tracing::warn!(
                    model,
                    http_count = http_tokens.len(),
                    local_count = local_tokens.len(),
                    first_diff_index = first_diff,
                    window_start = win_start,
                    http_window = ?http_win,
                    local_window = ?local_win,
                    http_text = %http_text,
                    local_text = %local_text,
                    "local tokens differ from /render"
                );
            }
            Err(e) => {
                self.prom.record_tokenize_compare("error");
                if self.failed.first(model) {
                    tracing::warn!(
                        model,
                        error = &e as &dyn std::error::Error,
                        "local tokenize failed"
                    );
                }
            }
        }
    }
}

impl Tokenizer for Compare {
    fn tokenize<'a>(&'a self, req: TokenizeRequest<'a>) -> BoxFuture<'a, TokenizeOutcome> {
        Box::pin(async move { self.render_then_compare(&req).await })
    }
}

#[cfg(test)]
mod tests;
