//! Tokenization for `score_workers`: one [`Tokenizer`] behind which the
//! three modes a pool profile can set live as backends.
//! - `Render`: vLLM's `/render`, rotating across workers ([`http`]).
//! - `Local`: in-process tokenization, falling back to `/render` ([`local`]).
//! - `Compare`: `/render` is authoritative; the local result is compared in
//!   the background ([`compare`]).
//!
//! [`ByMode`] picks the backend by the pool's mode and records latency and
//! failures once for every call.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures::future::BoxFuture;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::metrics::RouterMetrics;
use kinference_fleet::models::ModelIndex;
use kinference_fleet::pool::PoolRegistry;
use kinference_fleet::profile::TokenizeMode;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_fleet::workers::WorkerEntry;

pub mod compare;
pub mod http;
pub mod local;
#[cfg(test)]
pub(crate) mod test_support;

use self::compare::Compare;
use self::http::HttpRender;
use self::local::{LocalTokenizeConfig, LocalTokenizer};

/// `tokenize`: tokenization settings shared by all pools. The mode is set
/// per profile.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TokenizeConfig {
    /// Idle keep-alive connections kept per host. Set it at or above peak
    /// concurrency when all renders go to a single `render_url`. `None` means
    /// 16.
    #[serde(default)]
    pub pool_max_idle_per_host: Option<usize>,
    #[serde(default)]
    pub local: LocalTokenizeConfig,
}

/// Map of `chat_template_kwargs` forwarded into the engine's
/// `/render` body. Values are JSON-encoded: `"false"` becomes a bool,
/// `"1024"` an int, and a non-JSON value like `en` stays a string.
pub type ChatTemplateKwargs = HashMap<String, String>;

/// What to tokenize. Carries everything vLLM's `_preprocess_chat` needs to
/// produce the same token sequence the engine would.
///
/// `Clone` because compare mode re-tokenizes it in a background task.
#[derive(Clone)]
pub enum TokenizeInput {
    /// Raw completion-style prompt → `/v1/completions/render` with
    /// `{ model, prompt }`, with vLLM's default `add_special_tokens=true`.
    Raw(String),
    /// Chat-style messages (full OpenAI array) →
    /// `/v1/chat/completions/render` with `{ model, messages, tools,
    /// tool_choice, add_generation_prompt, chat_template_kwargs }`.
    Chat {
        messages: Vec<serde_json::Value>,
        kwargs: ChatTemplateKwargs,
        /// OpenAI `tools` array (JSON-schema dicts). Empty when absent.
        tools: Vec<serde_json::Value>,
        /// OpenAI `tool_choice` ("auto" | "none" | "required" | {...}).
        /// `None` when unset.
        tool_choice: Option<serde_json::Value>,
        /// OpenAI `add_generation_prompt` (default `true`); `false` for
        /// assistant-prefill continuations.
        add_generation_prompt: bool,
    },
}

/// Result of tokenizing one request. `Unavailable` is not an error: scoring
/// degrades to load-only.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TokenizeOutcome {
    Ok(Vec<u32>),
    Unavailable { reason: String, attempts: usize },
}

/// One request to tokenize, against the model's worker pool.
pub(crate) struct TokenizeRequest<'a> {
    /// The pool's mode. Read by [`ByMode`]; a backend serves one mode.
    pub mode: TokenizeMode,
    /// A dedicated `/render` URL; `None` rotates across `workers`.
    pub render_url: Option<&'a str>,
    pub workers: &'a [(u64, WorkerEntry)],
    pub model: &'a str,
    pub input: TokenizeInput,
    /// The client's body, forwarded verbatim to `/render` when present;
    /// local tokenization uses `input`.
    pub raw_body: Option<&'a [u8]>,
}

/// Turns one request into the token ids the engine will hash.
pub(crate) trait Tokenizer: Send + Sync {
    fn tokenize<'a>(&'a self, req: TokenizeRequest<'a>) -> BoxFuture<'a, TokenizeOutcome>;
}

/// The tokenizer the service holds: every backend built once, chosen per
/// request by the pool's mode. Records the duration and the `unavailable`
/// path once per call, for every mode.
pub(crate) struct ByMode {
    render: Arc<HttpRender>,
    local: Arc<LocalTokenizer>,
    compare: Arc<Compare>,
    prom: Arc<RouterMetrics>,
}

impl ByMode {
    pub(crate) fn new(
        render: Arc<HttpRender>,
        local: Arc<LocalTokenizer>,
        compare: Arc<Compare>,
        prom: Arc<RouterMetrics>,
    ) -> Self {
        Self {
            render,
            local,
            compare,
            prom,
        }
    }
}

impl Tokenizer for ByMode {
    fn tokenize<'a>(&'a self, req: TokenizeRequest<'a>) -> BoxFuture<'a, TokenizeOutcome> {
        Box::pin(async move {
            let started = Instant::now();
            let outcome = match req.mode {
                TokenizeMode::Render => self.render.render(&req).await,
                TokenizeMode::Local => self.local.encode_or_render(&req).await,
                TokenizeMode::Compare => self.compare.render_then_compare(&req).await,
            };
            self.prom
                .record_tokenize_duration(started.elapsed().as_secs_f64() * 1000.0);
            if matches!(outcome, TokenizeOutcome::Unavailable { .. }) {
                // Successful paths are counted where they succeed.
                self.prom.record_tokenize_path("unavailable");
            }
            outcome
        })
    }
}

/// Build the `/render` client and every backend, and start the warm-up
/// when configured. The one call the server makes.
pub(crate) fn build(
    cfg: &TokenizeConfig,
    scrape: Arc<MetricsRegistry>,
    prom: Arc<RouterMetrics>,
    pools: Arc<PoolRegistry>,
    models: &ModelIndex,
    cancel: CancellationToken,
) -> Result<Arc<dyn Tokenizer>, Error> {
    let pool_max_idle_per_host = cfg.pool_max_idle_per_host.unwrap_or(16);
    let http = Arc::new(HttpRender::new(pool_max_idle_per_host, scrape)?);
    // Fleet-wide and cheap to build, so always built; each pool's mode
    // decides whether to use it.
    let local = Arc::new(LocalTokenizer::new(
        cfg.local.clone(),
        http.clone(),
        prom.clone(),
    )?);
    if cfg.local.warmup {
        local
            .clone()
            .spawn_warmup(pools, models.subscribe(), cancel);
    }
    let compare = Arc::new(Compare::new(http.clone(), local.clone(), prom.clone()));
    Ok(Arc::new(ByMode::new(http, local, compare, prom)))
}

#[cfg(test)]
mod tests;
