//! The `/render` backend: vLLM's `/v1/chat/completions/render` runs the same
//! preprocessing as `/v1/chat/completions` (tools, multimodal placeholders,
//! system-prompt defaulting, reasoning prefixes, `add_generation_prompt`)
//! and stops before inference, so the token ids match what the engine
//! hashes into its `BlockStored` events.
//!
//! A raw request body is forwarded verbatim, so fields kinference doesn't
//! know about still reach vLLM; without one, a body is built from the
//! [`TokenizeInput`].

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use futures::future::BoxFuture;

use super::{ChatTemplateKwargs, TokenizeInput, TokenizeOutcome, TokenizeRequest, Tokenizer};
use crate::error::Error;
use kinference_fleet::scrape::MetricsRegistry;

/// Maximum number of workers tried in sequence, freshest metrics first, so
/// an outage doesn't try every worker on every request.
const FALLBACK_MAX_ATTEMPTS: usize = 3;

/// Wall-clock cap on the whole attempt loop. The client timeout bounds each
/// attempt. When it elapses, scoring degrades.
const TOTAL_DEADLINE: Duration = Duration::from_secs(5);

/// Tokenization over `/render`: a dedicated URL tried once, else the pool's
/// workers in turn, freshest scrape first, at most three, within one
/// deadline.
pub(crate) struct HttpRender {
    client: reqwest::Client,
    /// Scrape ages order the workers: a fresh scrape is a free liveness
    /// proxy.
    scrape: Arc<MetricsRegistry>,
}

impl HttpRender {
    /// One client for every pool, so connections pool.
    ///
    /// Settings mirror the Gateway API Inference Extension EPP's. vLLM speaks
    /// HTTP/1.1 only, so force it; without multiplexing that means several
    /// connections per host (`pool_max_idle_per_host`; raise to peak
    /// concurrency with a single `render_url`). `pool_idle_timeout = 90s`
    /// matches the EPP and AWS NLB idle tolerance; `tcp_keepalive` detects
    /// half-closed connections.
    pub(crate) fn new(
        pool_max_idle_per_host: usize,
        scrape: Arc<MetricsRegistry>,
    ) -> Result<Self, Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(pool_max_idle_per_host)
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .http1_only()
            .build()
            .map_err(|source| Error::HttpClient {
                what: "the tokenize HTTP client",
                source,
            })?;
        Ok(Self { client, scrape })
    }

    /// `/render` with retries across workers. Forwards `raw_body` verbatim
    /// when present, otherwise a body built from `input`. Workers serving one
    /// model share its tokenizer, so any of them returns the same tokens.
    pub(crate) async fn render(&self, req: &TokenizeRequest<'_>) -> TokenizeOutcome {
        let &TokenizeRequest {
            render_url,
            workers,
            model,
            ref input,
            raw_body,
            ..
        } = req;
        // A configured `render_url` (typically a Service) replaces the
        // per-worker rotation, keeping warm connections to one host. It fails
        // fast rather than falling back to individual workers.
        if let Some(url) = render_url {
            let started = std::time::Instant::now();
            let result = match raw_body {
                Some(body) => render_post_raw_at_url(&self.client, url, body).await,
                None => render_structured_at_url(&self.client, url, model, input).await,
            };
            let render_ms = started.elapsed().as_secs_f64() * 1000.0;
            match result {
                Ok(tokens) => {
                    tracing::trace!(
                        mode = "render",
                        tokenize_url = %url,
                        attempt = 1,
                        render_ms = format_args!("{render_ms:.1}"),
                        token_count = tokens.len(),
                        "render ok"
                    );
                    return TokenizeOutcome::Ok(tokens);
                }
                Err(e) => {
                    let err = e.chain();
                    tracing::warn!(
                        tokenize_url = url,
                        render_ms = format_args!("{render_ms:.1}"),
                        error = %err,
                        "render via the worker set's render_url failed; failing fast (no per-worker fallback)"
                    );
                    return TokenizeOutcome::Unavailable {
                        reason: format!("render_url={url} failed: {err}"),
                        attempts: 1,
                    };
                }
            }
        }

        // Filter to workers with an http_url; carry worker_id_str + url
        // for log context.
        let mut candidates: Vec<(u64, String, String)> = workers
            .iter()
            .filter_map(|(num, w)| {
                w.http_url
                    .as_deref()
                    .map(|u| (*num, w.worker_id_str.clone(), u.to_string()))
            })
            .collect();
        if candidates.is_empty() {
            return TokenizeOutcome::Unavailable {
                reason: "no worker has http_url configured".to_string(),
                attempts: 0,
            };
        }
        // Freshest metrics scrape first.
        candidates.sort_by_key(|(num, _, _)| self.scrape.age_ms_for(*num));
        candidates.truncate(FALLBACK_MAX_ATTEMPTS);
        let attempts = candidates.len();

        // The client timeout caps each attempt; the deadline caps the loop.
        let do_attempts = async {
            let mut last_err: Option<String> = None;
            for (i, (_num, worker_id_str, url)) in candidates.iter().enumerate() {
                // Timed per call, logged at TRACE.
                let render_start = std::time::Instant::now();
                let result = match raw_body {
                    Some(body) => render_via_vllm_raw(&self.client, url, body).await,
                    None => render_via_vllm_structured(&self.client, url, model, input).await,
                };
                let render_ms = render_start.elapsed().as_secs_f64() * 1000.0;
                match result {
                    Ok(tokens) => {
                        tracing::trace!(
                            mode = "render",
                            http_url = %url,
                            attempt = i + 1,
                            render_ms = format_args!("{render_ms:.1}"),
                            token_count = tokens.len(),
                            "render ok"
                        );
                        return Ok(tokens);
                    }
                    Err(e) => {
                        let err_string = e.chain();
                        tracing::debug!(
                            worker_id = %worker_id_str,
                            http_url = %url,
                            attempt = i + 1,
                            of_attempts = attempts,
                            render_ms = format_args!("{render_ms:.1}"),
                            error = %err_string,
                            "render failed; falling back to next worker"
                        );
                        last_err = Some(err_string);
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| "no attempts ran".to_string()))
        };

        match tokio::time::timeout(TOTAL_DEADLINE, do_attempts).await {
            Ok(Ok(tokens)) => TokenizeOutcome::Ok(tokens),
            Ok(Err(reason)) => TokenizeOutcome::Unavailable { reason, attempts },
            Err(_) => TokenizeOutcome::Unavailable {
                reason: format!("total deadline of {}s elapsed", TOTAL_DEADLINE.as_secs()),
                attempts,
            },
        }
    }
}

impl Tokenizer for HttpRender {
    fn tokenize<'a>(&'a self, req: TokenizeRequest<'a>) -> BoxFuture<'a, TokenizeOutcome> {
        Box::pin(async move { self.render(&req).await })
    }
}

/// vLLM `/render` returns a `GenerateRequest` object (chat) or an
/// array of `GenerateRequest` (completions). We only consume
/// `token_ids`; the rest (`request_id`, `sampling_params`, `features`,
/// `model`, `stream`, …) is ignored.
#[derive(Deserialize)]
struct RenderResponseObject {
    token_ids: Vec<u32>,
}

/// Build the chat-style `/render` request body from a structured
/// `TokenizeInput::Chat`.
fn build_chat_render_body(
    model: &str,
    messages: &[serde_json::Value],
    kwargs: &ChatTemplateKwargs,
    tools: &[serde_json::Value],
    tool_choice: Option<&serde_json::Value>,
    add_generation_prompt: bool,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "model": model,
        "messages": messages,
        "add_generation_prompt": add_generation_prompt,
    });
    if !tools.is_empty() {
        body["tools"] = serde_json::Value::Array(tools.to_vec());
    }
    if let Some(tc) = tool_choice {
        body["tool_choice"] = tc.clone();
    }
    if !kwargs.is_empty() {
        let mut kw_obj = serde_json::Map::with_capacity(kwargs.len());
        for (k, v) in kwargs.iter() {
            // JSON-parse the value, falling back to a literal string.
            let parsed =
                serde_json::from_str(v).unwrap_or_else(|_| serde_json::Value::String(v.clone()));
            kw_obj.insert(k.clone(), parsed);
        }
        body["chat_template_kwargs"] = serde_json::Value::Object(kw_obj);
    }
    body
}

/// Parse vLLM's `/render` response into a token-id list: a single object for
/// chat, an array for completions.
fn parse_render_response(json: &[u8]) -> Result<Vec<u32>, Error> {
    let decode = |what: &'static str| move |source| Error::RenderDecode { what, source };
    let value: serde_json::Value =
        serde_json::from_slice(json).map_err(decode("decoding /render response as JSON"))?;
    let obj: RenderResponseObject = match value {
        serde_json::Value::Object(_) => serde_json::from_value(value)
            .map_err(decode("/render response missing `token_ids` field"))?,
        serde_json::Value::Array(mut arr) => {
            let first = arr
                .drain(..)
                .next()
                .ok_or_else(|| Error::RenderShape("/render returned an empty array".into()))?;
            serde_json::from_value(first)
                .map_err(decode("/render response[0] missing `token_ids` field"))?
        }
        other => {
            return Err(Error::RenderShape(format!(
                "/render response was neither object nor array: {other}"
            )));
        }
    };
    Ok(obj.token_ids)
}

/// POST `raw_body` verbatim to `{base}/v1/chat/completions/render`.
async fn render_via_vllm_raw(
    client: &reqwest::Client,
    base: &str,
    raw_body: &[u8],
) -> Result<Vec<u32>, Error> {
    let url = format!("{}/v1/chat/completions/render", base.trim_end_matches('/'));
    render_post_raw_at_url(client, &url, raw_body).await
}

/// POST `raw_body` verbatim to a fully-qualified `/render` URL (a worker
/// set's `render_url`), without appending a path.
async fn render_post_raw_at_url(
    client: &reqwest::Client,
    url: &str,
    raw_body: &[u8],
) -> Result<Vec<u32>, Error> {
    let render = |source| Error::Render {
        url: url.to_string(),
        source,
    };
    let resp = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(raw_body.to_vec())
        .send()
        .await
        .map_err(render)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::RenderStatus {
            status,
            body: body.trim().to_string(),
        });
    }
    let bytes = resp.bytes().await.map_err(render)?;
    parse_render_response(&bytes)
}

/// [`render_via_vllm_structured`] for a fully-qualified `url`, which must be
/// the right endpoint for the input's chat or completion variant.
async fn render_structured_at_url(
    client: &reqwest::Client,
    url: &str,
    model: &str,
    input: &TokenizeInput,
) -> Result<Vec<u32>, Error> {
    let render = |source| Error::Render {
        url: url.to_string(),
        source,
    };
    let body = match input {
        TokenizeInput::Raw(prompt) => serde_json::json!({
            "model": model,
            "prompt": prompt,
        }),
        TokenizeInput::Chat {
            messages,
            kwargs,
            tools,
            tool_choice,
            add_generation_prompt,
        } => build_chat_render_body(
            model,
            messages,
            kwargs,
            tools,
            tool_choice.as_ref(),
            *add_generation_prompt,
        ),
    };
    let resp = client.post(url).json(&body).send().await.map_err(render)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::RenderStatus {
            status,
            body: body.trim().to_string(),
        });
    }
    let bytes = resp.bytes().await.map_err(render)?;
    parse_render_response(&bytes)
}

/// Build a structured body from `input` and POST it to vLLM's
/// `/render` endpoint that matches the variant
/// (`/v1/chat/completions/render` for `Chat`, `/v1/completions/render`
/// for `Raw`). For callers without a raw request body.
async fn render_via_vllm_structured(
    client: &reqwest::Client,
    base: &str,
    model: &str,
    input: &TokenizeInput,
) -> Result<Vec<u32>, Error> {
    let base = base.trim_end_matches('/');
    let (url, body) = match input {
        TokenizeInput::Raw(prompt) => (
            format!("{base}/v1/completions/render"),
            serde_json::json!({
                "model": model,
                "prompt": prompt,
            }),
        ),
        TokenizeInput::Chat {
            messages,
            kwargs,
            tools,
            tool_choice,
            add_generation_prompt,
        } => (
            format!("{base}/v1/chat/completions/render"),
            build_chat_render_body(
                model,
                messages,
                kwargs,
                tools,
                tool_choice.as_ref(),
                *add_generation_prompt,
            ),
        ),
    };
    let render = |source| Error::Render {
        url: url.clone(),
        source,
    };
    let resp = client.post(&url).json(&body).send().await.map_err(render)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::RenderStatus {
            status,
            body: body.trim().to_string(),
        });
    }
    let bytes = resp.bytes().await.map_err(render)?;
    parse_render_response(&bytes)
}

#[cfg(test)]
mod tests;
