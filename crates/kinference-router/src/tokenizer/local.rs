//! In-process tokenization via the HuggingFace `tokenizers` Rust crate.
//!
//! Used by pools whose profile sets tokenize mode `local` or `compare`; the
//! mode dispatch is [`crate::tokenizer::ByMode`] and the compare
//! harness [`crate::tokenizer::compare`].
//!
//! ## Loading model tokenizers
//!
//! 1. Mount path: `{mount_dir}/{model_name}/tokenizer.json`, plus
//!    `tokenizer_config.json` for the chat template. Works in air-gapped
//!    clusters.
//! 2. HuggingFace Hub fallback: `Tokenizer::from_pretrained`, plus
//!    `tokenizer_config.json` fetched via reqwest since the crate doesn't
//!    expose it. Honors `HUGGING_FACE_HUB_TOKEN` for gated models.
//!
//! Tokenizers are cached by model name for the process lifetime (sub-MB to
//! ~10MB each). A model whose load fails is not loaded again for a minute.
//! Until then, its requests fall back to `/render` without a load.
//!
//! ## Chat-template rendering
//!
//! The chat template is rendered with `minijinja` using the same context as
//! Python `transformers`: `messages`, `add_generation_prompt = true`, and any
//! `chat_template_kwargs` (e.g. Qwen3's `enable_thinking`). The result is
//! encoded with `add_special_tokens = false`, since the template emits them.
//!
//! The template is read inline from `tokenizer_config.json`'s
//! `chat_template` field (Llama 3.x, Mistral, Qwen 2.5), else from a sibling
//! `chat_template.jinja` (Gemma 3/4, recent multimodal builds).
//!
//! Minijinja gets `loop_controls`, `json` (for `tojson`), and
//! `minijinja-contrib`'s `pycompat` callback for the Python dict methods
//! Gemma templates call. `raise_exception` and `strftime_now` (Llama 3.1+)
//! are registered as globals.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use serde::Deserialize;
use tokenizers::tokenizer::Tokenizer;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

use super::http::HttpRender;
use futures::future::BoxFuture;

use super::{TokenizeInput, TokenizeOutcome, TokenizeRequest};
use crate::error::Error;
use crate::metrics::RouterMetrics;
use crate::warn_once::WarnOnce;
use kinference_fleet::models::ModelClaim;
use kinference_fleet::pool::{Pool, PoolRegistry};
use kinference_fleet::profile::TokenizeMode;

/// `tokenize.local`. See `tokenizer::local`.
#[derive(Debug, Clone, Deserialize)]
pub struct LocalTokenizeConfig {
    /// Directory checked for tokenizer files before fetching from the Hub:
    /// `{mount_dir}/{model_name}/{tokenizer,tokenizer_config}.json`. A `/` in
    /// the model name is a nested directory.
    #[serde(default)]
    pub mount_dir: Option<std::path::PathBuf>,
    /// Load tokenizers for newly discovered models in the background instead
    /// of on first request.
    #[serde(default = "default_warmup")]
    pub warmup: bool,
    /// `HUGGING_FACE_HUB_TOKEN`, read once at startup by `Config::load`:
    /// bearer auth for Hub fetches of gated models. Not a file key.
    #[serde(skip)]
    pub hub_token: Option<String>,
}

impl Default for LocalTokenizeConfig {
    fn default() -> Self {
        Self {
            mount_dir: None,
            warmup: default_warmup(),
            hub_token: None,
        }
    }
}

fn default_warmup() -> bool {
    true
}

/// A render-mode pool never uses the local tokenizer. A pool missing from the
/// snapshot is warmed anyway: it may be published a beat after its first
/// claim, and an extra load is harmless.
fn should_warm(pool: Option<&Arc<Pool>>) -> bool {
    pool.is_none_or(|p| !matches!(p.tokenize_mode, TokenizeMode::Render))
}

/// Cached, loaded artifacts for one model. Cheap to `Arc`-share.
struct LoadedTokenizer {
    tokenizer: Tokenizer,
    /// Jinja2 chat template string, if present (either inline in
    /// `tokenizer_config.json` or as a sibling `chat_template.jinja`).
    /// `None` for tokenizer-only models that don't publish a template
    /// (chat-mode requests for such a model will error from
    /// `LocalTokenizer::encode`).
    chat_template: Option<String>,
    /// Special-token strings bound into the Jinja render context (see
    /// [`extract_special_tokens`]). Without them, `{{- bos_token -}}`
    /// renders empty and the special token is silently dropped.
    special_tokens: HashMap<String, String>,
}

/// Two-source lookup result for a model's chat template + the
/// special-token bindings the template's render needs. Returned by
/// the mount and Hub loaders.
struct TokenizerConfigMeta {
    chat_template: Option<String>,
    special_tokens: HashMap<String, String>,
}

/// How long after a failed load the next request may load the model again.
const LOAD_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Most models with a slot. Model names can come from the client, so past
/// this a failed load leaves no slot and is not backed off.
const MAX_SLOTS: usize = 1024;

/// A model's tokenizer, or why a request can't have it yet.
enum Slot {
    Loaded(Arc<LoadedTokenizer>),
    /// One caller is loading it. Others fall back instead of loading too.
    Loading,
    /// The last load failed. No caller loads it again before `retry_at`.
    Failed {
        retry_at: Instant,
    },
}

/// In-process tokenization, falling back to `/render` on any local failure.
/// Owns the loaded tokenizers, lazily loaded per model, and their warm-up.
pub struct LocalTokenizer {
    slots: DashMap<String, Slot>,
    retry_after: Duration,
    config: LocalTokenizeConfig,
    hub: reqwest::Client,
    /// Models that fail locally, so each logs once.
    failed: WarnOnce<String>,
    http: Arc<HttpRender>,
    prom: Arc<RouterMetrics>,
}

impl LocalTokenizer {
    /// Build an empty backend. Models are loaded on first use.
    pub(crate) fn new(
        config: LocalTokenizeConfig,
        http: Arc<HttpRender>,
        prom: Arc<RouterMetrics>,
    ) -> Result<Self, Error> {
        let hub = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|source| Error::HttpClient {
                what: "the local-tokenize Hub client",
                source,
            })?;
        Ok(Self {
            slots: DashMap::new(),
            retry_after: LOAD_RETRY_AFTER,
            config,
            hub,
            failed: WarnOnce::default(),
            http,
            prom,
        })
    }

    /// Load a tokenizer for each new model claim whose pool uses the local
    /// tokenizer, until `cancel`. Each load runs in its own task, so a slow
    /// Hub fetch does not hold up the stream.
    pub fn spawn_warmup(
        self: Arc<Self>,
        pools: Arc<PoolRegistry>,
        mut claims: broadcast::Receiver<ModelClaim>,
        cancel: CancellationToken,
    ) {
        tokio::spawn(async move {
            loop {
                let claim = tokio::select! {
                    _ = cancel.cancelled() => return,
                    next = claims.recv() => match next {
                        Ok(claim) => claim,
                        Err(RecvError::Lagged(missed)) => {
                            tracing::warn!(
                                missed,
                                "tokenizer warmup fell behind"
                            );
                            continue;
                        }
                        Err(RecvError::Closed) => return,
                    },
                };
                if !should_warm(pools.snapshot().get(claim.pool)) {
                    continue;
                }
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(err) = this.warmup(&claim.model).await {
                        tracing::warn!(
                            model = %claim.model,
                            error = &err as &dyn StdError,
                            "tokenizer warmup failed"
                        );
                    }
                });
            }
        });
    }

    /// `encode`, else `/render`. Any local failure (unknown model, missing
    /// chat template, render or encode error) falls back.
    pub(crate) async fn encode_or_render(&self, req: &TokenizeRequest<'_>) -> TokenizeOutcome {
        let &TokenizeRequest {
            model, ref input, ..
        } = req;
        let started = std::time::Instant::now();
        let result = self.encode(model, input).await;
        let render_ms = started.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(tokens) => {
                self.prom.record_tokenize_path("local");
                tracing::trace!(
                    mode = "local",
                    model = %model,
                    render_ms = format_args!("{render_ms:.1}"),
                    token_count = tokens.len(),
                    "render ok"
                );
                TokenizeOutcome::Ok(tokens)
            }
            Err(e) => {
                if self.failed.first(model) {
                    tracing::warn!(
                        model,
                        error = &e as &dyn StdError,
                        "local tokenize failed; using /render"
                    );
                }
                let outcome = self.http.render(req).await;
                // A double failure is counted by the dispatcher.
                if matches!(outcome, TokenizeOutcome::Ok(_)) {
                    self.prom.record_tokenize_path("http_fallback");
                }
                outcome
            }
        }
    }

    /// Load `model_name`'s tokenizer into the cache, so requests find it
    /// warm. A load that is running or backing off counts as warm. The
    /// caller logs errors and moves on.
    pub async fn warmup(&self, model_name: &str) -> Result<(), Error> {
        match self.get_or_load(model_name).await {
            Ok(_) | Err(Error::TokenizerLoading { .. } | Error::TokenizerBackoff { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Tokenize `input` against `model_name`'s tokenizer, in process only.
    ///
    /// Errors when the tokenizer can't be loaded, chat input is given for a
    /// model with no `chat_template`, the template fails to render, or
    /// `encode` rejects the input.
    pub async fn encode(&self, model_name: &str, input: &TokenizeInput) -> Result<Vec<u32>, Error> {
        let loaded = self.get_or_load(model_name).await?;
        let encode = |source| Error::Encode {
            model: model_name.to_string(),
            source,
        };
        match input {
            TokenizeInput::Raw(prompt) => {
                let encoding = loaded
                    .tokenizer
                    .encode(prompt.as_str(), /* add_special_tokens */ true)
                    .map_err(encode)?;
                Ok(encoding.get_ids().to_vec())
            }
            TokenizeInput::Chat {
                messages,
                kwargs,
                tools,
                tool_choice,
                add_generation_prompt,
            } => {
                let template =
                    loaded
                        .chat_template
                        .as_deref()
                        .ok_or_else(|| Error::NoChatTemplate {
                            model: model_name.to_string(),
                        })?;
                let rendered = render_chat_template(
                    template,
                    messages,
                    kwargs,
                    tools,
                    tool_choice.as_ref(),
                    *add_generation_prompt,
                    &loaded.special_tokens,
                )?;
                let encoding = loaded
                    .tokenizer
                    // The template emits the special tokens itself.
                    .encode(rendered.as_str(), /* add_special_tokens */ false)
                    .map_err(encode)?;
                Ok(encoding.get_ids().to_vec())
            }
        }
    }

    /// Return the cached tokenizer, or load it (possibly from the network).
    /// At most one load per model runs at a time, and a failed load is not
    /// retried for `retry_after`. Meanwhile callers get an error at once.
    async fn get_or_load(&self, model_name: &str) -> Result<Arc<LoadedTokenizer>, Error> {
        if let Some(slot) = self.slots.get(model_name)
            && let Some(ready) = slot_state(&slot, model_name)
        {
            return ready;
        }
        match self.slots.entry(model_name.to_string()) {
            Entry::Occupied(mut slot) => {
                if let Some(ready) = slot_state(slot.get(), model_name) {
                    return ready;
                }
                slot.insert(Slot::Loading);
            }
            Entry::Vacant(slot) => {
                slot.insert(Slot::Loading);
            }
        }
        let mut claim = LoadClaim {
            slots: &self.slots,
            model: model_name,
            settled: false,
        };
        let result = self.load(model_name).await;
        match &result {
            Ok(loaded) => claim.settle(Slot::Loaded(loaded.clone())),
            Err(err) => {
                tracing::debug!(
                    model = model_name,
                    error = err as &dyn StdError,
                    "tokenizer load failed"
                );
                claim.settle(Slot::Failed {
                    retry_at: Instant::now() + self.retry_after,
                });
            }
        }
        result
    }

    async fn load(&self, model_name: &str) -> Result<Arc<LoadedTokenizer>, Error> {
        // The mount first. File reads block, so they run off the runtime.
        if let Some(mount_dir) = self.config.mount_dir.clone() {
            let model = model_name.to_string();
            let mounted = tokio::task::spawn_blocking(move || {
                load_from_mount(&mount_dir.join(&model), &model)
            })
            .await
            .map_err(Error::Join)??;
            if let Some(loaded) = mounted {
                tracing::info!(model = model_name, source = "mount", "tokenizer loaded");
                return Ok(Arc::new(loaded));
            }
        }

        // Fallback: HuggingFace Hub, on `spawn_blocking` since
        // `from_pretrained` blocks.
        let model_name_owned = model_name.to_string();
        let params = tokenizers::FromPretrainedParameters {
            token: self.config.hub_token.clone(),
            ..Default::default()
        };
        let tokenizer = tokio::task::spawn_blocking(move || {
            Tokenizer::from_pretrained(&model_name_owned, Some(params)).map_err(|source| {
                Error::TokenizerHub {
                    model: model_name_owned.clone(),
                    source,
                }
            })
        })
        .await
        .map_err(Error::Join)??;

        let meta =
            fetch_chat_template_from_hub(&self.hub, self.config.hub_token.as_deref(), model_name)
                .await
                .unwrap_or_else(|e| {
                    tracing::debug!(
                        model = model_name,
                        error = &e as &dyn StdError,
                        "no chat template on the Hub"
                    );
                    TokenizerConfigMeta {
                        chat_template: None,
                        special_tokens: HashMap::new(),
                    }
                });
        tracing::info!(model = model_name, source = "hub", "tokenizer loaded");
        Ok(Arc::new(LoadedTokenizer {
            tokenizer,
            chat_template: meta.chat_template,
            special_tokens: meta.special_tokens,
        }))
    }
}

/// What a request gets from `slot` without loading: the tokenizer, or an
/// error while a load runs or backs off. `None` when the caller should load.
fn slot_state(slot: &Slot, model_name: &str) -> Option<Result<Arc<LoadedTokenizer>, Error>> {
    let model = || model_name.to_string();
    match slot {
        Slot::Loaded(loaded) => Some(Ok(loaded.clone())),
        Slot::Loading => Some(Err(Error::TokenizerLoading { model: model() })),
        Slot::Failed { retry_at } if Instant::now() < *retry_at => {
            Some(Err(Error::TokenizerBackoff { model: model() }))
        }
        Slot::Failed { .. } => None,
    }
}

/// The right to load one model. Dropped unsettled, as when the request is
/// cancelled mid-load, it clears the slot so that the next request loads.
struct LoadClaim<'a> {
    slots: &'a DashMap<String, Slot>,
    model: &'a str,
    settled: bool,
}

impl LoadClaim<'_> {
    fn settle(&mut self, slot: Slot) {
        self.settled = true;
        if matches!(slot, Slot::Failed { .. }) && self.slots.len() > MAX_SLOTS {
            self.slots.remove(self.model);
            return;
        }
        self.slots.insert(self.model.to_string(), slot);
    }
}

impl Drop for LoadClaim<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.slots.remove(self.model);
        }
    }
}

/// Load `model_name` from its mount directory `dir`. `None` when the mount
/// has no `tokenizer.json` for it. Blocks on file reads.
fn load_from_mount(dir: &Path, model_name: &str) -> Result<Option<LoadedTokenizer>, Error> {
    let tok_path = dir.join("tokenizer.json");
    if !tok_path.exists() {
        return Ok(None);
    }
    let tokenizer = Tokenizer::from_file(&tok_path).map_err(|source| Error::TokenizerFile {
        path: tok_path.clone(),
        source,
    })?;
    let meta = read_chat_template_from_config(dir).unwrap_or_else(|e| {
        tracing::debug!(
            model = model_name,
            error = &e as &dyn StdError,
            "no chat template in the mount"
        );
        TokenizerConfigMeta {
            chat_template: None,
            special_tokens: HashMap::new(),
        }
    });
    Ok(Some(LoadedTokenizer {
        tokenizer,
        chat_template: meta.chat_template,
        special_tokens: meta.special_tokens,
    }))
}

// The HuggingFace type takes the short name here.
impl super::Tokenizer for LocalTokenizer {
    fn tokenize<'a>(&'a self, req: TokenizeRequest<'a>) -> BoxFuture<'a, TokenizeOutcome> {
        Box::pin(async move { self.encode_or_render(&req).await })
    }
}

/// Render a Jinja2 chat template with the same context Python's
/// `transformers.apply_chat_template` uses: `messages`,
/// `add_generation_prompt = true`, plus any caller-supplied
/// `chat_template_kwargs` (e.g. `enable_thinking=false` for Qwen3).
///
/// Values in `chat_kwargs` are JSON-parsed (`"false"` becomes `false`),
/// falling back to literal strings, as the HTTP body builder in
/// `tokenizer::http` does, so both render the same prompt.
fn render_chat_template(
    template: &str,
    messages: &[serde_json::Value],
    chat_kwargs: &HashMap<String, String>,
    tools: &[serde_json::Value],
    tool_choice: Option<&serde_json::Value>,
    add_generation_prompt: bool,
    special_tokens: &HashMap<String, String>,
) -> Result<String, Error> {
    let mut ctx = serde_json::Map::new();
    // Special tokens first, so `chat_kwargs` can override them.
    for (k, v) in special_tokens {
        ctx.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    // Normalize `content` to content-parts form, as vLLM does; see
    // `normalize_message_content`.
    let normalized_messages: Vec<serde_json::Value> =
        messages.iter().map(normalize_message_content).collect();
    ctx.insert(
        "messages".to_string(),
        serde_json::Value::Array(normalized_messages),
    );
    ctx.insert(
        "add_generation_prompt".to_string(),
        serde_json::Value::Bool(add_generation_prompt),
    );
    // `null` when absent, as transformers passes `None`.
    let tools_value = if tools.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::Array(tools.to_vec())
    };
    ctx.insert("tools".to_string(), tools_value);
    ctx.insert(
        "tool_choice".to_string(),
        tool_choice.cloned().unwrap_or(serde_json::Value::Null),
    );
    for (k, v) in chat_kwargs {
        let parsed = serde_json::from_str::<serde_json::Value>(v)
            .unwrap_or_else(|_| serde_json::Value::String(v.clone()));
        ctx.insert(k.clone(), parsed);
    }
    let mut env = minijinja::Environment::new();
    // Python dict methods (`.get()`, `.items()`, ...) that templates such
    // as Gemma's call on messages.
    env.set_unknown_method_callback(minijinja_contrib::pycompat::unknown_method_callback);
    // Used by some templates to validate message shape.
    env.add_function(
        "raise_exception",
        |msg: String| -> Result<(), minijinja::Error> {
            Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                msg,
            ))
        },
    );
    // Llama 3.1+ templates emit a date via `strftime_now('%d %b %Y')`,
    // which transformers provides.
    env.add_function(
        "strftime_now",
        |format: String| -> Result<String, minijinja::Error> {
            Ok(chrono::Utc::now().format(&format).to_string())
        },
    );
    let tmpl = env
        .template_from_str(template)
        .map_err(|source| Error::Template {
            op: "parse",
            source,
        })?;
    tmpl.render(serde_json::Value::Object(ctx))
        .map_err(|source| Error::Template {
            op: "render",
            source,
        })
}

/// Normalize one OpenAI-style chat message so its `content` field is
/// the multimodal content-parts list form (`[{"type":"text","text":...}]`),
/// matching what vLLM's `_preprocess_chat` does before invoking the
/// chat template.
///
/// Templates that branch on whether `content` is a string (e.g. Gemma 4's
/// system block) would otherwise differ from vLLM by a token, breaking the
/// block hashes. A message whose `content` is already a list, or missing, is
/// returned unchanged.
fn normalize_message_content(message: &serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(map) = message else {
        return message.clone();
    };
    let Some(content) = map.get("content") else {
        return message.clone();
    };
    if !content.is_string() {
        return message.clone();
    }
    let text = content.as_str().unwrap_or("").to_string();
    let mut out = map.clone();
    out.insert(
        "content".to_string(),
        serde_json::json!([{"type": "text", "text": text}]),
    );
    serde_json::Value::Object(out)
}

/// Read the chat template + special-token bindings for a mounted
/// model directory.
///
/// The template is the inline `chat_template` field, else a sibling
/// `chat_template.jinja` (see the module docs); `None` if neither exists.
/// Special tokens are always extracted. `Err` only if
/// `tokenizer_config.json` is missing or unparseable.
fn read_chat_template_from_config(model_dir: &Path) -> Result<TokenizerConfigMeta, Error> {
    let cfg_path = model_dir.join("tokenizer_config.json");
    if !cfg_path.exists() {
        return Err(Error::MissingFile(cfg_path));
    }
    let body = std::fs::read_to_string(&cfg_path).map_err(|source| Error::Io {
        path: cfg_path.clone(),
        source,
    })?;
    let value: serde_json::Value = serde_json::from_str(&body).map_err(|source| Error::Json {
        path: cfg_path.clone(),
        source,
    })?;
    let special_tokens = extract_special_tokens(&value);
    if let Some(inline) = value.get("chat_template").and_then(|v| v.as_str()) {
        return Ok(TokenizerConfigMeta {
            chat_template: Some(inline.to_string()),
            special_tokens,
        });
    }
    // A missing sibling means no chat template; failing to read one that
    // exists is an error.
    let sibling = model_dir.join("chat_template.jinja");
    if !sibling.exists() {
        return Ok(TokenizerConfigMeta {
            chat_template: None,
            special_tokens,
        });
    }
    let template = std::fs::read_to_string(&sibling).map_err(|source| Error::Io {
        path: sibling.clone(),
        source,
    })?;
    Ok(TokenizerConfigMeta {
        chat_template: Some(template),
        special_tokens,
    })
}

/// Pull every `*_token` string field from a parsed `tokenizer_config.json`
/// into a flat `name -> literal` map for binding into the Jinja render
/// context, like transformers' `tokenizer.special_tokens_map`.
///
/// A dict value like `{"content": "<bos>", ...}` contributes its `content`;
/// other non-string values are skipped.
fn extract_special_tokens(cfg: &serde_json::Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(obj) = cfg.as_object() else {
        return out;
    };
    for (k, v) in obj {
        if !k.ends_with("_token") {
            continue;
        }
        if let Some(s) = v.as_str() {
            out.insert(k.clone(), s.to_string());
            continue;
        }
        // Some configs (Llama 3.2, others) wrap special tokens in
        // a `{"content": "<token>", ...}` dict. Pull `content` out.
        if let Some(content) = v.get("content").and_then(|c| c.as_str()) {
            out.insert(k.clone(), content.to_string());
        }
    }
    out
}

/// Fetch the chat template + special-token bindings for a model from
/// the HuggingFace Hub, with the same lookup as
/// [`read_chat_template_from_config`]. A 404 for the sibling means no chat
/// template.
async fn fetch_chat_template_from_hub(
    client: &reqwest::Client,
    token: Option<&str>,
    model_name: &str,
) -> Result<TokenizerConfigMeta, Error> {
    // Step 1: tokenizer_config.json (inline template + special tokens).
    let cfg_url = format!("https://huggingface.co/{model_name}/resolve/main/tokenizer_config.json");
    let cfg_resp = hub_get(client, token, &cfg_url).await?;
    if !cfg_resp.status().is_success() {
        return Err(Error::HubStatus {
            url: cfg_url,
            status: cfg_resp.status(),
        });
    }
    let cfg_value: serde_json::Value = cfg_resp.json().await.map_err(|source| Error::Hub {
        url: cfg_url.clone(),
        source,
    })?;
    let special_tokens = extract_special_tokens(&cfg_value);
    if let Some(inline) = cfg_value.get("chat_template").and_then(|v| v.as_str()) {
        return Ok(TokenizerConfigMeta {
            chat_template: Some(inline.to_string()),
            special_tokens,
        });
    }
    // Step 2: sibling chat_template.jinja.
    let sibling_url =
        format!("https://huggingface.co/{model_name}/resolve/main/chat_template.jinja");
    let sib_resp = hub_get(client, token, &sibling_url).await?;
    if sib_resp.status() == reqwest::StatusCode::NOT_FOUND {
        // The repo has no chat template.
        return Ok(TokenizerConfigMeta {
            chat_template: None,
            special_tokens,
        });
    }
    if !sib_resp.status().is_success() {
        return Err(Error::HubStatus {
            url: sibling_url,
            status: sib_resp.status(),
        });
    }
    let body = sib_resp.text().await.map_err(|source| Error::Hub {
        url: sibling_url.clone(),
        source,
    })?;
    Ok(TokenizerConfigMeta {
        chat_template: Some(body),
        special_tokens,
    })
}

/// A Hub GET with optional bearer auth.
async fn hub_get(
    client: &reqwest::Client,
    token: Option<&str>,
    url: &str,
) -> Result<reqwest::Response, Error> {
    let mut req = client.get(url);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    req.send().await.map_err(|source| Error::Hub {
        url: url.to_string(),
        source,
    })
}

#[cfg(test)]
mod tests;
