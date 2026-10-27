//! Fakes for the tokenizer tests: a `/render` on a loopback port and the
//! worker entries that point at it.

use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::metrics::RouterMetrics;

use super::{TokenizeInput, TokenizeRequest};
use kinference_fleet::profile::TokenizeMode;
use kinference_fleet::workers::WorkerEntry;

/// What the fake `/render` answers.
#[derive(Clone)]
pub(crate) enum Render {
    /// Every request tokenizes to these ids.
    Tokens(Vec<u32>),
    /// Every request gets a 500.
    Failing,
    /// No worker has an `http_url`, so nothing is tried.
    Absent,
}

/// One request the fake `/render` was sent.
#[derive(Clone)]
pub(crate) struct Seen {
    pub path: String,
    pub body: Vec<u8>,
}

/// A `/render` on a loopback port that gives every request the same answer
/// and remembers what it was asked. Stops when dropped.
pub(crate) struct FakeRender {
    base: String,
    requests: Arc<Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeRender {
    pub(crate) async fn start(answer: Render) -> Self {
        use axum::response::IntoResponse as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        // Any path: a worker is POSTed `{http_url}/v1/chat/completions/render`.
        let app =
            axum::Router::new().fallback(move |uri: axum::http::Uri, body: axum::body::Bytes| {
                seen.lock().expect("requests").push(Seen {
                    path: uri.path().to_string(),
                    body: body.to_vec(),
                });
                let answer = answer.clone();
                async move {
                    match answer {
                        Render::Tokens(ids) => {
                            axum::Json(serde_json::json!({ "token_ids": ids })).into_response()
                        }
                        Render::Failing => {
                            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "render down")
                                .into_response()
                        }
                        Render::Absent => unreachable!("no server is started for Absent"),
                    }
                }
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve the fake /render");
        });
        Self {
            base,
            requests,
            task,
        }
    }

    /// The `http_url` a worker needs to reach this fake.
    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    pub(crate) fn calls(&self) -> usize {
        self.requests.lock().expect("requests").len()
    }

    /// Every request so far, oldest first.
    pub(crate) fn requests(&self) -> Vec<Seen> {
        self.requests.lock().expect("requests").clone()
    }
}

impl Drop for FakeRender {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A worker serving `model` at `addr`, tokenizing at `http_url`.
pub(crate) fn worker(
    model: &str,
    addr: &str,
    ready: bool,
    http_url: Option<String>,
) -> WorkerEntry {
    WorkerEntry {
        worker_id_str: addr.into(),
        model_name_hint: Some(model.into()),
        current_models: vec![model.into()],
        http_url,
        block_size: 16,
        initial_sync_complete: ready,
        kv_events_observed: true,
        score_addr: addr.into(),
        engine: None,
        pod_name: None,
        namespace: None,
        node_name: None,
        node_ip: None,
        pod_ip: None,
        availability_zone: None,
        instance_type: None,
    }
}

/// A Render-mode request over `workers` with no dedicated URL, raw body or
/// request id.
pub(crate) fn request<'a>(
    workers: &'a [(u64, WorkerEntry)],
    model: &'a str,
    input: TokenizeInput,
) -> TokenizeRequest<'a> {
    TokenizeRequest {
        mode: TokenizeMode::Render,
        render_url: None,
        workers,
        model,
        input,
        raw_body: None,
    }
}

/// A `tokenizer.json`: WordLevel on whitespace with `hello` 0, `world` 1 and
/// `<bos>` 99, which a post-processor prepends.
pub(crate) const WORDLEVEL_TOKENIZER_JSON: &str = r#"{
    "version": "1.0",
    "truncation": null,
    "padding": null,
    "added_tokens": [
        {"id": 99, "content": "<bos>", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true}
    ],
    "normalizer": null,
    "pre_tokenizer": {"type": "Whitespace"},
    "post_processor": {
        "type": "TemplateProcessing",
        "single": [
            {"SpecialToken": {"id": "<bos>", "type_id": 0}},
            {"Sequence": {"id": "A", "type_id": 0}}
        ],
        "pair": [
            {"Sequence": {"id": "A", "type_id": 0}},
            {"Sequence": {"id": "B", "type_id": 1}}
        ],
        "special_tokens": {
            "<bos>": {"id": "<bos>", "ids": [99], "tokens": ["<bos>"]}
        }
    },
    "decoder": null,
    "model": {
        "type": "WordLevel",
        "vocab": {"hello": 0, "world": 1, "<unk>": 2, "<bos>": 99},
        "unk_token": "<unk>"
    }
}"#;

/// Lay out `model` under `mount_dir` with the WordLevel tokenizer and a
/// `tokenizer_config.json` binding `bos_token`, plus `chat_template` when
/// given.
pub(crate) fn mount_wordlevel(mount_dir: &Path, model: &str, chat_template: Option<&str>) {
    let dir = mount_dir.join(model);
    std::fs::create_dir_all(&dir).expect("model dir");
    std::fs::write(dir.join("tokenizer.json"), WORDLEVEL_TOKENIZER_JSON).expect("tokenizer.json");
    let mut config = serde_json::json!({ "bos_token": "<bos>" });
    if let Some(template) = chat_template {
        config["chat_template"] = serde_json::Value::String(template.into());
    }
    std::fs::write(dir.join("tokenizer_config.json"), config.to_string())
        .expect("tokenizer_config.json");
}

/// A fresh registry and the router metrics registered in it.
pub(crate) fn router_metrics() -> (prometheus::Registry, Arc<RouterMetrics>) {
    let registry = prometheus::Registry::new();
    let prom = Arc::new(RouterMetrics::new(&registry, &Default::default()));
    (registry, prom)
}

/// The registry in Prometheus text form.
pub(crate) fn exposition(registry: &prometheus::Registry) -> String {
    use prometheus::Encoder as _;
    let mut buf = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&registry.gather(), &mut buf)
        .expect("encode the registry");
    String::from_utf8(buf).expect("utf-8 exposition")
}

/// A chat template over the WordLevel tokenizer. Reads the normalized
/// content parts, as vLLM's templates do, and emits `<bos>` itself.
pub(crate) const WORDLEVEL_CHAT_TEMPLATE: &str =
    "{{ bos_token }} {% for m in messages %}{{ m['content'][0]['text'] }}{% endfor %}";

/// One user message, with no tools or kwargs.
pub(crate) fn chat_input(text: &str) -> TokenizeInput {
    TokenizeInput::Chat {
        messages: vec![serde_json::json!({"role": "user", "content": text})],
        kwargs: Default::default(),
        tools: Vec::new(),
        tool_choice: None,
        add_generation_prompt: true,
    }
}
