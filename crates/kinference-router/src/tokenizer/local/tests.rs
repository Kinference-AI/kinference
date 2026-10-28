use super::*;
use crate::tokenizer::test_support::WORDLEVEL_TOKENIZER_JSON;

fn render(
    template: &str,
    messages: &[serde_json::Value],
    tools: &[serde_json::Value],
    add_generation_prompt: bool,
) -> Result<String, Error> {
    render_chat_template(
        template,
        messages,
        &HashMap::new(),
        tools,
        None,
        add_generation_prompt,
        &HashMap::new(),
    )
}

/// Same as `render`, with a special-token map (e.g.
/// `{"bos_token": "<bos>"}`).
fn render_with_tokens(
    template: &str,
    messages: &[serde_json::Value],
    special_tokens: &HashMap<String, String>,
) -> Result<String, Error> {
    render_chat_template(
        template,
        messages,
        &HashMap::new(),
        &[],
        None,
        true,
        special_tokens,
    )
}

#[test]
fn render_template_with_strftime_now_returns_today() {
    // Llama 3.1+ templates emit a date line via this global.
    let got = render("{{ strftime_now('%Y') }}", &[], &[], true)
        .expect("strftime_now should render without error");
    let expected_year = chrono::Utc::now().format("%Y").to_string();
    assert_eq!(
        got, expected_year,
        "strftime_now('%Y') should match today's UTC year"
    );
}

#[test]
fn render_template_with_tools_includes_tool_block() {
    let tmpl = "{% if tools %}HAS_TOOLS{% else %}NO_TOOLS{% endif %}";
    // Empty tools render as `null`, which is falsy.
    let no_tools = render(tmpl, &[], &[], true).expect("empty tools renders");
    assert_eq!(no_tools, "NO_TOOLS");
    // One tool dict is truthy, so the tool block is emitted.
    let with_tools = render(
        tmpl,
        &[],
        &[serde_json::json!({"type": "function", "function": {"name": "foo"}})],
        true,
    )
    .expect("tools-present renders");
    assert_eq!(with_tools, "HAS_TOOLS");
}

#[test]
fn render_template_with_loop_controls_parses() {
    // `{% break %}` / `{% continue %}` need minijinja's `loop_controls`
    // feature; Mistral and some Hermes templates use them.
    let tmpl = "{% for m in messages %}\
                    {% if loop.first %}{% continue %}{% endif %}\
                    X\
                    {% endfor %}";
    let messages: Vec<serde_json::Value> = (0..3).map(|i| serde_json::json!({"i": i})).collect();
    let got = render(tmpl, &messages, &[], true).expect("loop_controls feature should be on");
    assert_eq!(got, "XX", "first iteration `continue`s, two X's printed");
}

#[test]
fn render_template_with_tojson_filter_renders() {
    // `tojson` needs minijinja's `json` feature; Llama 3.2 vision and
    // others use it for `tools`.
    let tools = vec![serde_json::json!({"name": "foo"})];
    let got =
        render("{{ tools | tojson }}", &[], &tools, true).expect("tojson filter should be on");
    assert!(
        got.contains("\"name\""),
        "tojson should serialize tools as JSON: got {got}"
    );
    assert!(got.contains("\"foo\""), "got {got}");
}

#[test]
fn render_template_respects_add_generation_prompt_false() {
    let tmpl = "A{% if add_generation_prompt %}B{% endif %}";
    let on = render(tmpl, &[], &[], true).expect("agp=true renders");
    let off = render(tmpl, &[], &[], false).expect("agp=false renders");
    assert_eq!(on, "AB");
    assert_eq!(off, "A");
}

/// Smoke: the Gemma 4 chat template (macros, namespaces, `is mapping` /
/// `is string` / `is sequence`, `dictsort`, `.get()`, `loop.index0`) renders
/// without error.
///
/// Fixture committed verbatim from
/// `https://huggingface.co/google/gemma-4-12B-it/resolve/main/chat_template.jinja`.
#[test]
fn render_gemma4_template_against_minimal_chat() {
    let template = include_str!("fixtures/gemma4_chat_template.jinja");
    let messages = vec![serde_json::json!({"role": "user", "content": "Hello, world!"})];
    // Bind the special tokens as the registry would.
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let rendered = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        true,
        &tokens,
    )
    .expect("Gemma 4 chat template should render without errors");
    // The user message text must appear.
    assert!(
        rendered.contains("Hello, world!"),
        "rendered output should contain the user message text; got: {rendered}"
    );
    // And the bound bos_token comes first, as in vLLM's /render.
    assert!(
        rendered.starts_with("<bos>"),
        "rendered output must start with bos_token; got: {}",
        &rendered[..rendered.len().min(80)]
    );
}

/// `{{- bos_token -}}` renders empty without a binding, and as `<bos>` at
/// index 0 with one.
#[test]
fn render_binds_special_tokens_into_context() {
    let tmpl = "{{- bos_token -}}HELLO";
    let no_bind = render_with_tokens(tmpl, &[], &HashMap::new())
        .expect("render should succeed without binding");
    assert_eq!(
        no_bind, "HELLO",
        "unbound bos_token must render as empty (bug we're fixing)"
    );

    let mut bound = HashMap::new();
    bound.insert("bos_token".to_string(), "<bos>".to_string());
    let with_bind =
        render_with_tokens(tmpl, &[], &bound).expect("render should succeed with binding");
    assert_eq!(
        with_bind, "<bos>HELLO",
        "bound bos_token must appear at the start"
    );
}

#[test]
fn extract_special_tokens_pulls_string_and_dict_shapes() {
    // Gemma-style: every `*_token` is a bare string at top level.
    let g = serde_json::json!({
        "bos_token": "<bos>",
        "eos_token": "<eos>",
        "image_token": "<|image|>",
        "tokenizer_class": "GemmaTokenizer",  // should be ignored — no `_token` suffix
        "some_other_field": "ignored",
    });
    let m = extract_special_tokens(&g);
    assert_eq!(m.get("bos_token").map(String::as_str), Some("<bos>"));
    assert_eq!(m.get("eos_token").map(String::as_str), Some("<eos>"));
    assert_eq!(m.get("image_token").map(String::as_str), Some("<|image|>"));
    assert!(!m.contains_key("tokenizer_class"));
    assert!(!m.contains_key("some_other_field"));

    // Llama 3.2 style: each `*_token` is a dict with a `content` field.
    let l = serde_json::json!({
        "bos_token": {
            "content": "<|begin_of_text|>",
            "lstrip": false,
            "normalized": false,
            "rstrip": false,
            "single_word": false,
        },
        "eos_token": {"content": "<|eot_id|>"},
    });
    let m = extract_special_tokens(&l);
    assert_eq!(
        m.get("bos_token").map(String::as_str),
        Some("<|begin_of_text|>")
    );
    assert_eq!(m.get("eos_token").map(String::as_str), Some("<|eot_id|>"));
}

/// Mount path: when `tokenizer_config.json` has no `chat_template` field,
/// the template comes from a sibling `chat_template.jinja`.
#[test]
fn read_chat_template_falls_back_to_sibling_jinja_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    // tokenizer_config.json without a `chat_template` field, like Gemma 4's.
    std::fs::write(
        dir.path().join("tokenizer_config.json"),
        r#"{"bos_token": "<bos>", "eos_token": "<eos>"}"#,
    )
    .expect("write config");
    // Sibling chat_template.jinja
    std::fs::write(
        dir.path().join("chat_template.jinja"),
        "{{ messages[0]['content'] }}",
    )
    .expect("write template");
    let meta =
        read_chat_template_from_config(dir.path()).expect("should succeed; either source is fine");
    assert_eq!(
        meta.chat_template.as_deref(),
        Some("{{ messages[0]['content'] }}"),
        "sibling file body should be returned"
    );
    // Special tokens come from tokenizer_config.json regardless of
    // which template source hits.
    assert_eq!(
        meta.special_tokens.get("bos_token").map(String::as_str),
        Some("<bos>")
    );
    assert_eq!(
        meta.special_tokens.get("eos_token").map(String::as_str),
        Some("<eos>")
    );
}

#[test]
fn read_chat_template_prefers_inline_over_sibling() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Both sources present: inline wins.
    std::fs::write(
        dir.path().join("tokenizer_config.json"),
        r#"{"chat_template": "INLINE"}"#,
    )
    .expect("write config");
    std::fs::write(dir.path().join("chat_template.jinja"), "SIBLING").expect("write template");
    let meta = read_chat_template_from_config(dir.path()).expect("should succeed");
    assert_eq!(
        meta.chat_template.as_deref(),
        Some("INLINE"),
        "inline tokenizer_config field should win"
    );
}

#[test]
fn read_chat_template_returns_none_when_neither_source_has_template() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("tokenizer_config.json"),
        r#"{"bos_token": "<bos>"}"#,
    )
    .expect("write config");
    // No sibling chat_template.jinja
    let meta =
        read_chat_template_from_config(dir.path()).expect("should succeed even with no template");
    assert!(
        meta.chat_template.is_none(),
        "no template anywhere → chat_template: None, got {:?}",
        meta.chat_template
    );
    // Special tokens still come back even when template is missing.
    assert_eq!(
        meta.special_tokens.get("bos_token").map(String::as_str),
        Some("<bos>")
    );
}

/// Golden render: byte-exact output for canonical Gemma 4 chat-completions
/// shapes, captured from `render_chat_template` with `bos_token` and
/// `eos_token` bound. Any drift in the render path or fixture fails these.
#[test]
fn render_gemma4_golden_minimal() {
    let template = include_str!("fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let messages = vec![serde_json::json!({"role": "user", "content": "Hello, world!"})];
    let got = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        true,
        &tokens,
    )
    .unwrap();
    let want =
        "<bos><|turn>user\nHello, world!<turn|>\n<|turn>model\n<|channel>thought\n<channel|>";
    assert_eq!(got, want, "minimal chat render drift");
}

#[test]
fn render_gemma4_golden_multi_turn_with_system() {
    let template = include_str!("fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let messages = vec![
        serde_json::json!({"role": "system", "content": "Be concise."}),
        serde_json::json!({"role": "user", "content": "Q1"}),
        serde_json::json!({"role": "assistant", "content": "A1"}),
        serde_json::json!({"role": "user", "content": "Q2"}),
    ];
    let got = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        true,
        &tokens,
    )
    .unwrap();
    // The trailing `' '` after "Be concise." comes from the template's
    // sequence branch (`| trim + ' '`), taken because content is
    // normalized to parts as vLLM does.
    let want = "<bos><|turn>system\nBe concise. <turn|>\n<|turn>user\nQ1<turn|>\n<|turn>model\nA1<turn|>\n<|turn>user\nQ2<turn|>\n<|turn>model\n<|channel>thought\n<channel|>";
    assert_eq!(got, want, "multi-turn-with-system render drift");
}

#[test]
fn normalize_message_content_wraps_string_into_text_part() {
    // String content is wrapped into a parts list, as vLLM does.
    let msg = serde_json::json!({"role": "system", "content": "Be concise."});
    let got = normalize_message_content(&msg);
    let want = serde_json::json!({
        "role": "system",
        "content": [{"type": "text", "text": "Be concise."}],
    });
    assert_eq!(got, want);
}

#[test]
fn normalize_message_content_leaves_already_multipart_unchanged() {
    // Already a parts list: unchanged.
    let msg = serde_json::json!({
        "role": "user",
        "content": [{"type": "text", "text": "hi"}, {"type": "image"}],
    });
    let got = normalize_message_content(&msg);
    assert_eq!(got, msg);
}

#[test]
fn normalize_message_content_leaves_missing_content_unchanged() {
    // Messages may omit `content` (e.g. assistant turns with only
    // `tool_calls`); leave it absent so template guards still work.
    let msg = serde_json::json!({
        "role": "assistant",
        "tool_calls": [{"id": "x", "function": {"name": "f"}}],
    });
    let got = normalize_message_content(&msg);
    assert_eq!(got, msg);
}

#[test]
fn render_gemma4_golden_add_generation_prompt_false() {
    let template = include_str!("fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let messages = vec![serde_json::json!({"role": "user", "content": "Hi"})];
    let got = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        false, // ← AGP=false: no model-turn primer appended
        &tokens,
    )
    .unwrap();
    let want = "<bos><|turn>user\nHi<turn|>\n";
    assert_eq!(got, want, "AGP=false render drift");
}

/// Encoding a rendered template must use `add_special_tokens = false`, or the
/// tokenizer's post-processor adds a second BOS.
///
/// Uses a synthetic tokenizer (WordLevel vocab, a post-processor that
/// prepends `<bos>` as id 99) since Gemma's is gated.
#[test]
fn encode_with_add_special_tokens_false_avoids_postprocessor_bos() {
    let tok: Tokenizer = WORDLEVEL_TOKENIZER_JSON
        .parse()
        .expect("parse synthetic tokenizer spec");

    // Rendered text already includes the literal <bos>.
    let rendered = "<bos> hello world";

    let with_true = tok.encode(rendered, true).expect("encode true");
    let with_false = tok.encode(rendered, false).expect("encode false");

    let true_ids = with_true.get_ids();
    let false_ids = with_false.get_ids();
    // With `false`, <bos> appears exactly once.
    assert_eq!(
        false_ids.first().copied(),
        Some(99),
        "false-branch must start with the template's own <bos>; got {false_ids:?}"
    );
    assert_eq!(
        false_ids.iter().filter(|&&id| id == 99).count(),
        1,
        "false-branch must contain <bos> exactly once; got {false_ids:?}"
    );
    // With `true`, the post-processor prepends another.
    assert!(
        true_ids.len() > false_ids.len(),
        "true-branch should be strictly longer (post-processor added something); \
             true={true_ids:?} false={false_ids:?}"
    );
    assert_eq!(
        true_ids.iter().filter(|&&id| id == 99).count(),
        2,
        "true-branch should have <bos> twice (template's + post-processor's); \
             this is the bug we're guarding against. got {true_ids:?}"
    );
}

/// The Gemma 4 fixture against system, multi-turn and tools shapes, which
/// exercise the tools preamble macros, assistant tool calls and the
/// multi-turn loop.
#[test]
fn render_gemma4_template_against_multi_turn_with_tools() {
    let template = include_str!("fixtures/gemma4_chat_template.jinja");
    let messages = vec![
        serde_json::json!({"role": "system", "content": "You are a helpful assistant."}),
        serde_json::json!({"role": "user", "content": "What's the weather in SF?"}),
        serde_json::json!({"role": "assistant", "content": "Let me check."}),
        serde_json::json!({"role": "user", "content": "Thanks."}),
    ];
    let tools = vec![serde_json::json!({
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get current weather for a city.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "City name"},
                },
                "required": ["city"],
            },
        },
    })];
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let rendered = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &tools,
        None,
        true,
        &tokens,
    )
    .expect("Gemma 4 template should render with system + tools + multi-turn");
    assert!(rendered.starts_with("<bos>"), "must start with bos_token");
    assert!(rendered.contains("You are a helpful assistant"));
    assert!(rendered.contains("What's the weather in SF?"));
    assert!(rendered.contains("Thanks."));
    assert!(
        rendered.contains("get_weather"),
        "tools should be rendered into the prompt; got: {rendered}"
    );
}

/// Diagnostic probe: render two consecutive turns of one conversation and
/// print where they first differ. Appending messages should keep a long
/// common prefix, or prefix-cache reuse can't work across turns.
///
/// Marked #[ignore] so it doesn't run on every test invocation;
/// invoke with: `cargo test --release render_diff_across_turns -- --ignored --nocapture`
#[test]
#[ignore = "diagnostic: prints render diff between consecutive turns"]
fn render_diff_across_turns() {
    let template = include_str!("fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());

    // Two consecutive turns of a captured conversation.
    let c0t0 = vec![
        serde_json::json!({"role":"system","content":"amplitude node moment subscribe not embedding and config"}),
        serde_json::json!({"role":"user","content":"today test actor output with async broker read"}),
    ];
    let c0t1 = vec![
        serde_json::json!({"role":"system","content":"amplitude node moment subscribe not embedding and config"}),
        serde_json::json!({"role":"user","content":"today test actor output with async broker read"}),
        serde_json::json!({"role":"assistant","content":"only queue other black graph compare could small"}),
        serde_json::json!({"role":"user","content":"time south first open plot day warn go"}),
    ];
    let c0t2 = vec![
        serde_json::json!({"role":"system","content":"amplitude node moment subscribe not embedding and config"}),
        serde_json::json!({"role":"user","content":"today test actor output with async broker read"}),
        serde_json::json!({"role":"assistant","content":"only queue other black graph compare could small"}),
        serde_json::json!({"role":"user","content":"time south first open plot day warn go"}),
        serde_json::json!({"role":"assistant","content":"debug offset scalar two first length pipe warn"}),
        serde_json::json!({"role":"user","content":"east send task option iteration array system kind"}),
    ];

    let r0 =
        render_chat_template(template, &c0t0, &HashMap::new(), &[], None, true, &tokens).unwrap();
    let r1 =
        render_chat_template(template, &c0t1, &HashMap::new(), &[], None, true, &tokens).unwrap();
    let r2 =
        render_chat_template(template, &c0t2, &HashMap::new(), &[], None, true, &tokens).unwrap();

    eprintln!();
    eprintln!("=== c0t0 rendered ({} bytes) ===", r0.len());
    eprintln!("{r0:?}");
    eprintln!();
    eprintln!("=== c0t1 rendered ({} bytes) ===", r1.len());
    eprintln!("{r1:?}");
    eprintln!();
    eprintln!("=== c0t2 rendered ({} bytes) ===", r2.len());
    eprintln!("{r2:?}");
    eprintln!();

    fn diff_at(a: &str, b: &str, label: &str) {
        let ab = a.as_bytes();
        let bb = b.as_bytes();
        let n = ab.len().min(bb.len());
        let mut i = 0;
        while i < n && ab[i] == bb[i] {
            i += 1;
        }
        eprintln!("=== {label}: first divergence at byte {i} / shared_prefix_len={i} ===");
        let lo = i.saturating_sub(40);
        let hi_a = (i + 40).min(ab.len());
        let hi_b = (i + 40).min(bb.len());
        eprintln!("  shared prefix tail (last 40 bytes before divergence):");
        eprintln!(
            "    {:?}",
            std::str::from_utf8(&ab[lo..i]).unwrap_or("<utf8>")
        );
        eprintln!("  A continues with:");
        eprintln!(
            "    {:?}",
            std::str::from_utf8(&ab[i..hi_a]).unwrap_or("<utf8>")
        );
        eprintln!("  B continues with:");
        eprintln!(
            "    {:?}",
            std::str::from_utf8(&bb[i..hi_b]).unwrap_or("<utf8>")
        );
        eprintln!();
    }

    diff_at(&r0, &r1, "c0t0 vs c0t1");
    diff_at(&r1, &r2, "c0t1 vs c0t2");
}

/// Only a render-mode pool skips warm-up; an unknown pool is warmed.
#[test]
fn warmup_skips_only_render_mode_pools() {
    let process = tokio_util::sync::CancellationToken::new();
    let mut spec = kinference_fleet::pool::test_spec("a", 16);
    for (mode, warm) in [
        (TokenizeMode::Render, false),
        (TokenizeMode::Local, true),
        (TokenizeMode::Compare, true),
    ] {
        spec.tokenize_mode = mode;
        let pool = Arc::new(Pool::from_spec(
            kinference_policy::PoolId::next(),
            &spec,
            &process,
        ));
        assert_eq!(should_warm(Some(&pool)), warm, "{mode:?}");
    }
    assert!(should_warm(None));
}

// --- the backend, over a mounted tokenizer ----------------------------------

use crate::tokenizer::TokenizeOutcome;
use crate::tokenizer::http::HttpRender;
use crate::tokenizer::test_support::{
    FakeRender, Render, WORDLEVEL_CHAT_TEMPLATE as TEMPLATE, chat_input as chat, exposition,
    mount_wordlevel, request, router_metrics, worker,
};
use kinference_fleet::scrape::MetricsRegistry;

/// A backend over a mount holding model `m` with `template`, and the
/// registry its metrics land in.
fn local_over(dir: &Path, template: Option<&str>) -> (LocalTokenizer, prometheus::Registry) {
    mount_wordlevel(dir, "m", template);
    let (registry, prom) = router_metrics();
    let http = Arc::new(HttpRender::new(16, Arc::new(MetricsRegistry::new())).expect("http"));
    let config = LocalTokenizeConfig {
        mount_dir: Some(dir.to_path_buf()),
        warmup: false,
        hub_token: None,
    };
    let local = LocalTokenizer::new(config, http, prom).expect("a local backend");
    (local, registry)
}

/// A raw prompt encodes from the mount, with the post-processor's `<bos>`.
#[tokio::test]
async fn raw_input_encodes_from_the_mount() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (local, registry) = local_over(dir.path(), Some(TEMPLATE));

    let out = local
        .encode_or_render(&request(&[], "m", TokenizeInput::Raw("hello world".into())))
        .await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![99, 0, 1]));
    assert!(exposition(&registry).contains(r#"kinference_tokenize_total{path="local"} 1"#));
}

/// Chat input renders the mounted template, which emits `<bos>` itself.
#[tokio::test]
async fn chat_input_renders_the_mounted_template() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (local, _) = local_over(dir.path(), Some(TEMPLATE));

    let out = local
        .encode_or_render(&request(&[], "m", chat("hello world")))
        .await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![99, 0, 1]));
}

/// A model without a chat template cannot render chat locally, so the
/// request goes to `/render` and the fallback is counted.
#[tokio::test]
async fn a_model_without_a_chat_template_falls_back_to_render() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (local, registry) = local_over(dir.path(), None);
    let fake = FakeRender::start(Render::Tokens(vec![7, 8])).await;
    let workers = [(
        1,
        worker("m", "10.0.0.1:8000", true, Some(fake.base().to_string())),
    )];

    let out = local
        .encode_or_render(&request(&workers, "m", chat("hello world")))
        .await;
    assert_eq!(out, TokenizeOutcome::Ok(vec![7, 8]));
    assert_eq!(fake.calls(), 1);
    let out = exposition(&registry);
    assert!(
        out.contains(r#"kinference_tokenize_total{path="http_fallback"} 1"#),
        "{out}"
    );
    assert!(
        out.contains(r#"kinference_tokenize_total{path="local"} 0"#),
        "{out}"
    );
    assert!(
        local.warn_latch.contains_key("m"),
        "warned once for the model"
    );
}

/// When the fallback fails too, nothing is counted as a success.
#[tokio::test]
async fn a_double_failure_is_unavailable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (local, registry) = local_over(dir.path(), None);
    let fake = FakeRender::start(Render::Failing).await;
    let workers = [(
        1,
        worker("m", "10.0.0.1:8000", true, Some(fake.base().to_string())),
    )];

    let out = local
        .encode_or_render(&request(&workers, "m", chat("hello world")))
        .await;
    assert!(
        matches!(out, TokenizeOutcome::Unavailable { .. }),
        "{out:?}"
    );
    assert!(exposition(&registry).contains(r#"kinference_tokenize_total{path="http_fallback"} 0"#));
}

/// Warm-up loads the model so a later decode needs no load.
#[tokio::test]
async fn warmup_loads_the_model() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (local, _) = local_over(dir.path(), Some(TEMPLATE));

    assert!(local.decode("m", &[0, 1]).is_none(), "not loaded yet");
    local.warmup("m").await.expect("warm-up");
    assert_eq!(local.decode("m", &[0, 1]).as_deref(), Some("hello world"));
}
