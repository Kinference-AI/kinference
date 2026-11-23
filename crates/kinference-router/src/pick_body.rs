//! Pick-time rewrites of the request body.
//!
//! `PullBody()` serves a (possibly rewritten) body that the caller forwards
//! upstream in place of the one it sent. Two rewrites live here:
//!
//! 1. The resolved model: `auto` (see `choose_model_for_auto`) or a declared
//!    alias becomes the concrete model, which the engine requires.
//! 2. Streaming usage: for `stream=true`, vLLM only emits the final `usage`
//!    block, which `ReportFinalToken` reports, when
//!    `stream_options.include_usage=true`, so the flag is forced on. Chat
//!    completions only: a Messages stream always reports usage.
//!
//! The rewrite works on the already-parsed body and reports whether anything
//! changed, so an unchanged body is reused rather than re-serialized.

use kinference_policy::ApiCall;
use serde_json::Value;

/// Rewrite just the `model` field of an already-serialized body.
///
/// For when a policy picks a different model after scoring, by which point the
/// parsed body passed to [`apply_pick_body_mutations`] no longer holds
/// `messages`. Returns `None`, leaving the body alone, if the bytes are not a
/// JSON object.
pub(crate) fn rewrite_model(body: &[u8], model: &str) -> Option<Vec<u8>> {
    let mut obj: Value = serde_json::from_slice(body).ok()?;
    obj.as_object_mut()?
        .insert("model".to_string(), Value::String(model.to_string()));
    serde_json::to_vec(&obj).ok()
}

/// Whether the body asks for a streamed response, read the way the engine reads
/// it.
///
/// vLLM and SGLang use Pydantic's lax bool, which coerces `"true"`, `"yes"`,
/// `"on"`, `"t"`, `"y"` and `1` to `true` (other numbers are rejected, so stay
/// false here). Plain `as_bool` would treat `"stream": "true"` as
/// non-streaming while the engine streams. Shared by
/// [`apply_pick_body_mutations`] and `service::lifecycle` so the two agree.
pub(crate) fn stream_flag(obj: &Value) -> bool {
    match obj.get("stream") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "1" | "on" | "t" | "true" | "y" | "yes"
        ),
        Some(Value::Number(n)) => n.as_f64() == Some(1.0),
        _ => false,
    }
}

/// Apply the Pick-time body rewrites to `obj` in place.
///
/// `force_model` is the concrete model when the client's model was `auto` or
/// an alias; otherwise `None`.
///
/// Returns `true` if `obj` was modified, so the caller knows whether to
/// re-serialize. A non-object body is left unchanged; the caller reports it as
/// a bad request.
pub(crate) fn apply_pick_body_mutations(
    obj: &mut Value,
    force_model: Option<&str>,
    api_call: ApiCall,
) -> bool {
    // Read before taking the &mut borrow below.
    let streaming = api_call == ApiCall::ChatCompletions && stream_flag(obj);
    let Some(map) = obj.as_object_mut() else {
        return false;
    };
    let mut mutated = false;

    // (1) Rewrite the model when it was resolved to a concrete one.
    if let Some(model) = force_model {
        map.insert("model".to_string(), Value::String(model.to_string()));
        mutated = true;
    }

    // (2) For streaming chat completions, ensure stream_options.include_usage=true so
    //     the response carries the final usage block ReportFinalToken reports.
    if streaming {
        // Ensure `stream_options` exists AND is an object. `entry` avoids a
        // second borrow of `map`; coercing a malformed value in place (rather
        // than a separate `map.insert`) keeps a single live borrow.
        let so = map
            .entry("stream_options")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if !so.is_object() {
            *so = Value::Object(serde_json::Map::new());
        }
        if let Value::Object(so) = so
            && so.get("include_usage").and_then(Value::as_bool) != Some(true)
        {
            so.insert("include_usage".to_string(), Value::Bool(true));
            mutated = true;
        }
    }

    mutated
}

#[cfg(test)]
mod tests;
