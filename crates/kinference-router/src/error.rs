//! The router's error type.

use std::path::PathBuf;

/// Why a router operation failed. `Display` says what failed; the cause is
/// reachable through `source`.
#[derive(Debug)]
pub enum Error {
    /// Building a reqwest client; `what` names it.
    HttpClient {
        what: &'static str,
        source: reqwest::Error,
    },
    /// A `/render` request failed in flight, or its body could not be read.
    Render { url: String, source: reqwest::Error },
    /// `/render` answered with a non-2xx status. The body is dropped because
    /// it may echo the prompt.
    RenderStatus { status: reqwest::StatusCode },
    /// A `/render` body is not the JSON expected; `what` names the step.
    RenderDecode {
        what: &'static str,
        source: serde_json::Error,
    },
    /// A `/render` body parsed but has the wrong shape.
    RenderShape(String),
    /// `tokenizer.json` on the mount did not load.
    TokenizerFile {
        path: PathBuf,
        source: tokenizers::Error,
    },
    /// A Hub tokenizer did not load.
    TokenizerHub {
        model: String,
        source: tokenizers::Error,
    },
    /// A blocking tokenizer load was cancelled or panicked.
    Join(tokio::task::JoinError),
    /// `encode` rejected the input.
    Encode {
        model: String,
        source: tokenizers::Error,
    },
    /// Chat input for a model whose tokenizer has no chat template.
    NoChatTemplate { model: String },
    /// A chat template failed to parse or render; `op` names which.
    Template {
        op: &'static str,
        source: minijinja::Error,
    },
    /// `tokenizer_config.json` is missing from the mount.
    MissingFile(PathBuf),
    /// A mount file could not be read.
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// A mount file is not JSON.
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    /// A Hub GET failed in flight, or its body could not be read or parsed.
    Hub { url: String, source: reqwest::Error },
    /// The Hub answered with a non-2xx status.
    HubStatus {
        url: String,
        status: reqwest::StatusCode,
    },
}

impl Error {
    /// The error and its causes, joined by ": ", for the `Unavailable`
    /// reason a tokenize outcome carries.
    pub(crate) fn chain(&self) -> String {
        let mut out = self.to_string();
        let mut cause = std::error::Error::source(self);
        while let Some(e) = cause {
            out.push_str(": ");
            out.push_str(&e.to_string());
            cause = e.source();
        }
        out
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::HttpClient { what, .. } => write!(f, "building {what}"),
            Error::Render { url, .. } => write!(f, "POST {url}"),
            Error::RenderStatus { status } => write!(f, "/render returned {status}"),
            Error::RenderDecode { what, .. } => f.write_str(what),
            Error::RenderShape(what) => f.write_str(what),
            Error::TokenizerFile { path, .. } => {
                write!(f, "Tokenizer::from_file({})", path.display())
            }
            Error::TokenizerHub { model, .. } => write!(f, "Tokenizer::from_pretrained({model})"),
            Error::Join(_) => f.write_str("spawn_blocking join"),
            Error::Encode { model, .. } => write!(f, "tokenizer encode failed for {model}"),
            Error::NoChatTemplate { model } => write!(
                f,
                "model {model} has no chat_template in tokenizer_config.json; can't render \
                 chat-style input locally"
            ),
            Error::Template { op, .. } => write!(f, "minijinja: failed to {op} chat template"),
            Error::MissingFile(path) => write!(f, "{} does not exist", path.display()),
            Error::Io { path, .. } => write!(f, "reading {}", path.display()),
            Error::Json { path, .. } => write!(f, "parsing {} as JSON", path.display()),
            Error::Hub { url, .. } => write!(f, "GET {url}"),
            Error::HubStatus { url, status } => write!(f, "HTTP {status} fetching {url}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::HttpClient { source, .. } => Some(source),
            Error::Render { source, .. } => Some(source),
            Error::RenderStatus { .. }
            | Error::RenderShape(_)
            | Error::NoChatTemplate { .. }
            | Error::MissingFile(_)
            | Error::HubStatus { .. } => None,
            Error::RenderDecode { source, .. } => Some(source),
            Error::TokenizerFile { source, .. } => Some(&**source),
            Error::TokenizerHub { source, .. } => Some(&**source),
            Error::Join(source) => Some(source),
            Error::Encode { source, .. } => Some(&**source),
            Error::Template { source, .. } => Some(source),
            Error::Io { source, .. } => Some(source),
            Error::Json { source, .. } => Some(source),
            Error::Hub { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests;
