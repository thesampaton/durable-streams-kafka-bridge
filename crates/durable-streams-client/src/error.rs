use reqwest::StatusCode;
use std::time::Duration;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid durable streams base URL `{url}`")]
    InvalidBaseUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("invalid stream path `{path}`: {reason}")]
    InvalidStreamPath { path: String, reason: String },
    #[error("HTTP transport error during {operation} {path}: {source}")]
    Transport {
        operation: &'static str,
        path: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("{0}")]
    Http(HttpError),
    #[error("missing required header `{name}` on {operation} {path}")]
    MissingHeader {
        operation: &'static str,
        path: String,
        name: &'static str,
    },
    #[error("invalid header `{name}` on {operation} {path}: {reason}")]
    InvalidHeader {
        operation: &'static str,
        path: String,
        name: &'static str,
        reason: String,
    },
    #[error("invalid SSE event on {path}: {reason}")]
    InvalidSse { path: String, reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Network,
    Timeout,
    InvalidOffset,
    BadRequest,
    Forbidden,
    NotFound,
    Conflict,
    StreamClosed,
    PayloadTooLarge,
    TooManyRequests,
    ServiceUnavailable,
    InternalServerError,
    UnexpectedStatus,
}

#[derive(Debug, Error)]
#[error(
    "HTTP {status} ({kind:?}) during {operation} {path}{suffix}",
    suffix = display_message(message.as_deref())
)]
pub struct HttpError {
    pub operation: &'static str,
    pub path: String,
    pub status: StatusCode,
    pub kind: ErrorKind,
    pub message: Option<String>,
    pub retry_after: Option<Duration>,
    pub next_offset: Option<String>,
    pub stream_closed: bool,
}

fn display_message(message: Option<&str>) -> String {
    match message {
        Some(message) if !message.is_empty() => format!(": {message}"),
        _ => String::new(),
    }
}

impl HttpError {
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::Network
                | ErrorKind::Timeout
                | ErrorKind::TooManyRequests
                | ErrorKind::ServiceUnavailable
                | ErrorKind::InternalServerError
        )
    }
}
