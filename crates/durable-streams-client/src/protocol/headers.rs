use crate::error::{Error, ErrorKind, HttpError, Result};
use crate::types::Offset;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use std::time::Duration;

pub const STREAM_NEXT_OFFSET: &str = "stream-next-offset";
pub const STREAM_UP_TO_DATE: &str = "stream-up-to-date";
pub const STREAM_CLOSED: &str = "stream-closed";
pub const STREAM_CURSOR: &str = "stream-cursor";
pub const STREAM_SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";
pub const RETRY_AFTER: &str = "retry-after";
pub const ETAG: &str = "etag";
pub const CONTENT_TYPE: &str = "content-type";

pub fn require_offset(headers: &HeaderMap, operation: &'static str, path: &str) -> Result<Offset> {
    let value = headers
        .get(STREAM_NEXT_OFFSET)
        .ok_or_else(|| Error::MissingHeader {
            operation,
            path: path.to_string(),
            name: "Stream-Next-Offset",
        })?;
    let value = value
        .to_str()
        .map_err(|_| Error::InvalidHeader {
            operation,
            path: path.to_string(),
            name: "Stream-Next-Offset",
            reason: "header was not valid ASCII".to_string(),
        })?
        .to_string();
    Ok(Offset::from(value))
}

#[must_use]
pub fn optional_string(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

#[must_use]
pub fn has_true_header(headers: &HeaderMap, name: &'static str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

#[must_use]
pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

pub fn http_error(
    operation: &'static str,
    path: &str,
    status: StatusCode,
    headers: &HeaderMap,
    body: &bytes::Bytes,
) -> Error {
    let body_text = String::from_utf8_lossy(body).trim().to_string();
    let stream_closed = has_true_header(headers, STREAM_CLOSED);
    let next_offset = optional_string(headers, STREAM_NEXT_OFFSET);
    let retry_after = parse_retry_after(headers);
    let lower = body_text.to_ascii_lowercase();

    let kind = match status {
        StatusCode::BAD_REQUEST if lower.contains("offset") => ErrorKind::InvalidOffset,
        StatusCode::BAD_REQUEST => ErrorKind::BadRequest,
        StatusCode::FORBIDDEN => ErrorKind::Forbidden,
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::CONFLICT if stream_closed => ErrorKind::StreamClosed,
        StatusCode::CONFLICT => ErrorKind::Conflict,
        StatusCode::PAYLOAD_TOO_LARGE => ErrorKind::PayloadTooLarge,
        StatusCode::TOO_MANY_REQUESTS => ErrorKind::TooManyRequests,
        StatusCode::SERVICE_UNAVAILABLE => ErrorKind::ServiceUnavailable,
        StatusCode::INTERNAL_SERVER_ERROR => ErrorKind::InternalServerError,
        _ => ErrorKind::UnexpectedStatus,
    };

    Error::Http(HttpError {
        operation,
        path: path.to_string(),
        status,
        kind,
        message: (!body_text.is_empty()).then_some(body_text),
        retry_after,
        next_offset,
        stream_closed,
    })
}
