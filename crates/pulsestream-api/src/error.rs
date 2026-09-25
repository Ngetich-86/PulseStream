//! Stable API error responses: `{"code": "...", "message": "..."}`.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Seconds a client should wait before retrying after `QUEUE_FULL`.
pub const QUEUE_FULL_RETRY_AFTER_SECS: u32 = 1;

// Keep the PAYLOAD_TOO_LARGE message in sync with the enforced limit.
const _: () = assert!(crate::events::MAX_BODY_BYTES == 64 * 1024);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// 400: malformed JSON, wrong shape, or failed validation.
    InvalidEvent(String),
    /// 413: request body exceeds the configured limit.
    PayloadTooLarge,
    /// 415: request is not `application/json`.
    UnsupportedMediaType,
    /// 429: the admission queue is at capacity.
    QueueFull,
    /// 503: the pipeline is shutting down or has stopped.
    ProcessingUnavailable,
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
}

impl ApiError {
    /// Maps extractor rejections to stable errors without exposing
    /// framework or deserializer internals.
    pub fn from_json_rejection(rejection: &JsonRejection) -> Self {
        match rejection {
            JsonRejection::MissingJsonContentType(_) => Self::UnsupportedMediaType,
            JsonRejection::BytesRejection(_)
                if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE =>
            {
                Self::PayloadTooLarge
            }
            JsonRejection::JsonSyntaxError(_) => {
                Self::InvalidEvent("request body is not valid JSON".to_owned())
            }
            JsonRejection::JsonDataError(_) => Self::InvalidEvent(
                "request body must be a JSON object with string fields `source` and \
                 `event_type`, a `payload`, and no other fields"
                    .to_owned(),
            ),
            _ => Self::InvalidEvent("request body could not be read".to_owned()),
        }
    }

    fn parts(&self) -> (StatusCode, &'static str, &str) {
        match self {
            Self::InvalidEvent(message) => (StatusCode::BAD_REQUEST, "INVALID_EVENT", message),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "PAYLOAD_TOO_LARGE",
                "request body exceeds the 64 KiB limit",
            ),
            Self::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UNSUPPORTED_MEDIA_TYPE",
                "Content-Type must be application/json",
            ),
            Self::QueueFull => (
                StatusCode::TOO_MANY_REQUESTS,
                "QUEUE_FULL",
                "event admission capacity is temporarily exhausted",
            ),
            Self::ProcessingUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "PROCESSING_UNAVAILABLE",
                "event processing is temporarily unavailable",
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.parts();
        let mut response = (status, Json(ErrorBody { code, message })).into_response();
        if self == Self::QueueFull {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from(QUEUE_FULL_RETRY_AFTER_SECS),
            );
        }
        response
    }
}
