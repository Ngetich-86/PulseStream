//! `POST /v1/events`: validate an event and admit it to the bounded pipeline.
//!
//! `202 Accepted` means the event was validated and entered the bounded
//! **in-memory** pipeline. It has not been persisted, and a process crash can
//! lose it. Durable acceptance arrives in M2.

use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use pulsestream_core::event::Event;
use pulsestream_worker::pipeline::AdmitError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::app::AppState;
use crate::error::ApiError;

/// Maximum accepted request body size, in bytes.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// HTTP request shape. Clients cannot supply an event ID; it is server-generated.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRequest {
    source: String,
    event_type: String,
    payload: Value,
}

#[derive(Debug, Serialize)]
pub struct AcceptedResponse {
    event_id: String,
    /// Always `"accepted"`: admitted, not yet processed.
    status: &'static str,
}

pub async fn ingest(
    State(state): State<AppState>,
    body: Result<Json<EventRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<AcceptedResponse>), ApiError> {
    let Json(request) = body.map_err(|rejection| ApiError::from_json_rejection(&rejection))?;
    let event = Event::new(request.source, request.event_type, request.payload)
        .map_err(|err| ApiError::InvalidEvent(err.to_string()))?;

    let event_id = event.id;
    let event_type = event.event_type.clone();
    let source = event.source.clone();
    match state.admission.try_admit(event) {
        Ok(()) => {
            info!(
                %event_id,
                event_type = event_type.as_str(),
                source = source.as_str(),
                state = "admitted",
                "event admitted"
            );
            Ok((
                StatusCode::ACCEPTED,
                Json(AcceptedResponse {
                    event_id: event_id.to_string(),
                    status: "accepted",
                }),
            ))
        }
        Err(AdmitError::Full) => {
            warn!(
                event_type = event_type.as_str(),
                source = source.as_str(),
                state = "rejected",
                "queue full; event not admitted"
            );
            Err(ApiError::QueueFull)
        }
        Err(AdmitError::Closed) => {
            warn!(
                event_type = event_type.as_str(),
                source = source.as_str(),
                state = "rejected",
                "processing unavailable; event not admitted"
            );
            Err(ApiError::ProcessingUnavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use futures_util::stream;
    use pulsestream_core::config::PipelineConfig;
    use pulsestream_worker::pipeline::Pipeline;
    use pulsestream_worker::testing::GatedProcessor;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::MAX_BODY_BYTES;
    use crate::app::{AppState, router};

    fn pipeline(
        queue_capacity: usize,
        worker_concurrency: usize,
        processor: GatedProcessor,
    ) -> Pipeline {
        let config = PipelineConfig {
            queue_capacity,
            worker_concurrency,
            shutdown_timeout: Duration::from_secs(5),
        };
        Pipeline::start(&config, processor)
    }

    async fn send(
        pipeline: &Pipeline,
        request: Request<Body>,
    ) -> (StatusCode, Value, Option<String>) {
        let app = router(AppState {
            admission: pipeline.admission(),
        });
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let retry_after = response
            .headers()
            .get(header::RETRY_AFTER)
            .map(|v| v.to_str().unwrap().to_owned());
        let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap(), retry_after)
    }

    fn post(body: impl Into<Body>) -> Request<Body> {
        Request::post("/v1/events")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap()
    }

    fn post_json(body: &Value) -> Request<Body> {
        post(body.to_string())
    }

    fn valid() -> Value {
        json!({"source": "orders-api", "event_type": "order.created", "payload": {"order_id": "12345"}})
    }

    async fn assert_invalid(body: Value, expected_message: &str) {
        let p = pipeline(4, 1, GatedProcessor::open());
        let (status, json, _) = send(&p, post_json(&body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(json["code"], "INVALID_EVENT");
        assert_eq!(json["message"], expected_message);
        assert_eq!(p.shutdown(Duration::from_secs(5)).await.processed, 0);
    }

    #[tokio::test]
    async fn valid_event_is_accepted_and_reaches_the_processor() {
        let processor = GatedProcessor::open();
        let p = pipeline(4, 1, processor.clone());
        let (status, json, _) = send(&p, post_json(&valid())).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(json["status"], "accepted");
        let event_id = json["event_id"].as_str().unwrap().to_owned();
        assert_eq!(json.as_object().unwrap().len(), 2);

        let report = p.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.processed, 1);
        let seen: Vec<String> = processor.seen().iter().map(ToString::to_string).collect();
        assert_eq!(seen, vec![event_id]);
    }

    #[tokio::test]
    async fn rejects_empty_source() {
        let mut body = valid();
        body["source"] = json!("");
        assert_invalid(body, "source must not be empty").await;
    }

    #[tokio::test]
    async fn rejects_empty_event_type() {
        let mut body = valid();
        body["event_type"] = json!(" ");
        assert_invalid(body, "event_type must not be empty").await;
    }

    #[tokio::test]
    async fn rejects_overlong_source() {
        let mut body = valid();
        body["source"] = json!("s".repeat(101));
        assert_invalid(body, "source must be at most 100 characters").await;
    }

    #[tokio::test]
    async fn rejects_overlong_event_type() {
        let mut body = valid();
        body["event_type"] = json!("t".repeat(151));
        assert_invalid(body, "event_type must be at most 150 characters").await;
    }

    #[tokio::test]
    async fn rejects_null_or_missing_payload_and_unknown_fields() {
        let schema = "request body must be a JSON object with string fields `source` and \
                      `event_type`, a `payload`, and no other fields";
        let mut null_payload = valid();
        null_payload["payload"] = Value::Null;
        assert_invalid(null_payload, "payload must not be null").await;

        let mut missing = valid();
        missing.as_object_mut().unwrap().remove("payload");
        assert_invalid(missing, schema).await;

        let mut client_id = valid();
        client_id["event_id"] = json!("chosen-by-client");
        assert_invalid(client_id, schema).await;

        let mut wrong_type = valid();
        wrong_type["source"] = json!(42);
        assert_invalid(wrong_type, schema).await;
    }

    #[tokio::test]
    async fn rejects_malformed_json() {
        let p = pipeline(4, 1, GatedProcessor::open());
        let (status, json, _) = send(&p, post(r#"{"source": "a", "#)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            json,
            json!({"code": "INVALID_EVENT", "message": "request body is not valid JSON"})
        );
    }

    #[tokio::test]
    async fn rejects_non_json_content_type() {
        let p = pipeline(4, 1, GatedProcessor::open());
        let request = Request::post("/v1/events")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(valid().to_string()))
            .unwrap();
        let (status, json, _) = send(&p, request).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(json["code"], "UNSUPPORTED_MEDIA_TYPE");
    }

    fn oversized_body() -> String {
        let mut body = valid();
        body["payload"] = json!({"blob": "x".repeat(MAX_BODY_BYTES)});
        body.to_string()
    }

    #[tokio::test]
    async fn rejects_body_over_limit_with_content_length() {
        let p = pipeline(4, 1, GatedProcessor::open());
        let body = oversized_body();
        let request = Request::post("/v1/events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .unwrap();
        let (status, json, _) = send(&p, request).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json["code"], "PAYLOAD_TOO_LARGE");
        assert_eq!(p.shutdown(Duration::from_secs(5)).await.processed, 0);
    }

    #[tokio::test]
    async fn rejects_streamed_body_over_limit() {
        // No Content-Length: the limit must be enforced while streaming.
        let p = pipeline(4, 1, GatedProcessor::open());
        let chunks: Vec<Result<String, std::io::Error>> = oversized_body()
            .into_bytes()
            .chunks(8 * 1024)
            .map(|c| Ok(String::from_utf8(c.to_vec()).unwrap()))
            .collect();
        let (status, json, _) = send(&p, post(Body::from_stream(stream::iter(chunks)))).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json["code"], "PAYLOAD_TOO_LARGE");
    }

    #[tokio::test]
    async fn accepts_body_just_under_limit() {
        let p = pipeline(4, 1, GatedProcessor::open());
        let envelope = valid().to_string().len() - r#"{"order_id":"12345"}"#.len();
        let mut body = valid();
        let filler = MAX_BODY_BYTES - envelope - r#"{"b":""}"#.len();
        body["payload"] = json!({"b": "x".repeat(filler)});
        let body = body.to_string();
        assert_eq!(body.len(), MAX_BODY_BYTES);
        let (status, _, _) = send(&p, post(body)).await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn full_queue_returns_429_and_does_not_admit() {
        let processor = GatedProcessor::new();
        let p = pipeline(2, 1, processor.clone());

        let (status, first, _) = send(&p, post_json(&valid())).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        processor.wait_started(1).await; // the only worker slot is busy
        let mut admitted = vec![first["event_id"].as_str().unwrap().to_owned()];
        for _ in 0..2 {
            let (status, json, _) = send(&p, post_json(&valid())).await;
            assert_eq!(status, StatusCode::ACCEPTED);
            admitted.push(json["event_id"].as_str().unwrap().to_owned());
        }

        let (status, json, retry_after) = send(&p, post_json(&valid())).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            json,
            json!({"code": "QUEUE_FULL", "message": "event admission capacity is temporarily exhausted"})
        );
        assert_eq!(retry_after.as_deref(), Some("1"));

        processor.release(3);
        let report = p.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.processed, 3);
        let seen: Vec<String> = processor.seen().iter().map(ToString::to_string).collect();
        assert_eq!(
            seen, admitted,
            "only the three admitted events were processed"
        );
    }

    #[tokio::test]
    async fn closed_pipeline_returns_503() {
        let p = pipeline(4, 1, GatedProcessor::open());
        let admission = p.admission();
        p.shutdown(Duration::from_secs(5)).await;

        let app = router(AppState { admission });
        let response = app.oneshot(post_json(&valid())).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json,
            json!({"code": "PROCESSING_UNAVAILABLE", "message": "event processing is temporarily unavailable"})
        );
    }
}
