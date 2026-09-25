//! Liveness and readiness endpoints.
//!
//! Readiness reports only dependencies that exist. In M1 that is the
//! in-memory processing pipeline. PostgreSQL checks are added when the event
//! pipeline first uses the database (M2).

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Serialize;
use tracing::warn;

use crate::SERVICE;
use crate::app::AppState;

#[derive(Debug, Serialize)]
pub struct Liveness {
    status: &'static str,
    service: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Readiness {
    status: &'static str,
    service: &'static str,
    /// Dependency name -> `"ready"` or `"unavailable"`.
    checks: BTreeMap<&'static str, &'static str>,
}

pub async fn live() -> Json<Liveness> {
    Json(Liveness {
        status: "ok",
        service: SERVICE.as_str(),
    })
}

pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<Readiness>) {
    let processing_ready = state.admission.is_available();
    let checks = BTreeMap::from([(
        "processing",
        if processing_ready {
            "ready"
        } else {
            "unavailable"
        },
    )]);
    let (code, status) = if processing_ready {
        (StatusCode::OK, "ok")
    } else {
        warn!(check = "processing", "readiness check failed");
        (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
    };
    (
        code,
        Json(Readiness {
            status,
            service: SERVICE.as_str(),
            checks,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use pulsestream_core::config::PipelineConfig;
    use pulsestream_worker::pipeline::{AcknowledgeProcessor, Admission, Pipeline};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::app::{AppState, router};

    async fn get(admission: Admission, path: &str) -> (StatusCode, Option<String>, Value) {
        let response = router(AppState { admission })
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_owned());
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let json = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        (status, content_type, json)
    }

    fn start() -> Pipeline {
        Pipeline::start(&PipelineConfig::default(), AcknowledgeProcessor)
    }

    #[tokio::test]
    async fn live_reports_ok() {
        let (status, content_type, body) = get(start().admission(), "/health/live").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type.as_deref(), Some("application/json"));
        assert_eq!(body, json!({"status": "ok", "service": "pulsestream-api"}));
    }

    #[tokio::test]
    async fn ready_reports_processing_ready() {
        let (status, _, body) = get(start().admission(), "/health/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"status": "ok", "service": "pulsestream-api", "checks": {"processing": "ready"}})
        );
    }

    #[tokio::test]
    async fn ready_reports_unavailable_after_pipeline_stops() {
        let pipeline = start();
        let admission = pipeline.admission();
        pipeline.shutdown(Duration::from_secs(5)).await;
        let (status, _, body) = get(admission, "/health/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            json!({
                "status": "unavailable",
                "service": "pulsestream-api",
                "checks": {"processing": "unavailable"}
            })
        );
    }

    #[tokio::test]
    async fn unknown_routes_are_not_found() {
        let (status, _, _) = get(start().admission(), "/health").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
