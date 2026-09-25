//! Liveness and readiness endpoints.
//!
//! Readiness reports only dependencies that exist. M0 has none, so `checks` is
//! empty; PostgreSQL and admission-queue checks are added when those
//! components are implemented.

use std::collections::BTreeMap;

use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::SERVICE;

#[derive(Debug, Serialize)]
struct Liveness {
    status: &'static str,
    service: &'static str,
}

#[derive(Debug, Serialize)]
struct Readiness {
    status: &'static str,
    service: &'static str,
    /// Dependency name -> status. Empty until M0+ components exist.
    checks: BTreeMap<&'static str, &'static str>,
}

pub fn router() -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
}

async fn live() -> Json<Liveness> {
    Json(Liveness {
        status: "ok",
        service: SERVICE.as_str(),
    })
}

async fn ready() -> Json<Readiness> {
    Json(Readiness {
        status: "ok",
        service: SERVICE.as_str(),
        checks: BTreeMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn get(path: &str) -> (StatusCode, Option<String>, Vec<u8>) {
        let response = super::router()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_owned());
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        (status, content_type, body.to_vec())
    }

    #[tokio::test]
    async fn live_reports_ok() {
        let (status, content_type, body) = get("/health/live").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type.as_deref(), Some("application/json"));
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body, json!({"status": "ok", "service": "pulsestream-api"}));
    }

    #[tokio::test]
    async fn ready_reports_no_unimplemented_dependencies() {
        let (status, _, body) = get("/health/ready").await;
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            json!({"status": "ok", "service": "pulsestream-api", "checks": {}})
        );
    }

    #[tokio::test]
    async fn unknown_routes_are_not_found() {
        let (status, _, _) = get("/health").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
