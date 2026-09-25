//! PulseStream HTTP API.
//!
//! Hosts `POST /v1/events` and, in M1, the bounded in-memory processing
//! pipeline that admitted events flow into. Admission is non-durable: see
//! ADR-006.

mod app;
mod error;
mod events;
mod health;
mod shutdown;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

use axum::Router;
use pulsestream_core::ServiceName;
use pulsestream_core::config::ApiConfig;
use pulsestream_worker::pipeline::{AcknowledgeProcessor, Pipeline};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::app::AppState;

const SERVICE: ServiceName = ServiceName::Api;

#[tokio::main]
async fn main() -> ExitCode {
    telemetry::init();

    let config = match ApiConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            error!(service = %SERVICE, error = %err, "invalid configuration");
            return ExitCode::FAILURE;
        }
    };
    info!(
        service = %SERVICE,
        version = env!("CARGO_PKG_VERSION"),
        database_configured = config.database_url.is_some(),
        "starting"
    );

    let listener = match TcpListener::bind(config.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            error!(service = %SERVICE, bind = %config.bind, error = %err, "failed to bind");
            return ExitCode::FAILURE;
        }
    };
    match listener.local_addr() {
        Ok(addr) => info!(service = %SERVICE, bind = %addr, "listening"),
        Err(err) => info!(service = %SERVICE, bind = %config.bind, error = %err, "listening"),
    }

    let pipeline = Pipeline::start(&config.pipeline, AcknowledgeProcessor);
    let admission = pipeline.admission();
    let app = app::router(AppState {
        admission: admission.clone(),
    });

    // Shutdown order: close admission first, so requests still in flight get
    // 503 rather than being admitted; then stop the HTTP server; then drain
    // the pipeline within the configured timeout.
    let shutdown = async move {
        shutdown::signal().await;
        admission.close();
        info!(service = %SERVICE, state = "admission_closed", "shutdown started; new events are rejected");
    };
    let served = serve(listener, app, shutdown).await;
    if let Err(err) = &served {
        error!(service = %SERVICE, error = %err, "server error");
    }
    info!(service = %SERVICE, "http server stopped; draining event pipeline");

    let report = pipeline.shutdown(config.pipeline.shutdown_timeout).await;
    info!(
        service = %SERVICE,
        processed = report.processed,
        failed = report.failed,
        timed_out = report.timed_out,
        "shutdown complete"
    );
    if served.is_err() || report.timed_out {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Serves `app` until `shutdown` resolves, then stops accepting connections
/// and lets in-flight requests finish.
async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pulsestream_core::config::PipelineConfig;
    use pulsestream_worker::pipeline::{AcknowledgeProcessor, Pipeline};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use crate::app::{AppState, router};

    #[tokio::test]
    async fn serves_requests_then_shuts_down_gracefully() {
        let pipeline = Pipeline::start(&PipelineConfig::default(), AcknowledgeProcessor);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let app = router(AppState {
            admission: pipeline.admission(),
        });
        let server = tokio::spawn(super::serve(listener, app, async {
            let _ = stop_rx.await;
        }));

        let body = r#"{"source":"s","event_type":"t","payload":{}}"#;
        let request = format!(
            "POST /v1/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");

        stop_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server did not stop after shutdown was signalled")
            .unwrap();
        assert!(result.is_ok());
        let report = pipeline.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.processed, 1);
    }
}
