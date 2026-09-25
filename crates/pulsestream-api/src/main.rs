//! PulseStream HTTP API.
//!
//! M0 scope: configuration, structured logging, health endpoints, and graceful
//! shutdown. Event ingestion and admission control arrive in M1.

mod health;
mod shutdown;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

use pulsestream_core::ServiceName;
use pulsestream_core::config::ApiConfig;
use tokio::net::TcpListener;
use tracing::{error, info};

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

    if let Err(err) = serve(listener, shutdown::signal()).await {
        error!(service = %SERVICE, error = %err, "server error");
        return ExitCode::FAILURE;
    }
    info!(service = %SERVICE, "shutdown complete");
    ExitCode::SUCCESS
}

/// Serves the API until `shutdown` resolves, then stops accepting connections
/// and lets in-flight requests finish.
async fn serve(
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, health::router())
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn serves_requests_then_shuts_down_gracefully() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(super::serve(listener, async {
            let _ = stop_rx.await;
        }));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");

        stop_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server did not stop after shutdown was signalled")
            .unwrap();
        assert!(result.is_ok());
    }
}
