//! Process shutdown signal handling.

use tracing::{info, warn};

/// Resolves on Ctrl+C / SIGINT or, on Unix, SIGTERM.
pub async fn signal() {
    let interrupt = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to listen for SIGINT");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => {
                warn!(error = %err, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = interrupt => info!(signal = "SIGINT", "shutdown requested"),
        () = terminate => info!(signal = "SIGTERM", "shutdown requested"),
    }
}
