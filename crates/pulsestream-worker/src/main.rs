//! PulseStream worker.
//!
//! M0 scope: configuration, structured logging, and a clean process lifecycle.
//! The worker performs no event processing yet; it idles (no polling, no busy
//! loop) until asked to shut down. The bounded worker pool, retries, and
//! recovery arrive in later milestones, as does draining in-flight work on
//! shutdown.

mod shutdown;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

use pulsestream_core::ServiceName;
use pulsestream_core::config::WorkerConfig;
use tracing::{error, info};

const SERVICE: ServiceName = ServiceName::Worker;

#[tokio::main]
async fn main() -> ExitCode {
    telemetry::init();

    let config = match WorkerConfig::from_env() {
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

    run(shutdown::signal()).await;
    info!(service = %SERVICE, "shutdown complete");
    ExitCode::SUCCESS
}

/// Runs the worker until `shutdown` resolves.
async fn run(shutdown: impl Future<Output = ()>) {
    info!(service = %SERVICE, state = "idle", "ready; event processing not implemented in M0");
    shutdown.await;
    info!(service = %SERVICE, state = "stopping", "stopping");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[tokio::test]
    async fn runs_until_shutdown_is_signalled() {
        let still_running = tokio::time::timeout(
            Duration::from_millis(50),
            super::run(std::future::pending()),
        )
        .await;
        assert!(
            still_running.is_err(),
            "worker exited without a shutdown signal"
        );

        tokio::time::timeout(Duration::from_secs(1), super::run(std::future::ready(())))
            .await
            .expect("worker did not stop after shutdown was signalled");
    }
}
