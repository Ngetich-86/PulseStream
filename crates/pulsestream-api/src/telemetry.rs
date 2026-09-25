//! Structured logging setup.

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str = "info";

/// Installs a `tracing` subscriber filtered by `RUST_LOG` (default `info`).
///
/// An invalid `RUST_LOG` falls back to the default and is reported, never
/// silently ignored. ANSI colors are used only when stdout is a terminal, so
/// redirected or collected logs stay plain text.
pub fn init() {
    let (filter, invalid) = match std::env::var(EnvFilter::DEFAULT_ENV) {
        Ok(raw) => match EnvFilter::try_new(raw) {
            Ok(filter) => (filter, None),
            Err(err) => (EnvFilter::new(DEFAULT_FILTER), Some(err)),
        },
        Err(_) => (EnvFilter::new(DEFAULT_FILTER), None),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    if let Some(err) = invalid {
        tracing::warn!(error = %err, default = DEFAULT_FILTER, "invalid RUST_LOG, using default");
    }
}
