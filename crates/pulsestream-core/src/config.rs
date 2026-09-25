//! Environment-based configuration.
//!
//! Parsing takes a lookup function instead of reading `std::env` directly so it
//! can be tested without mutating process-global state.

use std::fmt;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::time::Duration;

use thiserror::Error;

pub const API_BIND_VAR: &str = "PULSESTREAM_API_BIND";
pub const DATABASE_URL_VAR: &str = "DATABASE_URL";
pub const QUEUE_CAPACITY_VAR: &str = "PULSESTREAM_QUEUE_CAPACITY";
pub const WORKER_CONCURRENCY_VAR: &str = "PULSESTREAM_WORKER_CONCURRENCY";
pub const SHUTDOWN_TIMEOUT_MS_VAR: &str = "PULSESTREAM_SHUTDOWN_TIMEOUT_MS";

pub const DEFAULT_QUEUE_CAPACITY: usize = 256;
pub const QUEUE_CAPACITY_RANGE: RangeInclusive<u64> = 1..=65_536;
pub const DEFAULT_WORKER_CONCURRENCY: usize = 4;
pub const WORKER_CONCURRENCY_RANGE: RangeInclusive<u64> = 1..=64;
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;
pub const SHUTDOWN_TIMEOUT_MS_RANGE: RangeInclusive<u64> = 1..=300_000;

/// Loopback-only default so an unconfigured process is never exposed publicly.
pub const DEFAULT_API_BIND: &str = "127.0.0.1:8088";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{var} must be a socket address such as 127.0.0.1:8088, got {value:?}")]
    InvalidBindAddress { var: &'static str, value: String },

    #[error("{var} must not be empty when set")]
    Empty { var: &'static str },

    #[error("{var} must use the postgres:// or postgresql:// scheme")]
    UnsupportedDatabaseScheme { var: &'static str },

    #[error("{var} is missing a host")]
    MissingDatabaseHost { var: &'static str },

    #[error("{var} must be an integer between {min} and {max}, got {value:?}")]
    OutOfRange {
        var: &'static str,
        value: String,
        min: u64,
        max: u64,
    },
}

/// A validated PostgreSQL connection URL.
///
/// The raw value may contain credentials, so neither `Debug` nor `Display`
/// ever print it.
#[derive(Clone, PartialEq, Eq)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        let var = DATABASE_URL_VAR;
        let value = value.trim();
        if value.is_empty() {
            return Err(ConfigError::Empty { var });
        }
        let rest = value
            .strip_prefix("postgres://")
            .or_else(|| value.strip_prefix("postgresql://"))
            .ok_or(ConfigError::UnsupportedDatabaseScheme { var })?;
        let authority = rest.split(['/', '?']).next().unwrap_or_default();
        let host = authority.rsplit('@').next().unwrap_or_default();
        if host.is_empty() || host.starts_with(':') {
            return Err(ConfigError::MissingDatabaseHost { var });
        }
        Ok(Self(value.to_owned()))
    }

    /// The raw connection string, for handing to a database driver only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DatabaseUrl(<redacted>)")
    }
}

/// Reads and validates `DATABASE_URL` when set.
///
/// Optional until M2: validated so mistakes surface early, but no process
/// connects to PostgreSQL yet.
pub fn database_url(
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<Option<DatabaseUrl>, ConfigError> {
    lookup(DATABASE_URL_VAR)
        .map(|raw| DatabaseUrl::parse(&raw))
        .transpose()
}

/// Reads `PULSESTREAM_API_BIND`, defaulting to [`DEFAULT_API_BIND`].
pub fn api_bind(lookup: &impl Fn(&str) -> Option<String>) -> Result<SocketAddr, ConfigError> {
    let raw = lookup(API_BIND_VAR).unwrap_or_else(|| DEFAULT_API_BIND.to_owned());
    raw.trim()
        .parse()
        .map_err(|_| ConfigError::InvalidBindAddress {
            var: API_BIND_VAR,
            value: raw,
        })
}

/// Reads an integer setting, applying `default` only when the variable is unset.
///
/// A value that is set but invalid or out of range is an error; it is never
/// silently replaced by the default.
fn bounded_integer(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: u64,
    range: RangeInclusive<u64>,
) -> Result<u64, ConfigError> {
    let Some(raw) = lookup(var) else {
        return Ok(default);
    };
    match raw.trim().parse::<u64>() {
        Ok(value) if range.contains(&value) => Ok(value),
        _ => Err(ConfigError::OutOfRange {
            var,
            value: raw,
            min: *range.start(),
            max: *range.end(),
        }),
    }
}

/// Limits of the bounded in-memory event pipeline.
///
/// At most `queue_capacity + worker_concurrency` events are held in memory at
/// any time: `queue_capacity` waiting for a worker and `worker_concurrency`
/// being processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineConfig {
    pub queue_capacity: usize,
    pub worker_concurrency: usize,
    /// How long shutdown waits for admitted events to drain before abandoning them.
    pub shutdown_timeout: Duration,
}

impl PipelineConfig {
    pub fn from_lookup(lookup: &impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        // The ranges fit in usize on every supported (>= 32-bit) target.
        let queue_capacity = bounded_integer(
            lookup,
            QUEUE_CAPACITY_VAR,
            DEFAULT_QUEUE_CAPACITY as u64,
            QUEUE_CAPACITY_RANGE,
        )? as usize;
        let worker_concurrency = bounded_integer(
            lookup,
            WORKER_CONCURRENCY_VAR,
            DEFAULT_WORKER_CONCURRENCY as u64,
            WORKER_CONCURRENCY_RANGE,
        )? as usize;
        let shutdown_timeout_ms = bounded_integer(
            lookup,
            SHUTDOWN_TIMEOUT_MS_VAR,
            DEFAULT_SHUTDOWN_TIMEOUT_MS,
            SHUTDOWN_TIMEOUT_MS_RANGE,
        )?;
        Ok(Self {
            queue_capacity,
            worker_concurrency,
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms),
        })
    }

    /// Upper bound on events held in memory by the pipeline.
    pub fn max_occupancy(&self) -> usize {
        self.queue_capacity + self.worker_concurrency
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            worker_concurrency: DEFAULT_WORKER_CONCURRENCY,
            shutdown_timeout: Duration::from_millis(DEFAULT_SHUTDOWN_TIMEOUT_MS),
        }
    }
}

/// Configuration for the `pulsestream-api` process.
///
/// In M1 the API process also hosts the in-memory processing pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    pub database_url: Option<DatabaseUrl>,
    pub pipeline: PipelineConfig,
}

impl ApiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            bind: api_bind(&lookup)?,
            database_url: database_url(&lookup)?,
            pipeline: PipelineConfig::from_lookup(&lookup)?,
        })
    }
}

/// Configuration for the `pulsestream-worker` process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub database_url: Option<DatabaseUrl>,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            database_url: database_url(&lookup)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |key| map.get(key).cloned()
    }

    fn config(vars: &[(&str, &str)]) -> Result<ApiConfig, ConfigError> {
        ApiConfig::from_lookup(env(vars))
    }

    #[test]
    fn defaults_to_loopback_bind_without_database() {
        let cfg = config(&[]).unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:8088".parse().unwrap());
        assert!(cfg.bind.ip().is_loopback());
        assert_eq!(cfg.database_url, None);
    }

    #[test]
    fn rejects_invalid_bind_address() {
        let err = config(&[(API_BIND_VAR, "localhost")]).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBindAddress { .. }));
    }

    #[test]
    fn accepts_postgres_urls() {
        for url in [
            "postgres://user:pw@127.0.0.1:55432/pulsestream",
            "postgresql://db.internal/pulsestream?sslmode=require",
        ] {
            let cfg = config(&[(DATABASE_URL_VAR, url)]).unwrap();
            assert_eq!(cfg.database_url.unwrap().expose(), url);
        }
    }

    #[test]
    fn rejects_malformed_database_urls() {
        assert_eq!(
            config(&[(DATABASE_URL_VAR, "  ")]).unwrap_err(),
            ConfigError::Empty {
                var: DATABASE_URL_VAR
            }
        );
        assert_eq!(
            config(&[(DATABASE_URL_VAR, "mysql://h/db")]).unwrap_err(),
            ConfigError::UnsupportedDatabaseScheme {
                var: DATABASE_URL_VAR
            }
        );
        assert_eq!(
            config(&[(DATABASE_URL_VAR, "postgres://user:pw@:5432/db")]).unwrap_err(),
            ConfigError::MissingDatabaseHost {
                var: DATABASE_URL_VAR
            }
        );
    }

    #[test]
    fn pipeline_defaults_are_conservative() {
        let cfg = config(&[]).unwrap().pipeline;
        assert_eq!(cfg, PipelineConfig::default());
        assert_eq!(cfg.queue_capacity, 256);
        assert_eq!(cfg.worker_concurrency, 4);
        assert_eq!(cfg.shutdown_timeout, Duration::from_secs(10));
        assert_eq!(cfg.max_occupancy(), 260);
    }

    #[test]
    fn accepts_pipeline_limits_at_range_bounds() {
        let cfg = config(&[
            (QUEUE_CAPACITY_VAR, "65536"),
            (WORKER_CONCURRENCY_VAR, "1"),
            (SHUTDOWN_TIMEOUT_MS_VAR, " 300000 "),
        ])
        .unwrap()
        .pipeline;
        assert_eq!(cfg.queue_capacity, 65_536);
        assert_eq!(cfg.worker_concurrency, 1);
        assert_eq!(cfg.shutdown_timeout, Duration::from_secs(300));
    }

    #[test]
    fn rejects_invalid_queue_capacity() {
        for bad in ["0", "65537", "18446744073709551615", "-1", "lots", ""] {
            let err = config(&[(QUEUE_CAPACITY_VAR, bad)]).unwrap_err();
            assert!(
                matches!(
                    err,
                    ConfigError::OutOfRange {
                        var: QUEUE_CAPACITY_VAR,
                        ..
                    }
                ),
                "{bad:?} -> {err:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_worker_concurrency() {
        for bad in ["0", "65", "4.5"] {
            let err = config(&[(WORKER_CONCURRENCY_VAR, bad)]).unwrap_err();
            assert!(
                matches!(
                    err,
                    ConfigError::OutOfRange {
                        var: WORKER_CONCURRENCY_VAR,
                        ..
                    }
                ),
                "{bad:?} -> {err:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_shutdown_timeout() {
        for bad in ["0", "300001"] {
            assert!(
                config(&[(SHUTDOWN_TIMEOUT_MS_VAR, bad)]).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn worker_ignores_api_bind_but_validates_database_url() {
        let cfg = WorkerConfig::from_lookup(env(&[(API_BIND_VAR, "not-an-address")])).unwrap();
        assert_eq!(cfg.database_url, None);
        assert!(WorkerConfig::from_lookup(env(&[(DATABASE_URL_VAR, "redis://h")])).is_err());
    }

    #[test]
    fn database_url_never_leaks_credentials() {
        let url = DatabaseUrl::parse("postgres://user:s3cret@host/db").unwrap();
        let rendered = format!(
            "{url:?} {:?}",
            WorkerConfig {
                database_url: Some(url.clone()),
            }
        );
        assert!(!rendered.contains("s3cret"), "{rendered}");
    }
}
