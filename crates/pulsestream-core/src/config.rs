//! Environment-based configuration.
//!
//! Parsing takes a lookup function instead of reading `std::env` directly so it
//! can be tested without mutating process-global state.

use std::fmt;
use std::net::SocketAddr;

use thiserror::Error;

pub const API_BIND_VAR: &str = "PULSESTREAM_API_BIND";
pub const DATABASE_URL_VAR: &str = "DATABASE_URL";

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
/// Optional in M0: validated so mistakes surface early, but no process
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

/// Configuration for the `pulsestream-api` process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    pub database_url: Option<DatabaseUrl>,
}

impl ApiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            bind: api_bind(&lookup)?,
            database_url: database_url(&lookup)?,
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
