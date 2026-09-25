//! PulseStream event processing.
//!
//! In M1 the bounded in-memory [`pipeline`] runs inside the API process,
//! because an in-memory queue cannot cross a process boundary. When durable
//! acceptance arrives in M2, the standalone `pulsestream-worker` binary will
//! consume events from PostgreSQL through the same bounded execution model
//! (ADR-006).

pub mod pipeline;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;
