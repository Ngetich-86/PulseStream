# ADR-001: Tokio as the asynchronous runtime

- Status: Accepted
- Date: 2026-09-25
- Milestone: M0

## Context

PulseStream is I/O-bound: HTTP ingestion, PostgreSQL round-trips, timers for
retries and leases, and signal handling. It needs a mature async runtime with
networking, timers, synchronization primitives, and signal support, all
compatible with the HTTP and database ecosystem we expect to use.

## Decision

Use **Tokio** with its multi-threaded runtime, configured through the
conventional `#[tokio::main]` entry point. M0 does not tune worker-thread
counts or other runtime parameters.

## Consequences

- `axum` and the mainstream PostgreSQL drivers are built on Tokio, so we avoid
  runtime-compatibility shims.
- Tokio does **not** bound application concurrency. `tokio::spawn` and
  unbounded channels make it easy to grow memory without limit. Bounding work
  is our responsibility; see [ADR-004](ADR-004-bounded-concurrency-backpressure.md).
- Runtime tuning, such as thread counts or a dedicated blocking pool, is
  deferred until benchmarks in M4 show a need.
- Only the Tokio features the current code uses are enabled.
