# ADR-004: Bounded concurrency and explicit backpressure

- Status: Accepted
- Date: 2026-09-25
- Milestone: M0

## Context

In an async runtime, the simplest ways to hand off work are `tokio::spawn` per
item and `mpsc::unbounded_channel`. Neither has a limit. Under overload they
turn excess demand into memory growth and rising latency until the process
fails, usually far from where the overload started.

## Decision

Concurrency and buffering in PulseStream **must be explicitly bounded**:

1. Unbounded channels (`unbounded_channel` and similar) are prohibited.
2. Spawning a task per incoming item without a limit is prohibited. Spawning is
   gated by a bounded mechanism such as a `Semaphore`, a `JoinSet` with a
   capacity check, or a fixed-size worker pool.
3. Every queue, pool, and in-flight limit has an explicit, configurable
   capacity with a documented default.
4. When capacity is exhausted, the system responds **explicitly**. The API
   rejects or defers with a clear status such as HTTP 429 or 503, and the worker
   stops claiming new work. It never buffers without limit.

An exception requires a written justification in code review explaining why
the input is bounded by construction. Examples are a fixed set of startup
tasks, or a channel whose only sender is itself rate-limited.

## Consequences

- Overload behavior is predictable and testable, and later milestones test it.
- Capacity limits become configuration that we must document and tune using
  M4 benchmarks.
- M0 has no queues or spawn loops. The API relies on `axum::serve`
  connection handling, and admission limits are added in M1.
