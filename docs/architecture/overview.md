# PulseStream Architecture Overview

Status: **M0 (Foundation)**. This document describes the intended architecture
and marks which parts exist today. Anything labelled *planned* is a design
intention, not an implemented capability.

## Data plane

```text
Producer / Client
      |
      v
PulseStream API ................ M0: health endpoints only | ingestion: planned (M1)
      |
      v
Bounded Admission Layer ........ planned (M1)
      |
      v
Durable Event Store ............ M0: PostgreSQL container only | schema: planned (M2)
      |
      v
Bounded Worker Pool ............ M0: idle lifecycle skeleton | processing: planned (M1/M2)
      |
      +--> success ............. planned
      |
      +--> retry ............... planned (M3)
      |
      +--> dead-letter ......... planned (M3)
```

### Components

| Component | Crate / artifact | Current (M0) | Planned |
| --- | --- | --- | --- |
| API | `pulsestream-api` | `GET /health/live` and `GET /health/ready`, env config, structured logs, graceful shutdown on SIGINT/SIGTERM | Event ingestion, request validation, admission and backpressure responses |
| Admission layer | (inside `pulsestream-api`) | Not implemented | Bounded capacity, with explicit rejection (for example HTTP 429/503) instead of unbounded buffering |
| Durable event store | PostgreSQL via `docker-compose.yml` | Local container with health check. No schema, no application connection | Event persistence, idempotency keys, lifecycle state, recovery queries |
| Worker | `pulsestream-worker` | Starts, logs, idles without polling, stops cleanly | Bounded concurrent execution, retries with backoff, dead-lettering, crash recovery, drain on shutdown |
| Core | `pulsestream-core` | Service identity, config parsing and validation, credential redaction | Event domain model and shared invariants |

### Acceptance and delivery semantics

These are deliberately **undefined in M0**. The point at which an event counts
as accepted, and the delivery guarantee offered, are specified in M2 alongside
the durable pipeline ([ADR-005](../adr/ADR-005-delivery-semantics-deferred.md)).
PulseStream does not claim exactly-once processing.

## Operational plane (planned)

Separate from the data path, later milestones add:

- **Readiness that reflects real dependencies.** `/health/ready` gains checks as
  dependencies are implemented (database first). In M0 `checks` is empty
  because no dependencies exist yet.
- **Metrics.** Admission queue depth, in-flight work, retry and dead-letter
  counts, and latency distributions (M4/M5).
- **Operations dashboard.** A small frontend for inspecting pipeline state and
  dead-lettered events (M6).
- **Load and benchmark evidence** for any performance claim (M4).

## Architectural principles

1. **Boundedness.** No unbounded queue, channel, or task-spawning loop. Every
   buffer and concurrency limit has an explicit, configured capacity
   ([ADR-004](../adr/ADR-004-bounded-concurrency-backpressure.md)).
2. **Backpressure.** Overload produces explicit, observable behavior such as
   rejection, not uncontrolled memory growth or silently rising latency.
3. **Durability.** Once PulseStream acknowledges an event as accepted, that event
   survives process failure, according to acceptance semantics documented in M2.
4. **Idempotency.** Producer retries must not cause unintended duplicate logical
   processing.
5. **Explicit delivery semantics.** No exactly-once claims. The delivery
   guarantee will be stated precisely, including its failure cases.
6. **Failure visibility.** Failures become observable states (retrying,
   dead-lettered) rather than disappearing into logs.
7. **Measured performance.** Performance claims require benchmark or load-test
   evidence. Tuning changes (LTO, runtime thread counts, and so on) need data.

## Process model

Two binaries share one library crate:

```text
pulsestream-api ----+
                    +--> pulsestream-core (no HTTP, no DB, no runtime)
pulsestream-worker -+
```

Both use Tokio's multi-threaded runtime with default settings, log through
`tracing` filtered by `RUST_LOG`, and shut down on SIGINT/SIGTERM. The API stops
accepting connections and lets in-flight requests finish. The worker has no
work to drain in M0. Draining in-flight events is designed with the worker pool.

See [ADR-002](../adr/ADR-002-workspace-and-process-boundaries.md) for why these
boundaries were chosen.
