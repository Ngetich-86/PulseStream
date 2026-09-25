# PulseStream Architecture Overview

Status: **M1 (Event ingestion and backpressure)**. This document describes the
intended architecture and marks what exists today. **IMPLEMENTED** means it is
in the code and covered by tests. **PLANNED** means it is a design intention
only.

## Data plane

### Implemented in M1

```text
Producer / Client
      |
      v
POST /v1/events ................ IMPLEMENTED: validation, 64 KiB limit, UUID v4 event ID, 202/400/413/415/429/503
      |
      v
Bounded admission queue ........ IMPLEMENTED: Tokio mpsc(queue_capacity), try_send; a full queue returns 429
      |
      v
Dispatcher + bounded tasks ..... IMPLEMENTED: at most worker_concurrency events in process at once
      |
      +--> processed ........... IMPLEMENTED (logged only; no business action)
      |
      +--> failed .............. IMPLEMENTED (logged and counted; not retried)
```

In M1 this whole path runs **in memory, inside the API process**
([ADR-006](../adr/ADR-006-in-memory-bounded-admission.md)). It is not durable.

### Target architecture

```text
Producer / Client
      |
      v
PulseStream API ................ IMPLEMENTED (M1)
      |
      v
Bounded Admission Layer ........ IMPLEMENTED in memory (M1); durable admission PLANNED (M2)
      |
      v
Durable Event Store ............ PLANNED (M2): PostgreSQL container exists; no schema
      |
      v
Bounded Worker Pool ............ IMPLEMENTED in-process (M1); consuming from the durable store PLANNED (M2)
      |
      +--> success
      |
      +--> retry ............... PLANNED (M3)
      |
      +--> dead-letter ......... PLANNED (M3)
```

### Components

| Component | Where | Status |
| --- | --- | --- |
| HTTP ingestion | `pulsestream-api` (`events.rs`) | **IMPLEMENTED.** Validation, body limit, stable error codes |
| Event model | `pulsestream-core` (`event.rs`) | **IMPLEMENTED.** `EventId`, `EventSource`, `EventType`, `Event`. HTTP DTOs are kept separate, in the API |
| Bounded admission | `pulsestream-worker` (`pipeline.rs`) | **IMPLEMENTED** in memory. Durable admission is **PLANNED** (M2) |
| Bounded processing | `pulsestream-worker` (`pipeline.rs`) | **IMPLEMENTED.** The processor is a no-op acknowledgement |
| Shutdown drain | API `main.rs` + `Pipeline::shutdown` | **IMPLEMENTED.** Bounded by a timeout |
| Readiness | `GET /health/ready` | **IMPLEMENTED.** The `processing` check |
| Durable event store | PostgreSQL | **PLANNED** (M2). Only the local container exists |
| Idempotency and deduplication | | **PLANNED** (M2) |
| Crash recovery | | **PLANNED** (M2) |
| Retry scheduler and dead-letter | | **PLANNED** (M3) |
| Benchmarks and load tests | | **PLANNED** (M4) |
| Operations dashboard | | **PLANNED** (M6) |

### Boundedness

```text
maximum logical pipeline occupancy = queue_capacity + worker_concurrency
                                   = 256 + 4 = 260 events (defaults)
```

- The API never waits for queue space. `try_send` either admits the event or
  returns `429`.
- The dispatcher dequeues only when it has a free processing slot. A slot is
  freed only after the dispatcher joins the finished task, so tasks never
  exceed `worker_concurrency`.
- This bounds the number of events held, not exact memory. Each event is
  capped by the 64 KiB request limit plus fixed overhead.

This is why an overloaded PulseStream answers `429` instead of building an
unlimited in-memory backlog ([ADR-004](../adr/ADR-004-bounded-concurrency-backpressure.md)).

### Acceptance and delivery semantics

- **M1:** `202 Accepted` means *validated and admitted to the bounded
  in-memory pipeline*. M1 acceptance is process-local and non-durable. A
  process crash may lose accepted but unfinished events. There is no
  deduplication, so a retried request becomes a new event.
- **M2:** durable acceptance begins, and the delivery guarantee is documented
  precisely ([ADR-005](../adr/ADR-005-delivery-semantics-deferred.md)).
  PulseStream does not claim exactly-once processing.

### Shutdown

1. On SIGINT/SIGTERM, admission closes and new events get `503`.
2. The HTTP server stops accepting connections and finishes in-flight requests.
3. The queue receiver closes and buffered events drain.
4. In-flight tasks finish.
5. The drain is bounded by `PULSESTREAM_SHUTDOWN_TIMEOUT_MS`. On timeout, the
   remaining tasks are aborted, abandoned counts are logged, and the process
   exits with code 1.

## Operational plane

- **Readiness (IMPLEMENTED).** `/health/ready` reports `processing`: `ready`
  while events can be admitted, and `unavailable` with HTTP 503 once admission
  is closed or the dispatcher has stopped. A database check is added when the
  event path first uses PostgreSQL (M2).
- **Lifecycle logs (IMPLEMENTED).** Each event logs admitted, processing
  started, completed, or failed, with event ID, type, source, and state.
  Queue-full rejections, drain start and complete, and drain timeout are also
  logged. Payloads are never logged.
- **Metrics (PLANNED, M4/M5).** Queue depth, in-flight work, retry and
  dead-letter counts, latency distributions.
- **Operations dashboard (PLANNED, M6).**
- **Load and benchmark evidence (PLANNED, M4)** for any performance claim.

## Architectural principles

1. **Boundedness.** No unbounded queue, channel, or task-spawning loop. Every
   buffer and concurrency limit has an explicit, configured capacity
   ([ADR-004](../adr/ADR-004-bounded-concurrency-backpressure.md)).
2. **Backpressure.** Overload produces explicit, observable behavior such as
   rejection, not uncontrolled memory growth or silently rising latency.
3. **Durability.** Once PulseStream acknowledges an event as durably accepted,
   that event survives process failure. M1 acceptance is explicitly *not*
   durable. Durable acceptance and its semantics arrive in M2.
4. **Idempotency.** Producer retries must not cause unintended duplicate logical
   processing. This is not yet implemented (M2).
5. **Explicit delivery semantics.** No exactly-once claims. The delivery
   guarantee will be stated precisely, including its failure cases.
6. **Failure visibility.** Failures become observable states (retrying,
   dead-lettered) rather than disappearing into logs. In M1, failures are
   still log-only. Durable failure states arrive in M2 and M3.
7. **Measured performance.** Performance claims require benchmark or load-test
   evidence. Tuning changes (LTO, runtime thread counts, queue sizes) need data.

## Process model

```text
pulsestream-api ────┬──> pulsestream-worker (lib: bounded pipeline) ──┐
                    └─────────────────────────────────────────────────┴──> pulsestream-core
pulsestream-worker (bin) ──> pulsestream-core      (idle until M2)
```

`pulsestream-core` has no HTTP, database, or runtime dependencies. Both
binaries use Tokio's multi-threaded runtime with default settings, and log
through `tracing` filtered by `RUST_LOG`. See
[ADR-002](../adr/ADR-002-workspace-and-process-boundaries.md) for the process
boundaries and [ADR-006](../adr/ADR-006-in-memory-bounded-admission.md) for why
M1 runs the pipeline inside the API process.
