# PulseStream

A Rust event processing platform being built for bounded concurrency,
idempotency, retries, recovery, and observability.

> **Status: Milestone 1 (Event ingestion and backpressure).** PulseStream
> accepts events over HTTP into a **bounded, in-memory** pipeline and processes
> them with bounded concurrency.
>
> **Events are not durable in M1.** `202 Accepted` means the event was
> validated and admitted to in-memory processing. If the process crashes,
> accepted events that have not finished processing are lost. Durable
> acceptance, idempotency, and crash recovery arrive in M2. Delivery and
> idempotency semantics are defined in later milestones.

## Purpose

PulseStream aims to accept events over HTTP, store them durably, and process
them with a bounded worker pool, with explicit backpressure, idempotent
acceptance, retries, dead-lettering, crash recovery, and measured performance.
The design principles are listed in the
[architecture overview](docs/architecture/overview.md#architectural-principles).

## Current vs. planned

| Area | Current (M1) | Planned |
| --- | --- | --- |
| Ingestion | `POST /v1/events` with validation, 64 KiB body limit, and server-generated UUID v4 event IDs | Producer idempotency keys (M2) |
| Admission / backpressure | Bounded in-memory queue (default 256). When the queue is full: `429 QUEUE_FULL` with `Retry-After: 1` | Durable admission (M2). Tuning based on benchmarks (M4) |
| Processing | Bounded concurrency (default 4). The M1 processor performs no business action | Retries and dead-lettering (M3) |
| Shutdown | Closes admission, drains queued and in-flight events, bounded by a timeout | Draining durable work (M2) |
| Persistence | PostgreSQL container only. **Events are not persisted** | Event store, idempotency, recovery (M2) |
| Delivery semantics | **Undefined.** M1 acceptance is process-local and non-durable. No exactly-once claim | Precisely documented in M2 ([ADR-005](docs/adr/ADR-005-delivery-semantics-deferred.md)) |
| Observability | Structured lifecycle logs (event ID, type, source, state). Payloads are never logged | Metrics and an operations dashboard (M5/M6) |
| Performance | **Not benchmarked. No performance claims** | Benchmarks and load tests (M4) |

## Architecture

```text
Producer ─▶ POST /v1/events ─▶ bounded queue ─▶ dispatcher ─▶ ≤ N processing tasks ─▶ processed / failed (logged)
            validate, 202        try_send;       (API process in M1, in-memory, non-durable)
                                 full → 429
```

At most `queue_capacity + worker_concurrency` events are held in memory
(260 with the defaults). The API never waits for queue space: a full queue gets
an immediate `429`. See
[ADR-006](docs/adr/ADR-006-in-memory-bounded-admission.md).

The workspace has three crates ([ADR-002](docs/adr/ADR-002-workspace-and-process-boundaries.md)):

| Crate | Kind | Responsibility |
| --- | --- | --- |
| [`pulsestream-core`](crates/pulsestream-core) | library | Validated event model, service identity, env config. No HTTP, DB, or runtime deps |
| [`pulsestream-api`](crates/pulsestream-api) | binary | HTTP API. In M1 it also hosts the in-memory pipeline |
| [`pulsestream-worker`](crates/pulsestream-worker) | library + binary | Library: the bounded pipeline. Binary: idle until M2 gives it a durable event source |

- [Architecture overview](docs/architecture/overview.md)
- [ADRs](docs/adr/)
- [Roadmap](docs/backlog/roadmap.md)

## Requirements

- Rust **1.98.0**, pinned in [`rust-toolchain.toml`](rust-toolchain.toml)
  (rustup installs it automatically, with `rustfmt` and `clippy`). Install
  Rust through [rustup](https://rustup.rs), not a distro package.
  If your `stable` toolchain is already 1.98.0 and disk is tight, you can use
  it in this checkout without downloading a second copy:
  `rustup override set stable`. CI always uses the exact pin.
- A C linker (`build-essential` on Ubuntu).
- Docker with Compose v2, for local PostgreSQL. The M1 event pipeline and its
  tests do not need it.

## Local setup

```bash
git clone git@github.com:Ngetich-86/PulseStream.git
cd PulseStream
cp .env.example .env          # local-only placeholder values; .env is git-ignored
cargo build --workspace
```

### Configuration

| Variable | Used by | Default | Notes |
| --- | --- | --- | --- |
| `PULSESTREAM_API_BIND` | api | `127.0.0.1:8088` | Must be a socket address |
| `PULSESTREAM_QUEUE_CAPACITY` | api | `256` | Admission queue size, `1`–`65536` |
| `PULSESTREAM_WORKER_CONCURRENCY` | api | `4` | Maximum events processed at once, `1`–`64` |
| `PULSESTREAM_SHUTDOWN_TIMEOUT_MS` | api | `10000` | How long shutdown waits for the drain, `1`–`300000`. After this, remaining events are abandoned and logged |
| `DATABASE_URL` | api, worker | unset | Optional. When set, must be `postgres://` or `postgresql://` with a host. Never logged. Not used by the M1 pipeline |
| `RUST_LOG` | api, worker | `info` | `tracing-subscriber` `EnvFilter` syntax. An invalid filter logs a warning and uses `info` |

Invalid values stop startup with a clear error. They are never silently
replaced by defaults. The binaries read the process environment and do not
load `.env` themselves. Export the variables with `set -a; source .env; set +a`
or your shell tooling.

### Run the API

```bash
cargo run -p pulsestream-api

curl -s http://127.0.0.1:8088/health/live
# {"status":"ok","service":"pulsestream-api"}
curl -s http://127.0.0.1:8088/health/ready
# {"status":"ok","service":"pulsestream-api","checks":{"processing":"ready"}}

curl -s -H 'Content-Type: application/json' http://127.0.0.1:8088/v1/events \
  -d '{"source":"orders-api","event_type":"order.created","payload":{"order_id":"12345"}}'
# 202 {"event_id":"<uuid>","status":"accepted"}
```

On Ctrl+C or SIGTERM, the API shuts down in this order:

1. Admission closes. New events get `503`.
2. The HTTP server stops accepting connections and finishes in-flight requests.
3. Queued and in-flight events drain, for at most `PULSESTREAM_SHUTDOWN_TIMEOUT_MS`.

The process exits with code 0 after a complete drain, and with code 1 if the
timeout abandoned events.

### Event API

`POST /v1/events` with `Content-Type: application/json`:

```json
{ "source": "orders-api", "event_type": "order.created", "payload": { "order_id": "12345" } }
```

- `source`: 1–100 characters, no control characters.
- `event_type`: 1–150 characters, no control characters.
- `payload`: any non-null JSON value. It is opaque to PulseStream and never logged.
- No other fields are accepted. Clients cannot choose the event ID.

| Status | `code` | When |
| --- | --- | --- |
| `202` | (none) | Admitted to the in-memory pipeline. Body: `{"event_id","status":"accepted"}` |
| `400` | `INVALID_EVENT` | Malformed JSON, wrong shape, or a failed validation rule |
| `413` | `PAYLOAD_TOO_LARGE` | Body over 64 KiB, with or without `Content-Length` |
| `415` | `UNSUPPORTED_MEDIA_TYPE` | Missing or non-JSON `Content-Type` |
| `429` | `QUEUE_FULL` | Queue at capacity. Sent with `Retry-After: 1`. The event was **not** admitted |
| `503` | `PROCESSING_UNAVAILABLE` | Pipeline shutting down or stopped. The event was **not** admitted |

Errors use the shape `{"code": "...", "message": "..."}`. `202` means
*admitted*, not *processed*. There is no status-lookup endpoint yet, because
nothing records event state durably.

`GET /health/ready` returns `200` with `checks.processing = "ready"` while
events can be admitted. Otherwise it returns `503` with `"unavailable"`.

### Run the worker

```bash
cargo run -p pulsestream-worker
```

The worker binary has no event source in M1, because admission is in-memory
inside the API process. It idles without polling until Ctrl+C or SIGTERM. From
M2 it will consume durably accepted events.

### PostgreSQL (Docker)

```bash
docker compose up -d postgres
docker compose ps                  # wait for (healthy)
docker compose down                # stops the container, keeps the data volume
```

The service uses `postgres:18.6-alpine` on `127.0.0.1:55432` with the named volume
`pulsestream_postgres-data`. See [infra/postgres](infra/postgres/README.md).
`docker compose down -v` deletes local data. M1 does not use the database.

## Tests and quality gates

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

See [tests/README.md](tests/README.md) for what the tests cover. Concurrency
and backpressure tests coordinate through semaphores, not sleeps.

### Disk footprint

`[profile.dev]` and `[profile.test]` disable incremental compilation and keep
only line-table debug info, because local WSL disk is constrained. The release
profile uses Cargo's defaults. Performance tuning waits for benchmark evidence.

## CI

[GitHub Actions](.github/workflows/ci.yml) runs on pull requests to and pushes
to `main`, with three jobs: `rust-quality` (rustfmt and clippy with `-D warnings`),
`rust-test`, and `rust-build`. All jobs run against the pinned toolchain with
`--locked`.

## Current limitations

- **Events are not durable in M1.** Accepted events can be lost if the process crashes.
- There is no producer idempotency yet. A retried request creates a new event.
- There are no retries and no dead-letter queue yet. A failed event is logged and dropped.
- There is no event status lookup and no persisted event state.
- Performance has not been benchmarked or load-tested yet. No throughput or latency claims are made.
- Delivery and idempotency semantics are defined in later milestones.

## Roadmap

See [docs/backlog/roadmap.md](docs/backlog/roadmap.md). M2 adds durable
acceptance, idempotency, and recovery.

## Contributors

PulseStream is being developed collaboratively by **Ngetich-86**, **LMichy1**,
and **MurayaSoftTouch**. Milestone ownership and actual contributions are
preserved through Git history and pull requests.
