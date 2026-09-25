# PulseStream

A Rust event processing platform being built for bounded concurrency,
idempotency, retries, recovery, and observability.

> **Status: Milestone 0 (Foundation).** This repository currently contains the
> workspace, service skeletons, health endpoints, local PostgreSQL
> infrastructure, CI, and architecture decisions. **It does not ingest or
> process events yet.** Delivery and idempotency semantics are defined in later
> milestones.

## Purpose

PulseStream aims to accept events over HTTP, store them durably, and process
them with a bounded worker pool, with explicit backpressure, idempotent
acceptance, retries, dead-lettering, crash recovery, and measured performance.
The design principles are listed in the
[architecture overview](docs/architecture/overview.md#architectural-principles).

## Current vs. planned

| Area | Current (M0) | Planned |
| --- | --- | --- |
| API | `GET /health/live`, `GET /health/ready` | Event ingestion and validation (M1) |
| Admission / backpressure | Not implemented | Bounded admission with explicit overload responses (M1, M4) |
| Persistence | PostgreSQL container with health check. No schema, no app connection | Event store, idempotency, recovery (M2) |
| Worker | Starts, idles without polling, shuts down cleanly | Bounded processing pool (M1/M2), retries and dead-letter (M3) |
| Delivery semantics | **Undefined.** No exactly-once claim | Precisely documented in M2 ([ADR-005](docs/adr/ADR-005-delivery-semantics-deferred.md)) |
| Observability | Structured `tracing` logs, `RUST_LOG` filter | Metrics and an operations dashboard (M5/M6) |
| Performance | No benchmarks, **no performance claims** | Benchmarks and load tests (M4) |

## Architecture

```text
Producer ─▶ API ─▶ Bounded Admission ─▶ Durable Store (PostgreSQL) ─▶ Bounded Worker Pool ─▶ success / retry / dead-letter
            M0:     planned              M0: container only            M0: lifecycle only       planned
            health
```

The workspace has three crates ([ADR-002](docs/adr/ADR-002-workspace-and-process-boundaries.md)):

| Crate | Kind | Responsibility |
| --- | --- | --- |
| [`pulsestream-core`](crates/pulsestream-core) | library | Service identity and env config validation. No HTTP, DB, or runtime deps |
| [`pulsestream-api`](crates/pulsestream-api) | binary | HTTP API (health endpoints in M0) |
| [`pulsestream-worker`](crates/pulsestream-worker) | binary | Processing lifecycle (idle skeleton in M0) |

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
- Docker with Compose v2, for local PostgreSQL.

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
| `PULSESTREAM_API_BIND` | api | `127.0.0.1:8088` | Must be a socket address. Invalid values stop startup |
| `DATABASE_URL` | api, worker | unset | Optional in M0. When set, must be `postgres://` or `postgresql://` with a host. Never logged |
| `RUST_LOG` | api, worker | `info` | `tracing-subscriber` `EnvFilter` syntax. An invalid filter logs a warning and uses `info` |

The binaries read the process environment. They do not load `.env` themselves.
Export the variables with `set -a; source .env; set +a` or your shell tooling.

### Run the API

```bash
cargo run -p pulsestream-api
curl -s http://127.0.0.1:8088/health/live    # {"status":"ok","service":"pulsestream-api"}
curl -s http://127.0.0.1:8088/health/ready   # {"status":"ok","service":"pulsestream-api","checks":{}}
```

`checks` is empty because M0 has no runtime dependencies. Readiness gains real
dependency checks as components are implemented. Stop with Ctrl+C or SIGTERM.
The server stops accepting connections and finishes in-flight requests.

### Run the worker

```bash
cargo run -p pulsestream-worker
```

The worker logs its startup state and idles until Ctrl+C or SIGTERM, then exits
cleanly. It does not process events in M0. Draining in-flight work on shutdown
is designed with the worker pool.

### PostgreSQL (Docker)

```bash
docker compose up -d postgres
docker compose ps                  # wait for (healthy)
docker compose down                # stops the container, keeps the data volume
```

The service uses `postgres:18.6-alpine` on `127.0.0.1:55432` with the named volume
`pulsestream_postgres-data`. See [infra/postgres](infra/postgres/README.md).
`docker compose down -v` deletes local data.

## Tests and quality gates

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

See [tests/README.md](tests/README.md) for what the tests cover.

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

- No event ingestion, storage, processing, retries, or dead-lettering.
- No database schema or application database connection.
- `/health/ready` performs no dependency checks, because none exist yet.
- No metrics, benchmarks, or load tests. No performance claims are made.
- Delivery and idempotency semantics are defined in later milestones.

## Roadmap

See [docs/backlog/roadmap.md](docs/backlog/roadmap.md). M1 adds event ingestion
with bounded admission.

## Contributors

PulseStream is being developed collaboratively by **Ngetich-86**, **LMichy1**,
and **MurayaSoftTouch**. Milestone ownership and actual contributions are
preserved through Git history and pull requests.
