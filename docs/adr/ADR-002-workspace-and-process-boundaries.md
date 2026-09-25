# ADR-002: Rust workspace and process boundaries

- Status: Accepted
- Date: 2026-09-25
- Milestone: M0

## Context

Ingestion and processing have different scaling and failure profiles. The
ingestion path must answer producers quickly and apply admission control. The
processing path runs longer work, retries, and recovery. We want the boundary to
be clear from day one without building a fleet of microservices.

## Decision

Use one Cargo workspace with three crates:

| Crate | Kind | Responsibility |
| --- | --- | --- |
| `pulsestream-core` | library | Domain concepts and shared config/errors. **No HTTP, database, or runtime dependencies.** |
| `pulsestream-api` | binary | HTTP surface: health (M0), ingestion, validation, admission and backpressure (M1+). |
| `pulsestream-worker` | binary | Processing lifecycle: bounded execution, retries, dead-lettering, recovery (M1+). |

The API and worker run as **separate processes**. They will communicate only
through the durable store ([ADR-003](ADR-003-postgresql-initial-durable-store.md)),
not through in-memory channels. A crash in one therefore cannot silently lose
work that the other has accepted.

## Consequences

- Each process can be scaled, restarted, and reasoned about on its own.
- A single `Cargo.lock` and shared `[workspace.dependencies]` keep versions consistent.
- Small process-lifecycle helpers (signal handling, logging setup) are duplicated
  in both binaries instead of pulling runtime dependencies into `core`. If
  shared runtime code grows beyond these helpers, we extract a dedicated
  internal crate, and record that decision in a new ADR.
- We add more crates only when a boundary is justified, not in advance.
