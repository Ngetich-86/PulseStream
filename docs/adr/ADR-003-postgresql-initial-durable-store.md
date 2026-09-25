# ADR-003: PostgreSQL as initial durable state store

- Status: Accepted
- Date: 2026-09-25
- Milestone: M0

## Context

PulseStream needs durable acceptance of events, idempotency (unique producer
keys), per-event lifecycle state, retry scheduling, dead-lettering, and crash
recovery (reclaiming abandoned work). Brokers such as Kafka, Redpanda, NATS,
RabbitMQ, or Redis Streams could supply some of this, but each adds an
operational system, a second consistency boundary, and failure modes that would
need to be reconciled against a database anyway.

## Decision

Use **PostgreSQL** as the single durable store for the first version. It gives
us transactional writes, unique constraints for idempotency, and row-level
locking (for example `FOR UPDATE SKIP LOCKED`) for claiming work. One system
holds all event state.

Locally it runs through Docker Compose (`postgres:18.6-alpine`, named volume
`pulsestream_postgres-data`). M0 provides only the container and its health
check. The schema is designed in M2.

## Consequences

- One durable system to operate, back up, and reason about.
- Throughput is bounded by PostgreSQL write and claim capacity. The M4 benchmarks
  must measure whether that is sufficient.
- **A message broker is added only with architectural evidence**, such as
  benchmark results showing PostgreSQL cannot meet a stated requirement.
  Adding one requires a new ADR.
- No database client dependency is added until code actually connects (M2).
