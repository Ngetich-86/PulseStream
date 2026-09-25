# PulseStream Roadmap

This is a **plan**, not a record of completed work. Actual contributions,
reviews, and ownership are preserved in Git history and pull requests.

| Milestone | Scope | Owner | Reviewer |
| --- | --- | --- | --- |
| **M0** | Foundation / architecture | Ngetich-86 | LMichy1 (requested) |
| **M1** | Event ingestion + bounded concurrency | Ngetich-86 | LMichy1 |
| **M2** | Persistence + idempotency + recovery | LMichy1 | MurayaSoftTouch |
| **M3** | Retry / dead-letter / failure handling | MurayaSoftTouch | Ngetich-86 |
| **M4** | Performance + backpressure + benchmarks | Ngetich-86 | LMichy1 |
| **M5** | Operational / security / reliability hardening | MurayaSoftTouch | LMichy1 |
| **M6** | Operations dashboard + end-to-end integration | LMichy1 | Ngetich-86 |
| **M7** | Release readiness | Collaborative | Collaborative |

## Milestone notes

- **M0.** Rust workspace, API and worker skeletons, health endpoints, env
  config, structured logging, graceful shutdown, local PostgreSQL, CI, ADRs.
- **M1.** Ingestion endpoint, validation, bounded admission with explicit
  overload responses ([ADR-004](../adr/ADR-004-bounded-concurrency-backpressure.md)).
- **M2.** Event schema and migrations, the acceptance point, idempotency keys,
  work claiming, crash recovery, and **the delivery semantics document**
  ([ADR-005](../adr/ADR-005-delivery-semantics-deferred.md)).
- **M3.** Retry policy with backoff, dead-letter state, and failure visibility.
- **M4.** Benchmarks and load tests. Evidence-driven tuning, and the broker
  question ([ADR-003](../adr/ADR-003-postgresql-initial-durable-store.md)).
- **M5.** Security review, dependency policy, metrics, and operational runbooks.
- **M6.** Small operations frontend and end-to-end tests.
- **M7.** Release checklist, documentation pass, versioning.
