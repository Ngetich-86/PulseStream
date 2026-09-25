# PostgreSQL (local development)

PulseStream uses PostgreSQL as its initial durable state store
([ADR-003](../../docs/adr/ADR-003-postgresql-initial-durable-store.md)). Locally it
runs through Docker Compose. Do not install PostgreSQL directly in WSL.

| Setting | Value |
| --- | --- |
| Compose service | `postgres` |
| Image | `postgres:18.6-alpine` (PostgreSQL 18, current supported major) |
| Host port | `127.0.0.1:55432` (override with `PULSESTREAM_POSTGRES_PORT`) |
| Named volume | `pulsestream_postgres-data` |
| Health check | `pg_isready` every 5 s |

```bash
docker compose up -d postgres
docker compose ps          # wait for "(healthy)"
docker compose down        # keeps the volume
```

`docker compose down -v` **deletes all local PulseStream data**. Only use it deliberately.

## M0 scope

M0 provides infrastructure only. It has no schema, no migrations, and no
application connections. The event store, idempotency keys, retry state, and
dead-letter tables are designed in M2 and M3. Migrations will live in this
directory, or in a crate-owned location chosen then.
