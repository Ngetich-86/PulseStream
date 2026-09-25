# Tests

Tests live beside the code they exercise (`#[cfg(test)]` modules). None of
them need Docker or PostgreSQL.

| Crate | Tests | What is tested |
| --- | --- | --- |
| `pulsestream-core` | 16 | Event validation (empty or whitespace labels, length limits counted in characters, control characters, null payload, unique IDs). Config parsing: bind address, database URL and redaction, pipeline limits with defaults, range bounds, and rejection of `0`, values over the maximum, and non-integers |
| `pulsestream-api` | 18 | Health: live, ready, unavailable after the pipeline stops, 404 on unknown routes. `POST /v1/events`: 202 plus delivery to the processor; every validation error; malformed JSON; 415; 413 with `Content-Length` and with a streamed body; exactly 64 KiB accepted; 429 `QUEUE_FULL` with `Retry-After` and no admission; 503 when closed. Real TCP serve followed by graceful shutdown |
| `pulsestream-worker` | 6 + 1 | Pipeline: concurrency bound on a 4-thread runtime, a full queue rejecting without dropping admitted events, shutdown drain rejecting new events, drain timeout abandoning stuck work, failures and panics counted without stopping processing, and 2,000 events completing with a bounded task count. Binary: runs until shutdown is signalled |

## Deterministic concurrency testing

`pulsestream_worker::testing::GatedProcessor` (enabled by the `test-util`
feature and in the crate's own tests) records `current`, `max_observed`,
`started`, and `completed` counts. Optionally, it blocks every event on a
semaphore until the test releases it. Tests wait on "N events started"
signals instead of sleeping. For example, the concurrency test:

1. admits 20 events with `worker_concurrency = 4`;
2. waits until exactly 4 have started;
3. checks that no fifth starts while the gate is shut;
4. releases all 20, then asserts `max_observed == 4` and `completed == 20`.

The only wall-clock timeouts are upper bounds that fail a hung test, plus the
drain-timeout test, where the 50 ms timeout is the behavior under test.

The 2,000-event test checks correctness, not performance. It reports no
throughput numbers.

## Running

```bash
cargo test --workspace
```

This top-level directory is reserved for cross-process and end-to-end tests,
such as API to PostgreSQL to worker. Those arrive with the durable pipeline
(M2 onward).
