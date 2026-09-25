# Tests

M0 tests live beside the code they exercise (`#[cfg(test)]` modules), because
each crate's surface is still small:

| Crate | What is tested |
| --- | --- |
| `pulsestream-core` | Service identity strings, configuration parsing and validation, credential redaction |
| `pulsestream-api` | `/health/live` and `/health/ready` response shape, unknown-route handling, real TCP serve followed by graceful shutdown |
| `pulsestream-worker` | Worker stays running until shutdown is signalled, then stops |

Run everything with:

```bash
cargo test --workspace
```

This top-level directory is reserved for cross-process and end-to-end tests,
such as API to PostgreSQL to worker. Those tests arrive once there is a durable
pipeline to exercise (M2 onward).
