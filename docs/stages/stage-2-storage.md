# Stage 2 Storage And Migration

Stage 2 adds immutable tenant-scoped Session Events, replayed Session/Summary state, Memory records, and selectable InMemory, Redis, SQLite, and PostgreSQL-compatible storage implementations. Routed executions persist input and output/failure events before they are exposed in the Management Console.

## Local Profiles

Start Redis and PostgreSQL:

```bash
docker compose -f compose.stage2.yml up -d
TRPC_TEST_REDIS_ADDR=127.0.0.1:6379 \
TRPC_TEST_POSTGRES_DSN='postgres://trpc:trpc@127.0.0.1:5432/trpc_agent?sslmode=disable' \
go test ./trpcservice/platform
```

Backend selection is tenant-scoped and server-authorized. Configure the selectable catalog with `TRPC_REDIS_ADDR` and `TRPC_SQLITE_PATH`, and set `TRPC_BACKEND_SELECTIONS` to a writable JSON control-plane file when selection must survive service restart. Clients choose only server-defined backend IDs; addresses and paths are never accepted or returned by the API.

Backend construction, tenant selection, and closure are server-owned. Requests hold a lease on the selected adapter; switching a tenant backend or shutting down the service waits for active leases before closing the old adapter. Routed failure events use a short, service-owned timeout so client cancellation cannot remove persistence bounds and service shutdown can cancel a blocked write.

Migration endpoints are server-owned. Configure `TRPC_MIGRATION_REDIS_ADDR`, `TRPC_MIGRATION_SQLITE_PATH`, and `TRPC_MIGRATION_CHECKPOINT_PATH`; tenant administrators can start and inspect jobs but cannot submit network addresses or filesystem paths.

## Redis-To-SQL Migration

Dry-run and execute with the same arguments:

```bash
go run ./cmd/storage-migrate -tenant tenant-dev -redis 127.0.0.1:6379 -sqlite data/stage2.db -dry-run
go run ./cmd/storage-migrate -tenant tenant-dev -redis 127.0.0.1:6379 -sqlite data/stage2.db
```

The command migrates in deterministic Session order, retries transient writes, persists an atomic checkpoint, resumes after interruption, and validates destination record counts plus versioned, length-delimited checksums over full Event and Memory content. Event checksums include identity, sequence, type, payload, idempotency key, and occurrence time; Memory checksums include identity, key, value, and update time. Replays are idempotent because destination events retain their source identity and timestamps.

## Contracts And Limits

- Event identity is scoped to Tenant, Session, and idempotency key. A replay with different content is rejected.
- Sequences are allocated atomically per Tenant and Session by Redis transactions or SQL constraints.
- Session and Summary state is rebuilt from immutable events; events cannot be updated through the API.
- Migration and backend-selection failures expose stable public error codes and sanitized messages; driver, network, and filesystem diagnostics remain internal.
- The data page remounts on Tenant Context changes and renders backend health independently of Session, Event, and Memory load failures.
- Arbitrary SQL and destructive event editing are excluded. Redis-to-SQL supports source freezing, complete validation, and optional atomic cutover; Qdrant generations can be rebuilt from authoritative Knowledge; S3 Artifact content uses version and checksum validation. Cross-provider incremental outbox and automatic cleanup remain production extensions.
- Redis and PostgreSQL integration profiles require the explicitly documented environment variables; local unit tests use deterministic isolated providers.

Backend Profile, S3, Qdrant, Mem0, and shared Audit Store configuration is documented in [the backend adapter guide](../backend-adapters.md). A real local integration profile is available in `compose.data.yml`.
