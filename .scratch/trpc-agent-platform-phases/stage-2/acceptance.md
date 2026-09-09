# Stage 2 Acceptance

Status: passed

## Scope

Stage 2 delivers immutable tenant-scoped Session Events, replayed Session/Summary state, Memory, InMemory/Redis/SQLite adapters, a PostgreSQL compatibility profile, server-controlled tenant backend selection, Redis-to-SQL migration, and the data-management increment of the existing frontend.

## Automated Gate

```bash
./format.sh
go test ./...
go test -race ./...
./lint.sh
go vet ./...
./build.sh
cd frontend
npm run typecheck
npm test -- --run
npm run build
npm run test:e2e
```

Real provider profile:

```bash
docker compose -f compose.stage2.yml up -d --wait
TRPC_TEST_REDIS_ADDR=127.0.0.1:6379 TRPC_TEST_POSTGRES_DSN='postgres://trpc:trpc@127.0.0.1:5432/trpc_agent?sslmode=disable' go test ./trpcservice/platform -run 'Test(RedisIntegrationProfile|PostgreSQLCompatibilityProfile)' -count=1
docker compose -f compose.stage2.yml down
```

## Proven Behavior

- Events are immutable, tenant/session scoped, idempotent, and monotonically sequenced under concurrency.
- Session and Summary state is deterministically replayed from events; Memory is tenant isolated.
- Redis provides shared cross-instance state; SQLite persists across reopen; PostgreSQL runs the shared adapter contract.
- Backend addresses and paths are server configured; clients select only authorized backend IDs. Unavailable configured storage never silently falls back to memory.
- Routed executions persist input/output/failure events for management inspection.
- Migration supports dry-run, bounded batches, transient retry, atomic checkpoint/resume, idempotent replay, cancellation, progress, and full Event/Memory count/content validation.
- Backend selection uses server-owned adapter leases and waits for active requests before closing a retired adapter; failure-event persistence is bounded and service-shutdown-aware.
- Migration and backend-selection failures expose sanitized public messages without backend addresses or filesystem paths.
- Desktop/mobile Playwright covers routed Session inspection, backend selection, tenant switching without stale cross-tenant data, layout overflow, and viewer mutation restrictions.

## Exclusions

Arbitrary SQL, event mutation, destructive audit editing, online provider cutover, and production support for every vector/object vendor remain excluded. Production identity, IM providers, policy governance, rollout, and rollback remain assigned to later stages.
