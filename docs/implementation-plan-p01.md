# P0 Implementation Plan

## Objective

Implement the first production-shaped slice of the platform while keeping every commit independently buildable and testable. The P0 slice will establish tenant-safe domain contracts, durable persistence boundaries, atomic idempotency and Session serialization, a real tRPC-Agent-Go runtime adapter, asynchronous Gateway/Worker execution, and durable Outbox retry handling. Existing `EchoResponder` remains only as an explicit local-development fallback.

## Fixed Decisions

- Use tRPC-Agent-Go as the Worker-side Agent runtime; do not expose framework-specific types from tenant, channel, or storage interfaces.
- Use PostgreSQL as the durable fact source for tenants, AgentApps, ChannelBindings, Sessions, SessionEvents, Memory, Summary, Artifact metadata, AuditLog, and Outbox.
- Use Redis for atomic deduplication, Session leases, fencing tokens, distributed rate limits, hot state, and queue support.
- Use `TenantContext` after verified Binding resolution and pass it through Gateway, Job, Worker, Runner, Tool, Store, Audit, and Outbox.
- Enforce `tenant_id` in all primary keys, unique constraints, foreign keys, cache keys, object keys, vector metadata, and Repository queries. Repository implementations must revalidate Context/resource tenant equality.
- Generate `session_id` with versioned SHA-256 over `tenant_id|channel|binding_id|scope`; private chats use user scope, groups use chat scope, and Telegram topics include thread ID.
- Claim inbound messages atomically by `(tenant_id, channel, binding_id, external_message_id)`. Use claim lease, owner, attempts, response reference, and fencing token; repeated messages return the persisted result and never rerun Agent execution.
- Serialize one Session with queue partitioning or a Redis lease. Lease expiration cancels the execution; fencing tokens prevent stale Workers from committing.
- Keep model/Tool execution outside SQL transactions. Commit event, Session CAS update, assistant message, and required Outbox records in bounded transactions.
- Use Outbox for Agent jobs, IM replies, Memory indexing, and audit delivery. Retry transient failures with bounded exponential backoff and place permanent/exhausted failures in DLQ.

## P0-01: Dependency and Quality Baseline

- Goal: pin compatible Go, tRPC-Agent-Go, Redis, PostgreSQL, and OTel dependencies and establish CI checks.
- Prerequisites: none.
- Modify: `go.mod`, `go.sum`, `.github/workflows/`, `lint.sh`, `coverage.sh`.
- Interfaces/types: no business API; dependency/version configuration only.
- Tests: `go test ./...`, `go vet ./...`, formatting, and static checks in CI.
- Acceptance: `go test ./... && go vet ./... && test -z "$(gofmt -l .)"`.
- Do not modify: runtime behavior, routes, domain semantics, or database data.
- Risks: framework/API and Go-version incompatibility. Rollback by restoring module and CI files.

## P0-02: Tenant Domain and TenantContext

- Goal: define and validate Tenant, AgentApp, ChannelBinding, BackendPolicy, configuration versioning, and immutable request context.
- Prerequisites: P0-01.
- Modify: `trpcservice/tenant/`, `trpcservice/config/`, `trpcservice/platform/`.
- Interfaces/types: `Tenant`, `AgentApp`, `ChannelBinding`, `TenantContext`, `TenantResolver`, `BackendPolicy`.
- Tests: tenant status/config validation, verified Binding mapping, missing/mismatched Context, cross-tenant denial.
- Acceptance: `go test ./trpcservice/tenant ./trpcservice/config ./trpcservice/platform -race`.
- Do not modify: concrete DB clients, IM protocol handling, and Runner implementation.
- Risks: trusting request-provided tenant IDs. Rollback by disabling the new resolver and retaining development tenant initialization.

## P0-03: Domain Entities and State Rules

- Goal: implement Session, SessionEvent, Memory, Summary, Artifact, AuditLog, event types, state transitions, and version fields.
- Prerequisites: P0-02.
- Modify: `trpcservice/session/`, `trpcservice/memory/`, `trpcservice/artifact/`, `trpcservice/audit/`.
- Interfaces/types: entity structs, event/state enums, validation functions, `ExecutionID` and attempt metadata.
- Tests: field validation, legal/illegal state transitions, event ordering metadata, version monotonicity, audit metadata redaction.
- Acceptance: package unit tests pass with `-race` where applicable.
- Do not modify: concrete persistence, Redis locking, and IM adapters.
- Risks: coupling to framework-internal types. Rollback by removing the new domain packages and preserving compatibility DTOs.

## P0-04: Identity, Session ID, Dedup, and Storage Contracts

- Goal: standardize all IDs and Repository contracts before concrete backend work.
- Prerequisites: P0-02 and P0-03.
- Modify: `trpcservice/session/`, `trpcservice/storage/`, `trpcservice/platform/`.
- Interfaces/types: `SessionKey`, `DedupKey`, `IdempotencyRepository`, `SessionRepository`, `MemoryRepository`, `SummaryRepository`, `ArtifactRepository`, `AuditRepository`, `OutboxRepository`.
- Tests: deterministic ID generation, tenant/Binding/topic isolation, invalid input rejection, stable key serialization.
- Acceptance: `go test ./trpcservice/session ./trpcservice/storage -race`.
- Do not modify: existing Web field names or implement Redis/SQL clients in this task.
- Risks: changing the ID algorithm after data exists. Use a `v1` prefix and preserve compatibility forever for existing data.

## P0-05: PostgreSQL Schema and Migrations

- Goal: create repeatable migrations for tenant, AgentApp, Binding, identity, Session, event, dedup, Memory, Summary, Artifact, Audit, Outbox, and DLQ tables.
- Prerequisites: P0-02 through P0-04.
- Modify: `migrations/`, `trpcservice/storage/postgres/`, migration scripts.
- Interfaces/types: `Migrator`, `PostgresConfig`, `MigrationVersion`.
- Tests: repeated migration, unique constraints, tenant-qualified foreign keys, transaction rollback, event sequence uniqueness.
- Acceptance: start test PostgreSQL, apply migrations, run PostgreSQL tests.
- Do not modify: production data or Agent execution code.
- Risks: long migration locks and invalid composite foreign keys. Rollback only through tested down/forward migration after backup.

## P0-06: Redis Deduplication, Lease, Fencing, and Rate Limiting

- Goal: implement atomic `Claim`, Session lease acquisition/renewal/release, fencing tokens, and tenant/Bot/Chat rate limits.
- Prerequisites: P0-04 and P0-05.
- Modify: `trpcservice/storage/redis/`, `trpcservice/queue/`, `trpcservice/ratelimit/`.
- Interfaces/types: `RedisIdempotencyRepository`, `Lease`, `FenceToken`, `RateLimiter`, `ClaimStatus`.
- Tests: 100-way Claim competition with one winner, lease takeover after expiry, stale fence rejection, rate-limit windows, cancellation on renewal failure.
- Acceptance: start Redis and run package tests with `-race`.
- Do not modify: model execution and concrete IM behavior.
- Risks: Redis partition and clock skew. Rollback by switching Claim/lease to PostgreSQL fallback and preserving dedup records.

## P0-07: Real tRPC-Agent-Go Runtime Adapter

- Goal: build tenant-scoped AgentFactory and AgentRuntime using tRPC-Agent-Go Runner while retaining Echo fallback for local mode.
- Prerequisites: P0-01 through P0-04.
- Modify: `trpcservice/agent/`, `trpcservice/tool/`, `trpcservice/platform/`.
- Interfaces/types: `AgentFactory`, `AgentRuntime`, `AgentInput`, `AgentResult`, `RunnerEvent`, runtime dependency bundle.
- Tests: successful response, model failure, Tool event propagation, Context cancellation, event-channel draining, tenant-specific Agent configuration.
- Acceptance: runtime package tests and race tests pass without external model credentials by using a fake provider.
- Do not modify: IM webhook verification or plaintext secret handling.
- Risks: tRPC-Agent-Go API changes and goroutine leaks. Rollback with explicit `MODEL_PROVIDER=echo` mode.

## P0-08: Gateway, AgentJob, Queue, and Worker

- Goal: implement fast ACK Gateway, durable AgentJob creation, Worker consumption, Context/trace restoration, Session lease, Runner invocation, and bounded shutdown.
- Prerequisites: P0-04, P0-06, P0-07.
- Modify: `trpcservice/gateway/`, `trpcservice/worker/`, `trpcservice/queue/`, `trpcservice/web/`.
- Interfaces/types: `AgentJob`, `JobQueue`, `GatewayHandler`, `Worker`, `ExecutionState`, `DrainController`.
- Tests: ACK does not wait for Agent; IDs survive queue boundary; same Session serializes; different Sessions run concurrently; worker crash allows lease takeover; SIGTERM cancels in-flight Context.
- Acceptance: Gateway/Worker/Queue tests pass with `-race` and fake Runner/Queue.
- Do not modify: complex admin UI and real IM protocol implementation.
- Risks: ACK after lost job and stale Worker writes. Use Job Outbox and fencing; rollback to development synchronous mode if necessary.

## P0-09: Outbox Dispatcher, Retry, and Dead Letter

- Goal: reliably deliver Agent jobs, IM replies, Memory index operations, and audits with bounded retries and DLQ.
- Prerequisites: P0-05, P0-06, P0-08.
- Modify: `trpcservice/outbox/`, `trpcservice/queue/`, `trpcservice/audit/`.
- Interfaces/types: `OutboxMessage`, `OutboxDispatcher`, `RetryPolicy`, `DeadLetterRecord`, `Sender`.
- Tests: competing `SKIP LOCKED` consumers, exponential backoff, permanent failure to DLQ, restart recovery, idempotent send ownership.
- Acceptance: Outbox package tests pass with a fake sender and PostgreSQL/Redis integration tests.
- Do not modify: concrete WeCom/Telegram protocol code.
- Risks: external timeout after successful send. Use Outbox IDs where provider supports idempotency and accept/alert on residual uncertainty.
- Rollback: pause dispatcher, retain pending/retry/DLQ rows, manually replay after repair.

## Definition of Done for P0

- All P0 package tests, `go test ./...`, `go vet ./...`, and race tests pass.
- A fake inbound message can be verified, mapped to a TenantContext, atomically claimed, queued, consumed by a Worker, executed by tRPC-Agent-Go, persisted as ordered Session events, and placed into an Outbox.
- Duplicate delivery produces exactly one Agent execution and returns the persisted response reference.
- Same-Session concurrent jobs cannot overwrite state or commit stale fencing tokens.
- Model/Tool cancellation closes execution cleanly without goroutine or Event-channel leaks.
- A local `MODEL_PROVIDER=echo` mode remains available for development without credentials.
- Each task is delivered as an independent commit with its own tests and rollback path; no enterprise IM or production deployment work is folded into P0.
