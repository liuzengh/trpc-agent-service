# Stage 2 Rework Design

Status: confirmed

This design closes the blocking Stage 2 code-review findings without expanding the phase scope. The Memory POST response discrepancy is treated as a small low-risk fix, not a phase gate.

## Problems

1. Switching tenants in the Management Console leaves the previous tenant's data visible because the data view does not reload on active-tenant changes.
2. An unavailable backend is replaced by a generic page error, so the required health state is not visible.
3. Migration checksums omit event identity and are not length-delimited, allowing different destination content to pass validation.
4. Migration goroutines can race with service shutdown, failure-event writes have no bounded context, and backend switching can close an adapter while requests still use it.
5. Migration API/CLI errors can expose raw backend diagnostics, and Memory POST can return an unpersisted record shape.

## Decisions

- The Management Console remounts the data page with the active Tenant Context as its React key. A tenant switch therefore clears all local state, request generations, and migration polling from the previous tenant.
- Backend health and data inspection load independently. Health remains visible even when Session, Event, or Memory reads fail; each data section reports its own empty or error state.
- Migration uses a versioned, canonical, length-delimited checksum. Event content includes Tenant, Session, event ID, sequence, type, payload, idempotency key, and occurrence time. Memory content includes Tenant, Session, memory ID, key, value, and update time.
- Migration orchestration owns a closing state. New jobs are admitted and registered with the worker group while holding the closing-state lock; shutdown sets closing, cancels active jobs, and only then waits for the group.
- `run.failed` persistence uses a bounded, service-owned failure context. It is independent of client cancellation but is cancelled during service shutdown and has a short timeout.
- A server-owned backend registry centralizes backend construction and selection. Requests acquire a lease on the selected adapter; selection changes mark the old adapter retiring, and the adapter closes only after all leases are released.
- API and CLI failures use stable public error codes and sanitized messages. Raw driver, network, and filesystem errors remain internal diagnostics.
- Memory writes return the persisted record shape, including generated ID and update time.

## Verification

- Go tests cover migration admission during concurrent shutdown, canonical checksum identity/conflict detection, bounded failure-event persistence, lease-based backend retirement, sanitized public errors, and Memory response consistency.
- Frontend unit tests cover tenant-switch remounting, independent health rendering, unavailable backend status, and per-section data failures.
- Playwright adds a desktop and mobile flow for switching tenants and proving that the prior tenant's data is not rendered.
- The common Stage 2 gate reruns Go, frontend, build, and applicable Redis/PostgreSQL integration tests.

## Exclusions

This rework does not introduce production cutover, online zero-downtime migration, general-purpose backend pooling, or Stage 3 chat/SSE behavior.
