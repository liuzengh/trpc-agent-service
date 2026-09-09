# 02: Materialized Sessions, Summaries, And Memory

**What to build:** Materialize Session state and Summary from the immutable event stream and expose Memory operations through the platform API. Users can inspect a Session, its ordered history, Summary, and tenant-scoped Memory, with replay producing the same materialized result.

**Blocked by:** 01: Storage Contracts And Immutable Session Events

**Status:** resolved

- [x] Session state and Summary are derived from ordered immutable events and can be rebuilt by replay.
- [x] Session and Memory reads and writes are isolated by trusted Tenant Context and Session identity.
- [x] Repeated materialization is deterministic and does not mutate immutable events.
- [x] API responses define stable state, Summary, Memory, empty, not-found, and backend-error behavior.
- [x] InMemory integration tests cover event-to-state projection, replay, Memory isolation, and concurrent access.

## Answer

Implemented replay-derived Session/Summary state and tenant-scoped Memory APIs with deterministic projection, empty/not-found/error behavior, and contract tests.
