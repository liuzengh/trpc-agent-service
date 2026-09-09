# 04: SQLite Adapter And PostgreSQL Compatibility Profile

**What to build:** Add a SQLite persistence adapter and a PostgreSQL compatibility test profile for Session Events, materialized Session state, Summary, and Memory. The same public data contract works against SQL storage with durable constraints and predictable restart behavior.

**Blocked by:** 01: Storage Contracts And Immutable Session Events; 02: Materialized Sessions, Summaries, And Memory

**Status:** resolved

- [x] SQLite persists and reloads Session Events, materialized state, Summary, and Memory while preserving tenant isolation.
- [x] SQL constraints enforce immutable events, idempotency, and monotonic sequence behavior under duplicate and concurrent writes.
- [x] PostgreSQL compatibility tests exercise the same contract and document any profile-specific setup or limitations.
- [x] Restart/reopen tests prove persisted data remains readable and materialization remains deterministic.
- [x] SQL adapter errors are classified through the shared API contract without exposing arbitrary SQL or sensitive values.

## Answer

Implemented SQL-backed SQLite and PostgreSQL profiles using schema constraints, serializable append transactions, durable reopen behavior, shared adapter contracts, and bounded API error responses.
