# 06: Repeatable Redis-To-SQL Migration

**What to build:** Provide a repeatable Redis-to-SQL Session and Memory migration command and status contract. Operators can run a dry-run, process bounded batches, retry transient failures, resume after interruption, and receive count/content validation results without mutating immutable source events.

**Blocked by:** 02: Materialized Sessions, Summaries, And Memory; 03: Redis Shared Session And Memory Adapter; 04: SQLite Adapter And PostgreSQL Compatibility Profile

**Status:** resolved

- [x] Dry-run reports the migration scope and expected counts without writing destination data.
- [x] Real migration supports bounded batches, retry classification, durable progress, and resume from an interrupted checkpoint.
- [x] Repeating a completed or partially completed migration is idempotent and does not duplicate events or Memory records.
- [x] Completion reports include source/destination counts and content or checksum validation, with mismatches marked failed.
- [x] Unsupported vector/object-store data and destructive operations are explicitly excluded and reported.
- [x] Migration command/API tests cover success, transient failure, resume, mismatch, and cancellation paths.

## Answer

Implemented the Redis-to-SQL command and serialized job API with dry-run/execute modes, bounded Session batches, retry, atomic checkpoints, resume, cancellation, progress, idempotency, and canonical Event/Memory count/content validation.
