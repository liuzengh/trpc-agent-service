# 03: Distributed Run Coordinator

Type: task
Status: ready-for-human

## Goal

Guarantee duplicate-request suppression and cancellation across Gateways while
preserving same-Session serialization, cross-Session concurrency, and fencing.

## Work

- Implement the `RunCoordinator` and `RunPermit` interfaces from `spec.md`.
- Add the PostgreSQL run state/schema migration and an in-memory adapter.
- Move execution claim, post-claim terminal reconciliation, cancellation, lease
  loss, and fencing ownership behind this module.
- Make process-local `activeRuns` optional acceleration rather than authority.
- Remove duplicate Session lease ownership from Runtime once callers migrate.
- Preserve dangerous Tool `outcome_unknown` behavior on owner loss.

## Tests

- Concurrent same-request A/B submission invokes Runner and Tool exactly once.
- Retry after terminal returns the persisted terminal without execution.
- B cancels A's active request and exactly one `run.cancelled` is persisted.
- Different input with the same request ID conflicts across Gateways.
- Same Session/different requests serialize; different Sessions overlap.
- Lease loss increments fencing and rejects stale event/Memory/Artifact writes.

## Done

All correctness assertions pass through the coordinator interface and through
the public two-Gateway Compose route. No test depends on sticky placement.

## Evidence

- `go test ./trpcservice/platform/...` and `go test -race ./trpcservice/platform/...`
  passed duplicate, queue, cancel, expiry, and fencing coverage.
- Stage 7 Compose passed cross-Gateway retry, remote cancellation, owner loss,
  higher fencing token, timeout, and PostgreSQL outage recovery.
- PostgreSQL is the durable run-state source; `activeRuns` is only local
  acceleration. Session Lease protects shared Session/Event/Memory/Artifact
  writes, while Run Coordinator owns request state and terminal ownership.
