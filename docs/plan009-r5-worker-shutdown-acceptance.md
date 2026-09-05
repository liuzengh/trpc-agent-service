# R5 Worker Shutdown Acceptance Report

## Scope

R5 covers bounded lifecycle shutdown in `trpcservice/worker` and its focused test fixtures. The change preserves the verified R1-R4 delivery action semantics and does not change the public Queue interface.

This report does not claim durable Queue behavior, cross-process atomicity, crash recovery, or production Repository persistence. Those remain deferred to P0-09.

## Implemented Semantics

- `Worker.Stop(ctx)` is single-owner and concurrent-safe. Repeated calls wait for and return the first Stop result; Queue `Close` is guarded by `sync.Once`.
- Stop cancels Receive first, so blocked Receive calls exit before an orderly Queue close.
- In-flight processing gets a bounded graceful drain window. Runtime cancellation is requested only after that window expires.
- A forced deadline returns `ErrDrainTimeout` and never treats incomplete work as successful.
- Delivery records retain explicit action state, including terminal states and `unresolved`; outstanding action failures retain their error chain.
- Ack/Nack/visibility actions use the shutdown or independent cleanup context. A context-cooperating action is bounded; if an external Queue action ignores cancellation, Stop returns a bounded failure while that action may remain in progress.
- Outstanding deliveries use bounded Nack/requeue recovery after workers exit. An action still using the Queue prevents early Queue close; a deferred finalizer completes recovery and Close after the worker/action exits. In that non-cooperating case, Stop may return before the finalizer completes and Queue ownership remains with Worker.
- Queue Close is performed at most once and is not invoked concurrently with an active Queue action.
- Receive non-closed errors retain bounded, cancellable backoff behavior. `ErrQueueClosed` exits without retry.

## Acceptance Evidence

| Area | Evidence | Result |
| --- | --- | --- |
| Receive cancellation and no post-Stop Receive | `TestWorkerStopStopsReceiveBeforeQueueClose` | verified |
| Graceful Runtime drain and commit/ack | `TestWorkerStopGracefullyDrainsRuntimeBeforeCancel` | verified |
| Forced cancellation and unresolved delivery | `TestWorkerStopForcesCancellationAndReportsUnresolved` | verified |
| Caller deadline precedence | `TestWorkerStopUsesCallerDeadlineBeforeConfiguredTimeout` | verified |
| No Close while visibility action is active | `TestWorkerStopDoesNotCloseWhileVisibilityActionIsInUse` | verified |
| Deferred recovery and Close after an action exits | `TestWorkerStopReportsIncompleteCleanupWhenVisibilityIgnoresCancel` | verified |
| Concurrent Stop and exactly-once Close | `TestWorkerStopConcurrentCallsCloseOnceAndShareResult`, `TestWorkerStopIsIdempotent` | verified |
| Outstanding terminal/pending/unresolved state handling | `TestWorkerOutstandingRecoveryRespectsTerminalAndUnknownStates` | verified |
| R4 Ack/Nack/visibility and Receive regressions | Existing Worker tests in the targeted package | verified |

## Verification Commands

```text
go test ./trpcservice/worker/... ./trpcservice/queue/... -count=1 -v
PASS

go test ./trpcservice/worker/... ./trpcservice/queue/... -race -count=1
PASS

go vet ./trpcservice/worker/... ./trpcservice/queue/...
PASS

gofmt -l trpcservice/worker trpcservice/queue
no output

git diff --check
no code whitespace errors; existing LF/CRLF warnings remain in unrelated pre-existing files
```

The verification intentionally remains targeted. A full repository test, full repository race run, real Queue integration, and production persistence test were not used as R5 evidence.

## Boundary and Residual Risk

- The test Queue is an in-process fixture. It verifies Worker coordination, cancellation, action ordering, error classification, and idempotence; it is not evidence of durable broker state or crash recovery.
- The current Queue API returns ordinary errors without a typed remote outcome. Worker therefore treats an Ack/Nack error conservatively as unknown and avoids an unsafe conflicting action.
- If an external Queue implementation violates the context contract and blocks forever inside an action, Worker returns bounded Stop failure and retains ownership; the deferred finalizer can close the Queue only after that action actually exits.
- Framework-wide background goroutine completion remains outside Worker ownership and remains subject to the existing tRPC-Agent-Go v1.11.2 public API limitation.
- R5 does not implement R6 documentation/status synchronization, P0-09 durable Repository Commit, persistent Queue, Outbox, Retry/DLQ, or real transport integration.

## Closure

R5 Worker shutdown behavior is `contract-level verified` for the scoped in-process lifecycle contract, with the residual risks above explicitly retained. In particular, this is not proof that a non-cooperating external Queue action has exited when Stop returns. R6 and P0-09 remain separate/deferred and must not be inferred as complete from this report.
