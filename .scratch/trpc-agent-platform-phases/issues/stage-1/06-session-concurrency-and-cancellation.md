# 06: Session Concurrency And Cancellation

**What to build:** Requests for the same Tenant, Agent App, and Session execute serially in arrival order, while requests for different Sessions can execute concurrently. Cancellation and service shutdown propagate through Gateway, Worker, and Runner Adapter so unfinished work exits promptly without deadlocks, leaked goroutines, stale admission state, or blocked channels.

**Blocked by:** 05: Deployment Lifecycle And Routing Loop

**Status:** resolved

- [x] The Session serialization key includes the trusted Tenant, Agent App, and Session identities so unrelated tenants or applications never share a lock accidentally.
- [x] Two overlapping requests for the same Session invoke the Runner in deterministic serial order.
- [x] Requests for different Sessions can enter the Runner concurrently, including Sessions belonging to different Tenants.
- [x] Waiting for Session admission observes request cancellation and does not later execute cancelled work.
- [x] Cancellation of active work propagates through Gateway, Worker, and Runner Adapter and produces a stable API error.
- [x] Service shutdown rejects new work and follows the frozen bounded cancellation and completion policy for queued and active work.
- [x] Session coordination state is released after success, failure, cancellation, and shutdown, with no unbounded key retention.
- [x] Deterministic concurrency and race-sensitive tests cover serialization, parallelism, cancellation, shutdown, and leak-prone paths.

## Answer

Implemented cancellation-aware Session coordination keyed by Tenant, Agent App, and Session. Deterministic tests prove same-Session serialization, cross-Session concurrency, cancelled waiter removal, active Runner cancellation during shutdown, rejection of new work while closing, and release of unused coordination keys.
