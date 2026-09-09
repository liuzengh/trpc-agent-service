# 03: Cancellation And Graceful Shutdown

**What to build:** Reliable cancellation and shutdown behavior so the service stops accepting new work, lets active work observe context cancellation, and releases goroutines and resources without deadlocks or panics.

**Blocked by:** 01: Runnable Service Baseline

**Status:** resolved

- [x] Shutdown stops new work and follows an explicit completion or timeout policy.
- [x] Active work observes `context.Context` cancellation and exits promptly.
- [x] Goroutines, channels, and owned resources are released on normal and cancelled shutdown paths.
- [x] Repeated shutdown is safe and does not panic or deadlock.
- [x] Automated tests cover cancellation, shutdown ordering, timeout behavior, and leak-sensitive lifecycle paths.

**Verification:** `trpcservice/lifecycle` provides admission control, context-bounded shutdown, idempotent release, and repeated shutdown tests. `go test ./...` and `go vet ./...` pass.
