# 05: Runtime Timeout, Cancellation, And Event Draining

**What to build:** Slow model and Tool executions end within service-owned timeout policy, client cancellation propagates correctly, and event channels drain without duplicate terminal events.

**Blocked by:** 03: Compose Multi-Component Baseline.

**Status:** resolved

- [x] Model, Tool, and runtime request timeout policy is server-owned, bounded, and visible in configuration or policy state without accepting arbitrary client timeout authority.
- [x] Timeout and client cancellation propagate to the upstream Runner and Tools, produce one `run.cancelled` or bounded failure terminal event, and release session and lifecycle leases.
- [x] Tool failures are represented with stable error classification and do not discard already-persisted governance, audit, trace, or event evidence.
- [x] Slow event consumers cannot deadlock shutdown; buffered channels are drained or closed and goroutine/wait-group lifetimes are bounded.
- [x] Dangerous Tool confirmations can still be resumed or cancelled correctly after a timeout or service drain without consuming approval twice.
- [x] Race-enabled tests cover blocked model, blocked Tool, slow consumer, client cancellation, service drain, timeout policy, and terminal-event uniqueness.
