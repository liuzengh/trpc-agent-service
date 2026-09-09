# 05: Retry And Failure Recovery

**What to build:** Let users retry a failed or interrupted chat request idempotently and recover the correct final state after failure, refresh, or reconnection. A retry creates at most one logical execution in the visible history and never duplicates persisted input/output/failure events.

**Blocked by:** 03: Chat Session Foundation And Workspace; 04: SSE Streaming And Cancellation

**Status:** resolved

- [x] Chat retry uses a stable idempotency contract scoped to the Tenant, Session, and logical request.
- [x] Replaying a request after failure, refresh, reconnect, or timeout returns or resumes the original logical run rather than creating a duplicate.
- [x] Failure, cancellation, retry, and completed states are distinct and visible in history and the workspace.
- [x] Concurrency and cancellation tests prove no lost terminal event, duplicate persisted event, or leaked goroutine/channel.
- [x] Cross-Tenant retry guesses are rejected without exposing another Tenant's run state.

## Answer

Implemented request-scoped idempotent replay, concurrent retry deduplication, cancellation-safe terminal events, refresh/reconnect recovery, and distinct failed/cancelled/completed states. The workspace reuses the same request key after an ambiguous network failure and starts a new logical key only after a terminal failure or cancellation.
