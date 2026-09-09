# 04: SSE Streaming And Cancellation

**What to build:** Stream chat execution through a stable SSE contract so users see ordered runtime progress and can cancel a run. The frontend renders the minimum event set, deduplicates events, survives reconnect or refresh, and receives a terminal cancelled state when cancellation completes.

**Blocked by:** 03: Chat Session Foundation And Workspace

**Status:** resolved

- [x] The SSE envelope contains event ID, request ID, Session ID, monotonic sequence, type, and data.
- [x] The minimum event types are run started, message delta, message completed, run failed, run cancelled, and run completed.
- [x] Server-side adapter translation hides upstream runtime event types from the frontend contract.
- [x] Client cancellation propagates through the request chain, releases resources, and emits a terminal cancelled state.
- [x] Clients deduplicate by event ID and either resume from the last event position or recover from persisted Session Events.
- [x] Automated backend and frontend tests prove SSE ordering, deduplication, reconnect/refresh recovery, cancellation, and shutdown.

## Answer

Implemented the stable SSE endpoint and envelope, monotonic persisted-event streaming, Last-Event-ID/query resume, terminal-event completion, run cancellation, and service-shutdown cancellation. The Chat Workspace now consumes SSE through EventSource, renders deltas, deduplicates by event ID, exposes cancellation, recovers from persisted history, and closes on terminal events.
