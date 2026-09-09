# 07: IM Retry And Duplicate Recovery

**What to build:** Provider and Mock IM failures retry within bounded policy, duplicate callbacks remain idempotent, and cancellation or shutdown does not create duplicate Agent executions or replies.

**Blocked by:** 03: Compose Multi-Component Baseline.

**Status:** resolved

- [x] Inbound provider updates retain duplicate and sequence classification across bounded retry windows; duplicates do not start another Runner execution.
- [x] Outbound provider delivery uses bounded attempts, provider retry-after hints, backoff, and stable terminal statuses with attempts and failure codes.
- [x] Cancellation during retry does not emit a later provider reply, and shutdown leaves pending work in a recoverable or explicitly failed state.
- [x] Delivery state and Session Events preserve request, trace, provider, Tenant, and conversation identity without exposing provider credentials.
- [x] Replay of a callback after dependency or provider recovery does not duplicate terminal Session Events, Memory writes, or provider replies.
- [x] Deterministic provider fixtures and Mock IM tests cover timeout, rate limit, retry exhaustion, reconnect, duplicate callback, cancellation, and shutdown.
