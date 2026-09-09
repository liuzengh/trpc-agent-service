# 06: Mock IM Fault Controls

**What to build:** Expose Mock IM fault controls in the Chat Workspace so a user can inject deterministic channel failures and observe the complete browser-to-channel behavior. The controls make timeout, retry, rate-limit, message-length, attachment, cancellation, and terminal-failure paths reproducible without manual setup.

**Blocked by:** 02: Mock IM Delivery Semantics; 04: SSE Streaming And Cancellation; 05: Retry And Failure Recovery

**Status:** resolved

- [x] Authorized users can select and clear Mock IM fault scenarios from the workspace.
- [x] Fault selection reaches the Mock IM provider through backend-enforced APIs and cannot be driven by untrusted browser identity or Tenant authority.
- [x] Each supported fault produces the expected observable streaming, delivery, retry, or terminal failure state.
- [x] Fault state and conversation data are scoped to the selected Tenant and Session, with no stale cross-tenant data after switching.
- [x] Playwright proves at least timeout/retry, rate-limit or length/attachment failure, cancellation, and recovery on desktop and mobile layouts.

## Answer

Implemented authorized session-scoped Mock fault controls for none, timeout, retry exhaustion, rate limit, message length, and attachment failures. Faults use the chat API and trusted Tenant Context, remain scoped to the selected Session with tenant fallback, and surface stable delivery/terminal states. Desktop and mobile Playwright now proves message-length failure, timeout cancellation, fault clearing, retry recovery, refresh restoration, and layout safety.
