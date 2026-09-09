# 05: Dangerous Tool Confirmation Closed Loop

**What to build:** A Tool marked dangerous pauses before invocation and creates a tenant-scoped confirmation that an authorized person can approve or reject. The Tool runs at most once after approval, and every transition is visible and auditable.

**Blocked by:** 04: Plugin/Guardrail Enforcement And Policy Decisions.

**Status:** resolved

- [x] Tenant policy can classify an allowed Tool as requiring confirmation; classification remains server-owned and cannot be weakened by Tool input or a client request.
- [x] A dangerous Tool request enters a stable pending-confirmation state before any side effect and records the user, Tenant, Agent App, Session, request, Tool, safe argument summary, policy revision, and expiry.
- [x] Only an authorized role in the same Tenant can approve or reject; expired, cross-Tenant, already-decided, and unauthorized attempts cannot invoke the Tool.
- [x] Concurrent or repeated approvals are idempotent and cause at most one Tool invocation and one terminal decision.
- [x] Approval, rejection, expiry, cancellation, Tool completion, and Tool failure produce coherent Session Events, SSE outcomes, and Audit Events.
- [x] The Management Console provides a confirmation queue and detail flow with approve/reject controls, clear terminal states, and desktop/mobile tests.
- [x] Service cancellation or shutdown does not leak goroutines or leave an in-memory waiter as the sole record of a pending confirmation.

## Answer

Implemented durable Tenant-scoped confirmations with 15-minute expiry, same-Tenant role decisions, idempotent approval/rejection, retry-after-approval using the original request ID, audit transitions, and responsive queue controls.
