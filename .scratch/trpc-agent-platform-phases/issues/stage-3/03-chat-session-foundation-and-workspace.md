# 03: Chat Session Foundation And Workspace

**What to build:** Add tenant-scoped chat Session APIs and a full-height Chat Workspace in the existing progressive frontend. A user can create or open a Session from an Agent App or Deployment, send a message, see ordered history, refresh the page, and restore the same history from the backend while `request_id` propagates through the browser, Gateway, Worker, and Runner.

**Blocked by:** 01: Channel Adapter And Binding Baseline

**Status:** resolved

- [x] Session creation/opening, history reading, and message sending are available through the documented chat API boundary with backend-enforced tenant and role rules.
- [x] Session ownership derives from trusted Tenant Context; guessing another Tenant's Session or Agent App identifier fails without leaking data or invoking the Runner.
- [x] The workspace restores ordered history after refresh using the backend as the source of truth and stores no conversation history in browser storage.
- [x] `request_id` is established for each browser-initiated request and propagates through Gateway, Worker, and Runner.
- [x] Loading, empty, error, authorization, and tenant-switch states are visible and covered by frontend tests.
- [x] The full-height workspace works on desktop and mobile layouts without overlap.

## Answer

Implemented chat Session creation/opening, backend-backed ordered history, request-scoped message execution, conflict handling, role enforcement, cross-Tenant Session rejection, and browser `request_id` propagation. Added the full-height progressive Chat Workspace, App/Deployment entry points, refresh recovery using only the most recently opened Session ID, and desktop/mobile styling.
