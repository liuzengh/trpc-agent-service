# Stage 3: Standard Channel Adapter, Mock IM, And Chat Workspace

Type: task
Status: needs-triage
Blocked by: 03

## Goal

Freeze the IM integration and SSE contracts, then prove the complete browser chat flow without depending on a real IM provider.

## In Scope

Standard inbound/outbound conversion; Channel Binding; trusted credential/signature interfaces; user identity mapping; single/group Session ID rules; duplicate and out-of-order delivery; timeout, retry, rate-limit, message-length, and attachment semantics; Mock IM with failure injection. Add chat Session/history/send/cancel/retry APIs, the stable SSE envelope and minimum event types, and a full-height chat workspace in the existing frontend with Session creation/opening, history restoration, streaming, cancellation, failures, idempotent retry, and Mock IM fault controls.

## Out Of Scope

Real provider protocols, production credentials, platform-specific sandbox setup, and treating Web UI as one of the two required real IM providers.

## Acceptance

Conversion, signature, deduplication, ordering, retry, rate-limit, length, Session isolation, cross-tenant mapping, SSE ordering/deduplication/cancellation, refresh recovery, frontend checks, API contracts, and end-to-end Playwright Mock IM chat tests pass.

## Handoff

Freeze Channel Adapter, Channel Binding, idempotency, retry, Session ID, chat API, SSE event envelope, event type, and request ID contracts for Stage 4.
