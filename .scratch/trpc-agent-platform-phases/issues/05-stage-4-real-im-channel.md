# Stage 4: Enterprise WeChat And Telegram Channels

Type: task
Status: needs-triage
Blocked by: 04

## Goal

Implement the two required real provider adapters after the standard channel contract is proven by Mock IM.

## In Scope

Enterprise WeChat and Telegram; webhook and signature handling; write-only credential configuration; identity mapping; platform limits; async replies; retry; minimal text/media support; deterministic protocol replay and optional credential smoke tests. Extend the frontend with role-enforced Channel Binding management, provider configuration, enable/disable controls, webhook and latest delivery status, protocol replay, and smoke-test status.

## Out Of Scope

A generic adapter for every IM provider, unsupported provider-specific features, and counting Web UI as a real provider.

## Acceptance

Both provider suites pass protocol parsing, signature, replay, duplicate delivery, identity isolation, timeout/retry, limits, Channel-management authorization, frontend/API contracts, and Playwright flows. Secrets never appear in logs, traces, errors, API responses, or the DOM. Credential smoke results are reported separately and are not marked passed when credentials are unavailable.

## Handoff

Freeze the Enterprise WeChat/Telegram provider matrix, Channel management APIs, write-only secret semantics, operational configuration, request ID propagation, and platform-specific limitations for Stage 5.
