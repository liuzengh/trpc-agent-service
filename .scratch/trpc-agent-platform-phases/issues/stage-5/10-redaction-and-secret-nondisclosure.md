# 10: Unified Redaction And Secret Non-Disclosure

**What to build:** Sensitive values are removed at platform boundaries so operators can diagnose identity, policy, Tool, Storage, trace, and IM failures without credentials or protected content appearing in logs, traces, errors, APIs, persisted operational records, or the Management Console DOM.

**Blocked by:** 05: Dangerous Tool Confirmation Closed Loop; 08: Tenant Rate Limits And Metrics/Cost Views; 09: End-To-End Trace Propagation And Inspection.

**Status:** resolved

- [x] A server-owned redaction policy covers Bot credentials, identity tokens, model API keys, database credentials and addresses, Tool/MCP secrets, confirmation arguments, and configured sensitive content patterns.
- [x] Redaction is applied before values enter application logs, trace attributes/events, Audit Events, metrics labels, public errors, management responses, or provider delivery diagnostics.
- [x] User input, model output, Tool arguments/results, and Storage errors are handled according to the configured policy without changing the immutable identity or ordering of Session Events.
- [x] Public Error Contracts preserve actionable stable codes while excluding raw driver, framework, identity-provider, policy-engine, and provider diagnostics.
- [x] Write-only secret replacement never echoes the previous or new value, and management reads expose only non-secret configuration and replacement status.
- [x] Table-driven canary tests inject unique secrets through every Stage 5 path and assert absence from serialized responses, persisted operational records, logs, traces, and metrics.
- [x] Management Playwright tests assert secrets and redacted source values are absent from the DOM, browser storage, rendered errors, and accessible page text on desktop and mobile.

## Answer

Added boundary redaction for structured logs, Runner input/output, policy responses, Audit/Trace data, errors and provider diagnostics; redaction values remain write-only, with Go canary and desktop/mobile DOM assertions.
