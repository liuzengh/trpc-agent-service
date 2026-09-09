# 08: Tenant Rate Limits And Metrics/Cost Views

**What to build:** Tenant administrators can configure request limits shared by Chat and real IM traffic, while operators can inspect tenant-scoped traffic, latency, error, delivery, token, and cost measurements from the same Management Console.

**Blocked by:** 06: External IM User Authorization; 07: Tenant Budgets And Execution Cost Tracking.

**Status:** resolved

- [x] Tenant rate-limit policy defines a documented window, capacity, and scope, and applies consistently to Chat, provider replay, Enterprise WeChat, and Telegram before Runner execution.
- [x] Concurrent requests cannot exceed the configured Tenant allowance, while activity in one Tenant cannot consume another Tenant's allowance.
- [x] Limited requests receive stable retry information, Delivery Status where applicable, and exactly one Audit Event without creating an Agent execution.
- [x] Metrics cover request volume, active executions, errors, model latency, Tool latency, Storage Adapter latency, IM delivery success, token usage, and cost with bounded Tenant/App/provider dimensions.
- [x] Framework, policy, Storage, and Channel instrumentation preserves cancellation behavior and does not make metric export a requirement for request success.
- [x] Authorized metric and cost queries enforce Tenant scope and bounded time ranges without exposing raw prompts, replies, Tool arguments, identities, or credentials.
- [x] The Management Console presents rate-limit configuration plus scan-friendly metrics and cost views with loading, empty, partial-data, error, desktop, and mobile coverage.

## Answer

Implemented synchronized Tenant rate windows shared by Chat/provider execution, stable limit decisions, bounded Tenant metric dimensions, request/execution/latency/token/cost/IM delivery counters, and responsive metric views.
