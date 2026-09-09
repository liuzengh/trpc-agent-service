# Stage 5 Handoff

Status: ready

Stage 5 freezes the following contracts for Stage 6:

- Authentication mode is server configuration. Production identity is
  established by `IdentityProvider`, exchanged into an HttpOnly session, and
  restricted to server-owned Tenant assignments and the roles
  `platform_admin`, `tenant_admin`, `operator`, and `viewer`.
- `TenantPolicy` is scoped by Tenant and Agent App and contains revisioned
  Tool/MCP, Guardrail, dangerous Tool, external IM authorization, redaction,
  budget, pricing, and rate-window fields.
- Governance executes before Session input persistence and Runner invocation;
  output filtering executes before Session completion persistence and provider
  delivery. The framework runtime remains `trpc-agent-go v1.11.2` behind the
  existing Runner Adapter and AgentFactory.
- A dangerous Tool creates `confirmation_required` from the real `BeforeTool`
  callback with a hashed argument summary and a Tenant-scoped confirmation ID.
  After an authorized decision, the same request ID is retried; approval is
  consumed once, and `AfterTool` records completion, failure, cancellation, and
  actual Tool latency.
- Audit Event fields are `tenant_id`, `channel`, `user_id`, `session_id`,
  `agent_name`, `tool_name`, `decision`, `latency`, `error_type`, `cost`,
  `trace_id`, `request_id`, and `occurred_at`. Public APIs are search-only.
- Tenant metric names are `requests`, `active_executions`,
  `completed_executions`, `failed_executions`, `denied_requests`,
  `rate_limited_requests`, `tokens`, `cost`, `model_latency_ms`,
  `tool_latency_ms`, `storage_latency_ms`, `im_delivered`, and `im_failed`.
  Queries accept bounded Agent App/provider dimensions and a maximum 31-day
  time range. Active budget reservations expire after 15 minutes and reconcile
  before the next admission; policy updates do not reset accounted usage.
- `trace_id` is part of the Gateway/Runner/runtime/SSE and Session Event path.
  The bounded Platform Trace contains ordered spans for callback, Gateway,
  policy, Worker, AgentFactory, Runner, Tool authorization, storage, and reply.
- Management endpoints remain under `/api/v1/admin/governance/`; trace lookup
  accepts request or trace ID, and every lookup is restricted to active Tenant
  Context.
- Secrets and configured redaction values are write-only and cannot appear in
  public policy responses, logs, Audit Events, Platform Traces, metrics labels,
  public errors, browser storage, or DOM text.
  Persistent redaction values are AES-GCM encrypted with the adjacent
  `governance.json.key` file, which is local protected state and not an API.

Stage 6 may consume these contracts for failure injection, draining, rollout,
rollback, and recovery evidence. It must not make client-supplied Tenant,
identity, policy, or trace ownership authoritative.
