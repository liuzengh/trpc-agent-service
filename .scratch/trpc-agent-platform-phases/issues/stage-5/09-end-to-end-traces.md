# 09: End-To-End Trace Propagation And Inspection

**What to build:** An operator can follow one request from browser or IM ingress through routing, policy, framework execution, Tool and Storage work, and the final reply using its request and trace identities, without coupling public contracts to framework telemetry internals.

**Blocked by:** 04: Plugin/Guardrail Enforcement And Policy Decisions; 06: External IM User Authorization.

**Status:** resolved

- [x] A server-created or validated `trace_id` is associated with the existing `request_id` and propagated through callback, Gateway, Worker, AgentFactory, Runner, Tool, Storage Adapter, policy, Audit Sink, and Channel reply operations.
- [x] OpenTelemetry or an equivalent standard tracer represents the full chain with coherent parent/child relationships, stable span names, status, and bounded non-sensitive attributes.
- [x] Browser Chat, Enterprise WeChat, Telegram, provider replay, successful Tool execution, policy denial, Storage failure, cancellation, and reply failure preserve request/trace correlation.
- [x] Trace propagation does not allow an untrusted external trace header to forge Tenant identity, authorization state, or Audit Event ownership.
- [x] Cancellation and shutdown end spans and exporters without leaking goroutines or blocking Runner event-channel draining.
- [x] Authorized trace lookup is tenant-scoped and returns a bounded platform trace model rather than upstream framework implementation types.
- [x] The Management Console can search by request or trace ID and inspect the ordered, nested execution path with failure and policy-decision correlation.

## Answer

Propagated server-owned `trace_id` through Gateway, Worker, AgentFactory, Runner events, Session Events, policy, Tool authorization, storage, callback, and reply, with bounded Tenant-scoped lookup and console inspection.
