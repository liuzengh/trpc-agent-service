# 04: Plugin/Guardrail Enforcement And Policy Decisions

**What to build:** Tenant policy can apply framework Plugin/Guardrail checks to Agent input, output, and Tool calls. Users receive stable allowed, denied, or transformed outcomes, and authorized operators can inspect why a policy decision occurred.

**Blocked by:** 03: Tenant Governance Policies And Tool/MCP Allowlists.

**Status:** resolved

- [x] The governance policy selects server-approved Plugin/Guardrail rules for input, output, and Tool-call checkpoints without exposing upstream framework types through public APIs.
- [x] Policy wiring extends the Stage 3.5 AgentFactory and Runner Adapter boundaries and does not replace or reimplement the framework runtime.
- [x] Deterministic tests cover allowed input, denied input before Agent execution, denied Tool calls before invocation, and blocked or transformed output before delivery.
- [x] Each decision records Tenant, policy revision, checkpoint, rule, decision, request, Session, latency, and safe reason in an Audit Event.
- [x] Denial and transformation produce stable Session/SSE and IM delivery behavior without duplicate terminal events or partial forbidden replies.
- [x] Guardrail cancellation and failure respect request context and return a stable, non-leaking public outcome.
- [x] The Management Console displays policy decisions and their request/Session correlation while preventing unrelated Tenant access.

## Answer

Registered the `platform-governance` upstream Plugin, enforced input/output and Tool checkpoints around the existing runtime, emitted correlated Audit/Trace decisions, and filtered forbidden output before Session persistence and delivery.
