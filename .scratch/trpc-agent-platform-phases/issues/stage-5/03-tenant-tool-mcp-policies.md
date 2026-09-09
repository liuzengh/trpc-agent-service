# 03: Tenant Governance Policies And Tool/MCP Allowlists

**What to build:** Tenant administrators can define which Tools and MCP capabilities an Agent App may use. The active policy is injected through AgentFactory into the framework-backed runtime, and disallowed capabilities are rejected before invocation with an inspectable policy decision.

**Blocked by:** 02: Persistent Audit Events And Tenant-Scoped Search.

**Status:** resolved

- [x] Tenant-scoped governance policies support Tool and MCP allowlists with a stable revision so an execution can identify the policy it evaluated.
- [x] Only authorized administrators can create or change policies; operators and viewers receive the appropriate read-only or denied behavior.
- [x] AgentFactory builds the active Deployment Version with its server-owned policy, and no request, Tool argument, or browser value can override the policy.
- [x] Deterministic framework-runtime fixtures prove an allowed Tool and MCP capability execute while a disallowed capability is denied before its implementation is invoked.
- [x] Policy changes have defined behavior for cached Runners so new executions use the intended revision without leaking policy or runtime state across Tenants.
- [x] Allow, deny, invalid-policy, and unavailable-policy outcomes use stable public contracts and emit tenant-scoped Audit Events linked to the execution.
- [x] The Management Console supports policy inspection and editing, allowlist selection, revision feedback, validation errors, and cross-Tenant isolation tests.

## Answer

Added revisioned Tenant/Agent App policy storage, server-owned Tool/MCP checks before Runner invocation, policy injection into the framework Runner/AgentFactory boundary, stable errors/audits, and role-enforced console editing.
