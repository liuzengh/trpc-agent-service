# 02: Persistent Audit Events And Tenant-Scoped Search

**What to build:** Authorized administrators can inspect immutable Audit Events for identity, authorization, management changes, and Agent executions. Searches are tenant-scoped, bounded, and useful for determining who acted, what decision was made, and which request or trace was involved.

**Blocked by:** 01: Production Identity Provider And Unified Backend Authorization.

**Status:** resolved

- [x] Audit Events persist through a server-owned Audit Sink and cannot be updated or deleted through public APIs.
- [x] The stable Audit Event contract contains `tenant_id`, `channel`, `user_id`, `session_id`, `agent_name`, `tool_name`, `decision`, `latency`, `error_type`, `cost`, `trace_id`, occurrence time, and request correlation where available.
- [x] Authentication outcomes, authorization denials, privileged management mutations, and normal/failed/cancelled Agent executions each produce exactly one applicable Audit Event without changing Session Event semantics.
- [x] Search supports bounded pagination and filters for time, user, channel, Session, Agent App, decision, error type, request ID, and trace ID.
- [x] Search authorization and storage queries enforce Tenant isolation; only platform administrators may select across their server-approved Tenant scope.
- [x] Required audited operations return a stable failure when their Audit Event cannot be durably accepted instead of falsely reporting an unaudited success.
- [x] The Management Console provides Audit Event filters, empty/error states, decision details, and desktop/mobile coverage without exposing raw payloads or credentials.

## Answer

Implemented persistent append-only Audit Events, bounded Tenant-scoped filtering and pagination, authorization/mutation/execution decisions, and console search by decision, request, and trace with responsive coverage.
