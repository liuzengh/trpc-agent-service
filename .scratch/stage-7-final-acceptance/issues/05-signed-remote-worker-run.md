# 05: Signed Remote Worker Run

**What to build:** A request entering the public Gateway is safely dispatched
to an independent Worker, produces a real or fixture-backed model response, and
returns through the unchanged HTTP/SSE contract with one correlated trace.

**Blocked by:** 02: Real Model Chat; 04: Cross-Gateway Session Fencing.

**Status:** resolved

- [x] Gateway signs an HS256 Execution Manifest using a dedicated rotating key;
  the Worker rejects missing, expired, tampered, or unknown-key manifests.
- [x] The manifest binds Tenant, Agent App, Deployment Version, Governance Policy
  revision, request identity, trace identity, and fencing token; secrets remain
  server-side references.
- [x] Worker constructs the Agent from authoritative immutable configuration and
  cannot accept client-selected Tenant, Version, policy, provider, or backend.
- [x] W3C `traceparent` and the Platform Trace correlate Gateway admission,
  Worker execution, model calls, and Session writes by request ID.
- [x] Cross-Tenant and stale-manifest attempts fail without reaching the model
  or exposing the existence of another Tenant's resources.
- [x] The ticket documents and runs its own remote-run acceptance command.

## Comments

签名清单、远程 Worker、追踪关联和租户隔离已由总门禁及 Compose 验收验证。
