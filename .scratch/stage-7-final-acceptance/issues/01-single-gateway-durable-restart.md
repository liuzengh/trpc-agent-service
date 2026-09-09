# 01: Single Gateway Durable Restart

**What to build:** A developer can create a Tenant, Agent App, Deployment
Version, Channel Binding, and Governance Policy through the existing product,
restart the Gateway, and continue querying, changing, and routing the same
resources without changing the Public API Contract.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] The existing HTTP API and Management Console create a complete routable
  Tenant configuration and observe the same configuration after a Gateway
  restart.
- [x] Development mode uses SQLite as its durable Control Plane Store; InMemory
  remains available only to tests.
- [x] `control-migrate` initializes and upgrades the schema with forward-only
  expand-contract migrations, while Gateway startup only checks compatibility.
- [x] A missing or incompatible schema produces a stable, actionable startup
  failure without leaking database details.
- [x] Existing public HTTP paths, payload semantics, SSE envelopes, errors, and
  Tenant isolation tests remain green.
- [x] The ticket documents and runs its own restart acceptance command.

## Comments

已由 `./scripts/stage7-acceptance.sh` 验证，验收证据映射见 `docs/acceptance.md`。
