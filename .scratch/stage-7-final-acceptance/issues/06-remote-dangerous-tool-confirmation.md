# 06: Remote Dangerous Tool Confirmation

**What to build:** A remote Worker pauses a dangerous Tool before side effects,
allows an authorized tenant user to approve it through the Gateway, executes it
at most once, and exposes the final governed outcome.

**Blocked by:** 05: Signed Remote Worker Run.

**Status:** resolved

- [x] Gateway instances host an internal Governance API behind an internal
  Service, and Worker consults it locally at the Tool execution boundary.
- [x] The persisted state sequence is `pending_confirmation -> approved ->
  executing -> completed|failed|outcome_unknown` with valid idempotent retries.
- [x] Tool allowlist, budget, Guardrail, and confirmation checks fail closed
  before side effects when governance state is missing or unavailable.
- [x] `outcome_unknown` is visible to the tenant and is never automatically
  replayed as though the Tool had not run.
- [x] A black-box test covers approval, rejection, duplicate decisions,
  Governance API outage, Worker loss during execution, and Tenant isolation.
- [x] The ticket documents and runs its own governed-Tool acceptance command.

## Comments

批准、拒绝、重复决策、治理服务故障和执行中 Worker 丢失均已由 Compose 验收验证。
