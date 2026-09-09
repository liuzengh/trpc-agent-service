# Stage 7: README Final Acceptance

**Status:** resolved

Stage 7 closes every remaining README acceptance gap through independently
verifiable vertical slices. It is the final delivery stage; there is no Stage 8.

## Delivery Rules

- Preserve the Public API Contract, including HTTP paths, SSE envelopes, and
  stable public error codes.
- Every ticket must leave its complete user-visible workflow runnable and must
  include an automated black-box acceptance path for that workflow.
- Internal Go APIs, the database schema, and the Gateway/Worker protocol may
  change when required by a slice.
- Shared abstractions are introduced only inside the first slice that exercises
  them end to end. Completing an interface or adapter without its consuming
  workflow does not complete a ticket.
- PostgreSQL is the production and Compose Control Plane Store. SQLite is the
  single-node development Control Plane Store. InMemory is test-only.
- Database migrations are forward-only expand-contract migrations applied by
  `control-migrate`; Gateway processes only verify schema compatibility.
- Acceptance evidence is generated as a CI artifact tied to the commit SHA and
  schema version. Generated evidence is not committed as a stale fixture.
- Mentor-facing deliverables are written in Chinese.

## Test Seams

Tests observe behavior through the existing public HTTP/SSE API, Management
Console workflows, the internal Gateway-to-Worker HTTP boundary, and black-box
process/Compose lifecycle behavior. Database tables are not used as a substitute
for a behavioral assertion through one of those seams.

## Comments

Stage 7 已通过 `./scripts/stage7-acceptance.sh` 的完整验收；真实模型链路另由
`./scripts/stage7-live-model-smoke.sh` 验证。验收映射和非阻塞限制记录在
`docs/acceptance.md`，本阶段为最终阶段，不再创建后续验收阶段。
