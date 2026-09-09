# 03: Two-Gateway Shared Control Plane

**What to build:** An operator can create or publish configuration through one
Gateway and immediately query and execute it through another Gateway; restarting
either Gateway does not lose or diverge the authoritative configuration.

**Blocked by:** 01: Single Gateway Durable Restart.

**Status:** resolved

- [x] Production and Compose use PostgreSQL as the shared Control Plane Store
  for Tenant, Agent App, Deployment, Deployment Version, Backend Selection,
  Channel routing, Governance Policy, and idempotency state.
- [x] A black-box test creates and publishes through Gateway A, reads and routes
  through Gateway B, restarts a Gateway, and repeats the workflow successfully.
- [x] Gateway instances do not rely on process-local correctness caches.
- [x] Control-plane failure returns `control_plane_unavailable`; there is no
  stale or InMemory fallback.
- [x] Concurrent lifecycle and idempotency invariants remain atomic across the
  two Gateway instances.
- [x] The ticket documents and runs its own two-Gateway acceptance command.

## Comments

所有请求期 Control Plane 操作均接收调用方 `context.Context`，数据库 5 秒上限从该 context 派生；HTTP 取消或服务关闭会立即取消共享状态查询。`TestControlPlaneLoadStopsWhenHTTPRequestIsCanceled` 覆盖此行为。
Tenant、Agent App、Deployment、Version、路由和 Governance Policy 等资源接口均显式返回持久层错误；HTTP GET/POST 和 Runtime 直接映射为 503 `control_plane_unavailable`，不读取旧快照，也不依赖跨请求共享的错误状态，因此不会在并发请求下误报 404/409。
已由 `./scripts/stage7-compose-acceptance.sh` 和总门禁验证。
