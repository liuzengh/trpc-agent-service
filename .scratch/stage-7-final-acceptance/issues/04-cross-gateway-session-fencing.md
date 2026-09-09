# 04: Cross-Gateway Session Fencing

**What to build:** Requests for the same Tenant Session remain serialized when
they arrive through different Gateways, and an execution that loses ownership
cannot corrupt newer Session history, State, or Summary.

**Blocked by:** 03: Two-Gateway Shared Control Plane.

**Status:** resolved

- [x] A renewable PostgreSQL Session Execution Lease grants a monotonically
  increasing fencing token, defaults to a 30-second TTL, and renews every 10
  seconds while execution is healthy.
- [x] Storage writes atomically reject a stale fencing token before mutating
  Session Event, State, Summary, Memory, or execution status.
- [x] Session Event is the source of truth; State and Summary advance only
  through ordered Projection Checkpoints that cannot skip an event.
- [x] A black-box race routes the same Session through both Gateways, forces the
  first owner to lose its lease, and proves only the current owner can commit.
- [x] Different Sessions continue to execute concurrently, and cancellation
  releases or expires ownership predictably.
- [x] The ticket documents and runs its own fencing acceptance command.

## Comments

跨 Gateway 租约丢失、fencing token 增长及旧执行取消已由 Compose 总验收验证。
