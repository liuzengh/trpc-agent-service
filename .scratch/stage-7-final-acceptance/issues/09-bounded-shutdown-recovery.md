# 09: Bounded Shutdown And Recovery

**What to build:** Operators can terminate or restart a Gateway/Worker during
model, storage, or dangerous Tool work and receive a bounded, truthful outcome
without duplicate side effects, leaked goroutines, or false recovery success.

**Blocked by:** 06: Remote Dangerous Tool Confirmation; 07: Tenant Memory And
Knowledge; 08: Tenant Artifact And Audit Trace.

**Status:** resolved

- [x] SIGTERM independently bounds readiness withdrawal, ingress/acquisition
  stop, HTTP admission stop, wait, cancellation, event drain, and resource close.
- [x] Non-cooperative Runner work is timed out, marked unhealthy, and retired;
  every spawned drain goroutine is tracked and bounded.
- [x] Compose recovery covers two Gateways, Worker restart, PostgreSQL outage,
  model timeout, Tool failure, governance outage, IM retry, and duplicate input.
- [x] Every assertion is correlated to the injected request and checks the exact
  terminal state; unrelated healthy requests cannot create a false positive.
- [x] Recovery proves fencing and Tool outcomes prevent stale writes or automatic
  replay of uncertain side effects.
- [x] The ticket documents and runs its own shutdown/recovery acceptance command.

## Comments

有界关闭和完整恢复矩阵已由 `./scripts/stage7-compose-acceptance.sh` 验证。
