# 09: Compose Fault Injection And Recovery Evidence

**What to build:** One reproducible Compose fault-injection workflow exercises component, dependency, runtime, IM, rollout, rollback, and capacity failures and records recovery evidence.

**Blocked by:** 04: Dependency Outage Recovery; 05: Runtime Timeout, Cancellation, And Event Draining; 06: Tenant Gray Release And Rollback; 07: IM Retry And Duplicate Recovery; 08: Capacity Estimation.

**Status:** resolved

- [x] A documented fault-injection command runs against the Compose environment and produces deterministic evidence without live provider credentials.
- [x] Injection can stop/restart components, degrade dependencies, delay or fail model/Tool execution, fail provider delivery, duplicate callbacks, and trigger drain.
- [x] Every scenario records its expected outcome, observed recovery, request/trace evidence, and terminal run or delivery state.
- [x] The console presents development/Compose-only fault controls, recovery results, component health, and rollout/drain evidence while production fault injection is disabled by default.
- [x] Production configuration rejects or ignores fault-injection controls; no request can enable production arbitrary fault injection.
- [x] Repeating the workflow from a clean environment produces the same recovery matrix and no leaked credentials or cross-Tenant data.
