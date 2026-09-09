# 08: Capacity Estimation

**What to build:** Operators can run a bounded capacity smoke test and view safe-concurrency, latency, token, cost, and bottleneck estimates for a Tenant and Agent App.

**Blocked by:** 05: Runtime Timeout, Cancellation, And Event Draining.

**Status:** resolved

- [x] An operator can start a bounded capacity test for one Tenant/App with explicit concurrency, run count, and timeout inputs validated by the backend.
- [x] The test uses deterministic or injectable runtime behavior, never requires real model credentials, and cannot be used as an unauthorized production load generator.
- [x] Results include safe concurrency, throughput, model/Tool/storage latency, active/failed/completed counts, token and cost estimates, and the first identifiable bottleneck.
- [x] Capacity evidence is linked by request/trace identity and is visible in the active Tenant scope only.
- [x] Running the test respects governance budget and rate limits, service drain, shutdown, and cancellation without corrupting Session Events.
- [x] Backend and console tests cover successful estimation, cancellation, resource exhaustion, isolation, bounded outputs, and result presentation.
