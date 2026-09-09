# 04: Dependency Outage Recovery

**What to build:** Redis and SQL outages are classified, bounded, and recoverable, so operators can distinguish dependency failure from Worker failure and service restarts without Tenant/session data corruption.

**Blocked by:** 03: Compose Multi-Component Baseline.

**Status:** resolved

- [x] Dependency reads and writes use bounded service contexts and expose stable public errors for timeout, unavailable, and closing states.
- [x] Component health reports the affected dependency as degraded or unavailable while preserving sanitized diagnostics and Tenant isolation.
- [x] An outage during execution does not leave a run without a recoverable terminal state, and bounded failure persistence is attempted without blocking shutdown indefinitely.
- [x] Restoring Redis or SQL allows subsequent requests to succeed without changing event identity, sequence, idempotency, or Tenant scope.
- [x] A service restart during dependency recovery preserves previously committed shared-storage state and rejects idempotency-key reuse with different content.
- [x] Compose-driven or deterministic integration tests exercise dependency stop, request failure, restart, recovery, and clean shutdown.
