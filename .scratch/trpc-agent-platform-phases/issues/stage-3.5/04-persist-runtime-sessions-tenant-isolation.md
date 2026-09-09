# 04: Persist Runtime Sessions With Tenant Isolation

**What to build:** Persist framework execution inputs, outputs, failures, and cancellations through the Stage 2 tenant-selected Storage Adapter so refresh, retry, and cross-node reads retain Stage 3 behavior without allowing Runtime data to cross Tenant or Deployment Version boundaries.

**Blocked by:** 02: Bridge Runtime Requests And Events; 03: Manage Deployment-Version Runner Lifecycle

**Status:** resolved

- [x] Input, output, failure, and cancellation events are persisted under the correct Tenant and Session.
- [x] The existing `Tenant + Session + request_id` idempotency behavior remains intact for framework execution.
- [x] Stage 2 Redis and SQLite-compatible backends can store and restore framework execution results.
- [x] Cross-Tenant requests are rejected before reaching the framework Runner.
- [x] Refresh restores history and terminal state from persisted Session Events.
- [x] Session and Memory access cannot cross Tenant or Deployment Version boundaries.

## Answer

Connected framework-backed Chat events to the existing Session Event persistence path, preserved request idempotency and recovery semantics, propagated trusted user and tenant identity, and added explicit Tenant/App/Deployment/Version scope validation before Runner execution.
