# 02: Gateway/Worker Dispatch Boundary

**What to build:** A Chat or provider request flows through a separately running Gateway and Worker boundary while preserving trusted Tenant, Deployment Version, Session, request, and trace identity from ingress to persistence.

**Blocked by:** 01: Operations Health And Graceful Drain Baseline.

**Status:** resolved

- [x] Gateway performs trusted identity, policy admission, routing resolution, and dispatch to an internal Worker interface; request fields cannot override Tenant, role, or routing authority.
- [x] Worker remains stateless and resolves Session/Memory through the Tenant-selected shared backend rather than relying on sticky placement.
- [x] A request can complete on either an existing or newly restarted Worker, and the same Tenant/App/Session remains serialized while different Sessions can execute concurrently.
- [x] Gateway returns stable, non-leaking errors when the Worker is unavailable, closing, or unhealthy, and reports the failure in component health.
- [x] A Worker stop/restart during active work produces a terminal failure or recovery path without duplicate terminal Session Events.
- [x] Tests prove identity and trace propagation, Worker restart behavior, concurrency boundaries, stable errors, and no cross-Tenant leakage.
