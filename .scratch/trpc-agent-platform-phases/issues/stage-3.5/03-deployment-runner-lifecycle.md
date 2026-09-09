# 03: Manage Deployment-Version Runner Lifecycle

**What to build:** Make each Active Deployment Version use a reusable framework Runner and release it safely when the version is retired or the service shuts down. New work is rejected at the correct lifecycle boundary, while active work observes cancellation and clean shutdown semantics.

**Blocked by:** 01: Pin Framework Runtime And Build Minimal Agent

**Status:** resolved

- [x] Requests for one Deployment Version reuse its Runner instance.
- [x] Requests for inactive or retired versions are rejected before framework execution.
- [x] Retiring a version calls Runner close exactly as required and does not affect other versions.
- [x] Service shutdown rejects new work, cancels active work, and releases Runner resources.
- [x] Context cancellation and Runtime Event channel draining or closing are covered by tests.
- [x] Repeated close and shutdown operations are safe and do not leak resources.

## Answer

Added per-Deployment-Version Runner caching, scope validation, Runtime stream admission, Context cancellation propagation, close-on-service shutdown, and idempotent close behavior. Lifecycle and framework integration tests pass under normal and race execution.
