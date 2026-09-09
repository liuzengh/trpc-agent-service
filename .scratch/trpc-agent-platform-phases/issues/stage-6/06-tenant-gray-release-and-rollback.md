# 06: Tenant Gray Release And Rollback

**What to build:** Operators can progressively route a Tenant to a new Deployment Version, preview rollback, confirm it, and observe audited rollout state and recovery behavior.

**Blocked by:** 03: Compose Multi-Component Baseline.

**Status:** resolved

- [x] A Deployment can hold an explicit rollout state with target, current, previous version, gray percentage, and bounded stable status fields.
- [x] Gray routing is server-owned and deterministic in trusted Tenant scope, with one active version selected for each request and no cross-Tenant routing.
- [x] New and in-flight work remain isolated: a rollout does not mutate immutable Versions or existing active runs, and a Worker restart does not assign a different version to the same request.
- [x] Rollback preview shows the prior version, affected Tenant/App, active work, and expected result before any change is committed.
- [x] Rollout and rollback require operator authorization and explicit confirmation and each action records an Audit Event with trace/request context.
- [x] Console and API tests prove gray progression, rollback preview/confirmation, unauthorized rejection, audit evidence, and failure recovery to the previous active version.
