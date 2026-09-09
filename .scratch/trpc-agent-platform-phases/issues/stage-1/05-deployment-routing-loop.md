# 05: Deployment Lifecycle And Routing Loop

**What to build:** An authorized operator can move a Deployment through the valid `draft -> published -> active -> paused` lifecycle, and a request for an active Agent App travels through Gateway and a stateless Worker to the replaceable fake Runner before returning a deterministic response. Routing is constrained by trusted Tenant Context and Deployment Version, while invalid lifecycle and routing cases fail predictably.

**Blocked by:** 04: Deployment Drafts And Immutable Versions

**Status:** resolved

- [x] The backend accepts only the defined Deployment lifecycle transitions and returns stable errors for skipped, reversed, repeated, or unauthorized transitions.
- [x] Publishing selects an immutable Deployment Version, activating makes that version routable, and pausing prevents new routing without deleting its history.
- [x] A Tenant and Agent App has at most one Active Deployment. Activating a second Deployment is rejected atomically with `409 agent_app_already_has_active_deployment` and does not change either Deployment.
- [x] The Management Console lets authorized roles publish, activate, and pause a Deployment, surfaces an activation conflict, and refreshes both Deployment records to their server-authoritative lifecycle and selected Version.
- [x] Gateway deterministically resolves the sole Active Deployment for the trusted Tenant and Agent App and delegates execution to a stateless Worker.
- [x] Worker invokes a replaceable Runner Adapter and returns the deterministic fake Runner result while propagating request context.
- [x] Requests for another Tenant, an unknown Agent App, a non-active Deployment, or a mismatched version cannot reach the Runner and return stable non-leaking errors.
- [x] Backend authorization defines and enforces which roles may publish, activate, pause, and invoke a Deployment.
- [x] `/v1/run` remains a compatible Stage 0 diagnostic endpoint while Stage 1 management operations remain under the management API boundary.
- [x] Repository, API, component, and Playwright tests prove the single-Active-Deployment invariant, deterministic routing, stable conflict response, preserved lifecycle state, and usable conflict treatment.

## Answer

Implemented and tested the strict draft/published/active/paused lifecycle, immutable version selection at publish, active-only routing, trusted Tenant/App resolution, stateless Worker execution through the replaceable Runner Adapter, and stable authorization/routing errors. The Management Console exposes version creation and lifecycle actions, and Stage 0 `/v1/run` remains separate and compatible.

## Comments

- Reopened after Stage 1 code review. Multiple Deployments for one Agent App could become active and map iteration made routing nondeterministic. Existing acceptance also did not execute both Tenant/App routes or prove that cross-Tenant guesses stop before the Runner.
- The complete two-Tenant/two-Agent-App routed acceptance matrix is isolated in Ticket 09 and starts after this invariant and Ticket 04's Version contract are complete.
- Resolved with an atomic activation invariant under the platform write lock, a stable conflict response, concurrent activation coverage, deterministic Runner selection, and server-authoritative Management Console refresh.
