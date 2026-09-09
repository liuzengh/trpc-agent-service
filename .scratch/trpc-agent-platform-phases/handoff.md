# Stage 1 Handoff To Stage 2

Status: ready

## Frozen Inputs

- Management APIs remain under `/api/v1/admin/...`; identity APIs remain under `/api/v1/auth/...`; Stage 0 compatibility remains `/v1/run`.
- `DevelopmentIdentity` assigns one of `platform_admin`, `tenant_admin`, `operator`, or `viewer` to server-approved Tenants. An opaque HttpOnly development session selects the active assignment.
- `TenantContext` is injected only by trusted server code and contains Tenant, user, and role. Client Tenant fields and headers are not authority.
- Deployment lifecycle is `draft -> published -> active -> paused`. Publishing selects one existing immutable Deployment Version. Within a Tenant and Agent App, at most one Deployment is active; a competing activation fails atomically with `409 agent_app_already_has_active_deployment` and does not perform an implicit rollout or rollback.
- Deployment Version numbers are monotonically increasing within a Deployment. Creation requires a 1–128 printable ASCII `Idempotency-Key` scoped to `(Tenant ID, Deployment ID, key)`: matching replay returns the original `201` result, payload mismatch returns `409 idempotency_key_reused`, and concurrent replay creates exactly one Version.
- Gateway resolves the sole Active Deployment in trusted Tenant scope, and Worker invokes `RunnerAdapter` without keeping business state.
- Session serialization uses the tuple `(Tenant ID, Agent App ID, Session ID)`. Waiters and active work must propagate `context.Context` cancellation, and unused coordination keys are released.
- Runtime status reports stable Gateway/Worker identities, lifecycle, availability, and bounded execution counters only.
- The single React/TypeScript/Vite source tree is `frontend/`. Development proxies `/api`; production assets are built into `trpcservice/web/dist` and embedded in the Go binary.

## Stage 2 May Replace

Stage 2 may replace process-local resource/session storage through explicit storage adapters and add immutable Session Events/materialized views. It must preserve Tenant isolation, cancellation, routing, lifecycle, identity/API conventions, progressive frontend ownership, and Stage 0 compatibility.

## Rework Completion

Tickets 04, 05, 07, 08, and 09 are `resolved`. The two-Tenant/two-App routing matrix and Version idempotency contract pass under normal and race-enabled tests, the frontend Worker `error` contract passes typed component coverage, and every Stage 1 acceptance command passes.

## Reproduction

Use the commands in `acceptance.md`. No secret, model credential, real IM provider, external database, or undocumented local state is required.
