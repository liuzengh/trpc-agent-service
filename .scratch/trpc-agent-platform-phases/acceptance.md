# Stage 1 Acceptance

Status: passed

## Scope

Stage 1 delivers the in-memory multi-Tenant management and routing vertical slice: server-validated Development Identity, trusted Tenant Context, backend role enforcement, Tenant/Agent App/Deployment/Deployment Version management, the Deployment lifecycle, Gateway/Worker/fake Runner routing, Session serialization and cancellation, runtime status, and the first increment of the progressive Management Console.

## Automated Gate

Run from the repository root unless noted:

```bash
./format.sh
go test ./...
go test -race ./...
./lint.sh
./build.sh
cd frontend
npm run typecheck
npm test
npm run build
npm run test:e2e
```

The Playwright suite runs the packaged Go service and repeats the full Development Identity, two-Tenant/two-Agent-App route matrix, Tenant switch, Deployment Version, lifecycle, cross-Tenant rejection, and Gateway/Worker status workflow at desktop and mobile viewports. It asserts that the final page has no horizontal document overflow and records a full-page runtime-status screenshot per project.

## Rework Gate Evidence

- Two Tenants and two Agent Apps with independent Active Deployment Versions pass through integration and packaged Playwright paths. Runner capture proves the trusted Tenant, Deployment, and Version, and a cross-Tenant App guess is non-leaking and never reaches the Runner.
- Concurrent activation proves a Tenant and Agent App has exactly one Active Deployment. The competing activation returns `409 agent_app_already_has_active_deployment` atomically and preserves the published loser.
- Required `Idempotency-Key` validation, normalized matching replay, `409 idempotency_key_reused`, same-config creation under a different key, cross-scope key reuse, and exactly-once numbering under concurrent replay pass.
- The frontend reuses a Version creation key after an ambiguous network failure, rotates it after success/configuration change, and visibly distinguishes the Worker `error` lifecycle.
- Every command in the Automated Gate passed after the final implementation and packaged asset build.

## Proven Behavior

- Tenant authority comes only from the opaque server-held Development Identity session. Request bodies and arbitrary headers cannot grant Tenant access.
- `platform_admin`, `tenant_admin`, `operator`, and `viewer` are shared frontend/backend roles, and mutations are enforced by the backend.
- Two-Tenant API tests prove Agent App and Deployment ownership isolation with non-leaking not-found responses. Routed two-Tenant/two-App acceptance remains part of the pending rework gate above.
- Deployment Versions are immutable deep copies with monotonically increasing numbers.
- Deployment Version creation is idempotent within `(Tenant ID, Deployment ID, Idempotency-Key)` for the process lifetime.
- Only `draft -> published -> active -> paused` is accepted. Publishing selects an existing version, active is routable, and paused rejects new routing.
- Each Tenant/Agent App has at most one Active Deployment; competing activation is rejected atomically without implicit rollout or rollback.
- Same Tenant/App/Session executions serialize; different Sessions run concurrently. Waiting and active work observe cancellation and service shutdown.
- Gateway and Worker status is backend-reported, bounded, and excludes request input and Tenant business data.
- Platform administrators receive platform-wide runtime counters; tenant roles receive only active-Tenant counters. Worker status distinguishes healthy, unavailable, closing, and execution-error states.
- The packaged desktop/mobile workflow includes a viewer assignment and proves management mutations are hidden while the backend returns a forbidden runtime-status state.
- `/healthz`, `/version`, and `/v1/run` retain Stage 0 behavior.

## Exclusions And Limitations

State, including Version idempotency records, is process-local and intentionally resets on restart. Development Identity is not production authentication. The Runner is deterministic and fake. Shared storage, real models, Session history, IM providers, policy governance, production observability, rollout, and rollback remain assigned to later stages. No credentials or external services are required.
