# 01: Tenant-Scoped Deployment Version Identity

Type: task
Status: ready-for-human

## Goal

Make Deployment Version resolution and Runner caching impossible to collide
across Tenants while preserving existing public Version IDs.

## Work

- Introduce `DeploymentVersionRef{TenantID, VersionID}`.
- Change the Control Plane resolver and every caller to require the ref.
- Key `FrameworkRunnerAdapter` runners/runs and `workerVersionStore` by the ref.
- Change retirement and chat/runtime validation to use the ref.
- Correct the uniqueness claim in `docs/data-model.md`.

## Tests

- Two Tenants create the same App/Deployment/Version IDs with different model,
  prompt, and Tool configuration; each request uses only its Tenant's config.
- Prime the Runner cache with Tenant A, then execute Tenant B and assert distinct
  Runner construction.
- Send a signed Worker manifest for both Tenants with the same Version ID and
  assert both are accepted with isolated configuration.

## Done

No code path resolves, caches, retires, or executes a Version using Version ID
alone, and the focused tests pass under `go test -race`.

## Evidence

- `go test -race ./trpcservice/platform/...` passed.
- Stage 7 Compose passed the cross-Gateway deployment/version route checks.
