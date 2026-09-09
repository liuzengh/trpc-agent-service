# Stage 6 Handoff

Status: ready

Stage 6 freezes the following contracts:

- `TRPC_SERVICE_ROLE=gateway|worker` selects the process role. A Worker
  exposes only `/healthz` and `/internal/worker/run`; the internal endpoint
  requires a bearer token and rejects requests whose embedded Version does not
  exactly match the server-resolved immutable Version.
- Gateway owns trusted identity, policy admission, routing, and dispatch. A
  request cannot override Tenant, role, Deployment, or Version authority.
- `TenantPolicy.runtime_timeout_ms` is server-owned, defaults to 30 seconds,
  and is bounded from 1 ms to 300 seconds. Timeout and cancellation produce one
  terminal Session Event and release runtime, storage, provider, and lifecycle
  resources.
- Dependency operations use bounded service contexts and stable public codes
  for `request_cancelled`, `service_closing`, `storage_timeout`, and
  `storage_unavailable`.
- Deployment rollout state records current, previous, and target immutable
  Versions plus gray percentage. Routing is deterministic from a Tenant-scoped
  request ID. Rollout and rollback require operator role and explicit
  confirmation and record Audit Events.
- Capacity tests are bounded to concurrency 1–10, runs 1–100, and timeout
  100–5000 ms. They use deterministic local execution and remain subject to
  governance admission.
- Runtime fault injection is development/Compose-only. Production identity
  disables it, and ordinary channel binding creation remains available in
  production.
- Compose recovery evidence records Worker restart, dependency outage, runtime
  fault, IM retry/duplicate callback, rollout/rollback, capacity, and graceful
  drain scenarios without live provider credentials.

## Reproduction

Use the commands in `acceptance.md` and the operational contract in
`docs/stage-6-operations.md`. No secret, hosted model, live IM provider, or
Kubernetes cluster is required.
