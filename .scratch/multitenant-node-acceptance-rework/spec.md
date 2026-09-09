# Multi-Tenant And Node Acceptance Rework

Status: ready-for-human

## Objective

Close the remaining README acceptance gaps in multi-tenant isolation and
multi-Gateway execution. Preserve the public HTTP/SSE contracts unless a ticket
explicitly says otherwise. The completed implementation must prove that node
placement cannot change tenant routing, Deployment Version selection, request
idempotency, or cancellation behavior.

## Fixed Findings

1. Deployment Version IDs are only unique inside a Tenant, but several runtime
   lookups and Worker caches treat them as globally unique.
2. real-IM Bot Routes are stored in per-Gateway JSON files instead of the
   shared Control Plane Store.
3. `activeRuns` is process-local, so a retry or cancellation sent to another
   Gateway cannot coordinate with the owner.
4. `Tenant` has no defined Audit Policy even though the data-model document
   claims one.
5. the standalone Worker shuts down HTTP without closing its Runner runtime.

## Design Decisions

### 1. Tenant-scoped Deployment Version identity

Keep the externally visible Version ID format (`<deployment_id>-v<number>`) for
compatibility. Treat the stable identity as:

```go
type DeploymentVersionRef struct {
	TenantID string
	VersionID string
}
```

`DeploymentVersionRef` is the only key accepted by runtime resolution, Runner
caches, active-run maps, retirement, and the Worker's version store. Do not
repair the collision by globally scanning and checking the returned Tenant
afterward: the wrong configuration must never be selected or cached.

Change the Control Plane interface from:

```go
DeploymentVersion(context.Context, string) (DeploymentVersion, bool, error)
```

to a tenant-scoped lookup, preferably:

```go
DeploymentVersion(context.Context, DeploymentVersionRef) (DeploymentVersion, bool, error)
```

The implementation directly searches the Tenant's version collections. The
runtime obtains the Tenant from trusted `TenantContext`; the Worker obtains it
from the verified Execution Manifest. A client-supplied Tenant ID is never
used to construct the ref.

The same ref keys `FrameworkRunnerAdapter.runners`, active runs by version, and
`workerVersionStore`. `RetireVersion` also accepts the ref. Update
`docs/data-model.md` to define Version uniqueness as
`(tenant_id, version_id)` instead of calling `version_id` global.

### 2. Shared Provider Route Registry

Provider routes are Control Plane configuration. Add them to
`controlPlaneSnapshot` and expose one package-private module interface:

```go
type ProviderRouteRegistry interface {
	Resolve(context.Context, string, string) (BotRoute, bool, error)
	List(context.Context) ([]BotRoute, error)
	Upsert(context.Context, BotRoute) (BotRoute, error)
	Update(context.Context, string, string, BotRoute) (BotRoute, error)
	Delete(context.Context, string, string) error
}
```

The two strings in lookup/mutation are the current `(provider,
external_subject)` identity. Do not broaden the key to provider account in this
rework unless a red test demonstrates that requirement; keep the change focused.

Implement the interface on `SnapshotControlPlane` using the existing
refresh-revision-copy-mutate-CAS pattern already used by Channel Bindings.
Provider callback resolution must refresh the shared snapshot and return
`control_plane_unavailable` on read failure. It must not fall back to a stale
process-local route.

`ProviderRuntime` depends on `ProviderRouteRegistry`, not a concrete
`BotTenantAllowlist`. HTTP route CRUD and callback resolution cross the same
interface, so a route created through Gateway B is immediately usable by the
Channel Adapter attached to Gateway A.

Single-node development may keep an in-memory Control Plane. Remove
`TRPC_BOT_ROUTES_PATH` from the Stage 7 Compose topology. Keep legacy JSON
reading only as an explicit migration input; never make it a second runtime
source of truth. If legacy migration is implemented, it is idempotent and
refuses conflicting entries.

### 3. Distributed Run Coordinator

Create a deep `RunCoordinator` module. It owns execution claim, same-Session
serialization, fencing, owner-loss detection, duplicate request recognition,
and cross-Gateway cancellation. HTTP handlers and `Runtime` must not implement
their own partial versions of these rules.

```go
type RunKey struct {
	TenantID string
	SessionID string
	RequestID string
}

type RunPermit interface {
	FencingToken() uint64
	Lost() <-chan struct{}
	CancelRequested() <-chan struct{}
	Finish(context.Context, RunTerminal) error
	Release()
}

type RunCoordinator interface {
	Claim(context.Context, RunKey, string) (RunPermit, ClaimDisposition, error)
	RequestCancel(context.Context, RunKey) (CancelDisposition, error)
	Close() error
}
```

`inputHash` is a SHA-256 hash of the normalized persisted input identity. It
detects reuse of one request ID with different input without storing message
content in the coordinator.

PostgreSQL is the production adapter; an in-memory adapter is used by unit tests
and single-node development. Add a durable table through Control Plane schema
version 2:

```sql
CREATE TABLE run_executions (
  tenant_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  enqueue_order BIGSERIAL NOT NULL,
  owner_id TEXT NOT NULL,
  fencing_token BIGINT NOT NULL,
  state TEXT NOT NULL,
  lease_expires_at TIMESTAMPTZ NOT NULL,
  cancel_requested_at TIMESTAMPTZ,
  terminal_type TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, session_id, request_id)
);
CREATE UNIQUE INDEX run_executions_one_owner_per_session
  ON run_executions (tenant_id, session_id)
  WHERE state = 'running';
```

Allowed states are `queued`, `running`, `completed`, `failed`, `cancelled`, and
`outcome_unknown`. Fencing tokens are monotonic per `(tenant_id, session_id)`;
reuse the existing `session_execution_leases` counter or migrate it into this
module without maintaining two independent fencing authorities.

Claim semantics:

1. Validate the trusted Tenant and normalized input hash.
2. If the same RunKey has a different hash, return `idempotency_key_reused`.
3. If it is terminal, return `existing_terminal`; no Runner or Tool is invoked.
4. If it has a live owner, return `already_running`; do not create a second
   goroutine that waits and later executes the same request.
5. Different request IDs for the same Session wait in arrival order or return a
   stable queued disposition. Promotion selects the lowest `enqueue_order`,
   matching current same-Session serialization without limiting the queue to
   one row.
6. After lease acquisition, re-read persisted Session Events before Runner
   invocation. A terminal event wins over coordinator state and is reconciled
   into the table.
7. On owner loss, issue a higher fencing token. An interrupted dangerous Tool
   remains `outcome_unknown`; it is never automatically replayed.

Cancellation semantics:

1. `RequestCancel` is idempotent and works from any Gateway.
2. The owning permit observes cancellation promptly. PostgreSQL may use bounded
   polling on lease renewal; the acceptance bound is two renewal intervals.
3. Cancellation composes with request context, service shutdown, lease loss,
   and runtime timeout into one run context.
4. The owner persists exactly one `run.cancelled` terminal event and marks the
   durable execution cancelled. A stale owner cannot write after fencing loss.
5. The public endpoint returns `running`/`cancellation_requested`/terminal based
   on shared state; it never reports `pending` merely because the request hit a
   non-owner Gateway.

Move the orchestration now split across `AdminHandler.startChatRun`, `runChat`,
`activeRuns`, and Runtime lease acquisition behind this module. A process-local
map may remain only as a latency optimization for immediate cancellation; it is
not authoritative. Remove duplicate lease ownership from `Runtime` after all
callers use the coordinator.

### 4. Tenant Audit Policy

Add a concrete, non-secret policy to the Tenant model:

```go
type AuditContentMode string

const (
	AuditMetadataOnly AuditContentMode = "metadata_only"
	AuditRedactedSummary AuditContentMode = "redacted_summary"
)

type AuditPolicy struct {
	RetentionDays int `json:"retention_days"`
	ContentMode AuditContentMode `json:"content_mode"`
	HighRiskFailureMode string `json:"high_risk_failure_mode"`
}
```

Defaults are 90 days, `metadata_only`, and `fail_closed`. Only supported enum
values and a bounded positive retention (1-3650 days) are accepted. Secrets,
raw Tool arguments, model credentials, and raw IM credentials never become
policy fields.

Embed the policy in `Tenant`, persist it in the Control Plane snapshot, expose
it in Tenant responses, and add a tenant-scoped update operation. A
`platform_admin` may update any Tenant; a `tenant_admin` may update its active
Tenant; other roles are denied. Existing Tenants receive deterministic defaults
on snapshot load.

Wire behavior far enough that the policy is not a placeholder:

- audit query and retention cleanup use `RetentionDays`;
- content capture follows `ContentMode` and always passes through redaction;
- high-risk operations remain fail-closed; this rework does not permit a
  fail-open mode.

Update `docs/data-model.md` and `docs/architecture.md` with the exact fields,
defaults, ownership, and enforcement points.

### 5. Worker lifecycle

Make `WorkerServer` implement `io.Closer` by delegating to its
`FrameworkRunnerAdapter.Close()`. Closing is idempotent. Standalone Worker
shutdown order is:

1. stop HTTP admission with the existing bounded context;
2. call `worker.Close()` to cancel and drain active runs and close cached
   upstream Runners;
3. report close errors without skipping process exit.

Add a lifecycle test with a blocking Runner that proves SIGTERM-equivalent
shutdown cancels execution, closes the event stream, and calls Runner Close
exactly once.

## Migration And Compatibility

- Bump `ControlPlaneSchemaVersion` from 1 to 2.
- `control-migrate` creates the distributed run table and any Provider Route
  storage required by the chosen snapshot representation.
- Existing serialized snapshots decode with missing `provider_routes` and
  `audit_policy`; normalize both after load.
- Existing public Version IDs, URLs, SSE envelopes, and error codes remain
  stable.
- Stage 7 Compose uses PostgreSQL for Provider Routes and Run Coordinator state.
- Delete the two Gateway-specific Bot Route paths from Compose after the shared
  registry test is green.

## Acceptance Matrix

The rework is complete only when every row has an automated assertion:

| Scenario | Required result |
| --- | --- |
| Two Tenants both create `deploy-main-v1` | each runtime and Worker receives its own model/prompt/tools |
| Version cached for Tenant A, then Tenant B runs same Version ID | separate Runner instances/configuration |
| Gateway B creates or disables a Provider Route | Gateway A callback immediately observes it |
| Control Plane fails during Provider Route resolution | callback fails closed with stable non-leaking error |
| Same request concurrently submitted through A and B | Runner and Tool invoked once |
| Same request retried on B after A completes | existing terminal returned; no invocation |
| Cancel sent to B for run owned by A | A cancels within two renewal intervals; one cancelled terminal |
| A loses lease while executing | B gets higher fence; stale A cannot write |
| Same request ID with different input across Gateways | `idempotency_key_reused` and no second invocation |
| Same Session, different requests | serialized |
| Different Sessions | concurrent |
| Tenant audit policy default/update/reload | isolated, validated, and durable |
| Audit retention/content mode | enforced without exposing secrets |
| Worker shutdown during a run | run cancelled/drained and Runner closed once |

## Required Gates

Run all of the following from the repository root:

```bash
./format.sh
go test ./...
go test -race ./...
./lint.sh
./build.sh
npm --prefix frontend run typecheck
npm --prefix frontend test
npm --prefix frontend run build
npm --prefix frontend run test:e2e
./scripts/verify-docs.sh
./scripts/stage7-compose-acceptance.sh
```

Extend `stage7-compose-acceptance.sh` with the cross-Gateway Provider Route,
duplicate request, and remote cancellation cases. A green existing script
without those assertions does not complete this rework.

## Recommended Execution Order

Luna should execute the tickets in this order and keep each numbered step as a
separate reviewable commit:

1. Ticket 01: establish tenant-scoped Version identity before touching run
   coordination.
2. Ticket 02: move Provider Routes into the shared Control Plane snapshot.
3. Ticket 04: add Audit Policy while the snapshot migration context is fresh;
   finish the schema-version bump only after all schema additions are known.
4. Ticket 03: implement the Run Coordinator and finalize schema version 2.
5. Ticket 05: close the Worker runtime lifecycle.
6. Ticket 06: extend Compose, update all design/acceptance documents, run the
   complete gate, and record evidence.

After every ticket, run focused tests plus `go test ./...`. Run race, frontend,
and Compose gates at Ticket 06. If a ticket changes a stable public contract,
stop and document why the compatibility-preserving design cannot work before
continuing.

## Completion Boundary

This effort is complete when all six tickets are resolved, the acceptance
matrix is automated, and the full gates pass from a clean checkout. S3,
Qdrant/Milvus, Kubernetes manifests, and globally shared governance/audit
runtime are outside this rework unless a required test proves they are needed
to close one of the fixed findings.
