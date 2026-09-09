# Stage 6 Failure Recovery And Operations

Stage 6 separates the Gateway and Worker process roles, adds bounded
dependency operations, server-owned runtime timeout policy, graceful drain,
Tenant gray release/rollback, bounded capacity estimation, and reproducible
Compose recovery evidence. The runtime remains the published
`trpc-agent-go v1.11.2` Runner; platform code owns routing, identity,
governance, storage, Channel, and lifecycle boundaries.

## Process Topology

- **Gateway** authenticates the operator or provider, resolves the active
  Deployment and immutable Version, applies governance, persists Session input,
  and dispatches the resolved request to a Worker.
- **Worker** is stateless. It receives an authenticated internal request that
  already carries Tenant/App/Deployment/Version identity, resolves the Agent
  through `AgentFactory`, and streams platform runtime events back to the
  Gateway.
- **Storage Adapter** is selected per Tenant and may be InMemory, Redis,
  SQLite, or PostgreSQL. Session Events use idempotency keys and per-Tenant
  sequence identity, so a restarted Worker can read shared state without sticky
  placement.
- **Channel Adapter** handles Mock, Telegram, and Enterprise WeChat ingress
  and replies. Provider delivery attempts and terminal states remain bounded.
- **Governance/Telemetry** owns admission, budget/rate state, Audit Events,
  traces, and metrics. High-risk operations require role, confirmation, and
  audit evidence.

`TRPC_SERVICE_ROLE=gateway|worker` selects the process role. The Worker exposes
only `/healthz` and `/internal/worker/run`; the internal endpoint requires a
bearer token and rejects requests whose embedded Version does not exactly match
the server-resolved immutable Version.

## Operations APIs

- `GET|POST /api/v1/admin/operations/drain`: operator role plus
  `{"confirm":true}` starts graceful shutdown. New work is rejected, active
  work finishes, and the operation records `operations.drain.started`.
- `GET /api/v1/admin/runtime/status`: Gateway, Worker, and selected storage
  dependency health with active/completed/failed counters.
- `GET|POST /api/v1/admin/operations/faults`: development/Compose-only Runner
  delay/error and Tool error injection. Production authentication disables it.
- `GET|POST /api/v1/admin/deployments/{id}/rollout`: confirms a target Version
  and gray percentage. Routing is deterministic by Tenant/App/request ID.
- `GET /api/v1/admin/deployments/{id}/rollback-preview`: shows previous
  Version, affected App, active executions, and expected result without
  mutating state.
- `POST /api/v1/admin/deployments/{id}/rollback`: confirms rollback to the
  previous immutable Version and records an Audit Event.
- `POST /api/v1/admin/capacity`, `GET /api/v1/admin/capacity/{id}`, and
  `POST /api/v1/admin/capacity/{id}/cancel`: bounded deterministic capacity
  estimation with request/trace identity.

Drain, rollout, and rollback require Audit Event persistence before their
state changes. If audit persistence fails, the API returns `audit_unavailable`
and leaves the previous operation state unchanged.

Rollouts never mutate Version content. The request's selected Version is
resolved before dispatch and embedded in the Worker request, so a Worker
restart cannot silently move an in-flight request to another Version.

## Runtime And Dependency Recovery

`TenantPolicy.runtime_timeout_ms` is server-owned and bounded from 1 ms to
300,000 ms; the default is 30,000 ms. The timeout context wraps the entire
runtime stream, so blocked model/Tool work ends as one `run.cancelled` terminal
event and releases the session gate and lifecycle lease.

Session storage reads and writes use bounded service contexts and stable public
errors:

- `request_cancelled`
- `service_closing`
- `storage_timeout`
- `storage_unavailable`

Failure persistence uses a bounded context that is independent of the client
request cancellation. Dependency health is separated from Worker health:
storage unavailable does not mark the Worker failed, and Worker failure does
not mutate shared Session state.

## Capacity Estimation

A capacity request is limited to concurrency 1–10, run count 1–100, and timeout
100–5,000 ms. It uses the deterministic local Runner, never real model
credentials, and does not write Session Events. Governance admission is still
required, so budgets and Tenant rate limits apply. Results include safe
concurrency, throughput, model/Tool/storage latency, completed/failed counts,
estimated tokens/cost, the first identifiable bottleneck, request ID, and trace
ID. Service drain, shutdown, and explicit cancellation terminate the run as a
terminal capacity state without leaving an active governance reservation.

The planning inputs make the production assumptions explicit instead of hiding
them in a node count:

- `peak_im_callbacks_per_second`: maximum one-minute callback rate observed per
  Tenant, multiplied by the expected campaign/reconnect burst factor.
- `average_tokens_per_session`: completed-run tokens divided by completed runs
  over a representative seven-day window. Size model quotas separately against
  the p95 value; when no measured value is supplied, the policy's
  `estimated_tokens_per_run` is used.
- `redis_operations_per_session` and `sql_operations_per_session`: read/write
  counts obtained from storage spans or backend metrics for one completed run.
- `headroom_percent`: capacity reserved for retries, uneven Tenant routing and
  dependency latency; the API defaults to 30 percent.

For callback peak `P`, average tokens `T`, Redis operations `R`, SQL operations
`S`, observed single-node throughput `Q`, safe concurrency `C`, and headroom
fraction `H`, the result reports:

```text
token/s                 = P * T
Redis QPS               = P * R
SQL QPS                 = P * S
sessions per Worker     = max(1, floor(C * (1-H)))
recommended Workers     = max(1, ceil(P / (Q * (1-H))))
```

Run the bounded estimator as a preflight, then validate its assumptions with a
stair-step test against a staging deployment using production-like model,
Tool, Redis and SQL latency. Stop increasing concurrency when the error rate is
non-zero, p95 latency exceeds the SLO, a backend reaches its connection/QPS
budget, or the IM backlog grows. Use the last healthy step as `C` and `Q`; do
not treat the deterministic smoke result as a production benchmark.

## Compose Recovery Evidence

Baseline:

```bash
./scripts/stage6-compose-smoke.sh
```

Fault and recovery matrix:

```bash
./scripts/stage6-compose-recovery.sh
```

The recovery script builds Gateway, Worker, Redis, and PostgreSQL, then
exercises Worker restart, PostgreSQL outage/recovery, Runner/Tool failure,
Mock IM retry and duplicate callback, gray rollout/rollback, capacity smoke,
and graceful drain. It writes deterministic evidence to
`.scratch/stage6-compose-recovery.json` and tears down volumes unless
`STAGE6_KEEP_COMPOSE=1` is set.

## Kubernetes Guidance

Kubernetes is a production profile and is not required for local acceptance.

The minimum runnable profile is `compose.stage6.yml`: one Gateway, one Worker,
one Redis and one PostgreSQL, plus the control-plane migration job. The
production starting profile is two or more Gateway replicas across failure
domains, at least the capacity result's `recommended_worker_nodes` Workers
(never fewer than two for availability), managed PostgreSQL and Redis, and an
external telemetry/audit backend. Recalculate Worker replicas from IM peak and
model throughput; size Redis and SQL against the reported QPS plus the same
headroom, rather than copying the Compose resource limits.

- Run Gateway and Worker as separate Deployments. Gateway replicas are stateless
  behind a Service; Worker replicas are stateless behind an internal Service.
  Do not expose `/internal/worker/run` outside the cluster.
- Use `readiness` for Gateway `/healthz`, `liveness` for process health, and a
  startup probe that waits for dependency health. Prefer separate dependency
  health checks in readiness so dependency failure removes traffic without
  restarting healthy Workers.
- Configure `terminationGracePeriodSeconds` above the maximum runtime timeout.
  On SIGTERM, call drain, stop accepting new work, wait for active work, close
  storage, then exit.
- Use rolling updates with max unavailable 0 and max surge 1. Keep immutable
  Versions in shared storage; do not rely on container image identity for Agent
  Version routing.
- Bound CPU/memory per pod, set `GOMAXPROCS`, and use horizontal autoscaling on
  queue depth or active execution rather than CPU alone.
- Put Worker tokens, JWT secrets, identity directory, and provider credentials
  in Kubernetes Secrets or an external secret manager. Do not place secrets in
  ConfigMaps, images, command arguments, logs, or traces.
- Keep PostgreSQL/Redis as managed or Stateful workloads with backups and
  volume snapshots. The platform assumes shared Session/Memory state and does
  not support a per-Worker-only SQL database for multi-node operation.
- Separate non-secret configuration, secret references, shared data volumes, and
  audit/governance persistence. Run migration jobs before rollout.
- Collect structured logs, metrics, and traces in a Tenant-aware backend;
  retain Audit Events according to compliance policy.

## Risk Register

| Risk | Impact | Mitigation | Acceptance classification |
| --- | --- | --- | --- |
| Worker loss during an active run | Terminal failure or lost work | resolved immutable request, shared storage, stable `worker_unavailable`, retry/recovery evidence | blocking when recovery is absent |
| Redis or SQL outage | Chat requests cannot persist input/output | bounded contexts, dependency health, stable errors, recovery script | blocking when recovery is absent |
| Unbounded model/Tool execution | Hangs and shutdown failure | server-owned policy timeout, lifecycle leases, race tests | blocking-acceptance |
| Duplicate provider callback | Duplicate execution or reply | request-ID idempotency, terminal-event checks, duplicate callback test | blocking-acceptance |
| Gray release changes in-flight routing | Wrong Version or cross-Tenant routing | immutable Versions, request-embedded Version, deterministic Tenant-scoped hash | blocking-acceptance |
| Capacity test becomes load generator | Resource exhaustion | role, bounds, governance, deterministic Runner, cancellation | blocking when unauthorized or unbounded |
| Production fault injection | Unauthorized failure injection | disabled under production identity, server configuration only | blocking-acceptance |
| Secret leakage in evidence/logs | Credential exposure | no credentials in evidence, write-only redaction, sanitized public errors | blocking-acceptance |
| Distributed governance counters drift | Budget bypass in future multi-node production | current local atomic state, documented shared-state requirement | non-blocking quality for local Stage 6; production prerequisite |
| Slow event consumer blocks shutdown | Goroutine/lease leak | lifecycle cancellation, channel drain/close, race tests | blocking-acceptance |

## Known Limitations

- Kubernetes manifests are guidance rather than a required local acceptance
  artifact.
- Governance state is an atomic local JSON snapshot; a distributed control-plane
  store and global rate/budget counters remain production work.
- Capacity preflight is deterministic and bounded; production sizing still
  requires the documented staging stair-step measurement.
- Gray rollout percentage is deterministic by request ID; it does not yet model
  user cohort affinity or external feature-flag systems.
- The Compose Worker uses a fixed local token for acceptance. Production must
  issue and rotate Worker credentials through a secret manager.

## Validation Matrix

| Capability | Commands/evidence |
| --- | --- |
| Backend and race behavior | `go test ./...`, `go test -race ./...`, `go vet ./...` |
| Frontend workflows | `cd frontend && npm run typecheck && npm test && npm run build && npm run test:e2e` |
| Packaged service | `./build.sh` |
| Zero-to-working Compose | `./scripts/stage6-compose-smoke.sh` |
| Component/dependency/runtime/IM/rollout/capacity/drain recovery | `./scripts/stage6-compose-recovery.sh` and `.scratch/stage6-compose-recovery.json` |
