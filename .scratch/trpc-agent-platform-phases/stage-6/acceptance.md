# Stage 6 Acceptance

Status: passed

## Delivered Scope

Stage 6 adds a separate Gateway/Worker process boundary, server-owned runtime
timeout policy, graceful drain, dependency health and recovery, immutable
Version dispatch, Tenant gray rollout and rollback, bounded capacity
estimation, development-only fault injection, and a reproducible Docker
Compose recovery matrix.

## Reproduction

```bash
gofmt -w cmd trpcservice
go test ./...
go test -race ./...
go vet ./...
cd frontend
npm run typecheck
npm test -- --run
npm run build
npm run test:e2e
cd ..
./build.sh
./scripts/stage6-compose-smoke.sh
./scripts/stage6-compose-recovery.sh
```

The Compose recovery command writes deterministic evidence to
`.scratch/stage6-compose-recovery.json`. The workflow uses only local
containers and deterministic fixtures; it does not require hosted models,
live IM credentials, or a Kubernetes cluster.

## Acceptance Matrix

| Requirement | Evidence |
| --- | --- |
| Gateway/Worker identity and immutable Version dispatch | `remote_worker_test.go`, `runtime.go`, `WorkerServer` |
| Worker stop/restart and stable public errors | `remote_worker_test.go`, Compose recovery evidence |
| Dependency outage, health, and bounded storage contexts | `stage6_recovery_test.go`, `data_http.go`, Compose recovery evidence |
| Runtime timeout, cancellation, and terminal-event uniqueness | `stage6_recovery_test.go`, `governance.go`, `chat.go` |
| Graceful drain and new-work rejection | `operations_test.go`, `operations.go`, Compose recovery evidence |
| Gray rollout, rollback preview/confirmation, and audit | `resources_http_test.go`, `resources_http.go`, Compose recovery evidence |
| IM retry, duplicate callback, and bounded provider recovery | `mock_delivery_test.go`, `chat_retry_test.go`, Compose recovery evidence |
| Bounded deterministic capacity estimation | `capacity.go`, `stage6_recovery_test.go`, Compose recovery evidence |
| Development/Compose-only fault injection | `operations_test.go`, `stage6_recovery_test.go`, Compose recovery evidence |
| Management Console operations workflows | `RuntimePage.test.tsx`, `DeploymentsPage.test.tsx`, Playwright suites |
| Kubernetes and production guidance | `docs/stage-6-operations.md` |

## Exclusions And Known Limitations

- `non-blocking-quality`: Kubernetes artifacts are documented guidance; a
  cluster is not required for local acceptance.
- `non-blocking-quality`: governance state remains an atomic local JSON
  snapshot rather than a distributed control-plane store.
- `non-blocking-quality`: capacity estimation is deterministic and bounded; it
  is not a production load test.
- `non-blocking-quality`: gray routing is deterministic by request ID and does
  not model user cohort affinity or an external feature-flag service.
- `non-blocking-quality`: Compose uses a fixed local Worker token; production
  must issue and rotate credentials through a secret manager.

No known limitation is classified as `blocking-acceptance`.

Final validation on 2026-09-04 passed every command in the reproduction
matrix, including eight desktop/mobile Playwright scenarios and both Compose
scripts.
