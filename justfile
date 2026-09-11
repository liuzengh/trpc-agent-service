set shell := ["zsh", "-cu"]

default:
    @just --list

# Go unit tests across every module.
test:
    go test ./...

# Reviewer demo: rebuilds the images and restarts the full platform plus Channel
# Lab, seeded with one tenant, one published Agent and one enabled bot binding.
# Existing volumes and private state are kept. Add --skip-build to reuse images.
demo *args:
    python3 scripts/compose-managed.py demo {{args}}

# Service state for the demo stack; also re-checks HTTP readiness.
demo-status *args:
    python3 scripts/compose-managed.py status --review {{args}}

# Stops the demo stack and keeps its private state and named volumes.
demo-stop *args:
    python3 scripts/compose-managed.py stop --review {{args}}

# Destroys the demo stack, its named volumes and its private state, then builds
# and seeds a fresh environment. Use this to discard all demo data.
demo-reset *args:
    python3 scripts/compose-managed.py reset {{args}}

# Full joint acceptance: real Control/Gateway/Worker processes, explicit model fixture.
test-joint:
    python3 scripts/test-worker-v1-joint.py --model-name gpt-4o --race --faults --contracts --recovery --durability --uncertainty --authorization --finality --manifest-rebuild

# Web unit tests.
test-web:
    cd web && npm test

# --- Everything below is a secondary entry point, hidden from `just --list`. ---

[private]
fmt:
    gofmt -w $(find services tests api platform -type f -name '*.go')

[private]
test-integration:
    bash scripts/test-control-integration.sh

# Creates and removes its own PostgreSQL 17 fixture; no production DSN is used.
[private]
test-database-v1:
    python3 scripts/test-v1-database-isolation.py

# Real storage/broker/SDK fixture gate; live model/Telegram acceptance is separate.
[private]
test-worker-v1:
    python3 scripts/test-worker-v1.py --race

[private]
openapi:
    go test ./api/... ./services/control-api/internal/agent/domain ./services/control-api/internal/runtimeprofile/domain ./services/control-api/internal/deployment/domain

[private]
test-race:
    go test -race ./services/control-api/internal/identity/... ./services/control-api/internal/tenant/... ./services/control-api/internal/admin/... ./services/control-api/internal/agent/... ./services/control-api/internal/runtimeprofile/... ./services/control-api/internal/deployment/... ./services/control-api/internal/bootstrap

[private]
vet:
    go vet ./services/... ./api/... ./platform/...

[private]
vuln:
    go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./services/control-api/...

[private]
build:
    mkdir -p bin
    CGO_ENABLED=0 go build -trimpath -o bin/control-api ./services/control-api/cmd/control-api

[private]
run:
    go run ./services/control-api/cmd/control-api

# Managed local full stack; private state is created on first run of managed-up.
# Flags pass through directly, for example: just managed-up --skip-build
[private]
managed-up *args:
    python3 scripts/compose-managed.py up {{args}}

# Service state for the managed project; also re-checks HTTP readiness.
[private]
managed-status *args:
    python3 scripts/compose-managed.py status {{args}}

# Stops services and keeps containers, private state and named volumes.
[private]
managed-stop *args:
    python3 scripts/compose-managed.py stop {{args}}

# Validates the rendered Compose configuration without printing secrets.
[private]
managed-config *args:
    python3 scripts/compose-managed.py config {{args}}

# Restarts PostgreSQL, Redis, Qdrant and MinIO on their existing volumes.
[private]
managed-restart-backends *args:
    python3 scripts/compose-managed.py restart-backends {{args}}

# Explicit tenant allowlist; repins the contract and recreates Control + Worker.
[private]
managed-grant tenant_id *args:
    python3 scripts/compose-managed.py grant --tenant-id {{tenant_id}} {{args}}

# Base-only Compose path; Control, Gateway, PostgreSQL and NATS only.
[private]
compose-config:
    docker compose -f deploy/compose/compose.yaml -f deploy/compose/compose.local.yaml config --quiet

[private]
compose-up: compose-config
    docker compose -f deploy/compose/compose.yaml -f deploy/compose/compose.local.yaml up --detach --build --wait

[private]
compose-down:
    docker compose -f deploy/compose/compose.yaml -f deploy/compose/compose.local.yaml down

[private]
web-install:
    cd web && npm install

[private]
web-test:
    cd web && npm test

[private]
web-lint:
    cd web && npm run lint

[private]
web-build:
    cd web && npm run build

[private]
web-dev:
    cd web && CONTROL_API_BASE="${CONTROL_API_BASE:-http://127.0.0.1:18081}" npm run dev -- --hostname "${WEB_HOST:-127.0.0.1}" --port "${WEB_PORT:-13001}"

[private]
web-start: web-build
    cd web && CONTROL_API_BASE="${CONTROL_API_BASE:-http://127.0.0.1:18081}" npm start -- --hostname "${WEB_HOST:-127.0.0.1}" --port "${WEB_PORT:-13001}"

# Gateway ingress vertical slice; requires a dedicated integration broker.
[private]
gateway-build:
    mkdir -p bin
    CGO_ENABLED=0 go build -trimpath -o bin/channel-gateway ./services/channel-gateway/cmd/channel-gateway

[private]
gateway-test:
    go test ./services/channel-gateway/... ./api/events/... ./gen/events/... ./platform/im/wecom/...

[private]
gateway-integration:
    test -n "${GATEWAY_TEST_DATABASE_URL:-}" && test -n "${GATEWAY_TEST_NATS_URL:-}" && test "${GATEWAY_TEST_ALLOW_NATS_RESET:-}" = 1
    go test -race -count=1 ./services/channel-gateway/...

[private]
nats-config:
    go run ./services/channel-gateway/cmd/channel-gateway nats-config deploy/nats/permissions.yaml > deploy/nats/server.conf

[private]
gateway-reconcile:
    go run ./services/channel-gateway/cmd/channel-gateway reconcile

# Public protocol library: local WebSocket fixtures, no PG/NATS/Bot credentials.
[private]
wecom-test:
    go test -race -count=1 ./platform/im/wecom/...
