#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

command -v go >/dev/null || { echo "required tool missing: go" >&2; exit 127; }
command -v docker >/dev/null || { echo "required tool missing: docker" >&2; exit 127; }
command -v curl >/dev/null || { echo "required tool missing: curl" >&2; exit 127; }

# Keep validation predictable on developer machines. CI may override
# GOMAXPROCS deliberately, but package scheduling remains serial so a full race
# gate cannot exhaust a workstation merely because it has many logical CPUs.
export GOMAXPROCS="${GOMAXPROCS:-1}"

echo "[1/10] production Go security baseline"
bash ./scripts/require_secure_go.sh

echo "[2/10] module integrity"
go mod verify

echo "[3/10] formatting"
mapfile -d '' go_files < <(find cmd pkg migrations test -name '*.go' -type f -print0)
unformatted="$(gofmt -l "${go_files[@]}")"
test -z "$unformatted" || { echo "$unformatted"; echo "Go files require gofmt" >&2; exit 1; }

echo "[4/10] build"
go build -buildvcs=false -p 1 ./cmd/...

echo "[5/10] vet"
go vet -p 1 ./...

echo "[6/10] unit and package tests"
go test -count=1 -p 1 ./...

echo "[7/10] race tests"
go test -race -count=1 -p 1 ./...

echo "[8/10] deployment syntax and image builds"
validation_password="validation-only-password-not-for-deployment"
POSTGRES_PASSWORD="$validation_password" \
MASTER_KEY="validation-only-master-key-32-bytes-minimum" \
SERVICE_AUTH_SECRET="validation-only-service-secret-32-bytes" \
ADMIN_API_TOKEN="validation-only-admin-token-32-bytes" \
GRAFANA_PASSWORD="validation-only-grafana-password" \
docker compose -f deploy/docker-compose.yml config >/dev/null
# Independent builds can otherwise fill the host disk with concurrent Go
# compiler workspaces. Check available space before every serial target.
minimum_build_free_kib="${MIN_BUILD_FREE_KIB:-8388608}"
[[ "$minimum_build_free_kib" =~ ^[1-9][0-9]*$ ]] || { echo 'MIN_BUILD_FREE_KIB must be positive' >&2; exit 1; }
for service in migrate data-migrate gateway worker summary-worker consumer delivery admin telegram-poller wecom-bot; do
  available_kib="$(df -Pk "$repo_dir" | awk 'END {print $4}')"
  if [[ ! "$available_kib" =~ ^[0-9]+$ ]] || (( available_kib < minimum_build_free_kib )); then
    echo "Insufficient disk space for $service build: ${available_kib} KiB available; ${minimum_build_free_kib} required" >&2
    exit 1
  fi
  if [[ "$service" == telegram-poller || "$service" == wecom-bot ]]; then
    docker build -f "deploy/Dockerfile.$service" .
    continue
  fi
  POSTGRES_PASSWORD="$validation_password" \
  MASTER_KEY="validation-only-master-key-32-bytes-minimum" \
  SERVICE_AUTH_SECRET="validation-only-service-secret-32-bytes" \
  ADMIN_API_TOKEN="validation-only-admin-token-32-bytes" \
  GRAFANA_PASSWORD="validation-only-grafana-password" \
    docker compose -f deploy/docker-compose.yml build "$service"
done
MSYS_NO_PATHCONV=1 docker run --rm --entrypoint /bin/promtool \
  -v "$repo_dir/deploy:/work:ro" \
  prom/prometheus:v2.54.1 \
  check rules /work/prometheus-rules.yml

echo "[9/10] real PostgreSQL, Redis, Qdrant and MinIO integration tests"
postgres_container="trpc-agent-postgres-validation-$$"
redis_container="trpc-agent-redis-validation-$$"
qdrant_container="trpc-agent-qdrant-validation-$$"
minio_container="trpc-agent-minio-validation-$$"
validation_minio_user="validation-minio-user"
validation_minio_password="validation-minio-password-not-for-deployment"
cleanup() {
  docker rm -f "$postgres_container" >/dev/null 2>&1 || true
  docker rm -f "$redis_container" >/dev/null 2>&1 || true
  docker rm -f "$qdrant_container" >/dev/null 2>&1 || true
  docker rm -f "$minio_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker run --rm -d --name "$postgres_container" -e POSTGRES_DB=trpc_agent \
  -e POSTGRES_USER=agent -e POSTGRES_PASSWORD="$validation_password" -P postgres:15.8-alpine >/dev/null
docker run --rm -d --name "$redis_container" -P redis:7.4-alpine \
  redis-server --appendonly yes >/dev/null
docker run --rm -d --name "$qdrant_container" -P \
  qdrant/qdrant:v1.16.3@sha256:0425e3e03e7fd9b3dc95c4214546afe19de2eb2e28ca621441a56663ac6e1f46 >/dev/null
docker run --rm -d --name "$minio_container" -P \
  -e MINIO_ROOT_USER="$validation_minio_user" \
  -e MINIO_ROOT_PASSWORD="$validation_minio_password" \
  minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e \
  server /data --address :9000 >/dev/null
qdrant_http_port="$(docker port "$qdrant_container" 6333/tcp | head -n 1 | sed 's/.*://')"
qdrant_grpc_port="$(docker port "$qdrant_container" 6334/tcp | head -n 1 | sed 's/.*://')"
minio_port="$(docker port "$minio_container" 9000/tcp | head -n 1 | sed 's/.*://')"
for _ in $(seq 1 40); do
  if docker exec "$postgres_container" pg_isready -U agent -d trpc_agent >/dev/null 2>&1 && \
     test "$(docker exec "$redis_container" redis-cli ping 2>/dev/null)" = "PONG" && \
     curl -fsS "http://127.0.0.1:${qdrant_http_port}/readyz" >/dev/null && \
     curl -fsS "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
docker exec "$postgres_container" pg_isready -U agent -d trpc_agent >/dev/null
test "$(docker exec "$redis_container" redis-cli ping)" = "PONG"
curl -fsS "http://127.0.0.1:${qdrant_http_port}/readyz" >/dev/null
curl -fsS "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null
postgres_port="$(docker port "$postgres_container" 5432/tcp | head -n 1 | sed 's/.*://')"
redis_port="$(docker port "$redis_container" 6379/tcp | head -n 1 | sed 's/.*://')"
TEST_DATABASE_URL="postgres://agent:${validation_password}@127.0.0.1:${postgres_port}/trpc_agent?sslmode=disable" \
TEST_REDIS_URL="redis://127.0.0.1:${redis_port}/0" \
TEST_QDRANT_HOST="127.0.0.1" \
TEST_QDRANT_GRPC_PORT="$qdrant_grpc_port" \
TEST_MINIO_ENDPOINT="127.0.0.1:${minio_port}" \
TEST_MINIO_ACCESS_KEY="$validation_minio_user" \
TEST_MINIO_SECRET_KEY="$validation_minio_password" \
  go test -tags=integration -count=1 -p 1 ./test/integration ./cmd/queue-bench

echo "[10/10] source and migration static gates"
./scripts/static_verify.sh

echo "repository validation passed: secure toolchain, build, vet, unit, race, image build, real PostgreSQL, Redis, Qdrant and MinIO integration all ran"
