#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${TRPC_AGENT_REDIS_IMAGE:-redis:7-alpine}"
CONTAINER="trpc-agent-service-redis-$$"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

diagnose() {
  local line="$1"
  echo "Redis integration test failed near line ${line}" >&2
  docker logs --tail 80 "$CONTAINER" >&2 || true
}

trap cleanup EXIT
trap 'diagnose "$LINENO"' ERR

docker run -d --name "$CONTAINER" -p 127.0.0.1::6379 "$IMAGE" >/dev/null

ready=false
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" redis-cli ping 2>/dev/null | grep -qx PONG; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "Redis did not become ready within 60 seconds" >&2
  exit 1
fi

redis_port="$(docker port "$CONTAINER" 6379/tcp | sed 's/.*://')"
(
  cd "$ROOT"
  TRPC_AGENT_REDIS_TEST_URL="redis://127.0.0.1:${redis_port}/0" \
    go test ./trpcservice/cluster -run 'TestRedis(StreamDistributesWorkAcrossNodes|ProducerOnlyGatewayQueuesUntilWorkerStarts)' -count=1
  TRPC_AGENT_REDIS_TEST_URL="redis://127.0.0.1:${redis_port}/0" \
    go test ./trpcservice/worker -run TestRedisRunLimiterCoordinatesNodesAndExpiresPermits -count=1
  TRPC_AGENT_REDIS_TEST_URL="redis://127.0.0.1:${redis_port}/0" \
    go test ./trpcservice/storage -run TestRedisSessionBackendIsSharedAndNamespaceIsolated -count=1
)

echo "Redis role-split queue, tenant quota, and shared Session integration paths passed"
