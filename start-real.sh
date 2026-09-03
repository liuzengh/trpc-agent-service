#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

ENV_FILE="${TRPC_AGENT_ENV_FILE:-$ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "real-model startup requires an env file: $ENV_FILE" >&2
  exit 1
fi

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

PID_FILE="$ROOT/data/trpc-service.pid"
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running: pid=$(cat "$PID_FILE")"
  exit 0
fi

wait_for_compose_service() {
  local service_name="$1"
  local container_id
  local container_status

  container_id="$(docker compose ps -q "$service_name" 2>/dev/null || true)"
  if [[ -z "$container_id" ]]; then
    return 0
  fi
  for _ in $(seq 1 60); do
    container_status="$(docker inspect --format \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "$container_id" 2>/dev/null || true)"
    case "$container_status" in
      healthy|running)
        echo "dependency ready: $service_name"
        return 0
        ;;
      exited|dead)
        echo "dependency failed before startup: $service_name status=$container_status" >&2
        return 1
        ;;
    esac
    sleep 1
  done
  echo "timed out waiting for dependency: $service_name status=$container_status" >&2
  return 1
}

# Local Compose containers may report running before PostgreSQL recovery or
# Redis RDB/AOF loading has completed. Their health checks are the reliable
# boundary for starting Session, Queue and Control Plane clients.
if command -v docker >/dev/null 2>&1; then
  wait_for_compose_service postgres
  wait_for_compose_service redis
fi

nohup env \
  -u TRPC_AGENT_MODEL_PROVIDER \
  -u TRPC_AGENT_MODEL_NAME \
  -u OPENAI_API_KEY \
  -u OPENAI_BASE_URL \
  -u TRPC_AGENT_MODEL_STREAM \
  TRPC_AGENT_MODEL_PROVIDER=openai \
  "$ROOT/bin/trpc-service" -env-file "$ENV_FILE" \
  >"$ROOT/data/trpc-service.log" 2>&1 &
pid=$!
echo "$pid" >"$PID_FILE"

# Give late startup failures (for example port conflicts after dependency
# initialization) enough time to surface before reporting success.
sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
  rm -f "$PID_FILE"
  echo "real-model service failed to start; latest log:" >&2
  tail -n 20 "$ROOT/data/trpc-service.log" >&2 || true
  exit 1
fi

echo "started real-model service: pid=$pid"
echo "log: $ROOT/data/trpc-service.log"
