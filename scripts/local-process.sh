#!/usr/bin/env bash
# Shared by manual Agent scripts. This never sources a user .env as shell code.
local_agent_init() {
  LOCAL_AGENT_ROOT="$1"
  cd "$LOCAL_AGENT_ROOT"
  umask 077
  mkdir -p bin data
  command -v flock >/dev/null 2>&1 || { echo "manual process scripts require Linux flock" >&2; return 1; }
  exec 9>"$LOCAL_AGENT_ROOT/data/trpc-service.lock"
  flock -n 9 || { echo "another Agent start/stop is in progress" >&2; return 1; }
  go build -o "$LOCAL_AGENT_ROOT/bin/trpc-local" ./cmd/trpc-local
}

local_agent_already_running() {
  local running_status=0
  "$LOCAL_AGENT_ROOT/bin/trpc-local" running -root "$LOCAL_AGENT_ROOT" || running_status=$?
  case "$running_status" in
    0) return 0 ;;
    3) return 1 ;;
    *) exit "$running_status" ;;
  esac
}

local_agent_register() {
  local started_pid="$1"
  local env_path="$2"
  if ! "$LOCAL_AGENT_ROOT/bin/trpc-local" record-pid -root "$LOCAL_AGENT_ROOT" -pid "$started_pid" -timeout 2s; then
    echo "Agent startup failed or identity unavailable; inspect data/trpc-service.log locally" >&2
    return 1
  fi
  "$LOCAL_AGENT_ROOT/bin/trpc-local" wait-ready -root "$LOCAL_AGENT_ROOT" -env-file "$env_path" -timeout 20s
  echo "started Agent: pid=$started_pid"
  echo "log: $LOCAL_AGENT_ROOT/data/trpc-service.log"
}

# Wait only for dependencies already present in this workspace's Compose.
# No .env sourcing, container creation or migration is performed.
local_agent_wait_dependencies() {
  [[ -f "$LOCAL_AGENT_ROOT/compose.yaml" ]] || return 0
  command -v docker >/dev/null 2>&1 || return 0
  local dependency
  for dependency in postgres redis minio qdrant; do
    wait_for_compose_service "$dependency"
  done
}

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
