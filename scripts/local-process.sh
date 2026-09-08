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
