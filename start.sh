#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

source "$ROOT/scripts/local-process.sh"
local_agent_init "$ROOT"
if local_agent_already_running; then exit 0; fi

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

ENV_FILE="${TRPC_AGENT_ENV_FILE:-$ROOT/.env}"
local_agent_wait_dependencies
nohup "$ROOT/bin/trpc-service" -env-file "$ENV_FILE" >>"$ROOT/data/trpc-service.log" 2>&1 9>&- &
local_agent_register "$!" "$ENV_FILE"
