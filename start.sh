#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin" "$ROOT/data"
: "${TRPC_AGENT_POSTGRES_DSN:?TRPC_AGENT_POSTGRES_DSN is required}"
: "${TRPC_AGENT_REDIS_URL:?TRPC_AGENT_REDIS_URL is required}"
: "${DEEPSEEK_API_KEY:?DEEPSEEK_API_KEY is required by configs/example.yaml}"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

PID_FILE="$ROOT/data/trpc-service.pid"
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running: pid=$(cat "$PID_FILE")"
  exit 0
fi

CONFIG_PATH="${TRPC_AGENT_CONFIG:-$ROOT/configs/example.yaml}"
LISTEN_ADDRESS="${TRPC_AGENT_LISTEN:-127.0.0.1:8080}"
nohup "$ROOT/bin/trpc-service" --config "$CONFIG_PATH" --listen "$LISTEN_ADDRESS" >"$ROOT/data/trpc-service.log" 2>&1 &
echo $! >"$PID_FILE"
echo "started: pid=$(cat "$PID_FILE")"
