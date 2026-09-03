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

sleep 0.2
if ! kill -0 "$pid" 2>/dev/null; then
  rm -f "$PID_FILE"
  echo "real-model service failed to start; latest log:" >&2
  tail -n 20 "$ROOT/data/trpc-service.log" >&2 || true
  exit 1
fi

echo "started real-model service: pid=$pid"
echo "log: $ROOT/data/trpc-service.log"
