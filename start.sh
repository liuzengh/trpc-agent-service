#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

PID_FILE="$ROOT/data/trpc-service.pid"
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running: pid=$(cat "$PID_FILE")"
  exit 0
fi

# Security defaults are closed everywhere, including local dev: the mock
# channel stays off, and the Admin API requires TRPC_ADMIN_TOKEN. Local
# opt-ins are explicit, e.g.:
#   TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./start.sh

nohup "$ROOT/bin/trpc-service" serve >"$ROOT/data/trpc-service.log" 2>&1 &
echo $! >"$PID_FILE"
echo "started: pid=$(cat "$PID_FILE")"
