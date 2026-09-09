#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/.env.local" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env.local"
  set +a
fi

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

"$ROOT/bin/control-migrate"

PID_FILE="$ROOT/data/trpc-service.pid"
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running: pid=$(cat "$PID_FILE")"
  exit 0
fi

nohup "$ROOT/bin/trpc-service" >"$ROOT/data/trpc-service.log" 2>&1 &
echo $! >"$PID_FILE"
echo "started: pid=$(cat "$PID_FILE")"
