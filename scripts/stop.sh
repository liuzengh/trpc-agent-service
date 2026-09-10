#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/data/trpc-service.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "not running"
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  echo "stopped: pid=$PID"
else
  echo "stale pid file: $PID"
fi
rm -f "$PID_FILE"
