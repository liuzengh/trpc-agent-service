#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# On Windows the MSYS path (/d/...) is not understood by the native go tool and
# the produced binary carries a .exe suffix.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    ROOT="$(cd "$ROOT" && pwd -W)"
    BIN_SUFFIX=".exe"
    ;;
  *) BIN_SUFFIX="" ;;
esac

mkdir -p "$ROOT/bin" "$ROOT/data"
SERVICE_BIN="$ROOT/bin/trpc-service$BIN_SUFFIX"
PID_FILE="$ROOT/data/trpc-service.pid"

running_pid=""
if [[ -f "$PID_FILE" ]]; then
  candidate_pid="$(cat "$PID_FILE")"
  if [[ -n "$candidate_pid" ]] && kill -0 "$candidate_pid" 2>/dev/null; then
    running_pid="$candidate_pid"
  fi
fi

needs_build=false
if [[ ! -x "$SERVICE_BIN" ]]; then
  needs_build=true
elif find \
    "$ROOT/cmd" "$ROOT/trpcservice" "$ROOT/migrations" "$ROOT/webui/src" "$ROOT/webui/public" \
    "$ROOT/go.mod" "$ROOT/go.sum" "$ROOT/webui/index.html" "$ROOT/webui/package.json" "$ROOT/webui/package-lock.json" \
    -type f -newer "$SERVICE_BIN" -print -quit 2>/dev/null | grep -q .; then
  needs_build=true
fi
if [[ "$needs_build" == true ]]; then
  "$ROOT/scripts/build.sh"
fi

if [[ -n "$running_pid" ]]; then
  if [[ "$needs_build" == false ]]; then
    echo "already running: pid=$running_pid"
    exit 0
  fi
  kill "$running_pid"
  for _ in {1..50}; do
    if ! kill -0 "$running_pid" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  if kill -0 "$running_pid" 2>/dev/null; then
    echo "failed to stop stale service after rebuild: pid=$running_pid" >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi

ENV_FILE="$ROOT/data/platform.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "warning: $ENV_FILE not found, running with current environment" >&2
fi

nohup "$ROOT/bin/trpc-service" serve >"$ROOT/data/trpc-service.log" 2>&1 &
echo $! >"$PID_FILE"
echo "started: pid=$(cat "$PID_FILE")"
