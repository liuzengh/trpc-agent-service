#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

PID_FILE="$ROOT/data/trpc-service.pid"
LOG_FILE="$ROOT/data/trpc-service.log"
ADMIN_KEY_FILE="$ROOT/data/admin-api-key"
ADDR="${TRPC_SERVICE_ADDR:-127.0.0.1:8080}"
HEALTH_URL="${TRPC_SERVICE_HEALTH_URL:-http://${ADDR}/healthz}"

# The Admin API authenticates, and its key has no published default the way the
# chat demo key does: a control plane that boots with a credential printed in the
# repository is a control plane with no credential. So when the demo profile is
# in use and the environment supplies nothing, one is generated here, once, and
# kept.
#
# Three rules make this safe to run repeatedly:
#
#   - An environment that already provides a key wins, and no file is written.
#     The operator's key is theirs to manage; writing a copy of it into the
#     working tree would be this script deciding where their secret lives.
#   - A custom security manifest opts out entirely. Its credentials come from
#     wherever it says, and generating a key nothing reads would be noise that
#     looks like configuration.
#   - The file is created with a 0600 umask and its permissions are re-asserted,
#     so neither a fresh create nor a file left by an earlier, laxer run is
#     group- or world-readable.
#
# The key itself is never printed — not to the terminal, not to the log. The path
# is, because an operator has to be able to find it.
if [[ -z "${TRPC_SERVICE_ADMIN_API_KEY:-}" && -z "${TRPC_SERVICE_SECURITY_CONFIG_FILE:-}" ]]; then
  if [[ ! -s "$ADMIN_KEY_FILE" ]]; then
    # 48 characters drawn from the kernel CSPRNG, comfortably past the
    # 32-character minimum the admin authenticator enforces. Every stage of the
    # pipeline reads its input to the end, so none of them dies of SIGPIPE under
    # `set -o pipefail` — which would otherwise turn a working generator into an
    # intermittent start failure.
    ADMIN_KEY="$(head -c 4096 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9_-' | cut -c1-48)"
    if (( ${#ADMIN_KEY} < 32 )); then
      echo "start failed: could not generate an admin API key" >&2
      exit 1
    fi
    (
      umask 077
      printf '%s' "$ADMIN_KEY" >"$ADMIN_KEY_FILE"
    )
    unset ADMIN_KEY
    echo "generated a new admin API key: $ADMIN_KEY_FILE"
  fi
  chmod 600 "$ADMIN_KEY_FILE"
  TRPC_SERVICE_ADMIN_API_KEY="$(cat "$ADMIN_KEY_FILE")"
  export TRPC_SERVICE_ADMIN_API_KEY
  echo "admin API key: $ADMIN_KEY_FILE"
fi

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE")"
  if [[ "$EXISTING_PID" =~ ^[0-9]+$ ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "already running: pid=$EXISTING_PID"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

nohup "$ROOT/bin/trpc-service" -addr "$ADDR" >"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" >"$PID_FILE"

for _ in {1..50}; do
  if ! kill -0 "$PID" 2>/dev/null; then
    wait "$PID" || EXIT_CODE=$?
    rm -f "$PID_FILE"
    echo "start failed: process exited with code ${EXIT_CODE:-0}" >&2
    tail -n 20 "$LOG_FILE" >&2 || true
    exit 1
  fi
  if curl --fail --silent --max-time 1 "$HEALTH_URL" >/dev/null; then
    echo "started: pid=$PID addr=$ADDR"
    exit 0
  fi
  sleep 0.1
done

kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "start failed: readiness check timed out: $HEALTH_URL" >&2
tail -n 20 "$LOG_FILE" >&2 || true
exit 1
