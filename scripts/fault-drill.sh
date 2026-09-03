#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${TRPC_AGENT_DRILL_CONFIRM:-}" != "yes" ]]; then
  echo "set TRPC_AGENT_DRILL_CONFIRM=yes to run the local Redis/PostgreSQL restart drill"
  exit 2
fi

TARGET_URL="${TRPC_AGENT_DRILL_URL:-http://127.0.0.1:8080/readyz}"

curl -fsS "$TARGET_URL"
docker compose stop redis
curl -sS -o /dev/null -w 'ready while redis stopped: %{http_code}\n' "$TARGET_URL" || true
docker compose start redis

docker compose stop postgres
curl -sS -o /dev/null -w 'ready while postgres stopped: %{http_code}\n' "$TARGET_URL" || true
docker compose start postgres

echo "dependencies restarted; verify queue recovery, pending jobs and audit logs"
