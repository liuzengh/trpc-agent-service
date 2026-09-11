#!/bin/sh
# Formal accepted-head Summary gate. Owns one temporary PG; no existing DB input.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CID=$(docker run --rm -d -e POSTGRES_USER=platform_admin -e POSTGRES_DB=agent_platform \
 -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 \
 -v "$ROOT/deploy/compose:/provision:ro" postgres:17.6-alpine)
cleanup() {
 docker rm -f "$CID" >/dev/null 2>&1 || true
 if docker inspect "$CID" >/dev/null 2>&1; then
  printf 'SUMMARY_ACCEPTED_HEAD_PG_CLEANUP=FAIL\n' >&2
  return 1
 fi
 printf 'SUMMARY_ACCEPTED_HEAD_PG_CLEANUP=PASS\n'
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' HUP TERM
n=0
# Use TCP and an actual database query: the image bootstrap socket server can
# report ready before POSTGRES_DB exists and before the final server starts.
until docker exec "$CID" psql -h 127.0.0.1 -U platform_admin -d agent_platform \
 -v ON_ERROR_STOP=1 -Atqc 'SELECT 1' >/dev/null 2>&1; do
 n=$((n+1)); test "$n" -lt 60 || exit 1; sleep 1
done
PORT=$(docker port "$CID" 5432/tcp | sed -n 's/^127\.0\.0\.1://p')
test -n "$PORT"
# Reuse the exact deployment role/schema provisioner. Trust authentication is
# confined to this disposable loopback fixture; runtime still uses ordinary roles.
for name in CONTROL_MIGRATOR_PASSWORD CONTROL_RUNTIME_PASSWORD GATEWAY_MIGRATOR_PASSWORD GATEWAY_RUNTIME_PASSWORD WORKER_MIGRATOR_PASSWORD WORKER_RUNTIME_PASSWORD SESSION_MIGRATOR_PASSWORD SESSION_RUNTIME_PASSWORD; do
 export "$name=summary-accepted-fixture-only"
done
docker exec -e PGUSER=platform_admin -e PGDATABASE=agent_platform \
 -e CONTROL_MIGRATOR_PASSWORD -e CONTROL_RUNTIME_PASSWORD \
 -e GATEWAY_MIGRATOR_PASSWORD -e GATEWAY_RUNTIME_PASSWORD \
 -e WORKER_MIGRATOR_PASSWORD -e WORKER_RUNTIME_PASSWORD \
 -e SESSION_MIGRATOR_PASSWORD -e SESSION_RUNTIME_PASSWORD \
 "$CID" sh /provision/provision-schemas.sh
cd "$ROOT"
export WORKER_TEST_MIGRATION_URL="postgres://worker_migrator:fixture-only@127.0.0.1:$PORT/agent_platform?sslmode=disable"
export WORKER_TEST_RUNTIME_URL="postgres://worker_runtime:fixture-only@127.0.0.1:$PORT/agent_platform?sslmode=disable"
export WORKER_SUMMARY_TEST_MIGRATION_URL="postgres://session_migrator:fixture-only@127.0.0.1:$PORT/agent_platform?sslmode=disable"
export WORKER_SUMMARY_TEST_RUNTIME_URL="postgres://session_runtime:fixture-only@127.0.0.1:$PORT/agent_platform?sslmode=disable"
# JSON validation requires the main test and every acceptance branch to run.
LOG=$(mktemp "${TMPDIR:-/tmp}/summary-accepted-head.XXXXXX")
set +e
go test -race ./services/agent-worker/internal/execution/adapter/outbound/trpcagent \
 -run '^TestSummaryAcceptedHeadPostgresTransaction$' -count=1 -json > "$LOG" 2>&1
RC=$?
set -e
cat "$LOG"
if test "$RC" -ne 0; then rm -f "$LOG"; exit "$RC"; fi
PYTHONDONTWRITEBYTECODE=1 python3 - "$LOG" <<'PY'
import json
from pathlib import Path
import sys
entries=[json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines() if line.startswith('{')]
assert entries and not any(row.get('Action')=='skip' for row in entries), 'required Summary gate skipped'
name='TestSummaryAcceptedHeadPostgresTransaction'
expected={name}|{name+'/'+case for case in ('next_attempt_load_and_sdk_consumption','summary_failure_has_no_candidate','failed_completion','rejected_parent','stale_lease')}
passed={row.get('Test') for row in entries if row.get('Action')=='pass'}
assert expected <= passed, 'missing actual Summary acceptance branch'
print('SUMMARY_ACCEPTED_HEAD_GATE=PASS tests=1 subtests=5 zero_skips=true')
PY
rm -f "$LOG"
