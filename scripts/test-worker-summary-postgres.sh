#!/bin/sh
# Own a disposable PostgreSQL container; never reuse a running deployment DB.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CID=$(docker run --rm -d -e POSTGRES_PASSWORD=summary_fixture_admin -e POSTGRES_DB=summary_fixture -p 127.0.0.1::5432 postgres:17.6-alpine)
trap 'docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT HUP INT TERM
n=0
until docker exec "$CID" pg_isready -U postgres -d summary_fixture >/dev/null 2>&1; do
 n=$((n+1)); test "$n" -lt 60 || exit 1; sleep 1
done
PORT=$(docker port "$CID" 5432/tcp | sed -n 's/^127\.0\.0\.1://p')
test -n "$PORT"
docker exec -i "$CID" psql -v ON_ERROR_STOP=1 -U postgres -d summary_fixture <<'SQL'
CREATE ROLE session_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD 'summary_fixture_migrator';
CREATE ROLE session_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD 'summary_fixture_runtime';
REVOKE ALL ON DATABASE summary_fixture FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE summary_fixture TO session_migrator,session_runtime;
CREATE SCHEMA runtime_session AUTHORIZATION session_migrator;
REVOKE ALL ON SCHEMA runtime_session FROM PUBLIC;
GRANT USAGE ON SCHEMA runtime_session TO session_runtime;
ALTER ROLE session_migrator IN DATABASE summary_fixture SET search_path TO runtime_session,pg_temp;
ALTER ROLE session_runtime IN DATABASE summary_fixture SET search_path TO runtime_session,pg_temp;
SQL
cd "$ROOT"
WORKER_SUMMARY_TEST_MIGRATION_URL="postgres://session_migrator:summary_fixture_migrator@127.0.0.1:$PORT/summary_fixture?sslmode=disable" \
WORKER_SUMMARY_TEST_RUNTIME_URL="postgres://session_runtime:summary_fixture_runtime@127.0.0.1:$PORT/summary_fixture?sslmode=disable" \
go test -race ./services/agent-worker/internal/execution/adapter/outbound/trpcagent \
 -run '^TestSummaryUsesSamePostgresSessionCandidate$' -count=1 -v
docker rm -f "$CID" >/dev/null
if docker inspect "$CID" >/dev/null 2>&1; then exit 1; fi
printf 'DISPOSABLE_POSTGRES_CLEANUP=PASS\n'
