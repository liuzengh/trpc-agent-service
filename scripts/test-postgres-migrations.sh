#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
  printf '%s\n' 'TEST_DATABASE_URL must point to a disposable local or CI PostgreSQL database.' >&2
  exit 2
fi
case "$TEST_DATABASE_URL" in
postgres://* | postgresql://*) ;;
*)
  printf '%s\n' 'TEST_DATABASE_URL must use a PostgreSQL URL.' >&2
  exit 2
  ;;
esac
authority="${TEST_DATABASE_URL#*://}"
hostport="${authority#*@}"
host="${hostport%%[:/?]*}"
database="${hostport#*/}"
database="${database%%\?*}"
case "$host" in
127.0.0.1 | localhost | ::1) ;;
*)
  printf '%s\n' 'Refusing non-local TEST_DATABASE_URL.' >&2
  exit 2
  ;;
esac
case "$database" in
*test* | *Test* | *TEST*) ;;
*)
  printf '%s\n' 'Refusing a database name without a test marker.' >&2
  exit 2
  ;;
esac

printf '%s\n' 'Running PostgreSQL migration integration tests against the explicit local test database.'
TEST_DATABASE_URL="$TEST_DATABASE_URL" go test ./trpcservice/storage/postgres -count=1 -run PostgreSQL
