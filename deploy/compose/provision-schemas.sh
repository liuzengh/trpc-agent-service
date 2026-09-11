#!/bin/sh
# V1 database provisioning. Does not move/drop existing application data.
set -eu
: "${PGDATABASE:?set the shared database}"
: "${CONTROL_MIGRATOR_PASSWORD:?set control_migrator password}"
: "${CONTROL_RUNTIME_PASSWORD:?set control_runtime password}"
: "${GATEWAY_MIGRATOR_PASSWORD:?set gateway_migrator password}"
: "${GATEWAY_RUNTIME_PASSWORD:?set gateway_runtime password}"
: "${WORKER_MIGRATOR_PASSWORD:?set worker_migrator password}"
: "${WORKER_RUNTIME_PASSWORD:?set worker_runtime password}"
: "${SESSION_MIGRATOR_PASSWORD:?set session_migrator password}"
: "${SESSION_RUNTIME_PASSWORD:?set session_runtime password}"
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec psql --no-psqlrc --set=ON_ERROR_STOP=1 --file="$HERE/provision-schemas.sql"
