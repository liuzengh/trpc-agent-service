#!/bin/sh
set -eu
printf '%s\n' 'V1 uses provision-schemas.sh for the shared database. Legacy database migration must be explicit.' >&2
exit 2
