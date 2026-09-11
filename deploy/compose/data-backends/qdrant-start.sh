#!/bin/sh
set -eu
QDRANT__SERVICE__API_KEY=$(cat "${QDRANT_API_KEY_FILE:-/run/secrets/qdrant_api_key}")
[ -n "$QDRANT__SERVICE__API_KEY" ] || exit 1
export QDRANT__SERVICE__API_KEY
cd /qdrant
exec ./entrypoint.sh
