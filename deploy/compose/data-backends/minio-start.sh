#!/bin/sh
set -eu
# No xtrace: credential values are never command arguments.
MINIO_ROOT_USER=$(cat "${MINIO_ROOT_USER_FILE:-/run/secrets/minio_user}")
MINIO_ROOT_PASSWORD=$(cat "${MINIO_ROOT_PASSWORD_FILE:-/run/secrets/minio_password}")
[ -n "$MINIO_ROOT_USER" ] && [ -n "$MINIO_ROOT_PASSWORD" ] || exit 1
export MINIO_ROOT_USER MINIO_ROOT_PASSWORD
exec minio server /data --console-address :9001
