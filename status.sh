#!/usr/bin/env bash
set -euo pipefail
LOCAL_STATUS_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$LOCAL_STATUS_ROOT"
mkdir -p bin
go build -o bin/trpc-local ./cmd/trpc-local
exec ./bin/trpc-local status -root "$LOCAL_STATUS_ROOT" \
  -env-file "${TRPC_AGENT_ENV_FILE:-$LOCAL_STATUS_ROOT/.env}" "$@"
