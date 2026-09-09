#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v node >/dev/null 2>&1; then
  echo "error: Node.js is required to build frontend/" >&2
  exit 1
fi
if ! command -v npm >/dev/null 2>&1; then
  echo "error: npm is required to build frontend/" >&2
  exit 1
fi

if [[ ! -d "$ROOT/frontend/node_modules" ]]; then
  echo "frontend dependencies are missing; installing from package-lock.json"
  npm --prefix "$ROOT/frontend" ci
fi

npm --prefix "$ROOT/frontend" run build
mkdir -p "$ROOT/bin"
go build -o "$ROOT/bin/trpc-service" ./cmd/trpc-service
go build -o "$ROOT/bin/control-migrate" ./cmd/control-migrate
echo "built frontend, $ROOT/bin/trpc-service, and $ROOT/bin/control-migrate"
