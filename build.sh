#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin"
COMMANDS=(trpc-service trpc-migrate trpc-loadgen trpc-modelcheck trpc-embeddingcheck trpc-tracecheck trpc-wecomcheck trpc-wecomsample trpc-wecomsetup trpc-permissions)
for command_name in "${COMMANDS[@]}"; do
  go build -o "$ROOT/bin/$command_name" "./cmd/$command_name"
done
echo "built: ${COMMANDS[*]} (in $ROOT/bin; running processes unchanged)"
