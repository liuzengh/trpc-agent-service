#!/usr/bin/env bash
set -euo pipefail

[[ $# == 0 ]] || { echo "usage: ./scripts/build.sh" >&2; exit 2; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

command -v npm >/dev/null || { echo "Node.js/npm are required to build the console; use Node 22.12+ or 24 LTS." >&2; exit 1; }
npm --prefix "$ROOT/trpcservice/web/console" ci --ignore-scripts --no-audit --no-fund
npm --prefix "$ROOT/trpcservice/web/console" run build

mkdir -p "$ROOT/bin"
COMMANDS=(trpc-service trpc-local trpc-migrate trpc-init trpc-permissions)
for command_name in "${COMMANDS[@]}"; do
  go build -o "$ROOT/bin/$command_name" "./cmd/$command_name"
done
echo "built: ${COMMANDS[*]} (in $ROOT/bin; running processes unchanged)"
