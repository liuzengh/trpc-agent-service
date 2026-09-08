#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
source "$ROOT/scripts/local-process.sh"
local_agent_init "$ROOT"
exec "$ROOT/bin/trpc-local" stop -root "$ROOT" -timeout 20s
