#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

TRPC_AGENT_MODEL_PROVIDER=mock "$ROOT/start.sh"
