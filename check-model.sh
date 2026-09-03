#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${TRPC_AGENT_ENV_FILE:-$ROOT/.env}"
cd "$ROOT"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "model check requires an env file: $ENV_FILE" >&2
  exit 1
fi

mkdir -p "$ROOT/bin"
if [[ ! -x "$ROOT/bin/trpc-modelcheck" ]]; then
  go build -o "$ROOT/bin/trpc-modelcheck" "$ROOT/cmd/trpc-modelcheck"
fi

env \
  -u TRPC_AGENT_MODEL_PROVIDER \
  -u TRPC_AGENT_MODEL_NAME \
  -u OPENAI_API_KEY \
  -u OPENAI_BASE_URL \
  -u TRPC_AGENT_MODEL_STREAM \
  TRPC_AGENT_MODEL_PROVIDER=openai \
  "$ROOT/bin/trpc-modelcheck" -env-file "$ENV_FILE" "$@"
