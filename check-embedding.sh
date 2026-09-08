#!/usr/bin/env bash
set -euo pipefail

EMBEDDING_CHECK_ROOT="$(cd "$(dirname "$0")" && pwd)"
EMBEDDING_CHECK_ENV="${TRPC_AGENT_ENV_FILE:-$EMBEDDING_CHECK_ROOT/.env}"
cd "$EMBEDDING_CHECK_ROOT"

if [[ ! -f "$EMBEDDING_CHECK_ENV" ]]; then
  echo "embedding check requires an env file" >&2
  exit 1
fi

# Only builds/runs this diagnostic. Does not restart the Agent or dependencies.
mkdir -p "$EMBEDDING_CHECK_ROOT/bin"
go build -o "$EMBEDDING_CHECK_ROOT/bin/trpc-embeddingcheck" ./cmd/trpc-embeddingcheck

# Match check-model.sh: the selected private file owns this check's settings,
# without reusing chat model keys, model IDs or URLs.
env \
  -u TRPC_AGENT_EMBEDDING_MODEL \
  -u TRPC_AGENT_EMBEDDING_BASE_URL \
  -u TRPC_AGENT_EMBEDDING_API_KEY \
  -u TRPC_AGENT_EMBEDDING_DIMENSIONS \
  "$EMBEDDING_CHECK_ROOT/bin/trpc-embeddingcheck" -env-file "$EMBEDDING_CHECK_ENV" "$@"
