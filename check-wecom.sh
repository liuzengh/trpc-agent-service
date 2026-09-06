#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${TRPC_AGENT_ENV_FILE:-$REPO_ROOT/.env}"
cd "$REPO_ROOT"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "WeCom discovery requires a local env file" >&2
  exit 1
fi

# No credential is passed on the command line. Build only this small checker;
# do not start/stop the Agent, database, model service or IM subscriptions.
mkdir -p "$REPO_ROOT/bin"
go build -o "$REPO_ROOT/bin/trpc-wecomcheck" ./cmd/trpc-wecomcheck
exec env -u WECOM_MCP_URL "$REPO_ROOT/bin/trpc-wecomcheck" -env-file "$ENV_FILE" "$@"
