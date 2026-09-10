#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

demo=false
env_file=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --demo)
      demo=true
      ;;
    --help|-h)
      echo "usage: $0 [--demo] [env-file]"
      echo "  --demo  provision the offline graph and verify the first /v1/chat response"
      exit 0
      ;;
    -* )
      echo "unknown option: $1" >&2
      echo "usage: $0 [--demo] [env-file]" >&2
      exit 2
      ;;
    * )
      if [[ -n "$env_file" ]]; then
        echo "usage: $0 [--demo] [env-file]" >&2
        exit 2
      fi
      env_file="$1"
      ;;
  esac
  shift
done

if [[ -z "$env_file" ]]; then
  if [[ -f deploy/service.env ]]; then
    env_file=deploy/service.env
  else
    env_file=deploy/example.env
  fi
fi
if [[ ! -f "$env_file" ]]; then
  echo "environment file not found: $env_file" >&2
  exit 1
fi

compose=(docker compose --env-file "$env_file" -f deploy/docker-compose.yml)
if [[ "$demo" == true ]]; then
  # Keep the one-shot bootstrap separate from the service process so the
  # service only starts after the generated Tenant/App IDs are known.
  "${compose[@]}" build service
  "${compose[@]}" up -d --wait postgres
  demo_output="$("${compose[@]}" run --rm --no-deps service demo --confirm)"
  tenant_id="$(printf '%s\n' "$demo_output" | sed -n "s/^export TRPC_TENANT_ID='\([^']*\)'$/\1/p")"
  app_id="$(printf '%s\n' "$demo_output" | sed -n "s/^export TRPC_APP_ID='\([^']*\)'$/\1/p")"
  if [[ ! "$tenant_id" =~ ^t_[0-7][0-9A-HJKMNP-TV-Z]{25}$ || ! "$app_id" =~ ^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$ ]]; then
    echo "demo bootstrap did not return valid Tenant/App IDs" >&2
    exit 1
  fi
  # The CLI output is deliberately limited to IDs and non-secret demo flags.
  printf '%s\n' "$demo_output"
  export TRPC_DEMO_MODE=true
  export TRPC_MODEL_PROVIDER=fake
  export TRPC_MODEL_NAMES=deterministic
  export TRPC_SESSION_BACKEND=inmemory
  export TRPC_TENANT_ID="$tenant_id"
  export TRPC_APP_ID="$app_id"
fi
"${compose[@]}" up -d --build --wait
"${compose[@]}" exec -T service /app/trpc-healthcheck http://127.0.0.1:8080/healthz
"${compose[@]}" exec -T service /app/trpc-healthcheck http://127.0.0.1:8080/readyz

if [[ "$demo" == true ]]; then
  api_token="${TRPC_API_TOKEN:-local-api-token}"
  if [[ -z "${TRPC_API_TOKEN:-}" ]]; then
    # Read the token value without sourcing the env file (which could execute
    # arbitrary shell syntax). The value is passed only to curl and never
    # printed.
    configured_token="$(awk -F= '$1 == "TRPC_API_TOKEN" {sub(/^[^=]*=/, ""); sub(/\r$/, ""); print; exit}' "$env_file")"
    if [[ -n "$configured_token" ]]; then
      api_token="$configured_token"
    fi
  fi
  http_port="${TRPC_HTTP_PORT:-}"
  if [[ -z "$http_port" ]]; then
    # Keep the probe aligned with Compose interpolation when the env file is
    # supplied as an argument rather than sourced into this shell.
    http_port="$(awk -F= '$1 == "TRPC_HTTP_PORT" {sub(/^[^=]*=/, ""); sub(/\r$/, ""); print; exit}' "$env_file")"
  fi
  http_port="${http_port:-8080}"
  response="$(curl --silent --show-error --fail \
    -H "Authorization: Bearer ${api_token}" \
    -H 'Content-Type: application/json' \
    --data '{"content":"hello from the local golden path","external_user_id":"quickstart-user","conversation_kind":"direct","external_peer_id":"quickstart"}' \
    "http://127.0.0.1:${http_port}/v1/chat")"
  if [[ "$response" != *"Hello from the tRPC Agent Service demo."* ]]; then
    echo "golden path chat response was not deterministic" >&2
    exit 1
  fi
  echo "tRPC Agent Service demo is healthy, ready, and returned the first deterministic /v1/chat response"
else
  echo "tRPC Agent Service is healthy and ready"
fi
echo "stop it with: ${compose[*]} down"
