#!/usr/bin/env bash
set -euo pipefail
umask 077

base_url="${TRPC_AGENT_ACCEPTANCE_BASE_URL:-}"
if [[ -z "$base_url" ]]; then
  echo "TRPC_AGENT_ACCEPTANCE_BASE_URL is required" >&2
  exit 1
fi
base_url="${base_url%/}"
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-agent-acceptance.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

curl --fail --silent --show-error --max-time 5 "$base_url/healthz" >"$work_dir/health.json"
jq -e '.status == "ok"' "$work_dir/health.json" >/dev/null
curl --fail --silent --show-error --max-time 5 "$base_url/readyz" >"$work_dir/ready.json"
jq -e '.status == "ready"' "$work_dir/ready.json" >/dev/null
echo "PASS liveness and shared-backend readiness"

if [[ -n "${TRPC_AGENT_ACCEPTANCE_ADMIN_TOKEN:-}" || -n "${TRPC_AGENT_ACCEPTANCE_TENANT:-}" ]]; then
  [[ -n "${TRPC_AGENT_ACCEPTANCE_ADMIN_TOKEN:-}" && -n "${TRPC_AGENT_ACCEPTANCE_TENANT:-}" ]] || {
    echo "admin token and tenant must be provided together" >&2
    exit 1
  }
  [[ "$TRPC_AGENT_ACCEPTANCE_TENANT" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
    echo "acceptance tenant has invalid characters" >&2
    exit 1
  }
  printf 'Authorization: Bearer %s\n' "$TRPC_AGENT_ACCEPTANCE_ADMIN_TOKEN" >"$work_dir/admin.headers"
  curl --fail --silent --show-error --max-time 10 \
    --header @"$work_dir/admin.headers" \
    "$base_url/v1/tenants/${TRPC_AGENT_ACCEPTANCE_TENANT}/configs/current" >"$work_dir/current.json"
  jq -e '.version > 0 or .config_version > 0' "$work_dir/current.json" >/dev/null
  echo "PASS authenticated tenant-scoped current config"
fi

if [[ "${TRPC_AGENT_ACCEPTANCE_RUN_MESSAGE:-0}" == "1" ]]; then
  [[ -n "${TRPC_AGENT_ACCEPTANCE_GATEWAY_TOKEN:-}" && -n "${TRPC_AGENT_ACCEPTANCE_BINDING:-}" ]] || {
    echo "message check requires gateway token and binding" >&2
    exit 1
  }
  [[ "$TRPC_AGENT_ACCEPTANCE_BINDING" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
    echo "acceptance binding has invalid characters" >&2
    exit 1
  }
  printf 'Authorization: Bearer %s\nX-Channel-Binding: %s\n' \
    "$TRPC_AGENT_ACCEPTANCE_GATEWAY_TOKEN" "$TRPC_AGENT_ACCEPTANCE_BINDING" >"$work_dir/gateway.headers"
  message_id="acceptance-$(date -u +%Y%m%dT%H%M%S)-$$"
  jq -n --arg id "$message_id" '{channel:"http",from:"production-acceptance",message_id:$id,text:"Reply with the single word OK"}' >"$work_dir/message.json"
  curl --fail --silent --show-error --max-time 10 -X POST \
    --header @"$work_dir/gateway.headers" \
    -H 'Content-Type: application/json' --data-binary @"$work_dir/message.json" \
    "$base_url/v1/gateway/messages" >"$work_dir/accepted.json"
  request_id="$(jq -er '.request_id | select(length > 0)' "$work_dir/accepted.json")"
  terminal=""
  for _ in $(seq 1 60); do
    curl --fail --silent --show-error --max-time 5 \
      --header @"$work_dir/gateway.headers" \
      --get --data-urlencode "request_id=$request_id" "$base_url/v1/gateway/status" >"$work_dir/status.json"
    terminal="$(jq -r '.type // ""' "$work_dir/status.json")"
    case "$terminal" in
      run.completed) echo "PASS bounded real Runner message"; break ;;
      run.error|run.canceled) echo "message ended as $terminal" >&2; exit 1 ;;
    esac
    sleep 2
  done
  [[ "$terminal" == "run.completed" ]] || { echo "message did not complete within 120 seconds" >&2; exit 1; }
fi

echo "Production acceptance checks passed"
