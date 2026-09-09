#!/usr/bin/env bash
set -euo pipefail
umask 077

project="${TRPC_AGENT_COMPOSE_PROJECT:-trpc-agent-service-acceptance}"
profile="multinode"
base_url="${TRPC_AGENT_ACCEPTANCE_BASE_URL:-http://127.0.0.1:${TRPC_AGENT_MULTINODE_PORT:-18080}}"
compose=(docker compose -p "$project" --profile "$profile")

for command in docker curl jq; do
  command -v "$command" >/dev/null 2>&1 || { echo "ERROR required command is unavailable: $command" >&2; exit 1; }
done
[[ "$project" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo "ERROR invalid Compose project name" >&2; exit 1; }

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-agent-multinode.XXXXXX")"
legacy_service_was_running=0
cleanup() {
  if [[ "$legacy_service_was_running" == "1" ]]; then
    "${compose[@]}" start service >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

compose_exec() { "${compose[@]}" exec -T "$@"; }
sql_scalar() {
  compose_exec postgres psql -X -v ON_ERROR_STOP=1 -U trpc_agent -d trpc_agent -Atqc "$1"
}

if [[ "${TRPC_AGENT_ACCEPTANCE_START:-0}" == "1" ]]; then
  "${compose[@]}" up -d --build gateway worker-a worker-b
fi

running_services="$("${compose[@]}" ps --status running --services)"
if grep -Fxq service <<<"$running_services"; then
  if [[ "${TRPC_AGENT_ACCEPTANCE_ISOLATE_TOPOLOGY:-0}" == "1" ]]; then
    "${compose[@]}" stop service
    legacy_service_was_running=1
    running_services="$("${compose[@]}" ps --status running --services)"
  elif [[ "${TRPC_AGENT_ACCEPTANCE_RUN_MESSAGES:-0}" == "1" ]]; then
    echo "ERROR active failover requires TRPC_AGENT_ACCEPTANCE_ISOLATE_TOPOLOGY=1 while legacy all-mode service is running" >&2
    exit 1
  fi
fi
for service in postgres redis gateway worker-a worker-b; do
  grep -Fxq "$service" <<<"$running_services" || { echo "ERROR required service is not running: $service" >&2; exit 1; }
done
echo "PASS explicit Gateway + two-Worker topology is running"

curl --fail --silent --show-error --max-time 5 "$base_url/healthz" >"$work_dir/health.json"
jq -e '.status == "ok"' "$work_dir/health.json" >/dev/null
curl --fail --silent --show-error --max-time 5 "$base_url/readyz" >"$work_dir/ready.json"
jq -e '.status == "ready"' "$work_dir/ready.json" >/dev/null
echo "PASS Gateway liveness and shared-backend readiness"

if "${compose[@]}" port worker-a 8080 >"$work_dir/worker-a.port" 2>/dev/null && [[ -s "$work_dir/worker-a.port" ]]; then
  echo "ERROR worker-a exposes an HTTP callback port" >&2
  exit 1
fi
if "${compose[@]}" port worker-b 8080 >"$work_dir/worker-b.port" 2>/dev/null && [[ -s "$work_dir/worker-b.port" ]]; then
  echo "ERROR worker-b exposes an HTTP callback port" >&2
  exit 1
fi
echo "PASS Workers expose no production HTTP callback port"

live_workers="$(sql_scalar "SELECT count(*) FROM worker_nodes WHERE node_id IN ('worker-a','worker-b') AND NOT draining AND stopped_at IS NULL AND last_heartbeat > NOW()-INTERVAL '20 seconds'")"
[[ "$live_workers" == "2" ]] || { echo "ERROR expected two live acceptance Workers; observed $live_workers" >&2; exit 1; }
echo "PASS two unique Worker registrations and heartbeats"

stream_length="$(compose_exec redis redis-cli --raw XLEN trpc-agent-service:work | tr -d '\r')"
[[ "$stream_length" =~ ^[0-9]+$ ]] || { echo "ERROR unable to inspect Redis Stream" >&2; exit 1; }
echo "PASS shared Redis Stream is inspectable"

sql_scalar "SELECT json_build_object('configs',count(*),'version_sum',COALESCE(sum(current_config_version),0)) FROM tenants" >"$work_dir/config-before.json"
sql_scalar "SELECT json_build_object('sessions',count(*),'events',(SELECT count(*) FROM message_events),'memories',(SELECT count(*) FROM memory_entries)) FROM session_heads" >"$work_dir/data-before.json"
jq -e '.configs >= 1 and .version_sum >= 1' "$work_dir/config-before.json" >/dev/null
jq -e '.sessions >= 0 and .events >= 0 and .memories >= 0' "$work_dir/data-before.json" >/dev/null
echo "PASS PostgreSQL configuration and shared Session/Event/Memory stores are readable"

if [[ "${TRPC_AGENT_ACCEPTANCE_RUN_INTEGRATION:-0}" == "1" ]]; then
  "${compose[@]}" --profile test run --rm --build integration-test
  echo "PASS isolated PostgreSQL/Redis fencing, takeover, ordering, and tenant tests"
fi

if [[ "${TRPC_AGENT_ACCEPTANCE_RUN_MESSAGES:-0}" == "1" ]]; then
  token="${TRPC_AGENT_ACCEPTANCE_GATEWAY_TOKEN:-}"
  binding="${TRPC_AGENT_ACCEPTANCE_BINDING:-}"
  tenant="${TRPC_AGENT_ACCEPTANCE_TENANT:-}"
  count="${TRPC_AGENT_ACCEPTANCE_MESSAGE_COUNT:-8}"
  [[ -n "$token" && -n "$binding" && -n "$tenant" ]] || { echo "ERROR active message checks require token, binding, and tenant" >&2; exit 1; }
  [[ "$binding" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ && "$tenant" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo "ERROR invalid acceptance scope" >&2; exit 1; }
  [[ "$count" =~ ^[0-9]+$ && "$count" -ge 2 && "$count" -le 20 ]] || { echo "ERROR message count must be 2..20" >&2; exit 1; }
  printf 'Authorization: Bearer %s\nX-Channel-Binding: %s\n' "$token" "$binding" >"$work_dir/gateway.headers"

  submit_message() {
    local message_id="$1" accepted="$work_dir/accepted-$1.json"
    jq -n --arg id "$message_id" --arg user "acceptance-$message_id" '{channel:"http",from:$user,message_id:$id,text:"Synthetic acceptance probe. Reply OK only."}' >"$work_dir/message-$1.json"
    curl --fail --silent --show-error --max-time 10 -X POST --header @"$work_dir/gateway.headers" -H 'Content-Type: application/json' --data-binary @"$work_dir/message-$1.json" "$base_url/v1/gateway/messages" >"$accepted"
    jq -er '.request_id | select(length > 0)' "$accepted" >"$work_dir/request-$1"
  }

  wait_message() {
    local message_id="$1" status="$work_dir/status-$1.json" terminal="" request_id
    request_id="$(<"$work_dir/request-$1")"
    for _ in $(seq 1 90); do
      curl --fail --silent --show-error --max-time 5 --header @"$work_dir/gateway.headers" --get --data-urlencode "request_id=$request_id" "$base_url/v1/gateway/status" >"$status"
      terminal="$(jq -r '.type // ""' "$status")"
      case "$terminal" in
        run.completed) jq -er '.worker_id | select(length > 0)' "$status"; return 0 ;;
        run.error|run.canceled) return 1 ;;
      esac
      sleep 2
    done
    return 1
  }

  submit_and_wait() {
    submit_message "$1"
    wait_message "$1"
  }

  run_id="multinode-$(date -u +%Y%m%dT%H%M%S)-$$"
  : >"$work_dir/workers"
  for index in $(seq 1 "$count"); do
    submit_message "$run_id-$index"
  done
  for index in $(seq 1 "$count"); do
    wait_message "$run_id-$index" >>"$work_dir/workers"
  done
  distinct_workers="$(sort -u "$work_dir/workers" | wc -l | tr -d ' ')"
  [[ "$distinct_workers" -ge 2 ]] || { echo "ERROR synthetic requests were not observed on two Workers" >&2; exit 1; }
  after_stream="$(compose_exec redis redis-cli --raw XLEN trpc-agent-service:work | tr -d '\r')"
  [[ "$after_stream" -gt "$stream_length" ]] || { echo "ERROR Redis Stream did not record active probes" >&2; exit 1; }
  echo "PASS synthetic messages traversed Redis Streams and two different Workers"

  duplicate_id="$run_id-duplicate"
  first_worker="$(submit_and_wait "$duplicate_id")"
  duplicate_worker="$(submit_and_wait "$duplicate_id")"
  [[ "$first_worker" == "$duplicate_worker" ]] || { echo "ERROR duplicate message changed Worker outcome" >&2; exit 1; }
  duplicate_rows="$(sql_scalar "SELECT count(*) FROM inbox_messages WHERE tenant_id='$tenant' AND binding_id='$binding' AND external_message_id='$duplicate_id'")"
  [[ "$duplicate_rows" == "1" ]] || { echo "ERROR duplicate message created $duplicate_rows Inbox rows" >&2; exit 1; }
  echo "PASS duplicate message_id has one durable Inbox execution"

  version_before="$(sql_scalar "SELECT current_config_version FROM tenants WHERE tenant_id='$tenant'")"
  "${compose[@]}" stop worker-a
  failover_worker="$(submit_and_wait "$run_id-failover")"
  [[ "$failover_worker" == "worker-b" ]] || { echo "ERROR request was not handled by surviving worker-b" >&2; "${compose[@]}" start worker-a >/dev/null; exit 1; }
  "${compose[@]}" start worker-a >/dev/null
  for _ in $(seq 1 30); do
    live_workers="$(sql_scalar "SELECT count(*) FROM worker_nodes WHERE node_id IN ('worker-a','worker-b') AND NOT draining AND stopped_at IS NULL AND last_heartbeat > NOW()-INTERVAL '20 seconds'")"
    [[ "$live_workers" == "2" ]] && break
    sleep 2
  done
  [[ "$live_workers" == "2" ]] || { echo "ERROR worker-a did not re-register after restart" >&2; exit 1; }
  version_after="$(sql_scalar "SELECT current_config_version FROM tenants WHERE tenant_id='$tenant'")"
  [[ "$version_after" == "$version_before" ]] || { echo "ERROR configuration version changed across Worker restart" >&2; exit 1; }
  echo "PASS surviving Worker handles requests and config version survives targeted restart"
fi

if [[ "${TRPC_AGENT_ACCEPTANCE_RESTART_WORKER:-0}" == "1" && "${TRPC_AGENT_ACCEPTANCE_RUN_MESSAGES:-0}" != "1" ]]; then
  "${compose[@]}" stop worker-a
  surviving="$(sql_scalar "SELECT count(*) FROM worker_nodes WHERE node_id='worker-b' AND NOT draining AND stopped_at IS NULL AND last_heartbeat > NOW()-INTERVAL '20 seconds'")"
  [[ "$surviving" == "1" ]] || { echo "ERROR worker-b was not healthy during worker-a stop" >&2; "${compose[@]}" start worker-a >/dev/null; exit 1; }
  "${compose[@]}" start worker-a >/dev/null
  live_workers=0
  for _ in $(seq 1 30); do
    live_workers="$(sql_scalar "SELECT count(*) FROM worker_nodes WHERE node_id IN ('worker-a','worker-b') AND NOT draining AND stopped_at IS NULL AND last_heartbeat > NOW()-INTERVAL '20 seconds'")"
    [[ "$live_workers" == "2" ]] && break
    sleep 2
  done
  [[ "$live_workers" == "2" ]] || { echo "ERROR worker-a did not re-register after restart" >&2; exit 1; }
  echo "PASS targeted Worker restart preserved a healthy surviving Worker"
fi

sql_scalar "SELECT json_build_object('configs',count(*),'version_sum',COALESCE(sum(current_config_version),0)) FROM tenants" >"$work_dir/config-after.json"
sql_scalar "SELECT json_build_object('sessions',count(*),'events',(SELECT count(*) FROM message_events),'memories',(SELECT count(*) FROM memory_entries)) FROM session_heads" >"$work_dir/data-after.json"
jq -e --slurpfile before "$work_dir/config-before.json" '.version_sum >= $before[0].version_sum and .configs >= $before[0].configs' "$work_dir/config-after.json" >/dev/null
jq -e --slurpfile before "$work_dir/data-before.json" '.sessions >= $before[0].sessions and .events >= $before[0].events and .memories >= $before[0].memories' "$work_dir/data-after.json" >/dev/null
echo "PASS PostgreSQL configuration and durable data did not regress"
echo "Multi-node acceptance passed (sanitized output; no message body or credential emitted)"
