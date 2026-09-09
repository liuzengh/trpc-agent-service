#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="${TRPC_AGENT_COMPOSE_PROJECT:-trpc-agent-service-acceptance}"
compose=(docker compose -p "$project")

for command in docker; do
  command -v "$command" >/dev/null 2>&1 || { echo "ERROR required command is unavailable: $command" >&2; exit 1; }
done
[[ "$project" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo "ERROR invalid Compose project name" >&2; exit 1; }

contains_fixed() {
  local needle="$1" file="$2"
  if command -v rg >/dev/null 2>&1; then
    rg --fixed-strings --quiet -- "$needle" "$file"
  else
    grep -Fq -- "$needle" "$file"
  fi
}

contains_line() {
  local needle="$1" file="$2"
  if command -v rg >/dev/null 2>&1; then
    rg --fixed-strings --line-regexp --quiet -- "$needle" "$file"
  else
    grep -Fxq -- "$needle" "$file"
  fi
}

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-agent-observability.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

DEEPSEEK_API_KEY="${DEEPSEEK_API_KEY:-structural-placeholder}" "${compose[@]}" config --quiet

for alert in AgentHighErrorRate AgentDLQNotEmpty AgentQueueBacklogGrowing AgentNoLiveWorker AgentPostgreSQLUnavailable AgentModelUsageMissing; do
  contains_fixed "alert: $alert" "$repo_root/deploy/prometheus-alerts.yml" || {
    echo "ERROR missing required alert: $alert" >&2
    exit 1
  }
done
contains_fixed 'exporters: [otlp/tempo]' "$repo_root/deploy/otel-collector.yaml"
contains_fixed 'uid: tempo' "$repo_root/deploy/grafana/provisioning/datasources/tempo.yml"
echo "PASS Compose, Tempo exporter, Grafana datasource, and six production alerts are structurally present"

if [[ "${TRPC_AGENT_OBSERVABILITY_ACCEPTANCE_LIVE:-0}" != "1" ]]; then
  echo "Observability structural acceptance passed (set TRPC_AGENT_OBSERVABILITY_ACCEPTANCE_LIVE=1 for live checks)"
  exit 0
fi

for command in curl jq; do
  command -v "$command" >/dev/null 2>&1 || { echo "ERROR required command is unavailable for live checks: $command" >&2; exit 1; }
done

running_services="$("${compose[@]}" ps --status running --services)"
for service in tempo otel-collector prometheus grafana; do
  contains_line "$service" <(printf '%s\n' "$running_services") || { echo "ERROR required service is not running: $service" >&2; exit 1; }
done

"${compose[@]}" exec -T prometheus promtool check config /etc/prometheus/prometheus.yml >"$work_dir/promtool.txt"

curl --fail --silent --show-error --retry 12 --retry-all-errors --retry-delay 2 --max-time 5 http://127.0.0.1:3200/ready >"$work_dir/tempo-ready.txt"
curl --fail --silent --show-error --retry 12 --retry-all-errors --retry-delay 2 --max-time 5 http://127.0.0.1:9090/-/ready >"$work_dir/prometheus-ready.txt"
curl --fail --silent --show-error --retry 12 --retry-all-errors --retry-delay 2 --max-time 5 http://127.0.0.1:3000/api/health >"$work_dir/grafana-health.json"
jq -e '.database == "ok"' "$work_dir/grafana-health.json" >/dev/null
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:9090/api/v1/rules >"$work_dir/rules.json"
for alert in AgentHighErrorRate AgentDLQNotEmpty AgentQueueBacklogGrowing AgentNoLiveWorker AgentPostgreSQLUnavailable AgentModelUsageMissing; do
  jq -e --arg alert "$alert" '[.data.groups[].rules[] | select(.name == $alert)] | length == 1' "$work_dir/rules.json" >/dev/null
done
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:3000/api/datasources/uid/tempo >"$work_dir/tempo-datasource.json"
jq -e '.uid == "tempo" and .type == "tempo"' "$work_dir/tempo-datasource.json" >/dev/null
echo "PASS Tempo, Prometheus alert evaluation, and Grafana Tempo datasource are live"

trace_id="${TRPC_AGENT_ACCEPTANCE_TRACE_ID:-}"
if [[ -n "$trace_id" ]]; then
  [[ "$trace_id" =~ ^[0-9a-fA-F]{32}$ ]] || { echo "ERROR acceptance trace ID must be 32 hexadecimal characters" >&2; exit 1; }
  curl --fail --silent --show-error --max-time 10 "http://127.0.0.1:3200/api/traces/$trace_id" >"$work_dir/trace.json"
  jq -r '.. | objects | .name? // empty' "$work_dir/trace.json" | sort -u >"$work_dir/span-names.txt"
  for span in channel.callback inbox.claim worker.run runner.execute model.stream session.write memory.summary.write outbox.write outbox.deliver; do
    contains_line "$span" "$work_dir/span-names.txt" || { echo "ERROR persisted trace is missing stage: $span" >&2; exit 1; }
  done
  echo "PASS persisted trace connects Callback, Inbox, Worker, Model, Storage, and Outbox delivery"

  canary="${TRPC_AGENT_ACCEPTANCE_PRIVATE_CANARY:-}"
  if [[ -n "$canary" ]]; then
    if contains_fixed "$canary" "$work_dir/trace.json"; then
      echo "ERROR private canary appeared in persisted trace" >&2
      exit 1
    fi
    curl --fail --silent --show-error --max-time 10 http://127.0.0.1:9090/api/v1/label/__name__/values >"$work_dir/metric-names.json"
    if contains_fixed "$canary" "$work_dir/metric-names.json"; then
      echo "ERROR private canary appeared in metric metadata" >&2
      exit 1
    fi
    echo "PASS supplied private canary is absent from persisted trace and metric metadata"
  fi
fi

echo "Live observability acceptance passed (sanitized output; no trace payload or credential emitted)"
