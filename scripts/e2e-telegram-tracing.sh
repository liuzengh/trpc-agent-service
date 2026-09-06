#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
for dependency in go curl; do
  command -v "$dependency" >/dev/null || { echo "missing dependency: $dependency" >&2; exit 1; }
done

# Keep the shared observability stack running. Never stop the user's Agent,
# model, databases or collector in a test cleanup handler.
docker compose --profile observability up -d tempo otel-collector prometheus grafana
ready=false
for attempt in $(seq 1 45); do
  if curl -fsS --max-time 2 http://127.0.0.1:3200/ready >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo "Tempo is not ready" >&2; exit 1; }

output="$(TEST_TRACE_OTLP_ENDPOINT=127.0.0.1:4317 \
  go test ./trpcservice/approval -run '^TestTelegramApprovalFlow$' -count=1 -v)"
printf '%s\n' "$output"
trace_pair="$(sed -n 's/.*command=批准 initial=\([0-9a-f]*\) decision=\([0-9a-f]*\).*/\1 \2/p' <<<"$output")"
read -r initial_trace decision_trace <<<"$trace_pair"
[[ "$initial_trace" =~ ^[0-9a-f]{32}$ && "$decision_trace" =~ ^[0-9a-f]{32}$ ]] || {
  echo "preflight did not produce valid trace IDs" >&2; exit 1;
}
mkdir -p "$ROOT/data"
trace_dir="$(mktemp -d "$ROOT/data/trace-preflight.XXXXXX")"

fetch_trace() {
  local id="$1" destination="$2"
  for attempt in $(seq 1 30); do
    if curl -fsS --max-time 3 -H 'Accept: application/json' \
      "http://127.0.0.1:3200/api/traces/$id" -o "$destination" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "Tempo did not return trace $id" >&2
  return 1
}
fetch_trace "$initial_trace" "$trace_dir/initial.json"
fetch_trace "$decision_trace" "$trace_dir/decision.json"

go run ./cmd/trpc-tracecheck -initial "$trace_dir/initial.json" -decision "$trace_dir/decision.json"
echo "Telegram protocol tracing preflight passed (mock model/API, memory backends, real Collector/Tempo)."
echo "initial_trace_id=$initial_trace"
echo "decision_trace_id=$decision_trace"
echo "artifacts=$trace_dir"
echo "Observability services were left running; no live Bot messages were sent."
