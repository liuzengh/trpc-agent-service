#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

run_demo() {
  local demo_project="${TRPC_DEMO_COMPOSE_PROJECT:-trpc-agent-demo}"
  local demo_port="${TRPC_DEMO_PORT:-58082}"
  local demo_image="trpc-agent-service-demo:quickstart"
  local demo_container="${demo_project}-service"
  local demo_network
  local demo_label
  local chat_response
  local stream_response
  local postgres_id
  local redis_id
  local postgres_health
  local redis_health
  local migration_versions

  [[ "$demo_project" =~ ^[a-z][a-z0-9-]{1,62}$ ]] || { echo "TRPC_DEMO_COMPOSE_PROJECT must be a lowercase Docker project name" >&2; exit 2; }
  [[ "$demo_port" =~ ^[1-9][0-9]{0,4}$ ]] && ((demo_port <= 65535)) || { echo "TRPC_DEMO_PORT must be a TCP port" >&2; exit 2; }
  command -v curl >/dev/null 2>&1 || { echo "curl is required for the demo acceptance gates" >&2; exit 2; }

  trap 'echo "Demo quickstart failed; dependencies remain available for diagnosis." >&2; if docker container inspect "${demo_container}" >/dev/null 2>&1; then docker logs "${demo_container}" >&2; else docker compose -p "${demo_project}" -f "${ROOT}/deploy/compose/docker-compose.backend-smoke.yml" logs postgres redis >&2; fi' ERR
  echo "[1/6] Building the credential-free demo image"
  docker build --tag "$demo_image" --file "$ROOT/deploy/compose/Dockerfile" "$ROOT"
  echo "[2/6] Starting disposable PostgreSQL and Redis"
  docker compose -p "$demo_project" -f "$ROOT/deploy/compose/docker-compose.backend-smoke.yml" up -d postgres redis
  postgres_id="$(docker compose -p "$demo_project" -f "$ROOT/deploy/compose/docker-compose.backend-smoke.yml" ps -q postgres)"
  redis_id="$(docker compose -p "$demo_project" -f "$ROOT/deploy/compose/docker-compose.backend-smoke.yml" ps -q redis)"
  [[ -n "$postgres_id" && -n "$redis_id" ]] || { echo "Demo dependencies were not started" >&2; exit 1; }
  for attempt in $(seq 1 30); do
    postgres_health="$(docker inspect -f '{{.State.Health.Status}}' "$postgres_id")"
    redis_health="$(docker inspect -f '{{.State.Health.Status}}' "$redis_id")"
    [[ "$postgres_health" == healthy && "$redis_health" == healthy ]] && break
    sleep 1
  done
  [[ "$postgres_health" == healthy && "$redis_health" == healthy ]] || { echo "Demo dependencies did not become healthy (postgres=${postgres_health}, redis=${redis_health})" >&2; exit 1; }
  demo_network="$(docker network ls -q --filter "label=com.docker.compose.project=$demo_project" | sed -n '1p')"
  [[ -n "$demo_network" ]] || { echo "Demo Compose network was not created" >&2; exit 1; }
  echo "[3/6] Publishing the deterministic fake-provider demo control plane"
  docker run --rm --network "$demo_network" \
    -e 'TRPC_POSTGRES_DSN=postgres://trpc:trpc_test_password@postgres:5432/postgres?sslmode=disable' \
    "$demo_image" demo --confirm
  echo "[4/6] Verifying the complete empty-database migration history"
  migration_versions="$(docker compose -p "$demo_project" -f "$ROOT/deploy/compose/docker-compose.backend-smoke.yml" exec -T postgres \
    psql -U trpc -d postgres -Atc 'SELECT version FROM public.schema_migrations ORDER BY version')"
  # A first-delivery demo starts from an empty database and must apply exactly
  # the consolidated schema baseline, not a stale or partially applied schema.
  [[ "$migration_versions" == "000001" ]] || { echo "Unexpected schema migration history: ${migration_versions:-<empty>}" >&2; exit 1; }
  if docker container inspect "$demo_container" >/dev/null 2>&1; then
    demo_label="$(docker inspect -f '{{index .Config.Labels "trpc-agent-service.demo"}}' "$demo_container")"
    [[ "$demo_label" == "$demo_project" ]] || { echo "Refusing to replace unrelated container: $demo_container" >&2; exit 1; }
    docker rm -f "$demo_container" >/dev/null
  fi
  echo "[5/6] Starting the fake-provider HTTP demo service"
  docker run -d --name "$demo_container" --label "trpc-agent-service.demo=$demo_project" --network "$demo_network" \
    -p "127.0.0.1:${demo_port}:8080" \
    -e 'TRPC_POSTGRES_DSN=postgres://trpc:trpc_test_password@postgres:5432/postgres?sslmode=disable' \
    -e 'TRPC_LISTEN_ADDRESS=:8080' "$demo_image" demo-server >/dev/null
  echo "[6/6] Checking health, readiness, deterministic chat, and streamed deltas"
  for attempt in $(seq 1 30); do
    if curl --fail --silent "http://127.0.0.1:${demo_port}/healthz" >/dev/null && \
      curl --fail --silent "http://127.0.0.1:${demo_port}/readyz" >/dev/null; then
      break
    fi
    sleep 1
  done
  curl --fail --silent "http://127.0.0.1:${demo_port}/readyz" >/dev/null || { docker logs "$demo_container" >&2; exit 1; }
  chat_response="$(curl --fail --silent --show-error -H 'Content-Type: application/json' \
    --data '{"message":"quickstart acceptance"}' "http://127.0.0.1:${demo_port}/v1/chat")"
  [[ "$chat_response" == *'"response":"demo: deterministic fake response"'* ]] || { echo "Unexpected fake chat response: $chat_response" >&2; exit 1; }
  stream_response="$(curl --fail --silent --show-error --no-buffer -H 'Content-Type: application/json' \
    --data '{"message":"quickstart streaming acceptance","stream":true}' "http://127.0.0.1:${demo_port}/v1/chat")"
  [[ "$stream_response" == *'event: delta'* && "$stream_response" == *'"delta":"demo: "'* && \
    "$stream_response" == *'"delta":"deterministic fake response"'* && "$stream_response" == *'event: done'* ]] || {
      echo "Unexpected fake stream response: $stream_response" >&2; exit 1;
    }
  trap - ERR
  cat <<EOF
Demo acceptance complete
  HTTP: http://127.0.0.1:${demo_port}
  Health: /healthz and /readyz passed
  Schema: empty database applied the consolidated migration 000001
  Chat: /v1/chat returned the deterministic fake response
  Stream: /v1/chat emitted the deterministic fake delta sequence over SSE
  Coverage: docs/runbook/demo-fake-coverage.md
  Stop: docker rm -f ${demo_container} && docker compose -p ${demo_project} -f deploy/compose/docker-compose.backend-smoke.yml down -v
EOF
}

if [[ "${1:-}" != "--demo" || $# -ne 1 ]]; then
  echo "usage: $0 --demo" >&2
  exit 2
fi
command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
run_demo
