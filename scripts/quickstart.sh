#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

project_name="trpc-agent-service-quickstart"
compose=(
  docker compose
  --project-name "$project_name"
  --env-file .env.example
  --file compose.yaml
  --file compose.deployment-e2e.yaml
  --file compose.quickstart.yaml
)

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" config --quiet
cleanup

"${compose[@]}" up -d --build --wait \
  postgres redis qdrant gateway worker-1 worker-2
"${compose[@]}" up --build --abort-on-container-exit --exit-code-from quickstart quickstart

echo "Golden Path PASSED"
