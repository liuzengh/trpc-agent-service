#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
compose_file="$ROOT/deploy/compose/docker-compose.local.yml"

command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
docker compose -f "$compose_file" --profile webui down
echo "Local WebUI stopped. Volumes were retained; use 'docker compose -f deploy/compose/docker-compose.local.yml --profile webui down -v' only to reset local data."
