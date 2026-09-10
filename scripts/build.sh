#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin"
go build -o "$ROOT/bin/trpc-service" ./cmd/trpc-service
echo "built: $ROOT/bin/trpc-service"
