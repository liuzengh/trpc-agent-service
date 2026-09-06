#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin"
go build -o "$ROOT/bin/trpc-service" ./cmd/trpc-service
go build -o "$ROOT/bin/trpc-migrate" ./cmd/trpc-migrate
go build -o "$ROOT/bin/trpc-loadgen" ./cmd/trpc-loadgen
go build -o "$ROOT/bin/trpc-modelcheck" ./cmd/trpc-modelcheck
go build -o "$ROOT/bin/trpc-wecomcheck" ./cmd/trpc-wecomcheck
echo "built: $ROOT/bin/trpc-service, trpc-migrate, trpc-loadgen, trpc-modelcheck, trpc-wecomcheck"
