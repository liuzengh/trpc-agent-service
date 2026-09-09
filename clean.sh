#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# $ROOT/trpc-service: a hand-run `go build ./cmd/trpc-service` lands here rather
# than in bin/, so sweep it too — a stray 60MB binary is easy to commit by hand.
rm -rf "$ROOT/bin" "$ROOT/trpc-service" coverage.out coverage.html
go clean ./...
echo "cleaned"
