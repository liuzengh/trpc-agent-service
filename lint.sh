#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

go vet ./...
if command -v golangci-lint >/dev/null 2>&1; then
  golangci-lint run ./...
fi
