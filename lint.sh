#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

go vet ./...
if ! command -v golangci-lint >/dev/null 2>&1; then
  echo "golangci-lint not found; install the CI-pinned version:" >&2
  echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2" >&2
  exit 1
fi
golangci-lint run ./...
