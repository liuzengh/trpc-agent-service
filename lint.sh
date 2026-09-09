#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

UNFORMATTED="$(gofmt -l .)"
if [[ -n "$UNFORMATTED" ]]; then
  printf 'unformatted files:\n%s\n' "$UNFORMATTED" >&2
  exit 1
fi

go vet ./...
if ! command -v golangci-lint >/dev/null 2>&1; then
  printf 'golangci-lint is required\n' >&2
  exit 1
fi
golangci-lint run ./...
