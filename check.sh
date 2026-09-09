#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
  echo "error: gofmt required for:" >&2
  echo "$unformatted" >&2
  exit 1
fi
go mod verify
if [[ "$(go list -m -f '{{.Version}}' trpc.group/trpc-go/trpc-agent-go)" != "v1.11.2" ]]; then
  echo "error: trpc-agent-go must be pinned to v1.11.2" >&2
  exit 1
fi
go vet ./...
go test ./...
go test -race ./...

if go list -f '{{range .Imports}}{{println .}}{{end}}' ./... | grep -E '^trpc\.group/trpc-go/trpc-agent-go/.*/internal(/|$)' >/dev/null; then
  echo "error: imports from trpc-agent-go internal packages are forbidden" >&2
  exit 1
fi

./build.sh
scripts/observability_acceptance.sh
