#!/usr/bin/env bash
# Usage:
#   ./scripts/format.sh          # format all Go files in place
#   ./scripts/format.sh --check  # CI mode: fail if any file is unformatted
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--check" ]]; then
  UNFORMATTED="$(gofmt -l .)"
  if [[ -n "$UNFORMATTED" ]]; then
    echo "::error::The following files are not gofmt-formatted (run ./scripts/format.sh):"
    echo "$UNFORMATTED"
    exit 1
  fi
  echo "format check passed"
  exit 0
fi

gofmt -w .
echo "formatted"
