#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out
