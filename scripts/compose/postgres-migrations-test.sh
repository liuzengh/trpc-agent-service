#!/usr/bin/env bash
set -euo pipefail

if [[ "${TRPC_MIGRATION_TEST:-}" != "1" ]]; then
  echo "refusing: set TRPC_MIGRATION_TEST=1 for an explicit disposable test database" >&2
  exit 2
fi
if [[ -z "${TRPC_POSTGRES_ADMIN_DSN:-}" ]]; then
  echo "refusing: TRPC_POSTGRES_ADMIN_DSN is required" >&2
  exit 2
fi
if ! command -v go >/dev/null 2>&1; then
  echo "go is required" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
go -C "${repo_root}" run ./cmd/postgres-migration-test
