#!/usr/bin/env bash
# Fully isolated latest-version workflow; never load .env or start shared Compose.
set -euo pipefail
JOINT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$JOINT_ROOT"
unset TEST_POSTGRES_URL TEST_REDIS_URL TEST_QDRANT_HOST TEST_QDRANT_PORT TEST_S3_ENDPOINT
TEST_RECOVERY_DOCKER=1 go test -race -count=1 -v ./trpcservice/recovery \
  -run '^TestIsolatedTwoTenantTwoWorkerWorkflow$'
