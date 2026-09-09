#!/usr/bin/env bash
set -euo pipefail

# Replays only synthetic encrypted callbacks and synthetic provider replies.
# It reads no local config, credentials, database password, or user content.
cache_dir="${GOCACHE:-/tmp/trpc-agent-service-contract-cache}"
GOCACHE="$cache_dir" go test ./trpcservice/acceptance -run '^TestDualIMContractE2E$' -count=1
printf '%s\n' 'dual IM contract acceptance: PASS'
