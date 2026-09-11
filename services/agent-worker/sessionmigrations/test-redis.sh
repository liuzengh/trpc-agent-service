#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd)
work=$(mktemp -d)
name="worker-session-redis-$RANDOM-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT
cat > "$work/users.acl" <<'ACL'
user default off
user fixture_admin on >fixture-admin-password ~* +@all
user session_runtime on >fixture-runtime-password ~runtime_session:* +@connection +get +type +pttl +eval +set
ACL
chmod 600 "$work/users.acl"
docker run -d --name "$name" --user 0 -p 127.0.0.1::6379 -v "$work:/fixture:ro" redis:7.4-alpine redis-server --aclfile /fixture/users.acl --appendonly yes --appendfsync always --save '' >/dev/null
port=$(docker port "$name" 6379/tcp | sed 's/.*://')
wait_ready() { for i in $(seq 1 60); do if docker exec "$name" redis-cli --user fixture_admin --no-auth-warning -a fixture-admin-password ping | grep -qx PONG; then return; fi; sleep 1; done; return 1; }
wait_ready
export WORKER_SESSION_REDIS_ADDR="127.0.0.1:$port"
export WORKER_SESSION_REDIS_PASSWORD=fixture-runtime-password
export WORKER_SESSION_REDIS_ADMIN_PASSWORD=fixture-admin-password
cd "$root"
go test -race ./services/agent-worker/internal/execution/adapter/outbound/sessionstore -run '^TestRedisSessionContract$' -count=1 -v
docker restart "$name" >/dev/null
port=$(docker port "$name" 6379/tcp | sed 's/.*://')
export WORKER_SESSION_REDIS_ADDR="127.0.0.1:$port"
wait_ready
export WORKER_SESSION_REDIS_VERIFY_RESTART=1
go test -race ./services/agent-worker/internal/execution/adapter/outbound/sessionstore -run '^TestRedisSessionAfterRestart$' -count=1 -v
printf 'REDIS_SESSION_AOF_RESTART=PASS\n'
