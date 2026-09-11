#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
pg="worker-artifact-pg-$RANDOM-$$"
s3="worker-artifact-s3-$RANDOM-$$"
cleanup(){ docker rm -f "$pg" "$s3" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name "$pg" -e POSTGRES_PASSWORD=fixture-postgres-password -p 127.0.0.1::5432 postgres:17.6-alpine >/dev/null
docker run -d --name "$s3" -e MINIO_ROOT_USER=fixture-access-key -e MINIO_ROOT_PASSWORD=fixture-secret-key -p 127.0.0.1::9000 minio/minio:RELEASE.2025-04-22T22-12-26Z server /data >/dev/null
for i in $(seq 1 60);do if docker exec "$pg" pg_isready -h 127.0.0.1 -U postgres | grep -q 'accepting connections';then break;fi;sleep 1;done
pgport=$(docker port "$pg" 5432/tcp | sed 's/.*://')
s3port=$(docker port "$s3" 9000/tcp | sed 's/.*://')
for i in $(seq 1 60);do if curl -fsS "http://127.0.0.1:$s3port/minio/health/ready" >/dev/null;then break;fi;sleep 1;done
export ARTIFACT_TEST_DATABASE_URL="postgres://postgres:fixture-postgres-password@127.0.0.1:$pgport/postgres?sslmode=disable"
export ARTIFACT_TEST_S3_ENDPOINT="http://127.0.0.1:$s3port"
cd "$root"
go test -race ./services/agent-worker/internal/execution/adapter/outbound/artifactstore -run '^TestArtifactS3Postgres$' -count=1 -v
printf 'ARTIFACT_MINIO_POSTGRES=PASS\n'
