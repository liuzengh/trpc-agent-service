#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
name="worker-knowledge-qdrant-$RANDOM-$$"
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$name" -e QDRANT__SERVICE__API_KEY=fixture-qdrant-key -p 127.0.0.1::6333 qdrant/qdrant:v1.15.4 >/dev/null
port=$(docker port "$name" 6333/tcp | sed 's/.*://')
ready=false
for i in $(seq 1 60);do if curl -fsS -H 'api-key: fixture-qdrant-key' "http://127.0.0.1:$port/readyz" >/dev/null;then ready=true;break;fi;sleep 1;done
$ready
export KNOWLEDGE_TEST_QDRANT_ENDPOINT="http://127.0.0.1:$port"
cd "$root"
go test -race ./services/agent-worker/internal/execution/adapter/outbound/knowledgestore -run '^TestKnowledgeQdrantSDKFixture$' -count=1 -v
printf 'KNOWLEDGE_QDRANT_FIXTURE=PASS\n'
