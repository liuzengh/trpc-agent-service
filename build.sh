#!/usr/bin/env bash
# 构建项目：同步前端嵌入资源，产出服务、schema 与数据迁移命令。
set -euo pipefail
cd "$(dirname "$0")"

VERSION="${VERSION:-$(git rev-parse --short HEAD 2>/dev/null || echo dev)}"
LDFLAGS="-X github.com/cyl6/trpc-agent-service/trpcservice.Version=$(cat VERSION 2>/dev/null || echo 0.1.0)"
LDFLAGS="${LDFLAGS} -X github.com/cyl6/trpc-agent-service/trpcservice.GitCommit=${VERSION}"

if [[ -f im-console/package.json ]]; then
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm is required to build the embedded console" >&2
    exit 1
  fi
  (
    cd im-console
    if [[ ! -d node_modules ]]; then
      npm ci --no-audit --no-fund
    fi
    npm run build:embed
  )
fi

mkdir -p bin
CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o bin/trpc-service ./cmd/trpc-service
CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o bin/trpc-migrate ./cmd/trpc-migrate
CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o bin/trpc-data-migrate ./cmd/trpc-data-migrate
echo "built bin/trpc-service, bin/trpc-migrate and bin/trpc-data-migrate"
