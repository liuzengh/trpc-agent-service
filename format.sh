#!/usr/bin/env bash
# 格式化 Go 代码并规范化 import 分组。
set -euo pipefail
cd "$(dirname "$0")"

if command -v goimports >/dev/null 2>&1; then
  goimports -local github.com/cyl6/trpc-agent-service -w .
else
  gofmt -w .
fi
echo "formatted"
