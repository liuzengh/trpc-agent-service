#!/usr/bin/env bash
# 静态检查：go vet，若安装了 golangci-lint 则追加执行。
set -euo pipefail
cd "$(dirname "$0")"

go vet ./...
if command -v golangci-lint >/dev/null 2>&1; then
  golangci-lint run
fi
echo "lint passed"
