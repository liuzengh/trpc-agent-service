#!/usr/bin/env bash
# 清理构建产物与运行时中间文件。
set -euo pipefail
cd "$(dirname "$0")"

rm -rf bin
rm -f coverage.out coverage.html
rm -f data/trpc-service.log data/trpc-service.pid
echo "cleaned"
