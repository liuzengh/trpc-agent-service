#!/usr/bin/env bash
# 运行单元测试并生成覆盖率报告（coverage.out / coverage.html）。
set -euo pipefail
cd "$(dirname "$0")"

go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | tail -1
go tool cover -html=coverage.out -o coverage.html
echo "report: coverage.out, coverage.html"
