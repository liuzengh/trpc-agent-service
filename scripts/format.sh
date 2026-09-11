#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if command -v rg >/dev/null; then
  rg --files -0 -g '*.go' -g '!data/**' -g '!vendor/**' -g '!**/node_modules/**' | xargs -0 -r gofmt -w
else
  find cmd trpcservice deploy tests -type d -name node_modules -prune -o -type f -name '*.go' -print0 | xargs -0 -r gofmt -w
fi
if [[ -d trpcservice/web/console/node_modules ]]; then
  npm --prefix trpcservice/web/console run format
fi
echo "formatted"
