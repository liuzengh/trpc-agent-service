#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# On Windows the MSYS path (/d/...) is not understood by the native go tool,
# which would write the binary to <drive>:/d/... Use the native path instead.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    ROOT="$(cd "$ROOT" && pwd -W)"
    BIN_SUFFIX=".exe"
    ;;
  *) BIN_SUFFIX="" ;;
esac

GO_BIN="${GO_BIN:-$(command -v go 2>/dev/null || true)}"
if [[ -z "$GO_BIN" && -x /usr/local/go/bin/go ]]; then
  GO_BIN=/usr/local/go/bin/go
fi
if [[ -z "$GO_BIN" ]]; then
  echo "Go toolchain is required to build trpc-service" >&2
  exit 1
fi

# Build the embedded development console first so go:embed always has assets.
if [[ -f "$ROOT/webui/package.json" ]]; then
  if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
    echo "Node.js and npm are required to build webui" >&2
    exit 1
  fi
  node_major="$(node -p 'process.versions.node.split(".")[0]')"
  if [[ ! "$node_major" =~ ^[0-9]+$ ]] || (( node_major < 20 || node_major >= 23 )); then
    echo "Node.js 20, 21, or 22 is required; found $(node --version)" >&2
    exit 1
  fi
  # Keep an active Vite dev server's dependency graph intact. Reinstall only
  # when dependencies are missing or the manifest/lockfile has changed.
  WEBUI_DEPS_STAMP="$ROOT/webui/node_modules/.trpc-install-stamp"
  if [[ ! -x "$ROOT/webui/node_modules/.bin/vite" ]] \
      || [[ ! -f "$WEBUI_DEPS_STAMP" ]] \
      || [[ "$ROOT/webui/package.json" -nt "$WEBUI_DEPS_STAMP" ]] \
      || [[ "$ROOT/webui/package-lock.json" -nt "$WEBUI_DEPS_STAMP" ]]; then
    # The web build needs Vite/TypeScript/Vitest from devDependencies even when
    # the caller exports NODE_ENV=production or npm is configured with omit=dev.
    (cd "$ROOT/webui" && npm ci --include=dev --no-audit --no-fund)
    touch "$WEBUI_DEPS_STAMP"
  fi
  if [[ ! -f "$ROOT/webui/dist/index.html" ]] || find "$ROOT/webui/src" "$ROOT/webui/index.html" -newer "$ROOT/webui/dist/index.html" -print -quit | grep -q .; then
    echo "building webui..."
    (cd "$ROOT/webui" && npm run build)
  fi
fi

mkdir -p "$ROOT/bin"
"$GO_BIN" build -o "$ROOT/bin/trpc-service$BIN_SUFFIX" ./cmd/trpc-service
echo "built: $ROOT/bin/trpc-service$BIN_SUFFIX"
