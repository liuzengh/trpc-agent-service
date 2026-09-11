#!/usr/bin/env bash
set -euo pipefail

CLEAN_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$CLEAN_ROOT"
umask 077
CLEAN_MODE="${1:---dry-run}"
[[ $# -le 1 && ( "$CLEAN_MODE" == --dry-run || "$CLEAN_MODE" == --apply ) ]] || {
  echo "usage: ./scripts/clean.sh [--dry-run|--apply]" >&2; exit 2;
}
for clean_dir in bin data data/archive; do
  [[ ! -L "$clean_dir" && ( ! -e "$clean_dir" || -d "$clean_dir" ) ]] || {
    echo "refusing non-directory or symlink: $clean_dir" >&2; exit 1;
  }
done
CLEAN_TARGETS=()
for clean_file in coverage.out coverage.html \
  bin/trpc-service bin/trpc-local bin/trpc-migrate bin/trpc-init \
  bin/trpc-modelcheck bin/trpc-embeddingcheck \
  bin/trpc-wecomcheck bin/trpc-permissions; do
  [[ ! -L "$clean_file" ]] || { echo "refusing symlink: $clean_file" >&2; exit 1; }
  [[ ! -e "$clean_file" || -f "$clean_file" ]] || { echo "refusing non-file: $clean_file" >&2; exit 1; }
  if [[ -f "$clean_file" ]]; then
    git ls-files --error-unmatch -- "$clean_file" >/dev/null 2>&1 && { echo "refusing tracked file: $clean_file" >&2; exit 1; }
    CLEAN_TARGETS+=("$clean_file")
  fi
done
if [[ ${#CLEAN_TARGETS[@]} == 0 ]]; then echo "no known build outputs to archive"; exit 0; fi
printf 'build output: %s\n' "${CLEAN_TARGETS[@]}"
if [[ "$CLEAN_MODE" == --dry-run ]]; then
  echo "preview only; --apply moves these files to a private archive (no deletion)"
  exit 0
fi
command -v flock >/dev/null || { echo "flock required" >&2; exit 1; }
mkdir -p data
[[ ! -L data/trpc-service.lock ]] || { echo "refusing lock symlink" >&2; exit 1; }
exec 9>data/trpc-service.lock
flock -n 9 || { echo "Agent start/stop or cleanup in progress" >&2; exit 1; }
if [[ -e data/trpc-service.pid || -L data/trpc-service.pid || -e data/trpc-service.process.json || -L data/trpc-service.process.json ]]; then
  echo "PID metadata exists; stop the Agent first; stale records need separate identity review" >&2
  exit 1
fi
mkdir -p data/archive
CLEAN_ARCHIVE="$(mktemp -d "$CLEAN_ROOT/data/archive/build-outputs.XXXXXXXX")"
mkdir "$CLEAN_ARCHIVE/bin"
for clean_file in "${CLEAN_TARGETS[@]}"; do
  mv -- "$CLEAN_ROOT/$clean_file" "$CLEAN_ARCHIVE/$clean_file"
done
echo "archived build outputs: $CLEAN_ARCHIVE"
echo "recover files to their original relative paths, or regenerate with ./scripts/build.sh"
echo "configuration, runtime data, Docker resources and Go caches unchanged"
