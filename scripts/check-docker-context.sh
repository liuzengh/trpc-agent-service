#!/usr/bin/env bash
# Verifies that every local COPY source in the repository Dockerfiles is present
# in the build context. A .dockerignore rule that is too broad silently removes
# runtime assets (for example tests/mock/mock-openai.mjs) and only fails later,
# deep inside a Kind or image build.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

status=0

while IFS= read -r dockerfile; do
  # Keep only local COPY sources: skip multi-stage "--from=" references and
  # treat every argument except the last one as a source path.
  mapfile -t sources < <(
    sed -nE 's/^[[:space:]]*COPY[[:space:]]+//p' "$dockerfile" 2>/dev/null \
      | grep -v -- '--from=' \
      | awk '{ for (i = 1; i < NF; i++) print $i }'
  )
  [ "${#sources[@]}" -eq 0 ] && continue

  tmp="$(mktemp -d)"
  {
    echo "FROM scratch"
    for source in "${sources[@]}"; do
      echo "COPY $source /check/"
    done
  } >"$tmp/Dockerfile"

  if ! docker build -f "$tmp/Dockerfile" . >"$tmp/build.log" 2>&1; then
    echo "::error file=$dockerfile::COPY source missing from build context (likely excluded by .dockerignore)"
    grep -iE 'not found|no such file|failed to compute' "$tmp/build.log" 2>/dev/null | head -5
    status=1
  else
    echo "OK   $dockerfile (${#sources[@]} COPY source(s))"
  fi
  rm -rf "$tmp"
done < <(
  find . \( -name 'Dockerfile' -o -name '*.Dockerfile' \) \
    -not -path './.git/*' \
    -not -path './.toolchain/*' \
    -not -path '*/node_modules/*'
)

exit "$status"
