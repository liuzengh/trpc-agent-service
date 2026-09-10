#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

# Formatting is a source admission check, not a scan of build artefacts.  In
# particular, CI's workspace-local disposable GOMODCACHE contains third-party
# source whose historical formatting is outside this repository's control.
# Include tracked files plus non-ignored local Go files so the script remains
# useful before a developer stages a new source file.
go_files=()
while IFS= read -r -d '' file; do
  go_files+=("${file}")
done < <(git ls-files -co --exclude-standard -z -- '*.go')

unformatted=""
if [[ ${#go_files[@]} -gt 0 ]]; then
  unformatted="$(gofmt -l "${go_files[@]}")"
fi
if [[ -n "$unformatted" ]]; then
  echo "Go files must be formatted with gofmt:" >&2
  echo "$unformatted" >&2
  exit 1
fi
