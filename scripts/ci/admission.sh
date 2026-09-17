#!/usr/bin/env bash
# Runs the repository-only admission gate with disposable Go caches. The
# backend smoke has its own Docker job because it needs a Docker daemon.
set -euo pipefail

case "${1:-}" in
  ""|--race|--demo) ;;
  *) echo "usage: $0 [--race|--demo]" >&2; exit 2 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

go_version="$(go env GOVERSION)"
if [[ "${go_version}" != go1.25.* ]]; then
  echo "Go 1.25.x is required for the CI compatibility gate; found ${go_version}" >&2
  exit 2
fi

# A GitHub-hosted runner discards its workspace after the job.  Keep the CI
# cache beneath that workspace and do not manually remove it: a subprocess can
# legitimately leave read-only module files, and cache cleanup must never hide
# the actual build/test result.  Local invocations retain the disposable
# system-temp cache and best-effort cleanup below.
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  cache_parent="${GITHUB_WORKSPACE:-${repo_root}}/.trpc-ci-cache"
  mkdir -p -- "${cache_parent}"
  cache_root="$(mktemp -d "${cache_parent%/}/run.XXXXXX")"
else
  temp_parent="${TMPDIR:-/tmp}"
  cache_root="$(mktemp -d "${temp_parent%/}/trpc-ci-go.XXXXXX")"
fi
cleanup() {
  local status=$?
  trap - EXIT
  if [[ "${GITHUB_ACTIONS:-}" != "true" ]] && ! rm -rf -- "${cache_root}"; then
    echo "warning: could not fully remove disposable Go cache ${cache_root}" >&2
  fi
  exit "${status}"
}
trap cleanup EXIT
export GOMODCACHE="${cache_root}/mod"
export GOCACHE="${cache_root}/build"
export GOPATH="${cache_root}/path"
mkdir -p "${GOMODCACHE}" "${GOCACHE}" "${GOPATH}"

go mod download
go mod verify

if [[ "${1:-}" == "--race" ]]; then
  go test -count=1 -race ./...
  exit 0
fi

while IFS= read -r -d '' script; do
  bash -n "${script}"
done < <(find scripts -type f -name '*.sh' -print0)

bash scripts/ci/check-format.sh
bash scripts/ci/check-dependency-boundaries.sh
bash scripts/ci/check-doc-links.sh
go build ./...
go vet ./...
go test -count=1 ./...
git diff --check

if [[ "${1:-}" == "--demo" ]]; then
  echo "Running credential-free final acceptance path"
  bash scripts/compose/quickstart.sh --demo
fi
