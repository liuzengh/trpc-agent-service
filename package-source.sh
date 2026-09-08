#!/usr/bin/env bash
# Export committed source only; never load .env or publish to a remote.
set -euo pipefail
PACKAGE_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PACKAGE_ROOT"
umask 077
[[ $# == 0 ]] || { echo "usage: ./package-source.sh" >&2; exit 2; }
[[ -z "$(git status --porcelain --untracked-files=normal)" ]] || {
  echo "commit or separately preserve workspace changes before packaging" >&2; exit 1;
}
# Defense in depth if a private/generated file was accidentally committed.
while IFS= read -r -d '' package_path; do
  case "$package_path" in
    .env.example|data/README.md) ;;
    .env|.env.*|*/.env|*/.env.*|*.env|bin/*|dist/*|data/*|*.log|coverage.out|coverage.html)
      echo "private/generated path is tracked; packaging refused" >&2; exit 1 ;;
  esac
  [[ ! -L "$package_path" ]] || { echo "tracked symlink requires manual review before packaging" >&2; exit 1; }
done < <(git ls-files -z)
[[ ! -L dist && ( ! -e dist || -d dist ) ]] || { echo "dist must be a real directory" >&2; exit 1; }
mkdir -p dist
PACKAGE_REVISION="$(git rev-parse --short=12 HEAD)"
[[ "$PACKAGE_REVISION" =~ ^[0-9a-f]{12}$ ]] || exit 1
PACKAGE_NAME="trpc-agent-service-$PACKAGE_REVISION.tar.gz"
PACKAGE_TMP="$(mktemp -d "$PACKAGE_ROOT/dist/.package.XXXXXXXX")"
package_cleanup() {
  rm -f -- "$PACKAGE_TMP/source.tar.gz" "$PACKAGE_TMP/source.sha256"
  rmdir -- "$PACKAGE_TMP"
}
trap package_cleanup EXIT
git archive --format=tar.gz --prefix=trpc-agent-service/ --output="$PACKAGE_TMP/source.tar.gz" HEAD
gzip -t "$PACKAGE_TMP/source.tar.gz"
PACKAGE_DIGEST="$(sha256sum "$PACKAGE_TMP/source.tar.gz")"
printf '%s  %s\n' "${PACKAGE_DIGEST%% *}" "$PACKAGE_NAME" > "$PACKAGE_TMP/source.sha256"
# Hard links fail instead of overwriting an existing delivery artifact.
ln "$PACKAGE_TMP/source.tar.gz" "$PACKAGE_ROOT/dist/$PACKAGE_NAME"
ln "$PACKAGE_TMP/source.sha256" "$PACKAGE_ROOT/dist/$PACKAGE_NAME.sha256"
echo "source package: dist/$PACKAGE_NAME"
echo "checksum: dist/$PACKAGE_NAME.sha256"
echo "committed source only; no private runtime data; no push performed"
