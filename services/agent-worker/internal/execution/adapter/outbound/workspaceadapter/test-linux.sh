#!/bin/sh
# Run only against an explicitly selected disposable image and seccomp profile.
set -eu
[ "$#" = 2 ] || { echo 'usage: test-linux.sh IMAGE SECCOMP_JSON' >&2; exit 2; }
image=$1
profile=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
case $(docker info --format '{{.Architecture}}') in aarch64|arm64) arch=arm64;; x86_64|amd64) arch=amd64;; *) exit 2;; esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
CGO_ENABLED=0 GOOS=linux GOARCH=$arch go test -c -o "$work/test" ./services/agent-worker/internal/execution/adapter/outbound/workspaceadapter
mkdir "$work/private"
printf 'owned-isolation-canary' > "$work/private/marker"
docker run --rm --read-only --user 65532:65532 --cap-drop ALL \
 --security-opt seccomp="$profile" --security-opt systempaths=unconfined \
 --tmpfs /workspace:rw,uid=65532,gid=65532,mode=0700 --tmpfs /tmp:rw,mode=1777 \
 --mount type=bind,src="$work/test",dst=/adapter-test,readonly \
 --mount type=bind,src="$work/private",dst=/run,readonly \
 --mount type=bind,src="$work/private",dst=/config,readonly \
 --mount type=bind,src="$work/private",dst=/credentials,readonly \
 --mount type=bind,src="$work/private",dst=/app,readonly \
 -e WORKSPACE_LINUX_INTEGRATION=1 --entrypoint /adapter-test "$image" -test.v -test.count=3
