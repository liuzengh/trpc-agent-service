#!/bin/sh
set -eu
digest=$(sha256sum /workspace/input.json)
bytes=$(wc -c < /workspace/input.json)
printf '{"bytes":%s,"sha256":"%s"}\n' "$bytes" "${digest%% *}"
