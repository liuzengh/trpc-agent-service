#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

required_module="trpc.group/trpc-go/trpc-agent-go"
required_version="v1.11.2"
required_qdrant_module="trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
required_qdrant_version="v1.11.0"
required_a2a_module="trpc.group/trpc-go/trpc-a2a-go"
required_a2a_version="v0.2.6-0.20260721084546-18c8244d0acb"
required_feishu_module="github.com/larksuite/oapi-sdk-go/v3"
required_feishu_version="v3.10.0"
required_aws_module="github.com/aws/aws-sdk-go-v2"
required_aws_version="v1.46.0"
required_aws_config_module="github.com/aws/aws-sdk-go-v2/config"
required_aws_config_version="v1.33.3"
required_aws_credentials_module="github.com/aws/aws-sdk-go-v2/credentials"
required_aws_credentials_version="v1.20.3"
required_s3_module="github.com/aws/aws-sdk-go-v2/service/s3"
required_s3_version="v1.111.0"
required_smithy_module="github.com/aws/smithy-go"
required_smithy_version="v1.28.1"

actual_version="$(go list -m -f '{{.Version}}' "$required_module")"
if [[ "$actual_version" != "$required_version" ]]; then
  echo "dependency baseline mismatch: $required_module=$actual_version, want $required_version" >&2
  exit 1
fi

# The service is permitted to consume only the official v1.11.2 module. A
# replace can preserve the displayed version while silently compiling a fork,
# so inspect the resolved module metadata as well as its version.
if go list -m -json "$required_module" | grep -q '"Replace"'; then
  echo "forbidden framework replacement: $required_module must resolve to the official $required_version module" >&2
  exit 1
fi

if grep -Eq '^replace[[:space:]].*trpc\.group/trpc-go/trpc-agent-go' go.mod; then
  echo "forbidden go.mod replacement for $required_module" >&2
  exit 1
fi

# Qdrant's official implementation is published as a public leaf module. The
# root SDK has no v1.11.2 leaf release, so pin its matching public v1.11.0
# module and reject replacements exactly as for the root SDK.
actual_qdrant_version="$(go list -m -f '{{.Version}}' "$required_qdrant_module")"
if [[ "$actual_qdrant_version" != "$required_qdrant_version" ]]; then
  echo "dependency baseline mismatch: $required_qdrant_module=$actual_qdrant_version, want $required_qdrant_version" >&2
  exit 1
fi

if go list -m -json "$required_qdrant_module" | grep -q '"Replace"'; then
  echo "forbidden framework replacement: $required_qdrant_module must resolve to the official $required_qdrant_version module" >&2
  exit 1
fi

actual_feishu_version="$(go list -m -f '{{.Version}}' "$required_feishu_module")"
if [[ "$actual_feishu_version" != "$required_feishu_version" ]]; then
  echo "dependency baseline mismatch: $required_feishu_module=$actual_feishu_version, want $required_feishu_version" >&2
  exit 1
fi

for baseline in \
  "$required_a2a_module $required_a2a_version" \
  "$required_aws_module $required_aws_version" \
  "$required_aws_config_module $required_aws_config_version" \
  "$required_aws_credentials_module $required_aws_credentials_version" \
  "$required_s3_module $required_s3_version" \
  "$required_smithy_module $required_smithy_version"; do
  read -r module version <<<"$baseline"
  actual="$(go list -m -f '{{.Version}}' "$module")"
  if [[ "$actual" != "$version" ]]; then
    echo "dependency baseline mismatch: $module=$actual, want $version" >&2
    exit 1
  fi
done

if go list -m all | awk '{print $1}' | grep -Eq '^trpc\.group/trpc-go/trpc-agent-go/openclaw$'; then
  echo "forbidden module dependency: trpc-agent-go/openclaw" >&2
  exit 1
fi

forbidden_imports='"trpc\.group/trpc-go/trpc-agent-go/(openclaw|internal)(/|\")'

scan_forbidden_imports() {
  if command -v rg >/dev/null 2>&1; then
    rg -n --glob '*.go' "$forbidden_imports" cmd trpcservice
    return
  fi

  local found=1
  local file
  while IFS= read -r -d '' file; do
    if grep -nEH "$forbidden_imports" "$file"; then
      found=0
    fi
  done < <(find cmd trpcservice -type f -name '*.go' -print0)
  return "$found"
}

if scan_forbidden_imports; then
  echo "service code must not import trpc-agent-go/openclaw or trpc-agent-go/internal" >&2
  exit 1
fi

if ! grep -Eq '^go 1\.25([[:space:]]|\.|$)' go.mod; then
  echo "go.mod must retain the Go 1.25 baseline" >&2
  exit 1
fi
