#!/usr/bin/env bash
# 浏览器关键路径 E2E：企业登录、控制台导航、应用管理和异步会话状态。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

BASE="${E2E_BASE:-http://127.0.0.1:18080}"
FAKE_PORT="${WECOM_FAKE_PORT:-10086}"
UI_PORT="${UI_E2E_PORT:-5178}"
UI_BASE="${UI_E2E_BASE:-http://127.0.0.1:${UI_PORT}}"
ARTIFACTS="$ROOT/tests/browser/artifacts"
mkdir -p "$ARTIFACTS"

ENV_FILE="${E2E_ENV_FILE:-$ROOT/data/platform.env}"
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

stop_service() {
  if [[ -f "$ROOT/data/trpc-service.pid" ]]; then
    local pid
    pid="$(cat "$ROOT/data/trpc-service.pid")"
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    rm -f "$ROOT/data/trpc-service.pid"
  fi
  sleep 1
}

start_with() { # $1=mode(mock|wecom) $2=LOGIN_SESSION_TTL
  stop_service
  unset LOGIN_PROVIDER LOGIN_PROVIDERS_FILE LOGIN_PROVIDERS_JSON LOGIN_MOCK_ENABLED
  unset SYSTEM_ADMIN_PROVIDER_ID SYSTEM_ADMIN_SUBJECT_ID WECOM_LOGIN_SECRET
  export LOGIN_SESSION_TTL="$2"
  export LOGIN_SECURE_COOKIE=false
  export LOGIN_STATE_SECRET=e2e-state-secret
  export LOGIN_CALLBACK_URL="$BASE/api/v1/auth/callback"
  if [[ "$1" == "mock" ]]; then
    export LOGIN_PROVIDER=mock
    export SYSTEM_ADMIN_PROVIDER_ID=mock
    export SYSTEM_ADMIN_SUBJECT_ID=mock-admin
  else
    export SYSTEM_ADMIN_PROVIDER_ID=wecom-e2e
    export SYSTEM_ADMIN_SUBJECT_ID=zhangsan
    export WECOM_LOGIN_SECRET=fake-secret
    export LOGIN_PROVIDERS_JSON="{\"providers\":[{\"id\":\"wecom-e2e\",\"type\":\"wecom\",\"display_name\":\"企业微信\",\"corp_id\":\"wwfake\",\"agent_id\":1000002,\"secret_ref\":\"env:WECOM_LOGIN_SECRET\",\"auth_base_url\":\"http://127.0.0.1:$FAKE_PORT\",\"api_base_url\":\"http://127.0.0.1:$FAKE_PORT\"}]}"
  fi
  nohup "$ROOT/bin/trpc-service" serve >"$ARTIFACTS/service.log" 2>&1 &
  echo $! > "$ROOT/data/trpc-service.pid"
  echo "[run] 启动服务 mode=$1 TTL=$2 pid=$(cat "$ROOT/data/trpc-service.pid")"
  local i
  for i in $(seq 1 60); do
    if ! kill -0 "$(cat "$ROOT/data/trpc-service.pid")" 2>/dev/null; then
      echo "[run] E2E 服务进程提前退出，日志见 $ARTIFACTS/service.log" >&2
      return 1
    fi
    if curl -fs -o /dev/null "$BASE/healthz" 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  echo "[run] 服务未能就绪，日志见 $ARTIFACTS/service.log" >&2
  exit 1
}

cleanup() {
  stop_service
  if [[ -n "${FAKE_PID:-}" ]]; then kill "$FAKE_PID" 2>/dev/null || true; fi
  if [[ -n "${MODEL_PID:-}" ]]; then kill "$MODEL_PID" 2>/dev/null || true; fi
  if [[ -n "${UI_PID:-}" ]]; then kill "$UI_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT

echo "[run] 构建服务（含前端）..."
./build.sh
if [[ ! -d "$ROOT/tests/browser/node_modules/playwright" ]]; then
  npm --prefix "$ROOT/tests/browser" ci --ignore-scripts --no-audit --no-fund
fi

node "$ROOT/scripts/mock-openai.mjs" >"$ARTIFACTS/mock-openai.log" 2>&1 &
MODEL_PID=$!
node "$ROOT/tests/browser/fake-wecom.mjs" >"$ARTIFACTS/fake-wecom.log" 2>&1 &
FAKE_PID=$!
sleep 1

echo "[run] 场景 1/7: Mock 测试登录"
start_with mock 24h
E2E_BASE="$BASE" node "$ROOT/tests/browser/e2e-login.mjs" mock

echo "[run] 场景 2/7: 企业微信扫码登录"
start_with wecom 24h
E2E_BASE="$BASE" node "$ROOT/tests/browser/e2e-login.mjs" wecom

echo "[run] 场景 3/7: 会话过期"
start_with wecom 4s
E2E_BASE="$BASE" node "$ROOT/tests/browser/e2e-login.mjs" expiry

stop_service
npm --prefix "$ROOT/webui" run preview -- --host 127.0.0.1 --port "$UI_PORT" \
  >"$ARTIFACTS/vite-preview.log" 2>&1 &
UI_PID=$!
for _ in $(seq 1 60); do
  if curl -fs -o /dev/null "$UI_BASE/console/" 2>/dev/null; then break; fi
  sleep 0.5
done
if ! curl -fs -o /dev/null "$UI_BASE/console/" 2>/dev/null; then
  echo "[run] 控制台预览未能就绪，日志见 $ARTIFACTS/vite-preview.log" >&2
  exit 1
fi

echo "[run] 场景 4/7: 控制台壳层、权限拒绝与响应式导航"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-ui-shell.mjs"

echo "[run] 场景 5/7: 应用创建、编辑、回滚冲突与失败反馈"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-applications.mjs"

echo "[run] 场景 6/7: 租户/会话切换、SSE 重连与陈旧响应抑制"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-chat-state.mjs"

echo "[run] 场景 7/7: 全部一级目的地与平台数据限制状态"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-ui-destinations.mjs"

echo "[run] 浏览器关键路径 E2E 全部通过（7/7）"
