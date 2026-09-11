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
SERVICE_PID_FILE="$ARTIFACTS/trpc-service.pid"
E2E_CONFIG="$ARTIFACTS/platform.e2e.json"

ENV_FILE="${E2E_ENV_FILE:-$ROOT/data/platform.env}"
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Browser E2E must never own the developer service on :8080. The local
# platform.env points at configs/platform.json, whose listen address is :8080;
# derive an E2E-only copy bound to BASE instead of mutating or stopping it.
SOURCE_CONFIG_PATH="${CONFIG_PATH:-$ROOT/configs/platform.json}"
python3 - "$SOURCE_CONFIG_PATH" "$BASE" "$E2E_CONFIG" <<'PY'
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

source = Path(sys.argv[1])
base = urlparse(sys.argv[2])
target = Path(sys.argv[3])
if not source.is_absolute():
    source = Path.cwd() / source
if base.scheme not in {"http", "https"} or base.hostname not in {"127.0.0.1", "localhost"} or not base.port:
    raise SystemExit(f"E2E_BASE must use localhost with an explicit port: {sys.argv[2]}")
config = json.loads(source.read_text(encoding="utf-8"))
config["service"]["listen_address"] = f"127.0.0.1:{base.port}"
target.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
export CONFIG_PATH="$E2E_CONFIG"

# Keep browser E2E state isolated from the developer runtime. CI already uses
# trpc_agent_test; local runs must do the same instead of mutating trpc_agent.
if [[ -n "${E2E_DATABASE_URL:-}" ]]; then
  export DATABASE_URL="$E2E_DATABASE_URL"
elif ! python3 - "$DATABASE_URL" <<'PYDB'
import sys
from urllib.parse import urlsplit

name = urlsplit(sys.argv[1]).path.rsplit('/', 1)[-1]
raise SystemExit(0 if name.endswith('_test') else 1)
PYDB
then
  echo "[run] 拒绝在开发数据库上运行浏览器 E2E；请设置 E2E_DATABASE_URL 指向独立测试库" >&2
  exit 1
fi
export KAFKA_TOPIC="${E2E_KAFKA_TOPIC:-${KAFKA_TOPIC:-agent.inbound.v1}.browser-e2e}"
export KAFKA_GROUP_ID="${E2E_KAFKA_GROUP_ID:-${KAFKA_GROUP_ID:-trpc-agent-worker}.browser-e2e}"

stop_service() {
  if [[ -f "$SERVICE_PID_FILE" ]]; then
    local pid
    pid="$(cat "$SERVICE_PID_FILE")"
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    rm -f "$SERVICE_PID_FILE"
  fi
  sleep 1
}

start_with() { # $1=mode(mock|wecom) $2=LOGIN_SESSION_TTL
  stop_service
  if python3 - "$BASE" <<'PY'
import socket
import sys
from urllib.parse import urlparse

target = urlparse(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(0.2)
    raise SystemExit(0 if sock.connect_ex((target.hostname, target.port)) == 0 else 1)
PY
  then
    echo "[run] E2E 地址 $BASE 已被其他进程占用；为避免误测未知服务，本次拒绝启动" >&2
    return 1
  fi
  unset LOGIN_PROVIDER LOGIN_PROVIDERS_FILE LOGIN_PROVIDERS_JSON LOGIN_MOCK_ENABLED
  unset SYSTEM_ADMIN_PROVIDER_ID SYSTEM_ADMIN_SUBJECT_ID WECOM_LOGIN_SECRET
  unset SYSTEM_ADMIN_LOCAL_USERNAME SYSTEM_ADMIN_LOCAL_PASSWORD SYSTEM_ADMIN_LOCAL_DISPLAY_NAME SYSTEM_ADMIN_LOCAL_EMAIL
  export LOGIN_SESSION_TTL="$2"
  export LOGIN_SECURE_COOKIE=false
  export LOGIN_STATE_SECRET=e2e-state-secret
  export LOGIN_CALLBACK_URL="$BASE/api/v1/auth/callback"
  if [[ "$1" == "mock" ]]; then
    export LOGIN_PROVIDER=mock
    export SYSTEM_ADMIN_LOCAL_USERNAME="${E2E_SYSTEM_ADMIN_USERNAME:-e2e-root}"
    export SYSTEM_ADMIN_LOCAL_PASSWORD="${E2E_SYSTEM_ADMIN_PASSWORD:-E2e-root-password-A9!}"
    export SYSTEM_ADMIN_LOCAL_DISPLAY_NAME="E2E System Admin"
  else
    export WECOM_LOGIN_SECRET=fake-secret
    export LOGIN_PROVIDERS_JSON="{\"providers\":[{\"id\":\"wecom-e2e\",\"type\":\"wecom\",\"display_name\":\"企业微信\",\"corp_id\":\"wwfake\",\"agent_id\":1000002,\"secret_ref\":\"env:WECOM_LOGIN_SECRET\",\"auth_base_url\":\"http://127.0.0.1:$FAKE_PORT\",\"api_base_url\":\"http://127.0.0.1:$FAKE_PORT\"}]}"
  fi
  nohup "$ROOT/bin/trpc-service" serve >"$ARTIFACTS/service.log" 2>&1 &
  echo $! > "$SERVICE_PID_FILE"
  echo "[run] 启动服务 mode=$1 TTL=$2 pid=$(cat "$SERVICE_PID_FILE")"
  local i
  for i in $(seq 1 60); do
    if ! kill -0 "$(cat "$SERVICE_PID_FILE")" 2>/dev/null; then
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
echo "[run] 安装当前数据库基线..."
"$ROOT/bin/trpc-service" migrate
if [[ ! -d "$ROOT/tests/browser/node_modules/playwright" ]]; then
  npm --prefix "$ROOT/tests/browser" ci --ignore-scripts --no-audit --no-fund
fi

node "$ROOT/tests/mock/mock-openai.mjs" >"$ARTIFACTS/mock-openai.log" 2>&1 &
MODEL_PID=$!
node "$ROOT/tests/browser/fake-wecom.mjs" >"$ARTIFACTS/fake-wecom.log" 2>&1 &
FAKE_PID=$!
sleep 1

echo "[run] 场景 1/14: Mock 测试登录"
start_with mock 24h
E2E_BASE="$BASE" node "$ROOT/tests/browser/e2e-login.mjs" mock

echo "[run] 场景 2/14: 真实平台产品链路"
E2E_BASE="$BASE" node "$ROOT/tests/browser/e2e-real-platform.mjs"

echo "[run] 场景 3/14: 企业微信扫码登录"
start_with wecom 24h
E2E_BASE="$BASE" node "$ROOT/tests/browser/e2e-login.mjs" wecom

echo "[run] 场景 4/14: 会话过期"
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

echo "[run] 场景 5/14: 飞书二维码登录状态机"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-feishu-qr-login.mjs"

echo "[run] 场景 6/14: 普通成员导航与越权深链保护"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-role-navigation.mjs"

echo "[run] 场景 7/14: 个人偏好与登录设置交互"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-personal-settings.mjs"

echo "[run] 场景 8/14: 成员、候选用户与平台用户分页"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-admin-lists.mjs"

echo "[run] 场景 9/14: 机器人配置表单控件"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-bot-form-controls.mjs"

echo "[run] 场景 10/14: 页面错误恢复与 URL 状态保持"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-error-recovery.mjs"

echo "[run] 场景 11/14: 控制台壳层、权限拒绝与响应式导航"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-ui-shell.mjs"

echo "[run] 场景 12/14: 应用创建、编辑、回滚冲突与失败反馈"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-applications.mjs"

echo "[run] 场景 13/14: 租户/会话切换、SSE 重连与陈旧响应抑制"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-chat-state.mjs"

echo "[run] 场景 14/14: 全部一级目的地与平台数据限制状态"
E2E_BASE="$UI_BASE" node "$ROOT/tests/browser/e2e-ui-destinations.mjs"

echo "[run] 浏览器关键路径 E2E 全部通过（14/14）"
