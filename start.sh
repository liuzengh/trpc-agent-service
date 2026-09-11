#!/usr/bin/env bash
# 后台启动 trpc-agent-service；PID 与日志写入 data/。
# 可用环境变量：ADMIN_TOKEN（必须从本地环境或被忽略的 .env 注入）、CONFIG，
# 以及 config 中的端口变量。
set -euo pipefail
cd "$(dirname "$0")"

# Load simple KEY=VALUE entries from the ignored local .env file without
# evaluating it as shell code. Explicitly exported values take precedence.
if [[ -f .env ]]; then
  while IFS='=' read -r env_name env_value; do
    [[ "${env_name}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    env_value="${env_value%$'\r'}"
    if [[ -z "$(printenv "${env_name}" 2>/dev/null)" ]]; then
      export "${env_name}=${env_value}"
    fi
  done <.env
fi

mkdir -p data
pid_file="data/trpc-service.pid"
log_file="data/trpc-service.log"

is_service_pid() {
  local pid="$1" command
  [[ "${pid}" =~ ^[0-9]+$ ]] && ((pid > 1)) || return 1
  kill -0 "${pid}" 2>/dev/null || return 1
  command="$(ps -p "${pid}" -o command= 2>/dev/null || true)"
  [[ "${command}" =~ (^|/)trpc-service([[:space:]]|$) ]]
}

if [[ -f "${pid_file}" ]]; then
  saved_pid="$(cat "${pid_file}")"
  if is_service_pid "${saved_pid}"; then
    echo "trpc-agent-service already running (pid ${saved_pid})"
    echo "run ./stop.sh && ./start.sh to load source or embedded-console changes"
    exit 0
  fi
  rm -f "${pid_file}"
fi

# 始终从当前前后端源码构建，避免 Vite 新页面配上旧 Go 二进制，或旧
# embed.FS 静态资源继续被启动。Go/Vite 都有增量缓存，日常启动开销很小。
./build.sh

# SKILLS_ROOT 指向平台内置技能仓库；置空可关闭技能注入。
if [[ -z "${ADMIN_TOKEN:-}" ]]; then
  echo "ADMIN_TOKEN must be provided by the environment or ignored .env file" >&2
  exit 2
fi
SKILLS_ROOT="${SKILLS_ROOT:-trpcservice/skill/skills}" \
  ADMIN_TOKEN="${ADMIN_TOKEN}" \
  nohup bin/trpc-service -config "${CONFIG:-config/example.yaml}" \
  >>"${log_file}" 2>&1 &
service_pid=$!
echo "${service_pid}" >"${pid_file}"

# Listen failures (most commonly an occupied port) happen immediately. Do not
# report a dead background process as a successful start.
sleep 0.2
for _ in $(seq 1 20); do
  if ! is_service_pid "${service_pid}"; then
    rm -f "${pid_file}"
    echo "trpc-agent-service failed to start; recent log output:" >&2
    # main emits a sanitized failure category on its final line. Avoid dumping
    # unrelated historical audit entries from an append-only log.
    tail -n 1 "${log_file}" >&2
    exit 1
  fi
  sleep 0.1
done
echo "trpc-agent-service started (pid ${service_pid}, log ${log_file})"
