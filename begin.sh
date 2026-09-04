#!/usr/bin/env bash
# =============================================================================
# begin.sh — 一键启动多租户 Agent 平台（全栈 Docker Compose）
#
# 用法：
#   ./begin.sh           启动全栈（构建 + 启动 + 等待就绪 + 打印访问地址）
#   ./begin.sh status    查看各服务运行状态
#   ./begin.sh logs      跟踪后端 + 前端日志（可追加服务名）
#   ./begin.sh down      停止并移除容器（数据卷保留，重启再跑 ./begin.sh）
#
# 前置：本机已安装 Docker Desktop / docker engine（含 compose 插件）。
# 说明：首次运行会拉取基础镜像（mysql/redis/etcd/minio/milvus/otel/jaeger/
#       prometheus）并构建 backend/frontend 镜像，耗时较长，属正常现象。
# =============================================================================
set -euo pipefail

# ---- 输出 ----
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { printf "${GREEN}==>${NC} %s\n" "$*"; }
warn() { printf "${YELLOW}!!${NC} %s\n" "$*"; }
die()  { printf "${RED}!!${NC} %s\n" "$*" >&2; exit 1; }

# ---- 定位 deployments 目录 ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/docker-compose.yml" ]]; then
    DEPLOY_DIR="$SCRIPT_DIR"
else
    DEPLOY_DIR="$SCRIPT_DIR/deployments"
fi
[[ -f "$DEPLOY_DIR/docker-compose.yml" ]] || die "未找到 docker-compose.yml（脚本应位于项目根目录或 deployments/）"
cd "$DEPLOY_DIR"

# ---- 依赖检查 ----
command -v docker >/dev/null 2>&1 || die "未检测到 docker，请先安装 Docker Desktop / docker engine"
if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
else
    die "未检测到 docker compose（需要 docker compose 插件，或 docker-compose）"
fi

# ---- 就绪探测 ----
wait_http() { # $1=url  $2=名称  $3=服务名
    local url=$1 name=$2 svc=$3 n=90
    while :; do
        if curl -fsS --connect-timeout 2 "$url" >/dev/null 2>&1; then
            info "$name 已就绪"
            return 0
        fi
        n=$((n - 1))
        if [[ $n -le 0 ]]; then
            warn "$name 未在预期时间内就绪（$url），查看日志：${COMPOSE[*]} logs $svc"
            return 1
        fi
        sleep 2
    done
}

# ---- 命令分发 ----
case "${1:-up}" in
    up)
        [[ -f .env ]] || { info "未找到 .env，从 .env.example 生成默认配置"; cp .env.example .env; }

        info "构建并启动全栈（首次需拉取镜像 + 构建 backend/frontend，请耐心等待）..."
        "${COMPOSE[@]}" up -d --build

        info "等待后端 API 就绪 ..."
        wait_http "http://127.0.0.1:8080/healthz" "后端 API" backend || true
        info "等待前端管理台就绪 ..."
        wait_http "http://127.0.0.1:5173/" "前端管理台" frontend || true

        echo
        printf "${GREEN}==================== 启动完成 ====================${NC}\n"
        printf "  前端管理台     http://localhost:5173\n"
        printf "  后端 API       http://localhost:8080\n"
        printf "  Jaeger 追踪    http://localhost:16686\n"
        printf "  Prometheus     http://localhost:9090\n"
        printf "  MinIO 控制台   http://localhost:9001\n"
        printf "${GREEN}=================================================${NC}\n"
        echo
        info "查看日志：${COMPOSE[*]} logs -f backend frontend"
        info "停止服务：./begin.sh down"
        ;;
    down)
        info "停止并移除容器（数据卷保留）..."
        "${COMPOSE[@]}" down
        info "已停止。重新启动：./begin.sh"
        ;;
    status)
        "${COMPOSE[@]}" ps
        ;;
    logs)
        shift || true
        "${COMPOSE[@]}" logs -f "${@:-backend frontend}"
        ;;
    *)
        die "未知参数：${1:-}（支持 up | down | status | logs）"
        ;;
esac
