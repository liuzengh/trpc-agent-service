#!/usr/bin/env bash
# =============================================================================
# begin.sh — 一键启动多租户 Agent 平台（全栈 Docker Compose）
#
# 用法：
#   ./begin.sh           启动全栈（构建 + 启动 + 等待就绪 + 打印访问地址）
#   ./begin.sh core      只启动核心四件（mysql/redis/backend/frontend）
#   ./begin.sh reset     重置数据库后启动（删 MySQL 数据卷 → 重跑 init/*.sql）
#   ./begin.sh status    查看各服务运行状态
#   ./begin.sh logs      跟踪后端 + 前端日志（可追加服务名）
#   ./begin.sh down      停止并移除容器（数据卷保留，重启再跑 ./begin.sh）
#
# 说明：可选服务由 compose profiles 分组（vector=Milvus 向量库、artifacts=
#       制品 MinIO、observability=OTel/Jaeger/Prometheus）；.env 里
#       COMPOSE_PROFILES=full 默认全开，等价于旧行为。
#
# 前置：本机已安装 Docker Desktop / docker engine（含 compose 插件）。
# 说明：首次运行会拉取基础镜像（mysql/redis/etcd/minio/milvus/otel/jaeger/
#       prometheus）并构建 backend/frontend 镜像，耗时较长，属正常现象。
#
# 数据库：deployments/mysql/init/*.sql 按文件名顺序执行，但 **只在 MySQL 数据卷
#       首次创建时执行**（Docker 官方镜像行为）。因此：
#         - 首次部署：./begin.sh 即可，表会自动建好；
#         - 表结构更新后：./begin.sh reset（或手动删卷），否则新列不会出现；
#         - 想连数据一起清空重来：./begin.sh reset。
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

# wait_mysql 用容器内 mysqladmin 探测，确保 init/*.sql 全部跑完（healthcheck 只在
# 初始化前就绪，随后 Docker 才执行 initdb 脚本，中间有窗口期）。
wait_mysql() {
    local n=120
    info "等待 MySQL 完成建库建表（init/*.sql）..."
    while :; do
        if "${COMPOSE[@]}" exec -T mysql mysqladmin ping -h 127.0.0.1 --silent >/dev/null 2>&1; then
            # 再确认表已建出，避免在 initdb 尚未结束时误判就绪。
            local db tables
            db="${MYSQL_DATABASE:-trpc_agent_service}"
            tables=$("${COMPOSE[@]}" exec -T mysql mysql -uroot -p"${MYSQL_ROOT_PASSWORD:-trpc123}" -N -B \
                -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='${db}'" 2>/dev/null | tr -d '\r' || echo 0)
            if [[ "${tables:-0}" -ge 20 ]]; then
                info "MySQL 就绪（${db}: ${tables} 张表）"
                return 0
            fi
        fi
        n=$((n - 1))
        if [[ $n -le 0 ]]; then
            warn "MySQL 建表未在预期时间内完成，查看日志：${COMPOSE[*]} logs mysql"
            return 1
        fi
        sleep 2
    done
}

print_addresses() {
    echo
    printf "${GREEN}==================== 启动完成 ====================${NC}\n"
    printf "  前端管理台     http://localhost:5173\n"
    printf "  后端 API       http://localhost:8080\n"
    printf "  Jaeger 追踪    http://localhost:16686\n"
    printf "  Prometheus     http://localhost:9090\n"
    printf "  MinIO 控制台   http://localhost:9001\n"
    printf "${GREEN}=================================================${NC}\n"
    echo
}

start_stack() {
    [[ -f .env ]] || { info "未找到 .env，从 .env.example 生成默认配置"; cp .env.example .env; }

    # Compose profiles: .env 的 COMPOSE_PROFILES=full 会拉起全部可选服务
    # （向量库 Milvus / 制品 MinIO / 观测栈）。第一条参数 core 时仅起核心四件
    # （mysql/redis/backend/frontend），适合只需要管理台+对话的最小验证。
    if [[ "${PROFILE_ARG:-}" == "core" ]]; then
        export COMPOSE_PROFILES=""
        info "核心模式：仅启动 mysql/redis/backend/frontend（无 Milvus/MinIO/观测栈）"
    fi

    info "构建并启动全栈（首次需拉取镜像 + 构建 backend/frontend，请耐心等待）..."
    "${COMPOSE[@]}" up -d --build

    wait_mysql || true
    info "等待后端 API 就绪 ..."
    wait_http "http://127.0.0.1:8080/healthz" "后端 API" backend || true
    info "等待前端管理台就绪 ..."
    wait_http "http://127.0.0.1:5173/" "前端管理台" frontend || true

    # The backend bootstraps the configured owner before serving requests.
    info "管理员账号由后端启动时自动初始化..."
    local ADMIN_TENANT="${ADMIN_TENANT_ID:-t-demo}"
    local ADMIN_USER="${ADMIN_USER_ID:-admin}"
    local ADMIN_PASS="${ADMIN_PASSWORD:-admin123}"

    print_addresses
    info "管理员账号：tenant=${ADMIN_TENANT}, user=${ADMIN_USER}, pass=${ADMIN_PASS}"
    echo
    info "查看日志：${COMPOSE[*]} logs -f backend frontend"
    info "停止服务：./begin.sh down"
    info "重置数据库：./begin.sh reset"
}

# ---- 命令分发 ----
case "${1:-up}" in
    up)
        PROFILE_ARG="${2:-}"
        start_stack
        ;;
    core)
        PROFILE_ARG="core"
        start_stack
        ;;
    reset)
        warn "将删除 MySQL 数据卷（所有租户/会话/审计数据丢失）并重跑 mysql/init/*.sql"
        info "停止容器 ..."
        "${COMPOSE[@]}" down
        # 卷名 = <compose 项目名>_mysql-data；项目名默认取目录名，可被
        # COMPOSE_PROJECT_NAME 覆盖，因此两者都试。
        proj="${COMPOSE_PROJECT_NAME:-$(basename "$DEPLOY_DIR")}"
        removed=0
        for v in "${proj}_mysql-data" "$(basename "$DEPLOY_DIR")_mysql-data"; do
            if docker volume inspect "$v" >/dev/null 2>&1; then
                docker volume rm "$v" >/dev/null && { info "已删除数据卷 $v"; removed=1; }
            fi
        done
        [[ $removed -eq 1 ]] || warn "未找到 MySQL 数据卷（可能尚未创建），本次将直接初始化"
        start_stack
        ;;
    down)
        info "停止所有容器（数据卷保留，重启更快）..."
        "${COMPOSE[@]}" stop
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
        die "未知参数：${1:-}（支持 up | reset | down | status | logs）"
        ;;
esac
