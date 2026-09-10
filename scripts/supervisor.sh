#!/bin/bash
# scripts/supervisor.sh - 一键启动 + 可视化验收向导
set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 配置
APP_PORT=${APP_PORT:-8080}
MYSQL_PORT=${MYSQL_PORT:-3307}
REDIS_PORT=${REDIS_PORT:-6379}
FRONTEND_PORT=${FRONTEND_PORT:-5173}
JAEGER_PORT=${JAEGER_PORT:-16686}
PROM_PORT=${PROM_PORT:-9090}

# 查找 docker compose
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo -e "${RED}❌ 未找到 docker compose，请先安装 Docker。${NC}"
    exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR="$SCRIPT_DIR/../deployments"

# 打印标题
print_header() {
    echo -e "\n${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    printf "${CYAN}║${NC} %-58s ${CYAN}║${NC}\n" "$1"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}\n"
}

print_step() {
    echo -e "${YELLOW}[$1/5]${NC} $2"
}

# 等待 URL 就绪
wait_for_url() {
    local url=$1
    local name=$2
    local max_attempts=${3:-90}
    local attempt=0

    echo -n "  等待 $name 就绪..."
    while [ $attempt -lt $max_attempts ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null | grep -q "200"; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    echo -e " ${RED}✗ (超时)${NC}"
    return 1
}

# 检查 Docker 运行状态
print_header "trpc-agent-service 验收向导"
print_step 1 "检查 Docker 环境..."

if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker 未运行。请先启动 Docker Desktop。${NC}"
    exit 1
fi
echo -e "   ${GREEN}✓${NC} Docker 运行正常"

# 检查端口占用
echo -n "   检查端口占用..."
if lsof -Pi :$APP_PORT -sTCP:LISTEN -t >/dev/null 2>&1 || \
   lsof -Pi :$MYSQL_PORT -sTCP:LISTEN -t >/dev/null 2>&1 || \
   lsof -Pi :$REDIS_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e " ${YELLOW}⚠${NC}"
    echo -e "   ${YELLOW}警告: 以下端口已被占用:${NC}"
    lsof -Pi :$APP_PORT -sTCP:LISTEN 2>/dev/null | awk 'NR>1{print "   - 端口 '$APP_PORT' 被 PID "$1" 占用"}' || true
    lsof -Pi :$MYSQL_PORT -sTCP:LISTEN 2>/dev/null | awk 'NR>1{print "   - 端口 '$MYSQL_PORT' 被 PID "$1" 占用"}' || true
    lsof -Pi :$REDIS_PORT -sTCP:LISTEN 2>/dev/null | awk 'NR>1{print "   - 端口 '$REDIS_PORT' 被 PID "$1" 占用"}' || true
    read -p "是否继续? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "已取消。"
        exit 1
    fi
else
    echo -e " ${GREEN}✓${NC}"
fi

# 启动服务
print_step 2 "启动 Docker Compose 服务..."

if [ -d "$DEPLOY_DIR" ]; then
    cd "$DEPLOY_DIR"
else
    echo -e "${RED}❌ 找不到 deployments 目录: $DEPLOY_DIR${NC}"
    exit 1
fi

echo "   执行: $COMPOSE_CMD up -d --build"
$COMPOSE_CMD up -d --build

# 等待服务就绪
print_step 3 "等待服务就绪..."

wait_for_url "http://127.0.0.1:$APP_PORT/healthz" "后端 API ($APP_PORT)" 90
wait_for_url "http://127.0.0.1:$FRONTEND_PORT/" "前端页面 ($FRONTEND_PORT)" 90

# 验证数据库初始化
print_step 4 "验证数据库初始化..."

if [ -x "$SCRIPT_DIR/verify_init.sh" ]; then
    bash "$SCRIPT_DIR/verify_init.sh" 2>&1 | sed 's/^/   /'
else
    echo -e "   ${YELLOW}⚠${NC} 找不到 verify_init.sh，跳过数据库验证"
fi

# 显示访问地址
print_step 5 "获取访问地址..."

echo -e "\n${GREEN}✨ 服务已启动！访问地址：${NC}\n"
echo -e "   ${CYAN}后端 API:${NC}     http://127.0.0.1:$APP_PORT"
echo -e "   ${CYAN}管理前端:${NC}     http://127.0.0.1:$FRONTEND_PORT"
echo -e "   ${CYAN}Jaeger 追踪:${NC}   http://127.0.0.1:$JAEGER_PORT"
echo -e "   ${CYAN}Prometheus:${NC}    http://127.0.0.1:$PROM_PORT"
echo -e "   ${CYAN}健康检查:${NC}      http://127.0.0.1:$APP_PORT/healthz"
echo ""
echo -e "${YELLOW}查看日志:${NC}"
echo -e "   $COMPOSE_CMD logs -f backend frontend"
echo ""
echo -e "${YELLOW}停止服务:${NC}"
echo -e "   $COMPOSE_CMD down"
echo ""

print_header "验收检查清单"

echo -e "${CYAN}基础功能：${NC}"
echo -e "  [ ] 1. 后端 /healthz 返回 200"
echo -e "  ${CYAN}登录权限：${NC}"
echo -e "  [ ] 2. POST /auth/register 创建用户 (tenant_id, user_id, password)"
echo -e "  [ ] 3. POST /auth/login 获取 JWT token"
echo -e "  [ ] 4. 携带 Bearer token 访问 /tenants (200 OK)"
echo -e "  [ ] 5. 无 token 访问 /tenants (401 Unauthorized)"
echo -e "  ${CYAN}业务功能：${NC}"
echo -e "  [ ] 6. 创建 tenant → 创建 agent → publish"
echo -e "  [ ] 7. 配置 channel_binding (wecom/feishu)"
echo -e "  [ ] 8. 企业微信发送消息给 Bot，收到回复"
echo -e "  [ ] 9. 飞书发送消息给 Bot，收到回复"
echo -e "  ${CYAN}流式/卡片：${NC}"
echo -e "  [ ] 10. 企业微信收到流式分片回复"
echo -e "  [ ] 11. 飞书收到完整文本回复（降级模式）"
echo -e "  [ ] 12. 发送卡片消息"
echo -e "  ${CYAN}监控审计：${NC}"
echo -e "  [ ] 13. Jaeger 显示完整 trace (im.callback → agent.run → im.reply)"
echo -e "  [ ] 14. /audit 返回审计记录"
echo -e "  [ ] 15. /usage 返回用量统计"
echo -e "  [ ] 16. /dlq 显示死信队列（如有失败消息）"
echo ""

echo -e "${GREEN}提示: 运行 $SCRIPT_DIR/verify_init.sh 可单独验证数据库状态${NC}"
echo ""
