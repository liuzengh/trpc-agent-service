#!/bin/bash
# scripts/verify_init.sh - 验证数据库初始化状态
set -e

echo "=== trpc-agent-service 数据库初始化验证 ==="

# 配置
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-trpc123}
MYSQL_DATABASE=${MYSQL_DATABASE:-trpc_agent_service}

# 查找 docker compose
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo "❌ 未找到 docker compose。"
    exit 1
fi

# 切换到 deployments 目录以使用 docker compose
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR="$SCRIPT_DIR/../deployments"
if [ -d "$DEPLOY_DIR" ]; then
    cd "$DEPLOY_DIR"
else
    echo "❌ 找不到 deployments 目录。"
    exit 1
fi

MYSQL_CMD="$COMPOSE_CMD exec -T mysql mysql -u$MYSQL_USER -p$MYSQL_PASSWORD"

# 检查连接
if ! $MYSQL_CMD -e "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ 无法连接到 MySQL。请确保服务已启动 ($COMPOSE_CMD up -d)。"
    exit 1
fi
echo "✅ 成功连接到 MySQL。"

# 检查数据库
if ! $MYSQL_CMD -e "USE $MYSQL_DATABASE;" > /dev/null 2>&1; then
    echo "❌ 数据库 '$MYSQL_DATABASE' 不存在。"
    exit 1
fi
echo "✅ 数据库 '$MYSQL_DATABASE' 存在。"

# 必需表（22 个，= deployments/mysql/init/*.sql 建出的全部表）
#
# 历史说明：roles / role_permissions / member_roles（阶段 43 删除，鉴权改由
# tenant_members.role + 资产 created_by 推导）与 artifacts（阶段 34 删除，制品
# 存 MinIO 由框架 artifact.Service 承担）都**不再是**初始化产物，故不在此列。
# 框架自建表（session_states/session_events/session_track_events/
# session_summaries/app_states/user_states、memories）由 session/mysql 与
# memory/mysql 在首次连接时创建，取决于租户的数据后端选择，因此也不在校验集内。
REQUIRED_TABLES=(
    "tenants" "tenant_members"
    "model_endpoints"
    "agents" "agent_versions"
    "tools" "agent_tool_grants"
    "skills" "skill_versions" "agent_skills"
    "knowledge_bases" "knowledge_documents"
    "chat_sessions" "chat_messages"
    "channel_bindings" "outbox_events" "idempotency_keys"
    "audit_logs" "usage_records"
    "secrets"
    "tenant_config_versions"
    "dead_letters"
)

MISSING=()
for table in "${REQUIRED_TABLES[@]}"; do
    if ! $MYSQL_CMD -D "$MYSQL_DATABASE" -e "DESCRIBE $table;" > /dev/null 2>&1; then
        MISSING+=("$table")
    fi
done

if [ ${#MISSING[@]} -eq 0 ]; then
    echo "✅ 所有 ${#REQUIRED_TABLES[@]} 个核心表已成功创建。"
else
    echo "❌ 缺少 ${#MISSING[@]} 个表:"
    for t in "${MISSING[@]}"; do
        echo "   - $t"
    done
    exit 1
fi

echo "=== 数据库初始化验证通过 ==="
