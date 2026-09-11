# P0a Agent 真实保存与发布验收

该脚本不启动服务，不读取共享 `.env`，不连接 Bot，不修改密码或既有 Agent。
须由统筹任务提供已验证运行 P0a 的隔离 Control/Web、现有测试 Tenant 及专用 OWNER。
当前执行状态：**待联调，未执行真实 HTTP 验收**。

从项目根目录执行，凭据由调用方注入环境变量，不写入代码或报告：

```sh
# WEB_BASE_URL、CONTROL_E2E_TENANT_ID、CONTROL_E2E_USERNAME、
# CONTROL_E2E_PASSWORD 均由隔离环境负责人提供并注入。
node web/test/agent-runtime-data-real.mjs
```

脚本消费 Control 的两个版本化 fixture，每个新建一个专用 Agent；立即打印
`created_agent_id` 和 `tenant_id`，包括后续断言失败时也可追溯。
测试真实登录、CAS 保存与过期 revision 冲突、GET 回读、校验、发布、版本回读及
相同源 revision 的幂等发布。规范化对比只排序 Memory 工具数组，其他字段保持精确比较。
凭据、Cookie 和响应正文不输出。成功标记为 `P0A_REAL_AGENT_AUTHORING=PASS`。

Deployment 未在此脚本发布：P0a 暂拒绝新增声明属于预期保护，不能绕过或算作运行成功。
本脚本不证明 Worker 执行、Memory 持久化或 Artifact 上传已可用。

## 限定清理

每次运行仅创建两个带 `P0a runtime-data-...` 前缀的 Agent，打印 ID 后保留供审计。
当前已核对的管理客户端没有 Agent 删除接口，脚本不编造删除端点或直接删数据库行。
验收负责人先收集本次输出的 ID 和结果；待无需复查后随**专属隔离环境**整体销毁。
若环境需要保留，则仅记录这些 ID 待服务端提供明确清理机制，不按名称批量处理其他记录。
