# 渠道账户与绑定语境

这个语境区分用户接入的机器人、该机器人的连接资格，以及消息对应的确定运行目标。

## Language

**ChannelAccount（渠道账户）**：某个租户接入的一个渠道机器人身份及其期望连接配置。
_Avoid_：部署环境、运维账号、Agent实例

**账户内部凭据**：与渠道账户及特定用途关联的机器人认证材料。
_Avoid_：Profile资源凭据、用户管理的Secret对象、环境变量名

**ChannelBinding（渠道绑定）**：一个渠道账户与一个精确部署修订之间的运行路由关系。
_Avoid_：Agent最新版选择器、连接配置、全局Deployment激活

**连接版本**：账户的连接配置、认证材料或启停状态的一次确定组合。
_Avoid_：Binding版本、路由代数

**Binding CAS**：用于识别某个Binding管理状态并避免覆盖并发修改的版本。
_Avoid_：账户路由版本

**RouteGeneration（账户路由代数）**：同一渠道账户的有效路由投影在其生命周期内的有序版本。
_Avoid_：Binding CAS、Deployment版本号

**期望状态**：用户已保存并希望运行侧达到的账户或绑定状态。
_Avoid_：已连接、已生效

**观测状态**：运行侧对某个账户版本的有时间范围的状态报告。
_Avoid_：授权证明、永久可用状态

**最低路由代数**：某份账户配置被应用后，新消息接纳所需达到的账户路由版本下限。
_Avoid_：连接版本、全局切流确认、旧回复的新目标

**接入预检（ChannelPreflight）**：针对已保存、停用渠道账户的一次有时间范围的只读接入诊断。
_Avoid_：启用接入、注册Webhook、运行就绪、真实投递验收

**预检新鲜度**：一次诊断事实与当前账户连接身份和检查时间窗口的关系。
_Avoid_：永久凭据授权、最新Gateway配置保证、连接Observation

## 代码开发进度（2026-09-07）

已接通Domain、命令/查询/Runtime Application、模块私有加密、Deployment拥有方目标读取、
真实PostgreSQL、11条公开Session API、3条内部mTLS API、Bootstrap和路由Outbox Relay。
提交期Identity/Tenant授权SQL各自由拥有方Adapter维护，Bootstrap组合；Channel不查询
Deployment/Profile表，目标由拥有方只读端口验证。SQL外键表达数据库一致性，不代替拥有方校验。

内存事务测试验证领域命令，真实PG验证事务/锁/回滚，真实TLS验证工作负载身份，独立NATS
验证受限Producer的持久PubAck和失败重投。真实Telegram联调另记，不以fixture成功代替。
运行与验收入口见`services/control-api/CHANNEL_RUNTIME.md`。

Telegram预检新增独立PreflightService/Store、2公开与3私有入口、0002增量迁移和每秒有界维护。
创建保留真实Session/OWNER提交授权，后台持久请求者授权不依赖Session续期；完成回执与
首次执行授权分开。按任务固定Token元数据解析，仅内部mTLS响应短时携带值。
预检不修改Account、Credential、Binding、Route、Catalog、Observation、普通Receipt/Outbox。
shared Schema与真实PG/Session/mTLS测试覆盖Control闭环；跨端真实Telegram/Web验收另记。
