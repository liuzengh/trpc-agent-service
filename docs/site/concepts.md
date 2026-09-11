# 理解核心概念

先把四个对象分清，再开始第一次发布。本文是 V1 参考文档；需要逐步操作时，请打开[完整图文教程](/docs/guide.html#chapter-1)。

## 四个对象，一条路径

| 对象 | 负责什么 | 发布后得到什么 |
| --- | --- | --- |
| Agent | 角色、指令、节点与资源需求 | 固定的 AgentVersion，例如 v1 |
| 运行配置 / Runtime Profile | 实际模型、资源地址与凭据关联 | 固定的 ProfileRevision，例如 r1 |
| 部署 / Deployment | 选择确定的 Agent 与 Profile 版本，校验兼容性 | DeploymentRevision 与运行清单 |
| 渠道 / Channel | 机器人身份、接收方式与消息路由 | 连接状态和指向固定部署版本的路由 |

Agent 说“我要一个名为 `primary` 的模型”；Profile 提供实际的 `Models.primary`。部署按同类别、同名称匹配它们。V1 没有单独的 Environment 页面，也不要求手工创建槽位映射表。

## 保存、发布、上线不是同一个动作

1. **保存**：把草稿存到服务端，后续仍可修改。
2. **发布**：生成固定版本，已发布内容不跟随草稿自动变化。
3. **上线**：渠道指向某个部署版本，启用接入并开启路由，新消息才进入该目标。
4. **实际可用**：发送真实消息并收到回复；再发一条承接上文的消息，确认会话连续性。

发布 Agent 不会自动创建运行配置；发布部署也不会自动启用机器人。

## V1 的第一条可运行路径

采用 **一个 LLM 节点 + 一个模型 + session 会话存储 + 文本对话**。

工作台展示了工具、知识、Memory 及组合节点的配置入口，但当前 Worker V1 会拒绝涉及这些执行能力的部署。第一次上线时，Tool Slots 和 Knowledge Slots 留空，只配置 `Storage.session`。

## 从哪个页面开始

- 没有账号：联系平台管理员。当前登录页没有自助注册。
- 已有账号：打开自己部署的控制台地址，选择租户，再进入 Agent 工作台。
- 负责安装：先看[安装检查与启动](/docs/guide.html#chapter-13)。
- 已有 Agent：继续配置[运行资源](runtime-profile.html)，然后[发布部署](deployment.html)。

## 阅读下一篇

[创建与发布 Agent](agent.html) · [渠道接入](channels.html) · [账号与权限](administration.html)
