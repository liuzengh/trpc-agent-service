# 运行配置与资源

运行配置把 Agent 里的逻辑需求，连接到真正可以使用的服务。页面入口：控制台 → 当前租户 → **运行配置**。

## 第一次上线需要什么

| 分类 | 推荐的首个资源名 | 填写内容 |
| --- | --- | --- |
| Models | `primary` | 模型服务类型、模型名称、Base URL、API Key |
| Storage | `session` | PostgreSQL 会话存储连接信息和凭据 |

资源名必须与 Agent 的对应槽位完全一致。`Models.primary` 与 `Models.main` 是两个不同资源。当前部署按同类别、同名匹配，不提供任意映射。

模型与数据库地址必须能从运行服务访问。浏览器可以访问的 `localhost`，不一定是 Worker 容器中的同一个地址。

## Models、Tools、Knowledge、Storage 的区别

- **Models**：实际语言模型。当前模型类型为 `openai_compatible`，填写服务提供方给出的模型名与兼容接口地址。
- **Tools**：工具连接配置，如 `mcp_streamable_http`。配置入口存在，但当前 Worker V1 不执行工具调用。
- **Knowledge**：知识资源连接，如 `qdrant_openai`。它是资源配置，不是文档上传中心；当前 Worker V1 不执行知识调用。
- **Storage**：状态存储。V1 使用 `postgres_state` 的 `session` 资源保存会话。

不要为了“填满所有分类”而添加资源。先完成最小可运行配置，再按平台后续能力逐步扩展。

## 凭据由谁填写

租户 OWNER 负责填写和替换凭据。普通 MEMBER 可以准备非密钥配置，但不能代替 OWNER 完成密钥管理。页面只展示是否已配置等状态，不回显完整密钥。

Telegram Bot Token 和企微 Bot Secret 属于[渠道账户](channels.html)，不填写在这里。

## 凭据更新的影响

- 修改草稿并替换凭据时，按页面流程保存；已有发布版本的旧凭据关联会被保留。
- 从已发布资源更新共享凭据时，可能影响所有引用该凭据的运行方案，而不增加 Profile Revision。先确认影响范围，再更新。
- 仅看到“已配置”，只能证明服务端保存了凭据，不证明外部模型或数据库能成功连接。

## 发布与检查

保存 Draft → 校验 → 发布 Profile r1。记录 Models 与 Storage 的资源名，确认必要凭据已配置。随后在部署页面选择此固定版本。

## 下一步

[部署与生命周期](deployment.html) · [完整配置步骤](/docs/guide.html#chapter-5)
