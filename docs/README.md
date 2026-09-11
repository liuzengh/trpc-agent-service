# 文档索引

## 项目主页与文档中心

Web 根地址 `/` 提供公开项目主页；`/docs` 是教程与六篇主题参考文档入口；
原会话恢复入口位于 `/console`。站点内容与更新方式见 [主页与站内文档](site/README.md)。

## 给使用者的指南

- [Agent tRPC V1 部署与使用指南](user-guide/v1/README.md)：逐页操作、单 LLM Agent 上线、Telegram / 企微接入、升级回退与常见问题，附图文。另提供[可独立阅读的 HTML 版](user-guide/v1/部署与使用指南.html)。


本仓库的架构文档分为两类：

- [`architecture-next/`](architecture-next/README.md)：当前重构使用的规范性文档，
  用于约束后续代码、协议、数据库和部署设计。
- 根目录 [`README.md`](../README.md)：项目入口、当前状态和生产 Workload 概览。

## 当前阶段

Control API 的 Identity、Admin、Tenant、Agent、Runtime Profile、Deployment Publication
和 ChannelAccount / ChannelBinding V1 已有实现。Deployment 将确定的 AgentVersion + ProfileRevision
经同类别同名匹配、校验和编译，生成不可变 DeploymentRevision 与最小
RuntimeManifest。V1 不引入 Environment 业务对象或用户映射表。

凭据交互采用 Profile 内直接填写，内部加密保存、公开只返回 configured/status；
Deployment 已通过 Profile Owner 的 `ProfileCredentialChecker` 检查必要凭据元数据；
无独立 Secret 产品，值不进入 Manifest、Outbox、Event、Receipt 或公开响应。
Worker 新 Attempt 经 Profile Owner 的内部认证接口批量解析是后续 Runtime 接线；
不增加协议代际或开发期 ref-only 兼容。

Deployment 的 Schema / Event、纯 Compiler、Application、PostgreSQL 原子发布、
八个 HTTP 路由、Bootstrap 和真实 PostgreSQL Integration 已落地。Manifest 发布事件停在同事务 `PENDING` Outbox；这不同于已实现并可投递的 Channel 路由 Outbox。
Manifest 正文的运行侧获取/分发及 Worker 执行仍分别验收。

ChannelAccount / ChannelBinding 已实现账户与私有凭据、11 个公开管理操作、内部 mTLS
快照/凭据/观测接口、精确部署目标和路由专用 Relay。Gateway 已实现动态接入与路由投影，
四 Module 和公开 Go Connector 同处一个 Go Workload；默认 Control 来源启动 Delivery
Runner 并由它独占 Maintenance，fixture 来源只独立维护，当前共 10 个迁移（0001–0010）。
真实 Telegram 消息已到达持久 RunRequested，见[Control 联合验收](architecture-next/control-api/channel-acceptance.md)
及[Gateway 入站证据](architecture-next/channel-gateway/telegram-real-inbound-20260906.md)。
Worker、模型/Storage 真实执行和完整回复尚未贯通；本工作树的 Channel Web 尚未接入。
ReplyIntent Consumer 尚未接入；实现与历史验收不代替发布上线或当前进程健康证明。
Helm 仍待全部生产 Workload 完成后进入 FINAL-INTEGRATION。

当前规范性入口：

- [下一代架构文档](architecture-next/README.md)
- [架构级约束](architecture-next/constraints.md)
- [Deployment V1 设计与实施路线](architecture-next/control-api/deployment.md)
- [ChannelAccount 设计与实现](architecture-next/control-api/channel-account.md)
- [ChannelBinding 设计与实现](architecture-next/control-api/channelbinding.md)
- [Gateway Control 接入与验收](architecture-next/channel-gateway/control-integration-v1.md)
- [产品易用性 TODO](architecture-next/control-api/product-usability-todo.md)：已确认改进方向，待设计与实现。

文档中的“已接受”表示设计决策已经确认；“已实现”必须以当前代码和
测试为依据。Control Publication、Distribution 和 Runtime Execution 是三个独立完成层级。

## Gateway 设计、实现与历史调研

- [Channel Gateway 技术栈与部署设计](architecture-next/channel-gateway/README.md)：纯 Go、公开企微协议库、模块职责与完整部署方向；已实现能力与剩余目标分开。

- [Channel Gateway 实施状态](architecture-next/channel-gateway/implementation-status.md)：入站切片、持续积压门禁、公开企微 P0、分项/最终验证状态和完整目标缺口。

- [Channel Gateway 四 Module 入门说明](architecture-next/channel-gateway/module-introduction.md)：一条消息的职责流、各 Module 的拥有事实/例子、公开库与 Adapter、开发分工和部署阶段。

- [Channel Gateway 四个业务 Module](architecture-next/channel-gateway/module-boundaries.md)：解释 routing、admission、connection、delivery 为何在同一 Workload 内分开，以及消息追踪、代码/人员分工、接口、同事务校验与失败归属。

- [Channel Gateway 设计复审](architecture-next/channel-gateway/design-review.md)：列出已修订矛盾、剩余 D0 决策，以及历史分支同步与分阶段运行验收。

- [Channel Gateway：Telegram 与企业微信智能机器人](architecture-next/channel-gateway/im-channel-sdk-semantics.md)：机器人行为、SDK 接纳边界与真实实验记录；历史 SDK 实验与当前真实 Gateway 入站验收分列，均不表示完整 Worker 回复链路已验收。

- [公开 Go Connector 设计](architecture-next/channel-gateway/public-go-connector.md)：企微库直接导入，Telegram SDK 直接引用；无独立 Connector 部署单元。

公开官网与文档现由独立的 `site/` 工程构建；管理 Web 的 `/help` 使用运行时 `DOCS_SITE_URL` 跳转。详见 [站点维护](../site/README.md)。
