# 阶段 0：范围与验收门槛

> 核对日期：2026-08-24  
> 阶段 0 结果：调研材料已形成，五项关键决策已于 2026-08-24 确认；阶段 0 正式关闭，进入阶段 1 规格准备

## 1. 本阶段完成的工作

| 验收项 | 状态 | 证据 |
| --- | --- | --- |
| 当前仓库真实能力和缺口明确 | 已完成 | `stage0-baseline.md` 的能力/缺口清单 |
| Git、Go 与依赖基线记录 | 已完成 | `main`、HEAD、origin、Go 1.21、空依赖基线 |
| tRPC-Agent-Go 模块路径与稳定版本核对 | 已完成 | module 为 `trpc.group/trpc-go/trpc-agent-go`；2026-08-24 最新稳定版为 `v1.11.2` |
| 核心 Runner/Session/Memory API 编译验证 | 已完成 | 独立临时模块验证构造、注入和 Close；未改服务 `go.mod` |
| OpenClaw 直接依赖边界 | 已完成 | 排除直接 API/import，只有设计参考 |
| Telegram SDK 候选比较 | 已完成 | `stage0-im-sdk-matrix.md` |
| 企业微信 SDK 与协议类型比较 | 已完成，首版协议已确认 | 首版按企业内部自建应用验证；`silenceper/wechat/v2` 作为阶段 1 Spike 候选 |
| Runner 两方案比较 | 已完成，方案已确认 | `stage0-runner-options.md` |
| 统一消息、Binding、Inbox 和 Session ID 草案 | 已完成 | `stage0-architecture.md` |
| 最小真实交付、非目标与 Web UI 边界 | 已完成 | 本文第 3-5 节 |
| 服务仓库基线构建和测试 | 已完成 | `go test ./...`、`go build ./...` 通过；当前无测试文件 |
| 用户确认阶段 1 方向 | **已完成（2026-08-24）** | 五项关键决策已确认；阶段 1 仍需先完成技术入口验证 |

阶段 0 的“已完成”表示调查结果已经足够供审阅，不表示目标平台功能已完成。

## 2. 阶段 0 固定边界

- 不直接复用、包装或引入 OpenClaw API。
- 不把 OpenClaw Telegram 成品示例当作 SDK；只参考其分层、注册、转换和重试思想。
- 不修改服务仓库 `go.mod`，不在阶段 0 执行 `go get`。
- 不实现真实 IM Adapter、Gateway、Worker、Runner 缓存、Redis、SQL、Web UI 或生产 Agent。
- 不使用真实 IM 凭据、公网 webhook 或线上账号。
- 不把 Mock 当作第二种 IM 实现；后续必须有两种真实 SDK/协议适配代码。
- 不把旧工作区代码结构当成新服务仓库的实施依据。
- 不把“AppName 会漏传/错传”当成已证实的旧 Runner 方案缺陷。

本轮已确认的实现方向：

- Runner 采用每个 Worker 按 `tenant_id + agent_app_id + config_version` 的有界本地缓存；Session/Memory 状态必须位于共享后端。
- Telegram 首选 `github.com/go-telegram/bot v1.23.0`，`telegram-bot-api/v5 v5.5.1` 保留为备选；依赖引入推迟到阶段 1。
- 企业微信首版按企业内部自建应用验证，先对 `github.com/silenceper/wechat/v2 v2.1.14` 做协议匹配 Spike；真实账号联调后置，不作为阶段 0 或自动化测试门槛。
- 群聊首版使用群主体作为 `runner_user_id`，形成群级 Session/Memory；真实发言人 `actor_user_id` 只用于权限和审计。
- 最小交付必须包含真实 Agent、双 IM 适配、Gateway/双 Worker、共享 Redis 状态、基础治理/观测和轻量 Web UI；生产级扩展以设计文档为主。

## 3. 后续最小真实交付

题目以架构设计为主，但明确要求 GitHub 实现代码。后续最小可运行结果不能只有接口和文档，建议至少包含：

1. 一个真实 tRPC-Agent-Go Agent/Runner 调用，能消费 Event 并返回模型结果。
2. 两种 IM 的真实 SDK/协议适配代码，当前方向为 Telegram + 企业微信；配套可追溯 Mock 契约测试。
3. 统一入站/出站消息、可信 ChannelBinding、多租户/Agent App 配置版本。
4. Inbox 幂等、任务租约、执行状态与投递状态分离。
5. Redis 支撑的共享协调能力，以及跨 Worker 可见的 Session/Memory 后端。
6. 同机一个 Gateway + 两个 Worker，证明两者都可处理任务、一个退出后可接管。
7. 至少两个租户/Agent App 的配置、数据和工具权限隔离。
8. 基础脱敏审计、trace、请求/错误/投递指标。
9. 轻量 Web UI 作为本地模拟渠道和演示入口。
10. Compose 或等价的一键本地启动与可复现实验步骤。

建议的最小演示闭环：

```text
Web UI/Telegram/企业微信 Mock
-> Gateway 验证与 Binding
-> Inbox 去重
-> Worker A/B
-> App 专属 Runner
-> 共享 Session/Memory
-> Outbox
-> 渠道回复
```

## 4. 以设计文档为主的能力

以下内容需要给出具体架构、数据流、接口、风险和未来步骤，但首版不要求全部生产实现：

- Redis 到 SQL 的完整在线迁移、双写、灰度切流和自动回滚；
- 向量库、对象存储、外部 Memory 的所有生产级 Adapter；
- 完整预算计费、复杂租户配额和审批系统；
- 生产级 Kubernetes、跨区域容灾和自动扩缩容；
- 大规模压测平台和精确容量结论；
- 全功能管理后台；
- 所有企业微信产品线和所有 Telegram 消息类型。

这不是省略题目要求，而是按“核心链路必须编码、生产扩展必须设计”的方式控制实际交付范围。

## 5. Web UI 范围

Web UI 是演示和本地验证入口，不能替代两种 IM Adapter。首版范围建议为：

- 常见聊天界面：输入文字、查看进度/最终回复、展示基础附件；
- 选择预置模拟渠道账号/ChannelBinding；
- 简单选择已发布 Agent App 配置用于演示；
- 显示 request/trace ID 和明确错误状态；
- 能模拟重复消息和指定平台消息 ID，验证 Inbox 幂等。

首版不做：

- 完整 Tenant/Agent/密钥管理后台；
- 节点调度大屏、完整审计检索和 Trace 可视化产品；
- 从浏览器直接提交并信任 `tenant_id`；
- 在前端保存 IM token、模型 key 或数据库密码。

## 6. 阶段 1 入口条件

五项阶段 0 决策已经满足。进入功能实现前仍必须完成：

- 以阶段 1 详细规格固定 Runner 缓存的 singleflight、容量/TTL、版本排空、共享资源引用和 race 测试。
- 对 `go-telegram/bot v1.23.0` 和 `silenceper/wechat/v2 v2.1.14` 做独立临时模块验证，不依赖真实账号。
- 重新核对 tRPC-Agent-Go 版本；当前基线为 `trpc.group/trpc-go/trpc-agent-go v1.11.2`，实际引入前仍需复查 release notes。
- 保持 Go 最低版本 1.21，使用真实依赖重新构建验证。
- 正式开发前保护当前未跟踪文件，配置 upstream/分支时不误提交用户材料。

上述入口任务完成后，才开始阶段 1 的生产代码实现；阶段 0 的方案选择不再重复讨论。

## 7. 已确认的阶段 0 决策（2026-08-24）

| 决策 | 已确认结果 | 阶段 1 执行边界 |
| --- | --- | --- |
| tRPC-Agent-Go | 基线为 `v1.11.2`，module path 为 `trpc.group/trpc-go/trpc-agent-go` | 阶段 1 开工时复核版本后再修改 `go.mod` |
| Go 最低版本 | 保持 1.21 | 引入全部依赖后用 Go 1.21 工具链复验 |
| Runner | 每 Worker 按 `tenant + app + config_version` 有界缓存 | 先完成缓存/生命周期 Spike，再写生产实现 |
| Telegram SDK | 首选 `go-telegram/bot v1.23.0`，备选 `telegram-bot-api/v5 v5.5.1` | 阶段 1 临时模块验证后接入 Adapter |
| 企业微信协议 | 首版按企业内部自建应用 | 其他企业微信产品线不纳入首版适配范围 |
| 企业微信 SDK | 首选对 `silenceper/wechat/v2 v2.1.14` 做协议匹配 Spike | 覆盖不足时由平台层补官方协议薄适配 |
| 群聊状态 | 首版群级 Session/Memory，actor 单独用于权限审计 | 不在首版同时承诺个人 Memory |
| 最小交付与 Web UI | 核心链路必须真实编码；Web UI 仅做聊天/模拟绑定/幂等演示 | 生产扩展和完整后台以文档为主 |

## 8. 阶段 0 验证记录

| 验证 | 结果 | 限制 |
| --- | --- | --- |
| `go test ./...` | 通过 | 当前所有包无测试文件 |
| `go build ./...` | 通过 | 只构建骨架 |
| 临时模块导入 tRPC-Agent-Go 核心 API | 通过 | 使用本地只读源码替换完成 API 编译核对，不是生产依赖锁定 |
| Go module proxy 版本核对 | `v1.11.2` | 版本会随时间变化，开发开始时复查 |
| SDK module/README/许可证核对 | 候选表已形成 | GitHub API/搜索限流导致未采用精确活跃度数值 |
| OpenClaw 直接依赖检查 | 服务 `go.mod` 无依赖，生产 Go 源码无 import | 包注释/文档中存在“参考 OpenClaw”文字不属于依赖 |

## 9. 阶段 0 产出索引

- `docs/stage0-baseline.md`：仓库现状、tRPC-Agent-Go API、缺口和 Go 基线。
- `docs/stage0-im-sdk-matrix.md`：Telegram/企业微信 SDK 和协议类型对比。
- `docs/stage0-runner-options.md`：旧方案、新候选方案、实例关系、更新和 Spike。
- `docs/stage0-architecture.md`：主链路、消息契约、Binding、Inbox、Session ID 和职责。
- `docs/stage0-acceptance.md`：范围、交付级别、审阅门槛和下一阶段入口。

## 10. 阶段 0 的结束定义

阶段 0 的最终结果不是“功能已经完成”，而是：后续实现人员不需要再猜测仓库差距、框架模块路径、SDK 候选、消息信任边界、Runner 责任和交付范围。

阶段 0 调查和五项关键决策均已完成。阶段 0 正式关闭；阶段 1 只能按上述入口条件先完成依赖和 Spike 验证，再开始功能实现。
