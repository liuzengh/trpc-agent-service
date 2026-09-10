# Agent 灰度发布设计调研

日期：2026-09-10

## 结论

当前 Agent 灰度发布应采用“一个 Stable + 最多一个 Candidate + 一个可选的进行中 Rollout”模型，而不是从历史版本集合中任意选择 Canary。

发布控制面：

1. 编辑 Agent 可以直接全量发布；也可以保存为唯一 Candidate。
2. Candidate 未开始灰度时不会接收生产流量。
3. 灰度开始后，只允许调整同一个 Candidate 的发布范围和比例；不能在灰度中静默创建另一个 Candidate 或普通直发新版本。
4. “停止灰度”只终止 Candidate 的新流量，Stable 不变，Candidate 保留，可重新灰度或放弃。
5. “全量发布”把 Candidate 原子提升为 Stable，同时清理 Rollout 和 Candidate 指针。
6. 历史版本只能用于“恢复此版本”；恢复会复制内容生成一个新的单调递增 Stable 版本，而不是让历史版本重新变成 Candidate。

路由控制面：

1. 先用 Stable 配置完成 Binding 校验、身份归一化和 Session 解析；Candidate 灰度期间禁止改变 Channel Binding 拓扑，因此这一步不会依赖 Candidate。
2. 再使用真实 `session_key` 做确定性分桶，不能使用 Provider 原始 `conversation_id`。同一平台 Session 在相同比例下必须稳定落到同一版本。
3. 发布范围是上层约束：先匹配 Web/具体 Channel Binding，再进行测试成员或百分比判断。
4. 测试成员使用 canonical `platform_user_id`，不使用飞书 `open_id`、企微 `userid`、Telegram user id 等 Provider 原始身份。测试成员只覆盖 direct Session；群聊按群 Session 做百分比分桶，避免同一群因不同发言人来回切版本。
5. 百分比扩大使用稳定哈希，不重排已进入 Candidate 的桶；缩小时允许一部分 Session 回 Stable。停止灰度则后续请求全部 Stable。任何已经进入 Kafka 的消息继续执行其 Envelope 已封存的 `config_version`，保持 INV-03。

UI 应直接表达发布状态，而不是暴露内部 `generation / basis_points / canary_version` 术语：

- 当前稳定：`vN`
- 候选版本：`vN+1`，状态为“待灰度 / 灰度中”
- 发布范围：网页或具体外部 Binding
- 测试成员：租户 Platform User
- 灰度比例：常用档位 + 可调整百分比
- 操作：开始灰度 / 调整灰度 / 停止灰度 / 全量发布 / 放弃候选
- 版本历史：Stable / Candidate / History 三种明确状态；History 只提供“恢复此版本”

## 主流实现依据

### Argo Rollouts

官方 Canary 模型明确保留 Stable 与新 Canary 两个角色，通过 `setWeight` 和 `pause` 逐步推进，最终 `promote` 后新版本才成为 Stable；`abort` 会把流量退回 Stable。

- https://argo-rollouts.readthedocs.io/en/stable/features/canary/
- https://argo-rollouts.readthedocs.io/en/stable/getting-started/
- https://argo-rollouts.readthedocs.io/en/stable/features/rollback/

这支持本项目把 Candidate 从“所有历史版本”中独立出来，并把“全量发布 / 停止灰度 / 历史回滚”定义成不同动作。

### Flagger

Flagger 的 Canary 是 Primary/Canary 双版本，逐步调整流量，并在失败或人工回滚时把流量切回 Primary。官方还明确指出，需要 Session Affinity 的应用应使用 cookie/header 等稳定匹配保证同一用户在灰度周期内保持版本一致。

- https://docs.flagger.app/main/usage/deployment-strategies
- https://docs.flagger.app/main/usage/webhooks
- https://docs.flagger.app/faq

Agent 是有状态对话系统，比普通无状态 HTTP 更需要稳定的 Session 路由，因此本项目应使用 canonical `session_key` 作为灰度分桶单位。

### LaunchDarkly

LaunchDarkly 的 percentage rollout 基于稳定 Context Attribute 做 bucket；Targeting Rule 可以先限定 audience，再在匹配范围内进行百分比分配。官方也强调选择能够在用户体验周期中保持稳定的 context key。

- https://launchdarkly.com/docs/home/releases/attribute-rollout
- https://launchdarkly.com/docs/sdk/concepts/flag-evaluation-rules
- https://launchdarkly.com/docs/home/releases/audience-based-releases

这支持“发布范围先于百分比”“canonical identity 而非 Provider 原始 sender ID”的设计。

### Unleash

Unleash 将 stickiness 定义为确定性哈希，并支持 `userId`、`sessionId` 或自定义上下文字段。扩大 rollout 百分比时已有命中不会被重新洗牌。

- https://docs.getunleash.io/guides/gradual-rollout
- https://docs.getunleash.io/concepts/stickiness

本项目采用 Session stickiness，而不是 request/conversation stickiness。

## 与当前实现的差异

当前实现存在以下结构性问题：

- `application_rollouts.canary_version` 可指向任意非 Stable 历史版本，Candidate 不是一等状态。
- UI 把“候选 / 历史版本”混成同一种版本。
- 灰度选择发生在 `ResolveInboundSession` 之前，并按原始 `conversation_id` 分桶；一个平台 Session 若存在多个 Conversation route，可能跨版本。
- `user_whitelist` 使用 Provider 原始 `senderID`，绕过 Platform User 统一身份。
- user whitelist 在 channel filter 之前判断，因此显式用户可以越过发布渠道范围。
- 灰度中普通 `Publish` 会直接删除 rollout，产品上属于隐式终止发布。

以上问题需要在 Repository、Ingress 和 Console 三个公开边界一起修复，单改 UI 不足以形成正确发布语义。
