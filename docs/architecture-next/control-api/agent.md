# Agent 子领域设计

- **边界状态**：已接受
- **V1 纵向切片状态**：已接受
- **AgentSpec 字段状态**：已接受并由版本化 Schema 固化
- **实现状态**：V1 纵向切片已实现
- **适用范围**：Control API 的 Agent 定义、Draft 校验与不可变版本发布

## 1. 目的

`agent` 负责 Tenant 范围内 Agent 定义的创建、编辑、校验和版本发布。它管理的是
声明式定义，不是正在运行的 tRPC-Agent-Go 对象。

V1 的最终发布结果不是两份彼此独立的资源，而是：

```text
一个 AgentVersion
└── 包含一份经过校验、规范化且不可变的 AgentSpec
```

为了生成这个结果，纵向切片还必须具备稳定的 Agent 身份、一个当前 Draft、校验
诊断、发布事务和版本查询。只保存一段 JSON 或只插入一条 Version 记录都不构成
完整纵向切片。

## 2. V1 统一语言

| 术语 | 定义 |
| --- | --- |
| Agent | Tenant 范围内稳定的 Agent 身份与展示元数据，不是运行实例 |
| AgentDraft | 一个 Agent 当前唯一、可修改且允许暂时不完整的工作副本 |
| Draft Revision | AgentSpec 语义内容每次成功保存后的乐观并发版本 |
| AgentSpec | 描述 Agent 逻辑行为和运行资源需求的声明式文档 |
| Canonical AgentSpec | 通过发布校验并完成确定性规范化的 AgentSpec |
| AgentVersion | 包含一份 Canonical AgentSpec 的不可变发布记录 |
| Spec Digest | Canonical AgentSpec 的内容摘要，不包含编辑器状态 |
| Validation Report | 针对 Draft 返回的结构化 Error 与 Warning |
| Editor State | 节点坐标、视口和面板状态等非运行语义数据 |

`AgentSpec` 是 `agent` 子领域拥有的窄协议，不建立新的全局 `contract`、`common`
或 `shared` 业务包。公开 JSON Schema 位于 `api/schemas/agentspec/v1/`，Go 领域类型
位于本模块 `domain/`。

## 3. 所有权边界

`agent` 拥有：

- Agent 的稳定 ID、TenantID、名称和描述。
- 每个 Agent 当前唯一的 AgentDraft 及 Draft Revision。
- AgentSpec 的结构与不依赖运行环境的领域校验规则。
- 从确定 Draft Revision 发布不可变 AgentVersion 的事务规则。
- Version Number、Source Draft Revision、Spec Digest 和发布审计元数据。

`agent` 不拥有：

- Model、Tool、Knowledge、Storage 的具体配置、Profile 私有凭据关联、值与修订。
- Environment、ProfileRevision、DeploymentRevision 或 RuntimeManifest。
- IM Channel Binding、Run Admission、Run、Attempt 或 ReplyIntent。
- tRPC-Agent-Go 运行对象的构造、Worker 执行或回复投递。

AgentSpec 只能声明逻辑 Model/Tool/Knowledge Slot 和能力需求。ProfileRevision 提供
具名的具体 Profile Resource；Deployment 选择确定的 AgentVersion 与 ProfileRevision，
按类别和名称精确匹配 Model/Tool/Knowledge 资源、校验兼容性并生成 RuntimeManifest。
用户不填写额外映射表，Capability 不用于搜索候选资源，名称不进行模糊匹配。V1 不引入
Environment 业务对象；Storage 是独立的运行角色，不是 AgentSpec Slot。匹配结果属于
编译产物，Worker 只消费固定 Manifest。Profile 发布仍不读取 AgentVersion。

```text
AgentVersion(Canonical AgentSpec)
                    +
ProfileRevision(具体 Model/Tool/Knowledge/Storage + 内部凭据关联)
                    |
                    v
       Deployment 同名匹配、校验与编译
                    |
                    v
      DeploymentRevision + RuntimeManifest
                    |
                    v
             Agent Worker 执行
```

因此 Agent 发布不进入运行热路径，不创建 Run，不向 Worker 发送消息，也不发布
Deployment Outbox 事件。具体资源边界见
[`runtime-profile.md`](runtime-profile.md) 与
[`runtime-profile-spec.md`](runtime-profile-spec.md)；Deployment 的规划边界见
[`deployment.md`](deployment.md)。

## 4. V1 纵向切片

V1 至少实现以下端到端能力：

### Command

- `CreateAgent`：在 Tenant 内创建 Agent，并原子建立初始 Draft。
- `UpdateAgent`：修改名称和描述，不修改已发布 Version。
- `SaveDraft`：使用 Expected Draft Revision 保存一份可继续编辑的文档。
- `ValidateDraft`：返回结构化 Validation Report，不改变发布状态。
- `PublishAgentVersion`：从指定 Draft Revision 发布不可变 AgentVersion。

### Query

- `GetAgent`
- `ListAgents`
- `GetDraft`
- `GetAgentVersion`
- `ListAgentVersions`

具体 HTTP 路由和 DTO 已固化在 `api/openapi/control/v1/openapi.yaml`。Handler 只完成身份上下文、
路径和请求转换；Application 编排授权、校验与 Repository；Domain 不依赖 Gin、
PostgreSQL、NATS 或 tRPC-Agent-Go。

V1 的 Agent 读写和发布权限采用最小规则：ACTIVE Tenant 的 `OWNER` 与 `MEMBER` 均可
执行；仅有 Platform Operator 身份而没有该 Tenant Membership 的用户不可访问。
细粒度 RBAC、发布审批和自定义 Tenant Policy 留待后续版本。

## 5. 生命周期

```text
CreateAgent
    |
    v
Agent + AgentDraft(revision=1)
    |
    +-- SaveDraft(expected=1) --> AgentDraft(revision=2)
    |                                  |
    |                                  +-- ValidateDraft --> Report
    |                                  |
    |                                  +-- Publish(expected=2)
    |                                          |
    |                                          v
    |                                  AgentVersion(version=1,
    |                                    source_draft_revision=2)
    |
    +-- Draft 继续编辑 --> revision=3 --> AgentVersion(version=2)
```

Draft 不会“变成”Version，也不需要 `DRAFT/PUBLISHED/DEPLOYED` 混合状态机。发布只是
从某个 Draft Revision 创建不可变快照；Draft 在发布后仍可继续编辑。Deployment
状态属于 `deployment` 子领域。

V1 每个 Agent 只有一个当前 Draft，不实现并行 Draft、分支、合并或审批工作流。

## 6. V1 不变量

- Agent、Draft、Version 和所有 Repository 条件必须显式携带 TenantID。
- 创建 Agent 与初始 Draft 必须原子提交。
- Draft 保存必须携带 Expected Draft Revision；过期 Revision 返回稳定冲突错误。
- Draft 可以语义不完整，但必须是可安全存储的 JSON 对象并满足大小限制。
- 发布必须携带 Expected Draft Revision，并在事务内重新检查当前 Revision。
- 有 Error 的 Validation Report 禁止发布；Warning 不阻止发布。
- 同一 `(TenantID, AgentID, SourceDraftRevision)` 重复发布必须幂等返回已有
  Version。
- `VersionNumber` 在单个 Agent 内从 1 单调递增。
- AgentVersion 发布后禁止更新或覆盖其 AgentSpec。
- Spec Digest 基于 Canonical AgentSpec 计算，禁止包含 Editor State。
- Spec Digest 不建立唯一约束；恢复旧内容后允许发布新的业务版本。
- Deployment 只能引用 AgentVersion，禁止引用 AgentDraft。
- AgentSpec、诊断、日志和审计数据禁止包含明文 Secret。

## 7. 校验边界

V1 将校验分为三层：

1. **Draft 保存校验**：JSON 对象、大小、Expected Revision 与敏感字段边界。
2. **Agent 发布校验**：Schema、Root、节点引用、组合结构、Slot 引用和领域不变量。
3. **Deployment 兼容性校验**：Agent 的 Model/Tool/Knowledge Slot 是否全部在所选
   ProfileRevision 中具有同类别同名且满足 Capability 的 Resource；实际使用的资源
   是否符合平台静态实现与限制契约；最小资源闭包所需的内部凭据关联是否经
   `ProfileCredentialChecker.CheckUsable` 验证属于该 Tenant/Profile/Revision、用途与
   固定目的范围匹配且当前可用；以及能否生成最小、不可变、保留节点工具分配的
   RuntimeManifest。检查只读元数据，不解密或探测 Provider。V1 不引入独立
   Environment，测试与生产使用不同 RuntimeProfile。Profile 的 CheckUsable
   Application Port 已实现，Deployment 已通过发布装配调用它。

AgentSpec 不保存 SecretRef、内部 CredentialID 或凭据值。Profile 直接接收凭据并拥有
加密存储，Agent 不直接访问其表或解密。Worker 的新 Attempt 必须通过
`RuntimeCredentialResolver.ResolveForAttempt` 并经过当前 Run/Attempt 拥有方授权后
批量取值，不能把发布时检查当作永久许可。Profile 静态发布不检查 Provider 在线状态或
凭据对 Provider 的实际有效性。

Agent 发布成功只证明 AgentSpec 在不依赖具体运行环境的情况下成立，不证明任意
ProfileRevision 都能部署它。跨领域兼容性必须留在 Deployment 发布阶段。
Profile 直接录入与内部加密凭据已经在当前 V1 实现。固定配置与凭据轮换分离，
AgentSpec V1 不增加任何凭据字段。

V1 不在 Control API 中构造实际 tRPC Agent。Worker 根据固定 RuntimeManifest
组装和执行 tRPC-Agent-Go 对象。因此 `agent` 不定义
`adapter/outbound/trpcagent/`；以后只有在出现明确的 Agent Application Port，且该
Port 的职责不能归属于 Deployment 或 Worker 时，才新增相应 Adapter。

## 8. 发布事务与幂等

`PublishAgentVersion` 的推荐顺序是：

```text
1. 读取指定 AgentDraft 和 Expected Draft Revision
2. 解析、校验并规范化 AgentSpec
3. 计算 Spec Digest
4. 开启 PostgreSQL 事务并锁定当前 Agent/Draft
5. 重新检查 TenantID 与 Draft Revision
6. 已存在相同 Source Draft Revision 的 Version：返回已有记录
7. 否则分配下一个 Version Number
8. 插入 AgentVersion 并更新 Agent.latest_version_number
9. 提交事务
```

校验可以在事务外完成，但事务内必须重新检查 Revision。建议数据库唯一约束：

```text
UNIQUE (tenant_id, agent_id, version_number)
UNIQUE (tenant_id, agent_id, source_draft_revision)
```

禁止使用 `UNIQUE (agent_id, spec_digest)` 代替发布幂等约束。

## 9. 持久化草案

```text
agents
├── id
├── tenant_id
├── name
├── description
├── latest_version_number
├── created_by / created_at
└── updated_at

agent_drafts
├── tenant_id
├── agent_id
├── spec_revision
├── spec_jsonb
├── updated_by / updated_at
├── editor_revision        # 只有服务端保存 Editor State 时才加入
└── editor_state_jsonb     # 不参与 Spec Digest

agent_versions
├── id
├── tenant_id
├── agent_id
├── version_number
├── source_draft_revision
├── schema_version
├── spec_jsonb
├── spec_digest
└── published_by / published_at
```

Draft 和 Version 使用 JSONB 保存完整文档，节点不在 V1 中拆成可独立写入的数据库
表。查询字段、Tenant 边界、版本和审计信息使用显式列。

## 10. 代码结构

```text
services/control-api/internal/agent/
├── domain/
│   ├── agent.go
│   ├── draft.go
│   ├── version.go
│   ├── spec.go
│   ├── diagnostic.go
│   ├── validation.go
│   ├── schema_validation.go
│   ├── semantic_validation.go
│   ├── canonicalization.go
│   └── validation_helpers.go
├── application/
│   ├── ports.go
│   ├── create_agent.go
│   ├── update_agent.go
│   ├── save_draft.go
│   ├── validate_draft.go
│   ├── publish_version.go
│   ├── version_integrity.go
│   └── queries.go
├── adapter/
│   ├── inbound/http/
│   │   ├── routes.go
│   │   ├── agent_handler.go
│   │   ├── draft_handler.go
│   │   ├── version_handler.go
│   │   ├── request.go
│   │   └── response.go
│   └── outbound/postgres/
│       ├── store.go
│       ├── agent_repository.go
│       ├── draft_repository.go
│       ├── version_repository.go
│       └── mapper.go
├── id.go
└── wiring.go
```

业务 Adapter 分别使用 `package httpadapter` 和 `package postgresadapter`。文件按用例
或持久化职责拆分，不建立 `service.go`、`model.go`、`common.go` 或 `utils.go` 兜底
文件。

## 11. V1 完成定义

只有同时满足以下条件，才能声明 Agent V1 纵向切片已经实现：

- 真实 HTTP 路由、Application 用例、Domain 规则和 PostgreSQL Adapter 已贯通。
- Tenant Membership 授权和 Tenant 范围数据访问经过测试。
- Draft 乐观并发、发布幂等和 Version 不可变性经过数据库集成测试。
- AgentSpec JSON Schema、有效示例和无效示例已经版本化。
- Validate 与 Publish 返回稳定、可定位字段或节点的诊断。
- OpenAPI 与实际注册路由、Handler 行为一致。
- 新基线迁移包含 Agent、Draft 和 Version 表及必要约束。
- 单元测试、集成测试和对应 HTTP 纵向测试通过。

最终可观察结果是：客户端可以把一份 Draft 校验并发布为一个包含 Canonical
AgentSpec 的 AgentVersion。Deployment 已生成 RuntimeManifest；实际执行 Agent 属于后续
Runtime Profile、Deployment 与 Worker 切片。

## 12. 明确不在 V1

- 多个并行 Draft、Draft 分支或多人实时协同编辑。
- 发布审批、Agent Marketplace、标签和复杂归档状态机。
- Deployment 的同类别同名资源匹配、跨对象兼容性校验和 RuntimeManifest 生成。
- Graph、任意表达式、Reducer、Condition 或可执行代码 Registry。
- Worker tRPC Agent 组装、Run 执行与 IM 回复。
- AgentVersion 发布 NATS 事件或直接触发 Deployment。

## 13. V1 已固化结果

AgentSpec V1 已收口为 `llm`、`sequence`、`parallel` 和有界 `loop` 四种节点。
以下结果已经由版本化 Schema、Fixture、Go Domain Validator、HTTP/OpenAPI 与
PostgreSQL 纵向集成测试共同固化：

1. Canonicalization、Golden Document 与稳定 Digest。
2. Root、节点所有权、组合结构和 Slot 的 JSON 表达。
3. Error/Warning 诊断码、JSON Pointer 和节点定位格式。
4. Agent 与 Draft 的 HTTP 资源路径和并发控制字段。
5. V1 中由 `OWNER`、`MEMBER` 执行编辑与发布的最小权限策略。

后续破坏性协议变化必须发布新的 Schema Version，不能静默修改 V1。
