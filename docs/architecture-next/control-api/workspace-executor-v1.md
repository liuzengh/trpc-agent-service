# Workspace Executor V1：Control 编译契约

本页描述新增的可选字段与发布门禁，不宣称 Worker 执行或 Gateway 附件投递已验收。
沿用现有 v1 Schema，未配置 workspace 的历史文档和 canonical 字段形状不变。

## 输入及固定映射

```json
{
  "requirements": {"executors": {"shell": {"capability": "workspace"}}},
  "nodes": {"assistant": {
    "workspace": {
      "executor_slot": "shell",
      "tools": ["workspace_exec", "workspace_save_artifact"]
    },
    "artifact": {"enabled": true}
  }}
}
```

以上是 AgentSpec 局部字段；节点仍需原有 kind、model_slot 等字段。
仅 LLM 节点允许 workspace。tools 必须非空、唯一、最多两项，且只允许示例中的两个名称。
关闭能力应删除 workspace，不能提交空 tools。save 必须显式启用同节点 Artifact，编译器不自动补齐。

Profile 可选字段：

```json
{"executors": {"shell": {"kind": "sdk_sandbox"}}}
```

Agent 与 Profile executors 均最多 16 项，名称沿用 `^[a-z][a-z0-9_-]{0,63}$`。
资源没有路径、环境变量、URL、Secret 或独立预算字段。sdk_sandbox 是固定 Worker Adapter 选择，
不代表用户可控制宿主机文件路径或选择任意执行后端。

Manifest 新增可选字段：

```json
{
  "resources": {"executors": {"shell": {
    "kind": "sdk_sandbox", "adapter_version": "sdk-sandbox-v1"
  }}},
  "resolved_requirements": {"executors": {"shell": "shell"}},
  "agent_plan": {"nodes": {"assistant": {"workspace": {
    "executor_resource": "shell", "tools": ["workspace_exec", "workspace_save_artifact"]
  }}}}
}
```

只编译节点实际选择的 executor。资源集合与 resolved_requirements 必须精确覆盖全树节点依赖，
多节点可共享同一个资源描述，但 workspace 工具权限逐节点保存。未选择的 Profile executor 不进入 Manifest。
公开 Manifest View 保留这些非秘密字段；Profile 保存、发布和公开读取均保留 executors。

## 发布与消费门禁

- Agent 校验声明存在、workspace 能力名称、闭合字段、工具枚举及 Artifact 显式依赖。
- Deployment 按类别和同名精确匹配，不猜测、不映射异名、不创建新的数据库或执行平台。
- 任意 workspace 工具均要求该节点最终模型提供 tool_call；使用现有模型能力诊断。
- 平台发布契约 `runtime_data_capabilities` 新增 workspace，改变当前 Worker release digest。
  历史 platform-v1 digest 不变；`.env.example` pin 按该文件实际 endpoint hosts 重新计算。
- `callable_entries` 仍仅保存 tools/knowledge 的既有逻辑入口；workspace 使用独立节点选择。
  既有 Provider 名称为 `fn_<hash>`，Memory/Artifact/workspace 固定名称互异，不隐式增加其他入口。
- 共享 Manifest schema/codec 与 Worker 静态门禁验证 kind/version、工具集合、精确闭包、模型能力和 save 依赖。
- Worker 负责 SDK Sandbox 实例、工作目录隔离、命令执行、文件保存及清理。
  Control 不实现执行器，不读取工作区文件，不扩大原 Attempt 凭据授权。

## 稳定诊断

| Code | Pointer / Path |
|---|---|
| AGENT_SPEC_EXECUTOR_SLOT_NOT_FOUND | /nodes/{id}/workspace/executor_slot |
| AGENT_SPEC_EXECUTOR_CAPABILITY_UNSUPPORTED | /requirements/executors/{slot}/capability |
| AGENT_SPEC_WORKSPACE_TOOL_UNSUPPORTED | /nodes/{id}/workspace/tools/{index} |
| AGENT_SPEC_DUPLICATE_WORKSPACE_TOOL | /nodes/{id}/workspace/tools/{index} |
| AGENT_SPEC_WORKSPACE_ARTIFACT_REQUIRED | /nodes/{id}/workspace/tools |
| RUNTIME_PROFILE_SPEC_EXECUTOR_KIND_UNSUPPORTED | /executors/{slot}/kind |
| DEPLOYMENT_RESOURCE_MISSING | /executors/{slot} |
| DEPLOYMENT_ADAPTER_UNSUPPORTED | /executors/{slot}/kind |
| DEPLOYMENT_ENTRYPOINT_UNSUPPORTED | /nodes/{id}/workspace |
| DEPLOYMENT_CAPABILITY_MISMATCH | /nodes/{id}/model_slot |

其他字段错误复用既有类型、必填、标识符及上限诊断；Agent 发布诊断的 pointer 与 Deployment 的 path 不混用。

## 本包验证范围

定向测试覆盖 canonical/公开 Schema、Profile 写入与读取、exec/save 编译、额外资源裁剪、节点隔离、
缺资源、错误 adapter、错误 resolved binding、模型不支持工具、save 缺 Artifact、拒绝额外宿主配置。
既有 Agent/Profile/Manifest fixtures 回归继续验证历史格式。
真实命令执行、对象存储文件内容、正式 Final 附件与渠道投递由 Worker/Gateway 联调包验收。
