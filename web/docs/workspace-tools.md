# Workspace 工具配置

此入口复用 Agent 和 Runtime Profile 编辑器，不创建独立执行器平台。

1. 在 Agent 的 Requirements → Executors 中添加槽位（例如 `sandbox`）。
   capability 固定为 `workspace`；添加槽位本身不会启用节点能力。
2. 在 LLM 节点的 Workspace 工作区工具中选择该槽位，再逐项勾选工具：
   - `workspace_exec`：在 SDK Sandbox 工作区执行。
   - `workspace_save_artifact`：将工作区文件保存到 Artifact。
3. 节点实际使用的模型必须支持 `tool_call`，由 Deployment 发布校验。保存文件还要求同节点显式
   启用 Artifact 服务，界面不会代为开启。
4. 在 Runtime Profile → Executors 添加同名资源，类型为 `sdk_sandbox`。
   不填写 URL、主机路径或环境秘密。保存文件时另行配置现有 Artifact
   存储及凭据。执行环境由部署侧提供。
5. 分别保存、校验和发布 Agent 与 Profile，再在 Deployment 选择对应
   新版本并校验发布。页面声明不等于实际执行或附件已送达，最终以服务端
   校验及 Worker → Gateway 的真实验收结果为准。

关闭节点能力会删除 `workspace` 字段，取消最后一项工具也会删除该字段；
不会发布空 tools。删除被引用的 Executor Requirement 不会悄悄重绑节点，
原引用会保留并显示缺失诊断。旧 Agent/Profile 未声明 executors 时保持兼容。
已发布 Profile 的 Executors 只读，配置变更通过新 Draft 和新发布版本完成。
