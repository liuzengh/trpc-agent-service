# Workspace 执行与 Final 文件交付 V1

## 最小运行链

AgentSpec 的 `requirements.executors` 声明 workspace 槽位；LLM 节点显式选择
`workspace_exec` 和/或 `workspace_save_artifact`。Profile 将槽位绑定到
`{"kind":"sdk_sandbox"}`。Control 编译为 immutable Manifest 的 executor resource
和节点 workspace selection，Worker 逐节点装配已有 SDK sandbox/workspaceexec。
保存工具要求**同节点**显式启用 Artifact；executor 资源存在本身不赋予工具权限。

本次复用一个 Run / Runner / 正式 Session，不增加执行服务、工作流 DSL、MCP Shell
包装、后台队列、累计 Token 预算或第二套 Artifact 存储。

```text
IM 文本 → Manifest → 节点 workspace_exec → SDK Linux sandbox Bash
       → 工作区文件 → SDK workspace_save_artifact
       → 原 artifact.Service（S3 bytes + Worker PostgreSQL metadata）
       → 成功保存收据 → Stage / Complete accepted Final
       → Gateway mTLS 读取已接受的精确版本 → SDK SendDocument multipart
       → Channel Lab 原会话文件 / file_id 回执 / 下载字节校验
```

## 文件与正式接受

SDK 的 `out/report.txt` 等逻辑路径映射为
`SHA256(完整规范逻辑路径) + "-" + basename`。保留扩展名，不将不同目录中同 basename
文件混为一个文件；禁止绝对路径、遍历、非规范路径、控制字符，遵守既有 255 字节名称
上限。普通 `artifact_save` 的物理名称和行为不改变，也不会自动成为回复附件。

Final 的可选 `content.attachments` 只记录底层真实保存成功、且 SDK 保存调用整体成功
后的 `name/version/mime_type/size_bytes/sha256`。模型返回的 ref 或文字不是凭据。
版本从 0 开始；不以 latest 替换旧 Final 的版本。不把文件 bytes/base64 填入模型回复。

只有成功的 accepted Completion 可授权下载。失败、拒绝 parent、fence 不匹配和只有
candidate 的状态不会授权；Memory PENDING 隐藏附件，APPLIED 后释放，FAILED 清空
附件并重算固定失败 Final。Artifact 保存沿用即时持久化语义，不承诺因后续 Run 失败
撤销已保存对象，没有新增 GC/补偿事务。

Worker 在 Stage 前使用与 Complete 相同的 ReplyIntent codec 检查既有 1 MiB envelope，
不另设附件数量预算。Gateway 在 Plan 阶段检查文本分片加文件的总数是否超过既有
`gateway_delivery_intents.part_count <= 64` 数据库边界，避免延迟到 SQL 约束失败。
MIME 同时满足共享 schema 的 255 字符和既有 Worker 的 256 字节上限。

## 完成态下载与回调

Gateway 仅提交 intent/run/completion/name/version，不能选择 tenant/session 或携带
存储凭据。Worker 从 accepted ledger 读取真实 Final 证明与 Run，重新核验固定 Manifest，
通过独立 `resolve-final-artifact` 路由取得该 Artifact 的两项固定用途凭据。
不复用过期 Attempt lease；新响应的 `lease_epoch=0` 使用独立严格解码。

下载仍为 Gateway principal 专属，Final Verify 允许 Gateway 与 Control。下载和 proof
各有独立有界槽位，实际 MaxConcurrent=1 三腿 mTLS 回调测试证明不会自锁；第二个
下载仍受容量限制。Worker 校验完整 bytes/MIME/长度/hash 后返回 rawbytes；Gateway
再次校验再调用 Telegram SDK multipart。没有成功 document.file_id 就不记 ACCEPTED。

## 明确的 Linux 部署条件

参见 `deploy/compose/workspace/README.md` 和 workspaceadapter/README.md：

- 非 root，cap_drop ALL，只读 root，专属 `/workspace` tmpfs；不跨 Worker 共享。
- 固定禁止读取 `/run /config /credentials /proc /sys /root /home /tmp /app`，目录必须存在。
  秘密与配置仅放入这些禁止读取的挂载位置。
- SDK 环境继承 None，只显式保留 PATH/HOME；网络隔离；无宿主 unrestricted shell。
- 仅新 Worker 使用已批准的 Moby 默认 seccomp + 五项 namespace syscall 和
  `systempaths=unconfined`。没有 privileged、SYS_ADMIN、seccomp=unconfined。
- workspace 能力 Attempt 显式独占：可取消等待 → 创建 → SDK 执行 → join/清理 → 释放。
  普通无 workspace 的 LLM Run 不经过此锁。清理失败阻止后继复用残留工作区。

`Dockerfile.worker-workspace` 支持显式 `WORKSPACE_BASE_IMAGE`。本轮 Docker Hub 认证
端点超时，使用磁盘上已验证的 Alpine 3.21.7 基底镜像构建，真实 image ID/Worker binary
SHA 记录在联合验收的 `linux-worker-build.json`；没有把未拉取的默认 Alpine 3.22 说成
已部署。新 `workspace/compose.yaml` 是 opt-in overlay，不进入旧启动入口。

## 实际验收

```sh
GOCACHE=/tmp/trpc-worker-go-cache python3 scripts/test-worker-workspace-joint.py \
  --base-image worker-workspace-preflight:alpine \
  --artifacts /private/tmp/worker-workspace-artifact-20260909/joint
```

验收新建独立 PG/NATS/MinIO、真实 Control/Gateway/Lab 进程和 Linux Worker 容器；仅模型
HTTP 响应为确定 fixture。测试专用 Go TCP relay 保持 loopback 发布地址和原 TLS，仅转发
字节，不解析或授权请求，不包含在产品执行路径中。脚本不调用外部付费模型或共享 Bot。

已通过报告：`/private/tmp/worker-workspace-artifact-20260909/joint-r3/workspace-summary.json`。
完整事实含 Manifest、Run/Completion/Intent、accepted snapshot、PG metadata、S3 对象
字节/hash、Lab 回执和所有拒绝状态；临时资源清理 PASS。

| 场景 | 真实结果 |
| --- | --- |
| TXT 首版 | 21 bytes，version 0，Lab document 回执及下载 hash 相同 |
| CSV | 26 bytes，version 0，实际 CSV bytes 与下载相同 |
| TXT 第二版 | 22 bytes，version 1；旧 Final 仍读 version 0 原 bytes |
| 纯文本回复 | 无附件 metadata、无 document，原文本 Final ACCEPTED |
| 非法读 | 错版本/Completion 404，额外 tenant 400，Control 下载 403，跨 Run 组合 404 |

Linux 隔离负例针对实际可读取的 canary/live listener 做正负对照，覆盖敏感路径、proc、
env、网络、旧 tenant 工作区、取消等待、dirty root、进程 join 与后台子进程清理。
PG 独立 race 门禁 36 项含子例、0 skip，覆盖未 accepted/Memory 状态与版本/身份隔离。

首轮镜像拉取超时未进入业务；第二轮已经交付首份文件，但旧文本-only Lab 验收函数
错误地拼入附件 descriptor，报告 FAIL。修复验收文本分片及严格容器清理后第三轮四个
Run 全部通过。最后只收口 MIME 字符/字节与既有 Gateway 64 parts 边界并定向回归，
不重复付费 live，也不把源码单测当真实后端验证。

## 留待后续

真实 Telegram / 外部模型文件生成质量、workspace Attempt 真正并行、跨 Worker
调度、对象回收、崩溃恢复与更多后端组合均未由本次有限验收替代。当前常驻 managed-local
服务仍是此前部署版本；本批只完成独立真实运行闭环和可选部署材料，未自动升级常驻
Worker 权限，也未推送 main。
