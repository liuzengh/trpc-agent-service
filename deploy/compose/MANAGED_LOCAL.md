# 一键本地全栈（Managed Local V1）

默认启动组合为 Web、Control、Worker、Gateway、PostgreSQL、NATS、Redis、
Qdrant、MinIO；不需要额外 profile，不是只启动存储的示例。

## 启动

要求 Docker Engine + 支持 `!reset` 的 Compose v2（本轮实测版本见验证记录）、Go 1.25+、Python 3.11+、OpenSSL；命令从任意
目录调用。首次生成配置/证书/服务密码并构建镜像；重复调用复用原私密 state。

```sh
just managed-up
```

从仓库根目录调用 `just managed-up`。第一次运行可传
`--state-dir /absolute/private/directory --project distinct-project`；已有 state 的项目、
端口、身份和密码保持固定，不因环境中的旧 DSN 改为另一套数据库。

Go 服务由宿主交叉编译（使用宿主 module cache）并装入 distroless 镜像，运行镜像
的构建上下文仅含 binary 和 Dockerfile。overlay 清除旧 Go build 配置，避免直接
`compose build` 覆盖为另一种二进制入口；构建 Go 镜像使用统一启动脚本。Web 使用根 context 的专用 Dockerfile，
仅 COPY web/ 与 api/，保留完整类型检查；根 `.dockerignore` 排除环境文件、
私钥、历史 artifacts、依赖与构建缓存。私密 state 不在仓库内。
`--skip-build` 只用于明确复用已经构建的五个镜像；不是首次启动的默认行为。

## 入口与隔离

默认 project：`trpc-agent-managed-local`。

| 服务 | 宿主入口 | 容器网络 |
| --- | --- | --- |
| Web | http://127.0.0.1:23000 | web:3000 |
| Control API | http://127.0.0.1:28080 | control-api:8080；内部 mTLS 8081/8082 |
| Gateway | http://127.0.0.1:28090 | channel-gateway:8090 |
| Gateway readiness | http://127.0.0.1:28091/readyz | channel-gateway:8091 |
| Worker readiness | http://127.0.0.1:28083/readyz | agent-worker:8083；内部 mTLS 8082 |
| PostgreSQL / NATS | 不发布宿主端口 | postgres:5432 / nats:4222 (TLS) |
| Redis / Qdrant / MinIO | 不发布宿主端口或管理控制台 | redis:6379 / qdrant:6333 / minio:9000 |

新 project 使用自己的一个 PostgreSQL 实例、一个 `agent_platform` 数据库。
Control/Gateway/Worker/Session/Memory 共用这一个数据库，以五个 schema、十个
非管理员迁移/运行账户隔离，不连接已有共享项目的数据库。Summary 跟随 Session，
没有摘要库。Redis Session 与 Memory 共用物理实例但分账户及 key 前缀。

持久命名卷均以 project 前缀隔离：`control-postgres-data`、`gateway-nats-data`、
`managed-redis-data`、`managed-qdrant-data`、`managed-minio-data`。Redis 固定
ACL default off、AOF、appendfsync always、noeviction；MinIO 为专用应用账户
配置单 bucket 权限，Profile 不使用 root 凭据；Qdrant 要求 API key。

`database-schemas`、`memory-schema`、`session-prepare`、`memory-prepare`、
`nats-reconcile`、`backend-init` 是有界一次性准备步骤，成功退出是正常状态。
`backend-tools` 是无 Docker socket 的私有网络健康探针容器；其持续健康检查同时
检查 Redis、MinIO、Qdrant。不能把 job exit 0 等同于后台服务一直健康。

## 私密配置和后端授权

默认目录：`~/.local/share/trpc-agent-service/managed-local`，目录 0700、文件 0600。

- `compose.env`：Compose 用到的固定配置与凭据，勿复制到仓库或聊天。
- `state.json`：可重渲染的固定身份/密码源，不删除或自动轮换。
- `secrets/bootstrap_password`：首次登录密码，用户名见 `metadata.json`。
- `secrets/redis_session` / `redis_memory`：相应 Profile storage 的 `dsn_password`。
- `secrets/pg_session` / `pg_memory`：PostgreSQL 后端密码。
- `secrets/minio_app_user` / `minio_app_password`：Artifact 的 `access_key_id` / `secret_access_key`。
- `secrets/qdrant_api_key`：显式配置 Knowledge 后使用的 Qdrant 凭据。
- `managed/catalog.json` / `targets.json`：平台授权及内部目标；只挂给 Control。
- `control-runtime` / `control-channel` / `worker-runtime` / `gateway-*` / `nats-tls`：角色隔离的配置与证书。

应用使用宿主文件 owner 的非 root UID/GID读取 0600 文件；不放宽为全员可读。
首次登录会要求改密码。改密后以新的数据库登录密码为准，初始 secret 文件不是
自动更新的密码管理器。不要删除 state 再对已有卷启动，也不要手工只改一侧密码。

初始 catalog 无 tenant 授权，不捏造 tenant ID。先正常登录并创建真实 tenant，
再在尚未发布业务的独立环境中显式授权：

```sh
just managed-grant ACTUAL_TENANT_ID
```

该命令生成静态 tenant allowlist、固定 catalog/targets 文件 SHA，使用实际 Control
镜像计算 contract digest，并同时更新 Control 与 Worker pin，再重启这两个应用。
当前公开 API 是 `GET /v1/tenants/:tenant_id/runtime-backends`，没有 grant 写 API。
目录可见不等于 Agent 自动启用 Memory/Summary/Artifact/Knowledge；Profile 仍需
明确选择 backend、写入凭据，AgentSpec 声明能力后才可发布。

**已承载业务的项目不应直接更换 catalog/pin。** 应先停止输入、排空运行、确认发布
与绑定切换方案；本入口的 grant 用于当前独立新环境，不迁移旧 revision 或 Bot。

## 外部配置边界与替换后端

默认不配置聊天模型、Embedding 或 IM。启动脚本只声明自己未配置这些业务凭据，不据此判断操作者后来在 Control 中添加的配置。
基础设施和应用 HTTP 可 ready，Agent 执行
与真实 IM 收发仍是 `NOT_CONFIGURED`，不伪造 Agent 成功。Qdrant 无业务维度时
保持基础设施可用，业务 collection 不创建，Knowledge 明确未配置，不默认 1536。

需要 Knowledge 时，先以生成器 `grant --help` 查看显式维度/collection/vector/distance
参数，绑定与实际 Embedding 相同的值，再执行 `just managed-up --skip-build`
计算 pin、运行 backend-init。已启用 backend revision 1 的维度/距离等字段不接受改写；不能把存储
连通性的一维专用 smoke collection 当业务 Embedding。所有初始化均拒绝覆盖维度
或距离不匹配的既有 collection。

外部托管后端继续使用同一 managed catalog/targets 协议：在独立私密发布配置中
显式固定 backend endpoint/账号/隔离字段与凭据，重新校验 SHA/contract/pin；Profile
继续持有角色用途凭据。不修改本地 state 生成器后偷偷让 next up 覆盖私有配置。
本地启动默认托管三种后端；外部替换是显式发布操作，不是自动发现或迁移。

## 运行与验收

```sh
just managed-config              # 只校验，不打印展开后的秘密
just managed-status
just managed-restart-backends    # 同卷重启 PG/Redis/Qdrant/MinIO
just managed-stop                # 停服务，保留容器和卷
just managed-up --skip-build     # 复用配置、镜像和卷恢复
```

联合验收入口为 `scripts/test-compose-managed-data.py`：bootstrap 创建专属 owner/tenant；
显式 grant 后 before 检查目录与未发布 Profile 凭据、数据库隔离和后端写入；操作者
同卷重启后 after 核实 StartedAt 确实变化与逐字读回，再清除专属探针。该脚本不创建
Agent/Deployment/Bot，不调用模型，也不启动/停止服务。详见脚本 `--help`。

本地测试与未来共享服务升级是两回事：本入口不替换 `trpc-agent-latest` 或
`channel-lab-dev`，不切换旧 Bot、不删除卷、不执行隐式数据迁移。

## 本轮部署验收记录（2026-09-09）

独立项目 `trpc-agent-managed-local` 已通过：真实 bootstrap / tenant grant / catalog
回读、未授权租户空目录、未发布 Profile 凭据只写不回显、五 schema/十 role 与二十项
跨 schema 拒绝、四种数据库/对象存储同卷重启读回。Redis 同时核验实际 AOF/always/
noeviction 与角色 ACL。Qdrant 额外以独立三维 Dot named-vector 探针验证幂等和维度
冲突拒绝，随后删去探针；不代表业务 Embedding 已配置。

完整 `up` 重复执行通过，39 个秘密/PKI 文件逐字不变、五个命名卷名称不变；六个
准备任务成功退出。原始证据：`/private/tmp/worker-compose-managed-20260909/`。
新环境保留运行；旧 `trpc-agent-latest` 与 `channel-lab-dev` 未替换。

验收创建的独立 `managed-admin` 已按首次登录协议改密，其当前密码仅保存在私密
`smoke-private.json` 的 `admin_password` 字段（另有专属 smoke owner），不再是
`secrets/bootstrap_password` 的初始值。该文件不入仓库。验收仅留下专属 tenant 与
未发布 Profile draft；没有 Agent、Deployment、模型凭据或 IM 绑定。

当前生成的目录提供 PG Memory、Redis Memory/Session、S3 Artifact；Qdrant 项显示
未配置。PostgreSQL Session 沿用既有 `postgres_state` Profile 绑定及准备流程，不把
当前目录描述为已新增 managed PostgreSQL Session 适配器。
