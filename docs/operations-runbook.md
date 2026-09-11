# 安装与运行手册

所有命令在安装目录执行。首次安装与已有环境升级是不同流程：不要对已有数据库重新初始化，也不要覆盖已有凭据或删除安装数据卷。

## 1. Docker 单机安装

需要 Docker 和 Docker Compose v2。首次构建需能访问镜像、Go 和 npm 依赖源；宿主机不需要额外安装 Go、Node 或数据库。

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml up -d --build --wait
```

平台会生成管理员凭据与加密主密钥，启动 PostgreSQL/Redis、执行数据库迁移并启动服务。默认地址为 `http://127.0.0.1:18080/admin/ui/`，仅监听本机回环地址。

查看管理员凭据并登录：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml exec platform trpc-init -show-token
```

登录后依次创建工作空间、配置模型连接、创建 Agent、调试并发布。未配置模型服务时，内置演示模型只提供固定回复。

公开端口、构建代理和模型地址策略由 `deploy/compose/demo.env.example` 配置。构建代理可选择 `https://proxy.golang.org,direct` 或企业可信镜像，不在公开配置中填写带凭据的代理 URL。

查看状态与停止：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml ps
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml stop
```

重新启动使用首条启动命令，数据与凭据保留。不要执行 `down -v`：setup 卷保存管理员凭据、数据库密码和加密主密钥，postgres/redis 卷保存平台数据。备份时分别保护 setup 卷与数据库。

## 2. 公网服务器部署

以下按 **Ubuntu 24.04 服务器 + Docker Compose + Nginx** 首次部署。准备好：

- 一台公网服务器，已安装 [Docker 和 Compose](https://docs.docker.com/engine/install/ubuntu/)，并准备平台安装文件。
- 一个域名，A 记录指向服务器公网 IP。下文的 `agent.example.com` 都换成自己的域名；未配置 IPv6 时不要添加 AAAA 记录。
- 安全组和防火墙放行 **80、443**，SSH 仅允许管理来源。不要公开平台的 18080/8080 或数据库端口。服务器须能访问模型和 IM 接口。

已有平台实例不要重复初始化；从第 2 步配置 Nginx，并将模板上游端口改成现有端口。除注明“在自己电脑上执行”的命令外，其余均在**服务器安装目录**执行。

#### 1. 启动平台

复制部署配置并启动，首次构建需要等待几分钟：

```bash
mkdir -p data/server
test -f data/server/public.env || cp deploy/compose/server.env.example data/server/public.env
chmod 600 data/server/public.env

docker compose -p trpc-agent-server \
  --env-file data/server/public.env \
  -f compose.demo.yaml -f deploy/compose/server.yaml \
  up -d --build --wait

curl -fsS http://127.0.0.1:18080/readyz
```

返回 `{"status":"ready"}` 即启动成功。管理员 Token 和数据库密码自动生成；模型 API Key 在网页中配置。

#### 2. 配置域名和 HTTPS

先安装 Nginx 和证书工具。执行 `sudoedit` 时，把文件中的 `agent.example.com` 换成自己的域名。**已有同名配置或同域名站点时，先备份并合并，不要直接覆盖。**

```bash
sudo apt update
sudo apt install -y nginx certbot
sudo install -d -m 755 /var/www/letsencrypt/.well-known/acme-challenge
sudo install -m 644 deploy/nginx/http-bootstrap.conf /etc/nginx/conf.d/trpc-agent.conf
sudoedit /etc/nginx/conf.d/trpc-agent.conf
sudo nginx -t
sudo systemctl enable --now nginx
sudo systemctl reload nginx
```

申请证书，替换命令中的域名，按提示填写邮箱：

```bash
sudo certbot certonly --webroot \
  -w /var/www/letsencrypt \
  --cert-name agent.example.com -d agent.example.com \
  --deploy-hook "nginx -t && systemctl reload nginx"
```

证书申请成功后，换用 HTTPS 配置。这次编辑要替换**所有** `agent.example.com`，包括证书路径：

```bash
sudo cp --backup=numbered /etc/nginx/conf.d/trpc-agent.conf /etc/nginx/trpc-agent.http-backup
sudo install -m 644 deploy/nginx/https.conf /etc/nginx/conf.d/trpc-agent.conf
sudoedit /etc/nginx/conf.d/trpc-agent.conf
sudo nginx -t && sudo systemctl reload nginx
```

启用并检查证书自动续期：

```bash
sudo systemctl enable --now certbot.timer
sudo certbot renew --dry-run --run-deploy-hooks
```

#### 3. 登录管理页面

管理页面不对公网开放。**在自己电脑上执行**，将 `deploy` 和 IP 换成服务器的 SSH 用户名和 IP：

```bash
ssh -N -L 127.0.0.1:18080:127.0.0.1:18080 deploy@203.0.113.10
```

保持终端打开，在浏览器访问 **http://127.0.0.1:18080/admin/ui/**。

另开终端连接服务器，在安装目录获取登录用的管理员 Token（不要公开）：

```bash
docker compose -p trpc-agent-server \
  --env-file data/server/public.env \
  -f compose.demo.yaml -f deploy/compose/server.yaml \
  exec platform trpc-init -show-token
```

登录后依次完成：**创建工作空间 → 配置模型 → 创建 Agent → 调试 → 发布**。

#### 4. 连接机器人

**在自己电脑上执行**，检查公网地址：

```bash
curl -fsS https://agent.example.com/healthz
```

返回 `{"status":"ok"}` 后，在控制台 **机器人 → 服务地址** 填写 `https://agent.example.com`，不加其他路径。然后添加 Telegram Bot，平台会注册回调；给机器人发消息，收到回复即完成。

企业微信消息 MCP 不需要公网回调，按[IM 接入说明](im-channels.md)连接即可。

<details>
<summary>遇到问题或需要启停服务时再看</summary>

- 域名不通 / 证书申请失败：检查 DNS 和 80、443 端口。
- 返回 502：检查 `http://127.0.0.1:18080/readyz`，确认平台在运行、Nginx 上游端口正确。
- `/healthz` 返回欢迎页或 404：检查 Nginx 是否加载了本项目配置，域名是否已替换。
- 公网 `/admin/ui/` 返回 404：正常，管理页面通过第 3 步的 SSH 转发访问。
- 电脑的 18080 已占用：SSH 命令只改第一个端口为 18081，浏览器也改用 18081。
- 机器人不回复：在控制台查看消息记录，确认是未收到、执行失败还是发送失败。
- 网页公网检查失败、外部访问却正常：检查服务器是否能访问自身公网地址、内部 DNS 是否指向私网。

在服务器安装目录停止 / 再次启动：

```bash
docker compose -p trpc-agent-server \
  --env-file data/server/public.env \
  -f compose.demo.yaml -f deploy/compose/server.yaml stop

docker compose -p trpc-agent-server \
  --env-file data/server/public.env \
  -f compose.demo.yaml -f deploy/compose/server.yaml up -d --wait
```

**不要执行 `down -v`，它会删除数据卷。** 升级和备份见第 7 节。服务器重启后由 Docker 恢复服务，须确保 Docker 随系统启动；关闭 SSH 转发不会停止机器人。

改域名只影响新连接。已有 Telegram 连接需先暂停、核对未完成请求，再移除并重新添加；历史记录保留，新连接使用新会话。迁移服务器须保留数据库和加密主密钥。

</details>


## 3. 控制台与模型配置

### Agent 使用流程

1. 在工作空间选择器创建租户，进入“Agent 应用”创建 Agent。
2. 在“模型连接”填写兼容 OpenAI Chat Completions 的模型 ID、完整 Base URL 和 API Key。密钥加密保存，页面不回显。
3. 在工作台配置提示词、工具、知识库和预算，保存草稿。草稿不会修改已发布版本。
4. 在右侧调试，确认回复、工具审批与执行结果。修改配置后使用“新调试”创建新快照。
5. 检查配置差异后发布。灰度与回滚在发布记录中操作；已固定版本的 IM 会话不会自动换版本。
6. 按[IM 接入说明](im-channels.md)连接机器人并设置群和成员授权。

网页调试使用独立身份和会话，不发送 IM，也不开放任意外部写工具。每个会话最多 50 轮，执行最长 2 分钟，显示输出最多 64 KiB，数据保留 7 天。运行、审批和投递成功与否以平台记录为准，不以模型生成的文字为准。

### 模型地址与密钥

`TRPC_AGENT_MODEL_ENDPOINT_POLICY=public_https` 允许管理员配置兼容协议的公网 HTTPS 服务。需要限定供应商时使用 `allowlist`，并在 `TRPC_AGENT_MODEL_ALLOWED_ORIGINS` 配置精确 origin。

本地、内网或 HTTP 模型必须由部署者显式加入允许列表。允许列表不包含路径、凭据或通配符；网页中的 Base URL 则需包含服务实际要求的路径，例如 `/v1`。容器中的 `127.0.0.1` 不指向宿主机。

模型名称可直接更新；模型 ID 或地址变化会生成新配置版本，需要在 Agent 草稿中选择并重新发布。API Key 可以独立更新，后续请求读取新密钥；在途请求不强行中断。更换地址必须重新填写目标服务的 Key，供应商侧旧 Key 的撤销由管理员完成。

API Key 更新不等于加密主密钥轮换。不得直接替换 `TRPC_AGENT_MODEL_MASTER_KEY`，也不能丢失 setup 卷后重新生成主密钥。

## 4. 后端与 Skill

核心平台只需 PostgreSQL 和 Redis。其他资源按功能选择：

| 功能 | 所需配置 |
| --- | --- |
| 持久 Memory | Redis/PostgreSQL 后端绑定及 memory 用途授权 |
| 知识库 | Qdrant、独立 Embedding 服务、匹配的向量维度及租户授权 |
| 附件 | 私有 S3-compatible bucket、Artifact 后端绑定和通道附件授权 |
| Skill 执行 | 已注册 Skill、租户授权、固定镜像和专用沙箱执行环境 |

配置方式见[多后端适配](backend-adapters.md)。容器部署使用容器可达的资源地址，不照搬宿主机回环地址。S3 bucket 应预先创建，并使用限定前缀和操作范围的身份。

以随系统提供的 `json-digest` Skill 为例，将租户 ID 替换为实际授权租户：

```dotenv
TRPC_AGENT_SKILLS_ROOT=./skills
TRPC_AGENT_SKILL_GRANTS_JSON='[{"tenant_id":"tenant-a","name":"json-digest","version":"1"}]'
TRPC_AGENT_SANDBOX_ENABLED=true
TRPC_AGENT_SANDBOX_IMAGE=alpine:3.22
TRPC_AGENT_SANDBOX_SOCKET=/var/run/docker.sock
```

Worker 需要 Docker CLI，指定 daemon 中需已有镜像，镜像提供 `/bin/sh` 和 `/bin/busybox`。服务固定镜像 ID，不自动拉取。默认容器模板不开放 Docker socket；应配置专用执行节点或受控 daemon，不能将宿主机 root socket 暴露给租户。

在 Agent 工作台选择已授权 Skill。平台保存 name/version/checksum，并加入 `skill_load`、`skill_run` 白名单；执行仍需审批。正数 `max_tool_calls` 至少为 2，才能完成加载与执行；0 表示不限制调用次数。

`json-digest` 计算输入 JSON 的字节数和 SHA-256。执行结果通过有界工具输出返回。新增 Skill 通过 `skills/catalog.json` 注册新的目录和版本，不覆盖已发布版本；页面不提供任意脚本上传。

## 5. 宿主机安装

源码构建需要 [go.mod](../go.mod) 指定的 Go 版本、Node.js 22.12+ 和 npm。Linux 启停脚本还需要 flock 与支持 pidfd 的内核。

```bash
test -f .env || cp .env.example .env
chmod 600 .env
./build.sh
```

在私有配置中填写已准备的数据库与 Redis 地址，并启用持久化后端：

```dotenv
TRPC_AGENT_ROLE=all
TRPC_AGENT_ADDR=127.0.0.1:8080
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
TRPC_AGENT_POSTGRES_URL="部署者提供的 PostgreSQL DSN"
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false
TRPC_AGENT_SESSION_BACKEND=redis
TRPC_AGENT_COORDINATOR_BACKEND=redis
TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
TRPC_AGENT_QUEUE_BACKEND=redis
TRPC_AGENT_QUOTA_BACKEND=redis
REDIS_URL="部署者提供的 Redis URL"
TRPC_AGENT_ADMIN_ENABLED=true
TRPC_AGENT_ADMIN_TOKEN="独立生成的强随机凭据"
TRPC_AGENT_MODEL_MASTER_KEY="Base64 编码的 32 字节主密钥"
```

可用 `openssl rand -hex 32` 生成管理员凭据，使用 `openssl rand -base64 32` 生成主密钥。只在首次安装时生成，已有环境保留原值。

数据库迁移使用具备迁移权限的身份，运行服务使用对应角色的账号：

```bash
./bin/trpc-migrate -env-file .env
./start.sh
./bin/trpc-local status
```

管理页面位于 `http://127.0.0.1:8080/admin/ui/`。停止使用 `./stop.sh`。指定其他配置文件可用 `TRPC_AGENT_ENV_FILE=/path/platform.env ./start.sh`。进程环境变量优先于配置文件，启停脚本不将 dotenv 当作 shell 代码执行。

## 6. 多节点、监控与接口

生产部署将同一程序按 `gateway / relay / worker / sender / jobs / admin` 分别启动。Worker 共享控制面、队列、协调器和 Session/Memory 后端，不依赖负载均衡 sticky session。InMemory 不用于多节点共享。

Kubernetes 清单位于 [deploy/kubernetes](../deploy/kubernetes/platform.yaml)。部署顺序为：配置身份与后端 → 命名空间和网络策略 → 独立迁移 Job → 各角色 Deployment/Service → Ingress。运行身份不应获得数据库 DDL 权限，分角色授权见[治理说明](governance-operations.md)。

所需活跃并发约为“峰值 turn/s × 平均执行秒数”。业务 Worker 每进程默认 4 个执行槽，网页调试执行并发独立为 1；它们仍受租户预算和供应商并发额度约束。容量评估同时观察队列排空时间、完成与送达率、token、SQL/Redis QPS、GC 和取消时延，预留节点故障余量。

OpenTelemetry 配置使用 `TRPC_AGENT_OTEL_ENABLED`、OTLP endpoint、service name 和采样率。Collector、Tempo、Prometheus、Grafana 配置位于 `deploy/compose`；告警接收方和阈值由部署者设置。

HTTP 接口默认关闭。启用 `/chat`、`/inbound` 时必须配置精确的租户、Binding 和用户授权；`/chat` 返回完整回复，`/inbound` 返回 202 表示已持久接收，不表示 Agent 或 IM 投递已完成。Admin 使用独立凭据，不能复用模型或 IM Key。

## 7. 升级、备份与回退

1. 备份私有配置、加密主密钥、当前程序和数据库，核对未完成工具与 unknown/attempting 投递。
2. 停止接收与相关执行角色，避免混跑不兼容的消息、权限或数据库协议。
3. 使用迁移身份执行数据库迁移，更新最小权限配置，再启动对应版本。
4. 检查就绪状态、配置读取、会话历史和消息收发。
5. Agent 行为通过不可变 Revision 灰度或回滚；切换稳定版本不会自动迁移已固定版本的会话。
6. 回退前确认程序与数据库兼容。不得通过清空队列、幂等记录、预算或审计来恢复服务。

PostgreSQL 使用全量备份和 WAL/PITR；Redis 配置持久化与复制；对象存储启用版本化并校验 checksum；Qdrant 使用 snapshot。备份应在独立环境恢复确认。数据恢复不代表外部工具副作用或已发送消息被撤销，未知结果需要对账。

## 8. 排障

- `/healthz` 表示进程存活，`/readyz` 检查依赖就绪；两者均不能替代真实模型生成和 IM 投递确认。
- 在“系统状态”查看 Worker 心跳、后端与沙箱观测；过期或尚未观测的依赖显示未知。
- 在消息记录中区分未接收、等待模型、执行失败和投递失败；使用 request_id/trace_id 关联审计。
- 模型暂时不可用且尚无输出或工具执行时，请求进入 waiting 并退避恢复，不持续占用执行槽。
- 出站或工具结果 unknown 时先查证，不盲目重发或重跑。
- 宿主机日志位于 `data/trpc-service.log`；分享诊断信息时只提供错误类别、时间和关联 ID，不提供原始会话或密钥。

可用的诊断命令：

| 命令 | 行为 |
| --- | --- |
| `./bin/trpc-local status -env-file .env` | 只读检查进程与依赖 |
| `./bin/trpc-modelcheck -env-file .env` | 发起一次模型生成，会消耗额度 |
| `./bin/trpc-embeddingcheck -env-file .env` | 检查 Embedding 连通性和向量维度 |
| `./bin/trpc-wecomcheck -env-file .env` | 检查 MCP 初始化和工具接口，不读取业务消息或发送 |
