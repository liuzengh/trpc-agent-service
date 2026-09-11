# 安装与运行手册

所有命令在安装目录执行。首次安装与已有环境升级是不同流程：不要对已有数据库重新初始化，也不要覆盖已有凭据或删除安装数据卷。

本文提供两种独立的安装方式：第 1、2 节通过 Docker Compose 管理容器；[第 5 节](#5-宿主机安装)通过根目录 `start.sh`、`stop.sh` 管理宿主机进程。启停时使用对应方式的命令，脚本不会管理 Compose 容器，Compose 也不会停止脚本启动的宿主机进程。

## 1. Docker 单机安装

需要 Docker 和 Docker Compose v2。首次构建需能访问镜像、Go 和 npm 依赖源；宿主机不需要额外安装 Go、Node 或数据库。

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml up -d --build --wait
```

平台会生成管理员凭据与加密主密钥，启动 PostgreSQL/Redis、执行数据库迁移并启动服务。默认地址为 `http://127.0.0.1:18080/admin/ui/`，仅监听本机回环地址。

这套安装流程不要求手工创建根目录 `.env`，配置来源如下：

| 配置 | 来源与用途 |
| --- | --- |
| `deploy/compose/demo.env.example` | 仓库自带的公开部署配置，由命令中的 `--env-file` 读取；设置端口、构建代理和模型地址策略，不存放密钥 |
| `/setup/platform.env` | 初始化容器生成，保存在 `setup` 数据卷中；包含服务配置、数据库与 Redis 地址、管理员凭据和加密主密钥，供迁移程序与平台读取 |
| `/setup/postgres-password` | 初始化容器生成的数据库密码文件，保存在同一数据卷中，供 PostgreSQL 读取 |

初始化仅为新安装生成凭据，已有安装继续使用原数据卷。模型服务和 IM 账号不由安装程序提供，需在平台启动后配置。

请完整保留命令中的 `--env-file` 和 `-f compose.demo.yaml`。默认 `compose.yaml` 只提供依赖服务，不能用裸的 `docker compose up` 替代本节的完整平台安装命令。

查看管理员凭据并登录：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml exec platform trpc-init -show-token
```

登录后依次创建工作空间、配置模型连接、创建 Agent、调试并发布。未配置模型服务时，内置演示模型只提供固定回复。

公开端口、构建代理和模型地址策略由 `deploy/compose/demo.env.example` 配置。构建代理可选择 `https://proxy.golang.org,direct` 或企业可信镜像，不在公开配置中填写带凭据的代理 URL。

查看状态与停止使用同一组 Compose 参数，不使用根目录的 `./stop.sh`：

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

### 网页配置数据后端

1. 进入“资源中心 → 数据后端”，点击“新建连接”，填写便于识别的连接名称。
2. 选择保存的数据和后端类型。Redis/PostgreSQL 填写主机、端口和数据库；Qdrant 填写 Collection 与向量维度；S3/MinIO 填写 Endpoint、Bucket 和访问密钥。InMemory 不显示凭据字段。
3. 保存连接。真实凭据加密保存，列表不回显；保存仅校验格式，不代表已通过连通性验证。
4. 在连接列表点击“绑定到 Agent”，选择应用，或作为工作空间默认后端。已有同类绑定不会被覆盖，后端切换需使用受控迁移流程。

新的外部地址与凭据仅由平台管理员配置。租户管理员可以选择当前空间已有连接，或创建 InMemory 连接。已使用环境变量管理凭据的部署，平台管理员可选择“已授权凭据”，无需手工填写引用。SQL 表、Bucket、网络连通性和运行身份权限由部署者准备。

<a id="knowledge-setup"></a>

### 知识库：手动配置与资料导入

**知识库后端已实现文本入库、删除、租户隔离检索和迁移。当前需要手动配置，资料通过 Admin API 导入；“资源中心 → 知识库”仅展示已绑定的存储后端，没有文档上传或编辑入口。** 修改环境变量不会自动创建绑定、导入资料或增加网页按钮。

以下流程面向首次接入。建议先使用尚未绑定机器人的 Agent 完成验证；已有业务应用应按发布和迁移流程变更，不直接替换其向量库或 Embedding 模型。

#### 1. 准备后端并记录标识

- 准备平台容器可访问的 Qdrant 和提供 `/embeddings` 接口的模型服务。默认 Docker 安装不会启动 Qdrant，也不提供真实 Embedding 服务。Qdrant 连接使用 gRPC 端口，通常为 `6334`；不要误填 HTTP 管理端口 `6333`。
- 向供应商确认 Embedding 模型 ID、API Base URL、API Key 和输出维度。聊天模型不一定支持 Embedding，不能直接照搬聊天模型配置。
- 在“租户与策略 → 查看详情”记录工作空间的 `tenant_id`；Agent 工作台地址中的 `/agents/<app_id>` 是应用 ID。下文的 `YOUR_TENANT_ID`、`YOUR_APP_ID` 等占位值必须替换为这些真实标识，而不是显示名称。

#### 2. 配置 Embedding 密钥与授权并重启

在平台实际读取的私有配置文件中设置以下两项；不要写入 `.env.example` 或公开部署模板：

```dotenv
TRPC_AGENT_EMBEDDING_API_KEY='替换为真实的Embedding服务密钥'
TRPC_AGENT_SECRET_GRANTS_JSON='[{"tenant_id":"YOUR_TENANT_ID","purpose":"embedding","reference":"env://TRPC_AGENT_EMBEDDING_API_KEY"}]'
```

如果已有 `TRPC_AGENT_SECRET_GRANTS_JSON`，将这个授权对象合并到原有 JSON 数组，保留其他租户和用途的授权；不要覆盖原数组，也不要在文件中重复定义同一变量。不同租户使用不同密钥时，为每个密钥设置独立环境变量，并在对应授权的 `reference` 中引用。

**Docker 单机安装：** 服务读取 setup 卷中的 `/setup/platform.env`，不读取根目录 `.env`。下面借用初始化服务的可写挂载，先在同一私有卷中备份配置，再用编辑器修改；不会重新运行初始化程序。需要 Docker 操作权限，编辑时不要复制、截图或公开文件中的现有密码和主密钥。

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml \
  run --rm --no-deps --entrypoint /bin/sh init -c '
    set -eu
    knowledge_backup=$(mktemp /setup/platform.env.before-knowledge.XXXXXX)
    cp -p /setup/platform.env "$knowledge_backup"
    vi /setup/platform.env
    chown 65534:65534 /setup/platform.env
    chmod 600 /setup/platform.env
  '

docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml restart platform
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml \
  exec -T platform wget -q -O - http://127.0.0.1:8080/readyz
```

`vi` 中按 `i` 编辑，按 Esc 后输入 `:wq` 保存退出。仅添加或修改 Embedding 密钥和授权；保留数据库地址、管理员 Token、加密主密钥及其他配置。最后的就绪检查应返回 `{"status":"ready"}`，启动中可稍后重试。不要执行 `down -v`。上面的配置副本只用于回退编辑，不代替独立备份。

第 2 节的服务器安装须沿用自己的 `-p trpc-agent-server --env-file data/server/public.env -f compose.demo.yaml -f deploy/compose/server.yaml` 参数，避免修改到另一套安装。

**宿主机安装：** 备份并编辑实际使用的 `.env`（或 `TRPC_AGENT_ENV_FILE` 指向的文件），然后执行 `./stop.sh` 和 `./start.sh`；自定义配置文件重启时仍须指定同一个 `TRPC_AGENT_ENV_FILE`。分角色部署需把 Embedding 密钥和精确授权提供给 Admin、Worker、Jobs，并重启这些角色。无需给 Gateway、Sender 分发该密钥。

#### 3. 绑定存储并配置 Agent

1. 在“资源中心 → 数据后端”新建连接，保存数据选择“知识库”，后端选择 Qdrant，填写主机、gRPC 端口、TLS、Collection、向量维度和所需 API Key。使用有权访问目标 Collection 的凭据；维度必须与 Embedding 输出一致。保存连接只校验配置格式，不代表连通性已验证。
2. 点击“绑定到 Agent”，绑定到目标应用，或作为工作空间默认后端。返回“知识库”标签页应能看到绑定记录。
3. 在 Agent 工作台“知识与记忆”开启知识库检索，选择 `OpenAI-compatible Embedding`，填写模型 ID、API 地址、向量维度及返回结果数；凭据选择 `env://TRPC_AGENT_EMBEDDING_API_KEY`。若下拉框为空，检查真实 `tenant_id`、`purpose=embedding` 和服务是否已重启。不要选“本地 Hash”代替真实语义检索。
4. 保存并校验配置。首次接入的未绑定机器人应用可发布这个版本，再到“发布记录 → 查看配置”复制 `revision_id`，供入库接口使用。草稿 ID、版本序号和应用 ID 不能代替 `revision_id`。发布本身不会导入资料；已有业务应用不要未经验证直接切换稳定版本。

#### 4. 通过 API 导入文本

使用有该工作空间写权限的 Admin Token。默认单机安装的原登录 Token 即可；它与 Embedding API Key 不是同一个密钥。接口仅支持已提取的文本 `content`，不是文件上传地址；PDF/Word 需自行转换为文本。

以下命令在本机 Bash 或 Zsh 终端执行。Token 通过隐藏输入读取并从标准输入传给 curl，不写进命令历史或 curl 参数；不要开启 shell 跟踪或 curl verbose。远程部署使用 SSH 转发到本机端口，或改成受保护的 HTTPS 管理地址。

```bash
printf 'Admin Token: '
read -r -s KNOWLEDGE_ADMIN_TOKEN
printf '\n'

knowledge_api() {
  printf 'Authorization: Bearer %s\n' "$KNOWLEDGE_ADMIN_TOKEN" |
    curl --noproxy '127.0.0.1,localhost' --fail --silent --show-error --max-time 30 \
      --header @- --header 'Content-Type: application/json' \
      --data-binary "$2" "http://127.0.0.1:18080$1"
}

knowledge_api /admin/knowledge/documents '{
  "tenant_id": "YOUR_TENANT_ID",
  "app_id": "YOUR_APP_ID",
  "revision_id": "YOUR_REVISION_ID",
  "document_id": "handbook-001",
  "operation_id": "handbook-import-001",
  "name": "业务手册",
  "content": "这里填写已提取的正文。平台会按版本配置分块并生成向量。",
  "metadata": {}
}'
```

默认 Docker 安装会返回 HTTP 202 和 `job_id`，表示任务已入队，不代表入库完成。将该 ID 填入下面的请求，重复查询直到 `Status` 为 `completed`；`pending`、`running` 表示尚未完成，`dead` 表示失败，应检查任务错误与后端配置。接口地址中的 `127.0.0.1:18080` 是宿主机访问地址，不要改成容器内 Qdrant 地址。

```bash
knowledge_api /admin/jobs/get '{"tenant_id":"YOUR_TENANT_ID","job_id":"YOUR_JOB_ID"}'
```

也可在 Agent 工作台的“后台任务”查看执行状态。不启用后台任务的部署可能直接返回 HTTP 200 和 `chunks`，这时没有任务需要轮询。

同一应用中再次提交相同 `document_id` 会替换其内容。请求响应不确定时，重发同一请求沿用原 `operation_id`；新修改或删除后重新导入使用新的操作 ID。任务已是 `dead` 时，修复配置后调用 `POST /admin/jobs/retry`，请求体同上面的任务查询，不能仅重复提交入库请求。知识资料按租户和应用隔离，不是每个 Revision 的私有副本；回滚 Agent 配置不会回滚已导入的资料。

删除资料使用相同鉴权方式：

```bash
knowledge_api /admin/knowledge/documents/delete '{
  "tenant_id":"YOUR_TENANT_ID",
  "app_id":"YOUR_APP_ID",
  "revision_id":"YOUR_REVISION_ID",
  "document_id":"handbook-001",
  "operation_id":"handbook-delete-001"
}'
```

仅在确实要删除时执行删除请求，并按相同方式确认其任务完成。完成所有操作和查询后，清理终端中的 Token 和函数：

```bash
unset KNOWLEDGE_ADMIN_TOKEN
unset -f knowledge_api
```

#### 5. 验证检索

入库完成后，在 Agent 工作台新建调试会话，询问资料中可核实的问题，并检查运行记录中 `knowledge_search` 的执行和返回内容。没有调用检索工具、没有检索结果或模型只是泛泛回答，都不能算验证通过。首次接入验证后再绑定机器人；变更 Embedding 模型或维度时走重建或迁移流程，不只改模型名称。

### 网页上传与授权 Skill

1. 进入“资源中心 → Skill 目录”，点击“上传 Skill”。平台管理员和当前租户管理员可提交。
2. 填写名称和版本，选择必需的 `SKILL.md` 和可选的 `run.sh`，也可上传仅含这些文件的 ZIP。ZIP 可使用根目录，或一个与 Skill 名称一致的文件夹；不接受其他文件、符号链接和路径跳转。`SKILL.md` 的 YAML 头部须包含对应的 `name` 和 `description`。
3. 上传后状态为“待审核”，不会执行脚本。平台管理员通过“查看 / 审核”检查正文和脚本，再批准对当前工作空间授权。
4. 在 Agent 工作台选择已批准的具体版本，保存草稿并按通常流程调试、发布。新版本不会自动替换已发布 Agent 的引用。

每个文本文件最多 64 KiB，ZIP 最多 256 KiB，每租户最多 128 个上传版本。版本内容和校验值不可修改，变更内容必须使用新版本号。撤销授权会阻止新运行和后续脚本执行，不会强行终止已经开始的沙箱进程。上传版本保存在共享 PostgreSQL 中，其他节点无需重启即可读取审核结果；备份数据库时一并保存。

仅有 `SKILL.md` 的说明型 Skill 通过 `skill_load` 加载，不需要沙箱。带 `run.sh` 的版本执行脚本前需部署者显式配置隔离执行环境；只使用网页上传版本时，不需要设置本地 Skill 目录：

```dotenv
TRPC_AGENT_SANDBOX_ENABLED=true
TRPC_AGENT_SANDBOX_IMAGE=alpine:3.22
TRPC_AGENT_SANDBOX_SOCKET=/var/run/docker.sock
```

Worker 需要 Docker CLI，指定 daemon 中需已有镜像，镜像提供 `/bin/sh` 和 `/bin/busybox`。服务固定镜像 ID，不自动拉取。默认容器模板不开放 Docker socket；应配置专用执行节点或受控 daemon，不能将宿主机 root socket 暴露给租户。

在 Agent 工作台选择已授权的具体版本。平台保存 name/version/checksum，说明型只启用 `skill_load`，脚本型同时启用 `skill_run`；脚本执行仍需审批。脚本型的正数 `max_tool_calls` 至少为 2，才能完成加载与执行；0 表示不限制调用次数。

项目不附带预置 Skill。需要从本地目录加载时，由部署者自行准备资源目录，在目录下的 `catalog.json` 中登记名称、版本和子目录，并设置 `TRPC_AGENT_SKILLS_ROOT` 与 `TRPC_AGENT_SKILL_GRANTS_JSON`。网页上传不依赖本地目录，也不能覆盖本地已注册的同名同版本 Skill。

## 5. 宿主机安装

本节直接运行宿主机上的 `bin/trpc-service`，不使用初始化容器，也不读取 Docker 安装的 `/setup/platform.env`。启动前需自行准备 PostgreSQL、Redis、访问凭据和数据库迁移；`start.sh` 不会自动创建这些依赖或生成安装凭据。

启动脚本默认读取仓库根目录 `.env`，可通过 `TRPC_AGENT_ENV_FILE` 指定其他文件。`start.sh`、`stop.sh` 仅启停本仓库登记的宿主机服务进程，不启停数据库或 Docker 容器。

源码构建需要 [go.mod](../go.mod) 指定的 Go 版本、Node.js 22.12+ 和 npm。Linux 启停脚本还需要 flock 与支持 pidfd 的内核。

```bash
test -f .env || cp .env.example .env
chmod 600 .env
./scripts/build.sh
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

网页存储连接与 Skill 上传需要 schema 32，迁移增加资源表和不可变版本约束，不清空已有空间、模型、会话或凭据。分角色部署还需补齐 Admin 对新资源表的管理权限、Worker 对 Skill 表的读取权限及 Jobs 对存储凭据的读取权限；沿用已有主密钥，不重新初始化安装数据卷。

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

宿主机部署可运行 `./bin/trpc-local status -env-file .env`，只读检查进程与依赖。模型调用可在 Agent 工作台调试验证，会消耗模型额度。
