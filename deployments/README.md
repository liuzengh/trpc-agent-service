# 部署与建表说明

本目录承载三块交付物，用于提高项目的可移植性：

| 目录/文件 | 说明 |
| --- | --- |
| `mysql/init/*.sql` | MySQL 建表语句（9 个迁移文件，按文件名序号顺序执行） |
| `docker-compose.yml` + `.env.example` | 本地完整栈编排（MySQL/Redis/Milvus/MinIO/后端/前端 + 观测栈） |
| `k8s/*.yaml` | Kubernetes 部署清单（namespace/configmap/secret/各组件/后端/前端/观测/Ingress） |
| `backend-compose.config.yaml` | compose 后端运行时配置（挂载覆盖镜像默认 dev 配置） |
| `prometheus.yml` | compose Prometheus 抓取配置 |

> **后端接线已就绪**：`main.go` 已接入 MySQL/Redis/Milvus/telemetry（不再是 dev InMemory）。
> 运行配置通过单个 YAML 文件加载：compose 挂载 `backend-compose.config.yaml`，K8s 从
> Secret 挂载（配置含 DSN 密码，不入 ConfigMap）。

---

## 一、建表语句（评审借鉴结论）

参考 DDL（Spring AI 项目）经过**评审借鉴而非照搬**，与本项目对齐后的关键差异：

| 参考 DDL | 本项目最终设计 | 理由 |
| --- | --- | --- |
| `org_id`（组织） | `tenant_id`（VARCHAR 字符串） | 项目采用 **Tenant→Agent 两级**模型；`tenant_id` 同时是 session `AppName`、Redis key 前缀、对象存储路径前缀，是天然主键 |
| endpoint 仅 org 范围 | `scope` GLOBAL/TENANT 双作用域 | 对应「共享资源是租户级资产」的决策 |
| `api_key` 明文存储（MVP 让步） | `api_key_ref` 引用密钥库 | 密钥不落库明文、不落日志/trace |
| agent `status` 0/1 二态 | `draft/published/disabled` 三态 + 不可变 `agent_versions` | 版本控制 + 原子回滚 |
| `skill.content TEXT`（单表） | `skills` + `skill_versions` + `skill_scripts` + `skill_references` 四级模型 | 资产沉淀：所有权/版本/生命周期/共享边界 |
| `ai_employee_skill`（M:N 绑定） | `agent_skills`（含版本锁 + sort_order） | 版本原子切换，不影响进行中会话 |
| `chat_session`/`chat_message` | 保留「事实账本」思路 | 与「prompt 上下文滑窗」解耦，`turn_id`+`turn_timestamp` 分页，`message_id` UUID 防枚举 |

**四级 Skill 语义**（对齐你在参考 DDL 中的描述）：

- `skills`：稳定身份 + 元数据 + 当前版本指针（`current_version`）。
- `skill_versions`：版本快照，承载 SKILL.md 正文（`content_md`）+ prompt 模板 + 执行器配置。
- `skill_scripts`：随版本打包的**可执行脚本**，供 SKILL.md/引用文件调用，精确高效完成具体任务。
- `skill_references`：随版本打包的**知识库/引用文件**。
- `agent_skills`：agent 安装了哪些 skill 的关联表（M:N，锁版本）。

> 核心认知：skill 以 MySQL 行存储，运行时加载为文本注入给大模型；文件/内存字符串/数据流只是存储形式差异，模型读到的都是文字。

**重要边界**：会话/事件的**运行时数据**由 tRPC-Agent-Go 的 `session/mysql` 后端自建表（`session_states`/`session_events`/`session_summaries`/`app_states`/`user_states`，可配前缀）。本项目的 `chat_sessions`/`chat_messages` 是**业务账本**，两者互补、不冲突。

---

## 二、Docker Compose（本地完整栈）

```bash
cd deployments
cp .env.example .env        # 按需改密码
docker compose up -d --build
```

| 服务 | 端口 | 说明 |
| --- | --- | --- |
| frontend | 5173 | nginx 托管 SPA，`/api/*` 反代到后端 |
| backend | 8080 | 多租户 Agent 服务（挂载 `backend-compose.config.yaml`：MySQL/Redis/Milvus/OTLP 全接） |
| mysql | 3306 | 首次启动自动执行 `mysql/init/*.sql` |
| redis | 6379 | Streams 消息总线 + session/memory 后端 |
| etcd + minio | — | Milvus standalone 依赖（内部） |
| milvus | 19530/9091 | 向量库 standalone（v2.5.6，BM25 全文检索） |
| artifact-minio | 9002/9003 | 产物对象存储 |
| otel-collector | 4317/4318/8889 | OTLP 接收 → Jaeger trace / Prometheus metrics（配置 `../configs/otel-collector.yaml`） |
| jaeger | 16686 | 分布式追踪 UI（traces 经 collector 汇聚） |
| prometheus | 9090 | 指标查询（抓取 collector :8889 Prometheus 导出端点） |

> 后端只读 YAML 配置，不支持环境变量注入；如需改连接串直接编辑
> `backend-compose.config.yaml`（或利用 `.env` + compose 变量拼入——见该文件注释）。

## 三、Kubernetes（生产推荐）

```bash
# 1) 建命名空间 + 观测配置 + 密钥（含后端 config.yaml 与 DSN）
kubectl apply -f deployments/k8s/00-namespace.yaml
kubectl apply -f deployments/k8s/01-configmap.yaml
kubectl apply -f deployments/k8s/02-secret.yaml

# 2) 建 MySQL 初始化 ConfigMap（从建表文件生成）
kubectl -n trpc-agent create configmap mysql-init --from-file=deployments/mysql/init/

# 3) 部署基础设施 + 应用 + 观测
kubectl apply -f deployments/k8s/03-mysql.yaml
kubectl apply -f deployments/k8s/04-redis.yaml
kubectl apply -f deployments/k8s/05-minio.yaml
kubectl apply -f deployments/k8s/06-milvus.yaml
kubectl apply -f deployments/k8s/07-backend.yaml
kubectl apply -f deployments/k8s/08-frontend.yaml
kubectl apply -f deployments/k8s/09-observability.yaml
```

要点：

- **后端配置**：`02-secret.yaml` 的 `stringData.config.yaml` 承载完整运行配置（含 MySQL DSN）；
  `07-backend.yaml` 从该 Secret 挂载 `/etc/trpc-service/config.yaml`。改配置后需重启后端。
  生产用 **External Secrets Operator + Vault/云 KMS** 替换 Secret；模型 API Key 绝不落日志/trace。
- **观测栈**：`09-observability.yaml` 部署 otel-collector/jaeger/prometheus；
  `01-configmap.yaml` 的 `observability-config` 承载其非敏感配置。
- **Milvus**：清单为 standalone（dev/预览）。生产用官方 **milvus-helm** 或 **Milvus Operator**（HA、备份、滚动升级）。
- **无状态 Worker**：`07-backend.yaml` 副本数可横向扩展（HPA 已配，CPU 70%→2~10 副本），
  会话/记忆走共享后端（Redis/MySQL），无需 sticky session。
- **Ingress**：`08-frontend.yaml` 中 `host: agent.example.com`，按需改域名 + 加 TLS。

---

## 四、镜像构建

```bash
# 后端（仓库根）
docker build -t trpc-agent-service:latest .

# 前端
docker build -t trpc-agent-service-front:latest ./front
```

K8s 清单中镜像 tag 为 `latest`，推送到你的镜像仓库后改为实际地址。
