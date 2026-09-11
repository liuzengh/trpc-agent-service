# 平台配置指南 (Platform Configuration)

本目录用于存放服务的平台级装配配置，定义了网络服务参数、大模型 Provider 目录、知识库全局策略以及敏感凭据的安全白名单。

---

## 一、配置文件与加载规则

- **默认配置文件**：服务启动时默认读取 `configs/platform.json`（若不存在可从 `platform.example.json` 复制）。
- **命令行指定**：可通过 `-config` 启动参数指定其它路径：
  ```bash
  ./bin/trpc-service serve -config configs/platform.json
  ```
- **安全约定**：
  平台配置文件中**严禁明文包含任何 API Key、密码或签名密钥**。所有敏感凭证必须通过 `env:<VAR_NAME>` 格式引用系统环境变量，且该变量名必须显式声明在 `allowed_secret_refs` 白名单中。

---

## 二、核心配置项说明

以 `configs/platform.example.json` 为例，结构如下：

```json
{
  "service": {
    "listen_address": ":8080",
    "request_timeout": "15s",
    "http_read_header_timeout": "15s",
    "http_read_timeout": "5m",
    "http_idle_timeout": "2m",
    "max_inbound_bytes": 1048576,
    "outbox_retention_age": "168h",
    "docling_endpoint": "http://127.0.0.1:5001",
    "document_extract_timeout": "5m",
    "knowledge": {
      "embedding_provider_id": "openai-primary",
      "embedding_model": "text-embedding-3-small",
      "embedding_dimensions": 1536,
      "reranker": {"type": "topk", "top_n": 5},
      "allowed_source_hosts": ["github.com", "raw.githubusercontent.com"]
    },
    "allowed_secret_refs": ["env:MODEL_API_KEY", "env:TELEGRAM_EXAMPLE_CONFIG_JSON", "env:TOOL_API_TOKEN"],
    "channel_credential_refs": ["env:TELEGRAM_EXAMPLE_CONFIG_JSON"],
    "tool_credential_refs": ["env:TOOL_API_TOKEN"]
  },
  "model_providers": [
    {
      "id": "openai-primary",
      "type": "openai",
      "base_url": "https://api.openai.com/v1",
      "api_key_ref": "env:MODEL_API_KEY",
      "models": []
    }
  ]
}
```

### 1. `service`（服务网络与运行时）

| 字段 | 类型 | 默认值 / 示例 | 说明 |
| :--- | :--- | :--- | :--- |
| `listen_address` | string | `":8080"` | 服务监听的 HTTP 地址与端口。 |
| `request_timeout` | string | `"15s"` | 常规 HTTP 请求处理总超时时间。 |
| `http_read_header_timeout`| string | `"15s"` | HTTP 读取请求头的最大超时（防御 Slowloris 攻击）。 |
| `http_read_timeout` | string | `"5m"` | HTTP 请求体读取最大超时（支持多模态大文件流式上传）。 |
| `http_idle_timeout` | string | `"2m"` | Keep-Alive 空闲连接保持时间。 |
| `max_inbound_bytes` | int | `1048576` (1MB) | 单次 HTTP 请求体最大允许字节数。 |
| `outbox_retention_age` | string | `"168h"` (7天) | 异步可靠下发 Outbox 记录在数据库中的保留与自动归档周期。 |
| `docling_endpoint` | string | `"http://127.0.0.1:5001"` | 知识库深度文档解析服务地址（Docling）。 |
| `document_extract_timeout`| string | `"5m"` | 文档解析与文本提取的最大单任务超时。 |

### 2. `service.knowledge`（知识库与 RAG 全局策略）

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `embedding_provider_id` | string | 默认向量化使用的 Provider ID（对应 `model_providers` 中的 `id`）。 |
| `embedding_model` | string | 默认向量模型名称（例如 `text-embedding-3-small`）。 |
| `embedding_dimensions` | int | 向量维度（例如 `1536`）。 |
| `reranker` | object | 重排策略，例如 `{"type": "topk", "top_n": 5}`。 |
| `allowed_source_hosts` | array | 知识库支持从远程下载同步文档的白名单域名列表（防止内网 SSRF 探测）。 |

### 3. 安全凭证白名单机制

为了防止未授权配置通过环境变量注入间接读取宿主机敏感凭证（如云平台 AK/SK、数据库密码等），系统强制采用三级白名单校验：

- **`allowed_secret_refs`**：全平台允许通过 `env:XXX` 引用的环境变量名总白名单；
- **`channel_credential_refs`**：允许注入给 IM 渠道适配器的环境变量白名单（例如 Telegram Token、企微/飞书 Secret）；
- **`tool_credential_refs`**：允许传递给外接 Tool 执行的环境变量白名单。

任何未在白名单中声明的 `env:<VAR>` 引用在服务装配时均会被系统拒绝并报错。

---

## 三、`model_providers`（大模型供应商目录）

定义平台支持的模型供应商与定价规则。例如：

```json
{
  "id": "openai-primary",
  "base_url": "https://api.openai.com/v1",
  "api_key_ref": "env:MODEL_API_KEY",
  "models": [
    {
      "name": "gpt-4o-mini",
      "prompt_cost_micros_per_million_tokens": 150000,
      "cached_prompt_cost_micros_per_million_tokens": 15000,
      "completion_cost_micros_per_million_tokens": 600000
    }
  ]
}
```

- **`id`**：平台内唯一的 Provider 标识符；
- **`base_url`**：兼容 OpenAI API 协议的端点地址；
- **`api_key_ref`**：指向安全环境变量的引用声明；
- **`models`**：该 Provider 下暴露的模型列表，包含每百万 Token 的输入、缓存命中与输出成本微美元计费参数（用于多租户使用量与账单统计）。

---

## 四、Artifact 对象存储接入范例（S3 / MinIO）

当租户希望把 tRPC-Agent-Go Artifact 保存到 S3 / MinIO 时，可参考 `configs/platform.s3.example.json`。Knowledge 的权威源文档当前仍由 PostgreSQL `knowledge_document_sources` 保存，不能把 S3 Artifact Profile 描述成 Knowledge 后端：
1. 在 `allowed_secret_refs` 中加入 `"env:S3_CONFIG_JSON"`；
2. 在环境变量中注入包含 S3 完整凭证与配置的 JSON 字符串：
   ```bash
   export S3_CONFIG_JSON='{
     "endpoint": "http://127.0.0.1:9000",
     "region": "us-east-1",
     "bucket": "trpc-artifacts",
     "access_key": "replace-access-key",
     "secret_key": "replace-secret-key",
     "use_ssl": false,
     "use_path_style": true
   }'
   ```
