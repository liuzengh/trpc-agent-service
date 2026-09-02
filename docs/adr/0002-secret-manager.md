# ADR-0002: 统一凭据管理（secret manager）

- 状态：已确认（2026-09-02，阶段 21）
- 决策者：用户 + AI 对齐（grill-with-docs）
- 关联术语：`CONTEXT.md`「凭据管理」分组

## 背景

平台两类敏感值长期散落、部分明文：`llm.Endpoint.APIKey` 明文存 MySQL（代码 TODO 早已标注「move to secret store (Phase 8)」）；`channel_bindings.credential_ref` 只是字符串引用、无落地的凭据存储。K8s 部署预留了 `model-api-keys` JSON 但 main 未消费。

## 决策

1. **范围**：统一管模型 api_key 与 IM 通道凭据两类（不做无第三类需求的通用 KV）。
2. **存储 + 加密**：MySQL `secrets` 表（DDL 010），**AES-256-GCM** 加密列；主密钥从 env `TRPC_SECRET_MASTER_KEY`（优先）或 config `secret.master_key` 注入，经 sha256 派生 32 字节 AES 密钥、只在内存、**永不落库**。无 MySQL 时用内存明文 store（dev）。**MySQL 下无主密钥 → 凭据存储禁用**（拒绝明文落盘）。
3. **引用模型**：统一 opaque key；模型约定 `endpoint:{id}`（`Endpoint.APIKeyRef`，`Resolve` 时经 `KeySource` 取用），通道用 `channel_bindings.credential_ref`。
4. **管理 API**：`POST/GET/DELETE /secrets`；GET 只返回 key + updated_at，**永不返回明文**。
5. **接口方向**：llm 定义 `KeySource`（`Get(ctx,key)`），由 `secret.Store` 满足，避免 llm 反向依赖 secret 包。

## 权衡

- 安全收益：消除 endpoints 表明文 api_key；主密钥泄露不影响密钥静态安全（仍需轮换）。
- 主密钥管理：本阶段依赖 env/config 注入（单机 Docker 部署够用）；外部 KMS/Vault、密钥轮换留后续——不在单机部署形态的 YAGNI 内。
- 兼容：`Endpoint.APIKey` 明文字段保留（向后兼容、dev 直连），新端点推荐 `APIKeyRef`。
- 未接线面：IM adapter 启动时的凭据取用（`credential_ref → Get`）随「IM 真实 SDK 接线」一并落地；`model-api-keys` K8s 预留的初始化导入未做（文档标注）。

## 验证

- secret 单测（AES 往返/不同主密钥无法解密/空主密钥拒绝）+ MySQL 集成（落库密文不含明文/往返/删除）。
- web SecretAPI handler 单测（不暴露明文/校验/删除）。
- llm KeySource 单测（APIKeyRef 取用/无 source 报错/legacy 明文兼容）。
