> **历史记录，已被替换。** 本文中的授权／预算实现已从当前运行路径撤出。当前业务范围、代码接线及迁移说明见 [最小身份与连续对话](minimal-identity-session.md)。以下保留原日期和证据，不表示当前仍启用。

# 模型 token 总预算发布契约

本增量对应优化设计 F01 的共享消耗预算，不引入计费服务或新的 workload。

## 字段与语义

在既有 quota definition 中增加可选字段 `max_total_model_tokens`：

```json
{
  "expected_revision": 0,
  "definition": {
    "enabled": true,
    "quota": {
      "public_limited": false,
      "max_concurrent_runs": 2,
      "max_runs_per_minute": 10,
      "max_total_model_tokens": 1000000
    }
  }
}
```

- 值为 0 到 9007199254740991 的整数。拒绝 null、字符串、负数和小数。
- 缺失表示没有配置模型消耗预算；不是无限额度，也不能据此授权新的模型调用。
- 显式 0 表示模型消耗额度为零，必须保留字段，不能序列化成缺失。
- 这是租户 model_tokens 维度的累计总上限，涵盖输入与输出 token。
  与预算账本一致，改变 policy ID/revision 不重置已发生消耗和未知预留。
  没有隐含的日/月重置、周期额度、价格换算或货币计费。
- 现有 Run 并发和每分钟配额字段的含义不变；旧文档不增加默认字段，摘要保持兼容。
- 停用策略不授予新调用。结算已发生消耗仍依赖原始预留和真实 usage，
  不应因策略停用而抹去历史消耗。

## 所有权与完整性

Control 通过既有 OWNER 校验、revision CAS、幂等命令回执及事务 outbox 发布该字段。
字段进入完整文档的 JCS 摘要和发布请求 MAC。修改、删除字段或用同一幂等键提交
不同额度会被现有完整性机制拒绝。领域构造器复制可选指针，调用方后续修改输入
不影响已构造的不可变候选。

公开 publish/document schema、公共/内部 OpenAPI 内嵌 schema、Control 领域和公共
Go consumer 类型同步。旧消费者仍可能因闭合 schema 拒绝新字段；应先升级消费者，
再发布含此字段的新 revision，不声称旧二进制具备前向兼容性。

## 本轮实现与未完成项

本轮实现的是生产策略配置与发布/读取契约。真实 PostgreSQL 验证 publication、
幂等冲突、零值版本、不可变历史、双 outbox 事件及 consumer 摘要核验。
测试使用隔离 schema 的既有数据库夹具，不等于正式运行角色与部署验收。

此字段本身不是当前授权证明，也不是单次最大消耗证明。Worker 仍需将最新有效
策略与租户/主体/运行目标授权共同核验，并校验真实模型输入与最大输出约束，
才能创建新预留和执行模型。Before-model、新调用预算 authority 和完整 PendingRun
接纳尚未接线；不能把本字段发布成功解释为硬预算已经生效。
