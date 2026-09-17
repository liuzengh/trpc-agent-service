# 渠道真机联调状态

此文档只记录可复现的真实渠道证据；单元测试、fake provider、WebUI 页面可打开以及 SDK 编译通过均不能标记为“真机通过”。每次联调完成后必须更新状态、日期、操作者、不可变证据位置和失败原因，避免把配置存在误写成协议已经验证。

| 渠道 | 当前状态 | 最近验证 | 必须保存的证据 | 下一次验证重点 |
| --- | --- | --- | --- | --- |
| WebUI local | 本地 Compose 验收入口，非外部真机 | 未登记 | 启动日志、`/readyz`、一次 request/reply 的 request ID 与 audit ID | 同一会话重投、artifact 下载、knowledge 检索 |
| 飞书 | 未验证 | 未登记 | Callback URL、平台 challenge/验签日志、入站 request ID、回执消息 ID、audit ID（均须脱敏） | URL verification、事件去重、主动回复和重投 |
| 企业微信 | 未验证 | 未登记 | 接收模式与协议版本、握手/验签日志、入站 request ID、回执消息 ID、audit ID（均须脱敏） | 若使用 WeCom WebSocket/WXKF，记录实际帧序列、重连与 ACK 语义 |

## 记录格式

每次真实联调在对应渠道下追加：

```text
日期（UTC）：
操作者：
环境与版本：git SHA、配置版本、渠道接收模式
输入：脱敏后的外部消息标识与 tenant/binding 选择依据
结果：accepted / rejected / retried，以及原因
端到端证据：request ID、reply/message ID、terminal audit ID、日志保存位置
已知差异与后续动作：
```

不得记录 token、签名原文、cookie、完整用户消息、SecretRef 内容或可重放回调载荷。若渠道协议实现发生重写，必须同时记录旧/新协议边界及回归结果；尤其不能只写“已连接”而缺少实际消息往返证据。
