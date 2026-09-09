# IM 通道接入与本地验证 Spec

> 输入：导师陈雷 8/21 群内答复（截图存档于任务会话）。
> 要点：①「至少两种 IM」必须在代码中实现，Web UI 不能替代 IM；② 可自行写一个网页版 IM（类 deepseek 网页版）做本地验证；③ 抽象统一 IM 类适配企微/微信/飞书；④ 企微真实配置验证导师后续代填。
> 本 spec 落地上述结论，并收敛方案文档 3.3 / 第 6 节的 IM 策略。

---

## 1. 结论与范围

| # | 决策 | 说明 |
| --- | --- | --- |
| 1 | 代码实现两种 IM | **企业微信 + 微信客服**，与方案文档 3.3 差异对比表及验收口径一致 |
| 2 | Web UI 定位 | `webchat` 通道：本地端到端自证手段 + 演示加分项，**不计入**「两种 IM」；但实现同一 Adapter 接口，作为第三通道 |
| 3 | 统一抽象 | `channels.Adapter`：验签/解析/归一化/发送；新通道以插件接入，不改核心链路 |
| 4 | 协议层策略 | 默认**手写最小协议实现**（零新增第三方依赖，配合 go.mod 9/9 冻结）；接口隔离实现，若企微调试成本超 0.5 天，按导师建议换成熟社区 SDK（保留替换点） |
| 5 | 飞书 / Telegram | 接口预留，不实现 |

## 2. 架构与接口契约

```
IM webhook / 浏览器 ──▶ Gateway(/callback/{channel}/{tenant_id})
                          │  Adapter.Callback：验签 + 写协议级 ACK + 归一化
                          │  msg_id 去重（现内存 TTL；Storage Adapter 落地后换 Redis SETNX）
                          ▼
                      按 tenant_id 取 Runner ──▶ runner.Run（流式事件）
                          ▼
                      Adapter.Send(chunk…/final) ──▶ IM 发送接口 / 浏览器 SSE
```

```go
// channels.Adapter：一种 IM 协议与归一化消息模型的桥梁。
type Adapter interface {
    Type() Type
    // Callback 验签并解析一次 webhook，返回 0..n 条消息（拉取型协议如
    // 微信客服 sync_msg 一次事件对应一批）；成功路径必须自行写协议级 ACK
    // （企微/微信客服 "success"、echostr 探测等）；返回 nil,nil 表示探测已应答。
    Callback(w http.ResponseWriter, r *http.Request) ([]*InboundMessage, error)
    // Send 投递一条回复（Chunk=true 为流式分片，Done=true 为结束标记）。
    Send(ctx context.Context, msg *OutboundMessage) error
}
```

- **session_id 规则**复用方案文档 3.3：单聊 `{tenant}:{channel}:{user}`，群聊 `{tenant}:{channel}:{group}`；
- **回复路径统一异步**：所有通道都是「入向请求立即 ACK → Runner 执行 → Adapter.Send 推送」，与企微「先 ACK 后主动推送」主链路同构，webchat 用 SSE 承担推送；
- **流式语义分层**：Gateway 逐 chunk 调 `Send`；IM 类 Adapter 在内部聚合/节流（按段追加、卡片更新），webchat 直接透传 chunk 到 SSE。

## 3. 三个通道实现要点

### 3.1 webchat（本切片，9/2 当天可用）

- `POST /callback/webchat/{tenant}`，body `{user, msg_id, text}` → 202；`msg_id` 缺省由服务端生成；
- `GET /webchat/stream?tenant=&user=`：SSE 推送 `{text, chunk, done}`；无订阅者时 Send 静默丢弃（等同 IM 用户离线）；
- 页面为单 HTML，`go:embed` 于 `trpcservice/web`，含租户下拉（`GET /api/tenants`）、消息气泡、流式渲染。

### 3.2 企业微信（9/2–9/3）

- URL 验证：`echostr` 解密回显；回调：AES-CBC 解密 XML（`msg_signature` = SHA1(sort(token, timestamp, nonce, encrypt))）；
- 立即 ACK `"success"`，异步执行后经**应用消息主动推送**（被动回复 5s 不作主链路）；
- `gettoken` 缓存（2h TTL，提前刷新）；单条 2048 字节切分 + 频控退避；
- 单测用官方文档向量：签名校验、AES 加解密往返、XML 解析。

### 3.3 微信客服（9/7 已实现，提前于 9/8 排期）

实现前按官方文档 94670/94677 重新核查，修正了本节旧稿的两处认知：回调加密与企微**同一套方案**（`msg_signature` = SHA1(sort(token,timestamp,nonce,encrypt))，AES-256-CBC，receiveid=corp_id，XML 信封），并非独立的简化验签；回调只推**事件通知**，消息需主动拉取：

- 回调解密后为 `kf_msg_or_event` 事件（携带 `Token` + `OpenKfId`），先 ACK `"success"` 再拉取；
- `kf/sync_msg`：携带 cursor（每轮成功后保存，故障续拉不丢消息）+ 事件 Token（10 分钟有效，免频控）+ open_kfid，`has_more=1` 续拉（单轮上限 1000，总轮数上限 10）；只归一化 `msgtype=text && origin=3`（微信客户发送）的消息；首次拉取（无 cursor）跳过进程启动前的历史，避免重放 3 天旧消息；
- `kf/send_msg`：回复目标 = `external_userid` + 回调时按会话记录的 `open_kfid`；流式 chunk 聚合至 Done，2048 字节 rune 边界切分；access_token 独立缓存（微信客服 secret ≠ 企微应用 secret），失效重试一次；
- 48h 会话窗/5 条限制为平台侧约束，代码侧无法预判，过期表现为 send_msg errcode（方案文档 3.3 已述降级：落库待补发/转人工，二期）；
- 已知限制：cursor 为内存态，重启后从 3 天内最早消息重拉，靠 msg_id 去重（10 分钟 TTL）+ 启动时间护栏兜底；持久化 cursor 待 Storage Adapter 二期扩展；
- 配套改动：`Adapter.Callback` 批量化（返回 `[]*InboundMessage`），Gateway 对同一批顺序 dispatch 保序，跨批仍并行；
- 单测同 3.2 策略：复用企微官方加密向量 + mock qyapi 服务端，9 个用例覆盖拉取分页/过滤/增量 cursor/验签失败/聚合切分发送。

## 4. 验证矩阵

| 通道 | 本地单测 | 本地端到端 | 真实验证 |
| --- | --- | --- | --- |
| webchat | — | 浏览器全流程（本切片验收方式） | 同左 |
| 企业微信 | 文档向量单测 + mock 回调集成 | mock IM 服务端 | **导师代填真实配置运行** |
| 微信客服 | 文档向量单测 + mock 回调集成 | mock IM 服务端 | 争取沙箱；不成则导师统一验证 |

## 5. 排期（与方案文档第 7 节收敛）

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 9/2 | Adapter 抽象 + Gateway + webchat 端到端；企微 Adapter 开工 | 浏览器可对话 |
| 9/3 | 企微 Adapter + 官方向量单测；Admin API 切片 2（租户 CRUD/通道绑定/原子持久化/Registry 热更新）；路由串行化占位（Gateway 按 session_id 串行 dispatch，含 -race 并发单测） | 企微代码路径可测；管理链路可用；同会话并发安全 |
| 9/4 ✅ | 数据层（Redis session 换 inmemory）提前完成，见 docs/spec-storage-redis.md | 共享后端落地，重启不丢上下文 |
| 9/7 ✅ | 微信客服 Adapter + 9 个单测（提前于 9/8 排期）；Adapter.Callback 批量化 + Gateway 批内保序 dispatch | **两种 IM 代码全部完成** |
| 9/9 | go.mod 与接口冻结 | 进入联调 |

## 6. 验收映射

- 原题「至少两种 IM」：企微 + 微信客服**均有代码实现与单测**（微信客服已于 9/7 提前补齐）；企微真实验证由导师代填，微信客服以 mock 验证，均符合导师 8/21 口径；
- Web UI：自证手段（导师明确建议形态）+ 演示入口；
- 方案文档 3.3 差异对比表继续作为「两类通道差异」验收材料。
