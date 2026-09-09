# 微信客服通道（wxkf）

微信客服（企业微信 App 内「微信客服」能力）通道。它和 wecom 的关键区别是**消息不是推过来的，
是拉过来的**：回调只推一个「有新消息」的事件指针，消息本体要用 `kf/sync_msg` 带游标主动拉取。
实现位于 `trpcservice/channels/wxkf/wxkf.go`。

> **实测状态**：当前实现按官方「事件回调 + `sync_msg` 拉取」协议编写（入站指针事件
> `kf_msg_or_event`、游标持久化、48h 窗口出站），但**未在真实微信客服账号上端到端实测**。

## 1. 适用场景与取舍

| 适合 | 不适合 |
|---|---|
| 面向**微信侧外部用户**的客服会话（用户在微信里直接聊） | 企业内部员工沟通 —— 用 [wecom](./wecom.md) |
| 单聊客服场景，能接受 48 小时会话窗口 | 需要群聊、需要随时主动触达（窗口外发不出去） |

与 wecom 一样是 webhook 模型，需要**公网 HTTPS 回调入口**；无公网入口的场景考虑
[wecomws](./wecomws.md)。wxkf 仅处理 text 消息，媒体消息降级为占位文本。

## 2. 微信客服后台配置

1. 企业微信管理后台 →「应用管理」→「微信客服」，开通后拿到 **客服账号 ID（open_kfid）**。
2. 「微信客服」→「API」里拿到独立的 **Secret**（注意：是微信客服自己的 secret，
   **不是**自建应用的 corpsecret，`wxkf.go` 用它换 `access_token`）。
3. 配置回调 URL：
   - `https://你的域名/wxkf/callback`（env 单绑定默认路径），
     或多租户路径 `https://你的域名/callback/wxkf/{binding_id}`；
   - Token / EncodingAESKey 同 wecom 的「接收消息」机制，保存时同样有 GET 验证请求。
4. 企业 ID（corpid）在「我的企业」。

## 3. 密钥文件与平台配置

```bash
echo -n '你的Token'          > data/secrets/wxkf-token     # = TRPC_WXKF_TOKEN_REF 默认值
echo -n '你的EncodingAESKey' > data/secrets/wxkf-aeskey    # = TRPC_WXKF_AESKEY_REF 默认值
echo -n '你的KF Secret'      > data/secrets/wxkf-secret    # = TRPC_WXKF_SECRET_REF 默认值
chmod 600 data/secrets/wxkf-*
```

**两个 env 都设了通道才挂载**：

```bash
TRPC_WXKF_CORP_ID=ww你的corpid TRPC_WXKF_KF_ACCOUNT=wk你的open_kfid ./start.sh
```

| env | 默认 | 说明 |
|---|---|---|
| `TRPC_WXKF_CORP_ID` | `""` | 企业 ID（与 `TRPC_WXKF_KF_ACCOUNT` 同时设置才启用） |
| `TRPC_WXKF_KF_ACCOUNT` | `""` | 客服账号 open_kfid |
| `TRPC_WXKF_TOKEN_REF` | `wxkf-token` | 回调 Token 引用名 |
| `TRPC_WXKF_AESKEY_REF` | `wxkf-aeskey` | EncodingAESKey 引用名 |
| `TRPC_WXKF_SECRET_REF` | `wxkf-secret` | 微信客服 secret 引用名（独立于 corpsecret） |
| `TRPC_WXKF_API_BASE` | `https://qyapi.weixin.qq.com` | API 端点 |

多租户绑定：`config` 只接受 `corp_id` / `kf_account` / `secret_ref` 三个键
（未知字段 400），缺省回退 env 全局；回调验签用绑定行的 `token_ref`/`aeskey_ref`。
绑定不可解析时发送直接失败，不回退全局身份。

## 4. 平台硬性约束（来源：微信客服官方文档）

- **回调只推指针**：回调是一个加密 XML 事件（`MsgType=event`、`Event=kf_msg_or_event`），
  只带 `Token` 和 `OpenKfId`，**不含消息本体**。本通道在回调处理内同步调
  `/cgi-bin/kf/sync_msg` 拉取（每页上限 1000 条，单次回调最多翻 5 页），拉取失败回 5xx，
  让平台重推事件、下次从已存游标续拉。
- **回调 Token 10 分钟有效**：事件里的 `Token` 是 sync_msg 的拉取凭证，过期作废——
  所以拉取必须在回调处理内完成，不能异步推迟。
- **`sync_msg` 只能拉最近 3 天的历史**：`next_cursor` 游标按 open_kfid **持久化在
  Redis**（`CursorStore`，进程挂了、多副本都共享）。游标丢了会从 3 天前重拉——
  为重拉窗口兜底，wxkf 的幂等键 TTL 被加宽到 **4 天**（`storage.DedupTTLWxkf`，
  dedup/sent/done 同），重拉的消息被入口去重吸收，不会重复进 LLM。
- **出站 48 小时窗口 + 最多 5 条**：只能在用户最后一次发消息后的 48 小时内回复，
  且窗口内最多发 5 条（`/cgi-bin/kf/send_msg`）。窗口外发送返回 errcode 95020。
- **仅单聊、仅 text**：用户 ID 是微信客服维度的 `external_userid`（与企微的不互通）；
  客服/系统消息（origin 4/5）不进流水线；markdown 回复会经 `channels.RenderPlain`
  降级成纯文本。
- **出站幂等**：`send_msg` 的 `msgid` 取 `{入站msgid}-{分段序号}`，重试期间保持稳定——
  「请求发出但响应丢了」的重试被微信侧按 msgid 去重，用户不会收到重复回复。

## 5. 常见错误码与处理

| errcode | 含义 | 本平台的处理 |
|---|---|---|
| 40014 / 42001 | access_token 无效 / 过期 | 自动失效该 `(corp_id, secret_ref)` 的缓存 token，重取后重试一次；持续出现核对 `data/secrets/wxkf-secret` |
| 95020 | 48 小时会话窗口外发送 | 平台限制，无法绕过：出站报错、留 pending 重试，最终超次数进 `stream:deadletter` 并告警；只能等用户再次发消息重开窗口 |
| 45009 | 接口限频 | 出站令牌桶排队兜底（`TRPC_SEND_RATE_QPS`/`_BURST`）；持续出现收紧租户 `rate_policy.send_qps` 或申请提额 |

## 6. 故障排查入口

```bash
grep -a 'wxkf' data/trpc-service.log | tail -50
```

| 日志关键字 | 含义 |
|---|---|
| `wxkf url verification failed` | 后台保存回调 URL 验签失败：核对 corpid / Token / EncodingAESKey |
| `wxkf skip callback msgtype=... event=...` | 非 `kf_msg_or_event` 事件，正常跳过 |
| `wxkf pull for <open_kfid> failed` | sync_msg 拉取失败，已回 5xx 等平台重推事件 |
| `wxkf sync_msg for <open_kfid> returned an empty next_cursor` | 平台回了空游标，已保留旧游标（空游标落库会让下次从 3 天前重拉） |
| `wxkf sync for <open_kfid> hit the 5-page cap` | 单次回调翻页上限打满，剩余由下一个事件续拉；持续出现说明消息量超过单回调拉取能力 |
| `wxkf send rejected` + errcode | 出站被拒（常见 95020，见上表） |

指标：`im_inbound_total{channel="wxkf"}`、`im_outbound_total{channel="wxkf",result}`。
排查动作详见 [运维手册 §5](../operations.md#5-故障排查-runbook)。
