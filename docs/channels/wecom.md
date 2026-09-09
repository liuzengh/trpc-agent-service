# 企业微信应用消息通道（wecom）

企业微信「自建应用」回调通道：企微把应用消息以 **AES 加密 XML** POST 到平台的公网回调地址，
平台验签解密后入队，Worker 异步调用 LLM，回复经企微「应用消息」接口主动下发。
实现位于 `trpcservice/channels/wecom/wecom.go`。

> **实测状态**：协议已逐项对照企微官方文档（验签、解密、5s 应答、`message/send`、
> `media/get`、撤回事件）核对无发现，但**未在真实 corp 上端到端实测**——接入前先按
> 本文「验证」一节过一遍。三个通道中实测过的是 wecomws（见
> [`wecomws.md`](./wecomws.md)）。

## 1. 适用场景与取舍

| 适合 | 不适合 |
|---|---|
| 有公网 HTTPS 入口（域名 + 可信证书），回调模型成熟 | 纯内网、无域名 —— 用 [wecomws](./wecomws.md) 长连接 |
| 需要群聊机器人（`appchat/send`）、媒体消息、markdown 回复 | 面向微信外部用户的客服场景 —— 用 [wxkf](./wxkf.md) |

webhook 与 WS 的根本取舍：**webhook 需要公网入口**，企微侧会验证回调 URL 并要求 5 秒内应答；
**WS 免公网**（平台主动出站连接），但每 bot 只允许一条连接，需要额外的 leader 互斥机制。

## 2. 企微管理后台配置

1. [企业微信管理后台](https://work.weixin.qq.com/) →「我的企业」拿到 **企业 ID（corpid）**。
2. 「应用管理」→ 创建自建应用，拿到 **AgentId** 和 **Secret（corpsecret）**。
3. 应用详情 →「接收消息」→ 设置 API 接收：
   - **URL**：`https://你的域名/wecom/callback`（env 单绑定默认路径），
     或多租户路径 `https://你的域名/callback/wecom/{binding_id}`（每个绑定用自己的密钥验签）；
   - **Token** 与 **EncodingAESKey**：自定义或随机生成，等下要写到 `data/secrets/`。
4. 保存时企微会向该 URL 发 **GET 验证请求**（`msg_signature`/`timestamp`/`nonce`/`echostr`），
   本通道用 `wxbizmsgcrypt` 验签解密后原样回显 `echostr`（`wecom.go` 的 `verifyURL`）。
   所以**先把服务起起来、URL 可达，再回后台点保存**。

## 3. 密钥文件与平台配置

密钥不落库、不进 env 明文，只存**引用名**，文件名必须等于 `*_REF` 的值
（机制见 [guide 附录 A](../guide.md#附录-a配置与密钥机制)）：

```bash
echo -n '你的Token'            > data/secrets/wecom-token    # = TRPC_WECOM_TOKEN_REF 默认值
echo -n '你的EncodingAESKey'   > data/secrets/wecom-aeskey   # = TRPC_WECOM_AESKEY_REF 默认值
echo -n '你的corpsecret'       > data/secrets/wecom-secret   # = TRPC_WECOM_SECRET_REF 默认值
chmod 600 data/secrets/wecom-*
```

启动时加通道开关（`TRPC_WECOM_CORP_ID` 不设 = 通道不挂载）：

```bash
TRPC_ADMIN_TOKEN=dev-insecure \
TRPC_WECOM_CORP_ID=ww你的corpid \
TRPC_WECOM_AGENT_ID=1000002 \
./start.sh
```

| env | 默认 | 说明 |
|---|---|---|
| `TRPC_WECOM_CORP_ID` | `""`（禁用） | 企业 ID；设了才挂载通道 |
| `TRPC_WECOM_AGENT_ID` | `""` | 自建应用 AgentId |
| `TRPC_WECOM_TOKEN_REF` | `wecom-token` | 回调 Token 的引用名 |
| `TRPC_WECOM_AESKEY_REF` | `wecom-aeskey` | EncodingAESKey 的引用名 |
| `TRPC_WECOM_SECRET_REF` | `wecom-secret` | corpsecret 的引用名（换 `access_token`） |
| `TRPC_WECOM_API_BASE` | `https://qyapi.weixin.qq.com` | 企微 API 端点 |

**多租户**：每个租户的绑定行（`channel_binding`）带自己的 `token_ref`/`aeskey_ref`，
回调按绑定各自的密钥验签（`/callback/wecom/{binding_id}`）；出站身份从绑定的
`config` 解析，字段全部可选、缺省回退 env 全局：

```bash
curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
  -d '{"channel":"wecom","token_ref":"acme-wecom-token","aeskey_ref":"acme-wecom-aeskey",
       "config":{"corp_id":"ww...","agent_id":1000003,"secret_ref":"acme-wecom-secret"}}'
```

`config` 只接受 `corp_id` / `agent_id` / `secret_ref` 三个键，其余一律 400
（`wecom.ValidateBindingConfig` 用 `DisallowUnknownFields`，防止明文密钥混进审计明细）。
绑定行查不到或 `config` 解析失败时**发送直接失败**，不会回退全局身份——
否则一个租户的回复会以另一个企业的机器人发出。

**验证**：日志出现 `gateway listening {"addr":":8080"}`，且没有 `wecom callback mount failed`
之类的 ERROR。

## 4. 平台硬性约束（来源：企微官方文档）

- **5 秒应答**：企微要求回调 5 秒内应答，超时/失败会重推（最多 3 次）。LLM 生成 P95 远超
  5s，所以平台统一走「验签去重 → 立即回 `success` → 异步入队 → Worker 消费 → 主动发送」，
  回调里不做任何生成。
- **单条 text ≤ 2048 字节**：超长回复由 `channels.SplitText` 按字节切分（不切坏 UTF-8），
  在同一次 `Send` 内**串行**分段下发；中间段失败报 partial delivery，留待出站重试。
  单聊 `message/send` 带 `enable_duplicate_check=1`（30 分钟窗口），重试已发出的前缀段
  会被企微侧吸收，不会重复送达。
- **卡片消息**：单聊回复可渲染 `template_card`（`card_type: text_notice`）——`OutboundMessage.Card`
  非空且单聊时整卡一次发送（不参与分段），当前由危险操作待确认通知使用（`guardrail.go`），
  卡片描述复用脱敏后的文案。群聊 `appchat/send` 不支持 template_card，自动回退纯文本；
  其余通道始终用 `Text` 兜底，因此生产者必须始终填好 `Text`。
- **应答时间窗**：回调签名不过期，平台侧防重放靠消息自带 `CreateTime`——偏离当前时间
  ±5 分钟（`channels.CallbackTimestampWindow`）的回调直接丢弃并 ack。
- **消息类型**：text 正常进 LLM；image/voice/file 先经 `/cgi-bin/media/get` 拉素材落
  Artifact，消息体只带引用；video/location/link 无内容可取，降级为占位文本（如 `[视频]`）
  让 Agent 能回答「暂不支持」而不是消息凭空消失；`event=revoke`（撤回）记审计并在
  session state 打标记，不回删事件。

## 5. 常见错误码与处理

| errcode | 含义 | 本平台的处理 |
|---|---|---|
| 40014 / 42001 | access_token 无效 / 过期 | 发送、拉素材时自动失效缓存中该 `(corp_id, secret_ref)` 的 token，重新 `gettoken` 后**重试一次**（`wecom.go` `sendSegment`/`fetchMedia`）；持续出现说明 corpsecret 错了，核对 `data/secrets/wecom-secret` |
| 45009 | 接口调用超过频率限制 | 出站按 `{channel, tenant}` 令牌桶限速（`TRPC_SEND_RATE_QPS`/`_BURST`，默认 20/40，租户 `rate_policy.send_qps` 可覆盖），超限在 Stream 内排队不丢弃；持续出现说明配额低于业务量，需向企微申请提额或收紧限速 |

验签/解密侧的行为（`receive`）：签名错误或 XML 解析失败的回调**按互联网垃圾处理**——
直接 ack（重推也读不出来）；签名通过但解密失败的回调回 **5xx 让企微重推**
（多半是 AESKey 配错，修好后重推能救回消息）。

## 6. 故障排查入口

```bash
grep -a 'wecom' data/trpc-service.log | tail -50
```

| 日志关键字 | 含义 |
|---|---|
| `wecom url verification failed` | 后台保存回调 URL 时验签失败：核对 corpid / Token / EncodingAESKey 三件套 |
| `wecom drop unverified callback` | 签名错或报文不是 XML，已 ack 丢弃 |
| `wecom cannot read an authenticated callback (code N)` | 签名对但解密失败，已回 5xx 等重推；查 AESKey |
| `wecom duplicate message ... dropped` | 幂等命中，正常 |
| `wecom handle msg ...` | 入队失败（路由/限流/Redis），已回 5xx 等重推 |
| `wecom send rejected` + errcode/errmsg | 出站被企微拒收，errcode 见上表 |
| `wecom media fetch` | 素材拉取失败，消息以占位文本继续 |

指标（`channel="wecom"` 维度）：`im_inbound_total`、`im_outbound_total{result}`、
`im_dedup_dropped_total`、`gateway_rejected_total{reason}`、`send_rate_limited_total`。
排查动作详见 [运维手册 §5](../operations.md#5-故障排查-runbook)。
