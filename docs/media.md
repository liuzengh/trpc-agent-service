# IM 媒体与飞书卡片

企业微信和飞书仅在回调完成验签、解密和租户绑定唯一匹配后，使用该配置版本对应的应用
凭据下载媒体。平台媒体标识只存在于回调处理内存中；下载完成后，进入 Inbox 的对象只包含
校验后的图片字节或提取后的文档文本，不包含 media key、下载 URL 或访问令牌。

默认下载超时 8 秒，单文件最大 10 MiB。允许的图片类型为 JPEG、PNG、GIF 和 WebP；基础
文档提取支持 UTF-8 的 TXT、Markdown、CSV 和 JSON，提取文本最大 256 KiB。PDF、Office、
OCR、压缩包、音视频和可执行文件不在这个最小闭环内。Content-Type、内容探测或大小校验
失败时拒绝文件，平台网络失败返回可重试响应。

下载内容写入权限为 `0600` 的随机临时文件，读取或校验的任一分支结束后都会删除。HTTP
客户端禁止重定向，避免 Authorization、access token 或媒体标识被带到其他主机。错误只
保留受控分类，不包含响应正文、请求 URL、凭据和媒体标识。

图片只在 App 的模型配置显式设置 `multimodal: true` 时构造成 tRPC-Agent-Go
`ContentPart(image)`；否则请求失败并按现有 Inbox 重试/DLQ 机制处理。文档以带安全文件名的
受控文本段加入用户输入。模型输入属于租户 Session 数据，但不会复制到日志、Trace 或审计
详情。

飞书 binding 可设置 `reply_format: card`，最终回复会通过原有 Outbox、共享限流、指数退避和
DLQ 投递为 `interactive` 卡片；默认或 `text` 保持原文本回复。卡片当前只包含一个
`lark_md` 文本块，复杂按钮回调和卡片状态更新留作后续增强。
