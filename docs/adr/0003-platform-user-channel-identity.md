# ADR-0003：平台用户与消息渠道身份归一

- 状态：**已接受**
- 日期：2026-09-09

平台使用全局唯一、独立稳定的 `platform_user_id` 表示“同一个人”，登录身份、外部身份和租户角色都不能替代这个身份；同一平台用户可以通过租户成员关系属于多个租户并拥有不同角色。外部身份首次出现即可按 `(tenant, channel, binding, external_user_id)` 建立轻量记录，`platform_user_id` 可以为空；同一外部身份在该作用域内最多关联一个平台用户。只有当消息通道能够在同一可信 Provider 边界内提供与某个 Login Identity 相同的稳定主体标识时，才允许自动关联；姓名、邮箱、昵称等弱标识一律不参与自动合并。无法满足该条件时统一通过一次性关联码显式绑定。解绑只影响未来身份解析，不改写既有 Session 的归属；后绑定也不把不同 Channel/Binding 的聊天历史自动合并。群聊归属于群会话，每条消息单独记录 Actor 与 `direct` / `mention` / `command` 触发方式，并且群会话默认不读取或写入 Actor 的私人长期 Memory。

该决策避免用 `wecom:xxx`、`telegram:xxx` 等外部账号直接充当平台用户；Platform User 只作为身份统一与长期 Memory/用户偏好的稳定边界，不强迫不同外部 Conversation 共享同一聊天 Session。
