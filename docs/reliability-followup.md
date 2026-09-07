# 后续可靠性与能力补齐

本轮持续开发范围：用户要求先完成明确的可靠性缺口，再继续 Agent MCP 与最小安全附件链路。每项完成后继续下一项，除非用户打断或确需外部操作。

- [x] Session 回填与在线写入协调，非破坏性、可恢复迁移：App/资源 I/O 协调、目标 staging generation、验证后发布别名；失败不删除当前目标，会话 ID 对调用者保持不变。
- [x] 摘要原样复制，事件内容/顺序、摘要正文/水位的完整校验：保留 Event ID/Version/FilterKey，双写只生成一次摘要，采用后端可移植摘要快照。
- [x] Session / Memory 服务器证明与切换保护：migration 021 元数据、完整已登记主体清单校验、写入 epoch 失效、调用方 passed 标记拒绝；旧 fencing token 在 Session 存储入口拒绝。
- [x] IM 分段发送的持久化状态、续发与未知结果处理：migration 022/023、逐段尝试与回执、发送租约保活、按 claim 次数拒绝旧终结写入；未知结果不重发，提供带权限/证据引用的人工对账入口。
- [x] 每个 Run 独立的并发配额租约、续租、过期回收和安全释放：Redis TIME + ZSET、唯一尝试 ID、续租失败取消执行；Worker/HTTP 使用租约 Context；本地、miniredis、独立 Redis ACL 测试通过。
- [ ] Agent 侧 MCP 工具接入与租户授权、审计、生命周期管理。
- [ ] 安全附件下载、校验、Artifact 保存和授权访问的最小链路。

开发不读取或修改日常 `.env`，不重启业务服务、不迁移业务库、不调用真实模型或 IM。测试使用合成输入、HTTP 测试服务器、miniredis 和独立容器。新表、权限、兼容性和已验证范围在各项完成时记录。

并发配额改用 `:quota:concurrent:v2:<tenant>`，不得与旧共享计数器版本混跑。Redis Worker ACL 增加 TIME 和受限 ZSET 操作。已验证活跃租约跨多个 TTL 保持占用、崩溃过期回收、旧释放不能删除新占用、失去租约取消执行、Close 结束续租 goroutine。新版本未部署到日常实例。

## Session / Memory 迁移边界

`resource_sync` 保存已登记主体、epoch、fencing token、staging/当前别名及服务器验证证明，不保存聊天正文。Worker/Jobs 拥有读写权限，Admin 只读。物理 Session 后端仍保存全部 Event、state 和摘要；`_platform:session_summaries_v1` 是保留的可移植摘要键，用户状态更新不允许覆盖它。摘要只调用一次模型，然后复制同一份结果及 boundary。

只有 Redis Coordinator 的持久序列参与跨进程 fencing 比较；Local Coordinator 的进程计数不能在重启后与旧值比较。Local 仍只用于单节点开发。生产变更 Redis prefix 或恢复旧 Redis 快照时，必须连同控制面的 fence 状态制定恢复方案，不能自动清除高水位绕过保护。

所有平台 Session/Memory I/O 与回填通过同一个 tenant/app/resource 协调边界。PostgreSQL 使用 advisory lock；开发内存控制面使用共享、可取消的锁。当前按 app/resource 串行 I/O，保守保障一致性，长回填/摘要可能增加该 app 的等待时间，需纳入容量评估。绕过平台直接操作后端不参与此协议；旧版未登记会话/用户须在迁移时显式列入回填、校验清单，不能用部分样本代表全量。服务器至少要求全部已登记主体通过当前 epoch 的校验，后续写入使证明失效。

Session 回填保留原目标和中断的 staging，恢复时检查已复制前缀再续写，不删除会话重建。别名存放在控制面，因此控制面与数据后端须一起备份。暂不自动清理旧 generation，避免删除仍需回滚的数据；容量要计入这些保留副本。普通读取和列表隐藏未发布 staging。Session/Memory binding 缓存不再因仅切换迁移状态的 version 变化而换成空的 InMemory 后端。

已增加：事件同数不同内容拒绝、摘要只生成一次、原事件 ID 保留、staging 故障不破坏目标、回填与写入互斥、写后旧证明失效、旧 fencing token 拒绝、部分主体清单不能切换，以及独立 PostgreSQL 元数据/权限测试。Memory 校验包含正文、topics、事件时间、参与者和地点，不比较各物理后端自行生成的 LastUpdated。

## 回复重试

`outbound_part` 按 tenant/outbound/part_index 保存 input hash、独立 owner、状态和真实 provider 回执。全部分段由同一固定正文/绑定版本派生；中途配置改变停止继续发送。已确认 sent 的段跨 Sender 重启复用，不再重新发送。Telegram/企业微信发送网络失败或响应不完整归入 unknown，不把连接报错当成远端一定未执行；企业微信不再伪造 provider message ID。

Sender 对整批已 claim 项保活，每条结束立即停止对应续租 goroutine。分段启动和父消息终结都核对当前 claim 次数；进程名称相同也不能让旧尝试覆盖新尝试。原版已经尝试但缺少新协议记录的 SQL 出站行保持 delivery_protocol=0，不能直接自动重发，升级前应排空或人工核对。

`POST /admin/outbound-parts/list` 查询 tenant_id/outbound_id；`POST /admin/outbound-parts/reconcile` 需要 operator 权限、part_index、expected_owner、outcome=sent/not_sent、provider_id 和 evidence_ref。只能核对 unknown/attempting 且父消息当前没有活跃租约的记录。证据引用只保存 hash，不能把模型判断当成“未发送”的证据。PostgreSQL 的分段修正、父消息恢复及审计回执在同一事务执行；没有事实证据时保留 unknown。不会自动读取真实聊天替操作者判断。

已验证：第二段限流后不重发第一段，跨 Sender 续发，成功后回执落库失败不盲目再发，长回复跨租约窗口保活，旧完成不能覆盖人工核对，以及独立 PostgreSQL 分段状态/审计函数合约。以上只使用合成消息。
