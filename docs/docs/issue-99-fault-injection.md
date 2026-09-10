# Issue #99：故障注入与并发一致性验证

Issue #99 的测试套件覆盖从请求入口到持久化 Outbox 的失败边界。测试使用受控 fake，不连接真实模型、IM、数据库或 Secret Manager，因此可以在普通 CI 中稳定复现。

## 覆盖矩阵

| 边界 | 注入场景 | 断言 |
| --- | --- | --- |
| HTTP / Gateway | 未知字段伪造租户、跨租户计划 | 在 Runner 启动前拒绝 |
| Runner / Provider | 超时、取消、携带机密的底层错误 | 返回稳定错误，不泄漏机密 |
| Reply Materializer | 批量写入失败 | 返回稳定 materialization 错误，无半成品行 |
| Outbox Worker | 可重试与不可重试错误、租约竞争、重启 | 有界重试、fencing、幂等且不重复投递 |
| Runner Registry | 两个租户并发构建相同计划 | 每个完整 cache key 只构建一次，租户不串用 |

## 运行

```bash
go test ./examples/fault-injection-e2e -count=1 -v
go test -race ./examples/fault-injection-e2e -count=1 -v
```

完整仓库的 `go test -race ./...` 仍由主 CI 的 Race Tests job 执行；独立的 `Fault-injection E2E` workflow 会在 push、pull request 和手动触发时运行上述两个命令。

## 可复现性约束

- fake 的失败点由测试显式控制，不使用随机网络或生产凭据。
- provider 错误只允许稳定分类进入持久化状态；底层错误文本不得进入响应、日志、审计或 trace。
- 并发测试必须在 `-race` 下通过，并验证顺序、租约和幂等结果。
