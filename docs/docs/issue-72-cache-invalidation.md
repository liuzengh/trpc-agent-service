# Issue #72：Precise Runtime Cache Invalidation

`runtime/runner.RunnerRegistry` 现在提供 `InvalidateMatching` 以及 tenant、app、model profile、backend profile 的精确 helper。配置发布、回滚或 disable 时只调用受影响 scope；无关租户和 profile 的既有可复用 entry 保持复用。

Admin 成功提交 tenant、App/revision publish 或 rollback、Model profile、Backend profile、Binding 的变更后，会发出最小范围的 `CacheInvalidation` 信号。bootstrap 将 tenant/App/Model/Backend 信号接到对应的 RunnerRegistry helper。Binding 信号同时保留在控制面变更流中；binding 验签和路由由每次请求的 Repository 校验完成。

失效会从新 lease 视图删除 entry，但不会中断已经借出的 Runner。最后一个 lease `Release` 后才关闭旧 Runner；正在构造的同 key build 会标记为 invalidated，并在构造完成后关闭并重试。

Provider 实例跟随 Runner/CapabilitySet 生命周期，不进入 cache key。多 binding 的 channel 路由仍必须在验签、租户、App 和 binding 状态校验完成后才允许 Runner execution；未知、inactive、duplicate、cross-tenant binding 在 Gateway 前置失败。
