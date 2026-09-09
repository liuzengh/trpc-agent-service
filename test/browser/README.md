# 控制台浏览器回归

`deployment_boundaries.js` 通过 Playwright CLI 验证部署表单的分页、流量边界和失败恢复。静态页面从当前浏览器标签页的同源 `/console/` 加载；测试另开标签页，对其中全部 `/api/v1/` 请求使用协议 fixture，POST 也由 fixture 接收。执行结束关闭测试标签页，保留原控制台页面。

先启动包含当前代码的 Admin，将 `CONSOLE_URL` 设置为它的完整控制台地址，然后在仓库根目录运行：

```sh
playwright-cli --session console open "$CONSOLE_URL"
playwright-cli --session console run-code --filename test/browser/deployment_boundaries.js
playwright-cli --session console run-code --filename test/browser/form_contracts.js
```

也可使用已安装的 Playwright CLI 包装脚本执行相同参数。脚本成功时返回含 `passed: true`、`mockedPosts: 1` 和九个场景名称的结果对象，由 CLI 显示；出现异常或 `### Error` 均表示验收失败，不能仅凭 CLI 退出码判定成功。

覆盖场景：

- active 部署和 published 版本跨页读取，独立于历史表当前页。
- 拒绝 100% 灰度，99.99% 转换为 9999 bps，稳定流量精确显示为 0.01%。
- 第二页读取失败后的恢复、重复游标阻断、异常租户响应阻断。
- 确认前发现部署变化时停止提交。
- 关闭窗口后丢弃在途响应，401 后清除租户页面和登录态。

真实 Admin、PostgreSQL 以及部署更新后刷新持久化的验收使用集成环境另行执行。提交前预读用于发现已发生的配置变化；服务端并发写入仍遵循现有 Admin API 的事务语义。

`form_contracts.js` 在独立标签页验证租户凭据必填、API key 与 SecretRef 互斥、高级 JSON 中全部模型的凭据完整性、提交及取消后的凭据清除，以及版本表单对默认和显式模型调用次数的保留。所有 API 也由 fixture 接收；成功返回 `passed: true`、`mockedPosts: 6`、`realAPIMutations: 0`。这些结果证明浏览器表单行为，真实服务端准入与持久化需在集成环境中验收。
