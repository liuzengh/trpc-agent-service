# 公开官网与文档维护

公开站点与管理 Web 同仓库、独立构建和发布。本站 Markdown 是用户参考内容，完整教程源文件在 `docs/user-guide/v1`。

- 官网和文档工程：`site/`，Next.js 静态导出，不调用 Control API。
- 管理控制台：`web/`，根地址未登录直接进入登录页，已有会话进入角色校验。
- 帮助入口：控制台及登录页的“帮助文档”打开 `/help`，由 Web 运行时 `DOCS_SITE_URL` 指向文档中心。

## 修改与检查

1. 修改本目录的主题 Markdown，或完整教程及插图。
2. 执行 `npm --prefix site run docs:sync`，生成在线页和离线指南。
3. 执行 `npm --prefix site test`、`npm --prefix site run build`。
4. 提交源文件及离线 HTML；在线 HTML 在构建时生成，不再提交到管理 Web。

目前六个主题在 `site/scripts/build-docs.mjs` 和 `site/components/site/doc-topics.ts` 显式登记。新主题需同时登记生成列表和导航。

部署命令、Pages 子路径与 GitHub Actions 见 `site/README.md`。只将这些用户文档作为站点输入，不会发布仓库中的内部架构笔记或验收目录。
