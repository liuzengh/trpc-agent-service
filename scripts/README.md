# 脚本入口

`scripts/` 按调用者分层。所有面向开发者或 CI 的脚本都会自行定位仓库根目录，因而可从
任意工作目录以 `bash /absolute/path/to/script.sh` 调用。

| 目录 | 调用者 | 内容 |
| --- | --- | --- |
| `ci/` | GitHub Actions、开发者 | admission、格式、依赖边界、文档链接、覆盖率 gate |
| `e2e/` | GitHub Actions、开发者 | 按域后端 e2e、IM 契约与依赖恢复 |
| `compose/` | `start.sh`、Compose 容器、开发者 | quickstart 与本地 PostgreSQL/Redis migration/runtime slice |
| `lib/` | 其他脚本 | 零-skip 等无独立业务语义的 helper |
| `internal/` | Compose 容器定义 | backend adapter e2e 的容器内执行器；不得作为用户入口 |

稳定的验收入口以 [README](../README.md) 和
[本地验证矩阵](../docs/runbook/verification-matrix.md) 为准。`ci/check-doc-links.sh` 会校验
Markdown 的仓库内链接、标题锚点和 `scripts/*.sh` 引用。新增脚本必须先归类：不要把容器内部
helper、CI helper 或一次性 e2e 再放回 `scripts/` 顶层。
