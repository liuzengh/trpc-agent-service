# 文档目录(MkDocs)

本目录是一个自包含的 [MkDocs](https://www.mkdocs.org/) 项目,使用 [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) 主题。

```text
docs/
|-- mkdocs.yml       # MkDocs 配置(导航、主题、插件)
|-- requirements.txt # Python 依赖
`-- docs/            # 文档源文件(Markdown)
    |-- index.md         # 首页
    |-- architecture.md  # 架构设计
    |-- data-model.md    # 数据模型
    |-- ops.md           # 运维方案
    |-- assets/           # 文档图片资产（含架构总览图）
    `-- javascripts/     # Mermaid 等页面增强脚本
```

## 本地构建

```bash
cd docs
pip install -r requirements.txt
mkdocs serve   # http://127.0.0.1:8000 实时预览
mkdocs build   # 生成静态站点到 docs/site/
```

## CI

`.github/workflows/docs.yml` 在 `docs/**` 变更的 push / PR 上自动执行 `mkdocs build --strict`(配置或链接错误会导致失败),并上传站点产物。

## 约定

新增文档页面后需在 `mkdocs.yml` 的 `nav` 中登记;页面使用中文撰写,遵循现有结构。
