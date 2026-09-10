# TrailForge 演示机器人

本目录保存可复现的 TrailForge 演示资产，不包含任何真实密钥。

- `application.json`：机器人、模型、工具、存储与外部渠道配置。渠道只保存 `env:` 凭据引用。
- `instruction.txt`：系统指令。
- `knowledge/`：测试知识库。
- `TEST.md`：人工验收问题与预期。
- `seed.py`：将当前演示配置写入本地服务。

默认使用显式开启的 Mock 登录：

```bash
python3 examples/trailforge/seed.py http://127.0.0.1:8080
```

也可以使用本地账号：

```bash
TRAILFORGE_AUTH=local \
TRAILFORGE_USERNAME=<username> \
TRAILFORGE_PASSWORD=<password> \
python3 examples/trailforge/seed.py http://127.0.0.1:8080
```

默认不启用企业微信、飞书、Telegram Binding，避免没有对应凭据的环境无法创建机器人。需要真实 IM 联调时先配置 `application.json` 中引用的环境变量，再设置：

```bash
TRAILFORGE_WITH_CHANNELS=1 python3 examples/trailforge/seed.py
```

平台模型配置来自 `configs/platform.json`。当前 TrailForge 使用 `zhipu-ai / glm-5.3-flash`，运行环境需要提供该配置引用的模型密钥。
