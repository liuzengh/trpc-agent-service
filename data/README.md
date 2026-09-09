# 运行时数据

服务启动后的 `trpc-service.log`、`trpc-service.pid` 和自动生成的 `admin-api-key` 存放于此。默认 InMemory 的配置、Session 和 Revision Pin 仅在进程内存中，**不会持久化到本目录**；退出进程后丢失。

本地私密环境配置建议使用 `data/local.env`，创建后执行 `chmod 600 data/local.env`。Bot Secret、数据库凭据和真实模型密钥只放在本地配置或进程环境中，不填入网页聊天。具体格式和启动方式见[可复现本地部署](../docs/local-deployment.md)。

`.gitignore` 已排除 `data/*`，仅保留本 README；不要强制添加环境文件、密钥、日志、PID 或真实消息数据。示例配置只能包含占位值。`admin-api-key` 权限由启动脚本设为 `0600`，重启会复用，不应为了普通停服删除它。
