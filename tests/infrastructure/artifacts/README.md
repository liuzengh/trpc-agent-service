# Infrastructure test artifacts

本目录由 `scripts/infrastructure-test.sh` 在本地或 CI 运行时生成，用于记录基础设施冒烟测试的输出快照：

- `compose-ps.txt`：`docker compose ps --all` 的容器状态快照。
- `test.log`：基础设施测试的日志输出。

这些文件是**运行时证据**（内容随每次运行变化），已被 `.gitignore` 忽略，不进入版本库。需要查看最新验证结果时，重新运行脚本即可在本目录生成。
