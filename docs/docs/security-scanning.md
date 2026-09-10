# CI 密钥扫描

主 CI workflow（`.github/workflows/ci.yml`）在每个 Pull Request 以及推送到 `main` 时运行提交密钥扫描。仓库管理员应在 `main` 的 branch protection 或 ruleset 中要求完整 status check：`CI / Commit Secret Scan`。

## 扫描策略

- 使用固定版本 Gitleaks v8.24.3，并在下载后校验 SHA-256。
- Pull Request 扫描 base 到 head 引入的提交；推送到 `main` 扫描完整 Git 历史。
- 日志和 SARIF 使用 redacted 输出，不打印密钥值或敏感环境变量。
- 报告以 SARIF 上传到 Code Scanning，并作为 artifact 保留 14 天；fork Pull Request 会跳过需要写权限的上传步骤。
- Gitleaks 返回 `0` 表示没有发现，返回 `1` 表示扫描完成但发现疑似密钥；后者会上传 SARIF 并在同仓库 PR 创建 inline annotation，不会使 CI 失败。
- 下载、配置、fixture、报告生成或同仓库报告上传失败等扫描内部错误仍会使 job 失败，清理步骤仍会执行。

## 本地运行

```bash
# 当前工作树/完整历史（需要 Gitleaks 8.24.3）
gitleaks git --redact --config gitleaks.toml --log-opts="--all"

# 验证通过与阻断 fixture
GITLEAKS_BIN=/path/to/gitleaks ./scripts/security-secret-fixture.sh
bash scripts/security-allowlist-fixture.sh
```

Pull Request 只检查引入的提交时，可将 `--log-opts` 替换为 `"$(git merge-base origin/main HEAD)..HEAD"`。

## 发现问题后的处理

发现真实凭据后，先立即吊销/轮换，再从 Git 历史中清理泄漏内容，并重新运行扫描。误报或临时例外必须在 `gitleaks.toml` 中以最小提交或路径范围登记；每个例外都要写明 owner、Reason、跟踪 issue 和未来到期日期，并通过 `scripts/validate-security-allowlist.sh` 校验。不得把密钥值写入日志、SARIF 或 issue。

## CI 范围

本页固定提交密钥扫描门槛；Go 依赖和容器镜像的安全检查沿 CI 的对应 job 与发布流程执行，
结果与本扫描的 SARIF/allowlist 记录保持一致。
