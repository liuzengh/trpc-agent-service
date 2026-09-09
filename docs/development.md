# 开发手册

这一篇覆盖在本仓库里改代码的全流程：搭环境、跑测试、过 CI 门禁、提交与代码检查。
读完 [`docs/quickstart.md`](./quickstart.md) 再来——那里已经把服务跑通了一遍。

| 章节 | 内容 |
|---|---|
| [1. 开发环境](#1-开发环境) | 依赖、密钥文件、构建运行 |
| [2. 测试](#2-测试) | 怎么跑、testenv 门控、CI 三道硬门禁 |
| [3. 提交规范](#3-提交规范) | conventional commits |
| [4. 代码检查](#4-代码检查) | fmt / vet / lint，wxbizmsgcrypt 例外 |

---

## 1. 开发环境

### 依赖

- **Go 1.27**（以 `go.mod` 的 `go` 指令为准）。
- **Docker + Compose 插件**：依赖中间件全部在 `docker-compose.yml` 里，服务本体在宿主机跑（方便调试）：

```bash
make deps          # = docker compose up -d
# 起 5 个容器：postgres(pgvector/pg16, 5432) / redis(宿主 6380) / minio(9000+9001)
#              / jaeger(4317+16686) / prometheus(9090)
```

PG 空卷首次启动时自动执行 `deploy/db/init.sql`（建表）和 `deploy/db/seed.sql`（演示数据）。

### 密钥文件

服务配置只来自环境变量；**密钥一律走引用名 + 文件**（`data/secrets/<引用名>`，
机制见 [guide.md 附录 A](./guide.md#附录-a配置与密钥机制)）。本地最少需要一个模型密钥：

```bash
mkdir -p data/secrets
echo -n 'sk-你的模型APIKey' > data/secrets/deepseek-apikey   # 对应 TRPC_MODEL_APIKEY_REF 默认值
chmod 600 data/secrets/*
```

`data/` 已被 `.gitignore` 忽略（只保留 `data/README.md`）。**任何密钥文件都不要提交。**

### 构建与运行

```bash
make build         # ./build.sh → bin/trpc-service
make start         # ./start.sh（all-in-one；环境变量在命令前缀里给）
make stop          # ./stop.sh
make fmt lint      # gofmt -w + go vet + golangci-lint
make migrate       # ./deploy/db/migrate.sh up（增量 schema 迁移）
./clean.sh         # 清 bin/、根目录游离二进制、coverage 产物
```

典型的本地启动（每个变量都是安全默认下的显式声明，含义见 quickstart）：

```bash
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true \
TRPC_METRICS_ADDR=127.0.0.1:8083 TRPC_SESSION_BACKEND=postgres ./start.sh
```

`Makefile` 的 target 只是脚本的一层薄封装，`*.sh` 是事实来源。

---

## 2. 测试

### 本地跑

```bash
make test          # go test ./...（不带 -race）
make cover         # ./coverage.sh → go test ./... -race -coverprofile=coverage.out
```

### testenv 门控：为什么本地覆盖率只有一半

集成测试依赖真实的 PG / Redis / MinIO，由 `trpcservice/testenv/testenv.go` 统一门控：

- 每个集成测试起手调 `testenv.PG(t)` / `testenv.Redis(t)`，连不上就 `t.Skip`，
  skip 消息里写明该设哪个变量。
- 测试地址由 **`TRPC_TEST_PG_DSN` / `TRPC_TEST_REDIS_ADDR` / `TRPC_TEST_S3_ENDPOINT`**
  决定，默认值就指向 compose 起的开发依赖（同一个 `trpc` 库、同一个 Redis）。

两个直接后果：

1. **依赖不在线时集成测试全 skip，本地覆盖率从 87.5% 掉到约 55%**——那不是有效测量。
   跑 `make cover` 前先 `make deps`。
2. **跑测试会污染开发数据**（PG 里多出测试租户、Redis 里多出队列消息）。建议给测试单独建库：

```bash
docker compose exec -T postgres psql -U trpc -d postgres -c 'CREATE DATABASE trpc_test'
docker compose exec -T postgres psql -U trpc -d trpc_test -v ON_ERROR_STOP=1 < deploy/db/init.sql
TRPC_TEST_PG_DSN='postgres://trpc:trpc-dev-only@localhost:5432/trpc_test?sslmode=disable' make test
```

Redis 侧没有 db index 开关（配置只有 host:port），队列残留按
[guide.md 附录 C](./guide.md#附录-c重置开发环境) 清理。

### CI 三道硬门禁（`.github/workflows/test.yml`）

CI 用 service container 起全新的 PG/Redis/MinIO，先灌 `init.sql` + `seed.sql` +
`deploy/db/migrations/` 里的增量迁移，再 `-race` 跑全量测试。三道门禁一道不过即红：

| 门禁 | 规则 | 为什么 |
|---|---|---|
| **禁止 skip** | `grep -c '^--- SKIP' test.log` 必须为 0 | 平台核心路径（PG session、Stream 重投、锁、迁移、S3、Admin）必须真实执行，skip 在 CI 里是失败而不是绿灯 |
| **总覆盖率 ≥ 85%，ratchet 只升不降** | `go tool cover -func` 末行总数 ≥ 85 | 85 是地板不是目标（当前实测约 87.5%）；地板只许上调，防止覆盖率缓慢漏下去 |
| **PR diff 覆盖率 ≥ 85%**（仅 PR） | `diff-cover` 对照 `origin/<base_ref>` 逐行算 | 存量覆盖率再高，也挡不住新提交的代码一行测试都没有 |

所以 **PR 里新增/修改的代码必须带测试**——不带测试的 PR 在第三道门上必然失败，
review 也无从谈起。本地可用 `make cover` 预跑同一套命令自查（记得先 `make deps`）。

---

## 3. 提交规范

仓库使用 [Conventional Commits](https://www.conventionalcommits.org/)：
`type(scope): 英文祈使句，小写开头，不加句号`。从 `git log` 归纳的实际用法：

- **type**：`feat` / `fix` / `docs` / `test` / `refactor` / `chore` / `ci` / `build`
- **scope**：模块目录名，如 `wecom` `wxkf` `wecomws` `tenant` `storage` `config` `k8s` `db`
- 描述用英文、说明「做了什么」而非「改了哪个文件」，例如：
  `fix(wxkf): rewrite inbound to the official event-and-pull protocol`

一个 commit 只做一件事；文档和代码的变更可以同 commit，但不要把无关改动捎进来。

---

## 4. 代码检查

提交前本地过一遍（CI 的同一批检查，外加 `go mod tidy` 干净度）：

```bash
make fmt       # gofmt -w .
./lint.sh      # go vet ./... + golangci-lint run ./...
go mod tidy    # CI 校验 go.mod/go.sum 无 diff
```

golangci-lint 版本以 CI 为准（`v2.13.2`，见 `lint.sh` 的安装提示）；规则集在
`.golangci.yml`（errcheck / govet / staticcheck / unused / ineffassign / misspell /
gosec / gocritic）。

### wxbizmsgcrypt 例外：不要改

`wxbizmsgcrypt/` 是 **vendor 的腾讯官方回调加解密库**（独立 go module，经 `go.mod`
的 `replace` 引入）。它被刻意排除在 vet/lint 范围之外——任何「顺手修一下」都会让我们
和上游代码漂移，升级时无法对账。发现它有问题时，在调用侧包一层，不要动 vendor 本体。
