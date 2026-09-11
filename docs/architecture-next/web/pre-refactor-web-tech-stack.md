# 重构前 Web 技术栈基线

本文记录旧实现 `/Users/jfs/.codex/worktrees/2bc0/trpc-agent-service/web` 在重构前的
实际技术栈、运行边界和代码组织。它是新 `control-web` 与 `local-im-web` 设计的
现状基线，不代表新前端必须沿用旧实现。

基线源码对应 Git 提交：
`615a9297ac4e7ccfa8afe63862a29c196c637eab`（`feat(channels): add local im simulator`）。

## 1. 前端板子来源（首要基线）

重构前的管理控制台不是从空白页面开始设计，而是以 **shadcn/ui 官方
`dashboard-01` Block** 作为可修改的前端板子。旧架构通过
`docs/adr/0022-build-the-console-on-shadcn-dashboard-source.md` 明确作出这一决定，
`docs/design/08-frontend-console.md` 与 `docs/spec/09-web-console.md` 继续将它列为 Web
技术基线。

原板子提供的界面骨架及其平台映射为：

| `dashboard-01` 源码组件 | 重构前平台用途 |
| --- | --- |
| `app-sidebar` | Tenant 导航与 Tenant Switcher |
| `site-header` | Search、Alerts、当前 Role/User |
| `section-cards` | 24h Run、成功率、p95、Token、Cost |
| `chart-area-interactive` | Run 与 Cost 趋势 |
| `data-table` | 待办、Deployment 状态和失败 Run |
| `nav-user` | 账号、当前角色和退出入口 |

采用方式是把 Block 当作**源码起点**，而不是把它当成不可修改的外部页面：示例导航、
示例业务数据和 `data.json` 应被 Tenant 领域内容替换。旧代码随后把这一 Dashboard
外观和信息架构收拢进 `components/console.tsx` 与手写 CSS。

需要区分“板子来源”和“最终依赖状态”：架构基线声明了 Tailwind CSS、shadcn/ui 与
`dashboard-01`，但当前最终 `package.json` 只保留 Next.js、React 和 React DOM，源码中
也没有保留 shadcn 组件目录或 Tailwind 配置。因此，`dashboard-01` 是这套控制台最重要
的设计与源码骨架来源，却不是最终产物中的 npm 运行时组件依赖。

## 2. 技术栈总览

| 层次 | 重构前实现 |
| --- | --- |
| Web 框架 | Next.js `16.3.2`，App Router |
| UI 运行时 | React `19.2.8`、React DOM `19.2.8` |
| 语言 | TypeScript `5.5.4`，`strict: true`、`noEmit: true` |
| 构建器 | Next.js 生产构建，实际构建输出使用 Turbopack |
| Node.js | Docker 构建与运行基于 `node:20-alpine` |
| 部署产物 | `output: "standalone"`，由 `node server.js` 启动，监听 `3000` |
| UI 板子 | shadcn/ui 官方 `dashboard-01` 可修改源码骨架 |
| 最终样式实现 | 手写全局 CSS；最终树中没有 Tailwind 配置或 shadcn 组件目录 |
| 状态管理 | React `useState`、`useEffect`、`useMemo`，没有独立状态容器 |
| 数据访问 | 浏览器 `fetch` + Next Route Handler/BFF 代理 |
| 包管理 | npm + `package-lock.json`，Docker 使用 `npm ci` |
| 测试 | `package.json` 没有前端测试脚本或测试框架 |

直接运行依赖只有 `next`、`react` 和 `react-dom`。板子源码被平台页面吸收后，没有继续
以 shadcn npm 依赖或独立组件目录存在。最终依赖中也没有路由扩展、表单库、Schema
校验库、Server State/Query 库、图编辑器或可观测性 SDK。

## 3. 应用形态

Next App Router 只有一个页面入口：

```text
app/layout.tsx
└── app/page.tsx
    └── components/console.tsx  # "use client"
```

`app/page.tsx` 只渲染一个 `Console`。`Console` 是 293 行的单体 Client Component，
通过本地字符串状态 `active` 在以下视图之间切换，而不是使用独立 URL 路由：

- 总览；
- Agent 编辑器与 Preview；
- Deployment；
- Channel Binding；
- IM Simulator；
- Run；
- Governance/Approval/Audit；
- Tenant Settings。

因此重构前虽然使用 Next.js，但主要交互仍是单页客户端控制台。页面级路由、布局
边界、错误边界、加载边界和按业务域拆包尚未形成。

## 4. 前端状态与 AgentSpec 编辑

页面状态全部保存在 React 组件内，没有跨页面 Store。数据加载由各视图在
`useEffect` 中直接触发，提交动作直接调用 API 函数后刷新本地状态。

`lib/agent-spec.ts` 自行维护 AgentSpec 编辑模型，包含：

- `AgentSpecDocument`、`SpecNode` 和表单投影类型；
- JSON 解析、规范化和稳定序列化；
- 表单字段到 AgentSpec 的映射；
- 基于 Web Crypto 的 SHA-256 digest。

该实现没有复用生成的 API 类型或 Schema 生成器。`console.tsx` 的复杂子组件参数
存在 `any`，因此 `strict` 编译并不等于页面内部已经具备完整类型边界。

## 5. API 与 BFF 拓扑

重构前页面同时访问三类后端能力：

```text
Browser
├── /platform-api/*
│   └── Next Route Handler -> Admin API（默认 http://localhost:8081）
├── /runtime-api/*
│   └── Next rewrite -> Durable Gateway（默认 http://localhost:8080）
└── /im-simulator-api/*
    └── Next Route Handler -> IM Simulator（默认 http://localhost:8093）
```

`lib/admin-api.ts` 集中定义 fetch 包装和前端手写响应类型，共 279 行。它同时包含
Agent、Deployment、Channel、Run、Preview、Approval、Audit 和 IM Simulator 调用，
没有按照业务域拆分 API Client。

### `/platform-api/*`

`app/platform-api/[...path]/route.ts` 是服务端代理：

- 从 `TRPC_ADMIN_API_TOKEN` 读取 Bearer Token；
- 从环境变量写入默认 Actor ID 和 Actor Roles；
- 转发浏览器提供的 `X-Tenant-ID`；
- 支持 GET、POST、PUT；
- 未配置 Token 时返回 `503`。

这是一套旧的 Token、Actor Header 和 Tenant Header 认证模型，不是新 Control API 的
`HttpOnly` Session Cookie 模型。

### `/runtime-api/*`

`next.config.mjs` 使用 rewrite 将 `/runtime-api/:path*` 转发到
`RUNTIME_API_BASE`。浏览器代码通过它提交 Production Run、Preview Run、轮询状态和
取消 Preview。这意味着旧 Web 同时处于管理面和执行热路径入口。

### `/im-simulator-api/*`

`app/im-simulator-api/[...path]/route.ts` 使用服务端 Bearer Token 代理消息、事件、
重放和故障注入请求。IM Simulator UI 与管理控制台位于同一个工程、同一个页面和
同一个发布产物中。

## 6. 配置方式

| 环境变量 | 使用位置 | 作用 |
| --- | --- | --- |
| `ADMIN_API_BASE` | Next 服务端 Route Handler | Admin API 上游地址 |
| `TRPC_ADMIN_API_TOKEN` | Next 服务端 Route Handler | Admin API Bearer Token |
| `ADMIN_ACTOR_ID` | Next 服务端 Route Handler | 注入旧 Actor ID |
| `ADMIN_ACTOR_ROLES` | Next 服务端 Route Handler | 注入旧 Actor Roles |
| `RUNTIME_API_BASE` | Next rewrite | Durable Gateway 地址 |
| `IM_SIMULATOR_BASE` | Next 服务端 Route Handler | IM Simulator 地址 |
| `TRPC_IM_SIMULATOR_TOKEN` | Next 服务端 Route Handler | IM Simulator Token |
| `NEXT_PUBLIC_TENANT_ID` | 浏览器 Bundle | 默认 Tenant ID |
| `NEXT_PUBLIC_AGENT_APP_ID` | 浏览器 Bundle | 默认 Agent Application ID |

Tenant 和 Agent 默认值在构建期进入浏览器 Bundle。页面没有登录流程、Session 恢复、
显式平台/Tenant 上下文选择或基于服务端 Capability 的导航构建。

## 7. 样式和静态兼容层

主要 Next 页面样式位于 `app/console.css` 和 `app/globals.css`，均为手写全局 CSS。
仓库中还同时保留 `index.html`、`app.js`、`styles.css` 和旧 README 描述的
`python3 -m http.server` 静态入口。这套静态 Shell 与 Next 应用并存，但 Next
生产构建不会把它们作为 App Router 页面入口。

## 8. 构建与质量门禁

重构前提供以下 npm 脚本：

```text
npm run dev
npm run build
npm run start
npm run lint
```

现场执行 `npm ci` 后，`npm run build` 已通过 TypeScript 检查和 Next 生产构建，
生成一个静态根页面以及两个动态代理 Route Handler：

```text
○ /
ƒ /im-simulator-api/[...path]
ƒ /platform-api/[...path]
```

代码库没有单元测试、组件测试或浏览器 E2E 脚本。`package.json` 中虽然声明
`next lint`，但没有单独安装 ESLint 或保存 ESLint 配置。

## 9. 对本次重构的直接约束

这份基线首先要求新 Web 明确是继续继承 `dashboard-01` 的设计语言，还是选择新的板子；
不能只讨论 Next.js、React 和状态库而遗漏页面骨架的来源。在此基础上，旧实现暴露出的
核心结构问题是边界混合：

1. Control 管理、Runtime Run/Preview 和 Local IM 调试共处一个工程。
2. 单一 Client Component 同时承担导航、业务状态、请求和展示。
3. API 类型、鉴权模型和 Tenant 上下文由前端手工拼装。
4. 页面没有真实 URL 级平台/Tenant 授权上下文。
5. 缺少自动化前端测试与跨后端契约校验。

新结构应以当前架构约束为准，将旧工程拆成两个独立应用：

```text
control-web -> control-api
local-im-web -> local-im-provider -> channel-gateway -> agent-worker
```

`control-web` 不进入消息执行热路径；`local-im-web` 不承载平台或 Tenant 管理能力。
是否继续采用 Next.js、如何选择 UI/状态/Schema 工具，应在下一份目标技术栈文档中
单独决策，不能由这份现状基线自动推导。
