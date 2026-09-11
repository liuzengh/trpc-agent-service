export const docTopics = [
  { slug: "concepts", number: "01", title: "理解核心概念", tag: "从这里开始", description: "Agent、运行配置、部署与渠道，四个对象如何一起工作。", icon: "layers" },
  { slug: "agent", number: "02", title: "Agent 与版本", tag: "定义行为", description: "从 Single LLM 模板开始，编辑指令、声明槽位、保存和发布。", icon: "agent" },
  { slug: "runtime-profile", number: "03", title: "运行配置与资源", tag: "连接资源", description: "模型、会话存储与凭据；看懂 Models、Tools、Knowledge 的边界。", icon: "settings" },
  { slug: "deployment", number: "04", title: "部署与生命周期", tag: "可追溯发布", description: "固定版本、校验发布，再有意识地升级、切换与回退。", icon: "deploy" },
  { slug: "channels", number: "05", title: "Telegram 与企业微信", tag: "连接用户", description: "选择接收方式、完成接入预检，理解接入与路由两个开关。", icon: "channel" },
  { slug: "administration", number: "06", title: "账号、租户与权限", tag: "团队协作", description: "区分平台管理员、OWNER 与 MEMBER，找到正确的操作入口。", icon: "users" },
] as const;
export const referenceUrl = (slug: string) => `/docs/reference/${slug}.html`;
