import { useEffect, useState } from "react";
import { Alert, Button, Form, Input, Skeleton, Space, Tag } from "antd";
import { api, errorText } from "./api";
import { Failure, PageHeading, Panel } from "./components";
import {
  type AgentApp,
  type Page,
  type Principal,
  type Tenant,
  writable,
} from "./types";
import { type ModelConnectionPage } from "./ModelConnections";
import { navigate } from "./App";

export function GettingStarted({
  tenant,
  principal,
  onTenantCreated,
}: {
  tenant: string;
  principal: Principal;
  onTenantCreated: (tenant: Tenant) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [id] = useState(() => "tenant-" + crypto.randomUUID());
  const [state, setState] = useState<{
    apps: AgentApp[];
    connections: number;
    more: boolean;
  } | null>(null);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    if (!tenant) return;
    let live = true;
    setError("");
    Promise.all([
      api<Page<AgentApp>>("catalog/list", {
        kind: "apps",
        tenant_id: tenant,
        limit: 100,
      }),
      api<ModelConnectionPage>("model-connections/list", { tenant_id: tenant }),
    ])
      .then(([a, c]) => {
        if (live)
          setState({
            apps: a.items,
            connections: c.items.length,
            more: !!a.next,
          });
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant, refresh]);
  async function create(values: { name: string }) {
    setBusy(true);
    setError("");
    try {
      const t = await api<Tenant>("tenants", {
        tenant_id: id,
        display_name: values.name.trim(),
        status: "active",
        region: "local",
        secret_namespace: "tenant/" + id,
        quota_config: {},
        audit_policy: {},
      });
      onTenantCreated(t);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  const app = state?.apps[0];
  const published = state?.apps.find((a) => a.stable_revision_id);
  return (
    <div className="getting-started">
      <PageHeading
        eyebrow="GET STARTED"
        title={tenant ? "从配置到第一条回复" : "欢迎使用 Agent 平台"}
        subtitle="先在网页跑通一个 Agent，再按需接入 IM、知识库和工具。不需要先配置所有后端。"
        action={
          tenant && (
            <Button onClick={() => setRefresh((v) => v + 1)}>刷新进度</Button>
          )
        }
      />
      {error && (
        <Failure
          error={error}
          retry={tenant ? () => setRefresh((v) => v + 1) : undefined}
        />
      )}
      {!tenant ? (
        <Panel
          title="1. 创建你的工作空间"
          subtitle="一个工作空间对应一个租户，隔离 Agent 配置、模型连接、会话和授权。名称之后显示在顶部租户选择器中。"
        >
          {principal.role !== "superadmin" ? (
            <Alert type="info" title="请联系平台管理员分配租户访问权限" />
          ) : (
            <Form layout="vertical" onFinish={create}>
              <Form.Item
                label="工作空间名称"
                name="name"
                rules={[
                  { required: true, whitespace: true, message: "请输入名称" },
                ]}
              >
                <Input placeholder="例如：研发团队" maxLength={100} />
              </Form.Item>
              <Button htmlType="submit" type="primary" loading={busy}>
                创建工作空间并继续
              </Button>
            </Form>
          )}
        </Panel>
      ) : !state ? (
        <Skeleton active />
      ) : (
        <>
          <div className="onboarding-grid">
            <Panel title="1. 工作空间" subtitle="当前租户已经可用。">
              <Tag color="green">已创建</Tag>
              <p>顶部可切换租户；此处所有操作仅针对当前租户。</p>
            </Panel>
            <Panel
              title="2. 准备模型"
              subtitle="使用自己的 OpenAI-compatible 模型服务。"
            >
              <Tag color={state.connections ? "green" : "default"}>
                {state.connections ? "已有模型连接" : "可配置真实模型"}
              </Tag>
              <p>
                添加模型地址和 API Key，然后在 Agent
                里选择。也可以先用服务器已经配置好的模型。
              </p>
              <Button onClick={() => navigate("models")}>配置模型连接</Button>
            </Panel>
            <Panel
              title="3. 创建并调试 Agent"
              subtitle="修改提示词、选择模型，保存草稿后在右侧发消息。"
            >
              <Tag color={app ? "green" : "default"}>
                {app ? "已有 Agent" : "待创建"}
              </Tag>
              <p>先写清楚 Agent 要做什么，在右侧发消息看看效果。</p>
              <Space wrap>
                <Button
                  type="primary"
                  disabled={!writable(principal) && !app}
                  onClick={() => navigate("agents", app?.app_id || "")}
                >
                  {app ? "打开 Agent 工作台" : "创建第一个 Agent"}
                </Button>
                {app && (
                  <Button onClick={() => navigate("agents")}>
                    选择其他 Agent
                  </Button>
                )}
              </Space>
            </Panel>
            <Panel
              title="4. 发布与接入"
              subtitle="确认回复符合预期后发布，业务通道才使用该版本。"
            >
              <Tag color={published ? "green" : "default"}>
                {published ? "已有发布版本" : "待发布"}
              </Tag>
              <p>
                发布后，可以连接 Telegram 或企业微信，让用户直接给机器人发消息。
              </p>
              <Button
                disabled={!published}
                onClick={() => navigate("channels")}
              >
                连接机器人
              </Button>
            </Panel>
          </div>
          {state.more && (
            <p className="muted">
              进度按当前加载的前 100 个应用显示，其他应用请在 Agent 列表中查看。
            </p>
          )}
        </>
      )}
    </div>
  );
}
