import { useEffect, useState } from "react";
import {
  Alert,
  App,
  Button,
  Descriptions,
  Drawer,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Skeleton,
  Space,
  Table,
  Tabs,
  Tag,
} from "antd";
import { api, errorText } from "./api";
import { Blank, Failure, Icon, PageHeading, Panel, Status } from "./components";
import {
  date,
  writable,
  type AgentApp,
  type Dict,
  type Page,
  type Principal,
} from "./types";
import { navigate } from "./App";
import { ChannelDiagnostics } from "./ChannelDiagnostics";
import { JobTable } from "./ActivityPanel";

export function AgentList({
  tenant,
  principal,
  compact = false,
  initialApps,
}: {
  tenant: string;
  principal: Principal;
  compact?: boolean;
  initialApps?: AgentApp[];
}) {
  const { message } = App.useApp();
  const [items, setItems] = useState<AgentApp[]>([]);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [next, setNext] = useState("");
  const [form] = Form.useForm();
  useEffect(() => {
    if (compact) {
      setItems(initialApps || []);
      setBusy(false);
      return;
    }
    let live = true;
    setBusy(true);
    setError("");
    api<Page<AgentApp>>("catalog/list", {
      kind: "apps",
      tenant_id: tenant,
      limit: 50,
    })
      .then((data) => {
        if (live) {
          setItems(data.items);
          setNext(data.next || "");
        }
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      })
      .finally(() => {
        if (live) setBusy(false);
      });
    return () => {
      live = false;
    };
  }, [tenant, compact, initialApps, refresh]);
  async function create(values: Dict) {
    setSaving(true);
    try {
      const app = await api<AgentApp>("apps/onboard", {
        ...values,
        tenant_id: tenant,
      });
      setOpen(false);
      navigate("agents", app.app_id);
    } catch (e) {
      message.error(errorText(e));
    } finally {
      setSaving(false);
    }
  }
  const filtered = items
    .filter((a) =>
      (a.name + a.description + a.app_id)
        .toLowerCase()
        .includes(search.toLowerCase()),
    )
    .sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || ""));
  return (
    <>
      {!compact && (
        <>
          <PageHeading
            eyebrow="AGENTS"
            title="Agent 应用"
            subtitle="每个 Agent 拥有独立配置、发布版本和业务通道。"
            action={
              <Button
                type="primary"
                icon={<Icon name="plus" />}
                disabled={!writable(principal)}
                onClick={() => {
                  form.resetFields();
                  setOpen(true);
                }}
              >
                创建 Agent
              </Button>
            }
          />
          <div className="list-toolbar">
            <Input
              prefix={<Icon name="agent" />}
              placeholder="搜索应用名称或描述"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{ width: 320 }}
            />
            <Button
              icon={<Icon name="refresh" />}
              onClick={() => setRefresh((v) => v + 1)}
            >
              刷新
            </Button>
          </div>
        </>
      )}
      {error && (
        <Failure error={error} retry={() => setRefresh((v) => v + 1)} />
      )}{" "}
      {busy ? (
        <Skeleton active />
      ) : filtered.length ? (
        <div className="agent-grid">
          {(compact ? filtered.slice(0, 6) : filtered).map((app, index) => (
            <button
              className="agent-card"
              key={app.app_id}
              onClick={() => navigate("agents", app.app_id)}
            >
              <div className="agent-card-top">
                <span className={"agent-avatar color-" + (index % 4)}>
                  <Icon name="agent" size={27} />
                </span>
                <Status
                  value={app.stable_revision_id ? "published" : "draft"}
                />
              </div>
              <h3>{app.name}</h3>
              <p>
                {app.description || "打开工作台，配置模型、指令与可用能力。"}
              </p>
              <div className="agent-card-footer">
                <span>
                  {app.status === "active" ? "应用已启用" : "应用已暂停"}
                </span>
                <span>
                  进入工作台 <Icon name="arrow" size={15} />
                </span>
              </div>
            </button>
          ))}
        </div>
      ) : (
        <Blank title="还没有 Agent 应用">
          {!compact && writable(principal) && (
            <Button onClick={() => setOpen(true)}>创建第一个 Agent</Button>
          )}
        </Blank>
      )}
      {next && !compact && (
        <Button
          className="load-more"
          onClick={async () => {
            try {
              const data = await api<Page<AgentApp>>("catalog/list", {
                kind: "apps",
                tenant_id: tenant,
                limit: 50,
                after: next,
              });
              setItems((old) => [...old, ...data.items]);
              setNext(data.next || "");
            } catch (e) {
              message.error(errorText(e));
            }
          }}
        >
          加载更多
        </Button>
      )}
      <Modal
        title="创建 Agent"
        open={open}
        onCancel={() => setOpen(false)}
        footer={null}
        destroyOnHidden
      >
        <p className="muted">
          创建后进入工作台编辑草稿。尚未发布的 Agent 不会接管现有业务通道。
        </p>
        <Form form={form} layout="vertical" onFinish={create}>
          <Form.Item
            name="name"
            label="应用名称"
            rules={[{ required: true, message: "请输入应用名称" }]}
          >
            <Input placeholder="例如：研发知识助手" maxLength={100} />
          </Form.Item>
          <Form.Item name="description" label="用途说明">
            <Input.TextArea
              rows={3}
              placeholder="这个 Agent 为谁服务、解决什么问题？"
              maxLength={500}
            />
          </Form.Item>
          <Button htmlType="submit" type="primary" loading={saving} block>
            创建并配置
          </Button>
        </Form>
      </Modal>
    </>
  );
}

const resourceNames: Record<string, string> = {
  models: "模型配置",
  knowledge: "知识库",
  skills: "Skill 目录",
  backends: "数据后端",
  channels: "通道接入",
  tenants: "租户与策略",
  audit: "审计日志",
  operations: "业务操作",
};
const ids = (v: Dict) =>
  v.resource_id ||
  v.channel_binding_id ||
  v.binding_id ||
  v.audit_id ||
  v.operation_id ||
  v.tenant_id ||
  v.name;
export function ResourcePage({
  tenant,
  principal,
  initial = "skills",
  onChanged,
}: {
  tenant: string;
  principal: Principal;
  initial?: string;
  onChanged?: () => void;
}) {
  const { message } = App.useApp();
  const [kind, setKind] = useState(initial);
  const [rows, setRows] = useState<Dict[]>([]);
  const [apps, setApps] = useState<AgentApp[]>([]);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [next, setNext] = useState("");
  const [selected, setSelected] = useState<Dict | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [edit, setEdit] = useState(false);
  const [saving, setSaving] = useState(false);
  const [form] = Form.useForm();
  useEffect(() => {
    let live = true;
    setBusy(true);
    setError("");
    const path = ["models", "knowledge"].includes(kind)
      ? "resources/list"
      : kind === "skills"
        ? "skills/list"
        : kind === "audit"
          ? "audit/query"
          : kind === "operations"
            ? "tool-operations/list"
            : "catalog/list";
    api<any>(path, {
      ...(["catalog/list", "resources/list"].includes(path) ? { kind } : {}),
      ...(kind === "tenants" ? {} : { tenant_id: tenant }),
      ...(kind === "skills" || path === "resources/list" ? {} : { limit: 50 }),
    })
      .then((data) => {
        if (live) {
          setRows(Array.isArray(data) ? data : data.items || []);
          setNext(data.next || "");
        }
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      })
      .finally(() => {
        if (live) setBusy(false);
      });
    if (tenant)
      api<Page<AgentApp>>("catalog/list", {
        kind: "apps",
        tenant_id: tenant,
        limit: 100,
      })
        .then((p) => {
          if (live) setApps(p.items);
        })
        .catch(() => {});
    return () => {
      live = false;
    };
  }, [tenant, kind, refresh]);
  function startCreate() {
    form.resetFields();
    form.setFieldsValue({
      status: "disabled",
      channel_type: "telegram",
      message_mode: "realtime",
      message_age: 120,
      resource_type: "session",
      backend_type: "inmemory",
      region: "local",
      config: "{}",
    });
    setEdit(false);
    setCreateOpen(true);
  }
  function startEdit() {
    if (!selected) return;
    form.resetFields();
    form.setFieldsValue({
      ...selected,
      message_mode: selected.config?.message_policy?.mode || "realtime",
      message_age: selected.config?.message_policy?.max_age_seconds || 120,
      config: JSON.stringify(selected.config || {}, null, 2),
      quota_config: JSON.stringify(selected.quota_config || {}, null, 2),
      audit_policy: JSON.stringify(selected.audit_policy || {}, null, 2),
    });
    setEdit(true);
    setCreateOpen(true);
  }
  async function save(values: Dict) {
    setSaving(true);
    try {
      let path =
        kind === "tenants"
          ? "tenants"
          : kind === "channels"
            ? "channel-bindings"
            : "backend-bindings";
      let body: Dict = { ...values, tenant_id: tenant };
      delete body.message_mode;
      delete body.message_age;
      if (kind === "tenants") {
        if (edit) {
          path = "tenants/policies";
          body = {
            tenant_id: selected!.tenant_id,
            expected_version: selected!.version,
            quota_config: JSON.parse(values.quota_config),
            audit_policy: JSON.parse(values.audit_policy),
          };
        } else {
          const id = "tenant-" + crypto.randomUUID();
          body = { ...values, tenant_id: id, secret_namespace: "tenant/" + id };
        }
      } else {
        body.config = JSON.parse(values.config || "{}");
        if (
          kind === "channels" &&
          ["telegram", "wecom", "wecom_mcp"].includes(
            edit ? selected!.channel_type : values.channel_type,
          )
        ) {
          body.config.message_policy = {
            mode: values.message_mode || "realtime",
            max_age_seconds: values.message_age || 120,
          };
        }
        if (edit) {
          path = "channel-bindings/update";
          body = {
            tenant_id: tenant,
            binding_id: selected!.channel_binding_id,
            expected_version: selected!.version,
            status: values.status,
            config: body.config,
          };
        }
      }
      if (JSON.stringify(body).includes("[REDACTED"))
        throw new Error("请将脱敏占位值替换为有效引用，不要直接保存");
      await api(path, body);
      setCreateOpen(false);
      setSelected(null);
      setRefresh((v) => v + 1);
      onChanged?.();
      message.success("已保存");
    } catch (e) {
      message.error(errorText(e));
    } finally {
      setSaving(false);
    }
  }
  const canCreate =
    writable(principal) &&
    ["backends", "channels", "tenants"].includes(kind) &&
    (kind !== "tenants" || principal.role === "superadmin");
  const columns = [
    {
      title: "名称 / 标识",
      key: "name",
      render: (_: unknown, v: Dict) => (
        <div className="table-name">
          <strong>
            {v.display_name ||
              v.name ||
              v.tool_name ||
              v.channel_type ||
              v.resource_type ||
              v.decision ||
              "记录"}
          </strong>
          <small>{ids(v)}</small>
        </div>
      ),
    },
    {
      title: "类型 / 版本",
      key: "type",
      render: (_: unknown, v: Dict) => (
        <Tag>
          {v.backend_type ||
            v.channel_type ||
            (kind === "skills"
              ? "v" + v.version
              : kind === "audit"
                ? v.channel || "platform"
                : "v" + (v.version || "1"))}
        </Tag>
      ),
    },
    {
      title: "状态",
      key: "status",
      render: (_: unknown, v: Dict) => (
        <Status
          value={
            v.status ||
            v.migration_state ||
            (kind === "skills" ? "ready" : undefined)
          }
        />
      ),
    },
    {
      title: "",
      key: "action",
      render: (_: unknown, v: Dict) => (
        <Button type="link" onClick={() => setSelected(v)}>
          查看详情
        </Button>
      ),
    },
  ];
  return (
    <>
      <PageHeading
        eyebrow={initial === "skills" ? "RESOURCES" : "MANAGEMENT"}
        title={initial === "skills" ? "资源中心" : resourceNames[kind]}
        subtitle={
          kind === "channels"
            ? "连接业务通道；凭据使用服务端引用，不在页面填写真实 Token。"
            : kind === "tenants"
              ? "管理租户范围、配额与审计策略。"
              : "为 Agent 选择已获授权的能力和数据资源。"
        }
        action={
          canCreate && (
            <Button
              type="primary"
              icon={<Icon name="plus" />}
              onClick={startCreate}
            >
              新建
              {kind === "backends"
                ? "后端"
                : kind === "channels"
                  ? "绑定"
                  : "租户"}
            </Button>
          )
        }
      />
      {initial === "skills" && (
        <Tabs
          activeKey={kind}
          onChange={(k) => {
            setKind(k);
            setSelected(null);
          }}
          items={[
            "models",
            "knowledge",
            "skills",
            "backends",
            "audit",
            "operations",
          ].map((k) => ({
            key: k,
            label: resourceNames[k],
          }))}
        />
      )}
      <div className="list-toolbar">
        <span className="muted">{rows.length} 条已加载记录</span>
        <Button
          icon={<Icon name="refresh" />}
          onClick={() => setRefresh((v) => v + 1)}
        >
          刷新
        </Button>
      </div>
      {error && <Failure error={error} />}
      <div className="table-panel">
        <Table
          rowKey={(v) => String(ids(v))}
          columns={columns}
          dataSource={rows}
          loading={busy}
          pagination={false}
          locale={{
            emptyText: (
              <Blank
                title={
                  kind === "skills"
                    ? "当前租户还没有获授权的 Skill"
                    : "当前范围暂无记录"
                }
              />
            ),
          }}
        />
      </div>
      {next && (
        <Button
          className="load-more"
          onClick={async () => {
            try {
              const resource = ["models", "knowledge"].includes(kind);
              const data = await api<Page<Dict>>(
                resource ? "resources/list" : "catalog/list",
                {
                  kind,
                  tenant_id: kind === "tenants" ? undefined : tenant,
                  ...(resource ? {} : { limit: 50 }),
                  after: next,
                },
              );
              setRows((old) => [
                ...new Map(
                  [...old, ...data.items].map((item) => [ids(item), item]),
                ).values(),
              ]);
              setNext(data.next || "");
            } catch (e) {
              message.error(errorText(e));
            }
          }}
        >
          加载更多
        </Button>
      )}
      <Drawer
        title={resourceNames[kind] + "详情"}
        open={!!selected}
        onClose={() => setSelected(null)}
        size="large"
        extra={
          selected &&
          writable(principal) &&
          ["channels", "tenants"].includes(kind) && (
            <Button onClick={startEdit}>
              编辑{kind === "tenants" ? "策略" : "绑定"}
            </Button>
          )
        }
      >
        {selected && (
          <>
            <Descriptions
              column={1}
              items={[
                {
                  key: "id",
                  label: "标识",
                  children: <code>{ids(selected)}</code>,
                },
                {
                  key: "status",
                  label: "状态",
                  children: (
                    <Status
                      value={selected.status || selected.migration_state}
                    />
                  ),
                },
                {
                  key: "updated",
                  label: "更新时间",
                  children: date(selected.updated_at || selected.occurred_at),
                },
              ]}
            />
            {kind === "skills" && (
              <Alert
                type="info"
                showIcon
                title={selected.description}
                description="在 Agent 工作台勾选后才会启用，执行仍需要审批。"
              />
            )}
            {kind === "backends" && (
              <Alert
                type="info"
                showIcon
                title="已注册后端不直接覆盖"
                description="物理后端切换应走受控迁移，避免丢失会话和知识库。"
              />
            )}
            {kind === "channels" && (
              <ChannelDiagnostics
                tenant={tenant}
                bindingID={selected.channel_binding_id}
              />
            )}
            <h4>配置与记录</h4>
            <pre className="json-view">{JSON.stringify(selected, null, 2)}</pre>
          </>
        )}
      </Drawer>
      <Modal
        title={(edit ? "编辑" : "新建") + resourceNames[kind]}
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        footer={null}
        width={650}
        destroyOnHidden
      >
        <Form form={form} layout="vertical" onFinish={save}>
          {kind === "tenants" ? (
            edit ? (
              <>
                <Form.Item name="quota_config" label="配额策略（高级 JSON）">
                  <Input.TextArea rows={5} />
                </Form.Item>
                <Form.Item name="audit_policy" label="审计策略（高级 JSON）">
                  <Input.TextArea rows={5} />
                </Form.Item>
              </>
            ) : (
              <>
                <Form.Item
                  name="display_name"
                  label="租户名称"
                  rules={[{ required: true }]}
                >
                  <Input />
                </Form.Item>
                <Form.Item
                  name="region"
                  label="区域"
                  rules={[{ required: true }]}
                >
                  <Input />
                </Form.Item>
                <p className="muted">
                  租户 ID
                  与密钥命名空间自动生成。新租户的外部资源仍需部署者授权。
                </p>
              </>
            )
          ) : (
            <>
              {!edit && (
                <Form.Item
                  name="app_id"
                  label="所属 Agent"
                  rules={kind === "channels" ? [{ required: true }] : []}
                >
                  <Select
                    allowClear
                    options={apps.map((a) => ({
                      value: a.app_id,
                      label: a.name,
                    }))}
                    placeholder={
                      kind === "backends"
                        ? "留空作为租户默认后端"
                        : "选择 Agent"
                    }
                  />
                </Form.Item>
              )}
              {kind === "channels" ? (
                <>
                  {!edit && (
                    <>
                      <Form.Item name="channel_type" label="通道类型">
                        <Select
                          options={[
                            { value: "telegram", label: "Telegram" },
                            { value: "wecom", label: "企业微信自建应用" },
                            { value: "wecom_mcp", label: "企业微信消息 MCP" },
                            { value: "http", label: "HTTP" },
                          ]}
                        />
                      </Form.Item>
                      <Form.Item
                        name="account_id"
                        label="外部账号标识"
                        rules={[{ required: true }]}
                      >
                        <Input />
                      </Form.Item>
                      <Form.Item name="secret_ref" label="通道凭据引用">
                        <Input placeholder="env://DEPLOYMENT_SECRET" />
                      </Form.Item>
                    </>
                  )}
                  <Form.Item name="status" label="接入状态">
                    <Select
                      options={[
                        { value: "disabled", label: "停用" },
                        { value: "active", label: "启用" },
                      ]}
                    />
                  </Form.Item>
                  <Form.Item
                    name="message_mode"
                    label="聊天消息处理策略"
                    help="适用于 IM 通道。近期优先不会自动执行过期请求；完整补读可能让历史积压延迟新消息。"
                  >
                    <Select
                      options={[
                        { value: "realtime", label: "近期优先（默认）" },
                        {
                          value: "reliable",
                          label: "完整补读（明确需要历史处理时启用）",
                        },
                      ]}
                    />
                  </Form.Item>
                  <Form.Item
                    name="message_age"
                    label="近期消息有效期（秒）"
                    help="30～120 秒；只约束尚未开始的聊天请求，不中断已经执行的操作。"
                  >
                    <InputNumber min={30} max={120} precision={0} />
                  </Form.Item>
                </>
              ) : (
                <>
                  <Form.Item name="resource_type" label="资源类型">
                    <Select
                      options={[
                        "session",
                        "memory",
                        "knowledge",
                        "artifact",
                      ].map((value) => ({
                        value,
                        label: {
                          session: "会话",
                          memory: "长期记忆",
                          knowledge: "知识库",
                          artifact: "附件",
                        }[value],
                      }))}
                    />
                  </Form.Item>
                  <Form.Item name="backend_type" label="后端类型">
                    <Select
                      options={[
                        "inmemory",
                        "redis",
                        "postgres",
                        "qdrant",
                        "s3",
                      ].map((value) => ({ value, label: value }))}
                    />
                  </Form.Item>
                  <Form.Item name="secret_ref" label="凭据引用">
                    <Input placeholder="env://DEPLOYMENT_SECRET" />
                  </Form.Item>
                </>
              )}
              <Form.Item
                name="config"
                label="连接配置（高级 JSON）"
                help="使用已授权的凭据引用；不要填写真实密码、API Key 或回调密钥。"
              >
                <Input.TextArea rows={8} className="code-input" />
              </Form.Item>
            </>
          )}
          <Button type="primary" htmlType="submit" block loading={saving}>
            保存{edit ? "修改" : "配置"}
          </Button>
        </Form>
      </Modal>
    </>
  );
}

export function RunsPage({
  tenant,
  principal,
  appID,
}: {
  tenant: string;
  principal: Principal;
  appID?: string;
}) {
  const [rows, setRows] = useState<Dict[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [status, setStatus] = useState("");
  const [appFilter, setAppFilter] = useState("");
  const [channel, setChannel] = useState("");
  const [dateStart, setDateStart] = useState("");
  const [dateEnd, setDateEnd] = useState("");
  const [next, setNext] = useState<Dict | null>(null);
  const [apps, setApps] = useState<AgentApp[]>([]);
  useEffect(() => {
    let live = true;
    if (!appID)
      api<Page<AgentApp>>("catalog/list", {
        tenant_id: tenant,
        kind: "apps",
        limit: 100,
      })
        .then((data) => {
          if (live) setApps(data.items);
        })
        .catch(() => {});
    return () => {
      live = false;
    };
  }, [tenant, appID]);
  function filters() {
    let before = "";
    if (dateEnd) {
      const end = new Date(dateEnd + "T00:00:00");
      end.setDate(end.getDate() + 1);
      before = end.toISOString();
    }
    return {
      tenant_id: tenant,
      app_id: appID || appFilter,
      status,
      channel,
      limit: 50,
      after_time: dateStart
        ? new Date(dateStart + "T00:00:00").toISOString()
        : "",
      before_time: before,
    };
  }
  const [detail, setDetail] = useState<Dict | null>(null);
  useEffect(() => {
    let live = true;
    setBusy(true);
    setError("");
    api<{ items: Dict[]; next: Dict | null }>("runs/list", filters())
      .then((data) => {
        if (live) {
          setRows(data.items);
          setNext(data.next);
        }
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      })
      .finally(() => {
        if (live) setBusy(false);
      });
    return () => {
      live = false;
    };
  }, [tenant, appID, status, refresh, appFilter, channel, dateStart, dateEnd]);
  async function open(row: Dict) {
    try {
      setDetail(
        await api<Dict>("runs/get", {
          tenant_id: tenant,
          request_id: row.request_id,
          kind: row.kind,
        }),
      );
    } catch (e) {
      setError(errorText(e));
    }
  }
  return (
    <>
      {!appID && (
        <PageHeading
          eyebrow="OBSERVABILITY"
          title="运行记录"
          subtitle="从一次请求追踪到模型、工具审批与通道投递。"
        />
      )}
      <div className="list-toolbar">
        <Space wrap>
          {!appID && (
            <Select
              style={{ width: 180 }}
              value={appFilter}
              onChange={setAppFilter}
              options={[
                { value: "", label: "全部 Agent" },
                ...apps.map((app) => ({ value: app.app_id, label: app.name })),
              ]}
            />
          )}
          <Select
            style={{ width: 135 }}
            value={channel}
            onChange={setChannel}
            options={[
              { value: "", label: "全部来源" },
              { value: "console", label: "网页调试" },
              { value: "telegram", label: "Telegram" },
              { value: "wecom_mcp", label: "企业微信 MCP" },
              { value: "wecom", label: "企业微信应用" },
              { value: "http", label: "HTTP" },
            ]}
          />
          <Select
            value={status}
            onChange={setStatus}
            style={{ width: 160 }}
            options={[
              { value: "", label: "全部状态" },
              ...[
                "running",
                "completed",
                "expired",
                "failed",
                "unknown",
                "awaiting_approval",
              ].map((value) => ({
                value,
                label: {
                  running: "执行中",
                  completed: "已完成",
                  expired: "已过期 · 未执行",
                  failed: "失败",
                  unknown: "待核对",
                  awaiting_approval: "等待审批",
                }[value],
              })),
            ]}
          />
          <Input
            type="date"
            aria-label="开始日期"
            style={{ width: 145 }}
            value={dateStart}
            onChange={(e) => setDateStart(e.target.value)}
          />
          <span className="muted">至</span>
          <Input
            type="date"
            aria-label="结束日期"
            style={{ width: 145 }}
            value={dateEnd}
            onChange={(e) => setDateEnd(e.target.value)}
          />
        </Space>
        <Button
          icon={<Icon name="refresh" />}
          onClick={() => setRefresh((v) => v + 1)}
        >
          刷新
        </Button>
      </div>
      {error && <Failure error={error} />}
      <div className="table-panel">
        <Table
          dataSource={rows}
          rowKey="request_id"
          loading={busy}
          pagination={false}
          columns={[
            {
              title: "请求",
              render: (_, r: Dict) => (
                <div className="table-name">
                  <strong>{r.app_name || r.agent_name || r.app_id}</strong>
                  <small>{r.request_id}</small>
                </div>
              ),
            },
            {
              title: "来源",
              render: (_, r: Dict) => (
                <Tag>{r.kind === "debug" ? "网页调试" : r.channel || "IM"}</Tag>
              ),
            },
            {
              title: "状态",
              render: (_, r: Dict) => (
                <Status
                  value={
                    r.status === "completed" && r.error_type
                      ? "completed_with_issues"
                      : r.status
                  }
                />
              ),
            },
            { title: "时间", render: (_, r: Dict) => date(r.created_at) },
            {
              title: "耗时",
              render: (_, r: Dict) =>
                typeof r.latency_ms === "number"
                  ? `${(r.latency_ms / 1000).toFixed(2)}s`
                  : "未记录",
            },
            {
              title: "Token / 成本",
              render: (_, r: Dict) => (
                <div className="table-name">
                  <strong>
                    {(r.prompt_tokens || 0) + (r.completion_tokens || 0)}
                  </strong>
                  <small>{Number(r.cost || 0).toFixed(6)}（计费单位）</small>
                </div>
              ),
            },
            {
              title: "",
              render: (_, r: Dict) => (
                <Button type="link" onClick={() => open(r)}>
                  查看执行过程
                </Button>
              ),
            },
          ]}
          locale={{ emptyText: <Blank title="当前范围暂无运行记录" /> }}
        />
      </div>
      {next && (
        <Button
          className="load-more"
          onClick={async () => {
            try {
              const data = await api<{ items: Dict[]; next: Dict | null }>(
                "runs/list",
                { ...filters(), ...next },
              );
              setRows((old) => [...old, ...data.items]);
              setNext(data.next);
            } catch (e) {
              setError(errorText(e));
            }
          }}
        >
          加载更早记录
        </Button>
      )}
      <Drawer
        title="执行详情"
        open={!!detail}
        onClose={() => setDetail(null)}
        size="large"
      >
        {detail && (
          <>
            <Descriptions
              column={1}
              items={[
                {
                  key: "id",
                  label: "请求编号",
                  children: <code>{detail.request_id}</code>,
                },
                {
                  key: "version",
                  label: "版本",
                  children: <code>{detail.revision_id || "—"}</code>,
                },
                {
                  key: "trace",
                  label: "Trace",
                  children: <code>{detail.trace_id || "未提供"}</code>,
                },
                {
                  key: "state",
                  label: "状态",
                  children: <Status value={detail.status} />,
                },
                {
                  key: "usage",
                  label: "耗时 / 用量",
                  children: `${typeof detail.latency_ms === "number" ? (detail.latency_ms / 1000).toFixed(2) + "s" : "耗时未记录"} · 输入 ${detail.prompt_tokens || 0} / 输出 ${detail.completion_tokens || 0} Token · 成本 ${Number(detail.cost || 0).toFixed(6)}（按部署计费单位；未配置价格时为 0）`,
                },
              ]}
            />
            {detail.error_type && (
              <Alert
                type={detail.status === "expired" ? "info" : "error"}
                title={
                  detail.status === "expired"
                    ? "聊天消息已过期，未进入 Agent 执行"
                    : detail.error_type
                }
                description={
                  detail.status === "expired"
                    ? "未调用模型或工具，也未补发旧回复。如仍需要处理，请发送一条新消息。"
                    : "执行失败不代表外部操作已回滚；未知结果请先核对。"
                }
              />
            )}
            <h4>工具执行</h4>
            {detail.tools?.length ? (
              detail.tools.map((tool: Dict) => (
                <div className="tool-event" key={tool.execution_id}>
                  <Icon name="code" />
                  <div>
                    <strong>{tool.tool_name}</strong>
                    <small>{tool.error_type || date(tool.completed_at)}</small>
                  </div>
                  <Status value={tool.status} />
                </div>
              ))
            ) : (
              <p className="muted">没有已授权执行的工具记录。</p>
            )}
            <h4>关联后台任务</h4>
            {detail.jobs_state === "unavailable" ? (
              <Alert type="warning" title="后台任务记录暂时无法读取" />
            ) : (
              <JobTable items={detail.jobs || []} />
            )}
            <p className="muted">
              网页调试默认不自动提取记忆或生成摘要；旧请求未记录关联编号时，不推测后台任务归属。
            </p>
            <h4>审批与安全决策</h4>
            <pre className="json-view">
              {JSON.stringify(detail.decisions || [], null, 2)}
            </pre>
            {detail.reply && writable(principal) && (
              <>
                <h4>回复</h4>
                <div className="message-content">{detail.reply}</div>
              </>
            )}
            {detail.delivery && (
              <>
                <h4>通道投递</h4>
                <pre className="json-view">
                  {JSON.stringify(detail.delivery, null, 2)}
                </pre>
              </>
            )}
          </>
        )}
      </Drawer>
    </>
  );
}

export function SystemPage({ tenant }: { tenant: string }) {
  const [result, setResult] = useState<Dict | null>(null);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [probing, setProbing] = useState(false);
  useEffect(() => {
    let live = true;
    api<Dict>("system/status", { tenant_id: tenant })
      .then((data) => {
        if (live) setResult(data);
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant, refresh]);
  return (
    <>
      <PageHeading
        eyebrow="SYSTEM HEALTH"
        title="系统状态"
        subtitle="区分配置状态、执行节点观测与真实模型调用结果。"
        action={
          <Button
            icon={<Icon name="refresh" />}
            onClick={() => {
              setError("");
              setRefresh((v) => v + 1);
            }}
          >
            刷新状态
          </Button>
        }
      />
      {error && <Failure error={error} />}
      <Alert
        type="info"
        showIcon
        title="此页面不会自动启动模型、Tunnel 或 IM 服务"
        description="刷新只执行平台依赖检查，不发送模型生成请求。需要验证模型时，请在 Agent 工作台发起一次调试。"
      />
      {result ? (
        <>
          <div className="health-grid">
            {(result.checks || []).map((check: Dict) => (
              <Panel
                key={check.name}
                title={check.label || check.name}
                action={<Status value={check.state} />}
              >
                <p>{check.description}</p>
                {check.probe_available && (
                  <Button
                    size="small"
                    loading={probing}
                    onClick={async () => {
                      setProbing(true);
                      setError("");
                      try {
                        await api("system/probe", { tenant_id: tenant });
                        setRefresh((v) => v + 1);
                      } catch (e) {
                        setError(errorText(e));
                      } finally {
                        setProbing(false);
                      }
                    }}
                  >
                    检查公网健康入口
                  </Button>
                )}
                <div className="health-meta">
                  <span>来源：{check.source || "未观测"}</span>
                  <span>{date(check.observed_at)}</span>
                </div>
              </Panel>
            ))}
          </div>
          <h3>Worker 依赖检查明细</h3>
          <Table
            rowKey="worker_id"
            pagination={false}
            dataSource={result.workers || []}
            columns={[
              { title: "节点", dataIndex: "worker_id" },
              { title: "最近心跳", render: (_, r: Dict) => date(r.updated_at) },
              {
                title: "检查结果",
                render: (_, r: Dict) => (
                  <Space direction="vertical">
                    {(r.checks || []).map((c: Dict) => (
                      <span key={c.component + ":" + (c.binding_id || "")}>
                        {c.binding_id || c.component} <Status value={c.state} />{" "}
                        <small>{date(c.observed_at)}</small>
                      </span>
                    ))}
                  </Space>
                ),
              },
            ]}
          />
        </>
      ) : (
        !error && <Skeleton active />
      )}
    </>
  );
}
