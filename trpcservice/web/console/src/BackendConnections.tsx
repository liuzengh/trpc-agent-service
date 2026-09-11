import { useEffect, useState } from "react";
import {
  Alert,
  App,
  Button,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tag,
} from "antd";
import { api, errorText } from "./api";
import { Failure, Panel } from "./components";
import { CredentialSelect } from "./CredentialSelect";
import {
  writable,
  type AgentApp,
  type Dict,
  type Page,
  type Principal,
} from "./types";

const resources: Record<string, string> = {
  session: "会话",
  memory: "长期记忆",
  knowledge: "知识库",
  artifact: "附件",
};
const kinds: Record<string, string[]> = {
  session: ["redis", "postgres", "inmemory"],
  memory: ["redis", "postgres", "inmemory"],
  knowledge: ["qdrant", "inmemory"],
  artifact: ["s3", "inmemory"],
};
const labels: Record<string, string> = {
  redis: "Redis",
  postgres: "PostgreSQL",
  qdrant: "Qdrant",
  s3: "S3 / MinIO",
  inmemory: "内存（非持久化）",
};
interface Connection {
  connection_id: string;
  name: string;
  resource_type: string;
  backend_type: string;
  settings: Dict;
}
interface ConnectionPage extends Page<Connection> {
  enabled: boolean;
}
const emptyCredentials = {
  password: "",
  api_key: "",
  access_key_id: "",
  secret_access_key: "",
};
function defaults(kind: string) {
  return {
    host: "",
    port: kind === "redis" ? 6379 : kind === "postgres" ? 5432 : 6334,
    database: kind === "redis" ? "0" : "",
    ssl_mode: "require",
    tls: false,
    dimensions: 1536,
    path_style: true,
    region: "us-east-1",
  };
}

export function BackendConnections({
  tenant,
  principal,
}: {
  tenant: string;
  principal: Principal;
}) {
  const { message } = App.useApp();
  const [rows, setRows] = useState<Connection[]>([]),
    [bindings, setBindings] = useState<Dict[]>([]),
    [apps, setApps] = useState<AgentApp[]>([]);
  const [enabled, setEnabled] = useState(false),
    [busy, setBusy] = useState(false),
    [saving, setSaving] = useState(false),
    [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0),
    [next, setNext] = useState(""),
    [open, setOpen] = useState(false),
    [selected, setSelected] = useState<Connection | null>(null),
    [appID, setAppID] = useState<string | undefined>();
  const [form] = Form.useForm();
  const [creationID, setCreationID] = useState("");
  const resource = Form.useWatch("resource_type", form) || "session";
  const kind = Form.useWatch("backend_type", form) || "redis";
  const mode = Form.useWatch("credential_mode", form) || "new";
  const superadmin = principal.role === "superadmin";
  useEffect(() => {
    let live = true;
    setBusy(true);
    setError("");
    setRows([]);
    setBindings([]);
    setSelected(null);
    Promise.all([
      api<ConnectionPage>("backend-connections/list", { tenant_id: tenant }),
      api<Page<Dict>>("catalog/list", {
        tenant_id: tenant,
        kind: "backends",
        limit: 100,
      }),
      api<Page<AgentApp>>("catalog/list", {
        tenant_id: tenant,
        kind: "apps",
        limit: 100,
      }),
    ])
      .then(([c, b, a]) => {
        if (live) {
          setRows(c.items);
          setNext(c.next || "");
          setEnabled(c.enabled);
          setBindings(b.items);
          setApps(a.items);
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
  }, [tenant, refresh]);
  function chooseKind(value: string) {
    form.setFieldsValue({
      backend_type: value,
      settings: defaults(value),
      credentials: emptyCredentials,
      credential_ref: undefined,
    });
  }
  function create() {
    setCreationID("storage-" + crypto.randomUUID());
    form.resetFields();
    form.setFieldsValue({
      resource_type: "session",
      backend_type: superadmin ? "redis" : "inmemory",
      credential_mode: "new",
      settings: defaults("redis"),
    });
    setError("");
    setOpen(true);
  }
  async function save(values: Dict) {
    setSaving(true);
    setError("");
    try {
      await api("backend-connections/create", {
        tenant_id: tenant,
        connection_id: creationID,
        name: values.name,
        resource_type: values.resource_type,
        backend_type: values.backend_type,
        settings:
          values.backend_type === "inmemory" ? {} : values.settings || {},
        credentials:
          values.credential_mode === "new" && values.backend_type !== "inmemory"
            ? values.credentials || {}
            : {},
        credential_ref:
          values.credential_mode === "existing" &&
          values.backend_type !== "inmemory"
            ? values.credential_ref || ""
            : "",
      });
      setOpen(false);
      setRefresh((v) => v + 1);
      message.success(
        "连接已保存，请选择要绑定的 Agent。保存不代表已通过连通性验证。",
      );
    } catch (e) {
      setError(errorText(e));
    } finally {
      form.setFieldsValue({ credentials: emptyCredentials });
      setSaving(false);
    }
  }
  async function bind() {
    if (!selected) return;
    setSaving(true);
    setError("");
    try {
      await api("backend-connections/bind", {
        tenant_id: tenant,
        connection_id: selected.connection_id,
        app_id: appID || "",
      });
      setSelected(null);
      setRefresh((v) => v + 1);
      message.success("后端绑定已创建");
    } catch (e) {
      setError(errorText(e));
    } finally {
      setSaving(false);
    }
  }
  const field = (
    name: string,
    label: string,
    required = false,
    child?: React.ReactNode,
  ) => (
    <Form.Item
      name={["settings", name]}
      label={label}
      rules={required ? [{ required: true, message: "请填写" + label }] : []}
      preserve={false}
    >
      {child || <Input autoComplete="off" />}
    </Form.Item>
  );
  const password = (name: string, label: string, required = false) => (
    <Form.Item
      name={["credentials", name]}
      label={label}
      rules={required ? [{ required: true, message: "请填写" + label }] : []}
      preserve={false}
    >
      <Input.Password
        autoComplete="new-password"
        placeholder="仅本次提交，加密保存，不回显"
      />
    </Form.Item>
  );
  return (
    <>
      <Panel
        title="数据后端连接"
        subtitle="先创建连接，再绑定到当前工作空间的 Agent。连接和凭据不会跨租户共享。"
      >
        <Space wrap>
          <Button
            type="primary"
            disabled={!writable(principal) || !enabled}
            onClick={create}
          >
            新建连接
          </Button>
          <Button onClick={() => setRefresh((v) => v + 1)}>刷新</Button>
        </Space>
        {!busy && !enabled && (
          <Alert
            type="info"
            showIcon
            title="连接管理需要持久化控制面和加密配置"
            description="部署者需完成 schema 32 迁移并配置加密主密钥。"
          />
        )}
        {!superadmin && (
          <p className="muted">
            你可以绑定当前空间已有连接，或创建内存连接。外部服务器地址和凭据由平台管理员统一配置。
          </p>
        )}
        {error && !open && !selected && <Failure error={error} />}
        <Table
          rowKey="connection_id"
          loading={busy}
          dataSource={rows}
          pagination={false}
          columns={[
            { title: "连接名称", dataIndex: "name" },
            {
              title: "用途",
              dataIndex: "resource_type",
              render: (v: string) => resources[v] || v,
            },
            {
              title: "后端",
              dataIndex: "backend_type",
              render: (v: string) => labels[v] || v,
            },
            {
              title: "操作",
              render: (_: unknown, c: Connection) => (
                <Button
                  disabled={!writable(principal)}
                  onClick={() => {
                    setSelected(c);
                    setAppID(undefined);
                    setError("");
                  }}
                >
                  绑定到 Agent
                </Button>
              ),
            },
          ]}
        />
        {next && (
          <Button
            onClick={async () => {
              try {
                const p = await api<ConnectionPage>(
                  "backend-connections/list",
                  { tenant_id: tenant, after: next },
                );
                setRows((v) => [...v, ...p.items]);
                setNext(p.next || "");
              } catch (e) {
                setError(errorText(e));
              }
            }}
          >
            加载更多连接
          </Button>
        )}
      </Panel>
      <Panel
        title="已绑定后端"
        subtitle="现有数据后端不在这里直接替换。切换有数据的后端需要执行受控迁移。"
      >
        <Table
          rowKey="binding_id"
          dataSource={bindings}
          pagination={false}
          columns={[
            {
              title: "资源",
              dataIndex: "resource_type",
              render: (v: string) => resources[v] || v,
            },
            { title: "后端", dataIndex: "backend_type" },
            {
              title: "范围",
              render: (_: unknown, b: Dict) =>
                apps.find((a) => a.app_id === b.app_id)?.name ||
                b.app_id ||
                "工作空间默认",
            },
            {
              title: "状态",
              dataIndex: "migration_state",
              render: (v: string) => <Tag>{v}</Tag>,
            },
          ]}
        />
      </Panel>
      <Modal
        title="新建存储连接"
        open={open}
        onCancel={() => {
          setOpen(false);
          form.setFieldsValue({ credentials: emptyCredentials });
        }}
        footer={null}
        width={650}
        destroyOnHidden
      >
        <Form form={form} layout="vertical" onFinish={save}>
          <Form.Item
            name="name"
            label="连接名称"
            rules={[
              { required: true, whitespace: true, message: "请填写连接名称" },
            ]}
          >
            <Input maxLength={100} placeholder="例如：客服会话 Redis" />
          </Form.Item>
          <div className="form-grid">
            <Form.Item
              name="resource_type"
              label="保存的数据"
              rules={[{ required: true }]}
            >
              <Select
                options={Object.entries(resources).map(([value, label]) => ({
                  value,
                  label,
                }))}
                onChange={(v) =>
                  chooseKind(superadmin ? kinds[v][0] : "inmemory")
                }
              />
            </Form.Item>
            <Form.Item
              name="backend_type"
              label="后端类型"
              rules={[{ required: true }]}
            >
              <Select
                options={(superadmin ? kinds[resource] : ["inmemory"]).map(
                  (value) => ({ value, label: labels[value] }),
                )}
                onChange={chooseKind}
              />
            </Form.Item>
          </div>
          {kind === "inmemory" ? (
            <Alert
              type="warning"
              showIcon
              title="内存后端不需要凭据"
              description="数据只保存在当前进程中，重启后丢失，不用于多节点或持久化业务。"
            />
          ) : (
            <>
              <Form.Item name="credential_mode" label="连接配置来源">
                <Select
                  options={[
                    ...(superadmin
                      ? [{ value: "new", label: "填写连接信息" }]
                      : []),
                    { value: "existing", label: "选择部署者已授权的凭据" },
                  ]}
                  onChange={() =>
                    form.setFieldsValue({
                      credentials: emptyCredentials,
                      credential_ref: undefined,
                    })
                  }
                />
              </Form.Item>
              {mode === "existing" && (
                <Form.Item
                  name="credential_ref"
                  label="已授权凭据"
                  rules={[{ required: true, message: "请选择已授权凭据" }]}
                  preserve={false}
                >
                  <CredentialSelect tenant={tenant} purpose={resource} />
                </Form.Item>
              )}
              {(kind === "qdrant" ||
                ((kind === "redis" || kind === "postgres") &&
                  mode === "new")) && (
                <div className="form-grid">
                  {field(
                    "host",
                    "主机地址",
                    true,
                    <Input placeholder="数据库主机名或 IP，不含协议和密码" />,
                  )}
                  {field(
                    "port",
                    "端口",
                    true,
                    <InputNumber
                      min={1}
                      max={65535}
                      style={{ width: "100%" }}
                    />,
                  )}
                </div>
              )}
              {(kind === "redis" || kind === "postgres") && mode === "new" && (
                <>
                  <div className="form-grid">
                    {field(
                      "database",
                      kind === "redis" ? "数据库编号" : "数据库名称",
                      true,
                    )}
                    {field("username", "用户名", kind === "postgres")}
                  </div>
                  {password("password", "密码", kind === "postgres")}
                  {kind === "postgres" ? (
                    field(
                      "ssl_mode",
                      "TLS 模式",
                      true,
                      <Select
                        options={[
                          { value: "require", label: "要求加密" },
                          { value: "verify-full", label: "校验证书与主机名" },
                          { value: "verify-ca", label: "校验证书" },
                          {
                            value: "disable",
                            label: "不加密（仅可信隔离网络）",
                          },
                        ]}
                      />,
                    )
                  ) : (
                    <Form.Item
                      name={["settings", "tls"]}
                      label="使用 TLS"
                      valuePropName="checked"
                      preserve={false}
                    >
                      <Switch />
                    </Form.Item>
                  )}
                </>
              )}
              {kind === "redis" && field("key_prefix", "键名前缀（可选）")}
              {kind === "postgres" && (
                <>
                  {resource === "memory" && field("schema", "Schema（可选）")}
                  {field(
                    "table_name",
                    resource === "memory" ? "表名（可选）" : "表名前缀（可选）",
                  )}
                </>
              )}
              {kind === "qdrant" && (
                <>
                  <div className="form-grid">
                    {field("collection", "Collection 名称", true)}
                    {field(
                      "dimensions",
                      "向量维度",
                      true,
                      <InputNumber
                        min={1}
                        max={65536}
                        style={{ width: "100%" }}
                      />,
                    )}
                  </div>
                  <Form.Item
                    name={["settings", "tls"]}
                    label="使用 TLS"
                    valuePropName="checked"
                    preserve={false}
                  >
                    <Switch />
                  </Form.Item>
                  {mode === "new" &&
                    password("api_key", "API Key（服务启用鉴权时填写）")}
                </>
              )}
              {kind === "s3" && (
                <>
                  {field(
                    "endpoint",
                    "Endpoint（AWS 默认端点可留空）",
                    false,
                    <Input placeholder="https://s3.example.com" />,
                  )}
                  <div className="form-grid">
                    {field("region", "Region", true)}
                    {field("bucket", "Bucket", true)}
                  </div>
                  <Form.Item
                    name={["settings", "path_style"]}
                    label="Path-style（MinIO 通常启用）"
                    valuePropName="checked"
                    preserve={false}
                  >
                    <Switch />
                  </Form.Item>
                  {mode === "new" && (
                    <>
                      {password("access_key_id", "Access Key ID", true)}
                      {password("secret_access_key", "Secret Access Key", true)}
                    </>
                  )}
                </>
              )}
            </>
          )}
          <p className="muted">
            保存仅校验配置，不会探测服务或迁移数据。首次使用 SQL
            后端时，凭据需要相应表的初始化权限；生产环境应由部署者预先建表并配置最小权限。
          </p>
          {error && <Failure error={error} />}
          <Button htmlType="submit" type="primary" loading={saving} block>
            保存连接
          </Button>
        </Form>
      </Modal>
      <Modal
        title={"绑定连接 · " + (selected?.name || "")}
        open={!!selected}
        onCancel={() => setSelected(null)}
        onOk={bind}
        confirmLoading={saving}
        okText="创建绑定"
      >
        <p>
          选择目标
          Agent，或作为当前工作空间的默认后端。不会覆盖已经存在的同类绑定。
        </p>
        <Select
          allowClear
          style={{ width: "100%" }}
          value={appID}
          onChange={setAppID}
          options={apps.map((a) => ({ value: a.app_id, label: a.name }))}
          placeholder="工作空间默认后端"
        />
        {error && <Failure error={error} />}
      </Modal>
    </>
  );
}
