import { useEffect, useState } from "react";
import {
  Alert,
  App,
  Button,
  Form,
  Input,
  Modal,
  Select,
  Skeleton,
  Space,
  Table,
  Tag,
} from "antd";
import { api, errorText } from "./api";
import { Blank, Failure, Icon, PageHeading, Panel } from "./components";
import { date, type Principal } from "./types";
import { navigate } from "./App";
import { EditModelConnection } from "./EditModelConnection";
import {
  ModelEndpointHint,
  type ModelEndpointPolicy,
} from "./ModelEndpointHint";

export interface ModelConnection {
  tenant_id: string;
  connection_id: string;
  name: string;
  model_name: string;
  base_url: string;
  created_at: string;
  root_connection_id: string;
  config_version: number;
  credential_version: number;
  version: number;
  superseded_by?: string;
  updated_at: string;
}
export interface ModelConnectionPage {
  items: ModelConnection[];
  next?: string;
  enabled: boolean;
  allowed_origins: string[];
  endpoint_policy: ModelEndpointPolicy;
}

export function CreateModelConnection({
  tenant,
  origins,
  policy,
  onCreated,
  onCancel,
}: {
  tenant: string;
  origins: string[];
  policy: ModelEndpointPolicy;
  onCreated: (value: ModelConnection) => void;
  onCancel: () => void;
}) {
  const [form] = Form.useForm();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [id] = useState(() => "model-" + crypto.randomUUID());
  async function save(values: Record<string, string>) {
    setBusy(true);
    setError("");
    try {
      const result = await api<ModelConnection>("model-connections/create", {
        ...values,
        tenant_id: tenant,
        connection_id: id,
      });
      form.resetFields();
      onCreated(result);
    } catch (e) {
      // Do not retain the credential after a failed or uncertain request.
      form.setFieldValue("api_key", "");
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title="新增模型连接"
      open
      footer={null}
      closable={!busy}
      maskClosable={false}
      onCancel={() => {
        form.resetFields();
        onCancel();
      }}
      destroyOnHidden
    >
      <p className="muted">
        添加后，当前工作空间的 Agent 就能选择这个模型。API Key 加密保存。
      </p>
      <Form form={form} layout="vertical" onFinish={save}>
        <Form.Item
          label="连接名称"
          name="name"
          rules={[
            { required: true, whitespace: true, message: "请为连接取一个名称" },
          ]}
        >
          <Input
            placeholder="例如：团队开发模型"
            maxLength={100}
            disabled={busy}
          />
        </Form.Item>
        <Form.Item
          label="模型 ID"
          name="model_name"
          rules={[
            {
              required: true,
              whitespace: true,
              message: "请输入供应商的模型 ID",
            },
          ]}
        >
          <Input
            placeholder="填写模型服务实际提供的模型 ID"
            maxLength={200}
            disabled={busy}
          />
        </Form.Item>
        <Form.Item
          label="API Base URL"
          name="base_url"
          rules={[{ required: true, message: "请输入完整 API 地址" }]}
          extra="填写服务商提供的 OpenAI 兼容 API 地址，通常以 /v1 结尾。"
        >
          <Input
            disabled={busy}
            maxLength={2048}
            placeholder="https://你的模型服务地址/v1"
          />
        </Form.Item>
        <Form.Item
          label="API Key"
          name="api_key"
          rules={[
            {
              required: true,
              whitespace: true,
              message: "请输入服务端 API Key",
            },
          ]}
        >
          <Input.Password
            autoComplete="new-password"
            placeholder="仅提交本次，不回显"
            disabled={busy}
            maxLength={16384}
          />
        </Form.Item>
        <ModelEndpointHint policy={policy} origins={origins} />
        <Alert
          type="info"
          showIcon
          title="保存后，在 Agent 里选择这个模型并试聊。"
        />
        {error && <Failure error={error} />}
        <Button
          type="primary"
          htmlType="submit"
          block
          loading={busy}
          style={{ marginTop: 16 }}
        >
          保存模型连接
        </Button>
      </Form>
    </Modal>
  );
}

export function ModelConnections({
  tenant,
  principal,
}: {
  tenant: string;
  principal: Principal;
}) {
  const { message } = App.useApp();
  const [data, setData] = useState<ModelConnectionPage | null>(null);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState<{
    id: string;
    rotate: boolean;
  } | null>(null);
  useEffect(() => {
    let live = true;
    setError("");
    api<ModelConnectionPage>("model-connections/list", { tenant_id: tenant })
      .then((v) => {
        if (live) setData(v);
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
        eyebrow="MODELS"
        title="模型连接"
        subtitle="在这里添加模型，在 Agent 里选择使用。"
        action={
          <Space>
            <Button onClick={() => setRefresh((v) => v + 1)}>刷新</Button>
            <Button
              type="primary"
              disabled={principal.role !== "superadmin" || !data?.enabled}
              onClick={() => setOpen(true)}
              icon={<Icon name="plus" />}
            >
              新增模型连接
            </Button>
          </Space>
        }
      />
      {error && (
        <Failure error={error} retry={() => setRefresh((v) => v + 1)} />
      )}
      {!data ? (
        <Skeleton active />
      ) : (
        <>
          {!data.enabled && (
            <Alert
              type="info"
              showIcon
              title="此部署尚未启用网页模型存储"
              description="原有 .env 模型仍可使用。新体验 Compose 会自动生成加密密钥；已有部署需先备份、迁移到 schema 29，并向 Admin/Worker/Jobs 注入相同的 TRPC_AGENT_MODEL_MASTER_KEY。不要在网页中填写该加密密钥。"
            />
          )}
          {principal.role !== "superadmin" && (
            <p className="muted">
              你可以在 Agent
              中选择本租户已有连接；管理连接与密钥请联系平台管理员。
            </p>
          )}
          {data.enabled && (
            <ModelEndpointHint
              policy={data.endpoint_policy}
              origins={data.allowed_origins}
            />
          )}
          <Panel
            title="当前租户的连接"
            subtitle="修改模型或地址后，需要在 Agent 里切换。"
          >
            {data.items.length ? (
              <Table
                rowKey="connection_id"
                dataSource={data.items}
                pagination={false}
                scroll={{ x: 1100 }}
                columns={[
                  { title: "名称", dataIndex: "name" },
                  {
                    title: "配置版本",
                    render: (_, v) => (
                      <Space direction="vertical" size={0}>
                        <Tag>v{v.config_version}</Tag>
                        {v.superseded_by && <Tag color="default">历史配置</Tag>}
                      </Space>
                    ),
                  },
                  { title: "模型 ID", dataIndex: "model_name" },
                  { title: "API 地址", dataIndex: "base_url" },
                  {
                    title: "凭据",
                    render: (_, v) => (
                      <Tag>已加密 · v{v.credential_version}</Tag>
                    ),
                  },
                  { title: "更新时间", render: (_, v) => date(v.updated_at) },
                  {
                    title: "操作",
                    fixed: "right",
                    render: (_, v) => (
                      <Space>
                        <Button
                          type="link"
                          disabled={
                            principal.role !== "superadmin" || !data.enabled
                          }
                          onClick={() =>
                            setEditing({ id: v.connection_id, rotate: false })
                          }
                        >
                          编辑
                        </Button>
                        <Button
                          type="link"
                          disabled={
                            principal.role !== "superadmin" || !data.enabled
                          }
                          onClick={() =>
                            setEditing({ id: v.connection_id, rotate: true })
                          }
                        >
                          更新 Key
                        </Button>
                      </Space>
                    ),
                  },
                ]}
              />
            ) : (
              <Blank title="还没有模型连接">
                可新增真实模型连接，也可先使用执行节点默认模型体验流程。
              </Blank>
            )}
            {data.next && (
              <Button
                className="load-more"
                onClick={async () => {
                  try {
                    const p = await api<ModelConnectionPage>(
                      "model-connections/list",
                      { tenant_id: tenant, after: data.next },
                    );
                    setData((old) =>
                      old ? { ...p, items: [...old.items, ...p.items] } : p,
                    );
                  } catch (e) {
                    setError(errorText(e));
                  }
                }}
              >
                加载更多
              </Button>
            )}
          </Panel>
          <Space>
            <Button type="primary" onClick={() => navigate("agents")}>
              下一步：创建或配置 Agent
            </Button>
            <Button onClick={() => navigate("start")}>查看上手引导</Button>
          </Space>
        </>
      )}
      {open && data && (
        <CreateModelConnection
          tenant={tenant}
          origins={data.allowed_origins}
          policy={data.endpoint_policy}
          onCancel={() => setOpen(false)}
          onCreated={() => {
            setOpen(false);
            setRefresh((v) => v + 1);
            message.success("模型连接已保存，请到 Agent 调试验证");
          }}
        />
      )}
      {editing && (
        <EditModelConnection
          key={editing.id + String(editing.rotate)}
          tenant={tenant}
          id={editing.id}
          rotate={editing.rotate}
          origins={data?.allowed_origins || []}
          policy={data?.endpoint_policy || "allowlist"}
          onCancel={() => setEditing(null)}
          onSaved={(result) => {
            setEditing(null);
            setRefresh((v) => v + 1);
            message.success(
              result.new_config_version
                ? `配置 v${result.connection.config_version} 已创建，请到 Agent 草稿中切换、调试并发布`
                : result.key_changed
                  ? "密钥已更新，后续调用读取新 Key；在途请求不受此操作撤回"
                  : "连接名称已保存",
            );
          }}
        />
      )}
    </>
  );
}

export function ConnectionSelect({
  data,
  value,
  onChange,
  disabled,
  onMore,
}: {
  data: ModelConnectionPage;
  value?: string;
  onChange: (value: string) => void;
  disabled: boolean;
  onMore: () => void;
}) {
  return (
    <>
      <Select
        disabled={disabled}
        showSearch
        optionFilterProp="label"
        value={value || undefined}
        placeholder="选择当前租户的模型连接"
        options={data.items.map((c) => ({
          value: c.connection_id,
          label:
            c.name +
            " · v" +
            c.config_version +
            " · " +
            c.model_name +
            (c.superseded_by ? "（历史配置）" : ""),
        }))}
        onChange={onChange}
      />
      {data.next && (
        <Button type="link" onClick={onMore}>
          加载更多连接
        </Button>
      )}
    </>
  );
}
