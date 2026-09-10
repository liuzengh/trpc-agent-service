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

export interface ModelConnection {
  tenant_id: string;
  connection_id: string;
  name: string;
  model_name: string;
  base_url: string;
  created_at: string;
}
export interface ModelConnectionPage {
  items: ModelConnection[];
  next?: string;
  enabled: boolean;
  allowed_origins: string[];
}

export function CreateModelConnection({
  tenant,
  origins,
  onCreated,
  onCancel,
}: {
  tenant: string;
  origins: string[];
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
        连接只属于当前租户。API Key 在服务端加密保存，不会回显，也不会写入 Agent
        草稿或版本。
      </p>
      <Form
        form={form}
        layout="vertical"
        onFinish={save}
        initialValues={{
          base_url: (origins[0] || "https://api.openai.com") + "/v1",
        }}
      >
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
          extra="填写 OpenAI Chat Completions 兼容地址，通常以 /v1 结尾；不要填 /chat/completions 或在地址中放 Key。"
        >
          <Input disabled={busy} maxLength={2048} />
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
        <p className="muted">
          部署允许的服务：{origins.join("、") || "未配置"}。容器内的 127.0.0.1
          指容器自身；访问宿主机请使用部署者提供的可达地址。
        </p>
        <Alert
          type="info"
          showIcon
          title="保存不会调用模型"
          description="保存后在 Agent 中选择这个连接，通过右侧调试发送消息验证。调试沿用 Runner、权限、预算和运行记录，会消耗模型额度。"
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
        subtitle="由平台管理员为租户配置模型，Agent 选择连接即可使用，无需逐个填写密钥。"
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
              description="原有 .env 模型仍可使用。新体验 Compose 会自动生成加密密钥；已有部署需先备份、迁移到 schema 28，并向 Admin/Worker/Jobs 注入相同的 TRPC_AGENT_MODEL_MASTER_KEY。不要在网页中填写该加密密钥。"
            />
          )}
          {principal.role !== "superadmin" && (
            <p className="muted">
              你可以在 Agent 中选择本租户已有连接；新增凭据请联系平台管理员。
            </p>
          )}
          <Panel
            title="当前租户的连接"
            subtitle="连接内容固定。更换模型或 Key 时新增连接，在 Agent 草稿中切换并重新调试、发布，不会暗中改变已发布版本。"
          >
            {data.items.length ? (
              <Table
                rowKey="connection_id"
                dataSource={data.items}
                pagination={false}
                scroll={{ x: 720 }}
                columns={[
                  { title: "名称", dataIndex: "name" },
                  { title: "模型 ID", dataIndex: "model_name" },
                  { title: "API 地址", dataIndex: "base_url" },
                  { title: "凭据", render: () => <Tag>已加密 · 不回显</Tag> },
                  { title: "创建时间", render: (_, v) => date(v.created_at) },
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
          onCancel={() => setOpen(false)}
          onCreated={() => {
            setOpen(false);
            setRefresh((v) => v + 1);
            message.success("模型连接已保存，请到 Agent 调试验证");
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
          label: c.name + " · " + c.model_name,
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
