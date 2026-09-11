import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  App,
  Button,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Table,
  Tag,
} from "antd";
import { api, errorText } from "./api";
import { Blank, Failure, Panel } from "./components";
import {
  date,
  writable,
  type AgentApp,
  type Dict,
  type Page,
  type Principal,
} from "./types";

interface DocumentItem {
  tenant_id: string;
  app_id: string;
  document_id: string;
  name: string;
  revision_id: string;
  checksum: string;
  bytes: number;
  status: string;
  job_id?: string;
  chunks: number;
  version: number;
  updated_at: string;
}

interface DocumentPage extends Page<DocumentItem> {
  enabled: boolean;
}

const statusLabel: Record<string, string> = {
  pending: "等待入库",
  running: "正在入库",
  ready: "可检索",
  deleting: "正在删除",
  failed: "处理失败",
};

export function KnowledgeManager({
  tenant,
  principal,
}: {
  tenant: string;
  principal: Principal;
}) {
  const { message, modal } = App.useApp();
  const [documents, setDocuments] = useState<DocumentItem[]>([]);
  const [apps, setApps] = useState<AgentApp[]>([]);
  const [bindings, setBindings] = useState<Dict[]>([]);
  const [busy, setBusy] = useState(true);
  const [saving, setSaving] = useState(false);
  const [enabled, setEnabled] = useState(false);
  const [error, setError] = useState("");
  const [open, setOpen] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [next, setNext] = useState("");
  const [form] = Form.useForm();
  const eligibleApps = useMemo(
    () =>
      apps.filter(
        (app) =>
          !!app.stable_revision_id &&
          bindings.some(
            (binding) => !binding.app_id || binding.app_id === app.app_id,
          ),
      ),
    [apps, bindings],
  );

  useEffect(() => {
    let live = true;
    setBusy(true);
    setError("");
    Promise.all([
      api<DocumentPage>("knowledge/documents/list", { tenant_id: tenant }),
      api<Page<AgentApp>>("catalog/list", {
        tenant_id: tenant,
        kind: "apps",
        limit: 100,
      }),
      api<Page<Dict>>("resources/list", {
        tenant_id: tenant,
        kind: "knowledge",
      }),
    ])
      .then(([docs, appPage, backendPage]) => {
        if (!live) return;
        setDocuments(docs.items);
        setNext(docs.next || "");
        setEnabled(docs.enabled);
        setApps(appPage.items);
        setBindings(backendPage.items);
      })
      .catch((e) => live && setError(errorText(e)))
      .finally(() => live && setBusy(false));
    return () => {
      live = false;
    };
  }, [tenant, refresh]);

  useEffect(() => {
    if (
      !documents.some((item) =>
        ["pending", "running", "deleting"].includes(item.status),
      )
    )
      return;
    const timer = window.setTimeout(
      () => setRefresh((value) => value + 1),
      2000,
    );
    return () => window.clearTimeout(timer);
  }, [documents]);

  function startUpload() {
    form.resetFields();
    form.setFieldsValue({
      app_id: eligibleApps.length === 1 ? eligibleApps[0].app_id : undefined,
      document_id: "doc-" + crypto.randomUUID(),
    });
    setOpen(true);
  }

  async function readFile(file?: File) {
    if (!file) return;
    if (file.size > 512 * 1024) {
      message.error("单个文本文件不能超过 512 KiB");
      return;
    }
    let content = "";
    try {
      content = new TextDecoder("utf-8", { fatal: true }).decode(
        await file.arrayBuffer(),
      );
    } catch {
      message.error("文件不是有效的 UTF-8 文本");
      return;
    }
    if (content.includes("\0")) {
      message.error("文件包含无效的二进制内容");
      return;
    }
    form.setFieldsValue({
      name: form.getFieldValue("name") || file.name,
      content,
    });
  }

  async function upload(values: Dict) {
    const app = eligibleApps.find((item) => item.app_id === values.app_id);
    if (!app?.stable_revision_id) return;
    setSaving(true);
    try {
      const result = await api<{ queued: boolean; chunks?: number }>(
        "knowledge/documents",
        {
          tenant_id: tenant,
          app_id: app.app_id,
          revision_id: app.stable_revision_id,
          document_id: values.document_id,
          operation_id: "upload-" + crypto.randomUUID(),
          name: values.name,
          content: values.content,
          metadata: {},
        },
      );
      form.setFieldValue("content", "");
      setOpen(false);
      setRefresh((value) => value + 1);
      message.success(
        result.queued
          ? "资料已提交，正在后台入库"
          : `资料已入库，共 ${result.chunks || 0} 个分块`,
      );
    } catch (e) {
      message.error(errorText(e));
    } finally {
      setSaving(false);
    }
  }

  function remove(item: DocumentItem) {
    modal.confirm({
      title: "删除知识资料？",
      content: `将从 ${apps.find((app) => app.app_id === item.app_id)?.name || item.app_id} 的知识库删除“${item.name}”。`,
      okText: "删除",
      okButtonProps: { danger: true },
      async onOk() {
        await api("knowledge/documents/delete", {
          tenant_id: tenant,
          app_id: item.app_id,
          revision_id: item.revision_id,
          document_id: item.document_id,
          operation_id: "delete-" + crypto.randomUUID(),
        });
        setRefresh((value) => value + 1);
        message.success("删除任务已提交");
      },
    });
  }

  return (
    <>
      <Alert
        type="info"
        showIcon
        title="知识库已支持网页配置和文本资料管理"
        description="先在“数据后端”创建并绑定知识库存储，再到 Agent 工作台开启检索并保存 Embedding 配置。这里可上传 UTF-8 文本、Markdown、JSON 或 CSV；PDF、Word 等文件需先转换为文本。"
        style={{ marginBottom: 16 }}
      />
      {error && <Failure error={error} />}
      <Panel
        title="知识资料"
        subtitle="资料按工作空间和 Agent 隔离；后台完成分块和向量化后才可检索。"
        action={
          <Space>
            <Button onClick={() => setRefresh((value) => value + 1)}>
              刷新
            </Button>
            <Button
              type="primary"
              disabled={
                !writable(principal) || !enabled || eligibleApps.length === 0
              }
              onClick={startUpload}
            >
              上传资料
            </Button>
          </Space>
        }
      >
        {!bindings.length && (
          <Alert
            type="warning"
            showIcon
            title="尚未绑定知识库存储，请先到“数据后端”创建并绑定。"
          />
        )}
        {!!bindings.length && !eligibleApps.length && (
          <Alert
            type="warning"
            showIcon
            title="请先为 Agent 绑定知识库存储并发布启用知识库的版本。"
          />
        )}
        <Table
          rowKey={(item) => `${item.app_id}/${item.document_id}`}
          loading={busy}
          dataSource={documents}
          pagination={false}
          locale={{ emptyText: <Blank title="当前工作空间还没有知识资料" /> }}
          columns={[
            {
              title: "资料",
              render: (_: unknown, item: DocumentItem) => (
                <div className="table-name">
                  <strong>{item.name}</strong>
                  <small>{item.document_id}</small>
                </div>
              ),
            },
            {
              title: "Agent",
              render: (_: unknown, item: DocumentItem) =>
                apps.find((app) => app.app_id === item.app_id)?.name ||
                item.app_id,
            },
            {
              title: "大小",
              render: (_: unknown, item: DocumentItem) =>
                `${Math.max(1, Math.ceil(item.bytes / 1024))} KiB`,
            },
            {
              title: "状态",
              render: (_: unknown, item: DocumentItem) => (
                <Tag
                  color={
                    item.status === "ready"
                      ? "success"
                      : item.status === "failed"
                        ? "error"
                        : "processing"
                  }
                >
                  {statusLabel[item.status] || item.status}
                </Tag>
              ),
            },
            {
              title: "更新时间",
              render: (_: unknown, item: DocumentItem) => date(item.updated_at),
            },
            {
              title: "",
              render: (_: unknown, item: DocumentItem) => (
                <Button
                  danger
                  type="link"
                  disabled={!writable(principal) || item.status === "deleting"}
                  onClick={() => remove(item)}
                >
                  删除
                </Button>
              ),
            },
          ]}
        />
        {next && (
          <Button
            onClick={async () => {
              try {
                const page = await api<DocumentPage>(
                  "knowledge/documents/list",
                  {
                    tenant_id: tenant,
                    after: next,
                  },
                );
                setDocuments((old) => [...old, ...page.items]);
                setNext(page.next || "");
              } catch (e) {
                setError(errorText(e));
              }
            }}
          >
            加载更多资料
          </Button>
        )}
      </Panel>
      <Modal
        title="上传知识资料"
        open={open}
        footer={null}
        onCancel={() => setOpen(false)}
        destroyOnHidden
      >
        <Form form={form} layout="vertical" onFinish={upload}>
          <Form.Item
            name="app_id"
            label="Agent"
            rules={[{ required: true, message: "请选择 Agent" }]}
          >
            <Select
              options={eligibleApps.map((app) => ({
                value: app.app_id,
                label: app.name,
              }))}
            />
          </Form.Item>
          <Form.Item
            name="document_id"
            label="资料标识"
            rules={[
              {
                required: true,
                pattern: /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/,
                message: "使用字母、数字、点、下划线或连字符",
              },
            ]}
          >
            <Input maxLength={128} />
          </Form.Item>
          <Form.Item
            name="name"
            label="资料名称"
            rules={[
              { required: true, whitespace: true, message: "请填写资料名称" },
            ]}
          >
            <Input maxLength={255} />
          </Form.Item>
          <Form.Item
            label="选择文本文件"
            help="支持 UTF-8 文本、Markdown、JSON、CSV，最大 512 KiB。选择后仍可在下方检查和修改正文。"
          >
            <input
              type="file"
              accept=".txt,.md,.markdown,.json,.csv,text/plain,text/markdown,application/json,text/csv"
              onChange={(event) => void readFile(event.target.files?.[0])}
            />
          </Form.Item>
          <Form.Item
            name="content"
            label="正文"
            rules={[
              {
                required: true,
                whitespace: true,
                message: "请选择文件或填写正文",
              },
            ]}
          >
            <Input.TextArea rows={10} maxLength={512 * 1024} showCount />
          </Form.Item>
          <Button type="primary" htmlType="submit" loading={saving} block>
            提交入库
          </Button>
        </Form>
      </Modal>
    </>
  );
}
