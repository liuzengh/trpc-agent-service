import { useEffect, useState } from "react";
import { Alert, Button, Form, Input, Modal, Skeleton, Space, Tag } from "antd";
import { APIError, api, errorText } from "./api";
import { Failure } from "./components";
import { navigate } from "./App";
import type { ModelConnection } from "./ModelConnections";
import {
  ModelEndpointHint,
  type ModelEndpointPolicy,
} from "./ModelEndpointHint";

interface Usage {
  app_id: string;
  name: string;
  stable: boolean;
  canary: boolean;
  draft: boolean;
  historical_revisions: number;
}
interface Detail {
  connection: ModelConnection;
  usage: Usage[];
  next?: string;
}
interface Result {
  connection: ModelConnection;
  new_config_version: boolean;
  key_changed: boolean;
}

export function EditModelConnection({
  tenant,
  id,
  rotate,
  origins,
  policy,
  onCancel,
  onSaved,
}: {
  tenant: string;
  id: string;
  rotate: boolean;
  origins: string[];
  policy: ModelEndpointPolicy;
  onCancel: () => void;
  onSaved: (result: Result) => void;
}) {
  const [form] = Form.useForm();
  const [detail, setDetail] = useState<Detail | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [conflict, setConflict] = useState(false);
  const [reload, setReload] = useState(0);
  const [newID] = useState(() => "model-" + crypto.randomUUID());
  const model = Form.useWatch("model_name", form);
  const baseURL = Form.useWatch("base_url", form);
  useEffect(() => {
    let live = true;
    setError("");
    setDetail(null);
    setConflict(false);
    api<Detail>("model-connections/get", {
      tenant_id: tenant,
      connection_id: id,
    })
      .then((value) => {
        if (!live) return;
        setDetail(value);
        form.setFieldsValue({
          name: value.connection.name,
          model_name: value.connection.model_name,
          base_url: value.connection.base_url,
          api_key: "",
        });
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant, id, reload, form]);
  const current = detail?.connection;
  const addressChanged =
    !rotate &&
    !!current &&
    (baseURL || "").trim().replace(/\/+$/, "") !== current.base_url;
  const newConfig =
    !rotate &&
    !!current &&
    (addressChanged || (model || "").trim() !== current.model_name);
  async function save(values: Record<string, string>) {
    if (!current) return;
    setBusy(true);
    setError("");
    try {
      const result = await api<Result>(
        rotate ? "model-connections/rotate-key" : "model-connections/update",
        {
          tenant_id: tenant,
          connection_id: id,
          expected_version: current.version,
          ...(rotate
            ? { api_key: values.api_key }
            : { ...values, new_connection_id: newID }),
        },
      );
      form.resetFields();
      onSaved(result);
    } catch (e) {
      form.setFieldValue("api_key", "");
      setConflict(e instanceof APIError && e.status === 409);
      setError(
        e instanceof APIError && e.status === 409
          ? "连接已被其他操作修改，或已有新的配置版本。请重新加载再编辑，避免覆盖他人的修改。"
          : errorText(e),
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      open
      title={rotate ? "更新 API Key" : "编辑模型连接"}
      width={680}
      footer={null}
      maskClosable={false}
      closable={!busy}
      onCancel={() => {
        form.resetFields();
        onCancel();
      }}
      destroyOnHidden
    >
      {error && <Failure error={error} />}
      {conflict && (
        <Button onClick={() => setReload((v) => v + 1)}>
          重新加载当前版本
        </Button>
      )}
      {!detail ? (
        <>
          {!error ? (
            <Skeleton active />
          ) : (
            <Button onClick={() => setReload((v) => v + 1)}>重试加载</Button>
          )}
        </>
      ) : (
        <>
          <p>
            <strong>{current!.name}</strong>{" "}
            <Tag>配置 v{current!.config_version}</Tag>
            <Tag>密钥 v{current!.credential_version}</Tag>
          </p>
          <Form
            form={form}
            layout="vertical"
            onFinish={save}
            disabled={busy || conflict}
          >
            {!rotate && (
              <>
                <Form.Item
                  name="name"
                  label="连接名称"
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: "请输入连接名称",
                    },
                  ]}
                >
                  <Input maxLength={100} />
                </Form.Item>
                <Form.Item
                  name="model_name"
                  label="模型 ID"
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: "请输入模型 ID",
                    },
                  ]}
                >
                  <Input
                    maxLength={200}
                    disabled={busy || conflict || !!current!.superseded_by}
                  />
                </Form.Item>
                <Form.Item
                  name="base_url"
                  label="API Base URL"
                  extra={
                    <ModelEndpointHint policy={policy} origins={origins} />
                  }
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: "请输入 API 地址",
                    },
                  ]}
                >
                  <Input
                    maxLength={2048}
                    disabled={busy || conflict || !!current!.superseded_by}
                  />
                </Form.Item>
                {current!.superseded_by && (
                  <Alert
                    type="info"
                    title="这是历史配置"
                    description="仍可更名或更新这个版本的 Key；修改模型或地址请到连接列表编辑最新配置版本。"
                  />
                )}
              </>
            )}
            <Form.Item
              name="api_key"
              label={rotate ? "新的 API Key" : "API Key（可选更新）"}
              rules={[
                {
                  required: rotate || addressChanged,
                  whitespace: true,
                  message: addressChanged
                    ? "地址已改变，请重新填写目标服务的 API Key"
                    : "请输入新的 API Key",
                },
              ]}
              extra={
                rotate
                  ? "更新只影响引用当前配置版本的后续调用，其他配置版本的 Key 不会自动改变。"
                  : addressChanged
                    ? "不会把旧 Key 自动带到新地址，必须重新填写并确认目标服务。"
                    : "留空保留现有 Key；填写新值则更新。不会显示旧 Key。"
              }
            >
              <Input.Password
                autoComplete="new-password"
                maxLength={16384}
                placeholder="旧密钥不回显"
              />
            </Form.Item>
            <Alert
              showIcon
              type={newConfig ? "warning" : "info"}
              title={
                newConfig
                  ? `将生成配置 v${current!.config_version + 1}`
                  : rotate
                    ? "后续调用使用新密钥，无需重启 Worker"
                    : "名称修改直接保存；填写新 Key 时更新当前版本的凭据"
              }
              description={
                newConfig
                  ? "原配置与引用它的 Agent、调试快照不变。保存后，请到 Agent 草稿选择新配置，再新建调试并发布。"
                  : "已经发出的请求不会被撤回，可能仍使用旧 Key；此操作不会替你撤销供应商侧的密钥。"
              }
            />
            <div style={{ marginTop: 16 }}>
              <strong>引用此配置的 Agent</strong>
              <p className="muted">
                包含草稿和发布版本记录；旧会话、调试快照也可能继续使用此配置。下列清单不表示这些
                Agent 当前正在执行。
              </p>
              {detail.usage.length ? (
                detail.usage.map((item) => (
                  <div key={item.app_id} style={{ marginBottom: 8 }}>
                    <Space wrap>
                      <Button
                        type="link"
                        disabled={busy}
                        onClick={() => {
                          form.resetFields();
                          onCancel();
                          navigate("agents", item.app_id);
                        }}
                      >
                        {item.name}
                      </Button>
                      {item.stable && <Tag color="green">稳定版本</Tag>}
                      {item.canary && <Tag color="blue">灰度版本</Tag>}
                      {item.draft && <Tag>草稿</Tag>}
                      {item.historical_revisions > 0 && (
                        <Tag>{item.historical_revisions} 个发布版本引用</Tag>
                      )}
                    </Space>
                  </div>
                ))
              ) : (
                <p className="muted">当前没有草稿或发布版本引用记录。</p>
              )}
              {detail.next && (
                <Button
                  disabled={busy}
                  onClick={async () => {
                    try {
                      const page = await api<Detail>("model-connections/get", {
                        tenant_id: tenant,
                        connection_id: id,
                        after: detail.next,
                      });
                      // Keep the form's original optimistic version until explicitly reloaded.
                      setDetail((old) =>
                        old
                          ? {
                              ...old,
                              usage: [...old.usage, ...page.usage],
                              next: page.next,
                            }
                          : old,
                      );
                    } catch (e) {
                      setError(errorText(e));
                    }
                  }}
                >
                  加载更多引用
                </Button>
              )}
            </div>
            <Button
              htmlType="submit"
              type="primary"
              block
              loading={busy}
              disabled={conflict}
              style={{ marginTop: 16 }}
            >
              {newConfig
                ? "保存为新配置版本"
                : rotate
                  ? "确认更新 API Key"
                  : "保存修改"}
            </Button>
          </Form>
        </>
      )}
    </Modal>
  );
}
