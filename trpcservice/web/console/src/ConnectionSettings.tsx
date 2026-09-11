import { useState } from "react";
import {
  Alert,
  App,
  Button,
  Checkbox,
  Divider,
  Input,
  Modal,
  Select,
  Space,
  Typography,
} from "antd";
import { api, errorText } from "./api";
import { Failure } from "./components";
import type { Connection } from "./Connections";
import type { AgentApp, Dict } from "./types";

export function ConnectionSettings({
  tenant,
  initial,
  apps,
  onClose,
}: {
  tenant: string;
  initial: Connection;
  apps: AgentApp[];
  onClose: () => void;
}) {
  const { message, modal } = App.useApp();
  const [c, setC] = useState(initial);
  const [credential, setCredential] = useState("");
  const [app, setApp] = useState<string>();
  const [localOnly, setLocalOnly] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const tg = c.channel_type === "telegram";
  const removing = c.operation === "remove";
  const editable =
    !removing &&
    ["paused", "draft", "needs_confirmation", "error"].includes(c.status);

  async function perform(action: string, extra: Dict = {}, success = "已保存") {
    setBusy(true);
    setError("");
    try {
      const result = await api<Connection>("connections/" + action, {
        tenant_id: tenant,
        connection_id: c.connection_id,
        expected_version: c.version,
        ...extra,
      });
      setC(result);
      if (result.status === "removed") {
        if (result.message) message.warning(result.message);
        else message.success("连接已移除，历史记录保留");
        onClose();
      } else message.success(success);
      return result;
    } catch (e) {
      setError(errorText(e));
      // A timed-out operation may still have committed. Refresh the server's
      // version/operation before offering another explicit action.
      try {
        setC(
          await api<Connection>("connections/get", {
            tenant_id: tenant,
            connection_id: c.connection_id,
          }),
        );
      } catch {
        /* keep the original error */
      }
    } finally {
      setBusy(false);
    }
  }
  async function remove() {
    const yes = await modal.confirm({
      title: removing ? "重试移除连接？" : "移除这个连接？",
      content:
        tg && !localOnly
          ? "如果 Telegram 的回调仍指向本服务，将取消这个回调并保留待处理消息。不会删除机器人、会话或审计记录。"
          : "停止本服务接收和处理这个机器人的新消息，保留会话和审计记录。不会删除机器人，也不会修改上游回调。",
      okText: "确认移除",
      cancelText: "取消",
      okButtonProps: { danger: true },
    });
    if (yes) await perform("remove", { consent: true, local_only: localOnly });
  }

  return (
    <Modal
      title={`连接设置 · ${c.name}`}
      open
      footer={null}
      width={600}
      onCancel={onClose}
      closable={!busy}
      maskClosable={false}
    >
      {error && <Failure error={error} />}
      {removing ? (
        <>
          <Alert
            type="warning"
            title="上次移除结果尚未确认"
            description="先检查结果。只有明确重试时，才会再次请求 Telegram 移除本服务的回调。"
          />
          <Button
            style={{ marginTop: 12 }}
            loading={busy}
            onClick={() => perform("check", {}, "已检查")}
          >
            检查移除结果
          </Button>
        </>
      ) : (
        <>
          {!editable && (
            <Alert
              type="info"
              title="先暂停连接，再修改设置"
              description="暂停不会撤回已发送消息，也不会取消已经执行的工具。未完成请求和审批可在消息记录中查看。"
            />
          )}
          {c.binding_id && c.status !== "paused" && (
            <Button
              style={{ marginTop: 12 }}
              loading={busy}
              onClick={() => perform("pause", {}, "已暂停，可以修改设置")}
            >
              暂停连接
            </Button>
          )}
          <Typography.Title level={5}>
            {tg ? "更新 Bot Token" : "更新 MCP 地址"}
          </Typography.Title>
          <p className="muted">
            {tg
              ? "从 BotFather 复制同一个机器人的新 Token。保存后点击连接恢复服务。"
              : "粘贴完整地址或 JSON Config。地址可能属于另一个机器人，保存后需要重新选择群、确认成员权限。旧会话保留。"}
          </p>
          {tg ? (
            <Input.Password
              aria-label="新的 Bot Token"
              autoComplete="new-password"
              value={credential}
              onChange={(e) => setCredential(e.target.value)}
              disabled={busy || !editable}
              placeholder="数字:字符，完整复制"
            />
          ) : (
            <Input.TextArea
              aria-label="新的 MCP 地址"
              autoComplete="off"
              value={credential}
              onChange={(e) => setCredential(e.target.value)}
              disabled={busy || !editable}
              rows={3}
              placeholder="StreamableHttp URL 或 JSON Config"
            />
          )}
          <Button
            style={{ marginTop: 12 }}
            disabled={!editable || !credential.trim()}
            loading={busy}
            onClick={async () => {
              const result = await perform(
                "update-credential",
                tg ? { token: credential } : { url: credential },
                tg
                  ? "Token 已更新，请重新连接"
                  : "地址已更新，请重新完成群授权",
              );
              if (result) {
                setCredential("");
                onClose();
              }
            }}
          >
            保存凭据
          </Button>
          <Divider />
          <Typography.Title level={5}>更换回复消息的 Agent</Typography.Title>
          <p className="muted">
            换绑后使用新会话，不继承原 Agent
            的聊天历史。原记录保留；保存后需要重新连接。
          </p>
          <Space.Compact style={{ width: "100%" }}>
            <Select
              aria-label="新的 Agent"
              style={{ flex: 1 }}
              placeholder="选择已发布的 Agent"
              value={app}
              disabled={busy || !editable}
              onChange={setApp}
              options={apps
                .filter(
                  (a) =>
                    a.app_id !== c.app_id &&
                    a.status === "active" &&
                    a.stable_revision_id,
                )
                .map((a) => ({ value: a.app_id, label: a.name }))}
            />
            <Button
              disabled={!editable || !app}
              loading={busy}
              onClick={async () => {
                const yes = await modal.confirm({
                  title: "更换 Agent？",
                  content:
                    "将停用原接收绑定，使用新 Agent 和独立会话。不会搬移旧消息或待处理审批。",
                  okText: "确认更换",
                  cancelText: "取消",
                });
                if (
                  yes &&
                  (await perform(
                    "rebind",
                    { app_id: app },
                    "Agent 已更换，请重新连接",
                  ))
                )
                  onClose();
              }}
            >
              保存
            </Button>
          </Space.Compact>
        </>
      )}
      <Divider />
      <Typography.Title level={5}>移除连接</Typography.Title>
      <p className="muted">
        会话、发送记录和审计保留。需要再次使用时，可重新添加这个机器人。
      </p>
      {tg && (
        <p>
          <Checkbox
            checked={localOnly}
            disabled={busy}
            onChange={(e) => setLocalOnly(e.target.checked)}
          >
            仅移除本地连接，不修改 Telegram 回调
          </Checkbox>
          <br />
          <span className="muted">
            Token 已失效时可选此项；原回调需要由你在新的接收服务中处理。
          </span>
        </p>
      )}
      <Button
        danger
        loading={busy}
        disabled={!editable && !removing}
        onClick={remove}
      >
        {removing ? "重试移除" : "移除连接"}
      </Button>
    </Modal>
  );
}
