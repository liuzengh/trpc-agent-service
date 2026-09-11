import { useEffect, useState } from "react";
import {
  Alert,
  App,
  Button,
  Checkbox,
  Drawer,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Steps,
  Table,
  Tag,
  Typography,
} from "antd";
import { api, errorText } from "./api";
import { Blank, Failure, PageHeading } from "./components";
import { ChannelDiagnostics } from "./ChannelDiagnostics";
import { ConnectionSettings } from "./ConnectionSettings";
import { navigate } from "./App";
import type { AgentApp, Dict, Page, Principal } from "./types";

export interface Connection {
  tenant_id: string;
  connection_id: string;
  app_id: string;
  channel_type: string;
  name: string;
  binding_id: string;
  status: string;
  message: string;
  version: number;
  callback_url?: string;
  last_received_at?: string;
  operation?: string;
}
interface ConnectionPage {
  items: Connection[];
  enabled: boolean;
  public_url: string;
  next?: string;
}
interface Group {
  id: string;
  name: string;
  selected: boolean;
  members?: { id: string; name: string }[];
}
interface Enrollment {
  group_id: string;
  marker: string;
  member?: { id: string; name: string };
  pending: boolean;
}
const states: Record<string, string> = {
  draft: "待设置",
  needs_confirmation: "待确认",
  connecting: "连接中",
  unknown: "需要检查",
  connected: "已连接",
  paused: "已暂停",
  error: "连接异常",
};
const types: Record<string, string> = {
  telegram: "Telegram",
  wecom_mcp: "企业微信",
  wecom: "企业微信应用",
  http: "HTTP",
};

export function Connections({
  tenant,
  principal,
}: {
  tenant: string;
  principal: Principal;
}) {
  const { message, modal } = App.useApp();
  const [data, setData] = useState<ConnectionPage>({
    items: [],
    enabled: false,
    public_url: "",
  });
  const [legacy, setLegacy] = useState<Dict[]>([]),
    [apps, setApps] = useState<AgentApp[]>([]);
  const [error, setError] = useState(""),
    [loading, setLoading] = useState(true),
    [refresh, setRefresh] = useState(0);
  const [adding, setAdding] = useState(false),
    [setting, setSetting] = useState(false),
    [address, setAddress] = useState("");
  const [editing, setEditing] = useState<Connection | null>(null),
    [diagnostic, setDiagnostic] = useState("");
  const [managing, setManaging] = useState<Connection | null>(null);
  const [busy, setBusy] = useState(false);
  const admin = principal.role === "superadmin";
  useEffect(() => {
    setAdding(false);
    setEditing(null);
    setManaging(null);
    setDiagnostic("");
    setLegacy([]);
    setData({ items: [], enabled: false, public_url: "" });
  }, [tenant]);
  const writable = ["superadmin", "tenant_admin"].includes(principal.role);
  function reload() {
    setRefresh((v) => v + 1);
  }
  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");
    Promise.all([
      api<ConnectionPage>("connections/list", { tenant_id: tenant }),
      api<Page<Dict>>("catalog/list", {
        kind: "channels",
        tenant_id: tenant,
        limit: 100,
      }),
      api<Page<AgentApp>>("catalog/list", {
        kind: "apps",
        tenant_id: tenant,
        limit: 100,
      }),
    ])
      .then(([c, b, a]) => {
        if (alive) {
          setData(c);
          setLegacy(
            b.items.filter(
              (x) => !String(x.secret_ref).startsWith("managed://"),
            ),
          );
          setApps(a.items);
        }
      })
      .catch((e) => alive && setError(errorText(e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [tenant, refresh]);
  async function act(c: Connection, action: string, extra: Dict = {}) {
    setBusy(true);
    try {
      const next = await api<Connection>("connections/" + action, {
        tenant_id: tenant,
        connection_id: c.connection_id,
        expected_version: c.version,
        ...extra,
      });
      reload();
      return next;
    } catch (e) {
      message.error(errorText(e));
      reload();
      return null;
    } finally {
      setBusy(false);
    }
  }
  async function connect(c: Connection) {
    if (
      c.channel_type === "wecom_mcp" &&
      (c.status === "draft" || c.status === "needs_confirmation")
    ) {
      setEditing(c);
      return;
    }
    if (c.status === "unknown" || c.status === "connecting") {
      await act(c, "check");
      return;
    }
    let confirm = false;
    if (c.status === "needs_confirmation")
      confirm = await modal.confirm({
        title: "切换到这里？",
        content:
          "这个机器人已连接其他服务。切换后，原服务将不再收到它的新消息。未处理的消息会保留。",
        okText: "切换连接",
        cancelText: "取消",
      });
    if (c.status === "needs_confirmation" && !confirm) return;
    const next = await act(c, "activate", { confirm_replace: confirm });
    if (next?.status === "needs_confirmation")
      message.info("连接情况有变化，请确认后再连接");
    else if (next?.status === "connected") message.success("机器人已连接");
  }
  const rows = [
    ...data.items.map((c) => ({ ...c, key: c.connection_id, managed: true })),
    ...legacy.map((b) => ({
      tenant_id: tenant,
      message: "",
      key: b.channel_binding_id,
      connection_id: "",
      name: b.config?.bot_username
        ? "@" + b.config.bot_username
        : types[b.channel_type] || b.account_id,
      app_id: b.app_id,
      channel_type: b.channel_type,
      binding_id: b.channel_binding_id,
      status: b.status === "active" ? "connected" : "paused",
      version: b.version,
      managed: false,
    })),
  ];
  return (
    <>
      <PageHeading
        eyebrow=""
        title="机器人"
        subtitle="把 Agent 接到 Telegram 或企业微信。"
        action={
          <Space>
            <Button onClick={reload}>刷新</Button>
            {admin && (
              <Button
                onClick={() => {
                  setAddress(data.public_url);
                  setSetting(true);
                }}
              >
                服务地址
              </Button>
            )}
            <Button
              type="primary"
              disabled={!admin || !data.enabled}
              onClick={() => setAdding(true)}
            >
              连接机器人
            </Button>
          </Space>
        }
      />
      {error && <Failure error={error} />}{" "}
      {!loading && !data.enabled && (
        <Alert
          type="warning"
          title="管理员尚未启用网页连接功能"
          description="已有机器人仍可使用。请管理员完成连接存储的安装设置。"
        />
      )}
      <Table
        loading={loading}
        rowKey="key"
        dataSource={rows}
        pagination={false}
        locale={{
          emptyText: (
            <Blank title="还没有机器人">
              先发布一个 Agent，再点击“连接机器人”。
            </Blank>
          ),
        }}
        columns={[
          {
            title: "机器人",
            render: (_, r) => (
              <>
                <strong>{r.name}</strong>
                <div className="muted">
                  {types[r.channel_type]}
                  {!r.managed && " · 服务器配置"}
                </div>
              </>
            ),
          },
          {
            title: "回复消息的 Agent",
            render: (_, r) =>
              apps.find((a) => a.app_id === r.app_id)?.name || r.app_id,
          },
          {
            title: "状态",
            render: (_, r) => (
              <Tag
                color={
                  r.status === "connected"
                    ? "green"
                    : r.status === "paused"
                      ? "default"
                      : "orange"
                }
              >
                {states[r.status] || r.status}
              </Tag>
            ),
          },
          {
            title: "",
            render: (_, r) => (
              <Space wrap>
                {r.managed && admin && (
                  <>
                    <Button
                      type="link"
                      disabled={busy}
                      onClick={() => setManaging(r as Connection)}
                    >
                      设置
                    </Button>
                    {!("operation" in r && r.operation === "remove") && (
                      <>
                        {r.status === "unknown" &&
                          r.channel_type === "telegram" && (
                            <Button
                              type="link"
                              disabled={busy}
                              onClick={async () => {
                                const yes = await modal.confirm({
                                  title: "重新连接？",
                                  content:
                                    "将再次设置本服务的回调地址，保留未处理消息。如果机器人现在连接着其他服务，该连接会被替换。",
                                  okText: "重新连接",
                                  cancelText: "取消",
                                });
                                if (yes)
                                  await act(r as Connection, "retry", {
                                    consent: true,
                                  });
                              }}
                            >
                              重试连接
                            </Button>
                          )}
                        {r.status === "connected" ? (
                          <Button
                            type="link"
                            disabled={busy}
                            onClick={() => act(r as Connection, "pause")}
                          >
                            暂停
                          </Button>
                        ) : (
                          <Button
                            type="link"
                            disabled={busy}
                            onClick={() => connect(r as Connection)}
                          >
                            {["unknown", "connecting"].includes(r.status)
                              ? "检查连接"
                              : r.channel_type === "wecom_mcp" &&
                                  r.status === "draft"
                                ? "继续设置"
                                : "连接"}
                          </Button>
                        )}
                        {r.status === "connected" && (
                          <Button
                            type="link"
                            onClick={() => setEditing(r as Connection)}
                          >
                            群和成员
                          </Button>
                        )}
                        {r.channel_type === "telegram" &&
                          r.status === "connected" && (
                            <Button
                              type="link"
                              disabled={busy}
                              onClick={async () => {
                                const c = await act(r as Connection, "check");
                                if (c?.status === "connected")
                                  message.success("回调设置正常");
                              }}
                            >
                              检查
                            </Button>
                          )}
                      </>
                    )}
                  </>
                )}
                {!r.managed && writable && (
                  <Button
                    type="link"
                    disabled={busy}
                    onClick={async () => {
                      setBusy(true);
                      try {
                        await api("connections/legacy-toggle", {
                          tenant_id: tenant,
                          binding_id: r.binding_id,
                          expected_version: r.version,
                          enabled: r.status !== "connected",
                        });
                        reload();
                      } catch (e) {
                        message.error(errorText(e));
                      } finally {
                        setBusy(false);
                      }
                    }}
                  >
                    {r.status === "connected" ? "暂停" : "启用"}
                  </Button>
                )}
                {r.binding_id && (
                  <Button
                    type="link"
                    onClick={() => setDiagnostic(r.binding_id)}
                  >
                    消息记录
                  </Button>
                )}
              </Space>
            ),
          },
        ]}
      />
      {managing && (
        <ConnectionSettings
          key={managing.connection_id}
          tenant={tenant}
          initial={managing}
          apps={apps}
          onClose={() => {
            setManaging(null);
            reload();
          }}
        />
      )}
      {data.next && (
        <Button
          onClick={async () => {
            try {
              const p = await api<ConnectionPage>("connections/list", {
                tenant_id: tenant,
                after: data.next,
              });
              setData((v) => ({ ...p, items: [...v.items, ...p.items] }));
            } catch (e) {
              setError(errorText(e));
            }
          }}
        >
          加载更多
        </Button>
      )}
      {adding && (
        <AddConnection
          tenant={tenant}
          apps={apps}
          hasAddress={!!data.public_url}
          onAddress={() => {
            setAddress(data.public_url);
            setSetting(true);
          }}
          onClose={() => setAdding(false)}
          onCreated={async (c) => {
            setAdding(false);
            reload();
            if (c.channel_type === "wecom_mcp") setEditing(c);
            else await connect(c);
          }}
        />
      )}
      {editing && (
        <GroupSettings
          key={editing.connection_id}
          tenant={tenant}
          initial={editing}
          onClose={() => {
            setEditing(null);
            reload();
          }}
        />
      )}
      <Drawer
        title="消息记录"
        open={!!diagnostic}
        onClose={() => setDiagnostic("")}
        size="large"
      >
        {diagnostic && (
          <ChannelDiagnostics tenant={tenant} bindingID={diagnostic} />
        )}
      </Drawer>
      <Modal
        title="服务器的公网地址"
        open={setting}
        confirmLoading={busy}
        onCancel={() => setSetting(false)}
        okText="保存"
        onOk={async () => {
          setBusy(true);
          try {
            await api("connections/public-address", {
              tenant_id: tenant,
              url: address,
            });
            setSetting(false);
            reload();
            message.success("地址已保存");
          } catch (e) {
            message.error(errorText(e));
          } finally {
            setBusy(false);
          }
        }}
      >
        <p>
          Telegram 会把消息发送到这个地址。只需设置一次；已有机器人保持原地址。
        </p>
        <Input
          value={address}
          onChange={(e) => setAddress(e.target.value)}
          placeholder="https://bot.example.com"
        />
        <p className="muted">
          使用反向代理或 Tunnel 时，放行 /callbacks/telegram/
          下的回调路径。不要公开管理页面。
        </p>
      </Modal>
    </>
  );
}

function AddConnection({
  tenant,
  apps,
  hasAddress,
  onAddress,
  onClose,
  onCreated,
}: {
  tenant: string;
  apps: AgentApp[];
  hasAddress: boolean;
  onAddress: () => void;
  onClose: () => void;
  onCreated: (c: Connection) => void;
}) {
  const [form] = Form.useForm();
  const [id] = useState(() => "connection-" + crypto.randomUUID());
  const [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const kind = Form.useWatch("channel_type", form) || "telegram";
  async function submit(values: Dict) {
    setBusy(true);
    setError("");
    try {
      const c = await api<Connection>("connections/prepare", {
        ...values,
        tenant_id: tenant,
        connection_id: id,
      });
      form.resetFields(["token", "url"]);
      onCreated(c);
    } catch (e) {
      form.resetFields(["token", "url"]);
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title="连接机器人"
      open
      footer={null}
      maskClosable={false}
      closable={!busy}
      onCancel={onClose}
      width={520}
      destroyOnHidden
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{ channel_type: "telegram" }}
        disabled={busy}
        onFinish={submit}
        onValuesChange={(changed) => {
          if (changed.channel_type) form.resetFields(["token", "url"]);
        }}
      >
        <Form.Item name="channel_type" label="平台">
          <Select
            options={[
              { value: "telegram", label: "Telegram" },
              { value: "wecom_mcp", label: "企业微信（消息 MCP）" },
            ]}
          />
        </Form.Item>
        <Form.Item
          name="app_id"
          label="回复消息的 Agent"
          rules={[{ required: true, message: "请选择 Agent" }]}
        >
          <Select
            placeholder="选择已发布的 Agent"
            options={apps.map((a) => ({
              value: a.app_id,
              label: a.name + (a.stable_revision_id ? "" : "（未发布）"),
              disabled: !a.stable_revision_id,
            }))}
          />
        </Form.Item>
        {!apps.some((a) => a.stable_revision_id) && (
          <p>
            还没有已发布的 Agent。
            <Button
              type="link"
              onClick={() => {
                onClose();
                navigate("agents");
              }}
            >
              去创建
            </Button>
          </p>
        )}
        {kind === "telegram" ? (
          <Form.Item
            name="token"
            label="Bot Token"
            rules={[{ required: true, message: "请填写 Bot Token" }]}
            extra={
              <a href="https://t.me/BotFather" target="_blank" rel="noreferrer">
                从 BotFather 获取
              </a>
            }
          >
            <Input.Password
              autoComplete="new-password"
              placeholder="粘贴 BotFather 给你的 Token"
              maxLength={512}
            />
          </Form.Item>
        ) : (
          <Form.Item
            name="url"
            label="连接地址"
            rules={[{ required: true, message: "请填写连接地址" }]}
            extra="复制企业微信“消息”权限页的 StreamableHttp URL 或 JSON Config。"
          >
            <Input.Password
              autoComplete="new-password"
              placeholder="粘贴完整连接地址"
              maxLength={16384}
            />
          </Form.Item>
        )}
        {kind === "telegram" && !hasAddress && (
          <Alert
            type="warning"
            title="还没有设置公网地址"
            action={<Button onClick={onAddress}>去设置</Button>}
          />
        )}
        <p className="muted">密钥加密保存，不会显示在页面上。</p>
        {error && <Failure error={error} />}
        <Button
          type="primary"
          block
          htmlType="submit"
          loading={busy}
          disabled={kind === "telegram" && !hasAddress}
        >
          {kind === "telegram" ? "连接" : "验证连接"}
        </Button>
      </Form>
    </Modal>
  );
}

function GroupSettings({
  tenant,
  initial,
  onClose,
}: {
  tenant: string;
  initial: Connection;
  onClose: () => void;
}) {
  const { message, modal } = App.useApp();
  const [c, setC] = useState(initial),
    [groups, setGroups] = useState<Group[]>([]),
    [selected, setSelected] = useState<string[]>([]);
  const [enroll, setEnroll] = useState<Enrollment | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const [groupsLoaded, setGroupsLoaded] = useState(false);
  const tg = c.channel_type === "telegram";
  async function call(action: string, body: Dict = {}) {
    return api<any>("connections/" + action, {
      tenant_id: tenant,
      connection_id: c.connection_id,
      expected_version: c.version,
      ...body,
    });
  }
  async function load(consent = false) {
    setBusy(true);
    setError("");
    setGroupsLoaded(false);
    try {
      const p = await call("groups", { consent });
      setGroups(p.items);
      setGroupsLoaded(true);
      setSelected(
        p.items.filter((g: Group) => g.selected).map((g: Group) => g.id),
      );
      if (p.enrollment?.pending) setEnroll(p.enrollment);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  useEffect(() => {
    // This reads only previously authorized metadata from our database. The
    // provider's group list is still fetched solely after a consent click.
    void load();
  }, []);
  async function run(action: string, body: Dict, done: (result: any) => void) {
    setBusy(true);
    setError("");
    try {
      done(await call(action, body));
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  const step = !groups.length
    ? 0
    : !enroll?.marker
      ? 1
      : !enroll?.member?.id
        ? 2
        : 3;
  return (
    <Modal
      title={tg ? "群聊设置" : "连接企业微信群"}
      open
      width={600}
      footer={null}
      maskClosable={false}
      closable={!busy}
      onCancel={onClose}
      destroyOnHidden
    >
      {error && <Failure error={error} />}
      {tg ? (
        <>
          <p>私聊可直接使用。群里只响应 @、回复和发给它的命令。</p>
          {groups.length ? (
            <Checkbox.Group
              value={selected}
              onChange={(v) => setSelected(v as string[])}
              options={groups.map((g) => ({ label: g.name, value: g.id }))}
            />
          ) : (
            <p className="muted">
              先把机器人加入群，再在群里发一条消息，然后刷新。这里不读取群聊历史。
            </p>
          )}
          <div style={{ marginTop: 24 }}>
            <Space>
              <Button loading={busy} onClick={() => load()}>
                刷新群列表
              </Button>
              <Button
                type="primary"
                loading={busy}
                onClick={() =>
                  run("save-groups", { group_ids: selected }, () => {
                    message.success("群聊设置已保存");
                    onClose();
                  })
                }
              >
                保存
              </Button>
            </Space>
          </div>
          <p className="muted">不选任何群时，机器人只回复私聊。</p>
        </>
      ) : (
        <>
          <Steps
            size="small"
            current={step}
            items={[
              { title: "读取群列表" },
              { title: "选择群" },
              { title: "发送确认消息" },
              { title: "完成" },
            ]}
          />
          {groups.some((g) => g.members?.length) && (
            <>
              <Typography.Title level={5}>已授权的群和成员</Typography.Title>
              {groups
                .filter((g) => g.members?.length)
                .map((g) => (
                  <div key={g.id} style={{ marginBottom: 12 }}>
                    <strong>{g.name}</strong>
                    <Button
                      type="link"
                      danger
                      disabled={busy}
                      onClick={async () => {
                        const yes = await modal.confirm({
                          title: "移除这个群的授权？",
                          content:
                            "停止接收这个群的新消息，历史记录保留。再次接入需要重新确认成员。",
                          okText: "移除授权",
                          cancelText: "取消",
                        });
                        if (yes)
                          await run(
                            "revoke-member",
                            { group_id: g.id, consent: true },
                            (r) => {
                              setC(r);
                              setEnroll(null);
                              setGroups(
                                groups.map((x) =>
                                  x.id === g.id
                                    ? { ...x, members: [], selected: false }
                                    : x,
                                ),
                              );
                            },
                          );
                      }}
                    >
                      移除群授权
                    </Button>
                    {g.members!.map((m) => (
                      <div key={m.id}>
                        <span>{m.name || "已授权成员"}</span>
                        <Button
                          type="link"
                          danger
                          disabled={busy}
                          onClick={async () => {
                            const yes = await modal.confirm({
                              title: `撤销 ${m.name || "这位成员"} 的权限？`,
                              content:
                                "只撤销此群的授权，不影响该成员在其他群的授权。已经执行的操作不会回滚。",
                              okText: "撤销权限",
                              cancelText: "取消",
                            });
                            if (yes)
                              await run(
                                "revoke-member",
                                {
                                  group_id: g.id,
                                  user_id: m.id,
                                  consent: true,
                                },
                                (r) => {
                                  setC(r);
                                  setEnroll(null);
                                  setGroups(
                                    groups.map((x) =>
                                      x.id === g.id
                                        ? {
                                            ...x,
                                            members: x.members?.filter(
                                              (y) => y.id !== m.id,
                                            ),
                                          }
                                        : x,
                                    ),
                                  );
                                },
                              );
                          }}
                        >
                          撤销权限
                        </Button>
                      </div>
                    ))}
                  </div>
                ))}
            </>
          )}
          {!groups.length ? (
            <>
              <p>
                先把这个机器人加入目标群，再在群里 <strong>@ 机器人</strong>
                发送一条消息（例如“连接测试”），然后点击“读取群列表”。
              </p>
              <p className="muted">
                输入 @ 后，请从成员列表中选择机器人。无需等待它回复。
                这里仅获取群列表，不读取聊天记录。
              </p>
              {groupsLoaded && !busy && !error && (
                <Alert
                  type="info"
                  showIcon
                  title="未找到可连接的群"
                  description="请确认群里加入的是当前连接的机器人，@ 它发送一条消息后再试。"
                  style={{ marginBottom: 16 }}
                />
              )}
              <Button type="primary" loading={busy} onClick={() => load(true)}>
                {groupsLoaded ? "重新读取群列表" : "读取群列表"}
              </Button>
            </>
          ) : (
            <>
              <p>
                选择要接入的群。已有成员继续可用；可让新成员发送确认消息来开通权限。
              </p>
              <Select
                style={{ width: "100%" }}
                disabled={busy}
                placeholder="选择群"
                value={enroll?.group_id}
                options={groups.map((g) => ({ label: g.name, value: g.id }))}
                onChange={(id) =>
                  run("select-group", { group_id: id }, (r) => {
                    setC(r.connection);
                    setEnroll(r.enrollment);
                  })
                }
              />
              <Button type="link" disabled={busy} onClick={() => load(true)}>
                刷新群列表
              </Button>
              <p className="muted">
                找不到目标群？把这个机器人加入群，@ 它发一条消息后刷新群列表。
              </p>
              {enroll?.marker && (
                <>
                  <p>
                    在这个群里 <strong>@ 机器人</strong>，发送下面的文字：
                  </p>
                  <Typography.Paragraph copyable={{ text: enroll.marker }}>
                    <code>{enroll.marker}</code>
                  </Typography.Paragraph>
                  {!enroll.member?.id ? (
                    <>
                      <p className="muted">
                        点击检查后，只读取所选群这几分钟的消息来确认发送者。
                      </p>
                      <Button
                        type="primary"
                        loading={busy}
                        onClick={() =>
                          run("check-message", { consent: true }, (r) => {
                            setC(r.connection);
                            setEnroll(r.enrollment);
                          })
                        }
                      >
                        我已发送，检查消息
                      </Button>
                    </>
                  ) : (
                    <>
                      <Alert
                        type="success"
                        title={`已识别：${enroll.member.name}`}
                        description="确认后，这位成员就能在所选群里使用 Agent。"
                      />
                      <Button
                        style={{ marginTop: 20 }}
                        type="primary"
                        loading={busy}
                        onClick={() =>
                          run("activate", {}, () => {
                            message.success("企业微信群已连接");
                            onClose();
                          })
                        }
                      >
                        确认连接
                      </Button>
                    </>
                  )}
                </>
              )}
            </>
          )}
        </>
      )}
    </Modal>
  );
}
