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
  Popconfirm,
  Space,
  Table,
  Tag,
} from "antd";
import { api, errorText } from "./api";
import { Failure, Panel } from "./components";
import { date, writable, type Principal, type Skill } from "./types";

interface ManagedSkill extends Skill {
  tenant_id: string;
  status: string;
  revision: number;
  created_by: string;
  reviewed_by: string;
  created_at: string;
  source?: string;
}
interface SkillPage {
  items: ManagedSkill[];
  deployment: Skill[];
  next?: string;
  enabled: boolean;
}
interface Inspection {
  skill: ManagedSkill;
  markdown: string;
  script: string;
}
const statusNames: Record<string, string> = {
  pending: "待审核",
  approved: "已授权",
  revoked: "已撤销",
  deployment: "部署者提供",
};

async function uploadFiles(files: File[]): Promise<Record<string, string>> {
  if (files.length === 1 && files[0].name.toLowerCase().endsWith(".zip")) {
    if (files[0].size > 256 * 1024) throw new Error("ZIP 文件最多 256 KiB");
    const bytes = new Uint8Array(await files[0].arrayBuffer());
    let raw = "";
    for (let i = 0; i < bytes.length; i += 8192)
      raw += String.fromCharCode(...bytes.subarray(i, i + 8192));
    return { archive: btoa(raw) };
  }
  const md = files.find((f) => f.name === "SKILL.md"),
    script = files.find((f) => f.name === "run.sh");
  if (
    (files.length !== 1 && files.length !== 2) ||
    !md ||
    (files.length === 2 && !script) ||
    md.size > 65536 ||
    (script && script.size > 65536)
  )
    throw new Error("请选择 SKILL.md，可同时选择 run.sh，每个文件最多 64 KiB");
  return {
    markdown: await md.text(),
    script: script ? await script.text() : "",
  };
}

export function SkillManager({
  tenant,
  principal,
}: {
  tenant: string;
  principal: Principal;
}) {
  const { message } = App.useApp();
  const [form] = Form.useForm();
  const [rows, setRows] = useState<ManagedSkill[]>([]),
    [local, setLocal] = useState<Skill[]>([]),
    [next, setNext] = useState("");
  const [enabled, setEnabled] = useState(false),
    [busy, setBusy] = useState(false),
    [saving, setSaving] = useState(false),
    [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0),
    [open, setOpen] = useState(false),
    [files, setFiles] = useState<File[]>([]),
    [selected, setSelected] = useState<Inspection | null>(null),
    [confirmed, setConfirmed] = useState(false);
  const superadmin = principal.role === "superadmin";
  useEffect(() => {
    let live = true;
    setBusy(true);
    setError("");
    setRows([]);
    setLocal([]);
    setSelected(null);
    api<SkillPage>("skills/manage/list", { tenant_id: tenant })
      .then((p) => {
        if (live) {
          setRows(p.items);
          setLocal(p.deployment || []);
          setEnabled(p.enabled);
          setNext(p.next || "");
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
  function begin(name = "") {
    form.resetFields();
    form.setFieldsValue({ name, version: name ? "" : "1" });
    setFiles([]);
    setError("");
    setOpen(true);
  }
  async function upload(values: { name: string; version: string }) {
    setSaving(true);
    setError("");
    try {
      const payload = await uploadFiles(files);
      await api("skills/upload", { tenant_id: tenant, ...values, ...payload });
      setOpen(false);
      setFiles([]);
      setRefresh((v) => v + 1);
      message.success("Skill 已保存为待审核版本，尚未执行或授权");
    } catch (e) {
      setError(errorText(e));
    } finally {
      setSaving(false);
    }
  }
  async function inspect(s: ManagedSkill) {
    setError("");
    setConfirmed(false);
    try {
      setSelected(
        await api<Inspection>("skills/inspect", {
          tenant_id: tenant,
          name: s.name,
          version: s.version,
        }),
      );
    } catch (e) {
      setError(errorText(e));
    }
  }
  async function review(status: string) {
    if (!selected) return;
    setSaving(true);
    setError("");
    try {
      await api("skills/review", {
        tenant_id: tenant,
        name: selected.skill.name,
        version: selected.skill.version,
        expected_revision: selected.skill.revision,
        status,
      });
      setSelected(null);
      setRefresh((v) => v + 1);
      message.success(
        status === "approved"
          ? "此版本已对当前工作空间授权"
          : "授权已撤销，后续运行和执行会重新校验",
      );
    } catch (e) {
      setError(errorText(e));
    } finally {
      setSaving(false);
    }
  }
  return (
    <>
      <Panel
        title="Skill 版本"
        subtitle="上传后由平台管理员审核。版本内容不可覆盖，新内容使用新版本号；执行仍需单次审批和隔离沙箱。"
      >
        <Space wrap>
          <Button
            type="primary"
            disabled={!enabled || !writable(principal)}
            onClick={() => begin()}
          >
            上传 Skill
          </Button>
          <Button onClick={() => setRefresh((v) => v + 1)}>刷新</Button>
        </Space>
        {!busy && !enabled && (
          <Alert
            type="info"
            showIcon
            title="上传功能需要 PostgreSQL 和 schema 32"
            description="本地目录 Skill 仍可按原部署配置使用。"
          />
        )}
        {error && !open && !selected && <Failure error={error} />}
        <Table
          rowKey={(s) => s.name + "@" + s.version}
          dataSource={rows}
          loading={busy}
          pagination={false}
          columns={[
            { title: "名称", dataIndex: "name" },
            { title: "版本", dataIndex: "version" },
            { title: "说明", dataIndex: "description" },
            {
              title: "状态",
              dataIndex: "status",
              render: (v: string) => (
                <Tag
                  color={
                    v === "approved"
                      ? "green"
                      : v === "pending"
                        ? "orange"
                        : "default"
                  }
                >
                  {statusNames[v] || v}
                </Tag>
              ),
            },
            { title: "上传时间", dataIndex: "created_at", render: date },
            {
              title: "操作",
              render: (_: unknown, s: ManagedSkill) => (
                <Space>
                  <Button onClick={() => inspect(s)}>
                    查看{superadmin ? " / 审核" : ""}
                  </Button>
                  {writable(principal) && (
                    <Button onClick={() => begin(s.name)}>上传新版本</Button>
                  )}
                </Space>
              ),
            },
          ]}
        />
        {next && (
          <Button
            onClick={async () => {
              try {
                const p = await api<SkillPage>("skills/manage/list", {
                  tenant_id: tenant,
                  after: next,
                });
                setRows((v) => [...v, ...p.items]);
                setNext(p.next || "");
              } catch (e) {
                setError(errorText(e));
              }
            }}
          >
            加载更多版本
          </Button>
        )}
        <p className="muted">
          已授权版本可在 Agent 工作台的 Skills
          区域选择。标准单机安装默认不开放沙箱执行；运行 run.sh
          前需要部署者配置专用 Docker 执行节点。
        </p>
      </Panel>
      {!!local.length && (
        <Panel
          title="部署者预置的 Skill"
          subtitle="这些内容来自服务器目录；不在网页覆盖，其授权由部署者管理。"
        >
          <Table
            rowKey={(s) => s.name + "@" + s.version}
            dataSource={local}
            pagination={false}
            columns={[
              { title: "名称", dataIndex: "name" },
              { title: "版本", dataIndex: "version" },
              { title: "说明", dataIndex: "description" },
            ]}
          />
        </Panel>
      )}
      {open && (
        <Modal
          title="上传 Skill 版本"
          open
          onCancel={() => {
            setOpen(false);
            setFiles([]);
          }}
          footer={null}
          destroyOnHidden
        >
          <Alert
            type="info"
            showIcon
            title="上传不会执行脚本"
            description="SKILL.md 为必需文件，run.sh 可选。也可上传仅包含这些文件的 ZIP，文件位于根目录或与 Skill 名称一致的文件夹中。每个工作空间最多保存 128 个上传版本。"
          />
          <Form form={form} layout="vertical" onFinish={upload}>
            <Form.Item
              name="name"
              label="Skill 名称"
              rules={[
                {
                  required: true,
                  pattern: /^[a-z][a-z0-9-]{0,47}$/,
                  message: "使用小写字母、数字和短横线，以字母开头，最长 48 位",
                },
              ]}
              extra="必须与 SKILL.md 顶部 YAML 中的 name 一致。"
            >
              <Input placeholder="例如：text-summary" />
            </Form.Item>
            <Form.Item
              name="version"
              label="版本"
              rules={[
                {
                  required: true,
                  pattern: /^[a-zA-Z0-9][a-zA-Z0-9.-]{0,31}$/,
                  message: "版本最长 32 位，使用字母、数字、点和短横线",
                },
              ]}
            >
              <Input placeholder="例如：1.0.0" />
            </Form.Item>
            <Form.Item
              label="选择文件"
              required
              extra="每个文本文件最多 64 KiB，ZIP 最多 256 KiB；不接收其他文件、符号链接或任意目录。"
            >
              <Input
                type="file"
                multiple
                accept=".zip,.md,.sh"
                onChange={(e) => setFiles(Array.from(e.target.files || []))}
              />
            </Form.Item>
            <p className="muted">
              SKILL.md 需要 YAML 头部的
              name、description，以及任务说明正文。run.sh 在现有无网络、非 root
              沙箱中运行，不允许访问宿主机目录。
            </p>
            {error && <Failure error={error} />}
            <Button htmlType="submit" type="primary" block loading={saving}>
              校验并提交审核
            </Button>
          </Form>
        </Modal>
      )}
      <Drawer
        title={
          selected
            ? `${selected.skill.name} · ${selected.skill.version}`
            : "Skill"
        }
        open={!!selected}
        onClose={() => setSelected(null)}
        size="large"
      >
        {selected && (
          <>
            <p>
              <Tag>{statusNames[selected.skill.status]}</Tag>上传者：
              {selected.skill.created_by}
            </p>
            <p className="muted">
              SHA-256：<code>{selected.skill.checksum}</code>
            </p>
            <h4>SKILL.md</h4>
            <Input.TextArea readOnly value={selected.markdown} rows={12} />
            {selected.skill.executable ? (
              <>
                <h4>run.sh</h4>
                <Input.TextArea readOnly value={selected.script} rows={12} />
              </>
            ) : (
              <p>说明型 Skill：不包含可执行脚本，也不需要开启沙箱。</p>
            )}
            {superadmin && (
              <Space
                direction="vertical"
                style={{ width: "100%", marginTop: 16 }}
              >
                {selected.skill.status !== "approved" && (
                  <>
                    <Checkbox
                      checked={confirmed}
                      onChange={(e) => setConfirmed(e.target.checked)}
                    >
                      我已审核此版本内容，同意仅对当前工作空间授权
                    </Checkbox>
                    <Button
                      type="primary"
                      disabled={!confirmed}
                      loading={saving}
                      onClick={() => review("approved")}
                    >
                      批准此版本
                    </Button>
                  </>
                )}
                {selected.skill.status !== "revoked" && (
                  <Popconfirm
                    title="撤销当前版本的授权？"
                    description="新运行和后续脚本执行将被拒绝；不会强制终止已经开始的沙箱进程。"
                    onConfirm={() => review("revoked")}
                  >
                    <Button danger loading={saving}>
                      撤销授权
                    </Button>
                  </Popconfirm>
                )}
              </Space>
            )}
            {error && <Failure error={error} />}
          </>
        )}
      </Drawer>
    </>
  );
}
