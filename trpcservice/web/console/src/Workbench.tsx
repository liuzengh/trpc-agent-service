import { useEffect, useRef, useState } from "react";
import {
  Alert,
  App,
  Button,
  Checkbox,
  Collapse,
  Drawer,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Skeleton,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
} from "antd";
import { APIError, api, errorText } from "./api";
import {
  Blank,
  Failure,
  Icon,
  Panel,
  Status,
  ValidationView,
} from "./components";
import {
  date,
  writable,
  type AgentApp,
  type Dict,
  type Draft,
  type Page,
  type Principal,
  type Revision,
  type Stored,
  type Validation,
  type Workspace,
} from "./types";
import { navigate } from "./App";
import { RunsPage } from "./pages";
import { DebugPanel } from "./debug";
import { CredentialSelect } from "./CredentialSelect";
import { channelLabels } from "./channel-types";
import { ActivityPanel } from "./ActivityPanel";
import {
  ConnectionSelect,
  CreateModelConnection,
  type ModelConnectionPage,
} from "./ModelConnections";

const sections: Record<string, string> = {
  agent_config: "Agent 指令与能力",
  model_config: "模型配置",
  tool_policy: "工具权限与预算",
  knowledge_config: "知识库",
  memory_config: "长期记忆",
  guardrail_config: "治理策略",
};
const canonical = (value: any): string =>
  JSON.stringify(value, (_key, v) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(
          Object.entries(v).sort(([a], [b]) => a.localeCompare(b)),
        )
      : v,
  );

export function Workbench({
  tenant,
  appID,
  principal,
}: {
  tenant: string;
  appID: string;
  principal: Principal;
}) {
  const { message, modal } = App.useApp();
  const [workspace, setWorkspace] = useState<Workspace | null>(null);
  const [cfg, setCfg] = useState<Revision | null>(null);
  const [conflict, setConflict] = useState<Stored<Draft> | null>(null);
  const [mergeKeys, setMergeKeys] = useState<string[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");
  const [tab, setTab] = useState("configure");
  const [report, setReport] = useState<Validation | null>(null);
  const [advanced, setAdvanced] = useState(false);
  const [raw, setRaw] = useState("");
  const [publishOpen, setPublishOpen] = useState(false);
  const [readiness, setReadiness] = useState<Dict | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settings, setSettings] = useState<Dict>({});
  const [rolloutOpen, setRolloutOpen] = useState(false);
  const [rollout, setRollout] = useState<Dict>({});
  const [rolloutReport, setRolloutReport] = useState<Validation | null>(null);
  const [versions, setVersions] = useState<Revision[]>([]);
  const [versionsNext, setVersionsNext] = useState("");
  const [models, setModels] = useState<Dict[]>([]);
  const [connections, setConnections] = useState<ModelConnectionPage>({
    items: [],
    enabled: false,
    allowed_origins: [],
    endpoint_policy: "allowlist",
  });
  const [connectionsError, setConnectionsError] = useState("");
  const [connectionOpen, setConnectionOpen] = useState(false);
  useEffect(() => {
    let live = true;
    api<ModelConnectionPage>("model-connections/list", { tenant_id: tenant })
      .then((data) => {
        if (live) setConnections(data);
      })
      .catch((e) => {
        if (live) setConnectionsError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant]);
  useEffect(() => {
    let live = true;
    api<Page<Dict>>("resources/list", { tenant_id: tenant, kind: "models" })
      .then((data) => {
        if (live) setModels(data.items);
      })
      .catch(() => {});
    return () => {
      live = false;
    };
  }, [tenant]);
  const [view, setView] = useState<Revision | null>(null);
  const [reload, setReload] = useState(0);
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    setError("");
    api<Workspace>("workspace", { tenant_id: tenant, app_id: appID })
      .then((data) => {
        if (alive.current) {
          setWorkspace(data);
          setCfg(structuredClone(data.draft.data.config));
        }
      })
      .catch((e) => {
        if (alive.current) setError(errorText(e));
      });
    return () => {
      alive.current = false;
    };
  }, [tenant, appID, reload]);
  useEffect(() => {
    let live = true;
    if (tab === "versions")
      api<Page<Revision>>("catalog/list", {
        tenant_id: tenant,
        app_id: appID,
        kind: "revisions",
        limit: 100,
      })
        .then((data) => {
          if (live) {
            setVersions(
              data.items.sort((a, b) => b.revision_no - a.revision_no),
            );
            setVersionsNext(data.next || "");
          }
        })
        .catch((e) => message.error(errorText(e)));
    return () => {
      live = false;
    };
  }, [tenant, appID, tab, reload, message]);
  const dirty =
    !!workspace &&
    !!cfg &&
    canonical(cfg) !== canonical(workspace.draft.data.config);
  const canWrite = writable(principal);
  useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);
  function patch(section: keyof Revision, key: string, value: any) {
    setCfg((old) =>
      old
        ? { ...old, [section]: { ...(old[section] as Dict), [key]: value } }
        : old,
    );
    setReport(null);
  }
  function embedding(key: string, value: any) {
    setCfg((old) =>
      old
        ? {
            ...old,
            knowledge_config: {
              ...old.knowledge_config,
              embedding: { ...old.knowledge_config.embedding, [key]: value },
            },
          }
        : old,
    );
    setReport(null);
  }
  function fail(e: unknown) {
    if (e instanceof APIError && e.validation) setReport(e.validation);
    setError(errorText(e));
    message.error(errorText(e));
  }
  async function save(): Promise<Stored<Draft>> {
    if (!workspace || !cfg) throw new Error("工作台尚未加载");
    setBusy("save");
    setError("");
    try {
      const draft = await api<Stored<Draft>>("drafts/save", {
        tenant_id: tenant,
        app_id: appID,
        expected_version: workspace.draft.version,
        config: cfg,
      });
      if (alive.current) {
        setWorkspace((old) => (old ? { ...old, draft } : old));
        setCfg(structuredClone(draft.data.config));
        message.success("草稿已保存，线上版本未改变");
      }
      return draft;
    } catch (e) {
      if (e instanceof APIError && e.status === 409) {
        try {
          const latest = await api<Stored<Draft>>("drafts/get", {
            tenant_id: tenant,
            app_id: appID,
          });
          if (alive.current) {
            setConflict(latest);
            setMergeKeys(
              Object.keys(sections).filter(
                (key) =>
                  canonical((workspace.draft.data.config as any)[key]) ===
                    canonical((latest.data.config as any)[key]) &&
                  canonical((cfg as any)[key]) !==
                    canonical((latest.data.config as any)[key]),
              ),
            );
          }
        } catch {
          /* Preserve local edits if the server cannot be reloaded. */
        }
      }
      fail(e);
      throw e;
    } finally {
      setBusy("");
    }
  }
  async function check() {
    if (!cfg) return;
    setBusy("check");
    setError("");
    try {
      const result = await api<Validation>("revisions/validate", {
        ...cfg,
        tenant_id: tenant,
        app_id: appID,
      });
      setReport(result);
      if (result.valid) message.success("配置有效，请同时查看运行依赖状态");
    } catch (e) {
      fail(e);
    } finally {
      setBusy("");
    }
  }
  async function preparePublish() {
    if (!workspace || !cfg) return;
    setBusy("publish");
    try {
      if (dirty || workspace.draft.version === 0) {
        await save();
      }
      const validation = await api<Validation>("revisions/validate", {
        ...cfg,
        tenant_id: tenant,
        app_id: appID,
      });
      setReport(validation);
      if (validation.valid) {
        setReadiness(
          await api<Dict>("drafts/readiness", {
            tenant_id: tenant,
            app_id: appID,
          }),
        );
        setPublishOpen(true);
      }
    } catch (e) {
      fail(e);
    } finally {
      setBusy("");
    }
  }
  async function publish() {
    if (!workspace) return;
    setBusy("publish");
    try {
      const result = await api<{
        draft: Stored<Draft>;
        revision_id: string;
        app_version: number;
      }>("drafts/publish", {
        tenant_id: tenant,
        app_id: appID,
        expected_version: workspace.draft.version,
        expected_app_version: workspace.app.version,
        request_id: crypto.randomUUID(),
      });
      setWorkspace((old) =>
        old
          ? {
              ...old,
              draft: result.draft,
              app: {
                ...old.app,
                version: result.app_version,
                stable_revision_id: result.revision_id,
              },
            }
          : old,
      );
      setPublishOpen(false);
      setReload((v) => v + 1);
      message.success("新版本已发布；旧会话继续使用原版本");
    } catch (e) {
      fail(e);
    } finally {
      setBusy("");
    }
  }
  function chooseSkill(name: string, version: string, checked: boolean) {
    if (!cfg || !workspace) return;
    const item = workspace.skills.find(
      (s) => s.name === name && s.version === version,
    );
    if (!item) return;
    const refs = (cfg.agent_config.skills || []).filter(
      (s: Dict) => s.name !== name,
    );
    if (checked)
      refs.push({
        name: item.name,
        version: item.version,
        checksum: item.checksum,
      });
    const allowed = new Set<string>(cfg.tool_policy.allowed_tools || []);
    if (refs.length) {
      allowed.add("skill_load");
      const executable = refs.some((ref: Dict) =>
        workspace.skills.some(
          (s) =>
            s.name === ref.name &&
            s.version === ref.version &&
            s.checksum === ref.checksum &&
            s.executable,
        ),
      );
      if (executable) allowed.add("skill_run");
      else allowed.delete("skill_run");
    } else {
      allowed.delete("skill_load");
      allowed.delete("skill_run");
    }
    setCfg({
      ...cfg,
      agent_config: { ...cfg.agent_config, skills: refs },
      tool_policy: { ...cfg.tool_policy, allowed_tools: [...allowed] },
    });
    setReport(null);
  }
  async function useVersion(revision: Revision) {
    if (!workspace) return;
    modal.confirm({
      title: "编辑为草稿",
      content:
        "将此版本复制到草稿。当前未保存的编辑将被替换，线上版本不会改变。",
      okText: "复制到草稿",
      onOk: async () => {
        await api("drafts/reset", {
          tenant_id: tenant,
          app_id: appID,
          source_revision_id: revision.revision_id,
          expected_version: workspace.draft.version,
        });
        setTab("configure");
        setView(null);
        setReload((v) => v + 1);
      },
    });
  }
  async function rollback(revision: Revision) {
    if (!workspace) return;
    modal.confirm({
      title: `回滚到版本 v${revision.revision_no}？`,
      content:
        "系统会重新检查当前授权与依赖。只改变新会话使用的稳定版本，不迁移旧会话。",
      okText: "检查并回滚",
      onOk: async () => {
        try {
          await api("revisions/publish", {
            tenant_id: tenant,
            app_id: appID,
            revision_id: revision.revision_id,
            expected_version: workspace.app.version,
          });
          setView(null);
          setReload((v) => v + 1);
          message.success("稳定版本已切换");
        } catch (e) {
          fail(e);
          throw e;
        }
      },
    });
  }
  if (!workspace || !cfg)
    return (
      <div className="workbench-loading">
        {error ? (
          <Failure error={error} retry={() => setReload((v) => v + 1)} />
        ) : (
          <Skeleton active />
        )}
      </div>
    );
  const selectedTools: string[] = cfg.tool_policy.allowed_tools || [];
  const source = cfg.model_config.source || "startup_env";
  const modelLimits = Object.fromEntries(
    [
      "max_prompt_tokens",
      "max_completion_tokens",
      "timeout_seconds",
      "prompt_cost_per_million",
      "completion_cost_per_million",
    ]
      .filter((k) => cfg.model_config[k] !== undefined)
      .map((k) => [k, cfg.model_config[k]]),
  );
  const changed = Object.keys(sections).filter(
    (key) =>
      canonical((cfg as any)[key]) !==
      canonical((workspace.stable as any)?.[key] || {}),
  );
  return (
    <>
      {connectionOpen && (
        <CreateModelConnection
          tenant={tenant}
          origins={connections.allowed_origins}
          policy={connections.endpoint_policy}
          onCancel={() => setConnectionOpen(false)}
          onCreated={(created) => {
            setConnectionOpen(false);
            setConnections((old) => ({
              ...old,
              items: [created, ...old.items],
            }));
            setCfg({
              ...cfg,
              model_config: {
                source: "connection",
                connection_id: created.connection_id,
                ...modelLimits,
              },
            });
            setReport(null);
            message.success("连接已选用，请保存草稿并开始新调试");
          }}
        />
      )}
      <div className="workbench-header">
        <div className="workbench-title">
          <button
            className="icon-button"
            title="返回应用"
            onClick={() => navigate("agents")}
          >
            <Icon name="back" />
          </button>
          <span className="agent-avatar">
            <Icon name="agent" size={23} />
          </span>
          <div>
            <h2>{workspace.app.name}</h2>
            <span>
              {dirty
                ? "有未保存修改"
                : workspace.draft.version
                  ? "草稿已保存 · v" + workspace.draft.version
                  : "尚未保存草稿"}
            </span>
          </div>
          <Status
            value={workspace.app.stable_revision_id ? "published" : "draft"}
          />
        </div>
        <Space>
          <Button
            disabled={!canWrite || !!busy}
            onClick={() => {
              setSettings({ ...workspace.app });
              setSettingsOpen(true);
            }}
          >
            应用设置
          </Button>
          <Button
            icon={<Icon name="code" />}
            onClick={() => {
              setRaw(JSON.stringify(cfg, null, 2));
              setAdvanced(true);
            }}
          >
            高级配置
          </Button>
          <Button
            disabled={!canWrite || !!busy}
            onClick={() => {
              void save().catch(() => {});
            }}
            loading={busy === "save"}
          >
            保存草稿
          </Button>
          <Button
            type="primary"
            icon={<Icon name="arrow" />}
            disabled={!canWrite || !!busy}
            loading={busy === "publish"}
            onClick={preparePublish}
          >
            发布
          </Button>
        </Space>
      </div>
      <div className="workbench-guide">
        <strong>
          配置模型和指令 → 保存草稿 → 右侧调试 → 发布版本 → 接入通道
        </strong>
        <span>
          {workspace.app.stable_revision_id
            ? "已发布；修改草稿不会改变线上版本。"
            : "尚未发布；你可以先调试，不需要先绑定机器人。"}
        </span>
      </div>
      <div className="workbench-tabs">
        <Tabs
          activeKey={tab}
          onChange={setTab}
          items={[
            { key: "configure", label: "配置与调试" },
            { key: "versions", label: "发布记录" },
            { key: "runs", label: "运行记录" },
            { key: "jobs", label: "后台任务" },
            { key: "channels", label: "接入通道" },
          ]}
        />
      </div>
      {error && (
        <div className="workbench-error">
          <Failure error={error} retry={() => setReload((v) => v + 1)} />
        </div>
      )}
      {tab === "configure" ? (
        <div className="workbench-columns">
          <div className="configuration">
            <div className="configuration-caption">
              <strong>
                <Icon name="settings" /> Agent 配置
              </strong>
              <Button
                size="small"
                disabled={!!busy || !canWrite}
                onClick={check}
                loading={busy === "check"}
              >
                检查配置
              </Button>
            </div>
            {report && <ValidationView report={report} />}
            <Form layout="vertical" disabled={!canWrite || !!busy}>
              <Panel
                title="模型"
                subtitle="选择租户模型连接或部署者默认模型。Agent 版本只记录引用，不保存 API Key。"
                action={
                  principal.role === "superadmin" &&
                  connections.enabled && (
                    <Button
                      disabled={!!busy}
                      onClick={() => setConnectionOpen(true)}
                    >
                      新增模型连接
                    </Button>
                  )
                }
              >
                {connectionsError && <Failure error={connectionsError} />}
                {source === "startup_env" &&
                  workspace.startup_model_name === "tutorial-mock-model" && (
                    <Alert
                      type="warning"
                      showIcon
                      title="当前使用 Mock 演示模型"
                      description="只能验证固定对话流程，不会真实理解提示词或调用在线 AI。请新增或选择真实模型连接，再开始新调试。"
                    />
                  )}
                <Form.Item label="模型来源">
                  <Select
                    value={source}
                    onChange={(value) => {
                      setCfg({
                        ...cfg,
                        model_config: {
                          ...modelLimits,
                          source: value,
                          ...(value === "revision"
                            ? {
                                provider: cfg.model_config.provider || "openai",
                              }
                            : {}),
                        },
                      });
                      setReport(null);
                    }}
                    options={[
                      {
                        value: "startup_env",
                        label:
                          "部署者默认模型" +
                          (workspace.startup_model_name
                            ? " · " + workspace.startup_model_name
                            : ""),
                      },
                      { value: "connection", label: "当前租户的模型连接" },
                      { value: "revision", label: "高级：使用环境凭据引用" },
                    ]}
                  />
                </Form.Item>
                {source === "connection" && (
                  <Form.Item
                    label="模型连接"
                    help="模型和地址固定到所选配置版本，Key 由管理员独立更新。选择新配置后请保存并新建调试，发布后再供业务使用。"
                  >
                    <ConnectionSelect
                      data={connections}
                      value={cfg.model_config.connection_id}
                      disabled={!canWrite || !!busy}
                      onChange={(value) =>
                        patch("model_config", "connection_id", value)
                      }
                      onMore={async () => {
                        try {
                          const p = await api<ModelConnectionPage>(
                            "model-connections/list",
                            { tenant_id: tenant, after: connections.next },
                          );
                          setConnections((old) => ({
                            ...p,
                            items: [...old.items, ...p.items],
                          }));
                        } catch (e) {
                          setConnectionsError(errorText(e));
                        }
                      }}
                    />
                    {!connections.items.length && (
                      <p className="muted">
                        还没有可用连接。平台管理员可在此新增；或先使用部署者默认模型。
                      </p>
                    )}
                  </Form.Item>
                )}
                {source === "revision" && models.length > 0 && (
                  <Form.Item
                    label="复用模型配置"
                    help="复用当前租户已发布的模型配置；保留本 Agent 已设置的 Token 和超时上限。"
                  >
                    <Select
                      value={undefined}
                      placeholder="选择已授权的配置"
                      options={models.map((m) => ({
                        value: m.resource_id,
                        label:
                          m.name + (m.source_app ? " · " + m.source_app : ""),
                        disabled: m.status === "unavailable",
                      }))}
                      onChange={(id) => {
                        const model = models.find((m) => m.resource_id === id);
                        if (model) {
                          const limits = Object.fromEntries(
                            [
                              "max_prompt_tokens",
                              "max_completion_tokens",
                              "timeout_seconds",
                            ]
                              .filter((k) => cfg.model_config[k] !== undefined)
                              .map((k) => [k, cfg.model_config[k]]),
                          );
                          setCfg({
                            ...cfg,
                            model_config: {
                              ...structuredClone(model.model_config),
                              ...limits,
                            },
                          });
                          setReport(null);
                        }
                      }}
                    />
                  </Form.Item>
                )}
                {source === "revision" && (
                  <>
                    <div className="form-grid">
                      <Form.Item label="供应商">
                        <Select
                          value={cfg.model_config.provider || "openai"}
                          onChange={(value) =>
                            patch("model_config", "provider", value)
                          }
                          options={[
                            { value: "openai", label: "OpenAI-compatible" },
                            { value: "mock", label: "本地 Mock（不联网）" },
                          ]}
                        />
                      </Form.Item>
                      <Form.Item label="模型名称">
                        <Input
                          value={cfg.model_config.name || ""}
                          onChange={(e) =>
                            patch("model_config", "name", e.target.value)
                          }
                          placeholder="部署者提供的模型 ID"
                        />
                      </Form.Item>
                    </div>
                    <Form.Item label="API 地址">
                      <Input
                        value={cfg.model_config.base_url || ""}
                        onChange={(e) =>
                          patch("model_config", "base_url", e.target.value)
                        }
                        placeholder="https://provider.example/v1"
                      />
                    </Form.Item>
                    <Form.Item
                      label="凭据引用"
                      help="不是 API Key；例如 env://TEAM_MODEL_KEY。引用必须已获租户授权。"
                    >
                      <CredentialSelect
                        tenant={tenant}
                        purpose="model"
                        disabled={!canWrite}
                        value={
                          cfg.model_config.api_key_ref ||
                          (cfg.model_config.api_key_env
                            ? "env://" + cfg.model_config.api_key_env
                            : "")
                        }
                        onChange={(reference) =>
                          setCfg({
                            ...cfg,
                            model_config: {
                              ...cfg.model_config,
                              api_key_ref: reference,
                              api_key_env: "",
                            },
                          })
                        }
                      />
                    </Form.Item>
                  </>
                )}
                <div className="form-grid">
                  <Form.Item label="单次回复 Token 上限">
                    <InputNumber
                      style={{ width: "100%" }}
                      min={0}
                      max={131072}
                      value={cfg.model_config.max_completion_tokens || 0}
                      onChange={(value) =>
                        patch(
                          "model_config",
                          "max_completion_tokens",
                          value || 0,
                        )
                      }
                    />
                  </Form.Item>
                  <Form.Item label="模型超时（秒）">
                    <InputNumber
                      style={{ width: "100%" }}
                      min={0}
                      max={300}
                      value={cfg.model_config.timeout_seconds || 0}
                      onChange={(value) =>
                        patch("model_config", "timeout_seconds", value || 0)
                      }
                    />
                  </Form.Item>
                </div>
                <span className="field-note">
                  0 沿用运行时默认值，不代表禁止调用。
                </span>
              </Panel>
              <Panel title="提示词" subtitle="说明角色、任务边界和回复要求。">
                <Form.Item label="执行名称">
                  <Input
                    value={cfg.agent_config.name || ""}
                    onChange={(e) =>
                      patch("agent_config", "name", e.target.value)
                    }
                    maxLength={100}
                  />
                </Form.Item>
                <Form.Item label="系统指令">
                  <Input.TextArea
                    value={cfg.agent_config.instruction || ""}
                    onChange={(e) =>
                      patch("agent_config", "instruction", e.target.value)
                    }
                    autoSize={{ minRows: 7, maxRows: 18 }}
                    placeholder="你是…；你的任务是…；当信息不足时…"
                  />
                </Form.Item>
              </Panel>
              <Panel
                title="工具"
                subtitle="只有选中的工具才会向模型开放。审批和用户权限仍由后端强制执行。"
              >
                <div className="tool-picker">
                  {workspace.tools
                    .filter(
                      (t) => !["skill_load", "skill_run"].includes(t.name),
                    )
                    .map((tool) => (
                      <Checkbox
                        key={tool.name}
                        checked={selectedTools.includes(tool.name)}
                        onChange={(e) =>
                          patch(
                            "tool_policy",
                            "allowed_tools",
                            e.target.checked
                              ? [...selectedTools, tool.name]
                              : selectedTools.filter((n) => n !== tool.name),
                          )
                        }
                      >
                        <div>
                          <strong>{tool.name}</strong>
                          {tool.requires_approval && (
                            <Tag color="orange">需审批</Tag>
                          )}
                          <small>{tool.description}</small>
                        </div>
                      </Checkbox>
                    ))}
                </div>
                <div className="form-grid">
                  <Form.Item label="每轮工具调用上限">
                    <InputNumber
                      style={{ width: "100%" }}
                      min={0}
                      max={1000}
                      value={cfg.tool_policy.max_tool_calls || 0}
                      onChange={(value) =>
                        patch("tool_policy", "max_tool_calls", value || 0)
                      }
                    />
                  </Form.Item>
                  <Form.Item label="整轮执行时长">
                    <Input
                      value={cfg.tool_policy.max_run_duration || ""}
                      placeholder="例如 2m"
                      onChange={(e) =>
                        patch("tool_policy", "max_run_duration", e.target.value)
                      }
                    />
                  </Form.Item>
                </div>
                {cfg.tool_policy.max_tool_calls === 1 &&
                  selectedTools.includes("skill_run") && (
                    <Alert
                      type="warning"
                      showIcon
                      title="加载并执行 Skill 至少需要两次工具调用"
                      description="请明确提高上限，平台不会自动修改预算。"
                    />
                  )}
              </Panel>
              <Panel
                title="Skills"
                subtitle="选择已授权的具体版本。说明型仅加载内容，脚本型执行需要审批和沙箱。"
              >
                {workspace.skills.length ? (
                  workspace.skills.map((skill) => (
                    <label
                      className="skill-choice"
                      key={skill.name + "@" + skill.version}
                    >
                      <Checkbox
                        checked={(cfg.agent_config.skills || []).some(
                          (s: Dict) =>
                            s.name === skill.name &&
                            s.checksum === skill.checksum,
                        )}
                        onChange={(e) =>
                          chooseSkill(
                            skill.name,
                            skill.version,
                            e.target.checked,
                          )
                        }
                      />
                      <span className="skill-icon">
                        <Icon name="code" />
                      </span>
                      <div>
                        <strong>
                          {skill.name} <Tag>v{skill.version}</Tag>
                        </strong>
                        <p>{skill.description}</p>
                        <small>
                          {skill.executable
                            ? "已授权 · 脚本执行需审批 · Docker 沙箱"
                            : "已授权 · 说明型 · 不执行脚本"}
                        </small>
                      </div>
                    </label>
                  ))
                ) : (
                  <Blank title="当前租户没有已授权 Skill" />
                )}
              </Panel>
              <Panel
                title="知识与记忆"
                subtitle="知识库使用已绑定的后端；调试使用独立身份，不写入业务用户的记忆。"
              >
                <div className="switch-row">
                  <div>
                    <strong>启用知识库检索</strong>
                    <p>需要已注册的知识库后端与匹配的 Embedding 配置。</p>
                  </div>
                  <Switch
                    checked={!!cfg.knowledge_config.enabled}
                    onChange={(value) =>
                      patch("knowledge_config", "enabled", value)
                    }
                  />
                </div>
                {cfg.knowledge_config.enabled && (
                  <>
                    <Alert
                      type="info"
                      title="知识库检索已实现，需手动配置"
                      description="先在资源中心绑定知识库存储，并在部署配置中设置 Embedding 密钥和授权；资料通过 Admin API 导入，网页暂不支持文档上传。完整步骤见安装运行手册的“知识库：手动配置与资料导入”。更换 Embedding 或向量维度需要重建或迁移索引。"
                    />
                    <Form.Item label="Embedding 供应商">
                      <Select
                        value={
                          cfg.knowledge_config.embedding?.provider || undefined
                        }
                        placeholder="请选择"
                        options={[
                          {
                            value: "openai",
                            label: "OpenAI-compatible Embedding",
                          },
                          { value: "hash", label: "本地 Hash（仅开发）" },
                        ]}
                        onChange={(value) => embedding("provider", value)}
                      />
                    </Form.Item>
                    <div className="form-grid">
                      <Form.Item label="向量维度">
                        <InputNumber
                          style={{ width: "100%" }}
                          min={1}
                          max={65536}
                          value={cfg.knowledge_config.embedding?.dimensions}
                          onChange={(value) => embedding("dimensions", value)}
                        />
                      </Form.Item>
                      <Form.Item label="返回结果数">
                        <InputNumber
                          style={{ width: "100%" }}
                          min={1}
                          max={100}
                          value={cfg.knowledge_config.max_results}
                          onChange={(value) =>
                            patch("knowledge_config", "max_results", value)
                          }
                        />
                      </Form.Item>
                    </div>
                    {cfg.knowledge_config.embedding?.provider === "openai" && (
                      <>
                        <Form.Item label="Embedding 模型">
                          <Input
                            value={cfg.knowledge_config.embedding?.model || ""}
                            onChange={(e) => embedding("model", e.target.value)}
                          />
                        </Form.Item>
                        <Form.Item label="Embedding API 地址">
                          <Input
                            value={
                              cfg.knowledge_config.embedding?.base_url || ""
                            }
                            onChange={(e) =>
                              embedding("base_url", e.target.value)
                            }
                            placeholder="https://provider.example/v1"
                          />
                        </Form.Item>
                        <Form.Item
                          label="Embedding 凭据引用"
                          help="必须拥有 embedding 用途授权；不要粘贴真实 API Key。"
                        >
                          <CredentialSelect
                            tenant={tenant}
                            purpose="embedding"
                            disabled={!canWrite}
                            value={
                              cfg.knowledge_config.embedding?.secret_ref || ""
                            }
                            onChange={(reference) =>
                              embedding("secret_ref", reference)
                            }
                          />
                        </Form.Item>
                      </>
                    )}
                  </>
                )}
                <div className="switch-row">
                  <div>
                    <strong>个人记忆仅用于私聊</strong>
                    <p>避免将个人长期记忆带入群聊。</p>
                  </div>
                  <Switch
                    checked={!!cfg.memory_config.direct_only}
                    onChange={(value) =>
                      patch("memory_config", "direct_only", value)
                    }
                  />
                </div>
                <Form.Item label="预加载记忆条数">
                  <InputNumber
                    min={0}
                    max={100}
                    value={cfg.agent_config.preload_memory || 0}
                    onChange={(value) =>
                      patch("agent_config", "preload_memory", value || 0)
                    }
                  />
                </Form.Item>
                <div className="switch-row">
                  <div>
                    <strong>自动提取长期记忆</strong>
                    <p>仅对业务执行生效；网页调试不会启动自动记忆任务。</p>
                  </div>
                  <Switch
                    checked={!!cfg.memory_config.auto_extract}
                    onChange={(value) =>
                      patch("memory_config", "auto_extract", value)
                    }
                  />
                </div>
                {cfg.memory_config.auto_extract && (
                  <Form.Item label="每隔多少轮提取记忆">
                    <InputNumber
                      min={1}
                      max={1000}
                      value={cfg.memory_config.every_turns}
                      onChange={(value) =>
                        patch("memory_config", "every_turns", value)
                      }
                    />
                  </Form.Item>
                )}
                {cfg.memory_config.auto_extract &&
                  cfg.memory_config.direct_only && (
                    <Alert
                      type="warning"
                      title="仅私聊记忆不能同时启用自动提取"
                      description="请关闭自动提取，使用受控记忆工具。"
                    />
                  )}
                <Form.Item label="每隔多少轮生成摘要（0 为关闭）">
                  <InputNumber
                    min={0}
                    max={1000}
                    value={cfg.agent_config.summary_every_turns || 0}
                    onChange={(value) =>
                      patch("agent_config", "summary_every_turns", value || 0)
                    }
                  />
                </Form.Item>
              </Panel>
              <Panel
                title="输入与输出治理"
                subtitle="发布后由后端执行，不依赖模型自行遵守。"
              >
                <Form.Item label="用户输入最大字符数（0 为不设置此限制）">
                  <InputNumber
                    min={0}
                    max={100000}
                    value={cfg.guardrail_config.max_input_chars || 0}
                    onChange={(value) =>
                      patch("guardrail_config", "max_input_chars", value || 0)
                    }
                  />
                </Form.Item>
                <Form.Item
                  label="禁止输入的匹配规则"
                  help="每行一个正则表达式，留空表示不额外限制。"
                >
                  <Input.TextArea
                    rows={3}
                    value={(
                      cfg.guardrail_config.blocked_input_patterns || []
                    ).join("\n")}
                    onChange={(e) =>
                      patch(
                        "guardrail_config",
                        "blocked_input_patterns",
                        e.target.value.split("\n").filter(Boolean),
                      )
                    }
                  />
                </Form.Item>
                <Form.Item
                  label="输出脱敏匹配规则"
                  help="每行一个正则表达式，匹配内容会被脱敏。"
                >
                  <Input.TextArea
                    rows={3}
                    value={(
                      cfg.guardrail_config.redact_output_patterns || []
                    ).join("\n")}
                    onChange={(e) =>
                      patch(
                        "guardrail_config",
                        "redact_output_patterns",
                        e.target.value.split("\n").filter(Boolean),
                      )
                    }
                  />
                </Form.Item>
              </Panel>
            </Form>
          </div>
          <DebugPanel
            tenant={tenant}
            appID={appID}
            principal={principal}
            draft={workspace.draft}
            dirty={dirty}
            onSave={save}
            onError={fail}
          />
        </div>
      ) : tab === "versions" ? (
        <div className="workbench-content">
          <div className="section-title">
            <div>
              <h2>不可变版本列表</h2>
              <Button
                disabled={!canWrite}
                onClick={() => {
                  setRollout({ ...workspace.app.rollout_policy });
                  setRolloutReport(null);
                  setRolloutOpen(true);
                }}
              >
                灰度策略
              </Button>
              <p className="muted">
                版本不可变。旧会话固定原版本，发布或回滚只影响新会话。
              </p>
            </div>
          </div>
          <Table
            rowKey="revision_id"
            dataSource={versions}
            pagination={false}
            columns={[
              {
                title: "版本",
                render: (_, r: Revision) => (
                  <Space>
                    <strong>v{r.revision_no}</strong>
                    {r.revision_id === workspace.app.stable_revision_id && (
                      <Tag color="success">稳定版本</Tag>
                    )}
                  </Space>
                ),
              },
              { title: "创建人", dataIndex: "created_by" },
              { title: "时间", render: (_, r: Revision) => date(r.created_at) },
              {
                title: "",
                render: (_, r: Revision) => (
                  <Button type="link" onClick={() => setView(r)}>
                    查看配置
                  </Button>
                ),
              },
            ]}
          />
          {versionsNext && (
            <Button
              onClick={async () => {
                try {
                  const data = await api<Page<Revision>>("catalog/list", {
                    tenant_id: tenant,
                    app_id: appID,
                    kind: "revisions",
                    limit: 100,
                    after: versionsNext,
                  });
                  setVersions((old) =>
                    [
                      ...new Map(
                        [...old, ...data.items].map((v) => [v.revision_id, v]),
                      ).values(),
                    ].sort((a, b) => b.revision_no - a.revision_no),
                  );
                  setVersionsNext(data.next || "");
                } catch (e) {
                  fail(e);
                }
              }}
            >
              加载更多版本
            </Button>
          )}
          <ActivityPanel
            key={reload}
            kind="releases"
            tenant={tenant}
            appID={appID}
          />
        </div>
      ) : tab === "runs" ? (
        <div className="workbench-content">
          <RunsPage tenant={tenant} appID={appID} principal={principal} />
        </div>
      ) : tab === "jobs" ? (
        <div className="workbench-content">
          <ActivityPanel kind="jobs" tenant={tenant} appID={appID} />
        </div>
      ) : (
        <ChannelSummary tenant={tenant} appID={appID} />
      )}
      <Modal
        title="草稿已被其他操作更新"
        width={850}
        open={!!conflict}
        onCancel={() => setConflict(null)}
        okText="应用合并，继续编辑"
        onOk={() => {
          if (!conflict) return;
          const merged = structuredClone(conflict.data.config);
          for (const key of mergeKeys)
            (merged as any)[key] = structuredClone((cfg as any)[key]);
          setWorkspace({ ...workspace, draft: conflict });
          setCfg(merged);
          setConflict(null);
          setError("");
          setReport(null);
          message.info(
            "合并仅应用到当前编辑器，请检查后保存；尚未修改服务器草稿",
          );
        }}
      >
        <Alert
          type="warning"
          showIcon
          title={`服务器草稿已更新至 v${conflict?.version}`}
          description="请选择哪些配置保留你的修改，未勾选的部分使用服务器版本。应用合并不会直接写入服务器，也不会发布。"
        />
        {conflict && (
          <Collapse
            items={Object.keys(sections)
              .filter(
                (key) =>
                  canonical((cfg as any)[key]) !==
                  canonical((conflict.data.config as any)[key]),
              )
              .map((key) => ({
                key,
                label: sections[key],
                children: (
                  <>
                    <Checkbox
                      checked={mergeKeys.includes(key)}
                      onChange={(event) =>
                        setMergeKeys((old) =>
                          event.target.checked
                            ? [...old, key]
                            : old.filter((k) => k !== key),
                        )
                      }
                    >
                      保留我的这部分修改
                    </Checkbox>
                    <div className="form-grid">
                      <div>
                        <h4>服务器最新版本</h4>
                        <pre className="json-view">
                          {JSON.stringify(
                            (conflict.data.config as any)[key],
                            null,
                            2,
                          )}
                        </pre>
                      </div>
                      <div>
                        <h4>我的未保存修改</h4>
                        <pre className="json-view">
                          {JSON.stringify((cfg as any)[key], null, 2)}
                        </pre>
                      </div>
                    </div>
                  </>
                ),
              }))}
          />
        )}
      </Modal>
      <Drawer
        title="高级配置"
        open={advanced}
        onClose={() => setAdvanced(false)}
        size="large"
        extra={
          <Button
            type="primary"
            disabled={!canWrite}
            onClick={() => {
              try {
                const parsed = JSON.parse(raw);
                if (
                  !parsed ||
                  Array.isArray(parsed) ||
                  typeof parsed !== "object"
                )
                  throw new Error("配置必须是对象");
                for (const key of Object.keys(sections)) {
                  if (
                    !parsed[key] ||
                    typeof parsed[key] !== "object" ||
                    Array.isArray(parsed[key])
                  )
                    throw new Error(key + " 必须是对象");
                }
                setCfg({
                  ...cfg,
                  ...Object.fromEntries(
                    Object.keys(sections).map((key) => [key, parsed[key]]),
                  ),
                });
                setReport(null);
                setAdvanced(false);
              } catch (e) {
                message.error(errorText(e));
              }
            }}
          >
            应用到草稿编辑器
          </Button>
        }
      >
        <Alert
          type="info"
          title="此处用于 MCP、Embedding、Guardrail 等高级设置"
          description="应用修改不会保存或发布。不要粘贴密钥原文。"
        />
        <Input.TextArea
          className="code-input advanced-code"
          value={raw}
          readOnly={!canWrite}
          onChange={(e) => setRaw(e.target.value)}
          rows={28}
        />
      </Drawer>
      <Modal
        title="发布新版本"
        width={800}
        open={publishOpen}
        onCancel={() => setPublishOpen(false)}
        onOk={publish}
        okText="确认发布"
        confirmLoading={busy === "publish"}
      >
        <p>
          发布应用 <strong>{workspace.app.name}</strong>{" "}
          的当前草稿。版本编号由服务端自动分配。
        </p>
        <h4>相对稳定版本的变化</h4>
        {changed.length ? (
          <div className="change-list">
            {changed.map((key) => (
              <div key={key}>
                <Icon name="check" />
                <span>{sections[key]}</span>
                <Tag color="purple">已修改</Tag>
              </div>
            ))}
          </div>
        ) : (
          <Alert type="info" title="配置与当前稳定版本一致" />
        )}
        {changed.length > 0 && (
          <Collapse
            size="small"
            items={changed.map((key) => ({
              key,
              label: sections[key] + " · 查看具体变更",
              children: (
                <div className="form-grid">
                  <div>
                    <strong>当前稳定版本</strong>
                    <pre className="json-view">
                      {JSON.stringify(
                        (workspace.stable as any)?.[key] || {},
                        null,
                        2,
                      )}
                    </pre>
                  </div>
                  <div>
                    <strong>待发布草稿</strong>
                    <pre className="json-view">
                      {JSON.stringify((cfg as any)[key], null, 2)}
                    </pre>
                  </div>
                </div>
              ),
            }))}
          />
        )}
        {report && <ValidationView report={report} />}
        {readiness && (
          <Alert
            showIcon
            type={readiness.state === "passed" ? "info" : "warning"}
            title={
              readiness.state === "passed"
                ? "当前配置已有隔离调试记录"
                : "当前配置尚未确认调试成功"
            }
            description={readiness.description}
          />
        )}
        <Alert
          type="warning"
          showIcon
          title="旧会话不会自动切换"
          description="发布后，新会话使用新版本；已有 Telegram 私聊和 Topic 保留原版本。"
        />
      </Modal>
      <Modal
        title="应用设置"
        open={settingsOpen}
        onCancel={() => setSettingsOpen(false)}
        confirmLoading={busy === "settings"}
        onOk={async () => {
          setBusy("settings");
          try {
            const app = await api<AgentApp>("apps/settings", {
              tenant_id: tenant,
              app_id: appID,
              name: settings.name,
              description: settings.description || "",
              status: settings.status,
              expected_version: workspace.app.version,
            });
            setWorkspace((old) => (old ? { ...old, app } : old));
            setSettingsOpen(false);
            message.success("应用设置已保存");
          } catch (e) {
            fail(e);
          } finally {
            setBusy("");
          }
        }}
      >
        <Form layout="vertical">
          <Form.Item label="应用名称">
            <Input
              value={settings.name}
              maxLength={100}
              onChange={(e) =>
                setSettings({ ...settings, name: e.target.value })
              }
            />
          </Form.Item>
          <Form.Item label="用途说明">
            <Input.TextArea
              rows={3}
              value={settings.description}
              maxLength={500}
              onChange={(e) =>
                setSettings({ ...settings, description: e.target.value })
              }
            />
          </Form.Item>
          <Form.Item label="应用状态">
            <Select
              value={settings.status}
              onChange={(value) => setSettings({ ...settings, status: value })}
              options={[
                { value: "active", label: "启用" },
                { value: "disabled", label: "停用" },
              ]}
            />
          </Form.Item>
        </Form>
        <Alert
          type="info"
          title="停用会阻止新的业务请求"
          description="不会自动取消已接受的执行，也不会撤回已经发送的消息。"
        />
      </Modal>
      <Modal
        title="灰度发布策略"
        open={rolloutOpen}
        onCancel={() => setRolloutOpen(false)}
        confirmLoading={busy === "rollout"}
        onOk={async () => {
          setBusy("rollout");
          setRolloutReport(null);
          try {
            const app = await api<AgentApp>("apps/rollout", {
              tenant_id: tenant,
              app_id: appID,
              expected_version: workspace.app.version,
              rollout_policy: {
                ...rollout,
                canary_percent: rollout.canary_percent || 0,
                salt: rollout.salt || "default",
              },
            });
            setWorkspace((old) => (old ? { ...old, app } : old));
            setRolloutOpen(false);
            message.success("灰度策略已保存");
          } catch (e) {
            if (e instanceof APIError && e.validation)
              setRolloutReport(e.validation);
            message.error(errorText(e));
          } finally {
            setBusy("");
          }
        }}
      >
        <Form layout="vertical">
          <Form.Item label="灰度候选版本">
            <Select
              allowClear
              value={rollout.canary_revision_id || undefined}
              onChange={(value) =>
                setRollout({ ...rollout, canary_revision_id: value || "" })
              }
              options={versions.map((v) => ({
                value: v.revision_id,
                label: "v" + v.revision_no + " · " + v.agent_config.name,
              }))}
            />
          </Form.Item>
          <Form.Item label="新会话进入灰度版本的比例（%）">
            <InputNumber
              min={0}
              max={100}
              value={rollout.canary_percent || 0}
              onChange={(value) =>
                setRollout({ ...rollout, canary_percent: value || 0 })
              }
            />
          </Form.Item>
        </Form>
        {rolloutReport && <ValidationView report={rolloutReport} />}
        <Alert
          type="info"
          title="0% 关闭灰度；已有会话保持原版本"
          description="保存时重新检查候选版本的权限和依赖；不会自动扩大工具预算。"
        />
      </Modal>
      <Drawer
        title={view ? "版本 v" + view.revision_no : ""}
        open={!!view}
        onClose={() => setView(null)}
        size="large"
        extra={
          view &&
          canWrite && (
            <Space>
              <Button onClick={() => useVersion(view)}>编辑为草稿</Button>
              <Button
                danger
                disabled={view.revision_id === workspace.app.stable_revision_id}
                onClick={() => rollback(view)}
              >
                回滚到此版本
              </Button>
            </Space>
          )
        }
      >
        {view && (
          <pre className="json-view">{JSON.stringify(view, null, 2)}</pre>
        )}
      </Drawer>
    </>
  );
}

function ChannelSummary({ tenant, appID }: { tenant: string; appID: string }) {
  const [rows, setRows] = useState<Dict[]>([]);
  const [error, setError] = useState("");
  useEffect(() => {
    let live = true;
    api<Page<Dict>>("catalog/list", {
      kind: "channels",
      tenant_id: tenant,
      app_id: appID,
      limit: 100,
    })
      .then((data) => {
        if (live) setRows(data.items);
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant, appID]);
  return (
    <div className="workbench-content">
      <div className="section-title">
        <h2>机器人</h2>
        <Button onClick={() => navigate("channels")}>连接机器人</Button>
      </div>
      {error && <Failure error={error} />}
      <Table
        rowKey="channel_binding_id"
        dataSource={rows}
        pagination={false}
        columns={[
          {
            title: "通道",
            render: (_, r: Dict) =>
              channelLabels[r.channel_type] || r.channel_type,
          },
          { title: "账号", dataIndex: "account_id" },
          {
            title: "状态",
            render: (_, r: Dict) => <Status value={r.status} />,
          },
          { title: "更新时间", render: (_, r: Dict) => date(r.updated_at) },
        ]}
        locale={{
          emptyText: (
            <Blank title="尚未绑定业务通道。可以先在右侧完成调试，再配置接入。" />
          ),
        }}
      />
    </div>
  );
}
