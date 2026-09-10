import { useEffect, useState } from "react";
import { App, Button, Input, Select, Skeleton, Tag } from "antd";
import {
  api,
  cancelRequests,
  errorText,
  login,
  logout,
  restoreSession,
} from "./api";
import { Brand, Failure, Icon, PageHeading, PageBoundary } from "./components";
import type { AgentApp, Page, Principal, Tenant } from "./types";
import { roleName, writable } from "./types";
import { AgentList, ResourcePage, RunsPage, SystemPage } from "./pages";
import { Workbench } from "./Workbench";
import { GettingStarted } from "./GettingStarted";
import { ModelConnections } from "./ModelConnections";

const navigation = [
  { id: "start", label: "上手引导", icon: "arrow" },
  { id: "overview", label: "工作空间", icon: "grid" },
  { id: "models", label: "模型连接", icon: "settings" },
  { id: "agents", label: "Agent 应用", icon: "agent" },
  { id: "resources", label: "资源中心", icon: "layers" },
  { id: "channels", label: "通道接入", icon: "channel" },
  { id: "runs", label: "运行记录", icon: "activity" },
  { id: "system", label: "系统状态", icon: "settings" },
];
function route() {
  const [section = "overview", id = ""] = location.hash
    .replace(/^#\/?/, "")
    .split("/");
  try {
    return { section: section || "overview", id: decodeURIComponent(id) };
  } catch {
    return { section: "overview", id: "" };
  }
}
export function navigate(section: string, id = "") {
  location.hash = "/" + section + (id ? "/" + encodeURIComponent(id) : "");
}

function SignIn({ onLogin }: { onLogin: (p: Principal) => void }) {
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const p = await login(token);
      setToken("");
      onLogin(p);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="login-page">
      <div className="login-story">
        <Brand />
        <div className="login-copy">
          <div className="eyebrow">YOUR AGENTS. ONE WORKSPACE.</div>
          <h1>
            从一次对话，
            <br />
            到真正可用的 Agent。
          </h1>
          <p>
            配置模型与知识，调试工具与技能，
            <br />
            以可追溯的版本交付到业务通道。
          </p>
          <div className="login-orbit">
            <span className="orbit-node">
              <Icon name="agent" size={36} />
            </span>
            <span className="orbit-chip one">
              <Icon name="layers" /> Knowledge
            </span>
            <span className="orbit-chip two">
              <Icon name="code" /> Skills
            </span>
            <span className="orbit-chip three">
              <Icon name="channel" /> Channels
            </span>
          </div>
        </div>
        <div className="login-footer">
          Built with tRPC-Agent-Go <span>自托管 · 多租户 · 受控执行</span>
        </div>
      </div>
      <div className="login-form-wrap">
        <form onSubmit={submit} className="login-form">
          <Tag color="purple">管理控制台</Tag>
          <h2>进入你的工作空间</h2>
          <p>使用部署者分配的管理凭据登录。登录后刷新页面无需再次输入。</p>
          <label>Admin Token</label>
          <Input.Password
            autoComplete="off"
            size="large"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="输入你的管理凭据"
            required
          />
          {error && <Failure error={error} />}
          <Button
            htmlType="submit"
            type="primary"
            size="large"
            block
            loading={busy}
            disabled={!token.trim()}
          >
            进入工作空间 <Icon name="arrow" />
          </Button>
          <div className="login-hint">
            <Icon name="check" /> 凭据不保存到浏览器本地存储；登录会话最长 8
            小时。
          </div>
          <div className="login-help">
            使用体验版 Compose？在终端执行{" "}
            <code>
              docker compose --env-file deploy/compose/demo.env.example -f
              compose.demo.yaml exec platform trpc-init -show-token
            </code>{" "}
            获取本机生成的凭据，请勿分享或截图。
            <br />
            首次使用？凭据来自服务端的 <code>TRPC_AGENT_ADMIN_TOKEN</code>{" "}
            或已配置的 Principal。它不是模型或机器人 API Key。
          </div>
        </form>
      </div>
    </div>
  );
}

export function ConsoleApp() {
  const { message } = App.useApp();
  const [principal, setPrincipal] = useState<Principal | null>(null);
  const [boot, setBoot] = useState(true);
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [tenant, setTenant] = useState("");
  const [current, setCurrent] = useState(route());
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [tenantsLoaded, setTenantsLoaded] = useState(false);
  useEffect(() => {
    let live = true;
    restoreSession()
      .then((p) => {
        if (live) setPrincipal(p);
      })
      .catch((e) => {
        if (live && e.status !== 401) setError(errorText(e));
      })
      .finally(() => {
        if (live) setBoot(false);
      });
    const expired = () => {
      cancelRequests();
      setPrincipal(null);
      setTenants([]);
      setTenant("");
      setTenantsLoaded(false);
    };
    const change = () => setCurrent(route());
    window.addEventListener("session-expired", expired);
    window.addEventListener("hashchange", change);
    return () => {
      live = false;
      window.removeEventListener("session-expired", expired);
      window.removeEventListener("hashchange", change);
    };
  }, []);
  useEffect(() => {
    if (!principal) return;
    let live = true;
    setError("");
    setTenantsLoaded(false);
    api<Page<Tenant>>("catalog/list", { kind: "tenants", limit: 100 })
      .then((data) => {
        if (live) {
          setTenants(data.items);
          setTenant((old) =>
            data.items.some((t) => t.tenant_id === old)
              ? old
              : data.items[0]?.tenant_id || "",
          );
        }
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      })
      .finally(() => {
        if (live) setTenantsLoaded(true);
      });
    return () => {
      live = false;
    };
  }, [principal, refresh]);
  if (boot)
    return (
      <div className="boot">
        <Brand />
        <p>正在连接工作空间…</p>
      </div>
    );
  if (!principal)
    return (
      <SignIn
        onLogin={(p) => {
          setPrincipal(p);
          setError("");
        }}
      />
    );
  const chosen = tenants.find((t) => t.tenant_id === tenant);
  const active = navigation.find((n) => n.id === current.section);
  return (
    <div className="shell">
      <aside className="sidebar">
        <Brand small />
        <div className="workspace-label">WORKSPACE</div>
        <nav>
          {navigation.map((item) => (
            <button
              key={item.id}
              onClick={() => navigate(item.id)}
              className={current.section === item.id ? "active" : ""}
            >
              <Icon name={item.icon} />
              <span>{item.label}</span>
              {current.section === item.id && <i />}
            </button>
          ))}
          <div className="nav-divider" />
          <button
            onClick={() => navigate("tenants")}
            className={current.section === "tenants" ? "active" : ""}
          >
            <Icon name="users" />
            <span>租户与策略</span>
          </button>
        </nav>
        <div className="sidebar-note">
          <span className="small-dot" /> 配置与运行分离
          <p>
            草稿不会改变线上 Agent；
            <br />
            发布前检查，执行中留痕。
          </p>
        </div>
        <div className="profile">
          <span className="avatar">
            {principal.name.slice(0, 1).toUpperCase()}
          </span>
          <div>
            <strong>{principal.name}</strong>
            <small>{roleName(principal.role)}</small>
          </div>
          <button
            title="退出登录"
            onClick={async () => {
              try {
                await logout();
                setPrincipal(null);
                setTenant("");
              } catch (e) {
                message.error(errorText(e));
              }
            }}
          >
            <Icon name="arrow" />
          </button>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            工作空间 <span>/</span>{" "}
            <strong>{active?.label || "租户与策略"}</strong>
            {current.id && (
              <>
                <span>/</span> Agent 工作台
              </>
            )}
          </div>
          <div className="top-actions">
            <span className="tenant-caption">当前租户</span>
            <Select
              value={tenant || undefined}
              placeholder="选择租户"
              style={{ minWidth: 190 }}
              options={tenants.map((t) => ({
                value: t.tenant_id,
                label: t.display_name || t.tenant_id,
              }))}
              onChange={(id) => {
                cancelRequests();
                setTenant(id);
                navigate("agents");
              }}
            />
            <Tag bordered={false}>SELF-HOSTED</Tag>
          </div>
        </header>
        <main
          key={tenant}
          className={
            current.section === "agents" && current.id
              ? "workbench-main"
              : "page-main"
          }
        >
          <PageBoundary key={tenant + "/" + current.section + "/" + current.id}>
            {error && (
              <Failure error={error} retry={() => setRefresh((v) => v + 1)} />
            )}{" "}
            {!tenantsLoaded ? (
              <Skeleton active />
            ) : current.section === "start" ||
              (!tenant && current.section !== "tenants") ? (
              <GettingStarted
                tenant={tenant}
                principal={principal}
                onTenantCreated={(created) => {
                  setTenants((old) => [
                    ...old.filter((t) => t.tenant_id !== created.tenant_id),
                    created,
                  ]);
                  setTenant(created.tenant_id);
                  navigate("start");
                }}
              />
            ) : current.section === "models" ? (
              <ModelConnections tenant={tenant} principal={principal} />
            ) : current.section === "agents" && current.id ? (
              <Workbench
                key={tenant + current.id}
                tenant={tenant}
                appID={current.id}
                principal={principal}
              />
            ) : current.section === "agents" ? (
              <AgentList tenant={tenant} principal={principal} />
            ) : current.section === "overview" ? (
              <Overview
                tenant={tenant}
                name={chosen?.display_name || tenant}
                principal={principal}
              />
            ) : current.section === "resources" ? (
              <ResourcePage tenant={tenant} principal={principal} />
            ) : current.section === "channels" ? (
              <ResourcePage
                tenant={tenant}
                principal={principal}
                initial="channels"
              />
            ) : current.section === "runs" ? (
              <RunsPage tenant={tenant} principal={principal} />
            ) : current.section === "system" ? (
              <SystemPage tenant={tenant} />
            ) : (
              <ResourcePage
                tenant={tenant}
                principal={principal}
                initial="tenants"
                onChanged={() => setRefresh((v) => v + 1)}
              />
            )}
          </PageBoundary>
        </main>
      </div>
    </div>
  );
}

function Overview({
  tenant,
  name,
  principal,
}: {
  tenant: string;
  name: string;
  principal: Principal;
}) {
  const [apps, setApps] = useState<AgentApp[]>([]);
  const [counts, setCounts] = useState<{
    channels: number;
    backends: number;
  } | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let live = true;
    Promise.all([
      api<Page<AgentApp>>("catalog/list", {
        tenant_id: tenant,
        kind: "apps",
        limit: 100,
      }),
      api<Page<unknown>>("catalog/list", {
        tenant_id: tenant,
        kind: "channels",
        limit: 100,
      }),
      api<Page<unknown>>("catalog/list", {
        tenant_id: tenant,
        kind: "backends",
        limit: 100,
      }),
    ])
      .then(([a, c, b]) => {
        if (live) {
          setApps(a.items);
          setCounts({ channels: c.items.length, backends: b.items.length });
        }
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant]);
  return (
    <>
      <PageHeading
        eyebrow="WORKSPACE OVERVIEW"
        title={name + "，一切从这里开始"}
        subtitle="在同一个工作空间中配置、调试和发布 Agent。"
        action={
          <Button
            type="primary"
            icon={<Icon name="plus" />}
            disabled={!writable(principal)}
            onClick={() => navigate("agents")}
          >
            管理 Agent
          </Button>
        }
      />
      {error && <Failure error={error} />}
      <div className="stats-grid">
        {[
          {
            title: "Agent 应用",
            value: counts ? apps.length : null,
            caption: "当前加载的应用",
            icon: "agent",
          },
          {
            title: "已发布应用",
            value: counts
              ? apps.filter((a) => a.stable_revision_id).length
              : null,
            caption: "拥有稳定版本",
            icon: "check",
          },
          {
            title: "接入通道",
            value: counts ? counts.channels : null,
            caption: "已注册的通道绑定",
            icon: "channel",
          },
          {
            title: "数据后端",
            value: counts ? counts.backends : null,
            caption: "已注册的存储绑定",
            icon: "layers",
          },
        ].map((item) => (
          <div className="stat-card" key={item.title}>
            <div>
              <span>{item.title}</span>
              <strong>{item.value ?? "—"}</strong>
              <small>{item.caption}</small>
            </div>
            <span className="stat-icon">
              <Icon name={item.icon} size={23} />
            </span>
          </div>
        ))}
      </div>
      <div className="overview-banner">
        <div>
          <Tag color="purple">AGENT WORKBENCH</Tag>
          <h2>先在工作台调试，再发布到业务。</h2>
          <p>
            常用配置不必编写
            JSON。保留草稿，查看工具执行与审批，确认变更后再发布。
          </p>
          <Button onClick={() => navigate("agents")}>
            进入 Agent 工作台 <Icon name="arrow" />
          </Button>
        </div>
        <div className="banner-flow">
          <span>
            <Icon name="settings" /> 配置
          </span>
          <i />
          <span>
            <Icon name="channel" /> 调试
          </span>
          <i />
          <span>
            <Icon name="check" /> 发布
          </span>
        </div>
      </div>
      <div className="section-title">
        <h2>最近更新的 Agent</h2>
        <Button type="text" onClick={() => navigate("agents")}>
          查看全部 <Icon name="arrow" />
        </Button>
      </div>
      {!counts && !error ? (
        <Skeleton active />
      ) : (
        <AgentList
          tenant={tenant}
          principal={principal}
          compact
          initialApps={apps}
        />
      )}
    </>
  );
}
