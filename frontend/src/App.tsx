import { type FormEvent, useEffect, useState } from "react";
import { Boxes, Building2, ChevronDown, Database, LayoutDashboard, LogIn, LogOut, MessageSquare, Network, Rocket, ServerCog, Plug, ShieldCheck } from "lucide-react";
import { APIError, api, type Identity } from "./api";
import { AsyncState } from "./AsyncState";
import { TenantsPage } from "./TenantsPage";
import { AppsPage } from "./AppsPage";
import { DeploymentsPage } from "./DeploymentsPage";
import { RuntimePage } from "./RuntimePage";
import { DataPage } from "./DataPage";
import { ChatPage } from "./ChatPage";
import { ChannelsPage } from "./ChannelsPage";
import { GovernancePage } from "./GovernancePage";

const navigation = [
  { label: "概览", icon: LayoutDashboard },
  { label: "租户", icon: Building2 },
  { label: "Agent 应用", icon: Boxes },
  { label: "部署", icon: Rocket },
  { label: "Chat", icon: MessageSquare },
  { label: "IM 通道", icon: Plug },
  { label: "运行节点", icon: Network },
  { label: "数据管理", icon: Database },
  { label: "治理观测", icon: ShieldCheck },
];

export default function App() {
  const [identity, setIdentity] = useState<Identity>();
  const [failed, setFailed] = useState(false);
  const [authRequired, setAuthRequired] = useState(false);
  const [active, setActive] = useState("概览");
  const [chatAppID, setChatAppID] = useState<string>();

  const load = () => {
    setFailed(false);
    setAuthRequired(false);
    api.identity().then(setIdentity).catch((error) => error instanceof APIError && error.status === 401 ? setAuthRequired(true) : setFailed(true));
  };
  useEffect(load, []);
  useEffect(() => { const expired = () => { setIdentity(undefined); setAuthRequired(true); }; window.addEventListener("trpc-auth-required", expired); return () => window.removeEventListener("trpc-auth-required", expired); }, []);

  const switchTenant = async (tenantID: string) => {
    try {
      setIdentity(await api.switchTenant(tenantID));
    } catch {
      setFailed(true);
    }
  };
  const openChat = (appID: string) => {
    setChatAppID(appID);
    setActive("Chat");
  };

  if (authRequired) return <LoginPanel onAuthenticated={(authenticated) => { setIdentity(authenticated); setAuthRequired(false); }} />;
  if (failed) return <main className="centered"><AsyncState kind="error" retry={load} /></main>;
  if (!identity) return <main className="centered"><AsyncState kind="loading" /></main>;

  return (
    <div className="app-shell">
      <aside>
        <div className="brand"><ServerCog aria-hidden="true" /><span>Agent Platform</span></div>
        <nav aria-label="主导航">
          {navigation.map(({ label, icon: Icon }) => (
            <button key={label} className={active === label ? "active" : ""} onClick={() => setActive(label)}>
              <Icon aria-hidden="true" /><span>{label}</span>
            </button>
          ))}
        </nav>
      </aside>
      <section className="workspace">
        <header>
          <div><span className="eyebrow">管理控制台</span><h1>{active}</h1></div>
          <label className="tenant-switcher">
            <span>当前租户</span>
            <div>
              <select value={identity.active_tenant_id} onChange={(event) => void switchTenant(event.target.value)}>
                {identity.assignments.map((assignment) => <option value={assignment.tenant_id} key={assignment.tenant_id}>{assignment.tenant_name}</option>)}
              </select>
              <ChevronDown aria-hidden="true" />
            </div>
          </label>
          <div className="identity"><span>{identity.name}</span><small>{identity.active_role}</small></div>
          {identity.auth_mode === "production" && <button className="icon-button" title="退出登录" aria-label="退出登录" onClick={() => void api.logout().then(() => { setIdentity(undefined); setAuthRequired(true); })}><LogOut aria-hidden="true" /></button>}
        </header>
        <main>
          <div className="page-heading"><div><h2>{active}</h2><p>由后端提供的实时平台数据</p></div></div>
          {active === "租户" ? <TenantsPage identity={identity} identityChanged={load} /> : active === "Agent 应用" ? <AppsPage identity={identity} onOpenChat={openChat} /> : active === "部署" ? <DeploymentsPage identity={identity} onOpenChat={openChat} /> : active === "Chat" ? <ChatPage key={identity.active_tenant_id} identity={identity} initialAppID={chatAppID} /> : active === "IM 通道" ? <ChannelsPage identity={identity} /> : active === "运行节点" ? <RuntimePage key={identity.active_tenant_id} identity={identity} /> : active === "数据管理" ? <DataPage key={identity.active_tenant_id} identity={identity} /> : active === "治理观测" ? <GovernancePage key={identity.active_tenant_id} identity={identity} /> : <AsyncState kind="empty" />}
        </main>
      </section>
    </div>
  );
}

function LoginPanel({ onAuthenticated }: { onAuthenticated: (identity: Identity) => void }) {
  const [token, setToken] = useState("");
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault(); setError("");
    try { const identity = await api.login(token); setToken(""); onAuthenticated(identity); }
    catch { setToken(""); setError("身份验证失败"); }
  };
  return <main className="login-shell"><form className="login-panel" onSubmit={(event) => void submit(event)}><ServerCog aria-hidden="true" /><h1>Agent Platform</h1><label>Identity Token<input type="password" autoComplete="off" value={token} onChange={(event) => setToken(event.target.value)} /></label>{error && <div className="inline-error" role="alert">{error}</div>}<button className="primary" disabled={!token}><LogIn aria-hidden="true" />登录</button></form></main>;
}
