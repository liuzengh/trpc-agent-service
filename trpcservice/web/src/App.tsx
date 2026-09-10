import { useEffect, useMemo, useState } from "react";
import {
  ArrowRight,
  Check,
  CheckCircle,
  ChatsCircle,
  Copy,
  Cpu,
  Plus,
  TelegramLogo,
  UsersThree,
  WechatLogo,
  X,
} from "@phosphor-icons/react";

type Channel = "telegram" | "wecom";
type Tenant = {
  id: string;
  name: string;
  updated: string;
  initials: string;
  version: number;
  defaultAgentAppID?: string;
  defaultBackendProfileID?: string;
  auditRetentionDays: number;
  logMaskingLevel: string;
  traceSamplingRate: number;
};
type Connection = { binding_id: string; channel: string; bot_id: string; url?: string; ready: boolean };
type AgentDetails = { ready: boolean; appID?: string; name?: string; model?: string; endpoint?: string; instruction?: string; status?: string; version?: number };

const channelDetails: Record<Channel, { label: string; description: string; tone: string; icon: typeof TelegramLogo }> = {
  telegram: { label: "Telegram", description: "A private bot for quick, direct conversations.", tone: "telegram", icon: TelegramLogo },
  wecom: { label: "WeCom", description: "Meet your team where work already happens.", tone: "wecom", icon: WechatLogo },
};

function toTenant(value: unknown): Tenant | null {
  if (!value || typeof value !== "object") return null;
  const item = value as Record<string, unknown>;
  const id = String(item.tenant_id ?? item.TenantID ?? "");
  const name = String(item.display_name ?? item.DisplayName ?? item.tenant_key ?? item.TenantKey ?? "");
  if (!id || !name) return null;
  return {
    id,
    name,
    updated: String(item.status ?? item.Status ?? "Active"),
    initials: name.slice(0, 2).toUpperCase(),
    version: Number(item.version ?? item.Version ?? 1),
    defaultAgentAppID: String(item.default_agent_app_id ?? item.DefaultAgentAppID ?? "") || undefined,
    defaultBackendProfileID: String(item.default_backend_profile_id ?? item.DefaultBackendProfileID ?? "") || undefined,
    auditRetentionDays: Number(item.audit_retention_days ?? item.AuditRetentionDays ?? 90),
    logMaskingLevel: String(item.log_masking_level ?? item.LogMaskingLevel ?? "basic"),
    traceSamplingRate: Number(item.trace_sampling_rate ?? item.TraceSamplingRate ?? 1),
  };
}

function LoginPanel({ busy, error, onLogin }: { busy: boolean; error: string; onLogin: (username: string, credential: string) => Promise<void> }) {
  const [username, setUsername] = useState("");
  const [credential, setCredential] = useState("");
  return <div className="flex min-h-[100dvh] items-center justify-center bg-[#f6f7f9] p-5"><form className="w-full max-w-[400px] rounded-[18px] border border-[#e2e6eb] bg-white p-7 shadow-[0_18px_50px_rgba(28,39,54,0.07)]" onSubmit={(event) => { event.preventDefault(); void onLogin(username, credential); }}><div className="flex items-center gap-2.5"><span className="brand-mark">t</span><span className="text-[15px] font-semibold">trpc</span></div><h1 className="mt-8 text-[26px] font-semibold tracking-[-0.035em]">Sign in to continue</h1><p className="mt-2 text-[14px] leading-6 text-[#77818e]">Use your admin account or bearer token to configure tenants and channels.</p><label className="field-label mt-7">Username<input className="field-input" value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" /></label><label className="field-label mt-5">Password or admin token<input className="field-input" type="password" value={credential} onChange={(event) => setCredential(event.target.value)} autoComplete="current-password" /></label>{error && <p className="mt-4 text-[13px] text-[#ba3d48]" role="alert">{error}</p>}<button className="button-primary mt-7 w-full" disabled={busy || !username || !credential} type="submit">{busy ? "Signing in..." : "Sign in"}<ArrowRight size={16} weight="bold" /></button></form></div>;
}

function App() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [selectedTenant, setSelectedTenant] = useState<Tenant | null>(null);
  const [agent, setAgent] = useState<AgentDetails | null>(null);
  const [agents, setAgents] = useState<AgentDetails[]>([]);
  const [creatingAgent, setCreatingAgent] = useState(false);
  const [agentConfirmed, setAgentConfirmed] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [tenantName, setTenantName] = useState("");
  const [channel, setChannel] = useState<Channel | null>(null);
  const [connected, setConnected] = useState(false);
  const [copied, setCopied] = useState(false);
  const [bindingID, setBindingID] = useState("");
  const [connectionURL, setConnectionURL] = useState("");
  const [botID, setBotID] = useState("");
  const [secret, setSecret] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [adminToken, setAdminToken] = useState("");

  const authHeaders = (): Record<string, string> => adminToken ? { Authorization: `Bearer ${adminToken}` } : {};

  useEffect(() => { void loadSession(); }, []);

  async function loadSession() {
    const response = await fetch("/admin/auth/session", { credentials: "include" });
    if (!response.ok) { setAuthenticated(false); return; }
    setAuthenticated(true);
    const tenantsResponse = await fetch("/admin/v1/tenants?limit=100", { credentials: "include", headers: authHeaders() });
    if (!tenantsResponse.ok) { setError("Unable to load tenants."); return; }
    const payload = await tenantsResponse.json() as { data?: { items?: unknown[] } };
    const values = Array.isArray(payload.data?.items) ? payload.data.items.map(toTenant).filter(Boolean) as Tenant[] : [];
    setTenants(values);
  }

  async function login(username: string, credential: string) {
    setBusy(true); setError("");
    const response = await fetch("/admin/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username, password: credential }) });
    if (response.ok) {
      setAdminToken("");
      setBusy(false);
      await loadSession();
      return;
    }
    const tokenResponse = await fetch("/admin/v1/me", { credentials: "include", headers: { Authorization: `Bearer ${credential}` } });
    setBusy(false);
    if (!tokenResponse.ok) { setError("Login failed. Check your username/password or admin token."); return; }
    setAdminToken(credential);
    setAuthenticated(true);
    const tenantsResponse = await fetch("/admin/v1/tenants?limit=100", { credentials: "include", headers: { Authorization: `Bearer ${credential}` } });
    if (!tenantsResponse.ok) { setError("Unable to load tenants."); return; }
    const payload = await tenantsResponse.json() as { data?: { items?: unknown[] } };
    setTenants(Array.isArray(payload.data?.items) ? payload.data.items.map(toTenant).filter(Boolean) as Tenant[] : []);
  }

  const step = useMemo(() => {
    if (!selectedTenant) return 1;
    if (!agentConfirmed) return 2;
    if (!channel) return 3;
    return 4;
  }, [selectedTenant, agentConfirmed, channel]);

  async function selectTenant(tenant: Tenant) {
    setSelectedTenant(tenant);
    setAgent(null);
    setAgents([]);
    setCreatingAgent(false);
    setAgentConfirmed(false);
    setChannel(null);
    setError("");
    try {
      const listResponse = await fetch(`/admin/v1/tenants/${encodeURIComponent(tenant.id)}/apps?limit=100`, { credentials: "include", headers: authHeaders() });
      if (!listResponse.ok) throw new Error("apps");
      const listPayload = await listResponse.json() as { data?: { items?: Record<string, unknown>[] } };
      const listed = listPayload.data?.items ?? [];
      const summaries = listed.map((item) => ({ ready: false, appID: String(item.app_id ?? item.AppID ?? ""), name: String(item.display_name ?? item.DisplayName ?? "Agent"), status: String(item.status ?? item.Status ?? ""), version: Number(item.version ?? item.Version ?? 1) })).filter((item) => item.appID);
      setAgents(summaries);
      const targetID = tenant.defaultAgentAppID || summaries[0]?.appID;
      if (!targetID) { setAgent({ ready: false }); return; }
      await selectAgent(tenant, targetID, summaries);
    } catch {
      setAgent({ ready: false });
      setError("Agent configuration could not be loaded.");
    }
  }

  async function selectAgent(tenant: Tenant, appID: string, summaries = agents) {
    try {
      const appResponse = await fetch(`/admin/v1/tenants/${encodeURIComponent(tenant.id)}/apps/${encodeURIComponent(appID)}`, { credentials: "include", headers: authHeaders() });
      if (!appResponse.ok) throw new Error("app");
      const appPayload = await appResponse.json() as { data?: Record<string, unknown> };
      const app = appPayload.data ?? {};
      const revision = Number(app.current_revision ?? app.CurrentRevision ?? 0);
      const status = String(app.status ?? app.Status ?? "");
      const name = String(app.display_name ?? app.DisplayName ?? "Agent");
      if (!revision || status !== "active" || !tenant.defaultBackendProfileID) {
        setAgent({ ready: false, appID, name, status, version: Number(app.version ?? app.Version ?? 1) });
        return;
      }
      const revisionsResponse = await fetch(`/admin/v1/tenants/${encodeURIComponent(tenant.id)}/apps/${encodeURIComponent(appID)}/revisions?limit=100`, { credentials: "include", headers: authHeaders() });
      if (!revisionsResponse.ok) throw new Error("revision");
      const revisionsPayload = await revisionsResponse.json() as { data?: { items?: Record<string, unknown>[] } };
      const current = revisionsPayload.data?.items?.find((item) => Number(item.revision ?? item.Revision) === revision);
      const modelID = String(current?.model_profile_id ?? current?.ModelProfileID ?? "");
      let model = "Configured model";
      let endpoint = "";
      if (modelID) {
        const modelResponse = await fetch(`/admin/v1/tenants/${encodeURIComponent(tenant.id)}/models/${encodeURIComponent(modelID)}`, { credentials: "include", headers: authHeaders() });
        if (modelResponse.ok) {
          const modelPayload = await modelResponse.json() as { data?: Record<string, unknown> };
          const configuration = (modelPayload.data?.configuration ?? modelPayload.data?.Configuration ?? {}) as Record<string, unknown>;
          model = String(configuration.model ?? configuration.Model ?? model);
          endpoint = String(configuration.endpoint ?? configuration.Endpoint ?? "");
        }
      }
      setAgent({ ready: true, appID, name, model, endpoint, instruction: String(current?.instruction ?? current?.Instruction ?? ""), status, version: Number(app.version ?? app.Version ?? 1) });
    } catch {
      setAgent({ ready: false });
      setError("Agent configuration could not be loaded.");
    }
  }

  async function makeDefaultAgent(next: AgentDetails) {
    if (!selectedTenant || !next.appID || !selectedTenant.defaultBackendProfileID) return;
    setBusy(true); setError("");
    const response = await fetch(`/admin/v1/tenants/${encodeURIComponent(selectedTenant.id)}`, { method: "PATCH", credentials: "include", headers: { "Content-Type": "application/json", ...authHeaders() }, body: JSON.stringify({ expected_version: selectedTenant.version, display_name: selectedTenant.name, audit_retention_days: selectedTenant.auditRetentionDays, log_masking_level: selectedTenant.logMaskingLevel, trace_sampling_rate: selectedTenant.traceSamplingRate, default_agent_app_id: next.appID, default_backend_profile_id: selectedTenant.defaultBackendProfileID }) });
    const payload = await response.json().catch(() => ({})) as { data?: unknown };
    setBusy(false);
    if (!response.ok || !payload.data) { setError("Could not switch the default agent."); return; }
    const updated = toTenant(payload.data); if (updated) { setSelectedTenant(updated); setTenants((all) => all.map((item) => item.id === updated.id ? updated : item)); }
    setAgent(next); setAgentConfirmed(false);
  }

  async function suspendAgent() {
    if (!selectedTenant || !agent?.appID) return;
    setBusy(true); setError("");
    const response = await fetch(`/admin/v1/tenants/${encodeURIComponent(selectedTenant.id)}/apps/${encodeURIComponent(agent.appID)}/status`, { method: "POST", credentials: "include", headers: { "Content-Type": "application/json", ...authHeaders() }, body: JSON.stringify({ expected_version: agent.version ?? 1, next_status: "suspended", reason: "agent taken offline", correlation_id: `web-suspend-${Date.now().toString(36)}` }) });
    const payload = await response.json().catch(() => ({})) as { data?: { app?: Record<string, unknown> } };
    setBusy(false);
    if (!response.ok || !payload.data?.app) { setError("Could not take this agent offline."); return; }
    setAgents((all) => all.map((item) => item.appID === agent.appID ? { ...item, status: "suspended", ready: false } : item));
    setAgent({ ...agent, status: "suspended", ready: false });
  }

  const selectChannel = (next: Channel) => {
    setChannel(next);
    setConnected(false);
  };

  const createTenant = () => {
    const trimmed = tenantName.trim();
    if (!trimmed) return;
    void (async () => {
      setBusy(true); setError("");
      const key = trimmed.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 56) || "tenant";
      const response = await fetch("/admin/v1/tenants", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json", ...authHeaders() }, body: JSON.stringify({ tenant_key: key, display_name: trimmed }) });
      const payload = await response.json().catch(() => ({})) as { data?: unknown };
      setBusy(false);
      if (!response.ok || !payload.data) { setError("Tenant could not be created."); return; }
      const created = toTenant(payload.data);
      if (!created) { setError("Tenant response was invalid."); return; }
      setTenants((current) => [created, ...current]); setTenantName(""); setShowCreate(false); void selectTenant(created);
    })();
  };

  async function setupAgent(name: string, instruction: string, model: string, endpoint: string) {
    if (!selectedTenant) return;
    setBusy(true); setError("");
    const tenantID = encodeURIComponent(selectedTenant.id);
    const suffix = Date.now().toString(36);
    const request = async (path: string, body: unknown, method = "POST") => {
      const response = await fetch(path, { method, credentials: "include", headers: { "Content-Type": "application/json", ...authHeaders() }, body: JSON.stringify(body) });
      const payload = await response.json().catch(() => ({})) as { data?: Record<string, unknown>; error?: string };
      if (!response.ok || !payload.data) throw new Error(payload.error ?? "request_failed");
      return payload.data;
    };
    try {
      const app = await request(`/admin/v1/tenants/${tenantID}/apps`, { app_key: `assistant-${suffix}`, display_name: name, description: "Configured from the web setup" });
      const appID = String(app.app_id ?? app.AppID ?? "");
      const appVersion = Number(app.version ?? app.Version ?? 1);
      const modelResult = await request(`/admin/v1/tenants/${tenantID}/models`, { profile_key: `model-${suffix}`, display_name: `${name} model`, reason: "web agent setup", correlation_id: `web-model-${suffix}`, configuration: { provider: "openai", model, endpoint, secret_ref: "env/trpc-model-api-key" } });
      const modelProfile = (modelResult.profile ?? {}) as Record<string, unknown>;
      const modelID = String(modelProfile.profile_id ?? modelProfile.ProfileID ?? "");
      const backendResult = await request(`/admin/v1/tenants/${tenantID}/backends`, { profile_key: `backend-${suffix}`, display_name: `${name} storage`, reason: "web agent setup", correlation_id: `web-backend-${suffix}`, bindings: [{ capability: "session", provider: "inmemory" }] });
      const backendProfile = (backendResult.profile ?? {}) as Record<string, unknown>;
      const backendID = String(backendProfile.profile_id ?? backendProfile.ProfileID ?? "");
      if (!appID || !modelID || !backendID) throw new Error("invalid_setup_response");
      const draft = await request(`/admin/v1/tenants/${tenantID}/apps/${encodeURIComponent(appID)}/revisions`, { expected_app_version: appVersion, kind: "llm", schema_version: 1, configuration: { instruction, model_profile_id: modelID } });
      const revision = Number(draft.revision ?? draft.Revision ?? 0);
      const draftVersion = Number(draft.draft_version ?? draft.DraftVersion ?? 1);
      if (!revision) throw new Error("invalid_revision");
      await request(`/admin/v1/tenants/${tenantID}/apps/${encodeURIComponent(appID)}/revisions/${revision}/publish`, { expected_app_version: appVersion, expected_draft_version: draftVersion, reason: "activate agent", correlation_id: `web-publish-${suffix}` });
      const updatedValue = await request(`/admin/v1/tenants/${tenantID}`, { expected_version: selectedTenant.version, display_name: selectedTenant.name, audit_retention_days: selectedTenant.auditRetentionDays, log_masking_level: selectedTenant.logMaskingLevel, trace_sampling_rate: selectedTenant.traceSamplingRate, default_agent_app_id: appID, default_backend_profile_id: backendID }, "PATCH");
      const updated = toTenant(updatedValue);
      if (!updated) throw new Error("invalid_tenant_response");
      setTenants((current) => current.map((item) => item.id === updated.id ? updated : item));
      setSelectedTenant(updated);
      setAgent({ ready: true, appID, name, model, endpoint, instruction });
      setAgents((current) => [...current.filter((item) => item.appID !== appID), { ready: true, appID, name, model, endpoint, instruction, status: "active" }]);
      setCreatingAgent(false);
      setAgentConfirmed(true);
    } catch (reason) {
      const code = reason instanceof Error ? reason.message : "request_failed";
      setError(code === "invalid_request" ? "Check that the model and endpoint match the server environment." : "Agent setup could not be completed. Check the model settings and try again.");
    } finally {
      setBusy(false);
    }
  }

  const copyLink = async () => {
    if (!connectionURL) return;
    await navigator.clipboard?.writeText(connectionURL);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1800);
  };

  const connectChannel = async () => {
    if (!selectedTenant || !channel || !botID.trim() || !secret.trim()) { setError("Bot ID and secret are required."); return; }
    setBusy(true); setError("");
    const response = await fetch(`/admin/v1/tenants/${encodeURIComponent(selectedTenant.id)}/connections`, { method: "POST", credentials: "include", headers: { "Content-Type": "application/json", ...authHeaders() }, body: JSON.stringify({ channel: channel === "wecom" ? "wecom_aibot" : "telegram", bot_id: botID.trim(), secret }) });
    const payload = await response.json().catch(() => ({})) as { data?: Connection; error?: string };
    setBusy(false);
    if (!response.ok || !payload.data) { setError(payload.error === "agent_not_ready" ? "This tenant needs an active agent before a channel can connect." : "Connection failed. Check the Bot ID and secret."); return; }
    setBindingID(payload.data.binding_id); setConnectionURL(payload.data.url ?? ""); setConnected(payload.data.ready);
  };

  useEffect(() => {
    if (!selectedTenant || !bindingID || connected) return;
    let cancelled = false;
    const poll = async () => {
      const response = await fetch(`/admin/v1/tenants/${encodeURIComponent(selectedTenant.id)}/connections`, { credentials: "include", headers: authHeaders() });
      if (!response.ok || cancelled) return;
      const payload = await response.json() as { data?: Connection[] };
      const current = payload.data?.find((item) => item.binding_id === bindingID);
      if (current?.ready && !cancelled) { setConnected(true); setConnectionURL(current.url ?? ""); }
    };
    const timer = window.setInterval(() => void poll(), 1000); void poll();
    return () => { cancelled = true; window.clearInterval(timer); };
  }, [selectedTenant, bindingID, connected]);

  if (authenticated === null) return <div className="min-h-[100dvh] bg-[#f6f7f9]" />;
  if (!authenticated) return <LoginPanel busy={busy} error={error} onLogin={login} />;

  return (
    <div className="min-h-[100dvh] bg-[#f6f7f9] text-[#18202a]">
      <header className="mx-auto flex h-[72px] max-w-[1180px] items-center justify-between px-5 lg:px-8">
        <a className="flex items-center gap-2.5" href="#top" aria-label="trpc home">
          <span className="brand-mark">t</span>
          <span className="text-[15px] font-semibold tracking-[-0.01em]">trpc</span>
        </a>
        <div className="flex items-center gap-3 text-[13px] text-[#65707d]">
          <span className="hidden sm:inline">Setup takes about 2 minutes</span>
          <span className="h-1 w-1 rounded-full bg-[#aeb6c2]" aria-hidden="true" />
          <button className="font-medium text-[#394451] transition hover:text-[#1d5fd2]" type="button">Need help?</button>
        </div>
      </header>

      <main id="top" className="mx-auto grid max-w-[1180px] gap-12 px-5 pb-16 pt-9 lg:grid-cols-[minmax(0,1fr)_340px] lg:gap-20 lg:px-8 lg:pt-16">
        <section className="min-w-0">
          <div className="mb-10 max-w-[620px]">
            <p className="mb-4 text-[12px] font-semibold uppercase tracking-[0.16em] text-[#1d5fd2]">Welcome to trpc</p>
            <h1 className="text-[clamp(2.4rem,5vw,4.25rem)] font-semibold leading-[0.98] tracking-[-0.055em] text-[#18202a]">Put your agent<br className="hidden sm:block" /> where people talk.</h1>
            <p className="mt-5 max-w-[520px] text-[17px] leading-7 text-[#66717f]">Choose a tenant, connect one channel, and send the first message. We will take care of the rest.</p>
          </div>

          <div className="mb-8 flex items-center gap-2" aria-label={`Setup step ${step} of 4`}>
            {[{ n: 1, label: "Tenant" }, { n: 2, label: "Agent" }, { n: 3, label: "Channel" }, { n: 4, label: "First message" }].map((item, index) => (
              <div className="flex items-center gap-2" key={item.n}>
                <div className={`step-dot ${step >= item.n ? "step-dot-active" : ""}`}>{step > item.n ? <Check size={13} weight="bold" /> : item.n}</div>
                <span className={`hidden text-[12px] font-medium sm:inline ${step >= item.n ? "text-[#394451]" : "text-[#9aa3ae]"}`}>{item.label}</span>
                {index < 3 && <span className="mx-1 h-px w-5 bg-[#d9dee5] sm:w-9" />}
              </div>
            ))}
          </div>

          {!selectedTenant ? (
            <TenantPanel tenants={tenants} onSelect={(tenant) => void selectTenant(tenant)} onCreate={() => setShowCreate(true)} />
          ) : !agentConfirmed ? (
            <AgentPanel tenant={selectedTenant} agent={agent} agents={agents} creating={creatingAgent} busy={busy} error={error} onBack={() => { setSelectedTenant(null); setAgent(null); setError(""); }} onRegister={() => { setCreatingAgent(true); setError(""); }} onCancelRegister={() => setCreatingAgent(false)} onSelectAgent={(next) => void makeDefaultAgent(next)} onSuspend={() => void suspendAgent()} onContinue={() => setAgentConfirmed(true)} onSetup={setupAgent} />
          ) : !channel ? (
            <ChannelPanel tenant={selectedTenant} onBack={() => { setAgentConfirmed(false); setError(""); }} onSelect={selectChannel} />
          ) : (
            <ConnectPanel tenant={selectedTenant} channel={channel} connected={connected} copied={copied} busy={busy} botID={botID} secret={secret} error={error} connectionURL={connectionURL} onBotID={setBotID} onSecret={setSecret} onBack={() => { setChannel(null); setError(""); }} onConnect={connectChannel} onCopy={copyLink} />
          )}
        </section>

        <aside className="lg:pt-[138px]">
          <div className="rounded-[18px] border border-[#e2e6eb] bg-white p-5 shadow-[0_18px_50px_rgba(28,39,54,0.06)]">
            <div className="mb-5 flex items-center justify-between">
              <div className="flex items-center gap-2.5"><span className="flex h-8 w-8 items-center justify-center rounded-[10px] bg-[#edf3ff] text-[#1d5fd2]"><ChatsCircle size={18} weight="duotone" /></span><span className="text-[13px] font-semibold">Your setup</span></div>
              <span className="text-[11px] font-medium text-[#9aa3ae]">{step}/4</span>
            </div>
            <div className="space-y-4">
              <SummaryRow label="Tenant" value={selectedTenant?.name ?? "Choose a tenant"} done={Boolean(selectedTenant)} />
              <SummaryRow label="Agent" value={agent?.ready ? agent.name ?? "Ready" : "Configure your agent"} done={Boolean(agent?.ready)} />
              <SummaryRow label="Channel" value={channel ? channelDetails[channel].label : "Choose a channel"} done={Boolean(channel)} />
              <SummaryRow label="First conversation" value={connected ? "Ready to chat" : "Waiting for connection"} done={connected} />
            </div>
            <div className="mt-6 border-t border-[#eef0f3] pt-4 text-[12px] leading-5 text-[#7a8592]">You can change these connections later from the tenant settings.</div>
          </div>
          <div className="mt-5 flex gap-3 rounded-[14px] border border-[#e2e6eb] bg-[#fbfcfd] p-4 text-[12px] leading-5 text-[#727d89]"><UsersThree size={18} className="mt-0.5 shrink-0 text-[#8995a4]" />Each tenant keeps its conversations and channel connections separate.</div>
        </aside>
      </main>

      {showCreate && <CreateTenantModal value={tenantName} busy={busy} onChange={setTenantName} onClose={() => setShowCreate(false)} onCreate={createTenant} />}
    </div>
  );
}

function SummaryRow({ label, value, done }: { label: string; value: string; done: boolean }) {
  return <div className="flex items-start gap-3"><span className={`mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full ${done ? "bg-[#dff6eb] text-[#1b8b5a]" : "bg-[#f0f2f5] text-[#a5adb7]"}`}>{done ? <Check size={12} weight="bold" /> : <span className="h-1.5 w-1.5 rounded-full bg-current" />}</span><div><div className="text-[11px] uppercase tracking-[0.08em] text-[#98a2ae]">{label}</div><div className={`mt-0.5 text-[13px] font-medium ${done ? "text-[#34404c]" : "text-[#9aa3ae]"}`}>{value}</div></div></div>;
}

function TenantPanel({ tenants, onSelect, onCreate }: { tenants: Tenant[]; onSelect: (tenant: Tenant) => void; onCreate: () => void }) {
  return <div className="animate-in max-w-[640px] rounded-[18px] border border-[#e2e6eb] bg-white p-5 shadow-[0_18px_50px_rgba(28,39,54,0.05)] sm:p-7"><div className="mb-6 flex items-end justify-between gap-4"><div><h2 className="text-[21px] font-semibold tracking-[-0.025em]">Choose your workspace</h2><p className="mt-1.5 text-[14px] text-[#77818e]">Where should this agent live?</p></div><button className="button-secondary shrink-0" onClick={onCreate} type="button"><Plus size={16} weight="bold" /> New tenant</button></div><div className="space-y-2">{tenants.map((tenant) => <button key={tenant.id} className="tenant-row group" onClick={() => onSelect(tenant)} type="button"><span className="tenant-avatar">{tenant.initials}</span><span className="min-w-0 flex-1 text-left"><span className="block truncate text-[14px] font-semibold text-[#2b3541]">{tenant.name}</span><span className="mt-0.5 block text-[12px] text-[#929ca8]">{tenant.updated}</span></span><ArrowRight className="text-[#b5bdc8] transition group-hover:translate-x-0.5 group-hover:text-[#1d5fd2]" size={18} /></button>)}</div></div>;
}

function AgentPanel({ tenant, agent, agents, creating, busy, error, onBack, onRegister, onCancelRegister, onSelectAgent, onSuspend, onContinue, onSetup }: { tenant: Tenant; agent: AgentDetails | null; agents: AgentDetails[]; creating: boolean; busy: boolean; error: string; onBack: () => void; onRegister: () => void; onCancelRegister: () => void; onSelectAgent: (agent: AgentDetails) => void; onSuspend: () => void; onContinue: () => void; onSetup: (name: string, instruction: string, model: string, endpoint: string) => Promise<void> }) {
  const [name, setName] = useState(`${tenant.name} Agent`);
  const [model, setModel] = useState("gpt-4o-mini");
  const [endpoint, setEndpoint] = useState("https://api.openai.com/v1");
  const [instruction, setInstruction] = useState("You are a helpful assistant. Reply clearly and concisely.");
  if (agent === null) return <div className="animate-in max-w-[640px]"><div className="mb-5 flex items-center gap-2 text-[13px] text-[#77818e]"><button className="back-button" onClick={onBack} type="button">Back</button><span>/</span><span className="font-medium text-[#394451]">{tenant.name}</span></div><div className="rounded-[18px] border border-[#e2e6eb] bg-white p-7"><div className="h-5 w-40 animate-pulse rounded bg-[#edf0f4]" /><div className="mt-3 h-4 w-64 animate-pulse rounded bg-[#f1f3f6]" /><div className="mt-8 h-32 animate-pulse rounded-[12px] bg-[#f5f6f8]" /></div></div>;
  if (agent.status === "suspended") return <div className="animate-in max-w-[640px]"><div className="mb-5 flex items-center gap-2 text-[13px] text-[#77818e]"><button className="back-button" onClick={onBack} type="button">Back</button><span>/</span><span className="font-medium text-[#394451]">{tenant.name}</span></div><div className="rounded-[18px] border border-[#e2e6eb] bg-white p-7"><h2 className="text-[21px] font-semibold">Agent is offline</h2><p className="mt-2 text-[14px] leading-6 text-[#77818e]">This agent will not accept new conversations until it is resumed.</p><button className="button-secondary mt-6" onClick={onRegister} type="button"><Plus size={16} weight="bold" /> Register another agent</button></div></div>;
  if (agent.ready && !creating) return <div className="animate-in max-w-[640px]"><div className="mb-5 flex items-center gap-2 text-[13px] text-[#77818e]"><button className="back-button" onClick={onBack} type="button">Back</button><span>/</span><span className="font-medium text-[#394451]">{tenant.name}</span></div><div className="rounded-[18px] border border-[#e2e6eb] bg-white p-5 shadow-[0_18px_50px_rgba(28,39,54,0.05)] sm:p-7"><div className="flex items-start justify-between gap-3"><h2 className="text-[21px] font-semibold">Choose an agent</h2><button className="button-secondary shrink-0" onClick={onRegister} type="button"><Plus size={16} weight="bold" /> Register agent</button></div><p className="mt-1 text-[14px] text-[#77818e]">Select which published agent should answer in this tenant.</p><div className="mt-5 space-y-2">{agents.map((item) => <button key={item.appID} type="button" onClick={() => item.appID !== agent.appID && onSelectAgent(item)} className={`tenant-row w-full ${item.appID === agent.appID ? "ring-2 ring-[#1d5fd2]" : ""}`}><span className="tenant-avatar"><Cpu size={16} /></span><span className="min-w-0 flex-1 text-left"><span className="block truncate text-[14px] font-semibold text-[#2b3541]">{item.name}</span><span className="mt-0.5 block text-[12px] text-[#929ca8]">{item.appID === agent.appID ? "Current default" : item.status || "Configured"}</span></span>{item.appID === agent.appID && <CheckCircle size={18} className="text-[#1b8b5a]" weight="fill" />}</button>)}</div><div className="agent-summary mt-7"><div><span>Model</span><strong>{agent.model}</strong></div><div><span>Endpoint</span><strong className="break-all">{agent.endpoint || "Server default"}</strong></div><div><span>Instructions</span><strong>{agent.instruction || "Default assistant behavior"}</strong></div></div><div className="mt-6 flex flex-col-reverse gap-3 sm:flex-row sm:items-center sm:justify-between"><button className="button-secondary text-[#ba3d48]" disabled={busy} onClick={onSuspend} type="button">Take agent offline</button><button className="button-primary" onClick={onContinue} type="button">Choose channel <ArrowRight size={16} weight="bold" /></button></div></div></div>;
  return <div className="animate-in max-w-[640px]"><div className="mb-5 flex items-center gap-2 text-[13px] text-[#77818e]"><button className="back-button" onClick={onBack} type="button">Back</button><span>/</span><span className="font-medium text-[#394451]">{tenant.name}</span></div><div className="rounded-[18px] border border-[#e2e6eb] bg-white p-5 shadow-[0_18px_50px_rgba(28,39,54,0.05)] sm:p-7"><div className="flex items-center gap-3"><span className="flex h-11 w-11 items-center justify-center rounded-[12px] bg-[#edf3ff] text-[#1d5fd2]"><Cpu size={23} weight="duotone" /></span><div><h2 className="text-[21px] font-semibold">Set up your agent</h2><p className="mt-1 text-[14px] text-[#77818e]">Give it a name, model, and clear role.</p></div></div><div className="mt-7 grid gap-5 sm:grid-cols-2"><label className="field-label">Agent name<input className="field-input" value={name} onChange={(event) => setName(event.target.value)} /></label><label className="field-label">Model<input className="field-input" value={model} onChange={(event) => setModel(event.target.value)} placeholder="gpt-4o-mini" /></label><label className="field-label sm:col-span-2">API endpoint<input className="field-input" value={endpoint} onChange={(event) => setEndpoint(event.target.value)} placeholder="https://api.openai.com/v1" /><span className="field-help">Must match a host allowed by the server environment.</span></label><label className="field-label sm:col-span-2">What should this agent do?<textarea className="field-input field-textarea" value={instruction} onChange={(event) => setInstruction(event.target.value)} /></label></div>{error && <p className="mt-4 text-[13px] text-[#ba3d48]" role="alert">{error}</p>}<div className="mt-7 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"><span className="text-[12px] leading-5 text-[#818c99]">The server uses the model API key from its environment.</span><button className="button-primary shrink-0" disabled={busy || !name.trim() || !model.trim() || !endpoint.trim() || !instruction.trim()} onClick={() => void onSetup(name.trim(), instruction.trim(), model.trim(), endpoint.trim())} type="button">{busy ? "Preparing agent..." : "Prepare agent"} <ArrowRight size={16} weight="bold" /></button></div></div></div>;
}

function ChannelPanel({ tenant, onBack, onSelect }: { tenant: Tenant; onBack: () => void; onSelect: (channel: Channel) => void }) {
  return <div className="animate-in max-w-[640px]"><div className="mb-5 flex items-center gap-2 text-[13px] text-[#77818e]"><button className="back-button" onClick={onBack} type="button">Back</button><span>/</span><span className="font-medium text-[#394451]">{tenant.name}</span></div><div className="rounded-[18px] border border-[#e2e6eb] bg-white p-5 shadow-[0_18px_50px_rgba(28,39,54,0.05)] sm:p-7"><h2 className="text-[21px] font-semibold tracking-[-0.025em]">Choose a channel</h2><p className="mt-1.5 text-[14px] text-[#77818e]">Your agent will be ready there after one quick connection.</p><div className="mt-7 grid gap-3 sm:grid-cols-2">{(Object.keys(channelDetails) as Channel[]).map((key) => { const item = channelDetails[key]; const Icon = item.icon; return <button className="channel-card group" key={key} onClick={() => onSelect(key)} type="button"><span className={`channel-icon ${item.tone}`}><Icon size={25} weight="fill" /></span><span className="mt-4 block text-left text-[15px] font-semibold text-[#2b3541]">{item.label}</span><span className="mt-1 block text-left text-[13px] leading-5 text-[#7c8793]">{item.description}</span><span className="mt-5 flex items-center gap-1 text-[12px] font-semibold text-[#1d5fd2]">Connect <ArrowRight size={14} /></span></button>; })}</div></div></div>;
}

function ConnectPanel({ tenant, channel, connected, copied, busy, botID, secret, error, connectionURL, onBotID, onSecret, onBack, onConnect, onCopy }: { tenant: Tenant; channel: Channel; connected: boolean; copied: boolean; busy: boolean; botID: string; secret: string; error: string; connectionURL: string; onBotID: (value: string) => void; onSecret: (value: string) => void; onBack: () => void; onConnect: () => void; onCopy: () => void }) {
  const item = channelDetails[channel]; const Icon = item.icon;
  return <div className="animate-in max-w-[640px]"><div className="mb-5 flex items-center gap-2 text-[13px] text-[#77818e]"><button className="back-button" onClick={onBack} type="button">Back</button><span>/</span><span className="font-medium text-[#394451]">{tenant.name}</span><span>/</span><span className="font-medium text-[#394451]">{item.label}</span></div><div className="rounded-[18px] border border-[#e2e6eb] bg-white p-5 shadow-[0_18px_50px_rgba(28,39,54,0.05)] sm:p-7">{connected ? <ConnectedState tenant={tenant} channel={channel} onCopy={onCopy} copied={copied} connectionURL={connectionURL} /> : <><div className="flex items-center gap-3"><span className={`channel-icon ${item.tone}`}><Icon size={24} weight="fill" /></span><div><h2 className="text-[21px] font-semibold tracking-[-0.025em]">Connect {item.label}</h2><p className="mt-1 text-[14px] text-[#77818e]">A secure handoff gets your first conversation started.</p></div></div><div className="mt-7 space-y-5"><label className="field-label">{channel === "telegram" ? "Bot ID" : "AI Bot ID"}<input className="field-input" value={botID} onChange={(event) => onBotID(event.target.value)} placeholder={channel === "telegram" ? "Numeric bot ID" : "Enter the WeCom AI Bot ID"} />{channel === "wecom" && <span className="field-help">Use the AI Bot ID from WeCom AI Bot settings, not Corp ID or Agent ID.</span>}</label><label className="field-label">{channel === "telegram" ? "Bot token" : "AI Bot secret"}<input className="field-input" type="password" value={secret} onChange={(event) => onSecret(event.target.value)} placeholder="Sent securely to the service" />{channel === "wecom" && <span className="field-help">This is the AI Bot secret for the same Bot ID.</span>}</label></div>{error && <p className="mt-4 text-[13px] text-[#ba3d48]" role="alert">{error}</p>}<div className="mt-7 flex flex-col-reverse gap-3 sm:flex-row sm:items-center sm:justify-between"><span className="flex items-center gap-2 text-[12px] text-[#818c99]"><CheckCircle size={16} className="text-[#1b8b5a]" weight="fill" />Credentials are encrypted</span><button className="button-primary" disabled={busy} onClick={onConnect} type="button">{busy ? "Connecting..." : "Connect channel"} <ArrowRight size={16} weight="bold" /></button></div></>}</div></div>;
}

function ConnectedState({ tenant, channel, onCopy, copied, connectionURL }: { tenant: Tenant; channel: Channel; onCopy: () => void; copied: boolean; connectionURL: string }) {
  const telegram = channel === "telegram";
  const agentName = `${tenant.name} Agent`;
  const destination = connectionURL || (telegram ? "Open Telegram and search for your bot" : agentName);
  return <div className="text-center"><span className={`mx-auto flex h-14 w-14 items-center justify-center rounded-[16px] ${telegram ? "bg-[#e3f4ff] text-[#1997dc]" : "bg-[#e6f8ec] text-[#2da65c]"}`}><CheckCircle size={30} weight="fill" /></span><h2 className="mt-5 text-[23px] font-semibold tracking-[-0.03em]">Your agent is connected</h2><p className="mx-auto mt-2 max-w-[400px] text-[14px] leading-6 text-[#77818e]">Open {telegram ? "Telegram" : "WeCom"} and send a message to finish the setup. Your agent will reply in this tenant.</p><div className="mx-auto mt-6 flex max-w-[360px] items-center gap-2 rounded-[10px] border border-[#e5e8ec] bg-[#fafbfc] p-2 pl-3 text-left"><span className="min-w-0 flex-1 truncate text-[13px] font-medium text-[#45515e]">{destination}</span>{connectionURL && <button className="icon-button" title="Copy conversation link" aria-label="Copy conversation link" onClick={onCopy} type="button">{copied ? <Check size={16} className="text-[#1b8b5a]" /> : <Copy size={16} />}</button>}</div>{telegram && connectionURL ? <a className="button-primary mt-7" href={connectionURL} target="_blank" rel="noreferrer">Open Telegram <ArrowRight size={16} weight="bold" /></a> : <span className="button-primary mt-7">Open {telegram ? "Telegram" : "WeCom"} <ArrowRight size={16} weight="bold" /></span>}<p className="mt-5 text-[12px] text-[#9aa3ae]">You can leave this tab open while you try it.</p></div>;
}

function CreateTenantModal({ value, busy, onChange, onClose, onCreate }: { value: string; busy: boolean; onChange: (value: string) => void; onClose: () => void; onCreate: () => void }) {
  return <div className="fixed inset-0 z-20 flex items-center justify-center bg-[#19212b]/25 p-5 backdrop-blur-[2px]" role="dialog" aria-modal="true" aria-labelledby="create-title"><div className="w-full max-w-[430px] rounded-[18px] border border-[#e1e5ea] bg-white p-6 shadow-[0_24px_80px_rgba(24,32,42,0.18)]"><div className="flex items-start justify-between"><div><h2 id="create-title" className="text-[20px] font-semibold tracking-[-0.025em]">Create a tenant</h2><p className="mt-1.5 text-[13px] text-[#77818e]">A focused home for one team or project.</p></div><button className="icon-button -mr-1 -mt-1" onClick={onClose} title="Close" aria-label="Close" type="button"><X size={18} /></button></div><label className="field-label mt-6">Tenant name<input autoFocus className="field-input" onChange={(event) => onChange(event.target.value)} onKeyDown={(event) => event.key === "Enter" && onCreate()} placeholder="e.g. Northstar Labs" value={value} /></label><div className="mt-7 flex justify-end gap-2"><button className="button-secondary" onClick={onClose} type="button">Cancel</button><button className="button-primary" disabled={!value.trim() || busy} onClick={onCreate} type="button">{busy ? "Creating..." : "Create tenant"} <ArrowRight size={16} weight="bold" /></button></div></div></div>;
}

export default App;
