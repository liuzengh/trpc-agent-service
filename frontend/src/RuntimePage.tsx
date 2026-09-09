import { useEffect, useState } from "react";
import { Activity, Server } from "lucide-react";
import { APIError, api, type AgentApp, type BackendHealth, type DrainStatus, type Identity, type ProviderStatus, type RuntimeFaultConfiguration, type RuntimeFaultScenario, type RuntimeStatus } from "./api";
import { AsyncState } from "./AsyncState";

export function RuntimePage({ identity }: { identity?: Identity }) {
  const [items, setItems] = useState<RuntimeStatus[]>(); const [error, setError] = useState<APIError>();
  const [drain, setDrain] = useState<DrainStatus>(); const [backend, setBackend] = useState<BackendHealth>(); const [providers, setProviders] = useState<ProviderStatus[]>(); const [drainError, setDrainError] = useState("");
  const [apps, setApps] = useState<AgentApp[]>(); const [faults, setFaults] = useState<RuntimeFaultConfiguration>(); const [faultAppID, setFaultAppID] = useState(""); const [faultScenario, setFaultScenario] = useState<RuntimeFaultScenario>("none"); const [faultDelay, setFaultDelay] = useState(0); const [faultError, setFaultError] = useState("");
  const [faultsEnabled, setFaultsEnabled] = useState(true);
  const load = () => {
    setError(undefined); setDrainError(""); setFaultError("");
    Promise.all([api.runtimeStatus(), api.operationsDrain(), api.backend(), api.providerStatuses(), api.apps(), api.runtimeFaults()])
      .then(([runtime, drainStatus, backendHealth, providerStatuses, appList, faultConfiguration]) => {
        setItems(runtime.items); setDrain(drainStatus); setBackend(backendHealth.health); setProviders(providerStatuses.items); setApps(appList.items); setFaultsEnabled(faultConfiguration.enabled);
      })
      .catch(setError);
  };
  useEffect(load, []);
  const drainState = drain?.state;
  useEffect(() => {
    if (drainState !== "draining") return;
    const timer = setInterval(() => {
      void api.operationsDrain().then(setDrain).catch(() => undefined);
    }, 500);
    return () => clearInterval(timer);
  }, [drainState]);
  const canOperate = !identity || ["platform_admin", "tenant_admin", "operator"].includes(identity.active_role);
  const startDrain = async () => {
    if (!window.confirm("确认开始优雅排水？新请求将被拒绝，活跃请求会等待完成。")) return;
    try { setDrain(await api.startOperationsDrain()); setDrainError(""); } catch (caught) { setDrainError(caught instanceof APIError ? caught.message : "排水请求失败"); }
  };
  const applyFault = async () => {
    if (!faultAppID) return;
    if (faultScenario !== "none" && !window.confirm("确认注入开发故障？该配置仅用于本地/Compose 验证。")) return;
    try { setFaults(await api.setRuntimeFaults(faultAppID, faultScenario, faultDelay)); setFaultError(""); }
    catch (caught) { setFaultError(caught instanceof APIError ? caught.message : "故障注入失败"); }
  };
  if (error?.status === 403) return <AsyncState kind="forbidden" />; if (error) return <AsyncState kind="error" retry={load} />; if (!items) return <AsyncState kind="loading" />; if (!items.length && !drain && !backend && !providers?.length) return <AsyncState kind="empty" />;
  const drainLifecycle = drain?.state === "closed" ? "healthy" : drain?.state === "idle" ? "published" : "closing";
  return <div className="runtime-grid">
    {drain && <article className="runtime-item"><div className="runtime-title"><Activity /><div><h3>操作状态</h3><span>drain</span></div><span className={`status ${drainLifecycle}`}>{drain.state}</span></div><dl><dt>正在执行</dt><dd>{drain.active_executions}</dd><dt>开始时间</dt><dd>{drain.started_at ? new Date(drain.started_at).toLocaleTimeString() : "未开始"}</dd><dt>完成时间</dt><dd>{drain.completed_at ? new Date(drain.completed_at).toLocaleTimeString() : "未完成"}</dd></dl>{canOperate && <div className="stack-actions"><button onClick={() => void startDrain()} disabled={drain.state !== "idle"}>开始优雅排水</button></div>}{drainError && <p className="form-error">{drainError}</p>}</article>}
    {backend && <article className="runtime-item"><div className="runtime-title"><Server /><div><h3>存储依赖</h3><span>{backend.backend}</span></div><span className={`status ${backend.status}`}>{backend.status}</span></div><dl><dt>检查时间</dt><dd>{new Date(backend.checked_at).toLocaleTimeString()}</dd><dt>状态消息</dt><dd>{backend.message ?? "无"}</dd></dl></article>}
    {providers?.map((provider) => <article className="runtime-item" key={provider.provider}><div className="runtime-title"><Server /><div><h3>IM Provider</h3><span>{provider.provider}</span></div><span className={`status ${provider.status}`}>{provider.status}</span></div><dl><dt>连接状态</dt><dd>{provider.status}</dd><dt>冒烟测试</dt><dd>{provider.credential_smoke_status}</dd></dl></article>)}
    {identity?.auth_mode !== "production" && apps && <article className="runtime-item"><div className="runtime-title"><Activity /><div><h3>开发故障注入</h3><span>{faultsEnabled ? "enabled" : "disabled"}</span></div><span className={`status ${faults?.scenario && faults.scenario !== "none" ? "error" : "healthy"}`}>{faults?.scenario ?? "none"}</span></div><form className="fault-form" onSubmit={(event) => { event.preventDefault(); void applyFault(); }}><label>Agent 应用<select value={faultAppID} onChange={(event) => setFaultAppID(event.target.value)}>{apps.map((app) => <option value={app.id} key={app.id}>{app.name}</option>)}</select></label><label>场景<select value={faultScenario} onChange={(event) => setFaultScenario(event.target.value as RuntimeFaultScenario)}><option value="none">清除</option><option value="runner_delay">Runner 延迟</option><option value="runner_error">Runner 失败</option><option value="tool_error">Tool 失败</option></select></label><label>延迟 ms<input type="number" min="0" max="5000" step="100" value={faultDelay} onChange={(event) => setFaultDelay(Number(event.target.value))} /></label>{canOperate && <button disabled={!faultAppID || !faultsEnabled}>注入</button>}</form>{faultError && <p className="form-error">{faultError}</p>}</article>}
    {items.map((item) => <article className="runtime-item" key={item.id}><div className="runtime-title">{item.role === "gateway" ? <Activity /> : <Server />}<div><h3>{item.id}</h3><span>{item.role}</span></div><span className={`status ${item.lifecycle}`}>{item.lifecycle}</span></div><dl><dt>可用性</dt><dd>{item.available ? "可用" : "不可用"}</dd><dt>正在执行</dt><dd>{item.active_executions}</dd><dt>已完成</dt><dd>{item.completed_executions}</dd><dt>失败</dt><dd>{item.failed_executions}</dd></dl></article>)}
  </div>;
}
