import { useEffect, useRef, useState, type FormEvent } from "react";
import { MessageSquare, Plus, Rocket, X } from "lucide-react";
import { APIError, api, type AgentApp, type CapacityTestResult, type Deployment, type DeploymentRollbackPreview, type DeploymentVersion, type Identity } from "./api";
import { AsyncState } from "./AsyncState";

export function DeploymentsPage({ identity, onOpenChat }: { identity: Identity; onOpenChat?: (appID: string) => void }) {
  const [items, setItems] = useState<Deployment[]>(); const [apps, setApps] = useState<AgentApp[]>([]); const [selected, setSelected] = useState<Deployment>(); const [versions, setVersions] = useState<DeploymentVersion[]>([]); const [mode, setMode] = useState<"deployment" | "version">(); const [error, setError] = useState<APIError>(); const requestGeneration = useRef(0); const versionAttempt = useRef<{ deploymentID: string; configText: string; key: string } | undefined>(undefined);
  const [targetVersionID, setTargetVersionID] = useState(""); const [grayPercentage, setGrayPercentage] = useState(10); const [rollbackPreview, setRollbackPreview] = useState<DeploymentRollbackPreview>(); const [capacity, setCapacity] = useState<CapacityTestResult>();
  const load = () => { const generation = ++requestGeneration.current; setError(undefined); Promise.all([api.deployments(), api.apps()]).then(([d, a]) => { if (requestGeneration.current === generation) { setItems(d.items); setApps(a.items); } }).catch((caught) => { if (requestGeneration.current === generation) setError(caught); }); };
  useEffect(() => { setSelected(undefined); setVersions([]); versionAttempt.current = undefined; load(); return () => { requestGeneration.current++; }; }, [identity.active_tenant_id]);
  const open = async (deployment: Deployment) => { setSelected(deployment); try { setVersions((await api.versions(deployment.id)).items); } catch (caught) { setError(caught as APIError); } };
  useEffect(() => {
    if (!capacity || capacity.status !== "running") return;
    const timer = setInterval(() => { void api.capacity(capacity.id).then(setCapacity).catch(() => undefined); }, 500);
    return () => clearInterval(timer);
  }, [capacity]);
  const createDeployment = async (event: FormEvent<HTMLFormElement>) => { event.preventDefault(); const data = new FormData(event.currentTarget); try { const item = await api.createDeployment({ id: String(data.get("id")), agent_app_id: String(data.get("agent_app_id")) }); setItems((current) => [...(current ?? []), item]); setMode(undefined); await open(item); } catch (caught) { setError(caught as APIError); } };
  const createVersion = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selected) return;
    const configText = String(new FormData(event.currentTarget).get("config"));
    let config: Record<string, unknown>;
    try { config = JSON.parse(configText) as Record<string, unknown>; } catch { setError(new APIError(400, "invalid_json", "配置必须是有效 JSON")); return; }
    let attempt = versionAttempt.current;
    if (!attempt || attempt.deploymentID !== selected.id || attempt.configText !== configText) {
      attempt = { deploymentID: selected.id, configText, key: crypto.randomUUID() };
      versionAttempt.current = attempt;
    }
    try {
      const item = await api.createVersion(selected.id, config, attempt.key);
      versionAttempt.current = undefined;
      setVersions((current) => [...current, item]);
      setError(undefined);
      setMode(undefined);
    } catch (caught) {
      setError(caught instanceof APIError ? caught : new APIError(0, "network_error", "无法连接服务，请重试"));
    }
  };
  const transition = async (status: "published" | "active" | "paused") => {
    if (!selected) return;
    try {
      const version_id = status === "published" ? versions.at(-1)?.id : undefined;
      const updated = await api.transition(selected.id, status, version_id);
      setError(undefined);
      setSelected(updated);
      setItems((current) => current?.map((item) => item.id === updated.id ? updated : item));
    } catch (caught) {
      const apiError = caught as APIError;
      setError(apiError);
      if (apiError.code === "agent_app_already_has_active_deployment") {
        try {
          const refreshed = (await api.deployments()).items;
          setItems(refreshed);
          setSelected(refreshed.find((item) => item.id === selected.id));
        } catch {
          // Keep the original conflict visible; a later list refresh can retry state synchronization.
        }
      }
    }
  };
  const updateDeployment = (updated: Deployment) => { setSelected(updated); setItems((current) => current?.map((item) => item.id === updated.id ? updated : item)); };
  const startRollout = async () => {
    if (!selected || !targetVersionID) return;
    if (!window.confirm(`确认将 ${selected.agent_app_id} 灰度 ${grayPercentage}% 到 ${targetVersionID}？`)) return;
    try { updateDeployment(await api.rollout(selected.id, { target_version_id: targetVersionID, gray_percentage: grayPercentage, confirm: true })); setError(undefined); }
    catch (caught) { setError(caught as APIError); }
  };
  const previewRollback = async () => {
    if (!selected) return;
    try { setRollbackPreview(await api.rollbackPreview(selected.id)); setError(undefined); }
    catch (caught) { setError(caught as APIError); }
  };
  const confirmRollback = async () => {
    if (!selected || !window.confirm("确认回滚到先前版本？")) return;
    try { updateDeployment(await api.rollback(selected.id)); setRollbackPreview(undefined); setError(undefined); }
    catch (caught) { setError(caught as APIError); }
  };
  const startCapacity = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selected) return;
    const data = new FormData(event.currentTarget);
    try {
      setCapacity(await api.startCapacity({
        agent_app_id: selected.agent_app_id, concurrency: Number(data.get("concurrency")),
        runs: Number(data.get("runs")), timeout_ms: Number(data.get("timeout_ms")),
        peak_im_callbacks_per_second: Number(data.get("peak_im_callbacks_per_second")),
        average_tokens_per_session: Number(data.get("average_tokens_per_session")),
        redis_operations_per_session: Number(data.get("redis_operations_per_session")),
        sql_operations_per_session: Number(data.get("sql_operations_per_session")),
        headroom_percent: Number(data.get("headroom_percent")),
      }));
      setError(undefined);
    } catch (caught) { setError(caught as APIError); }
  };
  if (!items && !error) return <AsyncState kind="loading" />; if (error?.status === 403 && !mode) return <AsyncState kind="forbidden" />; if (error && !mode && !items) return <AsyncState kind="error" retry={load} />;
  const canConfigure = identity.active_role === "platform_admin" || identity.active_role === "tenant_admin";
  const canOperate = canConfigure || identity.active_role === "operator";
  return <div className="resource-layout"><section className="resource-list"><div className="section-toolbar"><span>{items?.length ?? 0} 个部署</span>{canConfigure && <button className="primary" disabled={!apps.length} onClick={() => setMode("deployment")}><Plus />新建部署</button>}</div>{error && items && <p className="inline-error" role="alert">{error.message}</p>}{!items?.length ? <AsyncState kind="empty" /> : <div className="table-wrap"><table><thead><tr><th>部署</th><th>应用</th><th>状态</th></tr></thead><tbody>{items.map((item) => <tr key={item.id} onClick={() => void open(item)}><td><Rocket />{item.id}</td><td><code>{item.agent_app_id}</code></td><td><span className={`status ${item.status}`}>{item.status}</span></td></tr>)}</tbody></table></div>}</section>
    {selected && <aside className="detail-panel"><button className="icon-button" title="关闭详情" onClick={() => { versionAttempt.current = undefined; setSelected(undefined); setRollbackPreview(undefined); }}><X /></button><span className="eyebrow">Deployment</span><h3>{selected.id}</h3><dl><dt>状态</dt><dd><span className={`status ${selected.status}`}>{selected.status}</span></dd><dt>当前版本</dt><dd>{selected.version_id ?? "尚未发布"}</dd><dt>灰度状态</dt><dd>{selected.rollout_status ?? "idle"}{selected.gray_percentage ? ` · ${selected.gray_percentage}%` : ""}</dd><dt>目标/回滚版本</dt><dd>{selected.target_version_id ?? "-"} / {selected.previous_version_id ?? "-"}</dd><dt>版本历史</dt><dd>{versions.length ? versions.map((v) => <div className="version-record" key={v.id}><code>v{v.number}</code><pre>{JSON.stringify(v.config, null, 2)}</pre></div>) : "暂无版本"}</dd></dl>
      {canOperate && selected.status === "active" && versions.length > 1 && <div className="capacity-controls"><label>目标版本<select value={targetVersionID} onChange={(event) => setTargetVersionID(event.target.value)}>{versions.filter((version) => version.id !== selected.version_id).map((version) => <option value={version.id} key={version.id}>v{version.number}</option>)}</select></label><label>灰度 %<input type="number" min="0" max="100" value={grayPercentage} onChange={(event) => setGrayPercentage(Number(event.target.value))} /></label><button disabled={!targetVersionID} onClick={() => void startRollout()}>开始/推进灰度</button></div>}
      {canOperate && selected.status === "active" && <div className="stack-actions"><button onClick={() => void previewRollback()}>回滚预览</button>{rollbackPreview && <button onClick={() => void confirmRollback()}>确认回滚</button>}</div>}
      {rollbackPreview && <div className="rollback-preview"><strong>回滚预览</strong><dl><dt>当前版本</dt><dd>{rollbackPreview.current_version_id}</dd><dt>回滚版本</dt><dd>{rollbackPreview.previous_version_id}</dd><dt>活跃执行</dt><dd>{rollbackPreview.active_executions}</dd><dt>预期结果</dt><dd>{rollbackPreview.expected_result}</dd></dl></div>}
      {canOperate && <form className="capacity-controls" onSubmit={(event) => void startCapacity(event)}><label>并发<input name="concurrency" type="number" min="1" max="10" defaultValue={2} required /></label><label>次数<input name="runs" type="number" min="1" max="100" defaultValue={4} required /></label><label>超时 ms<input name="timeout_ms" type="number" min="100" max="5000" step="100" defaultValue={1000} required /></label><label>IM 峰值回调/s<input name="peak_im_callbacks_per_second" type="number" min="0" max="1000000" defaultValue={120} required /></label><label>平均 Token/Session<input name="average_tokens_per_session" type="number" min="0" max="10000000" defaultValue={800} required /></label><label>Redis 操作/Session<input name="redis_operations_per_session" type="number" min="0" max="10000" defaultValue={6} required /></label><label>SQL 操作/Session<input name="sql_operations_per_session" type="number" min="0" max="10000" defaultValue={4} required /></label><label>安全余量 %<input name="headroom_percent" type="number" min="1" max="90" defaultValue={25} required /></label><button className="primary">容量评估</button></form>}
      {capacity && <div className="capacity-result"><div className="runtime-title"><h4>容量结果</h4><span className={`status ${capacity.status === "completed" ? "healthy" : capacity.status === "running" ? "published" : "error"}`}>{capacity.status}</span></div><dl><dt>安全并发</dt><dd>{capacity.safe_concurrency}</dd><dt>每节点 Session / 推荐 Worker</dt><dd>{capacity.sessions_per_node} / {capacity.recommended_worker_nodes}</dd><dt>吞吐/s</dt><dd>{capacity.throughput_per_second.toFixed(2)}</dd><dt>IM QPS / Token/s</dt><dd>{capacity.im_callback_peak_qps.toFixed(2)} / {capacity.token_throughput_per_second.toFixed(2)}</dd><dt>Redis QPS / SQL QPS</dt><dd>{capacity.redis_qps.toFixed(2)} / {capacity.sql_qps.toFixed(2)}</dd><dt>模型/Tool/存储 ms</dt><dd>{capacity.model_latency_ms}/{capacity.tool_latency_ms}/{capacity.storage_latency_ms}</dd><dt>平均 Token/Session</dt><dd>{capacity.average_tokens_per_session}</dd><dt>安全余量</dt><dd>{capacity.headroom_percent}%</dd><dt>Tokens/成本</dt><dd>{capacity.estimated_tokens}/{capacity.estimated_cost.toFixed(4)}</dd><dt>首个瓶颈</dt><dd>{capacity.first_bottleneck}</dd><dt>Trace</dt><dd><code>{capacity.trace_id}</code></dd></dl></div>}
      <div className="stack-actions"><button onClick={() => onOpenChat?.(selected.agent_app_id)}><MessageSquare aria-hidden="true" />打开 Chat</button>{canOperate && <>{canConfigure && <button onClick={() => { versionAttempt.current = undefined; setError(undefined); setMode("version"); }}>创建版本</button>}{selected.status === "draft" && <button disabled={!versions.length} onClick={() => void transition("published")}>发布</button>}{selected.status === "published" && <button onClick={() => void transition("active")}>激活</button>}{selected.status === "active" && <button onClick={() => void transition("paused")}>暂停</button>}</>}</div></aside>}
    {mode === "deployment" && <div className="modal-backdrop"><form className="modal" onSubmit={(event) => void createDeployment(event)}><div className="modal-title"><h3>新建部署</h3><button type="button" className="icon-button" title="关闭" onClick={() => setMode(undefined)}><X /></button></div><label>部署标识<input name="id" required pattern="[a-z][a-z0-9-]{2,62}" /></label><label>Agent 应用<select name="agent_app_id">{apps.map((app) => <option key={app.id} value={app.id}>{app.name}</option>)}</select></label><div className="form-actions"><button type="button" onClick={() => setMode(undefined)}>取消</button><button className="primary">创建部署</button></div></form></div>}
    {mode === "version" && <div className="modal-backdrop"><form className="modal" onSubmit={(event) => void createVersion(event)}><div className="modal-title"><h3>创建不可变版本</h3><button type="button" className="icon-button" title="关闭" onClick={() => { versionAttempt.current = undefined; setMode(undefined); }}><X /></button></div><label>JSON 配置<textarea name="config" rows={7} defaultValue={'{"runner":"fake"}'} required /></label>{error && <p className="form-error">{error.message}</p>}<div className="form-actions"><button type="button" onClick={() => { versionAttempt.current = undefined; setMode(undefined); }}>取消</button><button className="primary">创建版本</button></div></form></div>}
  </div>;
}
