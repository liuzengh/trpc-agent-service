import { useEffect, useState } from "react";
import { Check, Gauge, Save, Search, ShieldCheck, X } from "lucide-react";
import { api, type AuditEvent, type Identity, type PlatformTrace, type TenantMetrics, type TenantPolicy, type ToolConfirmation } from "./api";
import { AsyncState } from "./AsyncState";

type View = "策略" | "确认" | "审计" | "指标与成本";

const emptyPolicy = (appID = ""): TenantPolicy => ({
  tenant_id: "", agent_app_id: appID, revision: 0, allowed_tools: [], allowed_mcp: [], dangerous_tools: [],
  denied_input_patterns: [], denied_output_patterns: [], redacted_patterns: [], allowed_im_users: [], allowed_im_subjects: [],
  allowed_provider_accounts: [], allowed_conversation_types: [],
  token_budget: 0, cost_budget: 0, cost_per_token: 0, tool_costs: {}, estimated_tokens_per_run: 0, rate_limit: 0, rate_window_seconds: 60,
  runtime_timeout_ms: 30000,
});

export function GovernancePage({ identity }: { identity: Identity }) {
  const [view, setView] = useState<View>("策略");
  const [apps, setApps] = useState<{ id: string; name: string }[]>([]);
  const [policy, setPolicy] = useState<TenantPolicy>(emptyPolicy());
  const [audits, setAudits] = useState<AuditEvent[]>([]);
  const [confirmations, setConfirmations] = useState<ToolConfirmation[]>([]);
  const [metrics, setMetrics] = useState<TenantMetrics>();
	const [metricsAppID, setMetricsAppID] = useState("");
	const [metricsProvider, setMetricsProvider] = useState("");
	const [metricsHours, setMetricsHours] = useState(24);
  const [trace, setTrace] = useState<PlatformTrace>();
  const [traceID, setTraceID] = useState("");
  const [auditDecision, setAuditDecision] = useState("");
  const [auditFrom, setAuditFrom] = useState("");
  const [auditTo, setAuditTo] = useState("");
  const [auditChannel, setAuditChannel] = useState("");
  const [auditUserID, setAuditUserID] = useState("");
  const [auditSessionID, setAuditSessionID] = useState("");
  const [auditAgentName, setAuditAgentName] = useState("");
  const [auditErrorType, setAuditErrorType] = useState("");
  const [auditRequestID, setAuditRequestID] = useState("");
  const [auditTraceID, setAuditTraceID] = useState("");
  const [status, setStatus] = useState("");
  const [failed, setFailed] = useState(false);
  const canConfigure = identity.active_role === "platform_admin" || identity.active_role === "tenant_admin";
  const canDecide = canConfigure || identity.active_role === "operator";

  useEffect(() => {
    setFailed(false);
    Promise.all([api.apps(), api.governanceAudit(), api.governanceMetrics(), api.confirmations()])
      .then(([appList, auditList, metric, confirmationList]) => {
        setApps(appList.items); setAudits(auditList.items); setMetrics(metric); setConfirmations(confirmationList.items);
        if (appList.items[0]) setPolicy(emptyPolicy(appList.items[0].id));
      }).catch(() => setFailed(true));
  }, [identity.active_tenant_id]);

  const loadPolicy = async (appID: string) => {
    setStatus("");
    if (!appID) { setPolicy(emptyPolicy()); return; }
    try { setPolicy(await api.governancePolicy(appID)); }
    catch { setPolicy(emptyPolicy(appID)); }
  };
  const list = (value: string) => value.split(",").map((item) => item.trim()).filter(Boolean);
	const costs = (value: string) => Object.fromEntries(value.split(",").map((item) => item.trim().split("=")).filter(([name, cost]) => name && cost !== undefined && Number.isFinite(Number(cost))).map(([name, cost]) => [name, Number(cost)]));
  const save = async () => {
    try { const updated = await api.saveGovernancePolicy(policy); setPolicy(updated); setStatus(`策略 revision ${updated.revision} 已生效`); }
    catch { setStatus("策略保存失败"); }
  };
  const decide = async (item: ToolConfirmation, approve: boolean) => {
    await api.decideConfirmation(item.id, approve);
    setConfirmations((await api.confirmations()).items);
  };
  const findTrace = async () => {
    setTrace(undefined);
    try { setTrace(await api.governanceTrace(traceID)); } catch { setStatus("Trace 未找到"); }
  };
  const searchAudits = async () => {
    setStatus("");
    try {
      setAudits((await api.governanceAudit({
        from: auditFrom, to: auditTo, channel: auditChannel, user_id: auditUserID,
        session_id: auditSessionID, agent_name: auditAgentName, decision: auditDecision,
        error_type: auditErrorType, request_id: auditRequestID, trace_id: auditTraceID,
      })).items);
    } catch {
      setStatus("审计查询失败");
    }
  };
	const refreshMetrics = async () => {
		setStatus("");
		const to = new Date();
		const from = new Date(to.getTime() - metricsHours * 60 * 60 * 1000);
		try { setMetrics(await api.governanceMetrics({ app_id: metricsAppID, provider: metricsProvider as "" | "mock" | "enterprise_wechat" | "telegram", from: from.toISOString(), to: to.toISOString() })); }
		catch { setStatus("指标查询失败"); }
	};

  if (failed) return <AsyncState kind="error" />;
  return <div className="governance-page">
    <div className="segmented-control" aria-label="治理视图">
      {(["策略", "确认", "审计", "指标与成本"] as View[]).map((item) => <button key={item} className={view === item ? "active" : ""} onClick={() => setView(item)}>{item}</button>)}
    </div>
    {status && <div className="governance-status" role="status">{status}</div>}
    {view === "策略" && <section className="governance-band">
      <div className="section-toolbar"><div><strong>Tenant 治理策略</strong><small>Revision {policy.revision || "new"}</small></div><button className="primary" disabled={!canConfigure || !policy.agent_app_id} onClick={() => void save()}><Save aria-hidden="true" />保存策略</button></div>
      <div className="policy-grid">
        <label>Agent 应用<select value={policy.agent_app_id} onChange={(event) => void loadPolicy(event.target.value)}><option value="">请选择</option>{apps.map((app) => <option value={app.id} key={app.id}>{app.name}</option>)}</select></label>
        <ListField label="允许的 Tools" value={policy.allowed_tools} onChange={(value) => setPolicy({ ...policy, allowed_tools: list(value) })} />
        <ListField label="允许的 MCP" value={policy.allowed_mcp} onChange={(value) => setPolicy({ ...policy, allowed_mcp: list(value) })} />
        <ListField label="危险 Tools" value={policy.dangerous_tools} onChange={(value) => setPolicy({ ...policy, dangerous_tools: list(value) })} />
        <ListField label="拒绝输入模式" value={policy.denied_input_patterns} onChange={(value) => setPolicy({ ...policy, denied_input_patterns: list(value) })} />
        <ListField label="拒绝输出模式" value={policy.denied_output_patterns} onChange={(value) => setPolicy({ ...policy, denied_output_patterns: list(value) })} />
        <ListField label="替换敏感模式" value={policy.redacted_patterns.filter((item) => item !== "[REDACTED]")} onChange={(value) => setPolicy({ ...policy, redacted_patterns: list(value) })} />
        <ListField label="允许的 IM 用户" value={policy.allowed_im_users} onChange={(value) => setPolicy({ ...policy, allowed_im_users: list(value) })} />
        <ListField label="允许的 IM 会话" value={policy.allowed_im_subjects} onChange={(value) => setPolicy({ ...policy, allowed_im_subjects: list(value) })} />
        <ListField label="允许的 Provider Account" value={policy.allowed_provider_accounts} onChange={(value) => setPolicy({ ...policy, allowed_provider_accounts: list(value) })} />
        <ListField label="允许的会话类型" value={policy.allowed_conversation_types} onChange={(value) => setPolicy({ ...policy, allowed_conversation_types: list(value) })} />
        <NumberField label="Token 预算" value={policy.token_budget} onChange={(value) => setPolicy({ ...policy, token_budget: value })} />
        <NumberField label="成本预算" value={policy.cost_budget} step="0.01" onChange={(value) => setPolicy({ ...policy, cost_budget: value })} />
        <NumberField label="每 Token 成本" value={policy.cost_per_token} step="0.0001" onChange={(value) => setPolicy({ ...policy, cost_per_token: value })} />
		<ListField label="Tool 成本" value={Object.entries(policy.tool_costs).map(([name, cost]) => `${name}=${cost}`)} onChange={(value) => setPolicy({ ...policy, tool_costs: costs(value) })} />
        <NumberField label="单次预留 Token" value={policy.estimated_tokens_per_run} onChange={(value) => setPolicy({ ...policy, estimated_tokens_per_run: value })} />
        <NumberField label="每窗口请求数" value={policy.rate_limit} onChange={(value) => setPolicy({ ...policy, rate_limit: value })} />
        <NumberField label="限流窗口秒数" value={policy.rate_window_seconds} onChange={(value) => setPolicy({ ...policy, rate_window_seconds: value })} />
        <NumberField label="运行超时 ms" value={policy.runtime_timeout_ms ?? 30000} onChange={(value) => setPolicy({ ...policy, runtime_timeout_ms: value })} />
      </div>
    </section>}
    {view === "确认" && <section className="governance-band"><div className="section-toolbar"><strong>危险 Tool 确认</strong></div>{confirmations.length === 0 ? <AsyncState kind="empty" /> : <div className="table-wrap"><table><thead><tr><th>Tool</th><th>参数摘要</th><th>Request</th><th>状态</th><th>过期时间</th><th>操作</th></tr></thead><tbody>{confirmations.map((item) => <tr key={item.id}><td>{item.tool_name}</td><td><code>{item.argument_summary}</code></td><td><code>{item.request_id}</code></td><td><span className={`status ${item.status}`}>{item.status}</span></td><td>{item.expires_at}</td><td><div className="row-actions"><button className="icon-button" title="批准" disabled={!canDecide || item.status !== "pending"} onClick={() => void decide(item, true)}><Check /></button><button className="icon-button" title="拒绝" disabled={!canDecide || item.status !== "pending"} onClick={() => void decide(item, false)}><X /></button></div></td></tr>)}</tbody></table></div>}</section>}
    {view === "审计" && <section className="governance-band">
      <div className="section-toolbar"><strong>Audit Events</strong><span>{audits.length} 条</span></div>
      <div className="audit-filters">
        <label>开始时间<input value={auditFrom} placeholder="RFC3339" onChange={(event) => setAuditFrom(event.target.value)} /></label>
        <label>结束时间<input value={auditTo} placeholder="RFC3339" onChange={(event) => setAuditTo(event.target.value)} /></label>
        <label>Channel<input value={auditChannel} onChange={(event) => setAuditChannel(event.target.value)} /></label>
        <label>用户 ID<input value={auditUserID} onChange={(event) => setAuditUserID(event.target.value)} /></label>
        <label>Session ID<input value={auditSessionID} onChange={(event) => setAuditSessionID(event.target.value)} /></label>
        <label>Agent App<input value={auditAgentName} onChange={(event) => setAuditAgentName(event.target.value)} /></label>
        <label>决策<input value={auditDecision} onChange={(event) => setAuditDecision(event.target.value)} /></label>
        <label>错误类型<input value={auditErrorType} onChange={(event) => setAuditErrorType(event.target.value)} /></label>
        <label>Request ID<input value={auditRequestID} onChange={(event) => setAuditRequestID(event.target.value)} /></label>
        <label>Trace ID<input value={auditTraceID} onChange={(event) => setAuditTraceID(event.target.value)} /></label>
        <button onClick={() => void searchAudits()}><Search aria-hidden="true" />查询审计</button>
      </div>
      {audits.length === 0 ? <AsyncState kind="empty" /> : <div className="table-wrap"><table><thead><tr><th>决策</th><th>检查点</th><th>规则/错误</th><th>用户</th><th>Request</th><th>Trace</th><th>成本</th></tr></thead><tbody>{audits.map((item) => <tr key={item.id}><td>{item.decision}</td><td>{item.checkpoint || "-"}</td><td>{item.rule || item.error_type || "-"}</td><td>{item.user_id || "-"}</td><td><code>{item.request_id || "-"}</code></td><td><code>{item.trace_id}</code></td><td>{item.cost}</td></tr>)}</tbody></table></div>}
    </section>}
    {view === "指标与成本" && <section className="governance-band">
		<div className="metrics-filters">
			<label>Agent 应用<select value={metricsAppID} onChange={(event) => setMetricsAppID(event.target.value)}><option value="">全部</option>{apps.map((app) => <option value={app.id} key={app.id}>{app.name}</option>)}</select></label>
			<label>Provider<select value={metricsProvider} onChange={(event) => setMetricsProvider(event.target.value)}><option value="">全部</option><option value="mock">Mock IM</option><option value="enterprise_wechat">Enterprise WeChat</option><option value="telegram">Telegram</option></select></label>
			<label>时间窗<select value={metricsHours} onChange={(event) => setMetricsHours(Number(event.target.value))}><option value={1}>最近 1 小时</option><option value={24}>最近 24 小时</option><option value={168}>最近 7 天</option><option value={720}>最近 30 天</option></select></label>
			<button onClick={() => void refreshMetrics()}><Search aria-hidden="true" />查询指标</button>
		</div>
      <div className="metrics-strip"><Metric icon={<ShieldCheck />} label="请求" value={metrics?.requests ?? 0} /><Metric icon={<Gauge />} label="执行中" value={metrics?.active_executions ?? 0} /><Metric icon={<Check />} label="完成" value={metrics?.completed_executions ?? 0} /><Metric icon={<X />} label="失败" value={metrics?.failed_executions ?? 0} /><Metric icon={<X />} label="拒绝" value={metrics?.denied_requests ?? 0} /><Metric icon={<Gauge />} label="限流" value={metrics?.rate_limited_requests ?? 0} /><Metric icon={<Gauge />} label="模型延迟 ms" value={metrics?.model_latency_ms ?? 0} /><Metric icon={<Gauge />} label="执行延迟 ms" value={metrics?.execution_latency_ms ?? 0} /><Metric icon={<Gauge />} label="Tool 延迟 ms" value={metrics?.tool_latency_ms ?? 0} /><Metric icon={<Gauge />} label="存储延迟 ms" value={metrics?.storage_latency_ms ?? 0} /><Metric icon={<Check />} label="IM 成功" value={metrics?.im_delivered ?? 0} /><Metric icon={<X />} label="IM 失败" value={metrics?.im_failed ?? 0} /><Metric icon={<Gauge />} label="Tokens" value={metrics?.tokens ?? 0} /><Metric icon={<Gauge />} label="剩余 Tokens" value={metrics?.tokens_remaining ?? 0} /><Metric icon={<Gauge />} label="成本" value={metrics?.cost ?? 0} /><Metric icon={<Gauge />} label="剩余成本" value={metrics?.cost_remaining ?? 0} /></div>
		{metrics?.budget_period_from && <small className="metrics-period">预算周期始于 {metrics.budget_period_from}</small>}
      <div className="trace-toolbar"><label>Request 或 Trace ID<input value={traceID} onChange={(event) => setTraceID(event.target.value)} /></label><button onClick={() => void findTrace()} disabled={!traceID}><Search aria-hidden="true" />查询 Trace</button></div>
      {trace && <ol className="trace-list">{trace.spans.map((span, index) => <li key={`${span.name}-${index}`}><span>{index + 1}</span><strong>{span.name}</strong><small className={`status ${span.status === "ok" ? "healthy" : "error"}`}>{span.status}</small></li>)}</ol>}
    </section>}
  </div>;
}

function ListField({ label, value, onChange }: { label: string; value: string[]; onChange: (value: string) => void }) { return <label>{label}<input value={value.join(", ")} onChange={(event) => onChange(event.target.value)} /></label>; }
function NumberField({ label, value, step = "1", onChange }: { label: string; value: number; step?: string; onChange: (value: number) => void }) { return <label>{label}<input type="number" min="0" step={step} value={value} onChange={(event) => onChange(Number(event.target.value))} /></label>; }
function Metric({ icon, label, value }: { icon: React.ReactNode; label: string; value: number }) { return <div className="metric-item">{icon}<span>{label}</span><strong>{value}</strong></div>; }
