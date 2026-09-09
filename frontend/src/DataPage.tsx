import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { Database, RefreshCw } from "lucide-react";
import { api, APIError, type BackendHealth, type Identity, type MemoryRecord, type MigrationResult, type SessionEvent, type SessionState } from "./api";
import { AsyncState } from "./AsyncState";

export function DataPage({ identity }: { identity: Identity }) {
  const [backend, setBackend] = useState<BackendHealth>();
  const [backendChoice, setBackendChoice] = useState("inmemory");
  const [availableBackends, setAvailableBackends] = useState<string[]>(["inmemory"]);
  const [sessionID, setSessionID] = useState("demo-session");
  const [session, setSession] = useState<SessionState>();
  const [events, setEvents] = useState<SessionEvent[]>([]);
  const [memory, setMemory] = useState<MemoryRecord[]>([]);
  const [migration, setMigration] = useState<MigrationResult>();
  const [dryRun, setDryRun] = useState(true);
  const [backendFailed, setBackendFailed] = useState(false);
  const [actionFailed, setActionFailed] = useState(false);
  const [sessionFailed, setSessionFailed] = useState(false);
  const [eventsFailed, setEventsFailed] = useState(false);
  const [memoryFailed, setMemoryFailed] = useState(false);
  const requestGeneration = useRef(0);

  const load = useCallback(async () => {
    const generation = ++requestGeneration.current;
    setBackendFailed(false);
    setActionFailed(false);
    setSessionFailed(false);
    setEventsFailed(false);
    setMemoryFailed(false);
    const [backendResult, sessionResult, eventResult, memoryResult] = await Promise.allSettled([
      api.backend(),
      api.session(sessionID),
      api.sessionEvents(sessionID),
      api.memory(sessionID),
    ]);
    if (generation !== requestGeneration.current) return;

    if (backendResult.status === "fulfilled") {
      setBackend(backendResult.value.health);
      setBackendChoice(backendResult.value.backend);
      setAvailableBackends(backendResult.value.available_backends);
    } else {
      setBackendFailed(true);
    }

    if (sessionResult.status === "fulfilled") {
      setSession(sessionResult.value);
      setSessionFailed(false);
    } else {
      setSession(undefined);
      const failure = sessionResult.reason;
      setSessionFailed(!(failure instanceof APIError && failure.status === 404));
    }
    setEvents(eventResult.status === "fulfilled" ? eventResult.value.items : []);
    setEventsFailed(eventResult.status === "rejected");
    setMemory(memoryResult.status === "fulfilled" ? memoryResult.value.items : []);
    setMemoryFailed(memoryResult.status === "rejected");
  }, [sessionID]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!migration || migration.status !== "running") return;
    const timer = window.setInterval(async () => {
      try { setMigration(await api.migration(migration.id)); } catch { setActionFailed(true); }
    }, 500);
    return () => window.clearInterval(timer);
  }, [migration]);

  const selectBackend = async () => {
    try { setBackend((await api.selectBackend(backendChoice)).health); }
    catch { setActionFailed(true); }
  };
  const createMigration = async () => {
    try {
      setMigration(await api.migrate({ dry_run: dryRun, batch_size: 100, cutover: !dryRun }));
    } catch { setActionFailed(true); }
  };

  if (backendFailed) return <AsyncState kind="error" retry={() => void load()} />;
  if (!backend) return <AsyncState kind="loading" />;
  const canConfigure = identity.active_role === "platform_admin" || identity.active_role === "tenant_admin";
  return <div className="data-page">
    <div className="data-toolbar"><div><strong>数据后端</strong><span className={`status ${backend.status}`}>{backend.status}</span><small>{backend.backend}</small></div><button className="icon-button" title="刷新" onClick={() => void load()}><RefreshCw aria-hidden="true" /></button></div>
    <div className="data-controls">
      <label>后端<select value={backendChoice} onChange={(event) => setBackendChoice(event.target.value)}>{availableBackends.map((backendID)=><option key={backendID} value={backendID}>{backendID}</option>)}</select></label>
      {canConfigure && <button onClick={() => void selectBackend()}>应用后端</button>}
      <label>Session ID<input value={sessionID} onChange={(event) => setSessionID(event.target.value)} /></label>
      {canConfigure && <label className="check-control"><input type="checkbox" checked={dryRun} onChange={(event)=>setDryRun(event.target.checked)} />仅校验</label>}
      {canConfigure && <button className="primary" onClick={() => void createMigration()}><Database aria-hidden="true" />启动迁移</button>}
    </div>
    {actionFailed && <div className="migration-state failed">操作失败，请重试</div>}
    {migration && <div className={`migration-state ${migration.status}`}>迁移 {migration.status}：Session {migration.processed_sessions || 0} / {migration.sessions || 0}，记录 {migration.source_count} / {migration.destination_count}{migration.matched === true ? "，校验一致" : migration.matched === false ? "，存在差异" : ""} {migration.message}</div>}
    <div className="data-grid">
      <DataPanel title="Session / Summary">{sessionFailed ? <AsyncState kind="error" /> : session ? <dl><dt>事件数</dt><dd>{session.event_count}</dd><dt>序列</dt><dd>{session.sequence}</dd><dt>摘要检查点</dt><dd>{session.summary_source_sequence || 0}</dd><dt>Summary</dt><dd>{session.summary || "暂无"}</dd></dl> : <AsyncState kind="empty" />}</DataPanel>
      <DataPanel title="Session Events">{eventsFailed ? <AsyncState kind="error" /> : events.length ? <ol className="event-list">{events.map((event) => <li key={event.id}><code>#{event.sequence}</code> {event.type}<span>{event.payload}</span></li>)}</ol> : <AsyncState kind="empty" />}</DataPanel>
      <DataPanel title="Memory">{memoryFailed ? <AsyncState kind="error" /> : memory.length ? <dl>{memory.map((item) => <div key={item.id}><dt>{item.key}</dt><dd>{item.value}</dd></div>)}</dl> : <AsyncState kind="empty" />}</DataPanel>
    </div>
  </div>;
}

function DataPanel({ title, children }: { title: string; children: ReactNode }) { return <section className="runtime-item"><h3>{title}</h3>{children}</section>; }
