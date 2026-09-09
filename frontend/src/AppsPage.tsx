import { useEffect, useRef, useState, type FormEvent } from "react";
import { Boxes, MessageSquare, Plus, X } from "lucide-react";
import { APIError, api, type AgentApp, type Identity } from "./api";
import { AsyncState } from "./AsyncState";

export function AppsPage({ identity, onOpenChat }: { identity: Identity; onOpenChat?: (appID: string) => void }) {
  const [items, setItems] = useState<AgentApp[]>(); const [selected, setSelected] = useState<AgentApp>(); const [creating, setCreating] = useState(false); const [error, setError] = useState<APIError>(); const requestGeneration = useRef(0);
  const load = () => { const generation = ++requestGeneration.current; setError(undefined); api.apps().then(({ items }) => { if (requestGeneration.current === generation) setItems(items); }).catch((caught) => { if (requestGeneration.current === generation) setError(caught); }); };
  useEffect(() => { setSelected(undefined); load(); return () => { requestGeneration.current++; }; }, [identity.active_tenant_id]);
  const create = async (event: FormEvent<HTMLFormElement>) => { event.preventDefault(); const data = new FormData(event.currentTarget); try { const app = await api.createApp({ id: String(data.get("id")), name: String(data.get("name")) }); setItems((current) => [...(current ?? []), app]); setCreating(false); } catch (caught) { setError(caught as APIError); } };
  if (!items && !error) return <AsyncState kind="loading" />; if (error?.status === 403 && !creating) return <AsyncState kind="forbidden" />; if (error && !creating) return <AsyncState kind="error" retry={load} />;
  const mutable = identity.active_role === "platform_admin" || identity.active_role === "tenant_admin";
  return <div className="resource-layout"><section className="resource-list"><div className="section-toolbar"><span>{items?.length ?? 0} 个 Agent 应用</span>{mutable && <button className="primary" onClick={() => setCreating(true)}><Plus />新建应用</button>}</div>
    {!items?.length ? <AsyncState kind="empty" /> : <div className="table-wrap"><table><thead><tr><th>名称</th><th>标识</th><th>租户</th></tr></thead><tbody>{items.map((app) => <tr key={app.id} onClick={() => setSelected(app)}><td><Boxes />{app.name}</td><td><code>{app.id}</code></td><td><code>{app.tenant_id}</code></td></tr>)}</tbody></table></div>}
    </section>{selected && <aside className="detail-panel"><button className="icon-button" title="关闭详情" onClick={() => setSelected(undefined)}><X /></button><span className="eyebrow">Agent App</span><h3>{selected.name}</h3><dl><dt>应用标识</dt><dd><code>{selected.id}</code></dd><dt>所属租户</dt><dd><code>{selected.tenant_id}</code></dd><dt>创建时间</dt><dd>{new Date(selected.created_at).toLocaleString()}</dd></dl><button className="primary" onClick={() => onOpenChat?.(selected.id)}><MessageSquare aria-hidden="true" />打开 Chat</button></aside>}
    {creating && <div className="modal-backdrop"><form className="modal" onSubmit={(event) => void create(event)}><div className="modal-title"><h3>新建 Agent 应用</h3><button type="button" className="icon-button" title="关闭" onClick={() => setCreating(false)}><X /></button></div><label>应用标识<input name="id" required pattern="[a-z][a-z0-9-]{2,62}" /></label><label>显示名称<input name="name" required minLength={2} maxLength={80} /></label>{error && <p className="form-error">{error.message}</p>}<div className="form-actions"><button type="button" onClick={() => setCreating(false)}>取消</button><button className="primary">创建应用</button></div></form></div>}
  </div>;
}
