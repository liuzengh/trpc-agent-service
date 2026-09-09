import { useEffect, useRef, useState, type FormEvent } from "react";
import { Building2, Plus, X } from "lucide-react";
import { APIError, api, type Identity, type Tenant } from "./api";
import { AsyncState } from "./AsyncState";

export function TenantsPage({ identity, identityChanged }: { identity: Identity; identityChanged: () => void }) {
  const [items, setItems] = useState<Tenant[]>();
  const [selected, setSelected] = useState<Tenant>();
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<APIError>();
  const requestGeneration = useRef(0);

  const load = () => {
	const generation = ++requestGeneration.current;
    setError(undefined);
	api.tenants().then(({ items }) => { if (requestGeneration.current === generation) setItems(items); }).catch((caught) => { if (requestGeneration.current === generation) setError(caught); });
  };
  useEffect(() => { load(); return () => { requestGeneration.current++; }; }, [identity.active_tenant_id]);

  const create = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    try {
      const tenant = await api.createTenant({ id: String(data.get("id")), name: String(data.get("name")) });
      setItems((current) => [...(current ?? []), tenant].sort((a, b) => a.id.localeCompare(b.id)));
      setSelected(tenant);
      setCreating(false);
      identityChanged();
    } catch (caught) {
      setError(caught as APIError);
    }
  };

  if (!items && !error) return <AsyncState kind="loading" />;
  if (error?.status === 403) return <AsyncState kind="forbidden" />;
  if (error && !creating) return <AsyncState kind="error" retry={load} />;

  return (
    <div className="resource-layout">
      <section className="resource-list">
        <div className="section-toolbar">
          <span>{items?.length ?? 0} 个租户</span>
          {identity.active_role === "platform_admin" && <button className="primary" onClick={() => { setError(undefined); setCreating(true); }}><Plus aria-hidden="true" />新建租户</button>}
        </div>
        {!items?.length ? <AsyncState kind="empty" /> : (
          <div className="table-wrap"><table><thead><tr><th>名称</th><th>标识</th><th>创建时间</th></tr></thead>
            <tbody>{items.map((tenant) => <tr key={tenant.id} onClick={() => setSelected(tenant)} tabIndex={0}><td><Building2 aria-hidden="true" />{tenant.name}</td><td><code>{tenant.id}</code></td><td>{new Date(tenant.created_at).toLocaleString()}</td></tr>)}</tbody>
          </table></div>
        )}
      </section>
      {selected && <aside className="detail-panel"><button className="icon-button" title="关闭详情" onClick={() => setSelected(undefined)}><X aria-hidden="true" /></button><span className="eyebrow">Tenant</span><h3>{selected.name}</h3><dl><dt>标识</dt><dd><code>{selected.id}</code></dd><dt>创建时间</dt><dd>{new Date(selected.created_at).toLocaleString()}</dd></dl></aside>}
      {creating && <div className="modal-backdrop"><form className="modal" onSubmit={(event) => void create(event)}><div className="modal-title"><div><span className="eyebrow">平台资源</span><h3>新建租户</h3></div><button type="button" className="icon-button" title="关闭" onClick={() => setCreating(false)}><X /></button></div><label>租户标识<input name="id" required pattern="[a-z][a-z0-9-]{2,62}" placeholder="team-east" /></label><label>显示名称<input name="name" required minLength={2} maxLength={80} placeholder="华东业务团队" /></label>{error && <p className="form-error" role="alert">{error.message}</p>}<div className="form-actions"><button type="button" onClick={() => setCreating(false)}>取消</button><button className="primary" type="submit">创建租户</button></div></form></div>}
    </div>
  );
}
