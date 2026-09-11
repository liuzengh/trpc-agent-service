"use client";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { controlApi } from "../../lib/control-api";
import { runtimeProfileApi } from "../../lib/runtime-profile-api";
import { deploymentRead, deploymentError } from "../../lib/deployment-api";
import type { Selection } from "../../lib/deployment-editor-state";
import { Button } from "../ui";
import styles from "./deployment.module.css";

type Choice = { value: string; label: string };
function PagedSelect({ label, value, disabled, load, onChange, resolve }: {
  label: string; value: string; disabled: boolean; load(offset: number): Promise<{ choices: Choice[]; total: number }>; onChange(value: string): void; resolve?(value: string): Promise<string>;
}) {
  const [offset, setOffset] = useState(0); const [epoch, setEpoch] = useState(0);
  const [options, setOptions] = useState<Choice[]>([]); const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true); const [error, setError] = useState("");
  const [fixedLabel, setFixedLabel] = useState("");
  useEffect(() => {
    let current = true; setFixedLabel("");
    if (!value || loading || options.some((option) => option.value === value) || !resolve) return;
    void deploymentRead(resolve(value)).then((name) => { if (current) setFixedLabel(name); }).catch((e) => { if (current) setError(deploymentError(e)); });
    return () => { current = false; };
  }, [value, options, resolve, loading, epoch]);
  useEffect(() => {
    let current = true; setLoading(true); setError("");
    void deploymentRead(load(offset)).then((page) => { if (current) { setOptions(page.choices); setTotal(page.total); } }).catch((e) => { if (current) setError(deploymentError(e)); }).finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [offset, epoch, load]);
  return <div><label className={styles.field}><span>{label}</span><select title={fixedLabel || options.find((option) => option.value === value)?.label || label} aria-label={label} value={value} disabled={disabled || loading || !!error} onChange={(event) => onChange(event.target.value)}>
    <option value="">{loading ? "正在读取…" : `请选择${label}`}</option>
    {value && !options.some((option) => option.value === value) && <option value={value}>{fixedLabel || (label === "Agent 固定版本" ? `v${value}` : label === "Profile 固定版本" ? `r${value}` : value)} · 当前固定选择</option>}
    {options.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
  </select></label>
    {error && <div className={styles.error} role="alert">{error} <Button variant="secondary" onClick={() => setEpoch((n) => n + 1)}>重试读取{label}</Button></div>}
    {!loading && !error && total === 0 && <p className={styles.small}>暂无可选{label}，请先在来源工作台创建并发布。</p>}
    {total > 20 && <div className={styles.actions}><Button type="button" variant="ghost" disabled={disabled || loading || offset === 0} onClick={() => setOffset(offset - 20)}>上一页{label}</Button><span className={styles.small}>{offset + 1}–{Math.min(total, offset + 20)} / {total}</span><Button type="button" variant="ghost" disabled={disabled || loading || offset + 20 >= total} onClick={() => setOffset(offset + 20)}>下一页{label}</Button></div>}
  </div>;
}
// Stable loader identities prevent selection changes from restarting unrelated queries.
export function SourceSelector({ tenantId, value, onChange, disabled }: { tenantId: string; value: Selection; onChange(value: Selection): void; disabled: boolean }) {
  const agents = useCallback(async (offset: number) => { const p = await controlApi.listAgents(tenantId, { offset, limit: 20 }); return { choices: p.agents.map((a) => ({ value: a.id, label: `${a.name}${a.latest_version_number ? ` · 最新 v${a.latest_version_number}` : " · 尚未发布"}` })), total: p.total }; }, [tenantId]);
  const profiles = useCallback(async (offset: number) => { const p = await runtimeProfileApi.listProfiles(tenantId, { offset, limit: 20 }); return { choices: p.runtime_profiles.map((a) => ({ value: a.id, label: `${a.name}${a.latest_revision_number ? ` · 最新 r${a.latest_revision_number}` : " · 尚未发布"}` })), total: p.total }; }, [tenantId]);
  const versions = useCallback(async (offset: number) => { const p = await controlApi.listAgentVersions(tenantId, value.agentId, { offset, limit: 20 }); return { choices: p.versions.map((v) => ({ value: String(v.version_number), label: `v${v.version_number} · ${new Date(v.published_at).toLocaleString("zh-CN")}` })), total: p.total }; }, [tenantId, value.agentId]);
  const revisions = useCallback(async (offset: number) => { const p = await runtimeProfileApi.listRevisions(tenantId, value.profileId, { offset, limit: 20 }); return { choices: p.revisions.map((v) => ({ value: String(v.revision_number), label: `r${v.revision_number} · ${new Date(v.published_at).toLocaleString("zh-CN")}` })), total: p.total }; }, [tenantId, value.profileId]);
  const resolveAgent = useCallback(async (id: string) => (await controlApi.getAgent(tenantId, id)).name, [tenantId]);
  const resolveProfile = useCallback(async (id: string) => (await runtimeProfileApi.getProfile(tenantId, id)).name, [tenantId]);
  return <div className={styles.sources}>
    <section className={styles.card}><h2><span className={styles.number}>1</span>Agent 版本</h2><p className={styles.small}>确定节点逻辑和资源需求。仅使用已发布版本。</p>
      <PagedSelect label="Agent" value={value.agentId} disabled={disabled} load={agents} resolve={resolveAgent} onChange={(agentId) => onChange({ ...value, agentId, agentVersion: 0 })} />
      {value.agentId && <PagedSelect key={value.agentId} label="Agent 固定版本" value={value.agentVersion ? String(value.agentVersion) : ""} disabled={disabled} load={versions} onChange={(v) => onChange({ ...value, agentVersion: Number(v) })} />}
      <Link className={styles.link} href={`/tenants/${encodeURIComponent(tenantId)}/agents${value.agentId ? `/${encodeURIComponent(value.agentId)}${value.agentVersion ? `/versions/${value.agentVersion}` : ""}` : ""}`}>查看 Agent 来源 →</Link>
    </section>
    <section className={styles.card}><h2><span className={styles.number}>2</span>运行配置版本</h2><p className={styles.small}>提供同名资源、具体连接方式和凭据关联。</p>
      <PagedSelect label="运行配置" value={value.profileId} disabled={disabled} load={profiles} resolve={resolveProfile} onChange={(profileId) => onChange({ ...value, profileId, profileRevision: 0 })} />
      {value.profileId && <PagedSelect key={value.profileId} label="Profile 固定版本" value={value.profileRevision ? String(value.profileRevision) : ""} disabled={disabled} load={revisions} onChange={(v) => onChange({ ...value, profileRevision: Number(v) })} />}
      <Link className={styles.link} href={`/tenants/${encodeURIComponent(tenantId)}/runtime-profiles${value.profileId ? `/${encodeURIComponent(value.profileId)}${value.profileRevision ? `/revisions/${value.profileRevision}` : ""}` : ""}`}>查看运行配置来源 →</Link>
    </section>
  </div>;
}
