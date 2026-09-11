"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { Plus } from "lucide-react";
import { useCallback, useEffect, useId, useRef, useState, type ReactNode } from "react";
import { runtimeProfileApi, type RuntimeProfile } from "../../lib/runtime-profile-api";
import { ApiNotice, Button, EmptyState, Field, PageHeader, StatusBadge } from "../ui";
import { ProfileDialog } from "./profile-dialog";
import styles from "./profile-workspace.module.css";

export const profileHref = (tenantId: string, profileId?: string) => `/tenants/${encodeURIComponent(tenantId)}/runtime-profiles${profileId ? `/${encodeURIComponent(profileId)}` : ""}`;
type MetadataInput = { name: string; description: string };
export function ProfileMetadataDialog({ profile, busy, error, onClose, onSave, submitDisabled = false, recoveryAction, initialInput }: {
  profile?: RuntimeProfile; busy: boolean; error?: unknown; onClose: () => void; onSave: (input: MetadataInput) => void;
  submitDisabled?: boolean; recoveryAction?: ReactNode; initialInput?: MetadataInput;
}) {
  const [name, setName] = useState(profile?.name ?? initialInput?.name ?? "");
  const [description, setDescription] = useState(profile?.description ?? initialInput?.description ?? "");
  const formId = useId();
  return <ProfileDialog title={profile ? "编辑配置信息" : "新建运行配置"} description="运行配置提供模型、工具、知识与存储的具体连接方式。创建后在草稿中添加资源。" busy={busy} onClose={onClose}
    footer={<><Button type="button" variant="secondary" disabled={busy} onClick={onClose}>取消</Button><Button type="submit" form={formId} disabled={busy || submitDisabled || !name.trim()}>{busy ? "提交中…" : profile ? "保存信息" : "创建配置"}</Button></>}>
    <ApiNotice error={error} />
    {recoveryAction}
    <form id={formId} onSubmit={(event) => { event.preventDefault(); if (!busy && !submitDisabled && name.trim()) onSave({ name: name.trim(), description }); }}>
      <Field label="配置名称" required maxLength={128} value={name} disabled={busy} onChange={(event) => setName(event.target.value)} placeholder="例如：研究工作流运行配置" />
      <label className="field"><span>描述</span><textarea maxLength={4096} value={description} disabled={busy} onChange={(event) => setDescription(event.target.value)} placeholder="说明资源用途与适用场景" /></label>
    </form>
  </ProfileDialog>;
}

export function ProfileList({ tenantId }: { tenantId: string }) {
  const router = useRouter();
  const [profiles, setProfiles] = useState<RuntimeProfile[]>([]);
  const [offset, setOffset] = useState(0); const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true); const [error, setError] = useState<unknown>();
  const [create, setCreate] = useState(false); const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<unknown>();
  const [uncertainCreate, setUncertainCreate] = useState<{ input: MetadataInput; reviewed: boolean } | null>(null);
  const [initialCreateInput, setInitialCreateInput] = useState<MetadataInput>();
  const sequence = useRef(0); const createLock = useRef(false);
  const load = useCallback(async () => {
    const id = ++sequence.current; setLoading(true); setError(undefined);
    try { const page = await runtimeProfileApi.listProfiles(tenantId, { offset, limit: 20 }); if (sequence.current === id) { setProfiles(page.runtime_profiles); setTotal(page.total); return true; } return false; }
    catch (caught) { if (sequence.current === id) setError(caught); return false; }
    finally { if (sequence.current === id) setLoading(false); }
  }, [tenantId, offset]);
  useEffect(() => { void load(); return () => { sequence.current++; }; }, [load]);
  async function createProfile(input: { name: string; description: string }) {
    if (createLock.current || uncertainCreate) return; createLock.current = true; setCreating(true); setCreateError(undefined);
    try { const result = await runtimeProfileApi.createProfile(tenantId, input); setCreate(false); router.push(profileHref(tenantId, result.profile.id)); }
    catch (caught) {
      const status = typeof caught === "object" && caught !== null && "status" in caught && typeof caught.status === "number" ? caught.status : undefined;
      if (status !== undefined && status >= 400 && status < 500 && status !== 408) {
        setCreateError(new Error(status === 401 ? "本次创建未通过身份校验，请重新登录后重试。" : status === 403 ? "本次创建被拒绝，当前账户没有操作权限；请核对租户和账户。" : "本次创建被拒绝，请检查名称、描述及当前租户后重试。"));
      } else {
        setUncertainCreate({ input, reviewed: false });
        setCreateError(new Error("创建结果未确认，已暂停再次提交。请关闭并刷新列表核实是否已创建，避免产生重复配置。"));
      }
    }
    finally { createLock.current = false; setCreating(false); }
  }
  async function reviewCreation() {
    if (loading || creating || !uncertainCreate) return;
    setCreate(false);
    setUncertainCreate((current) => current ? { ...current, reviewed: false } : null);
    if (await load()) setUncertainCreate((current) => current ? { ...current, reviewed: true } : null);
  }
  return <div className={styles.page}>
    <PageHeader eyebrow="RUNTIME PROFILE" title="运行配置" description="配置模型、工具、知识与存储的连接方式。发布配置不等于部署或连接成功。" action={<Button disabled={!!uncertainCreate} onClick={() => { setInitialCreateInput(undefined); setCreateError(undefined); setCreate(true); }}><Plus size={16} />新建运行配置</Button>} />
    <div className={styles.info}>Agent 定义逻辑槽位，运行配置提供具体资源。可以保留草稿，同时继续使用已发布的不可变版本。</div>
    {uncertainCreate && !create && <section className={styles.warning} aria-label="核实创建结果">
      <p>“{uncertainCreate.input.name}”的创建结果尚未确认。请按名称和创建时间核实列表；列表按页读取，必要时翻页检查，不会自动判定该配置不存在。</p>
      <div className={styles.actions}><Button variant="secondary" disabled={loading} onClick={() => void reviewCreation()}>{loading ? "读取中…" : "刷新列表核实创建结果"}</Button>
        <Button variant="secondary" disabled={!uncertainCreate.reviewed || loading || !!error} onClick={() => { setUncertainCreate(null); setCreateError(undefined); }}>已找到配置，结束核实</Button>
        <Button variant="danger" disabled={!uncertainCreate.reviewed || loading || !!error} onClick={() => { setInitialCreateInput(uncertainCreate.input); setUncertainCreate(null); setCreateError(undefined); setCreate(true); }}>已核实列表，仍要重新创建</Button></div>
      {uncertainCreate.reviewed && <p>重新创建是新的操作，可能产生另一份配置；请先确认现有记录再继续。</p>}
    </section>}
    <ApiNotice error={error} />
    {!!error && <Button variant="secondary" onClick={() => void load()}>重新读取列表</Button>}
    <section className="panel" aria-label="运行配置列表">
      {loading ? <div className="panel-loading" role="status">正在读取运行配置…</div> : error ? null : profiles.length === 0 ? <EmptyState title="还没有运行配置" detail="新建配置后，添加所需资源，保存并校验，再发布不可变版本。" /> :
        <div className="table-wrap"><table><thead><tr><th>名称 / 描述</th><th>最新发布版本</th><th>创建者</th><th>更新时间</th><th>操作</th></tr></thead><tbody>
          {profiles.map((profile) => <tr key={profile.id}><td><div className="table-copy"><strong>{profile.name}</strong><small>{profile.description || "暂无描述"}</small></div></td><td><StatusBadge tone={profile.latest_revision_number ? "green" : "gray"}>{profile.latest_revision_number ? `r${profile.latest_revision_number}` : "尚未发布"}</StatusBadge></td><td className={styles.identifier}>{profile.created_by}</td><td>{new Date(profile.updated_at).toLocaleString("zh-CN")}</td><td><Link className="text-link" href={profileHref(tenantId, profile.id)}>编辑配置 →</Link></td></tr>)}
        </tbody></table></div>}
      {!loading && !error && total > 0 && <div className="pagination"><span>第 {offset + 1}–{Math.min(offset + 20, total)} 条，共 {total} 条</span><div className="pagination-actions"><Button variant="secondary" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 20))}>上一页</Button><Button variant="secondary" disabled={offset + 20 >= total} onClick={() => setOffset(offset + 20)}>下一页</Button></div></div>}
    </section>
    <div className={styles.steps}><span><strong>1 · 编辑草稿</strong>添加资源并保存配置</span><span><strong>2 · 静态校验</strong>核对结构与必填字段</span><span><strong>3 · 发布版本</strong>保留不可变配置快照</span></div>
    {create && <ProfileMetadataDialog busy={creating} error={createError} initialInput={initialCreateInput} submitDisabled={!!uncertainCreate} recoveryAction={uncertainCreate && <Button variant="secondary" disabled={loading} onClick={() => void reviewCreation()}>关闭并刷新列表核实</Button>} onClose={() => setCreate(false)} onSave={(input) => void createProfile(input)} />}
  </div>;
}
