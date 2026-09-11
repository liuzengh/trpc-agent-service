"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { controlApi, type Tenant } from "../../lib/control-api";
import { runtimeProfileApi, RuntimeProfileApiError, type RuntimeProfile, type ProfileDraft, type ProfileConfig, type CredentialActions, type ProfileWrite, type ProfileValidationReport, type ProfileValidationDiagnostic, type ResourceCategory, type ProfileRevisionPage } from "../../lib/runtime-profile-api";
import { ApiNotice, Button, EmptyState, StatusBadge } from "../ui";
import { DeploymentReturnLink } from "../deployments/return-link";
import { ProfileDialog } from "./profile-dialog";
import { ProfileMetadataDialog, profileHref } from "./profile-list";
import { ProfileResourceEditor } from "./profile-resource-editor";
import styles from "./profile-workspace.module.css";

type FocusTarget = { category: ResourceCategory; name: string; pointer?: string };
export function diagnosticTarget(diagnostic: ProfileValidationDiagnostic): FocusTarget | null {
  const categories: Record<string, ResourceCategory> = { model: "models", tool: "tools", knowledge: "knowledge", storage: "storage", executor: "executors" };
  const parts = diagnostic.pointer.split("/").slice(1).map((part) => part.replace(/~1/g, "/").replace(/~0/g, "~"));
  if (parts[0] === "config") parts.shift();
  const category = diagnostic.resource_kind ? categories[diagnostic.resource_kind] : ["models", "tools", "knowledge", "storage", "executors"].includes(parts[0]) ? parts[0] as ResourceCategory : undefined;
  const name = diagnostic.resource_key ?? parts[1];
  return category && name ? { category, name, pointer: diagnostic.pointer } : null;
}
function hasCredentialWrites(credentials: CredentialActions) {
  return Object.values(credentials).some((resources) => Object.values(resources).some((purposes) => Object.values(purposes).some((action) => action.action !== "keep")));
}

export function ProfileWorkspace({ tenantId, profileId }: { tenantId: string; profileId: string }) {
  const router = useRouter();
  const [profile, setProfile] = useState<RuntimeProfile | null>(null);
  const [draft, setDraft] = useState<ProfileDraft | null>(null);
  const [config, setConfig] = useState<ProfileConfig | null>(null);
  const [credentials, setCredentials] = useState<CredentialActions>({});
  const [tenant, setTenant] = useState<Tenant | null>(null);
  const [loading, setLoading] = useState(true); const [busy, setBusy] = useState("");
  const [error, setError] = useState<unknown>(); const [notice, setNotice] = useState("");
  const [report, setReport] = useState<ProfileValidationReport | null>(null);
  const [focusTarget, setFocusTarget] = useState<FocusTarget | null>(null);
  const [conflict, setConflict] = useState(false); const [refreshNeeded, setRefreshNeeded] = useState(false);
  const [confirmReload, setConfirmReload] = useState(false); const [leaveHref, setLeaveHref] = useState<string | null>(null);
  const [editingMetadata, setEditingMetadata] = useState(false); const [metadataError, setMetadataError] = useState<unknown>();
  const [tab, setTab] = useState<"config" | "history">("config"); const [historyEpoch, setHistoryEpoch] = useState(0);
  const [published, setPublished] = useState<number | null>(null); const [showJson, setShowJson] = useState(false);
  const sequence = useRef(0); const lock = useRef(false);
  const pendingSave = useRef<{ body: ProfileWrite; fingerprint: string; key: string } | null>(null);
  const dirty = !!draft && !!config && (JSON.stringify(config) !== JSON.stringify(draft.config) || hasCredentialWrites(credentials));
  const navigating = useRef(false); const currentDirty = useRef(false); currentDirty.current = dirty;
  const href = profileHref(tenantId, profileId);

  const load = useCallback(async () => {
    const id = ++sequence.current; setLoading(true); setError(undefined);
    try {
      const [nextProfile, nextDraft, nextTenant] = await Promise.all([runtimeProfileApi.getProfile(tenantId, profileId), runtimeProfileApi.getDraft(tenantId, profileId), controlApi.getTenant(tenantId)]);
      if (sequence.current !== id) return;
      setProfile(nextProfile); setDraft(nextDraft); setConfig(nextDraft.config); setTenant(nextTenant);
      setCredentials({}); pendingSave.current = null; setConflict(false); setRefreshNeeded(false); setReport(null);
    } catch (caught) { if (sequence.current === id) setError(caught); }
    finally { if (sequence.current === id) setLoading(false); }
  }, [tenantId, profileId]);
  useEffect(() => { void load(); return () => { sequence.current++; pendingSave.current = null; }; }, [load]);
  useEffect(() => {
    const unload = (event: BeforeUnloadEvent) => { if (currentDirty.current) { event.preventDefault(); event.returnValue = ""; } };
    const click = (event: MouseEvent) => {
      if (!currentDirty.current || navigating.current || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      const anchor = event.target instanceof Element ? event.target.closest("a[href]") : null;
      if (!(anchor instanceof HTMLAnchorElement) || anchor.target === "_blank" || anchor.hasAttribute("download")) return;
      const destination = new URL(anchor.href, location.href);
      if (destination.href === location.href || destination.protocol !== "http:" && destination.protocol !== "https:") return;
      event.preventDefault(); event.stopPropagation(); setLeaveHref(destination.href);
    };
    window.addEventListener("beforeunload", unload); document.addEventListener("click", click, true);
    return () => { window.removeEventListener("beforeunload", unload); document.removeEventListener("click", click, true); };
  }, []);

  function edit(nextConfig: ProfileConfig, nextCredentials: CredentialActions) {
    setConfig(nextConfig); setCredentials(nextCredentials); setReport(null); setNotice(""); setPublished(null); setError(undefined); pendingSave.current = null;
  }
  async function saveIfNeeded(): Promise<ProfileDraft | null> {
    if (!draft || !config) return null;
    if (!dirty) return draft;
    const body: ProfileWrite = { expected_draft_revision: draft.draft_revision, credential_protocol_version: "v1", config, ...(hasCredentialWrites(credentials) ? { credentials } : {}) };
    const fingerprint = JSON.stringify(body);
    if (!pendingSave.current || pendingSave.current.fingerprint !== fingerprint) pendingSave.current = { body, fingerprint, key: crypto.randomUUID() };
    const pending = pendingSave.current;
    const receipt = await runtimeProfileApi.saveDraft(tenantId, profileId, pending.body, pending.key);
    pendingSave.current = null; setCredentials({}); setReport(null);
    // Commit the successful receipt locally before reading dynamic state. A failed
    // follow-up GET must never turn an accepted write into a second mutation.
    setDraft({ ...draft, draft_revision: receipt.draft_revision, updated_at: receipt.updated_at, config, credential_states: undefined });
    try {
      const next = await runtimeProfileApi.getDraft(tenantId, profileId);
      if (next.draft_revision !== receipt.draft_revision) {
        setConflict(true);
        setNotice(`草稿 r${receipt.draft_revision} 已保存，但服务器已更新到 r${next.draft_revision}。本次校验/发布已停止，保留刚保存的非密钥配置；请重新读取并确认最新草稿。`);
        return null;
      }
      setDraft(next); setConfig(next.config); return next;
    } catch {
      setRefreshNeeded(true); setNotice("草稿已保存；最新凭证状态读取失败。请重新读取状态，不要重复提交。"); return null;
    }
  }
  async function act(action: "save" | "validate" | "publish") {
    if (lock.current || conflict || refreshNeeded) return;
    lock.current = true; setBusy(action); setError(undefined); setNotice(""); setPublished(null);
    try {
      const saved = await saveIfNeeded();
      if (!saved) return;
      if (action === "save") { setNotice(`草稿 r${saved.draft_revision} 已保存。`); return; }
      const result = await runtimeProfileApi.validateDraft(tenantId, profileId, { expected_revision: saved.draft_revision });
      setReport(result);
      if (!result.valid) { setNotice("校验未通过，请按右侧诊断修正；已保存的草稿会保留。"); return; }
      if (action === "validate") { setNotice(`草稿 r${saved.draft_revision} 静态校验通过；尚未验证远端连接。`); return; }
      const resultPublish = await runtimeProfileApi.publishRevision(tenantId, profileId, { expected_revision: saved.draft_revision });
      setProfile((previous) => previous ? { ...previous, latest_revision_number: resultPublish.revision.revision_number } : previous);
      setPublished(resultPublish.revision.revision_number); setHistoryEpoch((value) => value + 1);
      setNotice(`已发布不可变版本 r${resultPublish.revision.revision_number}。发布不会部署 Agent，也不代表远端连接成功。`);
    } catch (caught) {
      if (caught instanceof RuntimeProfileApiError && caught.validation) setReport(caught.validation);
      if (caught instanceof RuntimeProfileApiError && caught.status === 409) {
        setConflict(true); setCredentials({}); pendingSave.current = null;
        setNotice("版本或凭证关联发生冲突。本地非密钥配置已保留，待提交凭证已清空；请查看/复制配置后重新读取最新草稿。");
      } else if (caught instanceof RuntimeProfileApiError && caught.status < 500) pendingSave.current = null;
      setError(caught);
    } finally { setBusy(""); lock.current = false; }
  }
  async function refreshState() {
    if (lock.current) return; lock.current = true; setBusy("refresh"); setError(undefined);
    try { const next = await runtimeProfileApi.getDraft(tenantId, profileId); setDraft(next); setConfig(next.config); setCredentials({}); setRefreshNeeded(false); setConflict(false); setReport(null); pendingSave.current = null; setNotice(`已读取最新草稿 r${next.draft_revision}。`); }
    catch (caught) { setError(caught); }
    finally { setBusy(""); lock.current = false; }
  }
  async function updateMetadata(input: { name: string; description: string }) {
    if (lock.current) return; lock.current = true; setBusy("metadata"); setMetadataError(undefined);
    try { setProfile(await runtimeProfileApi.updateProfile(tenantId, profileId, input)); setEditingMetadata(false); }
    catch (caught) { setMetadataError(caught); }
    finally { setBusy(""); lock.current = false; }
  }

  if (loading) return <div role="status" className="panel-loading">正在读取运行配置…</div>;
  if (!profile || !draft || !config || !tenant) return <><ApiNotice error={error} /><Button variant="secondary" onClick={() => void load()}>重新读取运行配置</Button></>;
  const disabled = !!busy || refreshNeeded;
  return <div className={styles.page}>
    <DeploymentReturnLink tenantId={tenantId} onFocus={setFocusTarget} />
    <header className={styles.header}><div><Link className="text-link" href={profileHref(tenantId)}>← 运行配置列表</Link><h1>{profile.name}</h1><p>{profile.description || "配置模型、工具、知识与存储资源。"}</p><div className={styles.badges}><StatusBadge tone="blue">Draft r{draft.draft_revision}</StatusBadge><StatusBadge tone="gray">{profile.latest_revision_number ? `最新版本 r${profile.latest_revision_number}` : "尚未发布"}</StatusBadge><StatusBadge tone={dirty ? "amber" : "green"}>{dirty ? "未保存" : "已保存"}</StatusBadge><StatusBadge tone="gray">{tenant.role ?? "MEMBER"}</StatusBadge></div></div>
      <div className={styles.actions}><Button variant="ghost" disabled={disabled} onClick={() => { setMetadataError(undefined); setEditingMetadata(true); }}>编辑信息</Button><Button variant="secondary" disabled={disabled || conflict || !dirty} onClick={() => void act("save")}>{busy === "save" ? "保存中…" : "保存草稿"}</Button><Button variant="secondary" disabled={disabled || conflict} onClick={() => void act("validate")}>{busy === "validate" ? "校验中…" : "校验"}</Button><Button disabled={disabled || conflict} onClick={() => void act("publish")}>{busy === "publish" ? "发布中…" : "校验并发布"}</Button></div>
    </header>
    <ApiNotice error={error} />
    {notice && <div role="status" className={conflict || refreshNeeded ? styles.warning : styles.success}>{notice}{published && <> <Link href={`${href}/revisions/${published}`}>查看版本 r{published} →</Link></>}</div>}
    {refreshNeeded && <Button variant="secondary" disabled={!!busy} onClick={() => void refreshState()}>重新读取已保存状态</Button>}
    {conflict && <div className={styles.actions}><Button variant="secondary" disabled={!!busy} onClick={() => setConfirmReload(true)}>重新读取最新草稿</Button><Button variant="ghost" onClick={() => setShowJson(true)}>查看保留的非密钥配置</Button></div>}
    <div className={styles.tabs} role="tablist" aria-label="运行配置内容"><button role="tab" aria-selected={tab === "config"} onClick={() => setTab("config")}>配置</button><button role="tab" aria-selected={tab === "history"} onClick={() => setTab("history")}>版本记录</button></div>
    {tab === "config" ? <div className={styles.editorGrid}><div><ProfileResourceEditor tenantId={tenantId} config={config} credentials={credentials} credentialStates={draft.credential_states ?? {}} onChange={edit} isOwner={tenant.role === "OWNER"} disabled={disabled} focusTarget={focusTarget} /></div>
      <aside className={styles.sidePanels} aria-label="静态校验结果"><section className={styles.diagnostics}><h2>静态校验</h2>{report ? <><StatusBadge tone={report.valid ? "green" : "amber"}>{report.valid ? "校验通过" : "校验未通过"}</StatusBadge><p>对应 Draft r{report.draft_revision} · {report.diagnostics.filter((item) => item.severity === "error").length} 错误 · {report.diagnostics.filter((item) => item.severity === "warning").length} 警告</p><ul>{report.diagnostics.map((item, index) => <li key={`${item.pointer}-${item.code}-${index}`}><button type="button" onClick={() => setFocusTarget(diagnosticTarget(item))}><strong>{item.resource_key ?? "配置"} · {item.severity === "error" ? "错误" : "警告"}</strong>{item.message}<code>{item.pointer}</code><small>{item.code}</small></button></li>)}</ul></> : <p>{dirty ? "有未保存修改，校验或发布时会先保存。" : "点击校验，检查当前草稿的结构和必填字段。"}</p>}<hr /><p>静态校验不访问模型、MCP、Qdrant 或数据库，不表示连接可用。</p></section><section className={styles.releaseNote} aria-label="发布说明"><h2>发布说明</h2><p>发布后配置不可变。OWNER 可单独更新已关联的活跃凭证；发布运行配置不会绑定或部署 Agent。</p>{tenant.role !== "OWNER" && <p>MEMBER 可编辑非密钥配置、校验和发布；凭证变更需 OWNER。</p>}</section></aside>
    </div> : <ProfileHistory tenantId={tenantId} profileId={profileId} epoch={historyEpoch} />}
    <div className={styles.footnotes}><span>保存后会重新读取凭证状态；密钥只写入，不回显。</span><Button variant="ghost" onClick={() => setShowJson(true)}>查看非密钥配置 JSON</Button></div>
    {editingMetadata && <ProfileMetadataDialog profile={profile} busy={!!busy} error={metadataError} onClose={() => setEditingMetadata(false)} onSave={(input) => void updateMetadata(input)} />}
    {showJson && <ProfileDialog title="非密钥配置 JSON" description="仅包含 config，不包含凭证输入、内部关联 token 或内部 canonical spec。可选中文本复制。" onClose={() => setShowJson(false)}><pre className={styles.configPreview}>{JSON.stringify(config, null, 2)}</pre></ProfileDialog>}
    {confirmReload && <ProfileDialog title="重新读取最新草稿？" description="这会放弃当前本地编辑和待提交凭证。可先取消并复制非密钥配置；读取后不会自动覆盖服务端内容。" onClose={() => setConfirmReload(false)} footer={<><Button variant="secondary" onClick={() => setConfirmReload(false)}>保留本地编辑</Button><Button onClick={() => { setConfirmReload(false); void refreshState(); }}>放弃本地编辑并读取</Button></>}><p>当前本地基于 Draft r{draft.draft_revision}。</p></ProfileDialog>}
    {leaveHref && <ProfileDialog title="离开未保存的配置？" description="离开会丢弃本地配置修改和待提交凭证。建议先保存草稿。" onClose={() => setLeaveHref(null)} footer={<><Button variant="secondary" onClick={() => setLeaveHref(null)}>继续编辑</Button><Button variant="danger" onClick={() => { navigating.current = true; currentDirty.current = false; pendingSave.current = null; setCredentials({}); const target = new URL(leaveHref); if (target.origin === location.origin) router.push(target.pathname + target.search + target.hash); else window.location.assign(target.href); setLeaveHref(null); }}>放弃修改并离开</Button></>}><p>尚未保存的修改不会进入已发布版本。</p></ProfileDialog>}
  </div>;
}

function ProfileHistory({ tenantId, profileId, epoch }: { tenantId: string; profileId: string; epoch: number }) {
  const [offset, setOffset] = useState(0); const [page, setPage] = useState<ProfileRevisionPage | null>(null);
  const [loading, setLoading] = useState(true); const [error, setError] = useState<unknown>(); const [retry, setRetry] = useState(0);
  useEffect(() => { let active = true; setLoading(true); setError(undefined); void runtimeProfileApi.listRevisions(tenantId, profileId, { offset, limit: 20 }).then((next) => { if (active) setPage(next); }).catch((caught) => { if (active) setError(caught); }).finally(() => { if (active) setLoading(false); }); return () => { active = false; }; }, [tenantId, profileId, offset, epoch, retry]);
  return <section className="panel" aria-label="版本记录"><ApiNotice error={error} />{loading ? <div className="panel-loading" role="status">正在读取版本记录…</div> : error ? <Button variant="secondary" onClick={() => setRetry((value) => value + 1)}>重试读取版本</Button> : !page?.revisions.length ? <EmptyState title="尚无发布版本" detail="完成草稿校验并发布后，这里会显示不可变版本记录。" /> : <><div className="table-wrap"><table><thead><tr><th>版本</th><th>来源草稿</th><th>发布者</th><th>发布时间</th><th>操作</th></tr></thead><tbody>{page.revisions.map((item) => <tr key={item.id}><td><StatusBadge>r{item.revision_number}</StatusBadge></td><td>Draft r{item.source_draft_revision}</td><td className={styles.identifier}>{item.published_by}</td><td>{new Date(item.published_at).toLocaleString("zh-CN")}</td><td><Link className="text-link" href={`${profileHref(tenantId, profileId)}/revisions/${item.revision_number}`}>查看版本 →</Link></td></tr>)}</tbody></table></div><div className="pagination"><span>第 {offset + 1}–{Math.min(offset + 20, page.total)} 条，共 {page.total} 条</span><div className="pagination-actions"><Button variant="secondary" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 20))}>上一页版本</Button><Button variant="secondary" disabled={offset + 20 >= page.total} onClick={() => setOffset(offset + 20)}>下一页版本</Button></div></div></>}</section>;
}
