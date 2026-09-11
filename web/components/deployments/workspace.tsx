"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { controlApi, type AgentVersion } from "../../lib/control-api";
import { runtimeProfileApi, type ProfileRevision } from "../../lib/runtime-profile-api";
import { deploymentApi, DeploymentApiError, deploymentError, deploymentRead, type Deployment, type DeploymentInput, type DeploymentReport } from "../../lib/deployment-api";
import { deploymentHref, emptySelection, establishPreparationIdentity, inputFrom, inputFingerprint, loadPreparation, pairingRows, preparationKey, savePreparation, selectionFrom, selectionFromQuery, selectionQuery, type PendingOperation, type Preparation, type Selection } from "../../lib/deployment-editor-state";
import { Button, PageHeader, StatusBadge } from "../ui";
import { ProfileDialog } from "../runtime-profiles/profile-dialog";
import { SourceSelector } from "./source-selector";
import { ValidationReport } from "./validation-report";
import { DeploymentMetadataDialog } from "./metadata-dialog";
import { DeploymentHistory } from "./history";
import styles from "./deployment.module.css";

type Checked = { fingerprint: string; report: DeploymentReport; at: string };
type Sources = { fingerprint: string; agent: AgentVersion; profile: ProfileRevision };
const initialPreparation = (): Preparation => ({ version: 1, selection: emptySelection(), name: "", description: "", pending: null });

export function DeploymentWorkspace({ tenantId, deploymentId, query = "" }: { tenantId: string; deploymentId?: string; query?: string }) {
  const router = useRouter();
  const [state, setState] = useState<Preparation>(initialPreparation);
  const stateRef = useRef(state); stateRef.current = state;
  const [deployment, setDeployment] = useState<Deployment | null>(null);
  const [owner, setOwner] = useState(false); const [userId, setUserId] = useState("");
  const [loading, setLoading] = useState(true); const [loadEpoch, setLoadEpoch] = useState(0);
  const [error, setError] = useState(""); const [notice, setNotice] = useState("");
  const [storageFailed, setStorageFailed] = useState(false);
  const [busy, setBusy] = useState(""); const lock = useRef(false);
  const generation = useRef(0); const validationSequence = useRef(0);
  const storageKey = useRef(""); const ready = useRef(false);
  const [tab, setTab] = useState<"overview" | "prepare" | "history">(deploymentId ? "overview" : "prepare");
  const [checked, setChecked] = useState<Checked | null>(null);
  const [sources, setSources] = useState<Sources | null>(null); const [sourceError, setSourceError] = useState(""); const [sourceEpoch, setSourceEpoch] = useState(0);
  const [baselineInput, setBaselineInput] = useState<{ number: number; input: DeploymentInput } | null>(null);
  const [confirmation, setConfirmation] = useState(false); const [metadataOpen, setMetadataOpen] = useState(false);
  const [conflict, setConflict] = useState(false); const [reviewed, setReviewed] = useState(false); const [reviewText, setReviewText] = useState("");
  const autoValidate = useRef(false);
  const input = inputFrom(state.selection);
  const fingerprint = input ? inputFingerprint(input) : "";
  const selectionFingerprint = JSON.stringify(state.selection);
  const report = checked?.fingerprint === fingerprint ? checked.report : null;
  const fixedSources = sources?.fingerprint === fingerprint ? sources : null;
  const href = deploymentHref(tenantId, deploymentId);
  const returnTo = `${href}${deploymentId ? "" : "/new"}?prepare=1&${selectionQuery(state.selection)}`;
  const frozen = !!busy || !!state.pending;

  function persist(next: Preparation, key = storageKey.current) {
    if (!key) return false;
    try { const ok = savePreparation(sessionStorage, key, next); setStorageFailed(!ok); return ok; }
    catch { setStorageFailed(true); return false; }
  }
  function change(next: Preparation) {
    stateRef.current = next; setState(next);
    if (ready.current) persist(next);
  }
  function invalidate() { validationSequence.current++; setChecked(null); }
  function select(selection: Selection) {
    if (lock.current || stateRef.current.pending) return;
    invalidate(); setError(""); setNotice(""); change({ ...stateRef.current, selection });
  }

  useEffect(() => {
    const run = ++generation.current; let current = true;
    ready.current = false; setLoading(true); setError("");
    void (async () => {
      try {
        const [session, tenant, currentDeployment] = await deploymentRead(Promise.all([
          controlApi.getMe(), controlApi.getTenant(tenantId), deploymentId ? deploymentApi.get(tenantId, deploymentId) : Promise.resolve(null),
        ]));
        if (!current || generation.current !== run) return;
        setUserId(session.user.id); setOwner(tenant.role === "OWNER"); setDeployment(currentDeployment);
        const key = preparationKey(session.user.id, tenantId, deploymentId); storageKey.current = key;
        let saved: Preparation | null = null;
        try { establishPreparationIdentity(sessionStorage, session.user.id); saved = loadPreparation(sessionStorage, key); }
        catch { setStorageFailed(true); }
        const params = new URLSearchParams(query);
        // An unchanged entry link must not overwrite later edits on refresh.
        // A genuinely different deep link intentionally seeds a new preparation.
        const sourceQuery = new URLSearchParams(["from", "agent", "version", "profile", "revision"].flatMap((key) => params.has(key) ? [[key, params.get(key)!]] : [])).toString();
        let next = saved ?? initialPreparation();
        if (saved?.pending) setNotice("已恢复上次结果待确认的请求。请先重试确认原请求，再修改选择。");
        else if (deploymentId && params.has("from")) {
          const number = Number(params.get("from"));
          if (!Number.isSafeInteger(number) || number < 1) throw new Error("请选择发布历史中的正整数版本。");
          const revision = await deploymentApi.getRevision(tenantId, deploymentId, number);
          if (!current) return;
          if (!saved || saved.sourceQuery !== sourceQuery) next = { ...next, selection: selectionFrom(revision.input) };
          setBaselineInput({ number, input: revision.input });
        } else if ((!saved || saved.sourceQuery !== sourceQuery) && (params.has("agent") || params.has("profile"))) {
          next = { ...next, selection: selectionFromQuery(params) };
        }
        if (!next.pending) next = { ...next, sourceQuery };
        if (saved && !saved.pending) setNotice("已恢复当前浏览器的待发布选择；请重新校验。");
        if (next.pending || params.get("prepare") === "1" || params.has("from")) setTab("prepare");
        autoValidate.current = params.get("validate") === "1" && !next.pending;
        stateRef.current = next; setState(next); ready.current = true; persist(next, key);
      } catch (e) { if (current) setError(deploymentError(e)); }
      finally { if (current) setLoading(false); }
    })();
    return () => { current = false; ready.current = false; generation.current++; validationSequence.current++; };
  }, [tenantId, deploymentId, query, loadEpoch]);

  useEffect(() => {
    let current = true; setSources(null); setSourceError("");
    if (!input || !ready.current || loading) return;
    const selected = input; const fp = inputFingerprint(selected);
    void deploymentRead(Promise.all([
      controlApi.getAgentVersion(tenantId, selected.agent.agent_id, selected.agent.version_number),
      runtimeProfileApi.getRevision(tenantId, selected.profile.profile_id, selected.profile.revision_number),
    ])).then(([agent, profile]) => { if (current) setSources({ fingerprint: fp, agent, profile }); }).catch((e) => { if (current) setSourceError(deploymentError(e)); });
    return () => { current = false; };
  }, [tenantId, selectionFingerprint, sourceEpoch, loading]);

  useEffect(() => {
    const focus = () => { invalidate(); setConfirmation(false); };
    const visibility = () => { if (document.visibilityState === "visible") focus(); };
    window.addEventListener("focus", focus); document.addEventListener("visibilitychange", visibility);
    const unload = (event: BeforeUnloadEvent) => {
      if (storageFailed && (stateRef.current.pending || stateRef.current.name || stateRef.current.selection.agentId)) { event.preventDefault(); event.returnValue = ""; }
    };
    window.addEventListener("beforeunload", unload);
    return () => { window.removeEventListener("focus", focus); document.removeEventListener("visibilitychange", visibility); window.removeEventListener("beforeunload", unload); };
  }, [storageFailed]);

  async function validate() {
    const selected = inputFrom(stateRef.current.selection);
    if (!deploymentId || !selected || lock.current || stateRef.current.pending) return;
    lock.current = true; setBusy("validate"); setError(""); setChecked(null);
    const sequence = ++validationSequence.current; const run = generation.current;
    try {
      const result = await deploymentApi.validate(tenantId, deploymentId, selected);
      if (run === generation.current && sequence === validationSequence.current) {
        setChecked({ fingerprint: inputFingerprint(selected), report: result, at: new Date().toLocaleString("zh-CN") });
        setNotice(result.valid ? "当前两份版本校验通过；发布时仍会重新检查，不代表远端连接测试成功。" : "部署身份已保存，尚未发布本次选择。请按诊断修复后重新校验。");
      }
    } catch (e) { if (run === generation.current) setError(deploymentError(e)); }
    finally { if (run === generation.current) { lock.current = false; setBusy(""); } }
  }
  useEffect(() => {
    if (autoValidate.current && !loading && fixedSources && !state.pending) { autoValidate.current = false; void validate(); }
  }, [loading, fixedSources, state.pending]);

  async function perform(operation: PendingOperation) {
    if (lock.current) return;
    lock.current = true; setBusy(operation.kind); setError(""); setNotice(""); setConfirmation(false);
    const run = generation.current; let acknowledged = false;
    // Persist the entire immutable logical request before sending. A retry never
    // refreshes expected_latest_revision_number or invents a replacement key.
    change({ ...stateRef.current, pending: operation });
    try {
      if (operation.kind === "create") {
        const result = await deploymentApi.create(tenantId, operation.body, operation.key);
        if (run !== generation.current) return;
        const next = { ...stateRef.current, pending: null };
        const nextKey = preparationKey(userId, tenantId, result.deployment.id);
        const saved = persist(next, nextKey);
        if (saved) { try { sessionStorage.removeItem(storageKey.current); } catch { /* The new keyed receipt is already retained. */ } }
        acknowledged = true;
        router.replace(`${deploymentHref(tenantId, result.deployment.id)}?prepare=1&validate=1&${selectionQuery(next.selection)}`);
      } else {
        if (!deploymentId) throw new Error("发布请求缺少部署身份。");
        const result = await deploymentApi.publish(tenantId, deploymentId, operation.body, operation.key);
        if (run !== generation.current) return;
        change({ ...stateRef.current, pending: null });
        try { sessionStorage.removeItem(storageKey.current); } catch { /* Receipt is confirmed, never auto-republish. */ }
        acknowledged = true;
        router.push(deploymentHref(tenantId, deploymentId, result.revision.revision_number));
      }
    } catch (e) {
      if (run !== generation.current) return;
      setError(deploymentError(e));
      if (e instanceof DeploymentApiError && e.status === 409) { setConflict(true); setReviewed(false); }
      else if (e instanceof DeploymentApiError && e.status >= 400 && e.status < 500) {
        change({ ...stateRef.current, pending: null });
        invalidate();
        if (e.validation && operation.kind === "publish") setChecked({ fingerprint: inputFingerprint(operation.body.input), report: e.validation, at: new Date().toLocaleString("zh-CN") });
        if (e.status === 403) setOwner(false);
      } else setNotice("操作结果尚未确认。当前选择已锁定，请使用原请求重试确认，避免重复创建或发布。");
    } finally { if (run === generation.current && !acknowledged) { lock.current = false; setBusy(""); } }
  }
  function create() {
    if (!fixedSources || !state.name.trim() || [...state.name.trim()].length > 128 || [...state.description].length > 4096) return;
    void perform({ kind: "create", key: crypto.randomUUID(), body: { name: state.name.trim(), description: state.description } });
  }
  function publish() {
    if (!owner || !deployment || !input || !report?.valid || !fixedSources || state.pending) return;
    void perform({ kind: "publish", key: crypto.randomUUID(), body: { expected_latest_revision_number: deployment.latest_revision_number, input } });
  }
  async function review() {
    if (lock.current) return; lock.current = true; setBusy("review"); setError("");
    const run = generation.current;
    try {
      if (deploymentId) {
        const [latest, history] = await Promise.all([deploymentApi.get(tenantId, deploymentId), deploymentApi.listRevisions(tenantId, deploymentId)]);
        if (run !== generation.current) return;
        setDeployment(latest);
        setReviewText(`服务器最近发布：${latest.latest_revision_number ? `r${latest.latest_revision_number}` : "无"}。当前页记录：${history.revisions.map((r) => `r${r.revision_number}（Agent v${r.agent_version_number} / Profile r${r.profile_revision_number}）`).join("；") || "无"}。`);
      } else {
        const page = await deploymentApi.list(tenantId);
        if (run !== generation.current) return;
        setReviewText(`租户共有 ${page.total} 个部署。当前页：${page.deployments.map((d) => `${d.name} · ${d.id}`).join("；") || "无"}。请结合部署列表核实原创建是否成功。`);
      }
      setReviewed(true);
    } catch (e) { if (run === generation.current) setError(deploymentError(e)); }
    finally { if (run === generation.current) { lock.current = false; setBusy(""); } }
  }
  async function prepare(number: number) {
    if (!deploymentId || lock.current || stateRef.current.pending) return;
    lock.current = true; setBusy("source"); setError(""); const run = generation.current;
    try {
      const revision = await deploymentApi.getRevision(tenantId, deploymentId, number);
      if (run !== generation.current) return;
      invalidate(); setBaselineInput({ number, input: revision.input });
      change({ ...stateRef.current, selection: selectionFrom(revision.input) }); setTab("prepare");
      setNotice(`已读取 r${number} 的固定来源。调整后重新校验，不会跟随 latest 自动变化。`);
    } catch (e) { if (run === generation.current) setError(deploymentError(e)); }
    finally { if (run === generation.current) { lock.current = false; setBusy(""); } }
  }
  async function share() {
    try { await navigator.clipboard.writeText(`${window.location.origin}${returnTo}`); setNotice("准备链接已复制。接收人将重新鉴权和校验；链接不是共享草稿或审批记录。"); }
    catch { setNotice(`复制失败，可手动复制准备路径：${returnTo}`); }
  }

  if (loading) return <p className={styles.loading} role="status">正在读取部署工作区…</p>;
  if (!ready.current) return <div className={styles.error} role="alert">{error || "部署工作区读取失败。"}<Button variant="secondary" onClick={() => setLoadEpoch(loadEpoch + 1)}>重试读取工作区</Button></div>;
  const rows = fixedSources ? pairingRows(fixedSources.agent, fixedSources.profile) : [];
  return <div className={styles.page}>
    <PageHeader eyebrow="DEPLOYMENT PUBLICATION" title={deployment?.name ?? "新建部署"} description="固定 Agent 与运行配置版本，校验后发布可追溯的执行快照。" action={<Link className="button secondary" href={deploymentHref(tenantId)}>返回部署列表</Link>} />
    <div className={styles.banner}>发布仅生成不可变配置，不启动运行或切换流量。 <StatusBadge tone="gray">{owner ? "OWNER" : "MEMBER"}</StatusBadge> {deployment && <StatusBadge tone="blue">{deployment.latest_revision_number ? `最近发布 r${deployment.latest_revision_number}` : "尚未发布"}</StatusBadge>}</div>
    {storageFailed && <div className={styles.warning} role="alert">浏览器本地恢复存储不可用。当前输入仅保留在此页面，请先完成操作再离开；写请求超时后请在本页重试确认。</div>}
    {error && <div className={styles.error} role="alert">{error}</div>}
    {notice && <div className={styles.banner} role="status">{notice}</div>}
    {state.pending && <section className={styles.warning} aria-label="待确认操作"><strong>{conflict ? "请求冲突，等待确认" : "上次操作结果待确认"}</strong><p>原请求和幂等标识已保留。选择锁定期间不会自动生成新请求。</p><div className={styles.actions}>
      <Button disabled={!!busy || conflict} onClick={() => state.pending && void perform(state.pending)}>重试确认本次{state.pending.kind === "create" ? "创建" : "发布"}</Button>
      <Button variant="secondary" disabled={!!busy} onClick={() => void review()}>读取最新发布记录</Button><Link className={styles.link} href={deploymentHref(tenantId)}>核对部署列表</Link>
    </div>{reviewed && <><p>{reviewText}</p><Button variant="secondary" disabled={!!busy} onClick={() => { change({ ...stateRef.current, pending: null }); invalidate(); setConflict(false); setReviewed(false); setError(""); setNotice("已结束原待确认操作。你的选择已保留，请重新校验并确认新的发布基线。"); }}>已核实记录，结束原请求并重新准备</Button></>}</section>}
    {deploymentId && <div className={styles.tabs} role="tablist" aria-label="部署工作台"><Button role="tab" variant="ghost" aria-selected={tab === "overview"} onClick={() => setTab("overview")}>概览</Button><Button role="tab" variant="ghost" aria-selected={tab === "prepare"} onClick={() => setTab("prepare")}>准备新版本</Button><Button role="tab" variant="ghost" aria-selected={tab === "history"} onClick={() => setTab("history")}>发布历史</Button></div>}
    {tab === "overview" && deployment && <section className={styles.card}><h2>部署信息</h2><p>{deployment.description || "暂无描述"}</p><p className={styles.small}>元数据 r{deployment.metadata_revision} · {deployment.id}</p><div className={styles.actions}><Button variant="secondary" disabled={frozen} onClick={() => setMetadataOpen(true)}>编辑名称和描述</Button>{deployment.latest_revision_number ? <><Link className="button secondary" href={deploymentHref(tenantId, deployment.id, deployment.latest_revision_number)}>查看最近发布版本</Link><Button disabled={frozen} onClick={() => void prepare(deployment.latest_revision_number!)}>基于最近发布准备</Button></> : <Button onClick={() => setTab("prepare")}>准备首次发布</Button>}</div></section>}
    {tab === "history" && deploymentId && <DeploymentHistory tenantId={tenantId} deploymentId={deploymentId} onPrepare={(n) => void prepare(n)} disabled={frozen} />}
    {tab === "prepare" && <>
      {!deploymentId && <section className={styles.card}><h2>部署信息</h2><label className={styles.field}>部署名称<input value={state.name} disabled={frozen} placeholder="例如：多工具研究助手" onChange={(e) => change({ ...stateRef.current, name: e.target.value })} /></label><label className={styles.field}>描述<textarea value={state.description} disabled={frozen} placeholder="说明这组固定版本的用途" onChange={(e) => change({ ...stateRef.current, description: e.target.value })} /></label><p className={styles.small}>名称最多 128 字符，描述最多 4096 字符；点击“创建部署并校验”后才保存到服务器。</p></section>}
      <SourceSelector tenantId={tenantId} value={state.selection} onChange={select} disabled={frozen} />
      <div className={styles.grid}><div className={styles.main}>
        {baselineInput && <section className={styles.card}><h2>相对 r{baselineInput.number} 的来源变化</h2><div className={styles.small}>Agent：{baselineInput.input.agent.agent_id} v{baselineInput.input.agent.version_number} → {state.selection.agentId || "未选择"} v{state.selection.agentVersion || "—"}<br />Profile：{baselineInput.input.profile.profile_id} r{baselineInput.input.profile.revision_number} → {state.selection.profileId || "未选择"} r{state.selection.profileRevision || "—"}</div></section>}
        <section className={styles.card}><h2><span className={styles.number}>3</span>资源配对概览</h2><p className={styles.small}>按类别和 key 精确匹配。MCP tool_name 不是资源 key；此表是本地预检查，不是最终 Manifest。</p>
          {sourceError ? <div className={styles.error} role="alert">{sourceError}<Button variant="secondary" disabled={frozen} onClick={() => setSourceEpoch(sourceEpoch + 1)}>重试读取固定来源</Button></div> : !input ? <p>请先选择两份已发布版本。</p> : !fixedSources ? <p role="status">正在读取固定来源…</p> : <div className={styles.scroll}><table className={styles.table}><thead><tr><th>类别 / 名称</th><th>使用节点</th><th>预检查</th></tr></thead><tbody>{rows.map((row) => <tr key={`${row.category}/${row.name}`}><td>{row.category}<br /><strong>{row.name}</strong></td><td>{row.category === "storage" ? "固定运行角色" : row.nodes.join("、") || "无节点引用"}</td><td className={row.present ? styles.good : styles.bad}>{row.note}</td></tr>)}</tbody></table></div>}
          <p className={styles.matchNote}>session 必需，memory 可选。多余 Profile 资源不自动授权；实际节点分配以发布后的 Manifest 为准。</p>
        </section>
      </div><aside className={styles.sticky}><ValidationReport report={report} tenantId={tenantId} selection={state.selection} returnTo={returnTo} checkedAt={checked?.at} /></aside></div>
      {!owner && <div className={styles.warning}>MEMBER 可准备和校验。发布需要租户 OWNER；可复制固定来源链接交接，接收人需重新校验。</div>}
      <div className={styles.bar}><div className={styles.small}>{state.pending ? "操作待确认" : "版本选择保存在当前浏览器，尚未发布"}</div><div className={styles.actions}>
        <Button variant="ghost" disabled={!input || frozen} onClick={() => void share()}>复制准备链接</Button>
        {deploymentId ? <><Button variant="secondary" disabled={frozen || !fixedSources} onClick={() => void validate()}>{busy === "validate" ? "校验中…" : "重新校验"}</Button><Button disabled={frozen || !owner || !report?.valid || !fixedSources} onClick={() => setConfirmation(true)}>发布部署版本</Button></> : <Button disabled={frozen || !fixedSources || !state.name.trim() || [...state.name.trim()].length > 128 || [...state.description].length > 4096} onClick={create}>{busy === "create" ? "创建中…" : "创建部署并校验"}</Button>}
      </div></div>
    </>}
    {confirmation && deployment && <ProfileDialog title="确认发布部署版本" description="发布会生成不可变 Revision 和 Manifest，不会自动切换流量。" onClose={() => setConfirmation(false)} footer={<><Button variant="secondary" onClick={() => setConfirmation(false)}>返回检查</Button><Button disabled={!report?.valid || !owner || frozen} onClick={publish}>确认发布</Button></>}>
      <p>Agent：{state.selection.agentId} · v{state.selection.agentVersion}</p><p>Profile：{state.selection.profileId} · r{state.selection.profileRevision}</p><p>发布基线：{deployment.latest_revision_number === null ? "尚无发布（首次）" : `r${deployment.latest_revision_number}`}</p><p>当前报告：{report?.diagnostics.filter((d) => d.severity === "warning").length ?? 0} 条警告。提交时会再次校验。</p>
    </ProfileDialog>}
    {metadataOpen && deployment && <DeploymentMetadataDialog tenantId={tenantId} deployment={deployment} onClose={() => setMetadataOpen(false)} onSaved={(next) => { setDeployment(next); setMetadataOpen(false); setNotice("部署名称和描述已保存，发布版本保持不变。"); }} />}
  </div>;
}
