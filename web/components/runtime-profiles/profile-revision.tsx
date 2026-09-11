"use client";

import Link from "next/link";
import { ArrowLeft, Info, KeyRound, LockKeyhole, RefreshCw } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";

import { controlApi } from "../../lib/control-api";
import {
  runtimeProfileApi,
  type CredentialState,
  type CredentialUpdate,
  type ProfileRevision,
  type ResourceCategory,
  type RuntimeProfile,
} from "../../lib/runtime-profile-api";
import { Button, EmptyState, PageHeader, StatusBadge } from "../ui";
import { DeploymentReturnLink } from "../deployments/return-link";
import { deploymentHref } from "../../lib/deployment-editor-state";
import { ProfileDialog } from "./profile-dialog";
import { ProfileResourceEditor } from "./profile-resource-editor";
import styles from "./profile-revision.module.css";

type Purpose = CredentialUpdate["target"]["purpose_field"];
type CredentialRow = { category: ResourceCategory; resourceName: string; purpose: Purpose; state: CredentialState };

const categories: { key: ResourceCategory; label: string; purposes: Purpose[] }[] = [
  { key: "models", label: "Models", purposes: ["api_key"] },
  { key: "tools", label: "Tools", purposes: ["bearer_token"] },
  { key: "knowledge", label: "Knowledge", purposes: ["qdrant_api_key", "embedding_api_key"] },
  { key: "storage", label: "Storage", purposes: ["dsn", "dsn_password", "access_key_id", "secret_access_key"] },
];

function credentialRows(revision: ProfileRevision): CredentialRow[] {
  return categories.flatMap(({ key, purposes }) => Object.entries(revision.credential_states?.[key] ?? {}).flatMap(([resourceName, states]) => (
    purposes.flatMap((purpose) => {
      const state = states[purpose];
      return state ? [{ category: key, resourceName, purpose, state }] : [];
    })
  )));
}

function canUpdate(row: CredentialRow) {
  return row.state.status === "active" && row.state.configured && row.state.credential_revision >= 1 && !!row.state.association_token;
}

function statusOf(error: unknown): number | undefined {
  if (typeof error === "object" && error !== null && "status" in error && typeof error.status === "number") return error.status;
}

function readError(error: unknown) {
  const status = statusOf(error);
  if (status === 401) return "会话已过期，请重新登录后查看运行配置。";
  if (status === 403) return "当前账户没有查看此运行配置的权限。";
  if (status === 404) return "运行配置或版本不存在。";
  return "读取运行配置失败，请重试。";
}

function stateLabel(state: CredentialState) {
  return state.status === "active" ? "已配置" : state.status === "cleared" ? "已清除" : "未配置";
}

function resourceLabel(row: CredentialRow) {
  return `${categories.find(({ key }) => key === row.category)?.label} / ${row.resourceName} / ${row.purpose}`;
}

function replacementLabel(row: CredentialRow) {
  if (row.purpose === "access_key_id") return "新 S3 Access Key ID";
  if (row.purpose === "secret_access_key") return "新 S3 Secret Access Key";
  return row.purpose === "dsn" ? "新 DSN" : row.purpose === "dsn_password" ? `新 ${row.resourceName === "session" ? "Session" : "Memory"} 后端密码` : "新凭证值";
}

export function ProfileRevisionDetail({ tenantId, profileId, revisionNumber }: { tenantId: string; profileId: string; revisionNumber: number }) {
  const secretHintId = useId();
  const [profile, setProfile] = useState<RuntimeProfile | null>(null);
  const [revision, setRevision] = useState<ProfileRevision | null>(null);
  const [isOwner, setIsOwner] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadAttempt, setLoadAttempt] = useState(0);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [refreshWarning, setRefreshWarning] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [statesStale, setStatesStale] = useState(false);
  const [target, setTarget] = useState<CredentialRow | null>(null);
  const [action, setAction] = useState<"replace" | "clear">("replace");
  const [value, setValue] = useState("");
  const [clearConfirmed, setClearConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [mutationError, setMutationError] = useState("");
  const [conflict, setConflict] = useState(false);
  const [uncertain, setUncertain] = useState(false);
  const pending = useRef<{ body: CredentialUpdate; key: string } | null>(null);

  useEffect(() => {
    let current = true;
    setLoading(true);
    setError("");
    if (!Number.isSafeInteger(revisionNumber) || revisionNumber < 1) {
      setError("版本号无效，请从配置工作台选择已发布版本。");
      setLoading(false);
      return;
    }
    void Promise.all([
      runtimeProfileApi.getProfile(tenantId, profileId),
      runtimeProfileApi.getRevision(tenantId, profileId, revisionNumber),
      controlApi.getTenant(tenantId),
    ]).then(([nextProfile, nextRevision, tenant]) => {
      if (!current) return;
      setProfile(nextProfile);
      setRevision(nextRevision);
      setIsOwner(tenant.role === "OWNER");
    }).catch((cause: unknown) => {
      if (current) setError(readError(cause));
    }).finally(() => {
      if (current) setLoading(false);
    });
    return () => { current = false; pending.current = null; };
  }, [tenantId, profileId, revisionNumber, loadAttempt]);

  async function refreshStates() {
    setRefreshing(true);
    setRefreshWarning("");
    try {
      const latest = await runtimeProfileApi.getRevision(tenantId, profileId, revisionNumber);
      setRevision(latest);
      setStatesStale(false);
      return latest;
    } catch {
      setStatesStale(true);
      setRefreshWarning("当前凭证状态刷新失败；页面保留上次读取的配置。请刷新状态后再更新凭证。");
      return null;
    } finally {
      setRefreshing(false);
    }
  }

  function resetAttempt() {
    pending.current = null;
    setUncertain(false);
    setMutationError("");
  }

  function openUpdate(row: CredentialRow) {
    resetAttempt();
    setTarget(row);
    setAction("replace");
    setValue("");
    setClearConfirmed(false);
    setConflict(false);
  }

  function closeUpdate() {
    if (busy) return;
    pending.current = null;
    setValue("");
    setTarget(null);
    setMutationError("");
    setConflict(false);
    setUncertain(false);
  }

  async function submitCredential() {
    if (!target || !isOwner || !canUpdate(target) || busy || conflict || statesStale || refreshing) return;
    if (action === "replace" && (!value.trim() || /[\r\n\0]/.test(value))) return;
    if (action === "clear" && !clearConfirmed) return;
    const credentialTarget = {
      profile_revision_number: revisionNumber,
      category: target.category,
      resource_name: target.resourceName,
      purpose_field: target.purpose,
      association_token: target.state.association_token!,
    };
    const body: CredentialUpdate = action === "replace"
      ? { target: credentialTarget, action, expected_credential_revision: target.state.credential_revision, value }
      : { target: credentialTarget, action, expected_credential_revision: target.state.credential_revision };
    const attempt = pending.current ?? { body, key: crypto.randomUUID() };
    pending.current = attempt;
    setBusy(true);
    setMutationError("");
    setNotice("");
    try {
      await runtimeProfileApi.updateCredential(tenantId, profileId, attempt.body, attempt.key);
    } catch (cause: unknown) {
      const status = statusOf(cause);
      if (status === 409) {
        pending.current = null;
        setValue("");
        setClearConfirmed(false);
        setUncertain(false);
        setConflict(true);
        setMutationError("凭证版本或关联已变化。请查看最新状态并重新确认，不会自动重放更新。");
        await refreshStates();
      } else if (status === 401 || status === 403) {
        pending.current = null;
        setValue("");
        setIsOwner(false);
        setMutationError(status === 401 ? "会话已过期，请重新登录。" : "更新凭证需要当前租户 OWNER 权限。");
      } else if (status !== undefined && status < 500) {
        pending.current = null;
        setUncertain(false);
        setMutationError("凭证更新被拒绝，请检查新值和目标。DSN 需与已发布配置的连接目标一致。");
      } else {
        setUncertain(true);
        setMutationError("请求结果尚未确认。保持输入并重试会使用同一个幂等键，避免重复更新。");
      }
      setBusy(false);
      return;
    }
    pending.current = null;
    setValue("");
    setTarget(null);
    setUncertain(false);
    setNotice("凭证已更新，配置 Revision 与 digest 未改变。");
    // Mutation acknowledgement and the following read are separate outcomes.
    // A failed refresh must never invite resubmission of a successful update.
    setStatesStale(true);
    await refreshStates();
    setBusy(false);
  }

  const backHref = `/tenants/${encodeURIComponent(tenantId)}/runtime-profiles/${encodeURIComponent(profileId)}`;
  const rows = revision ? credentialRows(revision) : [];
  const refreshedTarget = target && rows.find((row) => row.category === target.category && row.resourceName === target.resourceName && row.purpose === target.purpose);
  const invalidValue = action === "replace" && (!value.trim() || /[\r\n\0]/.test(value));

  if (loading) return <div className="panel panel-loading" role="status"><div className="loading-mark" />正在读取运行配置版本…</div>;
  if (error || !revision || !profile) return <div className={styles.page}>
    <Link className={styles.back} href={backHref}><ArrowLeft size={15} />返回配置</Link>
    <div className="api-notice" role="alert">{error || "运行配置版本读取失败。"}</div>
    <Button variant="secondary" onClick={() => setLoadAttempt((attempt) => attempt + 1)}>重试读取</Button>
  </div>;

  return <div className={styles.page}>
    <Link className={styles.back} href={backHref}><ArrowLeft size={15} />返回配置工作台</Link>
    <DeploymentReturnLink tenantId={tenantId} />
    <PageHeader eyebrow="RUNTIME PROFILE · REVISION" title={`版本 r${revision.revision_number}`} description={profile.name} action={<Link className="button primary" href={`${deploymentHref(tenantId)}/new?profile=${encodeURIComponent(profileId)}&revision=${revision.revision_number}`}>用此版本创建部署</Link>} />
    {notice && <div className="success-notice" role="status">{notice}</div>}
    {refreshWarning && <div className={styles.warning} role="alert">{refreshWarning}</div>}
    <dl className={styles.metadata}>
      <div><dt>来源草稿</dt><dd>Draft r{revision.source_draft_revision}</dd></div>
      <div><dt>发布时间</dt><dd><time dateTime={revision.published_at}>{new Date(revision.published_at).toLocaleString("zh-CN", { hour12: false })}</time></dd></div>
      <div><dt>发布者</dt><dd>{revision.published_by}</dd></div>
      <div><dt>Schema / 凭证协议</dt><dd>{revision.schema_version} / {revision.credential_protocol_version}</dd></div>
      <div className={styles.digest}><dt>配置摘要</dt><dd><code>{revision.spec_digest}</code></dd></div>
    </dl>
    <div className={styles.information}><Info size={17} /><span>配置快照不可变；下方凭证状态是当前状态，不是发布时间快照。发布不代表部署上线或远端连接可用。</span></div>

    <section className="panel" aria-labelledby="current-credential-heading">
      <header className={styles.sectionHeader}><div><h2 id="current-credential-heading"><KeyRound size={16} />当前凭证状态</h2><p>{isOwner ? "OWNER 可更新有效关联；更新可能影响共享该凭证关联的其他已发布版本。" : "当前角色只读；更新已发布关联的凭证需要 OWNER。"}</p></div><Button variant="secondary" disabled={refreshing || busy} onClick={() => void refreshStates()}><RefreshCw size={14} />{refreshing ? "刷新中…" : "刷新状态"}</Button></header>
      {rows.length === 0 ? <EmptyState title="没有凭证状态" detail="此版本没有返回凭证关联；配置快照仍可在下方查看。" /> : <div className="table-wrap"><table className={styles.credentialTable}>
        <thead><tr><th>资源</th><th>用途</th><th>当前状态</th><th>凭证版本</th><th>操作</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={`${row.category}/${row.resourceName}/${row.purpose}`}>
          <td><div className="table-copy"><strong>{row.resourceName}</strong><small>{categories.find(({ key }) => key === row.category)?.label}</small></div></td>
          <td><code>{row.purpose}</code></td>
          <td><StatusBadge tone={row.state.status === "active" ? "green" : row.state.status === "cleared" ? "amber" : "gray"}>{stateLabel(row.state)}</StatusBadge>{statesStale && <small className={styles.stale}>待刷新</small>}</td>
          <td>{row.state.credential_revision > 0 ? `v${row.state.credential_revision}` : "—"}</td>
          <td>{isOwner && canUpdate(row) ? <Button variant="secondary" disabled={statesStale || refreshing || busy} aria-label={`更新 ${resourceLabel(row)}`} onClick={() => openUpdate(row)}>更新凭证</Button> : <span className={styles.muted}>{row.state.status === "cleared" ? "需新 Draft 关联" : row.state.status === "unconfigured" ? "未关联" : isOwner ? "无可更新关联" : "仅 OWNER"}</span>}</td>
        </tr>)}</tbody>
      </table></div>}
    </section>

    <section className={styles.snapshot} aria-labelledby="profile-snapshot-heading">
      <header className={styles.sectionHeader}><div><h2 id="profile-snapshot-heading"><LockKeyhole size={16} />配置快照</h2><p>Models、Tools、Knowledge、Storage 只读浏览；连接目标变更请返回 Draft 编辑并发布新版本。</p></div></header>
      <ProfileResourceEditor tenantId={tenantId} config={revision.config} credentials={{}} credentialStates={revision.credential_states ?? {}} onChange={() => {}} isOwner={isOwner} readOnly />
      <details className={styles.json}><summary>查看脱敏配置 JSON</summary><pre>{JSON.stringify(revision.config, null, 2)}</pre></details>
    </section>

    {target && <ProfileDialog title="更新凭证" description="仅 OWNER · 不创建新的配置版本" onClose={closeUpdate} busy={busy} footer={<><Button variant="secondary" disabled={busy} onClick={closeUpdate}>取消</Button><Button variant={action === "clear" ? "danger" : "primary"} disabled={busy || refreshing || statesStale || conflict || !isOwner || !canUpdate(target) || invalidValue || (action === "clear" && !clearConfirmed)} onClick={() => void submitCredential()}>{busy ? "更新中…" : uncertain ? "重试同一更新" : action === "clear" ? "确认清除凭证" : "确认更新凭证"}</Button></>}>
      <div className={styles.updateForm}>
        <dl className={styles.target}><div><dt>目标</dt><dd>{resourceLabel(target)}</dd></div><div><dt>配置版本</dt><dd>r{revisionNumber}</dd></div><div><dt>当前凭证版本</dt><dd>v{target.state.credential_revision}</dd></div></dl>
        {mutationError && <div className="api-notice" role="alert">{mutationError}</div>}
        {conflict && <div className={styles.warning}>
          <p>最新状态：{refreshedTarget ? `${stateLabel(refreshedTarget.state)} · v${refreshedTarget.state.credential_revision}` : "没有有效关联"}。新值已清空。</p>
          {statesStale ? <Button variant="secondary" disabled={refreshing} onClick={() => void refreshStates()}>重新读取状态</Button> : refreshedTarget && canUpdate(refreshedTarget) ? <Button variant="secondary" onClick={() => { setTarget(refreshedTarget); setConflict(false); resetAttempt(); }}>确认使用最新状态</Button> : <p>请返回 Draft 建立新关联并发布。</p>}
        </div>}
        <fieldset className={styles.actions} disabled={busy || conflict || !isOwner}><legend>更新方式</legend><label><input type="radio" name="credential-action" checked={action === "replace"} onChange={() => { setAction("replace"); setValue(""); setClearConfirmed(false); resetAttempt(); }} />替换</label><label><input type="radio" name="credential-action" checked={action === "clear"} onChange={() => { setAction("clear"); setValue(""); setClearConfirmed(false); resetAttempt(); }} />清除</label></fieldset>
        {action === "replace" ? <label className={`field ${styles.secret}`}><span>{replacementLabel(target)}</span><input aria-label={replacementLabel(target)} aria-describedby={secretHintId} aria-invalid={!!value && invalidValue} type="password" autoComplete="new-password" spellCheck={false} value={value} maxLength={65536} disabled={busy || conflict || !isOwner} onChange={(event) => { setValue(event.target.value); resetAttempt(); }} placeholder="输入新的凭证，不回填已有值" /><small id={secretHintId}>{target.purpose === "dsn" ? "PostgreSQL URI 需包含密码和 sslmode，连接目标需保持不变。" : target.purpose === "dsn_password" ? "填写原始密码，不是完整 DSN；沿用此发布版本的固定关联，不依赖当前后端目录。" : "凭证只用于本次提交；成功或关闭后清空，不保存到浏览器存储。"}</small>{value && invalidValue && <small className="error-text">请输入非空且不含换行的凭证值。</small>}</label> : <label className={styles.confirm}><input type="checkbox" checked={clearConfirmed} disabled={busy || conflict || !isOwner} onChange={(event) => { setClearConfirmed(event.target.checked); resetAttempt(); }} /><span>我确认清除将使共享该关联的已发布配置失去此凭证；清除后需通过新 Draft 关联重新配置。</span></label>}
        <div className={styles.warning}><Info size={16} /><span>更新会影响引用该凭证关联的已发布配置，配置 Revision 和 digest 保持不变。</span></div>
      </div>
    </ProfileDialog>}
  </div>;
}
