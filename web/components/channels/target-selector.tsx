"use client";
import Link from "next/link";
import { useEffect, useState } from "react";
import { deploymentApi, deploymentError, type Deployment, type DeploymentRevisionSummary, type DeploymentRevision } from "../../lib/deployment-api";
import type { ChannelTarget } from "../../lib/channel-api";
import { Button } from "../ui";
import styles from "./channel.module.css";

type Props = { tenantId: string; initialTarget?: ChannelTarget | null; disabled: boolean; onChange: (target: ChannelTarget | null, revision: DeploymentRevision | null) => void };
/** Both lists are bounded pages. A newer deployment revision never silently changes the selected target. */
export function ChannelTargetSelector({ tenantId, initialTarget, disabled, onChange }: Props) {
  const [deployments, setDeployments] = useState<Deployment[]>([]); const [depOffset, setDepOffset] = useState(0); const [depMore, setDepMore] = useState(false);
  const [deploymentId, setDeploymentId] = useState(initialTarget?.deployment_id ?? ""); const [number, setNumber] = useState(initialTarget?.revision_number ? String(initialTarget.revision_number) : "");
  const [revisions, setRevisions] = useState<DeploymentRevisionSummary[]>([]); const [revOffset, setRevOffset] = useState(0); const [revMore, setRevMore] = useState(false);
  const [preview, setPreview] = useState<DeploymentRevision | null>(null); const [previewError, setError] = useState(""); const [depError, setDepError] = useState(""); const [revError, setRevError] = useState(""); const [epoch, setEpoch] = useState(0); const [loading, setLoading] = useState(false);
  const [depLoading, setDepLoading] = useState(true); const [revLoading, setRevLoading] = useState(false);
  useEffect(() => { let active = true; setDepLoading(true); setDepError("");
    void deploymentApi.list(tenantId, depOffset, 50).then((page) => { if (!active) return; setDeployments((old) => depOffset ? [...new Map([...old, ...page.deployments].map((d) => [d.id, d])).values()] : page.deployments); setDepMore(page.offset + page.limit < page.total); }).catch((e) => { if (active) setDepError(deploymentError(e)); }).finally(() => { if (active) setDepLoading(false); });
    return () => { active = false; };
  }, [tenantId, depOffset, epoch]);
  useEffect(() => { let active = true; if (!deploymentId) { setRevisions([]); setRevMore(false); setRevLoading(false); setRevError(""); return; } setRevLoading(true); setRevError("");
    void deploymentApi.listRevisions(tenantId, deploymentId, revOffset, 50).then((page) => { if (!active) return; setRevisions((old) => revOffset ? [...new Map([...old, ...page.revisions].map((r) => [r.revision_number, r])).values()] : page.revisions); setRevMore(page.offset + page.limit < page.total); }).catch((e) => { if (active) setRevError(deploymentError(e)); }).finally(() => { if (active) setRevLoading(false); });
    return () => { active = false; };
  }, [tenantId, deploymentId, revOffset, epoch]);
  useEffect(() => { let active = true; onChange(null, null); setPreview(null); if (!deploymentId || !number) { setLoading(false); return; } setLoading(true); setError("");
    const n = Number(number);
    if (!Number.isSafeInteger(n) || n < 1) { setError("请选择有效的固定发布版本。"); setLoading(false); return; }
    void deploymentApi.getRevision(tenantId, deploymentId, n).then((revision) => { if (!active) return; setPreview(revision); onChange({ deployment_id: deploymentId, revision_number: n }, revision); }).catch((e) => { if (active) setError(deploymentError(e)); }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [tenantId, deploymentId, number, epoch, onChange]);
  const error = depError || revError || previewError;
  const root = `/tenants/${encodeURIComponent(tenantId)}`;
  return <div className={styles.stack}>
    <div className={styles.grid}>
      <label className={styles.field}><span>部署</span><select aria-label="渠道运行部署" value={deploymentId} disabled={disabled || depLoading} onChange={(e) => { setDeploymentId(e.target.value); setNumber(""); setRevisions([]); setRevOffset(0); setPreview(null); onChange(null, null); }}><option value="">选择已发布的部署</option>{deploymentId && !deployments.some((d) => d.id === deploymentId) && <option value={deploymentId}>{deploymentId}（指定部署）</option>}{deployments.map((d) => <option key={d.id} value={d.id} disabled={!d.latest_revision_number}>{d.name}{d.latest_revision_number ? "" : "（尚未发布）"}</option>)}</select><small>不是 Agent 或 Profile 草稿，不使用 latest。</small></label>
      <label className={styles.field}><span>固定发布版本</span><select aria-label="渠道固定部署版本" value={number} disabled={disabled || !deploymentId || revLoading} onChange={(e) => { setNumber(e.target.value); setPreview(null); onChange(null, null); }}><option value="">选择固定 rN</option>{number && !revisions.some((r) => String(r.revision_number) === number) && <option value={number}>r{number}（指定版本）</option>}{revisions.map((r) => <option key={r.revision_number} value={r.revision_number}>r{r.revision_number} · Agent v{r.agent_version_number} / Profile r{r.profile_revision_number}</option>)}</select><small>发布新版本不会自动切换此选择。</small></label>
    </div>
    {(depMore || revMore) && <div className={styles.actions}>{depMore && <Button variant="secondary" disabled={disabled || depLoading || !!error} onClick={() => setDepOffset((n) => n + 50)}>加载更多部署</Button>}{revMore && <Button variant="secondary" disabled={disabled || revLoading || !!error} onClick={() => setRevOffset((n) => n + 50)}>加载更多版本</Button>}</div>}
    {error && <div role="alert" className={styles.error}>{error}<Button variant="secondary" onClick={() => setEpoch((n) => n + 1)}>重试读取目标</Button></div>}
    {loading && <p role="status" className={styles.hint}>正在核对固定部署快照…</p>}
    {preview && <div className={styles.notice}><strong>待保存目标 · r{preview.revision_number}</strong><p>Agent v{preview.agent_version_number} / Profile r{preview.profile_revision_number}</p><div className={styles.actions}><Link className={styles.link} href={`${root}/deployments/${encodeURIComponent(preview.deployment_id)}/revisions/${preview.revision_number}`}>查看部署快照</Link><Link className={styles.link} href={`${root}/agents/${encodeURIComponent(preview.agent_id)}/versions/${preview.agent_version_number}`}>Agent 来源</Link><Link className={styles.link} href={`${root}/runtime-profiles/${encodeURIComponent(preview.profile_id)}/revisions/${preview.profile_revision_number}`}>Profile 来源</Link></div><p className={styles.hint}>这里只预选目标，点击保存并确认后才提交。</p></div>}
    {!depLoading && !deployments.some((d) => !!d.latest_revision_number) && !depMore && <p className={styles.hint}>还没有可选择的发布部署。<Link className={styles.link} href={`${root}/deployments/new`}>先准备并发布部署 →</Link></p>}
  </div>;
}
