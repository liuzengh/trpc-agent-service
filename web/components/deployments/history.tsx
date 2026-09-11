"use client";
import Link from "next/link";
import { useEffect, useState } from "react";
import { deploymentApi, deploymentError, type DeploymentRevisionPage } from "../../lib/deployment-api";
import { deploymentHref } from "../../lib/deployment-editor-state";
import { Button, EmptyState } from "../ui";
import styles from "./deployment.module.css";

export function DeploymentHistory({ tenantId, deploymentId, onPrepare, disabled = false }: {
  tenantId: string; deploymentId: string; onPrepare(n: number): void; disabled?: boolean;
}) {
  const [offset, setOffset] = useState(0); const [epoch, setEpoch] = useState(0);
  const [page, setPage] = useState<DeploymentRevisionPage | null>(null); const [error, setError] = useState("");
  useEffect(() => {
    let current = true; setPage(null); setError("");
    void deploymentApi.listRevisions(tenantId, deploymentId, offset, 20).then((result) => { if (current) setPage(result); }).catch((e) => { if (current) setError(deploymentError(e)); });
    return () => { current = false; };
  }, [tenantId, deploymentId, offset, epoch]);
  return <section className={styles.card}><h2>发布历史</h2><p>历史是发布事实，不代表流量当前选择。基于旧版发布会按当前平台契约重新编译。</p>
    {error ? <div className={styles.error} role="alert">{error}<Button variant="secondary" onClick={() => setEpoch(epoch + 1)}>重试读取历史</Button></div> : !page ? <p role="status">正在读取发布历史…</p> : <>
      {page.total === 0 ? <EmptyState title="尚未发布" detail="选择两份固定版本并校验后，由 OWNER 发布。" /> : <div className={styles.scroll}><table className={styles.table}><thead><tr><th>版本</th><th>固定来源</th><th>发布记录</th><th>操作</th></tr></thead><tbody>{page.revisions.map((r) => <tr key={r.id}><td><Link className={styles.link} href={deploymentHref(tenantId, deploymentId, r.revision_number)}>r{r.revision_number}</Link></td><td>Agent v{r.agent_version_number} / Profile r{r.profile_revision_number}<div className={styles.small}>{r.agent_id}<br />{r.profile_id}</div></td><td>{new Date(r.published_at).toLocaleString("zh-CN")}<div className={styles.small}>{r.published_by}</div><details className={styles.small}><summary>Digest</summary>{r.input_digest}<br />{r.manifest_digest}</details></td><td><Button variant="secondary" disabled={disabled} onClick={() => onPrepare(r.revision_number)}>基于 r{r.revision_number} 准备</Button></td></tr>)}</tbody></table></div>}
      {page.total > 20 && <div className={styles.actions}><Button variant="ghost" disabled={offset === 0} onClick={() => setOffset(offset - 20)}>上一页</Button><span>{offset + 1}–{Math.min(offset + 20, page.total)} / {page.total}</span><Button variant="ghost" disabled={offset + 20 >= page.total} onClick={() => setOffset(offset + 20)}>下一页</Button></div>}
    </>}
  </section>;
}
