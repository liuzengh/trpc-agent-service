"use client";
import Link from "next/link";
import { useEffect, useState } from "react";
import { deploymentApi, deploymentError, type DeploymentPage } from "../../lib/deployment-api";
import { deploymentHref } from "../../lib/deployment-editor-state";
import { Button, EmptyState, PageHeader, StatusBadge } from "../ui";
import styles from "./deployment.module.css";
export function DeploymentList({ tenantId }: { tenantId: string }) {
  const [offset, setOffset] = useState(0); const [epoch, setEpoch] = useState(0);
  const [page, setPage] = useState<DeploymentPage | null>(null); const [error, setError] = useState(""); const [loading, setLoading] = useState(true);
  useEffect(() => { let current = true; setLoading(true); setError(""); void deploymentApi.list(tenantId, offset).then((p) => { if (current) setPage(p); }).catch((e) => { if (current) setError(deploymentError(e)); }).finally(() => { if (current) setLoading(false); }); return () => { current = false; }; }, [tenantId, offset, epoch]);
  return <div className={styles.page}><PageHeader eyebrow="DEPLOYMENT" title="部署" description="将 Agent 版本与运行配置版本组合，发布固定的执行配置快照。" action={<Link className="button primary" href={`${deploymentHref(tenantId)}/new`}>＋ 新建部署</Link>} />
    <div className={styles.banner}>选择两份已发布版本 → 按类别和名称匹配 → 校验并发布。发布配置不等于启动 Agent 或切换流量。</div>
    {error && <div className={styles.error} role="alert">{error} <Button variant="secondary" onClick={() => setEpoch(epoch + 1)}>重新读取部署</Button></div>}
    <section className={styles.card} aria-label="部署列表">{loading ? <p role="status" className={styles.loading}>正在读取部署…</p> : error ? null : !page?.deployments.length ? <EmptyState title="还没有部署" detail="准备好 Agent 和运行配置的已发布版本后，创建你的第一份部署。" /> : <div className={styles.scroll}><table className={styles.table}><thead><tr><th>名称 / 描述</th><th>最近发布</th><th>创建者</th><th>更新时间</th><th>操作</th></tr></thead><tbody>{page.deployments.map((d) => <tr key={d.id}><td><Link className={styles.link} href={deploymentHref(tenantId, d.id)}><strong>{d.name}</strong></Link><p className={styles.small}>{d.description || "暂无描述"}</p></td><td><StatusBadge tone={d.latest_revision_number ? "blue" : "gray"}>{d.latest_revision_number ? `已发布 r${d.latest_revision_number}` : "尚未发布"}</StatusBadge></td><td className={styles.small}>{d.created_by}</td><td>{new Date(d.updated_at).toLocaleString("zh-CN")}</td><td><Link className={styles.link} href={deploymentHref(tenantId, d.id)}>打开工作台 →</Link>{d.latest_revision_number && <p><Link className={styles.link} href={deploymentHref(tenantId, d.id, d.latest_revision_number)}>查看最近版本</Link></p>}</td></tr>)}</tbody></table></div>}
      {!loading && !error && !!page?.total && <div className={styles.actions}><span className={styles.small}>{offset + 1}–{Math.min(offset + 20, page.total)} / {page.total}</span><Button variant="secondary" disabled={!offset} onClick={() => setOffset(offset - 20)}>上一页</Button><Button variant="secondary" disabled={offset + 20 >= page.total} onClick={() => setOffset(offset + 20)}>下一页</Button></div>}
    </section></div>;
}
