"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { channelApi, channelError, type ChannelBinding } from "../../lib/channel-api";
import { deploymentApi, deploymentError, type DeploymentRevision } from "../../lib/deployment-api";
import { emptySelection, inputFrom, type Selection } from "../../lib/deployment-editor-state";
import { Button, PageHeader, StatusBadge } from "../ui";
import { SourceSelector } from "./source-selector";
import styles from "./deployment.module.css";

type Stage = "PREPARING" | "MIGRATING" | "PUBLISHED" | "SWITCHING" | "SWITCHED";

export function BackendMigration({ tenantId, deploymentId, sourceRevision, initialBindingId = "" }: { tenantId: string; deploymentId: string; sourceRevision: number; initialBindingId?: string }) {
  const [source, setSource] = useState<DeploymentRevision | null>(null);
  const [selection, setSelection] = useState<Selection>(emptySelection());
  const [bindingId, setBindingId] = useState(initialBindingId);
  const [binding, setBinding] = useState<ChannelBinding | null>(null);
  const [stage, setStage] = useState<Stage>("PREPARING");
  const [copied, setCopied] = useState(0);
  const [publishedRevision, setPublishedRevision] = useState<number | null>(null);
	const [expectedLatest, setExpectedLatest] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const input = useMemo(() => inputFrom(selection), [selection]);

  useEffect(() => {
    let current = true;
    void Promise.all([deploymentApi.getRevision(tenantId, deploymentId, sourceRevision), deploymentApi.get(tenantId, deploymentId)]).then(([revision, deployment]) => {
      if (!current) return;
      setSource(revision);
		setExpectedLatest(deployment.latest_revision_number);
      setSelection({ agentId: revision.agent_id, agentVersion: revision.agent_version_number, profileId: revision.profile_id, profileRevision: revision.profile_revision_number });
    }).catch((e) => { if (current) setError(deploymentError(e)); });
    return () => { current = false; };
  }, [tenantId, deploymentId, sourceRevision]);

  async function loadBinding(): Promise<ChannelBinding> {
    const details = await channelApi.getBinding(tenantId, bindingId.trim());
    if (details.binding.target.deployment_id !== deploymentId || details.binding.target.revision_number !== sourceRevision) throw new Error("该 ChannelBinding 当前未指向来源 DeploymentRevision，请重新选择。");
    setBinding(details.binding);
    return details.binding;
  }

  async function migrate() {
    if (!source || !input || !bindingId.trim()) return;
    setBusy(true); setError(""); setStage("MIGRATING");
    try {
      const currentBinding = await loadBinding();
      const key = crypto.randomUUID();
      const result = await deploymentApi.migrateAndPublish(tenantId, deploymentId, {
        source_revision_number: sourceRevision,
        expected_latest_revision_number: expectedLatest,
        input,
      }, key);
      const revision = result.publication.revision.revision_number;
      setCopied(result.memory_scopes_copied); setPublishedRevision(revision); setStage("PUBLISHED");
      await switchBinding(currentBinding, revision);
    } catch (e) {
      setError(e instanceof Error && e.message.startsWith("该 ChannelBinding") ? e.message : deploymentError(e));
      setStage((current) => current === "PUBLISHED" || current === "SWITCHING" ? "PUBLISHED" : "PREPARING");
    } finally { setBusy(false); }
  }

  async function switchBinding(current = binding, revision = publishedRevision) {
    if (!current || !revision) return;
    setBusy(true); setError(""); setStage("SWITCHING");
    try {
      const result = await channelApi.setBindingTarget(tenantId, current.binding_id, {
        expected_binding_revision: current.binding_revision,
        target: { deployment_id: deploymentId, revision_number: revision },
      }, crypto.randomUUID());
      if (!result.binding) throw new Error("渠道切换响应缺少 Binding。");
      setBinding(result.binding); setStage("SWITCHED");
    } catch (e) { setError(channelError(e)); setStage("PUBLISHED"); }
    finally { setBusy(false); }
  }

  const steps = ["迁移并校验 Memory", "创建并发布 DeploymentRevision", "CAS 切换 ChannelBinding"];
  const stateFor = (index: number) => {
	if (stage === "SWITCHED") return "完成";
	if (stage === "SWITCHING") return index < 2 ? "完成" : "进行中";
	if (stage === "PUBLISHED") return index < 2 ? "完成" : "等待";
	if (stage === "MIGRATING") return index === 0 ? "进行中" : "等待";
	return "等待";
  };
  return <div className={styles.page}>
    <PageHeader eyebrow="BACKEND MIGRATION" title={`迁移 Deployment r${sourceRevision}`} description="迁移成功后才发布新版本；发布成功后再以 Binding Revision 并发控制切换渠道。" action={<Link className="button secondary" href={`/tenants/${encodeURIComponent(tenantId)}/deployments/${encodeURIComponent(deploymentId)}/revisions/${sourceRevision}`}>返回来源版本</Link>} />
    <section className={styles.card}><h2>迁移进度</h2><ol className={styles.progress}>{steps.map((label, index) => { const state = stateFor(index); return <li key={label}><StatusBadge tone={state === "完成" ? "green" : state === "进行中" ? "blue" : "gray"}>{state}</StatusBadge><span>{label}</span></li>; })}</ol>{stage === "SWITCHED" && <div className={styles.banner}>迁移完成：复制 {copied} 个 Memory Scope，新版本 r{publishedRevision} 已发布，ChannelBinding 已切换。</div>}</section>
    {error && <div className={styles.error} role="alert">{error}</div>}
    <SourceSelector tenantId={tenantId} value={selection} onChange={setSelection} disabled={busy || publishedRevision !== null} />
    <section className={styles.card}><h2>渠道切换</h2><label className={styles.field}><span>ChannelBinding ID</span><input aria-label="ChannelBinding ID" value={bindingId} disabled={busy || publishedRevision !== null} onChange={(e) => setBindingId(e.target.value)} placeholder="cbd_…" /></label><p className={styles.small}>开始前会确认 Binding 正指向本页来源 r{sourceRevision}；最终切换使用读取到的 binding_revision 做 CAS。</p></section>
    <div className={styles.bar}><span className={styles.small}>{publishedRevision ? `新版本 r${publishedRevision} 已固定；渠道${stage === "SWITCHED" ? "已" : "尚未"}切换。` : "尚未修改发布状态或渠道目标。"}</span><div className={styles.actions}>{publishedRevision && stage !== "SWITCHED" ? <Button disabled={busy} onClick={() => void switchBinding()}>{busy ? "正在切换…" : "重试切换渠道"}</Button> : <Button disabled={busy || !source || expectedLatest === null || !input || !bindingId.trim() || stage === "SWITCHED"} onClick={() => void migrate()}>{busy ? "正在执行…" : "迁移、发布并切换"}</Button>}</div></div>
  </div>;
}
