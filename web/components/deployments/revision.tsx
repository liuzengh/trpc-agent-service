"use client";
import Link from "next/link";
import { useEffect, useState } from "react";
import { deploymentApi, deploymentError, type DeploymentRevision } from "../../lib/deployment-api";
import { deploymentHref } from "../../lib/deployment-editor-state";
import { PageHeader, Button, StatusBadge } from "../ui";
import styles from "./deployment.module.css";
import { ArtifactPanel } from "./artifact-panel";
import { KnowledgeImportPanel } from "./knowledge-import-panel";
export function DeploymentRevisionDetail({ tenantId, deploymentId, revisionNumber }: { tenantId: string; deploymentId: string; revisionNumber: number }) {
  const [revision, setRevision] = useState<DeploymentRevision | null>(null); const [name, setName] = useState("部署"); const [error, setError] = useState(""); const [epoch, setEpoch] = useState(0);
  useEffect(() => { let current = true; setRevision(null); setError("");
    if (!Number.isSafeInteger(revisionNumber) || revisionNumber < 1) { setError("版本号必须为正整数，请从发布历史选择版本。"); return; }
    void Promise.all([deploymentApi.get(tenantId, deploymentId), deploymentApi.getRevision(tenantId, deploymentId, revisionNumber)]).then(([d, v]) => { if (current) { setName(d.name); setRevision(v); } }).catch((e) => { if (current) setError(deploymentError(e)); });
    return () => { current = false; };
  }, [tenantId, deploymentId, revisionNumber, epoch]);
  const root = `/tenants/${encodeURIComponent(tenantId)}`; const view = revision?.manifest_view;
  const channelHref = revision?.tenant_id === tenantId && revision.deployment_id === deploymentId && revision.revision_number === revisionNumber
    ? `${root}/channels?${new URLSearchParams({ deployment_id: revision.deployment_id, revision_number: String(revision.revision_number) })}`
    : null;
  const knowledgeResources = view ? [...new Set(Object.values(view.agent_plan.nodes).flatMap((node) => node.knowledge_resources ?? []))].filter((resource) => {
    const item = view.resources.knowledge[resource];
    return item && typeof item === "object" && "kind" in item && item.kind === "managed_knowledge";
  }) : [];
  function download() { if (!revision) return; const url = URL.createObjectURL(new Blob([JSON.stringify(revision.manifest_view, null, 2)], { type: "application/json" })); const a = document.createElement("a"); a.href = url; a.download = `deployment-r${revision.revision_number}-manifest-view.json`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); }
  const migrationHref = `${root}/deployments/${encodeURIComponent(deploymentId)}/migrate?source=${revisionNumber}`;
  return <div className={styles.page}><PageHeader eyebrow="IMMUTABLE DEPLOYMENT REVISION" title={`${name} · r${revisionNumber}`} description="这是固定的发布快照。历史版本不随 Agent 或 Profile 的最新版本变化。" action={<div className={styles.actions}>{channelHref && <Link className="button primary" href={channelHref}>接入渠道</Link>}<Link className="button secondary" href={migrationHref}>迁移存储</Link><Link className="button secondary" href={deploymentHref(tenantId, deploymentId)}>返回部署工作台</Link></div>} />
    {error ? <div className={styles.error} role="alert">{error} <Button variant="secondary" onClick={() => setEpoch(epoch + 1)}>重试读取版本</Button></div> : !revision ? <p className={styles.loading} role="status">正在读取版本快照…</p> : view && <>
      <div className={styles.banner}><StatusBadge tone="blue">已发布 r{revision.revision_number}</StatusBadge>　{new Date(revision.published_at).toLocaleString("zh-CN")} · {revision.published_by}<br />已生成固定执行快照；发布不会自动接入渠道或切换现有流量。请在渠道接入中明确选择此固定版本，并分别确认接入与消息路由的启停。</div>
      <section className={styles.card}><h2>固定来源</h2><div className={styles.sources}><div><p>Agent · v{revision.agent_version_number}</p><Link className={styles.link} href={`${root}/agents/${encodeURIComponent(revision.agent_id)}/versions/${revision.agent_version_number}`}>查看 Agent v{revision.agent_version_number} →</Link><p className={styles.small}>{revision.agent_id}<br />{view.sources.agent.digest}</p></div><div><p>运行配置 · r{revision.profile_revision_number}</p><Link className={styles.link} href={`${root}/runtime-profiles/${encodeURIComponent(revision.profile_id)}/revisions/${revision.profile_revision_number}`}>查看 Profile r{revision.profile_revision_number} / 当前凭据 →</Link><p className={styles.small}>{revision.profile_id}<br />{view.sources.profile.digest}</p></div></div></section>
      <section className={styles.card}><h2>逐节点资源分配</h2><p className={styles.small}>只展示真实 Manifest 中的节点分配，不把整个 Profile 自动授权给所有节点。</p><div className={styles.scroll}><table className={styles.table}><thead><tr><th>节点 / 类型</th><th>模型</th><th>Tools</th><th>Knowledge</th><th>调用入口</th></tr></thead><tbody>{Object.entries(view.agent_plan.nodes).map(([id, node]) => <tr key={id}><td><strong>{id}</strong><div className={styles.small}>{node.kind}</div></td><td>{node.model_resource ?? "—"}</td><td>{node.tool_resources?.join("、") || "无"}</td><td>{node.knowledge_resources?.join("、") || "无"}</td><td>{node.callable_entries?.join("、") || "无"}</td></tr>)}</tbody></table></div></section>
      <section className={styles.card}><h2>执行配置</h2><div className={styles.sources}><div><h3>Storage 角色</h3>{Object.entries(view.storage_roles).map(([role, resource]) => <p key={role}>{role} → {resource}</p>)}{!view.storage_roles.memory && <p className={styles.small}>memory 未启用</p>}</div><div><h3>平台执行上限</h3><p>{view.execution.max_run_seconds} 秒 / {view.execution.max_tool_calls} 次工具调用 / {view.execution.max_output_tokens} 输出 Tokens</p><p className={styles.small}>{view.execution.backend}</p></div></div><details><summary>实际选用的资源与已解析关系</summary><pre className={styles.json}>{JSON.stringify({ resources: view.resources, resolved_requirements: view.resolved_requirements }, null, 2)}</pre></details><p className={styles.small}>credential_present 仅表示快照包含凭据关联，不表示当前凭据在线有效。当前状态请在 Profile 查看。</p></section>
      <section className={styles.card}><h2>发布技术信息</h2><dl className={styles.small}><dt>Manifest ID</dt><dd>{revision.manifest_id}</dd><dt>Manifest Digest</dt><dd>{revision.manifest_digest}</dd><dt>Input Digest</dt><dd>{revision.input_digest}</dd><dt>Compiler / Runtime Contract</dt><dd>{view.compiler_version} / {view.runtime_contract_version}</dd><dt>Platform Contract</dt><dd>{view.platform_contract.version} / {view.platform_contract.digest}</dd></dl><details><summary>脱敏 Manifest JSON</summary><pre className={styles.json}>{JSON.stringify(view, null, 2)}</pre></details><p className={styles.small}>脱敏视图不能重新计算内部 Manifest Digest。</p><div className={styles.actions}><Button variant="secondary" onClick={download}>下载脱敏 JSON</Button><Link className="button primary" href={`${deploymentHref(tenantId, deploymentId)}?from=${revision.revision_number}&prepare=1`}>基于此版本准备新发布</Link></div></section>
      {channelHref && knowledgeResources.length > 0 && <KnowledgeImportPanel key={revision.manifest_id + ":knowledge"} tenantId={tenantId} deploymentId={deploymentId} revisionNumber={revisionNumber} resources={knowledgeResources} />}
      {channelHref && view.storage_roles.artifact && Object.values(view.agent_plan.nodes).some((node) => node.artifact?.enabled === true && node.artifact.resource === view.storage_roles.artifact) && <ArtifactPanel key={revision.manifest_id} tenantId={tenantId} deploymentId={deploymentId} revisionNumber={revisionNumber} />}
    </>}
  </div>;
}
