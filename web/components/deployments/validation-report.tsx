"use client";
import Link from "next/link";
import { useState } from "react";
import { Button } from "../ui";
import type { DeploymentReport, DeploymentDiagnostic } from "../../lib/deployment-api";
import type { Selection } from "../../lib/deployment-editor-state";
import styles from "./deployment.module.css";
const labels: Record<string, string> = {
  DEPLOYMENT_RESOURCE_MISSING: "缺少同类别、同名资源", DEPLOYMENT_CAPABILITY_MISMATCH: "资源能力不满足节点需求", DEPLOYMENT_UNUSED_REQUIREMENT: "声明存在，但没有节点使用",
  DEPLOYMENT_STORAGE_ROLE_MISSING: "缺少必需的 session 存储", DEPLOYMENT_STORAGE_ROLE_UNSUPPORTED: "存储不支持对应运行角色", DEPLOYMENT_CREDENTIAL_UNAVAILABLE: "所需凭据目前不可用",
  DEPLOYMENT_ADAPTER_UNSUPPORTED: "平台未接入所需资源适配器", DEPLOYMENT_EXECUTION_RANGE_DENIED: "连接目标不在平台允许范围", DEPLOYMENT_LIMIT_EXCEEDED: "配置超过平台上限",
  DEPLOYMENT_MANIFEST_TOO_LARGE: "执行快照超过平台大小限制", DEPLOYMENT_ENTRYPOINT_UNSUPPORTED: "节点调用入口不受支持", DEPLOYMENT_SOURCE_SCHEMA_UNSUPPORTED: "来源协议不受支持",
};
export function diagnosticRepair(tenant: string, selection: Selection, d: DeploymentDiagnostic, returnTo: string): { href: string; label: string } | null {
  if (d.source === "platform" || d.source === "input") return null;
  const root = `/tenants/${encodeURIComponent(tenant)}`;
  if (d.code === "DEPLOYMENT_UNUSED_REQUIREMENT" || (d.source === "agent" && !["DEPLOYMENT_RESOURCE_MISSING", "DEPLOYMENT_CAPABILITY_MISMATCH"].includes(d.code))) return { href: `${root}/agents/${encodeURIComponent(selection.agentId)}`, label: "查看 Agent 声明" };
  if (!selection.profileId) return null;
  const query = new URLSearchParams({ returnTo });
  if (d.category && d.name) { query.set("focusCategory", d.category); query.set("focusName", d.name); }
  const credentials = d.code === "DEPLOYMENT_CREDENTIAL_UNAVAILABLE";
  return { href: `${root}/runtime-profiles/${encodeURIComponent(selection.profileId)}${credentials ? `/revisions/${selection.profileRevision}` : ""}?${query}`, label: credentials ? "查看当前凭据状态" : "去 Profile 草稿修复" };
}
export function ValidationReport({ report, tenantId, selection, returnTo, checkedAt }: { report: DeploymentReport | null; tenantId: string; selection: Selection; returnTo: string; checkedAt?: string }) {
  const [copyNotice, setCopyNotice] = useState("");
  async function copy() {
    try { await navigator.clipboard.writeText(JSON.stringify(report, null, 2)); setCopyNotice("诊断已复制"); }
    catch { setCopyNotice("复制失败，可展开技术信息手动复制。"); }
  }
  return <section className={styles.card} aria-label="服务端校验结果"><h2>服务端校验</h2>
    {!report ? <p>尚未校验当前选择。名称匹配不等于满足发布条件。</p> : <>
      <strong className={report.valid ? styles.good : styles.bad}>{report.valid ? "校验通过" : "校验未通过"} · {report.diagnostics.filter((d) => d.severity === "error").length} 错误 / {report.diagnostics.filter((d) => d.severity === "warning").length} 警告</strong>
      {report.diagnostics.map((d, index) => { const fix = diagnosticRepair(tenantId, selection, d, returnTo); return <article className={styles.diagnostic} key={`${d.code}/${d.path}/${index}`}>
        <strong className={d.severity === "error" ? styles.bad : styles.muted}>{d.severity === "error" ? "错误" : "警告"} · {labels[d.code] ?? d.code}</strong>
        <p>{d.category && d.name ? `${d.category}.${d.name} · ` : ""}{d.node_id ? `节点 ${d.node_id} · ` : ""}{d.message}</p>
        <div className={styles.small}>{d.source} · {d.path}</div>
        {fix ? <Link className={styles.link} href={fix.href}>{fix.label} →</Link> : <p className={styles.small}>{d.source === "input" ? "请返回上方重新选择固定来源。" : "由平台执行配置处理；部署表单不修改平台允许范围。"}</p>}
      </article>; })}
      <Button variant="ghost" onClick={() => void copy()}>复制诊断</Button>{copyNotice && <p role="status" className={styles.small}>{copyNotice}</p>}
      <details className={styles.small}><summary>校验技术信息</summary><p>{checkedAt ? `校验于 ${checkedAt}` : ""}<br />{report.compiler_version}<br />{report.platform_contract_digest}</p><pre className={styles.json}>{JSON.stringify(report, null, 2)}</pre></details>
    </>}
    <p className={styles.small}>校验不预留版本、不探测 Provider 在线状态；发布时会再次校验。</p>
  </section>;
}
