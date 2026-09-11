"use client";

import { ArrowLeft, CheckCircle2, Copy, Fingerprint } from "lucide-react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";

import { ApiNotice, Button, PageHeader, StatCard, StatusBadge } from "../../../../../../../components/ui";
import { controlApi, type Agent, type AgentVersion } from "../../../../../../../lib/control-api";

export default function AgentVersionPage() {
  const { tenantId, agentId, versionNumber } = useParams<{ tenantId: string; agentId: string; versionNumber: string }>();
  const [agent, setAgent] = useState<Agent | null>(null);
  const [version, setVersion] = useState<AgentVersion | null>(null);
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState<unknown>();

  useEffect(() => {
    const parsedVersion = Number(versionNumber);
    if (!Number.isSafeInteger(parsedVersion) || parsedVersion < 1) {
      setError(new Error("版本号必须是正整数"));
      return;
    }
    void Promise.all([
      controlApi.getAgent(tenantId, agentId),
      controlApi.getAgentVersion(tenantId, agentId, parsedVersion),
    ]).then(([currentAgent, currentVersion]) => {
      setAgent(currentAgent);
      setVersion(currentVersion);
    }).catch(setError);
  }, [agentId, tenantId, versionNumber]);

  const agentHref = `/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}`;

  async function copySpec() {
    if (!version) return;
    await navigator.clipboard.writeText(JSON.stringify(version.spec, null, 2));
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1600);
  }

  return (
    <>
      <PageHeader
        eyebrow="IMMUTABLE AGENT VERSION"
        title={agent && version ? `${agent.name} · v${version.version_number}` : "Agent 版本"}
        description="这是 Control API 返回的不可变 Canonical AgentSpec。此页面不会修改 Draft。"
        action={<div className="toolbar-group"><Link className="button secondary" href={agentHref}><ArrowLeft size={15} />返回工作台</Link>{version && <Link className="button primary" href={`/tenants/${encodeURIComponent(tenantId)}/deployments/new?agent=${encodeURIComponent(agentId)}&version=${version.version_number}`}>用此版本创建部署</Link>}</div>}
      />
      <ApiNotice error={error} />
      {!version && !error && <div className="panel-loading"><div className="loading-mark" /><span>正在读取不可变版本…</span></div>}
      {version && (
        <>
          <div className="stats-grid version-stats">
            <StatCard detail="不可变的单调版本号" label="版本" value={`v${version.version_number}`} />
            <StatCard detail="发布时使用的 Draft revision" label="来源 Draft" value={version.source_draft_revision} />
            <StatCard detail="AgentSpec 协议" label="Schema" value={version.schema_version} />
          </div>
          <div className="panel immutable-version-panel">
            <div className="toolbar">
              <div className="toolbar-group"><StatusBadge tone="green"><CheckCircle2 size={12} />已发布</StatusBadge><span>{new Date(version.published_at).toLocaleString("zh-CN")}</span></div>
              <Button onClick={() => void copySpec()} variant="secondary"><Copy size={14} />{copied ? "已复制" : "复制 JSON"}</Button>
            </div>
            <dl className="version-metadata">
              <div><dt>Version ID</dt><dd>{version.id}</dd></div>
              <div><dt>发布人</dt><dd>{version.published_by}</dd></div>
              <div className="digest-row"><dt><Fingerprint size={14} />Spec Digest</dt><dd>{version.spec_digest}</dd></div>
            </dl>
            <div className="json-readonly"><header><strong>Canonical AgentSpec</strong><span>只读</span></header><pre>{JSON.stringify(version.spec, null, 2)}</pre></div>
          </div>
        </>
      )}
    </>
  );
}
