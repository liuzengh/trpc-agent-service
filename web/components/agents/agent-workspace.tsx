"use client";

import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Clock3,
  Copy,
  Download,
  Edit3,
  ExternalLink,
  History,
  RefreshCw,
  Save,
  Send,
  ShieldAlert,
  X,
} from "lucide-react";
import Link from "next/link";
import { type FormEvent, useCallback, useEffect, useMemo, useState } from "react";

import {
  controlApi,
  ControlApiError,
  type Agent,
  type AgentDraft,
  type AgentSpecDocument,
  type AgentVersion,
  type ValidationReport,
} from "../../lib/control-api";
import { ApiNotice, Button, Field, PageHeader, StatusBadge } from "../ui";
import { AgentSpecEditor } from "./agent-spec-editor";

const VERSION_PAGE_SIZE = 20;

type BusyOperation = "metadata" | "saving" | "validating" | "publishing" | "reloading" | null;

type DraftConflict = {
  expectedRevision: number;
  latest: AgentDraft | null;
};

export function AgentWorkspace({ tenantId, agentId }: { tenantId: string; agentId: string }) {
  const [agent, setAgent] = useState<Agent | null>(null);
  const [draft, setDraft] = useState<AgentDraft | null>(null);
  const [workingSpec, setWorkingSpec] = useState<AgentSpecDocument>({});
  const [dirty, setDirty] = useState(false);
  const [validation, setValidation] = useState<ValidationReport | null>(null);
  const [versions, setVersions] = useState<AgentVersion[]>([]);
  const [versionTotal, setVersionTotal] = useState(0);
  const [versionOffset, setVersionOffset] = useState(0);
  const [busy, setBusy] = useState<BusyOperation>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>();
  const [conflict, setConflict] = useState<DraftConflict | null>(null);
  const [notice, setNotice] = useState("");
  const [editingMetadata, setEditingMetadata] = useState(false);
  const [metadataName, setMetadataName] = useState("");
  const [metadataDescription, setMetadataDescription] = useState("");

  const loadVersions = useCallback(async () => {
    const page = await controlApi.listAgentVersions(tenantId, agentId, {
      offset: versionOffset,
      limit: VERSION_PAGE_SIZE,
    });
    setVersions(page.versions);
    setVersionTotal(page.total);
  }, [agentId, tenantId, versionOffset]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      const [currentAgent, currentDraft] = await Promise.all([
        controlApi.getAgent(tenantId, agentId),
        controlApi.getAgentDraft(tenantId, agentId),
      ]);
      setAgent(currentAgent);
      setDraft(currentDraft);
      setWorkingSpec(currentDraft.spec);
      setDirty(false);
      setMetadataName(currentAgent.name);
      setMetadataDescription(currentAgent.description);
    } catch (caught) {
      setError(caught);
    } finally {
      setLoading(false);
    }
  }, [agentId, tenantId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    void loadVersions().catch(setError);
  }, [loadVersions]);

  const tenantSegment = encodeURIComponent(tenantId);
  const agentSegment = encodeURIComponent(agentId);
  const listHref = `/tenants/${tenantSegment}/agents`;
  const versionBaseHref = `${listHref}/${agentSegment}/versions`;

  const status = useMemo(() => {
    if (busy === "saving") return { tone: "amber" as const, label: "正在保存 Draft" };
    if (busy === "validating") return { tone: "amber" as const, label: "正在保存并校验" };
    if (busy === "publishing") return { tone: "amber" as const, label: "正在保存、校验并发布" };
    if (conflict) return { tone: "amber" as const, label: "Revision 冲突" };
    if (dirty) return { tone: "amber" as const, label: "有未保存修改" };
    return { tone: "green" as const, label: "与服务端 Draft 一致" };
  }, [busy, conflict, dirty]);

  async function captureOperationError(caught: unknown, expectedRevision: number) {
    setError(caught);
    if (caught instanceof ControlApiError && caught.validation) {
      setValidation(caught.validation);
    }
    if (caught instanceof ControlApiError && (
      caught.status === 409 || caught.code === "AGENT_DRAFT_REVISION_CONFLICT"
    )) {
      const latest = await controlApi.getAgentDraft(tenantId, agentId).catch(() => null);
      setConflict({ expectedRevision, latest });
    }
  }

  async function persistWorkingSpec(): Promise<AgentDraft> {
    if (!draft) throw new Error("Draft 尚未加载");
    if (!dirty) return draft;
    const saved = await controlApi.saveAgentDraft(tenantId, agentId, {
      expected_revision: draft.revision,
      spec: workingSpec,
    });
    setDraft(saved);
    setWorkingSpec(saved.spec);
    setDirty(false);
    setConflict(null);
    return saved;
  }

  async function saveDraft() {
    if (!draft || busy) return;
    setBusy("saving");
    setError(undefined);
    setNotice("");
    try {
      const saved = await persistWorkingSpec();
      setNotice(`Draft revision ${saved.revision} 已保存到 Control API。`);
    } catch (caught) {
      await captureOperationError(caught, draft.revision);
    } finally {
      setBusy(null);
    }
  }

  async function validateDraft() {
    if (!draft || busy) return;
    let attemptedRevision = draft.revision;
    setBusy("validating");
    setError(undefined);
    setNotice("");
    try {
      const current = await persistWorkingSpec();
      attemptedRevision = current.revision;
      const report = await controlApi.validateAgentDraft(tenantId, agentId, {
        expected_revision: current.revision,
      });
      setValidation(report);
      setNotice(report.valid
        ? `Draft revision ${current.revision} 通过服务端发布校验。`
        : `Draft revision ${current.revision} 已完成校验，请处理错误后再发布。`);
    } catch (caught) {
      await captureOperationError(caught, attemptedRevision);
    } finally {
      setBusy(null);
    }
  }

  async function publishDraft() {
    if (!draft || busy) return;
    let attemptedRevision = draft.revision;
    setBusy("publishing");
    setError(undefined);
    setNotice("");
    try {
      const current = await persistWorkingSpec();
      attemptedRevision = current.revision;
      const report = await controlApi.validateAgentDraft(tenantId, agentId, {
        expected_revision: current.revision,
      });
      setValidation(report);
      if (!report.valid) {
        setNotice(`Draft revision ${current.revision} 未通过服务端校验，未执行发布。`);
        return;
      }
      const result = await controlApi.publishAgentVersion(tenantId, agentId, {
        expected_revision: current.revision,
      });
      setValidation(result.validation);
      setNotice(`AgentVersion v${result.version.version_number} 已由 Control API 返回。重复发布同一 revision 会返回同一版本。`);
      const [updatedAgent] = await Promise.all([
        controlApi.getAgent(tenantId, agentId),
        loadVersions(),
      ]);
      setAgent(updatedAgent);
    } catch (caught) {
      await captureOperationError(caught, attemptedRevision);
    } finally {
      setBusy(null);
    }
  }

  async function reloadServerDraft() {
    if (busy) return;
    setBusy("reloading");
    setError(undefined);
    try {
      const latest = await controlApi.getAgentDraft(tenantId, agentId);
      setDraft(latest);
      setWorkingSpec(latest.spec);
      setDirty(false);
      setConflict(null);
      setValidation(null);
      setNotice(`已明确放弃本地修改并加载服务端 revision ${latest.revision}。`);
    } catch (caught) {
      setError(caught);
    } finally {
      setBusy(null);
    }
  }

  async function copyLocalSpec() {
    await navigator.clipboard.writeText(JSON.stringify(workingSpec, null, 2));
    setNotice("本地未保存 AgentSpec JSON 已复制。");
  }

  function downloadLocalSpec() {
    const url = URL.createObjectURL(new Blob([JSON.stringify(workingSpec, null, 2)], { type: "application/json" }));
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${agent?.name || agentId}-local-agentspec.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  async function updateMetadata(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy || !agent) return;
    setBusy("metadata");
    setError(undefined);
    try {
      const updated = await controlApi.updateAgent(tenantId, agentId, {
        name: metadataName.trim(),
        description: metadataDescription.trim(),
      });
      setAgent(updated);
      setMetadataName(updated.name);
      setMetadataDescription(updated.description);
      setEditingMetadata(false);
      setNotice("Agent 名称与描述已保存。");
    } catch (caught) {
      setError(caught);
    } finally {
      setBusy(null);
    }
  }

  function updateWorkingSpec(next: AgentSpecDocument) {
    setWorkingSpec(next);
    setDirty(true);
    setValidation(null);
    setNotice("");
  }

  if (loading) {
    return <div className="panel-loading"><div className="loading-mark" /><span>正在读取 Agent、Draft 与版本…</span></div>;
  }

  return (
    <>
      <PageHeader
        eyebrow="AGENT WORKSPACE"
        title={agent?.name ?? "Agent 工作台"}
        description={agent ? `${agent.description || "暂无描述"} · ${agent.id}` : agentId}
        action={
          <div className="header-actions">
            <Link className="button secondary" href={listHref}><ArrowLeft size={15} />Agent 列表</Link>
            <Button onClick={() => setEditingMetadata((value) => !value)} variant="secondary"><Edit3 size={14} />编辑信息</Button>
          </div>
        }
      />
      <ApiNotice error={error} />
      {notice && <div className="success-notice" role="status"><CheckCircle2 size={15} />{notice}</div>}
      {conflict && (
        <section className="conflict-panel" role="alert">
          <div className="conflict-heading"><ShieldAlert size={20} /><div><h2>Draft revision 已发生冲突</h2><p>本地使用 revision {conflict.expectedRevision}，{conflict.latest ? `服务端当前为 revision ${conflict.latest.revision}` : "服务端最新 revision 读取失败"}。本地内容仍保留，页面没有自动覆盖或重试。</p></div></div>
          <div className="conflict-actions">
            <Button onClick={() => void copyLocalSpec()} variant="secondary"><Copy size={14} />复制本地 JSON</Button>
            <Button onClick={downloadLocalSpec} variant="secondary"><Download size={14} />下载本地 JSON</Button>
            <Button disabled={busy !== null} onClick={() => void reloadServerDraft()} variant="danger"><RefreshCw size={14} />放弃本地并重载</Button>
          </div>
          {conflict.latest && <details><summary>查看服务端最新 Draft</summary><pre>{JSON.stringify(conflict.latest.spec, null, 2)}</pre></details>}
        </section>
      )}
      {editingMetadata && agent && (
        <form className="panel metadata-editor" onSubmit={(event) => void updateMetadata(event)}>
          <div className="metadata-fields">
            <Field label="名称" maxLength={128} onChange={(event) => setMetadataName(event.target.value)} required value={metadataName} />
            <label className="field"><span>描述</span><textarea maxLength={4096} onChange={(event) => setMetadataDescription(event.target.value)} rows={3} value={metadataDescription} /></label>
          </div>
          <div className="metadata-actions"><Button onClick={() => setEditingMetadata(false)} type="button" variant="ghost"><X size={14} />取消</Button><Button disabled={busy !== null || !metadataName.trim()} type="submit">保存信息</Button></div>
        </form>
      )}
      <section className="draft-command-bar">
        <div className="draft-status">
          <StatusBadge tone={status.tone}>{status.label}</StatusBadge>
          <span><Clock3 size={13} />Draft revision <strong>{draft?.revision ?? "—"}</strong></span>
          {agent?.latest_version_number === null ? <span>尚未发布版本</span> : <span>最新版本 <strong>v{agent?.latest_version_number}</strong></span>}
        </div>
        <div className="draft-actions">
          <Button disabled={!draft || !dirty || busy !== null || conflict !== null} onClick={() => void saveDraft()} variant="secondary"><Save size={14} />{busy === "saving" ? "保存中…" : "保存 Draft"}</Button>
          <Button disabled={!draft || busy !== null || conflict !== null} onClick={() => void validateDraft()} variant="secondary"><CheckCircle2 size={14} />{busy === "validating" ? "校验中…" : dirty ? "保存并校验" : "校验 Draft"}</Button>
          <Button disabled={!draft || busy !== null || conflict !== null} onClick={() => void publishDraft()}><Send size={14} />{busy === "publishing" ? "发布中…" : dirty ? "保存、校验并发布" : "校验并发布"}</Button>
        </div>
      </section>
      <div className="agent-workspace-layout">
        <section className="agent-editor-stage">
          <AgentSpecEditor
            diagnostics={validation?.diagnostics}
            disabled={busy !== null}
            onChange={updateWorkingSpec}
            storageKey={`agent-editor:${tenantId}:${agentId}:${draft?.revision ?? "loading"}`}
            value={workingSpec}
          />
        </section>
        <aside className="agent-workspace-sidebar">
          <ValidationPanel report={validation} />
          <section className="panel version-panel">
            <header><div><span className="feature-icon"><History size={16} /></span><div><h2>版本历史</h2><p>Control API 中的不可变版本</p></div></div><span>{versionTotal}</span></header>
            {versions.length === 0 ? (
              <div className="compact-empty">校验通过并发布后，版本会出现在这里。</div>
            ) : (
              <div className="version-list">
                {versions.map((version) => (
                  <Link href={`${versionBaseHref}/${version.version_number}`} key={version.id}>
                    <span><strong>v{version.version_number}</strong><small>Draft r{version.source_draft_revision}</small></span>
                    <span><time>{new Date(version.published_at).toLocaleString("zh-CN")}</time><ExternalLink size={13} /></span>
                  </Link>
                ))}
              </div>
            )}
            {versionTotal > VERSION_PAGE_SIZE && (
              <div className="compact-pagination">
                <Button disabled={versionOffset === 0} onClick={() => setVersionOffset((value) => Math.max(0, value - VERSION_PAGE_SIZE))} variant="ghost">上一页</Button>
                <span>{versionOffset + 1}–{Math.min(versionOffset + VERSION_PAGE_SIZE, versionTotal)}</span>
                <Button disabled={versionOffset + VERSION_PAGE_SIZE >= versionTotal} onClick={() => setVersionOffset((value) => value + VERSION_PAGE_SIZE)} variant="ghost">下一页</Button>
              </div>
            )}
          </section>
        </aside>
      </div>
    </>
  );
}

function ValidationPanel({ report }: { report: ValidationReport | null }) {
  if (!report) {
    return <section className="panel validation-panel"><header><span className="feature-icon"><CheckCircle2 size={16} /></span><div><h2>服务端校验</h2><p>保存后对精确 revision 执行</p></div></header><div className="compact-empty">尚未校验。浏览器提示只辅助编辑，发布以服务端报告为准。</div></section>;
  }

  const errors = report.diagnostics.filter((item) => item.severity === "error").length;
  const warnings = report.diagnostics.filter((item) => item.severity === "warning").length;

  return (
    <section className="panel validation-panel">
      <header><span className={report.valid ? "feature-icon success" : "feature-icon danger"}>{report.valid ? <CheckCircle2 size={16} /> : <AlertTriangle size={16} />}</span><div><h2>{report.valid ? "校验通过" : "校验未通过"}</h2><p>revision {report.draft_revision} · {errors} errors · {warnings} warnings</p></div></header>
      {report.diagnostics.length === 0 ? <div className="compact-empty success-copy">没有服务端诊断，可发布此 revision。</div> : <ul className="diagnostic-list">{report.diagnostics.map((diagnostic, index) => <li className={diagnostic.severity} key={`${diagnostic.code}-${diagnostic.pointer}-${index}`}><span>{diagnostic.severity === "error" ? <AlertTriangle size={14} /> : <ShieldAlert size={14} />}</span><div><strong>{diagnostic.code}</strong><p>{diagnostic.message}</p><small>{diagnostic.node_id ? `node: ${diagnostic.node_id} · ` : ""}{diagnostic.pointer || "/"}</small></div></li>)}</ul>}
    </section>
  );
}
