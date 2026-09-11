"use client";

import { RefreshCw, ShieldAlert } from "lucide-react";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";

import { ChannelDialog } from "../../../../components/channels/confirmation-dialog";
import { ApiNotice, Button, EmptyState, PageHeader, StatusBadge } from "../../../../components/ui";
import { controlApi, type ToolApproval } from "../../../../lib/control-api";

const PAGE_SIZE = 50;
type PendingDecision = { operation: ToolApproval; action: "approve" | "reject" };

function tone(status: string): "green" | "blue" | "amber" | "gray" {
  if (status === "SUCCEEDED") return "green";
  if (status === "PENDING" || status === "APPROVED" || status === "EXECUTING") return "blue";
  if (status === "REJECTED" || status === "EXPIRED" || status === "UNKNOWN") return "amber";
  return "gray";
}

export default function ToolApprovalsPage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  const [operations, setOperations] = useState<ToolApproval[]>([]);
  const [total, setTotal] = useState(0);
  const [isOwner, setIsOwner] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>();
  const [decision, setDecision] = useState<PendingDecision | null>(null);
  const [reason, setReason] = useState("");

  const load = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const [page, tenant] = await Promise.all([
        controlApi.listToolApprovals(tenantId, { offset: 0, limit: PAGE_SIZE }),
        controlApi.getTenant(tenantId),
      ]);
      setOperations(page.operations); setTotal(page.total); setIsOwner(tenant.role === "OWNER"); setError(undefined);
    } catch (caught) { setError(caught); }
    finally { if (!silent) setLoading(false); }
  }, [tenantId]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(true), 2000);
    return () => window.clearInterval(timer);
  }, [load]);

  async function submit() {
    if (!decision) return;
    setBusy(true); setError(undefined);
    try {
      await controlApi.decideToolApproval(tenantId, decision.operation.operation_id, {
        action: decision.action,
        reason: reason.trim() || undefined,
        expected_arguments_digest: decision.operation.arguments_digest,
      });
      setDecision(null); setReason(""); await load(true);
    } catch (caught) { setError(caught); }
    finally { setBusy(false); }
  }

  return <>
    <PageHeader eyebrow="HUMAN IN THE LOOP" title="工具审批" description="查看并确认具体的副作用操作。批准严格绑定租户、工具、目标与参数摘要；参数变化需要重新审批。" action={<Button variant="secondary" onClick={() => void load()} disabled={loading}><RefreshCw size={14}/>刷新</Button>} />
    <ApiNotice error={error} />
    <div className="stats-grid">
      <div className="stat-card"><span>待确认</span><strong>{operations.filter((item) => item.status === "PENDING").length}</strong><small>当前页面中等待 OWNER 决定的操作</small></div>
      <div className="stat-card"><span>执行中或已批准</span><strong>{operations.filter((item) => item.status === "APPROVED" || item.status === "EXECUTING").length}</strong><small>批准不等于执行成功</small></div>
      <div className="stat-card"><span>操作记录</span><strong>{total}</strong><small>重复提交同一决定不会重复执行工具</small></div>
    </div>
    <div className="panel">
      {loading ? <div className="panel-loading"><div className="loading-mark"/><span>正在读取待确认操作…</span></div> : operations.length === 0 ? <EmptyState title="暂无工具审批" detail="Agent 提出测试工单状态修改后，确定的操作会显示在这里。"/> : <div className="table-wrap"><table><thead><tr><th>操作</th><th>目标与参数</th><th>来源</th><th>状态</th><th>时间</th><th>决定</th></tr></thead><tbody>{operations.map((operation) => <tr key={operation.operation_id}>
        <td><strong>{operation.tool_resource}</strong><br/><small>{operation.capability}</small></td>
        <td><strong>{operation.target}</strong><br/><small>{operation.parameter_summary}</small><br/><small title={operation.arguments_digest}>{operation.arguments_digest.slice(0, 22)}…</small></td>
        <td><strong>{operation.node_id}</strong><br/><small>Run {operation.run_id}</small></td>
        <td><StatusBadge tone={tone(operation.status)}>{operation.status}</StatusBadge>{operation.result_summary ? <><br/><small>{operation.result_summary}</small></> : null}</td>
        <td>{new Date(operation.requested_at).toLocaleString("zh-CN")}<br/><small>截止 {new Date(operation.expires_at).toLocaleTimeString("zh-CN")}</small></td>
        <td>{operation.status === "PENDING" && isOwner ? <div className="toolbar-group"><Button onClick={() => { setDecision({ operation, action: "approve" }); setReason(""); }}>批准</Button><Button variant="danger" onClick={() => { setDecision({ operation, action: "reject" }); setReason(""); }}>拒绝</Button></div> : operation.status === "PENDING" ? <small>仅 OWNER 可决定</small> : <small>{operation.decided_by || "—"}</small>}</td>
      </tr>)}</tbody></table></div>}
    </div>
    {decision && <ChannelDialog title={decision.action === "approve" ? "确认批准此操作" : "确认拒绝此操作"} busy={busy} confirmLabel={decision.action === "approve" ? "批准并允许执行" : "确认拒绝"} onConfirm={() => void submit()} onClose={() => { if (!busy) { setDecision(null); setReason(""); } }}>
      <p><ShieldAlert size={16}/> {decision.operation.parameter_summary}</p>
      <dl className="version-metadata"><div><dt>工具</dt><dd>{decision.operation.tool_name}</dd></div><div><dt>目标</dt><dd>{decision.operation.target}</dd></div><div className="digest-row"><dt>参数摘要</dt><dd>{decision.operation.arguments_digest}</dd></div></dl>
      <label className="field"><span>决定说明（可选）</span><textarea maxLength={500} value={reason} onChange={(event) => setReason(event.target.value)} placeholder="记录批准或拒绝原因"/></label>
    </ChannelDialog>}
  </>;
}
