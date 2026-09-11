"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";

import { ApiNotice, EmptyState, PageHeader, StatCard, StatusBadge } from "../../../../../components/ui";
import { controlApi, type RunDetail } from "../../../../../lib/control-api";

export default function RunDetailPage() {
  const { tenantId, runId } = useParams<{ tenantId: string; runId: string }>();
  const [run, setRun] = useState<RunDetail | null>(null);
  const [error, setError] = useState<unknown>();
  const usageAvailable = run?.usage_status !== "UNAVAILABLE";
  useEffect(() => { void controlApi.getRun(tenantId, runId).then(setRun).catch(setError); }, [runId, tenantId]);
  return <>
    <PageHeader eyebrow="RUN TIMELINE" title={run ? `Run ${run.run_id}` : "运行详情"} description="显示 Worker 已持久化的业务事实；Gateway 交接后仅表示已发布 ReplyIntent，不等于渠道已送达。" action={<Link className="button secondary" href={`/tenants/${encodeURIComponent(tenantId)}/runs`}>返回运行记录</Link>} />
    <ApiNotice error={error} />
    {!run ? (!error && <div className="panel-loading"><div className="loading-mark"/><span>正在读取运行详情…</span></div>) : <>
      <div className="stats-grid"><StatCard label="当前阶段" value={run.stage} detail={run.status}/><StatCard label="Attempt" value={run.attempts} detail={run.failure_reason || "无持久化失败原因"}/><StatCard label="Token" value={usageAvailable ? run.total_tokens.toLocaleString("zh-CN") : "未采集"} detail={usageAvailable ? `${run.input_tokens} 输入 / ${run.output_tokens} 输出 · ${run.usage_status}` : "当前运行路径没有持久化用量记录"}/><StatCard label="回复" value={run.reply_status} detail={run.memory_status || "无 Memory 阻塞"}/></div>
      <div className="panel"><h2>执行时间线</h2>{run.timeline.length === 0 ? <EmptyState title="暂无时间线" detail="该 Run 尚未产生 Attempt 事实。"/> : <div className="table-wrap"><table><thead><tr><th>时间</th><th>来源</th><th>事件</th><th>状态</th><th>原因</th></tr></thead><tbody>{run.timeline.map((event, index) => <tr key={`${event.category}-${event.occurred_at}-${index}`}><td>{new Date(event.occurred_at).toLocaleString("zh-CN")}</td><td>{event.source}</td><td><strong>{event.category}</strong></td><td><StatusBadge tone={event.status === "FAILED" ? "amber" : "blue"}>{event.status}</StatusBadge></td><td>{event.reason || "—"}</td></tr>)}</tbody></table></div>}</div>
      <div className="panel"><h2>事实覆盖</h2><p>{run.coverage.join(" · ")}</p><p><small>当前账本没有持久化节点级模型调用和普通工具调用明细，因此页面不会从 trace 猜测“哪个工具失败”。</small></p></div>
    </>}
  </>;
}
