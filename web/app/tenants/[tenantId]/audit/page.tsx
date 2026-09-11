"use client";

import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";

import { ApiNotice, Button, EmptyState, PageHeader, StatusBadge } from "../../../../components/ui";
import { controlApi, type AuditEvent } from "../../../../lib/control-api";

const PAGE_SIZE = 25;
const V1_MERGE_WINDOW = 100;

export default function AuditPage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<unknown>();
  const [loading, setLoading] = useState(true);
  const load = useCallback(async (requestedOffset: number) => {
    setLoading(true);
    setError(undefined);
    try {
      const page = await controlApi.listAuditEvents(tenantId, { offset: requestedOffset, limit: PAGE_SIZE });
      setEvents(page.events);
      setTotal(page.total);
      setOffset(requestedOffset);
    } catch (cause) {
      setError(cause);
    } finally {
      setLoading(false);
    }
  }, [tenantId]);
  useEffect(() => { void load(0); }, [load]);

  return <>
    <PageHeader
      eyebrow="BUSINESS AUDIT"
      title="审计记录"
      description="统一检索 Control 配置创建/发布事实与 Worker 执行结果；V1 展示最近 100 条，不以结构化日志或 trace 代替业务审计。"
    />
    <ApiNotice error={error}/>
    <div className="panel">
      {loading
        ? <div className="panel-loading"><div className="loading-mark"/><span>正在聚合审计事实…</span></div>
        : error && events.length === 0
          ? null
          : events.length === 0
          ? <EmptyState title="暂无审计记录" detail="配置创建、版本发布或运行执行后会显示在这里。"/>
          : <div className="table-wrap"><table><thead><tr><th>时间</th><th>来源</th><th>类别</th><th>动作</th><th>对象</th><th>执行者</th><th>结果</th></tr></thead><tbody>{events.map((event) => <tr key={event.event_id}><td>{new Date(event.occurred_at).toLocaleString("zh-CN")}</td><td>{event.source}</td><td>{event.category}</td><td><strong>{event.action}</strong>{event.reason ? <><br/><small>{event.reason}</small></> : null}</td><td>{event.resource_type}<br/><small>{event.resource_id}</small></td><td>{event.actor_id || "system"}</td><td><StatusBadge tone={event.outcome === "SUCCEEDED" ? "green" : "amber"}>{event.outcome}</StatusBadge></td></tr>)}</tbody></table></div>}
      {!loading && total > 0 && <div className="pagination">
        <span>第 {offset + 1}–{Math.min(offset + PAGE_SIZE, total, V1_MERGE_WINDOW)} 条，共 {total} 条；V1 可浏览最近 {Math.min(total, V1_MERGE_WINDOW)} 条</span>
        <div className="pagination-actions">
          <Button disabled={loading || offset === 0} onClick={() => void load(Math.max(0, offset - PAGE_SIZE))} variant="secondary">上一页</Button>
          <Button disabled={loading || offset + PAGE_SIZE >= total || offset + PAGE_SIZE >= V1_MERGE_WINDOW} onClick={() => void load(offset + PAGE_SIZE)} variant="secondary">下一页</Button>
        </div>
      </div>}
    </div>
  </>;
}
