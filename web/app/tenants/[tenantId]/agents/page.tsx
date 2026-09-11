"use client";

import { Bot, ChevronLeft, ChevronRight, Plus } from "lucide-react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";

import { ApiNotice, Button, EmptyState, PageHeader, StatusBadge } from "../../../../components/ui";
import { controlApi, type Agent, type Tenant } from "../../../../lib/control-api";

const PAGE_SIZE = 20;

export default function TenantAgentsPage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  const [tenant, setTenant] = useState<Tenant | null>(null);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>();

  const load = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      const [currentTenant, page] = await Promise.all([
        controlApi.getTenant(tenantId),
        controlApi.listAgents(tenantId, { offset, limit: PAGE_SIZE }),
      ]);
      setTenant(currentTenant);
      setAgents(page.agents);
      setTotal(page.total);
    } catch (caught) {
      setError(caught);
    } finally {
      setLoading(false);
    }
  }, [offset, tenantId]);

  useEffect(() => {
    void load();
  }, [load]);

  const tenantSegment = encodeURIComponent(tenantId);

  return (
    <>
      <PageHeader
        eyebrow="AGENT AUTHORING"
        title={tenant ? `${tenant.name} · Agents` : "Agents"}
        description="创建 Agent，在服务端 Draft revision 上编辑 AgentSpec，并发布不可变版本。"
        action={
          <Link className="button primary" href={`/tenants/${tenantSegment}/agents/new`}>
            <Plus size={15} />创建 Agent
          </Link>
        }
      />
      <ApiNotice error={error} />
      <div className="panel">
        {loading ? (
          <div className="panel-loading"><div className="loading-mark" /><span>正在读取 Agent…</span></div>
        ) : agents.length === 0 ? (
          <EmptyState detail="创建第一个 Agent 后，可在画布中生成并保存 AgentSpec V1。" title="当前租户还没有 Agent" />
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr><th>Agent</th><th>最新版本</th><th>创建者</th><th>更新时间</th><th>操作</th></tr>
              </thead>
              <tbody>
                {agents.map((agent) => (
                  <tr key={agent.id}>
                    <td>
                      <div className="cell-primary">
                        <span className="mini-avatar"><Bot size={15} /></span>
                        <span className="table-copy"><strong>{agent.name}</strong><small>{agent.description || agent.id}</small></span>
                      </div>
                    </td>
                    <td>{agent.latest_version_number === null ? <StatusBadge tone="gray">未发布</StatusBadge> : <StatusBadge tone="green">v{agent.latest_version_number}</StatusBadge>}</td>
                    <td>{agent.created_by}</td>
                    <td>{new Date(agent.updated_at).toLocaleString("zh-CN")}</td>
                    <td><Link className="text-link" href={`/tenants/${tenantSegment}/agents/${encodeURIComponent(agent.id)}`}>打开画布 →</Link></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!loading && total > 0 && (
          <div className="pagination">
            <span>第 {offset + 1}–{Math.min(offset + PAGE_SIZE, total)} 条，共 {total} 条</span>
            <div className="pagination-actions">
              <Button aria-label="上一页" disabled={offset === 0} onClick={() => setOffset((value) => Math.max(0, value - PAGE_SIZE))} variant="secondary"><ChevronLeft size={14} />上一页</Button>
              <Button aria-label="下一页" disabled={offset + PAGE_SIZE >= total} onClick={() => setOffset((value) => value + PAGE_SIZE)} variant="secondary">下一页<ChevronRight size={14} /></Button>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
