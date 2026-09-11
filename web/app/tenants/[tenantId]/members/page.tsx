"use client";

import { ChevronLeft, ChevronRight, Plus, Search, UserPlus } from "lucide-react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { type FormEvent, useCallback, useEffect, useState } from "react";

import { ApiNotice, Button, Drawer, EmptyState, PageHeader, StatCard, StatusBadge } from "../../../../components/ui";
import {
  controlApi,
  ControlApiError,
  type MemberCandidate,
  type Membership,
  type Tenant,
} from "../../../../lib/control-api";

const CANDIDATE_PAGE_SIZE = 10;

export default function TenantMembersPage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  const router = useRouter();
  const [tenant, setTenant] = useState<Tenant | null>(null);
  const [members, setMembers] = useState<Membership[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>();
  const [drawer, setDrawer] = useState(false);

  const agentsHref = `/tenants/${encodeURIComponent(tenantId)}/agents`;

  const loadMembers = useCallback(async () => {
    const result = await controlApi.listMembers(tenantId);
    setMembers(result.members);
  }, [tenantId]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(undefined);
    setTenant(null);
    setMembers([]);
    void controlApi.getTenant(tenantId).then(async (currentTenant) => {
      if (cancelled) return;
      if (currentTenant.role !== "OWNER") {
        router.replace(`${agentsHref}?notice=owner-only`);
        return;
      }
      setTenant(currentTenant);
      const result = await controlApi.listMembers(tenantId);
      if (!cancelled) setMembers(result.members);
    }).catch((caught) => {
      if (cancelled) return;
      if (caught instanceof ControlApiError && caught.status === 403) {
        router.replace("/tenants");
        return;
      }
      setError(caught);
    }).finally(() => {
      if (!cancelled) setLoading(false);
    });
    return () => { cancelled = true; };
  }, [agentsHref, router, tenantId]);

  async function remove(userId: string) {
    if (!confirm("确认移除该租户成员？")) return;
    setError(undefined);
    try {
      await controlApi.removeMember(tenantId, userId);
      await loadMembers();
    } catch (caught) {
      setError(caught instanceof ControlApiError && caught.code === "OWNER_REQUIRES_TRANSFER"
        ? new Error("Owner 不能直接移除，请先完成所有权转移")
        : caught);
    }
  }

  if (error && !tenant) {
    return <><ApiNotice error={error} /><Link className="button secondary" href="/tenants">返回租户选择</Link></>;
  }

  if (loading || !tenant) {
    return <div className="panel-loading"><div className="loading-mark" /><span>正在确认 OWNER 权限…</span></div>;
  }

  return (
    <>
      <PageHeader
        eyebrow="TENANT MEMBERSHIP"
        title={`${tenant.name} · 成员管理`}
        description={`Tenant ${tenant.slug} · 仅 OWNER 可以搜索、添加和移除成员。`}
        action={<div className="header-actions"><Link className="button secondary" href={agentsHref}>返回 Agent 工作台</Link><Button onClick={() => setDrawer(true)}><Plus size={15} />添加成员</Button></div>}
      />
      <ApiNotice error={error} />
      <div className="stats-grid">
        <StatCard detail="当前 Membership 数量" label="成员" value={members.length} />
        <StatCard detail="当前登录账号在此租户中的角色" label="我的角色" value="OWNER" />
        <StatCard detail="成员写操作由服务端再次鉴权" label="管理权限" value="Owner only" />
      </div>
      <div className="panel">
        {members.length === 0 ? (
          <EmptyState detail="搜索已有平台账号并添加为 Tenant Member。" title="没有成员" />
        ) : (
          <div className="table-wrap">
            <table>
              <thead><tr><th>用户 ID</th><th>角色</th><th>创建者</th><th>加入时间</th><th>操作</th></tr></thead>
              <tbody>{members.map((member) => <tr key={member.id}><td><strong>{member.user_id}</strong></td><td><StatusBadge tone={member.role === "OWNER" ? "blue" : "gray"}>{member.role}</StatusBadge></td><td>{member.created_by}</td><td>{new Date(member.created_at).toLocaleString("zh-CN")}</td><td><Button disabled={member.role === "OWNER"} onClick={() => void remove(member.user_id)} variant="danger">移除</Button></td></tr>)}</tbody>
            </table>
          </div>
        )}
      </div>
      {drawer && <AddMemberDrawer onClose={() => setDrawer(false)} onDone={() => { setDrawer(false); void loadMembers(); }} tenantId={tenantId} />}
    </>
  );
}

function AddMemberDrawer({
  tenantId,
  onClose,
  onDone,
}: {
  tenantId: string;
  onClose: () => void;
  onDone: () => void;
}) {
  const [query, setQuery] = useState("");
  const [candidates, setCandidates] = useState<MemberCandidate[]>([]);
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [searched, setSearched] = useState(false);
  const [searching, setSearching] = useState(false);
  const [addingUserId, setAddingUserId] = useState("");
  const [error, setError] = useState<unknown>();

  async function searchCandidates(nextOffset: number) {
    const normalized = query.trim();
    if (!normalized) return;
    setSearching(true);
    setError(undefined);
    try {
      const result = await controlApi.searchMemberCandidates(tenantId, {
        query: normalized,
        offset: nextOffset,
        limit: CANDIDATE_PAGE_SIZE,
      });
      setCandidates(result.candidates);
      setOffset(result.offset);
      setTotal(result.total);
      setSearched(true);
    } catch (caught) {
      setError(caught);
    } finally {
      setSearching(false);
    }
  }

  async function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await searchCandidates(0);
  }

  async function add(candidate: MemberCandidate) {
    setAddingUserId(candidate.user_id);
    setError(undefined);
    try {
      await controlApi.addMember(tenantId, candidate.user_id);
      onDone();
    } catch (caught) {
      setError(caught);
    } finally {
      setAddingUserId("");
    }
  }

  return (
    <Drawer description="按用户名或显示名称搜索尚未加入该租户的平台账号。" onClose={onClose} title="添加租户成员">
      <div className="form-stack">
        <ApiNotice error={error} />
        <form className="candidate-search" onSubmit={(event) => void submitSearch(event)}>
          <label className="field" htmlFor="member-candidate-query">
            <span>搜索账号</span>
          </label>
          <div className="search-row">
            <input
              autoFocus
              id="member-candidate-query"
              onChange={(event) => setQuery(event.target.value)}
              placeholder="输入用户名或显示名称"
              value={query}
            />
            <Button disabled={searching || query.trim().length === 0} type="submit">
              <Search size={14} />
              {searching ? "搜索中…" : "搜索"}
            </Button>
          </div>
          <small>至少输入 1 个非空字符。</small>
        </form>
        {!searched ? (
          <div className="candidate-placeholder"><UserPlus size={22} /><span>搜索后选择要加入的账号。</span></div>
        ) : candidates.length === 0 ? (
          <EmptyState detail="请调整用户名或显示名称后重试。" title="没有可添加的账号" />
        ) : (
          <div className="candidate-list">
            {candidates.map((candidate) => <div className="candidate-item" key={candidate.user_id}><span className="mini-avatar">{(candidate.display_name || candidate.username).slice(0, 2).toUpperCase()}</span><span className="candidate-copy"><strong>{candidate.display_name || candidate.username}</strong><small>{candidate.username} · {candidate.user_id}</small></span><Button disabled={Boolean(addingUserId)} onClick={() => void add(candidate)} variant="secondary">{addingUserId === candidate.user_id ? "添加中…" : "添加"}</Button></div>)}
          </div>
        )}
        {searched && total > 0 && <div className="candidate-pagination"><span>第 {offset + 1}–{Math.min(offset + CANDIDATE_PAGE_SIZE, total)} 条，共 {total} 条</span><div><Button aria-label="候选上一页" disabled={searching || offset === 0} onClick={() => void searchCandidates(Math.max(0, offset - CANDIDATE_PAGE_SIZE))} variant="ghost"><ChevronLeft size={14} /></Button><Button aria-label="候选下一页" disabled={searching || offset + CANDIDATE_PAGE_SIZE >= total} onClick={() => void searchCandidates(offset + CANDIDATE_PAGE_SIZE)} variant="ghost"><ChevronRight size={14} /></Button></div></div>}
        <div className="form-actions"><Button onClick={onClose} type="button" variant="secondary">关闭</Button></div>
      </div>
    </Drawer>
  );
}
