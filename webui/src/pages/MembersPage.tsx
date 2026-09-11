import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useInfiniteQuery } from '@tanstack/react-query'

import { getTenantMemberCandidates, getTenantMembers, updateTenantMember, type MemberSummary } from '../api'
import { canAdminTenant } from '../access'
import { useAppContext } from '../context'
import { DialogHeader } from '../components/DialogHeader'
import { EmptyState } from '../components/EmptyState'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { AccountIcon } from '../components/Icons'
import { PlusIcon } from '../components/PageIcons'
import { LoadingState } from '../components/LoadingState'
import { formatMemberLastLogin, MemberIdentity, memberDisplayName, memberProviderText } from '../components/MemberIdentity'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SearchField } from '../components/SearchField'
import { SelectControl } from '../components/SelectControl'
import { TogglePill } from '../components/TogglePill'

const TENANT_ROLES = [
  { value: 'admin', label: '租户管理员' },
  { value: 'member', label: '租户成员' },
]

export function MembersPage() {
  const { tenant, tenantSummaries, tenantsLoading, user } = useAppContext()
  const [adding, setAdding] = useState(false)
  const [candidateID, setCandidateID] = useState('')
  const [candidateRole, setCandidateRole] = useState('member')
  const [busy, setBusy] = useState('')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [candidateSearchInput, setCandidateSearchInput] = useState('')
  const [candidateSearch, setCandidateSearch] = useState('')

  const currentTenant = tenantSummaries.find((entry) => entry.tenant_id === tenant)
  const canManage = Boolean(tenant) && canAdminTenant(user, tenant)
  const membersQuery = useInfiniteQuery({
    queryKey: ['console', 'tenant-members', tenant],
    queryFn: ({ signal, pageParam }) => getTenantMembers(tenant, { cursor: pageParam, limit: 50 }, signal),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.next_cursor || undefined,
    enabled: Boolean(tenant && canManage),
  })
  const members = membersQuery.data?.pages.flatMap((page) => page.members) ?? []
  const candidatesQuery = useInfiniteQuery({
    queryKey: ['console', 'tenant-member-candidates', tenant, candidateSearch],
    queryFn: ({ signal, pageParam }) => getTenantMemberCandidates(tenant, { cursor: pageParam, query: candidateSearch, limit: 50 }, signal),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.next_cursor || undefined,
    enabled: Boolean(tenant && canManage && adding),
  })
  const candidates = candidatesQuery.data?.pages.flatMap((page) => page.candidates) ?? []

  const reload = async () => {
    if (!tenant || !canManage) return
    setError('')
    const result = await membersQuery.refetch()
    if (result.error) setError(result.error instanceof Error ? result.error.message : '读取成员失败')
  }

  useEffect(() => {
    setCandidateID((current) => candidates.some((item) => item.platform_user_id === current)
      ? current
      : candidates[0]?.platform_user_id ?? '')
  }, [candidates])

  const updateMember = async (member: MemberSummary, role: string, status: string, contentAudit = member.conversation_content_audit) => {
    if (!tenant) return
    setBusy(member.platform_user_id)
    setError('')
    setNotice('')
    try {
      await updateTenantMember(tenant, member.platform_user_id, role, status, role === 'admin' && status === 'active' ? contentAudit : false)
      setNotice('成员权限已更新')
      await membersQuery.refetch()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const addMember = async () => {
    const candidate = candidates.find((item) => item.platform_user_id === candidateID)
    if (!candidate) return
    await updateMember(candidate, candidateRole, 'active')
    await candidatesQuery.refetch()
    setAdding(false)
  }

  if (tenantsLoading) {
    return <div className="page-stack members-page"><LoadingState label="正在读取租户…" /></div>
  }
  if (!tenant) {
    return <EmptyState title="尚未选择租户" description="请选择一个租户后管理成员。" />
  }
  if (!canManage) {
    return <EmptyState title="无成员管理权限" description="只有当前租户的租户管理员可以管理成员。" />
  }
  if (membersQuery.isLoading) {
    return <div className="page-stack members-page"><LoadingState label="正在读取成员…" /></div>
  }

  return (
    <div className="page-stack members-page">
      {(error || membersQuery.error || candidatesQuery.error) && (
        <FeedbackBanner tone="error">{error || ((membersQuery.error ?? candidatesQuery.error) as Error).message}</FeedbackBanner>
      )}
      {notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}

      <section className="members-panel">
        <PanelHeader
          icon={<AccountIcon size={17} />}
          title="租户成员"
          description={`${currentTenant?.display_name || tenant} · ${membersQuery.hasNextPage ? `已加载 ${members.length}` : members.length} 名已授权成员`}
          actions={<div className="members-header-actions">
            <RefreshButton onClick={() => void reload()} loading={membersQuery.isFetching} label="刷新成员" />
            <button type="button" className="secondary" disabled={busy !== ''} onClick={() => setAdding(true)}>
              <PlusIcon size={14} /> 添加成员
            </button>
          </div>}
        />

        <div className="table-scroll members-table-wrap">
          <table className="ui-table members-table">
            <thead><tr><th>成员</th><th>登录方式</th><th>租户角色</th><th>状态</th><th>会话正文审计</th><th>最近登录</th></tr></thead>
            <tbody>
              {members.length === 0 ? (
                <tr><td colSpan={6} className="table-empty">当前租户还没有成员</td></tr>
              ) : members.map((member) => {
                const selfLocked = member.platform_user_id === user?.platform_user_id && !user?.is_system_admin
                return (
                  <tr key={member.platform_user_id}>
                    <td><MemberIdentity member={member} /></td>
                    <td>{memberProviderText(member)}</td>
                    <td><SelectControl ariaLabel={`${memberDisplayName(member)}租户角色`} value={member.role} disabled={busy !== '' || selfLocked} onValueChange={(role) => void updateMember(member, role, member.status)} options={TENANT_ROLES} shellClassName="members-inline-select" valueLabel={TENANT_ROLES.find((item) => item.value === member.role)?.label} /></td>
                    <td><TogglePill active={member.status === 'active'} activeLabel="正常" inactiveLabel="已停用" inactiveClassName="suspended" disabled={busy !== '' || selfLocked} onChange={(active) => void updateMember(member, member.role, active ? 'active' : 'suspended')} /></td>
                    <td>
                      {member.role === 'admin' && member.status === 'active' ? (
                        <TogglePill
                          active={member.conversation_content_audit}
                          activeLabel="已授权"
                          inactiveLabel="未授权"
                          disabled={busy !== ''}
                          onChange={(enabled) => void updateMember(member, member.role, member.status, enabled)}
                        />
                      ) : '—'}
                    </td>
                    <td>{formatMemberLastLogin(member.last_login_at)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        {membersQuery.hasNextPage && (
          <div className="members-pagination">
            <button type="button" className="secondary" disabled={membersQuery.isFetchingNextPage} onClick={() => void membersQuery.fetchNextPage()}>
              {membersQuery.isFetchingNextPage ? '正在加载…' : '加载更多成员'}
            </button>
          </div>
        )}
      </section>

      <Dialog.Root open={adding} onOpenChange={(open) => { if (!open && busy === '') setAdding(false) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="modal bot-dialog form-dialog">
            <DialogHeader
              className="bot-dialog-head"
              closeClassName="bot-dialog-close"
              title="添加租户成员"
              description={`从平台用户中选择成员并加入 ${currentTenant?.display_name || tenant}。`}
              closeLabel="关闭添加成员"
            />
            <div className="bot-dialog-body form-dialog-body">
              {(error || candidatesQuery.error) && <FeedbackBanner tone="error">{error || (candidatesQuery.error as Error).message}</FeedbackBanner>}
              <SearchField
                value={candidateSearchInput}
                onValueChange={setCandidateSearchInput}
                placeholder="搜索姓名、邮箱或用户 ID"
                ariaLabel="搜索可添加用户"
                onSubmit={() => setCandidateSearch(candidateSearchInput.trim())}
                onClear={() => { setCandidateSearchInput(''); setCandidateSearch('') }}
                disabled={candidatesQuery.isFetching}
              />
              <div className="form-dialog-grid">
                <label>
                  平台用户
                  <SelectControl
                    ariaLabel="选择用户"
                    value={candidateID}
                    onValueChange={setCandidateID}
                    disabled={candidates.length === 0}
                    options={candidates.map((member) => ({ value: member.platform_user_id, label: memberDisplayName(member) }))}
                    valueLabel={candidatesQuery.isLoading ? '正在读取…' : candidates.length > 0 ? memberDisplayName(candidates.find((item) => item.platform_user_id === candidateID) ?? candidates[0]) : '没有匹配用户'}
                  />
                </label>
                <label>
                  租户角色
                  <SelectControl
                    ariaLabel="加入后的角色"
                    value={candidateRole}
                    onValueChange={setCandidateRole}
                    options={TENANT_ROLES}
                    valueLabel={TENANT_ROLES.find((item) => item.value === candidateRole)?.label}
                  />
                </label>
              </div>
              {candidatesQuery.hasNextPage && (
                <button type="button" className="text-button form-dialog-more" disabled={candidatesQuery.isFetchingNextPage} onClick={() => void candidatesQuery.fetchNextPage()}>
                  {candidatesQuery.isFetchingNextPage ? '正在加载更多用户…' : '加载更多用户'}
                </button>
              )}
            </div>
            <div className="bot-dialog-footer">
              <Dialog.Close asChild><button type="button" className="secondary" disabled={busy !== ''}>取消</button></Dialog.Close>
              <button type="button" className="primary" disabled={busy !== '' || !candidateID} onClick={() => void addMember()}>{busy ? '正在添加…' : '添加成员'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}
