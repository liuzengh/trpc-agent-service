import { useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useInfiniteQuery } from '@tanstack/react-query'

import { createLocalUser, getUsers, resetLocalUserPassword, updateUser, type LocalUserCredentialResult, type MemberSummary } from '../api'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { CopyButton } from '../components/CopyButton'
import { DialogHeader } from '../components/DialogHeader'
import { AccountIcon } from '../components/Icons'
import { PlusIcon } from '../components/PageIcons'
import { LoadingState } from '../components/LoadingState'
import { formatMemberLastLogin, MemberIdentity, memberProviderText } from '../components/MemberIdentity'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SearchField } from '../components/SearchField'
import { TogglePill } from '../components/TogglePill'
import { useAppContext } from '../context'

export function UsersPage() {
  const { user } = useAppContext()
  const [creating, setCreating] = useState(false)
  const [username, setUsername] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [email, setEmail] = useState('')
  const [credential, setCredential] = useState<LocalUserCredentialResult | null>(null)
  const [busy, setBusy] = useState('')
  const [error, setError] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [searchQuery, setSearchQuery] = useState('')

  const usersQuery = useInfiniteQuery({
    queryKey: ['console', 'users', searchQuery],
    queryFn: ({ signal, pageParam }) => getUsers({ cursor: pageParam, query: searchQuery, limit: 50 }, signal),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.next_cursor || undefined,
  })
  const users = usersQuery.data?.pages.flatMap((page) => page.users) ?? []
  const activeUsers = users.filter((target) => target.status === 'active').length
  const systemAdmins = users.filter((target) => target.is_system_admin).length
  const localUsers = users.filter((target) => (target.providers ?? []).some((provider) => provider === '本地账号' || provider.toLowerCase() === 'local')).length

  const reloadUsers = async () => {
    setError('')
    const result = await usersQuery.refetch()
    if (result.error) setError(result.error instanceof Error ? result.error.message : '读取用户失败')
  }

  const saveUser = async (target: MemberSummary, status: string, isSystemAdmin: boolean) => {
    setBusy(`user:${target.platform_user_id}`)
    setError('')
    try {
      await updateUser(target.platform_user_id, status, isSystemAdmin)
      await usersQuery.refetch()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const createUser = async () => {
    if (!username.trim()) return
    setBusy('create-local')
    setError('')
    setCredential(null)
    try {
      const result = await createLocalUser(username.trim(), displayName.trim(), email.trim())
      setCredential(result)
      setUsername('')
      setDisplayName('')
      setEmail('')
      setCreating(false)
      await usersQuery.refetch()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const resetPassword = async (target: MemberSummary) => {
    setBusy(`reset:${target.platform_user_id}`)
    setError('')
    setCredential(null)
    try {
      setCredential(await resetLocalUserPassword(target.platform_user_id))
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  if (usersQuery.isLoading) return <div className="page-stack members-page"><LoadingState label="正在读取平台用户…" /></div>

  return (
    <div className="page-stack members-page">
      {(error || usersQuery.error) && <FeedbackBanner tone="error">{error || (usersQuery.error as Error).message}</FeedbackBanner>}
      {credential && (
        <section className="user-credential-reveal" aria-live="polite">
          <div className="user-credential-copy">
            <span>一次性凭据</span>
            <strong>{credential.username ? `${credential.username} 的临时密码` : '临时密码已生成'}</strong>
            <p>仅显示这一次。用户首次登录后必须立即修改。</p>
          </div>
          <div className="user-credential-secret">
            <code>{credential.temporary_password}</code>
            <CopyButton value={credential.temporary_password} label="复制密码" copiedLabel="已复制" className="secondary" iconSize={14} />
            <button type="button" className="text-button" onClick={() => setCredential(null)}>关闭</button>
          </div>
        </section>
      )}

      <dl className="user-summary-strip" aria-label="平台用户概览">
        <div><dt>已加载用户</dt><dd>{users.length}</dd></div>
        <div><dt>正常账号</dt><dd>{activeUsers}</dd></div>
        <div><dt>系统管理员</dt><dd>{systemAdmins}</dd></div>
        <div><dt>本地账号</dt><dd>{localUsers}</dd></div>
      </dl>

      <section className="members-panel users-panel">
        <PanelHeader
          icon={<AccountIcon size={17} />}
          title="平台用户"
          description="管理平台登录主体、系统级权限与本地登录凭据。租户权限请在对应租户的成员页管理。"
          actions={<div className="members-header-actions">
            <RefreshButton onClick={() => void reloadUsers()} loading={usersQuery.isFetching} label="刷新用户" />
            <button type="button" className="secondary" disabled={busy !== ''} onClick={() => setCreating(true)}><PlusIcon size={14} /> 新建本地用户</button>
          </div>}
        />

        <div className="users-toolbar">
          <SearchField
            className="users-search"
            value={searchInput}
            onValueChange={setSearchInput}
            placeholder="搜索姓名、邮箱或用户 ID"
            ariaLabel="搜索平台用户"
            onSubmit={() => setSearchQuery(searchInput.trim())}
            onClear={() => { setSearchInput(''); setSearchQuery('') }}
          />
          <span className="users-result-count">{usersQuery.hasNextPage ? `已加载 ${users.length} 个，仍有更多` : `共 ${users.length} 个结果`}</span>
        </div>

        <div className="members-table-wrap">
          <table className="members-table">
            <thead><tr><th>用户</th><th>登录方式</th><th>系统权限</th><th>状态</th><th>本地凭据</th><th>最近登录</th></tr></thead>
            <tbody>
              {users.length === 0 ? <tr><td colSpan={6} className="table-empty">暂无用户</td></tr> : users.map((target) => {
                const self = target.platform_user_id === user?.platform_user_id
                const hasLocal = (target.providers ?? []).some((provider) => provider === '本地账号' || provider.toLowerCase() === 'local')
                return (
                  <tr key={target.platform_user_id}>
                    <td><MemberIdentity member={target} /></td>
                    <td>{memberProviderText(target)}</td>
                    <td><TogglePill active={target.is_system_admin} activeLabel="系统管理员" inactiveLabel="普通用户" disabled={busy !== '' || self} onChange={(isSystemAdmin) => void saveUser(target, target.status, isSystemAdmin)} /></td>
                    <td><TogglePill active={target.status === 'active'} activeLabel="正常" inactiveLabel="已停用" inactiveClassName="suspended" disabled={busy !== '' || self} onChange={(active) => void saveUser(target, active ? 'active' : 'suspended', target.is_system_admin)} /></td>
                    <td>{hasLocal ? <button type="button" className="text-button" disabled={busy !== '' || self} onClick={() => void resetPassword(target)}>{busy === `reset:${target.platform_user_id}` ? '正在重置…' : '重置密码'}</button> : <span className="users-muted">非本地账号</span>}</td>
                    <td>{formatMemberLastLogin(target.last_login_at)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        {usersQuery.hasNextPage && (
          <div className="members-pagination"><button type="button" className="secondary" disabled={usersQuery.isFetchingNextPage} onClick={() => void usersQuery.fetchNextPage()}>{usersQuery.isFetchingNextPage ? '正在加载…' : '加载更多用户'}</button></div>
        )}
      </section>

      <Dialog.Root open={creating} onOpenChange={(open) => { if (!open && busy !== 'create-local') setCreating(false) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="modal bot-dialog form-dialog">
            <DialogHeader
              className="bot-dialog-head"
              closeClassName="bot-dialog-close"
              title="新建本地用户"
              description="创建后只显示一次临时密码，之后无法再次读取明文密码。"
              closeLabel="关闭新建用户"
            />
            <div className="bot-dialog-body form-dialog-body">
              {error && <FeedbackBanner tone="error">{error}</FeedbackBanner>}
              <div className="form-dialog-grid">
                <label>用户名<input autoComplete="off" placeholder="例如 ming" value={username} onChange={(event) => setUsername(event.target.value)} /></label>
                <label>显示名称<input placeholder="用户姓名（可选）" value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label>
                <label className="form-dialog-full">邮箱<input type="email" placeholder="name@example.com（可选）" value={email} onChange={(event) => setEmail(event.target.value)} /></label>
              </div>
            </div>
            <div className="bot-dialog-footer">
              <Dialog.Close asChild><button type="button" className="secondary" disabled={busy === 'create-local'}>取消</button></Dialog.Close>
              <button type="button" className="primary" disabled={busy !== '' || !username.trim()} onClick={() => void createUser()}>{busy === 'create-local' ? '正在创建…' : '创建用户'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}
