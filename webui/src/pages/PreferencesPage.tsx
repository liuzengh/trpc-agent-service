import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { deleteMemory, getMemories } from '../api'
import { useAppContext } from '../context'
import type { MemoryEntry } from '../types'
import { EmptyState } from '../components/EmptyState'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SearchField } from '../components/SearchField'
import { MemoryIcon } from '../components/PageIcons'
import './PlatformDataPage.css'

export function PreferencesPage() {
  const { apps, appsLoading, appsError, tenant, activeAppKey, user } = useAppContext()
  const selectedApp = useMemo(
    () => apps.find((entry) => `${entry.Config.tenant_id}/${entry.Config.app_code}` === activeAppKey && entry.Config.tenant_id === tenant),
    [activeAppKey, apps, tenant],
  )
  const appCode = selectedApp?.Config.app_code ?? ''
  const [queryInput, setQueryInput] = useState('')
  const [query, setQuery] = useState('')
  const [memoryToDelete, setMemoryToDelete] = useState<MemoryEntry | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [error, setError] = useState('')

  const memoriesQuery = useQuery({
    queryKey: ['console', 'my-preferences', tenant, appCode, user?.platform_user_id ?? '', query],
    queryFn: ({ signal }) => getMemories({ tenant, app: appCode, query, kind: 'fact', signal }),
    enabled: Boolean(tenant && appCode && user?.platform_user_id),
  })
  const memories: MemoryEntry[] = memoriesQuery.data ?? []

  const removeMemory = async () => {
    if (!memoryToDelete) return
    setDeleting(true)
    setError('')
    try {
      await deleteMemory(tenant, appCode, memoryToDelete.id)
      setMemoryToDelete(null)
      await memoriesQuery.refetch()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setDeleting(false)
    }
  }

  if (appsLoading) return <div className="page-stack"><LoadingState label="正在读取机器人…" /></div>
  if (!appCode) {
    return <EmptyState title="暂无可用机器人" description="当前租户没有正在运行的机器人，因此没有可查看的个人偏好范围。" />
  }

  return (
    <div className="page-stack platform-data-page preferences-page">
      {appsError && <FeedbackBanner tone="error">{appsError}</FeedbackBanner>}
      {(error || memoriesQuery.error instanceof Error) && <FeedbackBanner tone="error">{error || (memoriesQuery.error as Error).message}</FeedbackBanner>}

      <section className="data-panel-card">
        <PanelHeader
          title="我的偏好"
          description="只显示当前登录账号在这个机器人下形成的长期偏好。租户管理员和系统管理员也不能从这里查看其他用户。"
          actions={<RefreshButton onClick={() => void memoriesQuery.refetch()} loading={memoriesQuery.isFetching} label="刷新我的偏好" />}
        />

        <SearchField
          className="preferences-search"
          value={queryInput}
          onValueChange={setQueryInput}
          placeholder="搜索回答风格、语言或长期要求…"
          ariaLabel="搜索我的偏好"
          onSubmit={() => setQuery(queryInput.trim())}
          onClear={() => { setQueryInput(''); setQuery('') }}
          disabled={memoriesQuery.isFetching}
        />

        {memoriesQuery.isLoading ? (
          <LoadingState label="正在读取我的偏好…" />
        ) : memories.length === 0 ? (
          <EmptyState
            icon={<MemoryIcon size={48} />}
            title={query ? '没有匹配的偏好' : '还没有长期偏好'}
            description={query ? '换一个关键词试试。' : '后续对话中形成的稳定偏好会显示在这里，并且只属于你的账号。'}
          />
        ) : (
          <div className="memory-entry-list">
            {memories.map((entry) => (
              <article className="memory-entry" key={entry.id}>
                <div className="memory-entry-head">
                  <strong>长期偏好</strong>
                  <div className="memory-entry-meta">
                    <span>{entry.memory.topics?.join(' · ') || '未分类'}</span>
                    <button type="button" className="text-button danger" onClick={() => setMemoryToDelete(entry)}>删除</button>
                  </div>
                </div>
                <p>{entry.memory.memory}</p>
                <small>{memoryEntryCaption(entry)}</small>
              </article>
            ))}
          </div>
        )}
      </section>

      <ConfirmDialog
        open={Boolean(memoryToDelete)}
        title="删除这条偏好？"
        description="删除后，机器人后续不会再从长期偏好中读取这条内容。"
        confirmLabel="删除偏好"
        busy={deleting}
        onConfirm={() => void removeMemory()}
        onOpenChange={(open) => { if (!open && !deleting) setMemoryToDelete(null) }}
      />
    </div>
  )
}

function memoryEntryCaption(entry: MemoryEntry): string {
  const parts: string[] = []
  if (entry.memory.event_time) parts.push(new Date(entry.memory.event_time).toLocaleDateString())
  if (typeof entry.score === 'number' && entry.score > 0) parts.push(`相关度 ${entry.score.toFixed(2)}`)
  parts.push(new Date(entry.updated_at).toLocaleString())
  return parts.join(' · ')
}
