import { useMemo, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useQuery } from '@tanstack/react-query'

import { createBackendProfile, deleteBackendProfile, getBackendProfiles, updateBackendProfile } from '../api'
import type { BackendProfile } from '../types'
import { DatabaseIcon, TrashIcon } from '../components/Icons'
import { PlusIcon } from '../components/PageIcons'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { DialogHeader } from '../components/DialogHeader'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SelectControl } from '../components/SelectControl'
import { StatusIndicator } from '../components/StatusIndicator'

const DRIVERS = [
  ['postgres', 'PostgreSQL'],
  ['redis', 'Redis'],
  ['pgvector', 'pgvector'],
  ['qdrant', 'Qdrant'],
  ['s3', 'S3 兼容对象存储'],
  ['cos', '腾讯云 COS'],
  ['mem0', 'Mem0'],
] as const

const DOMAIN_LABELS: Record<string, string> = {
  session: '会话', memory: '长期偏好', knowledge: '知识库', artifact: '文件',
}

const CONNECTION_HELP: Record<string, { required: boolean; placeholder: string; detail: string }> = {
  postgres: { required: false, placeholder: '留空使用平台 PostgreSQL', detail: '可留空；独立实例填写 env:变量名（值为 PostgreSQL DSN）' },
  pgvector: { required: false, placeholder: '留空使用平台 PostgreSQL', detail: '可留空；独立实例填写 env:变量名（值为 PostgreSQL DSN）' },
  redis: { required: true, placeholder: '例如 env:SESSION_REDIS_URL', detail: '填写 env:变量名（值为 Redis URL）' },
  qdrant: { required: true, placeholder: '例如 env:QDRANT_CONFIG', detail: '填写 env:变量名（值为 Qdrant JSON 配置）' },
  s3: { required: true, placeholder: '例如 env:S3_CONFIG', detail: '填写 env:变量名（值为 S3 JSON 配置）' },
  cos: { required: true, placeholder: '例如 env:COS_CONFIG', detail: '填写 env:变量名（值为 COS JSON 配置）' },
  mem0: { required: true, placeholder: '例如 env:MEM0_HOST', detail: '填写 env:变量名（值为 Mem0 服务地址）' },
}

type Draft = {
  profile_id: string
  display_name: string
  driver: string
  connection_ref: string
  status: 'active' | 'disabled'
}

const EMPTY_DRAFT: Draft = { profile_id: '', display_name: '', driver: 'postgres', connection_ref: '', status: 'active' }

export function BackendProfilesPage() {
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT)
  const [editing, setEditing] = useState('')
  const [formOpen, setFormOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<BackendProfile | null>(null)
  const profilesQuery = useQuery({
    queryKey: ['console', 'backend-profiles'],
    queryFn: ({ signal }) => getBackendProfiles(signal),
    staleTime: 10_000,
  })
  const profiles = profilesQuery.data ?? []
  const connectionHelp = CONNECTION_HELP[draft.driver] ?? { required: true, placeholder: '例如 env:BACKEND_CONFIG', detail: '填写 env:变量名' }
  const canSave = Boolean(
    draft.profile_id.trim()
    && draft.display_name.trim()
    && (editing || !connectionHelp.required || draft.connection_ref.trim()),
  )
  const activeCount = profiles.filter((profile) => profile.status === 'active').length
  const domainCount = useMemo(() => new Set(profiles.flatMap((profile) => profile.domains)).size, [profiles])

  const openCreate = () => {
    setDraft(EMPTY_DRAFT)
    setEditing('')
    setFormOpen(true)
    setError('')
  }

  const openEdit = (profile: BackendProfile) => {
    setDraft({
      profile_id: profile.profile_id,
      display_name: profile.display_name,
      driver: profile.driver,
      connection_ref: profile.connection_ref ?? '',
      status: profile.status,
    })
    setEditing(profile.profile_id)
    setFormOpen(true)
    setError('')
  }

  const save = async () => {
    if (!draft.profile_id.trim() || !draft.display_name.trim()) return
    setBusy(true)
    setError('')
    setNotice('')
    try {
      if (editing) {
        await updateBackendProfile(editing, {
          display_name: draft.display_name.trim(), driver: draft.driver, connection_ref: draft.connection_ref.trim() || undefined, status: draft.status,
        })
      } else {
        await createBackendProfile({
          profile_id: draft.profile_id.trim(), display_name: draft.display_name.trim(), driver: draft.driver,
          connection_ref: draft.connection_ref.trim() || undefined,
        })
      }
      await profilesQuery.refetch()
      setFormOpen(false)
      setEditing('')
      setDraft(EMPTY_DRAFT)
      setNotice(editing ? '数据后端已更新' : '数据后端已创建')
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const remove = async () => {
    if (!deleteTarget) return
    setBusy(true)
    setError('')
    try {
      await deleteBackendProfile(deleteTarget.profile_id)
      await profilesQuery.refetch()
      setDeleteTarget(null)
      setNotice('数据后端已删除')
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy(false)
    }
  }

  if (profilesQuery.isLoading) return <div className="page-stack backend-profiles-page"><LoadingState label="正在读取数据后端…" /></div>

  return (
    <div className="page-stack backend-profiles-page">
      {(error || profilesQuery.error) && <FeedbackBanner tone="error">{error || (profilesQuery.error as Error).message}</FeedbackBanner>}
      {notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}

      <dl className="backend-profile-summary" aria-label="数据后端汇总">
        <div><dt>数据后端</dt><dd>{profiles.length}</dd></div>
        <div><dt>当前可用</dt><dd>{activeCount}</dd></div>
        <div><dt>覆盖数据域</dt><dd>{domainCount} / 4</dd></div>
      </dl>

      <section className="members-panel">
        <PanelHeader
          icon={<DatabaseIcon size={18} />}
          title="数据后端"
          description="平台统一维护数据连接。租户只能看到已授权后端的名称、类型、能力与可用状态。"
          actions={<div className="members-header-actions">
            <RefreshButton onClick={() => void profilesQuery.refetch()} loading={profilesQuery.isFetching} label="刷新数据后端" />
            <button type="button" className="secondary" onClick={openCreate}><PlusIcon size={14} /> 新建数据后端</button>
          </div>}
        />

        <div className="members-table-wrap">
          <table className="members-table backend-profile-table">
            <thead><tr><th>数据后端</th><th>类型</th><th>可用数据域</th><th>连接配置引用</th><th>状态</th><th className="actions-col">操作</th></tr></thead>
            <tbody>
              {profiles.length === 0 ? <tr><td colSpan={6} className="table-empty">暂无数据后端</td></tr> : profiles.map((profile) => (
                <tr key={profile.profile_id}>
                  <td><div className="backend-profile-name"><strong>{profile.display_name}</strong><span className="mono-value">{profile.profile_id}</span></div></td>
                  <td><span className="backend-driver-badge">{DRIVERS.find(([value]) => value === profile.driver)?.[1] ?? profile.driver}</span></td>
                  <td><div className="backend-domain-list">{profile.domains.map((domain) => <span key={domain}>{DOMAIN_LABELS[domain] ?? domain}</span>)}</div></td>
                  <td><span className="mono-value">{profile.connection_ref || '平台默认连接'}</span></td>
                  <td><StatusIndicator tone={profile.status === 'active' ? 'success' : 'neutral'} appearance="pill">{profile.status === 'active' ? '可用' : '已停用'}</StatusIndicator></td>
                  <td className="actions-col"><div className="backend-profile-actions"><button type="button" className="secondary small-btn" onClick={() => openEdit(profile)}>编辑</button><button type="button" className="icon-btn danger" aria-label={`删除 ${profile.display_name}`} disabled={profile.profile_id === 'platform-postgres' || profile.profile_id === 'platform-pgvector'} onClick={() => setDeleteTarget(profile)}><TrashIcon size={14} /></button></div></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <Dialog.Root open={formOpen} onOpenChange={(open) => { if (!open && !busy) setFormOpen(false) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="modal bot-dialog backend-profile-dialog">
            <DialogHeader
              className="bot-dialog-head"
              closeClassName="bot-dialog-close"
              closeLabel="关闭数据后端配置"
              title={editing ? '编辑数据后端' : '新建数据后端'}
              description={editing ? '可修改显示名称和状态；后端类型与连接配置保持不变。' : '连接信息保存在部署环境变量中，这里只保存引用。'}
            />

            <div className="bot-dialog-body backend-profile-dialog-body">
              {error && <FeedbackBanner tone="error">{error}</FeedbackBanner>}
              <div className="backend-profile-dialog-fields">
                <label>
                  后端标识
                  <input disabled={Boolean(editing)} value={draft.profile_id} onChange={(event) => setDraft((current) => ({ ...current, profile_id: event.target.value }))} placeholder="例如 session-redis-a" />
                </label>
                <label>
                  显示名称
                  <input value={draft.display_name} onChange={(event) => setDraft((current) => ({ ...current, display_name: event.target.value }))} placeholder="例如会话 Redis A" />
                </label>
                <label>
                  类型
                  <SelectControl value={draft.driver} disabled={Boolean(editing)} options={DRIVERS.map(([value, label]) => ({ value, label }))} onValueChange={(driver) => setDraft((current) => ({ ...current, driver }))} />
                </label>
                {editing && (
                  <label>
                    状态
                    <SelectControl value={draft.status} options={[{ value: 'active', label: '可用' }, { value: 'disabled', label: '已停用' }]} onValueChange={(status) => setDraft((current) => ({ ...current, status: status as Draft['status'] }))} />
                  </label>
                )}
                <label className="backend-profile-dialog-connection">
                  <span>连接配置引用{!editing && !connectionHelp.required ? '（可选）' : ''}</span>
                  <input
                    disabled={Boolean(editing)}
                    value={draft.connection_ref}
                    onChange={(event) => setDraft((current) => ({ ...current, connection_ref: event.target.value }))}
                    placeholder={editing && !draft.connection_ref ? '平台默认连接' : connectionHelp.placeholder}
                  />
                  <small>{editing ? '连接配置不可原地修改；需要更换时请新建数据后端。' : connectionHelp.detail}</small>
                </label>
              </div>
            </div>

            <div className="bot-dialog-footer">
              <Dialog.Close asChild><button type="button" className="secondary" disabled={busy}>取消</button></Dialog.Close>
              <button type="button" className="primary" disabled={busy || !canSave} onClick={() => void save()}>{busy ? '保存中…' : editing ? '保存修改' : '创建数据后端'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      <ConfirmDialog open={Boolean(deleteTarget)} title="删除数据后端？" description={deleteTarget ? `将删除 ${deleteTarget.display_name}。已授权给租户或被配置历史引用的数据后端不能删除。` : ''} confirmLabel="删除" busy={busy} onConfirm={() => void remove()} onOpenChange={(open) => { if (!open && !busy) setDeleteTarget(null) }} />
    </div>
  )
}
