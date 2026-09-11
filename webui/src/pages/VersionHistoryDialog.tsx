import { useEffect, useMemo, useRef, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useQuery } from '@tanstack/react-query'

import {
  discardApplicationCandidate,
  getApplicationCandidate,
  getApplicationRollout,
  getTenantMembers,
  listApplicationVersions,
  promoteApplicationCandidate,
  rollbackApplicationVersion,
  stopApplicationRollout,
  updateApplicationRollout,
} from '../api'
import { ChannelBrandIcon } from '../components/ChannelBrand'
import { channelLabel } from '../components/channelMeta'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { DialogHeader } from '../components/DialogHeader'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { SelectControl } from '../components/SelectControl'
import { channelsOf, type Snapshot } from '../types'

type PendingVersionAction =
  | { type: 'restore'; version: number }
  | { type: 'stop' }
  | { type: 'promote' }
  | { type: 'discard' }

const PERCENT_PRESETS = [0, 500, 1000, 2500, 5000] as const

function ingressKey(channel: string, bindingID = '') {
  return channel === 'web' ? 'web' : `${channel}/${bindingID}`
}

function formatVersionTime(value: string) {
  return value.slice(0, 19).replace('T', ' ')
}

export function VersionHistoryDialog({
  app,
  onClose,
  onRestored,
}: {
  app: Snapshot
  onClose: () => void
  onRestored: () => Promise<void>
}) {
  const [basisPoints, setBasisPoints] = useState(1000)
  const [testUserIDs, setTestUserIDs] = useState<string[]>([])
  const [ingresses, setIngresses] = useState<string[]>([])
  const [nextTestUser, setNextTestUser] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState('')
  const [pendingAction, setPendingAction] = useState<PendingVersionAction | null>(null)
  const [rolloutSettingsOpen, setRolloutSettingsOpen] = useState(true)
  const rolloutPanelRef = useRef<HTMLElement>(null)

  const versionsQuery = useQuery({
    queryKey: ['console', 'application-versions', app.Config.tenant_id, app.Config.app_code],
    queryFn: () => listApplicationVersions(app.Config.tenant_id, app.Config.app_code),
    staleTime: 0,
    refetchOnMount: 'always',
  })
  const candidateQuery = useQuery({
    queryKey: ['console', 'application-candidate', app.Config.tenant_id, app.Config.app_code],
    queryFn: () => getApplicationCandidate(app.Config.tenant_id, app.Config.app_code),
    staleTime: 0,
    refetchOnMount: 'always',
  })
  const rolloutQuery = useQuery({
    queryKey: ['console', 'application-rollout', app.Config.tenant_id, app.Config.app_code],
    queryFn: () => getApplicationRollout(app.Config.tenant_id, app.Config.app_code),
    staleTime: 0,
    refetchOnMount: 'always',
  })
  const membersQuery = useQuery({
    queryKey: ['console', 'rollout-members', app.Config.tenant_id],
    queryFn: ({ signal }) => getTenantMembers(app.Config.tenant_id, { limit: 100 }, signal),
  })

  const versions = versionsQuery.data ?? []
  const candidate = candidateQuery.data ?? null
  const rollout = rolloutQuery.data ?? null
  const members = (membersQuery.data?.members ?? []).filter((member) => member.status === 'active')
  const queryError = versionsQuery.error ?? candidateQuery.error ?? rolloutQuery.error ?? membersQuery.error
  const visibleError = error || (queryError instanceof Error ? queryError.message : '')

  const ingressOptions = useMemo(() => [
    { key: 'web', channel: 'web', label: '网页' },
    ...channelsOf(app.Config).map((binding) => ({
      key: ingressKey(binding.type, binding.binding_id),
      channel: binding.type,
      label: `${channelLabel(binding.type)} · ${binding.binding_id}`,
    })),
  ], [app.Config])

  useEffect(() => {
    if (candidate) setRolloutSettingsOpen(true)
  }, [candidate?.Config.config_version])

  useEffect(() => {
    if (rollout) {
      setBasisPoints(rollout.basis_points)
      setTestUserIDs(rollout.test_user_ids ?? [])
      setIngresses(rollout.ingresses?.length ? rollout.ingresses : ingressOptions.map((entry) => entry.key))
      return
    }
    setBasisPoints(1000)
    setTestUserIDs([])
    setIngresses(ingressOptions.map((entry) => entry.key))
  }, [candidate?.Config.config_version, ingressOptions, rollout])

  const selectedMembers = useMemo(
    () => testUserIDs.map((id) => members.find((member) => member.platform_user_id === id) ?? { platform_user_id: id, display_name: id }),
    [members, testUserIDs],
  )
  const availableMembers = members.filter((member) => !testUserIDs.includes(member.platform_user_id))

  const refreshRelease = async () => {
    await Promise.all([candidateQuery.refetch(), rolloutQuery.refetch(), versionsQuery.refetch()])
  }

  const restore = async (version: number) => {
    setBusy(`restore-${version}`)
    setError('')
    try {
      await rollbackApplicationVersion(app.Config.tenant_id, app.Config.app_code, version)
      await onRestored()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
      setPendingAction(null)
    }
  }

  const saveRollout = async () => {
    if (!candidate) return
    if (basisPoints === 0 && testUserIDs.length === 0) {
      setError('请选择至少一名测试成员，或设置灰度比例。')
      return
    }
    if (ingresses.length === 0) {
      setError('请选择至少一个发布入口。')
      return
    }
    setBusy('rollout')
    setError('')
    try {
      await updateApplicationRollout(app.Config.tenant_id, app.Config.app_code, {
        expected_generation: rollout?.generation ?? 0,
        basis_points: basisPoints,
        test_user_ids: testUserIDs,
        ingresses,
      })
      await rolloutQuery.refetch()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const stopRollout = async () => {
    if (!rollout) return
    setBusy('stop')
    setError('')
    try {
      await stopApplicationRollout(app.Config.tenant_id, app.Config.app_code, rollout.generation)
      await rolloutQuery.refetch()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
      setPendingAction(null)
    }
  }

  const promote = async () => {
    if (!candidate) return
    setBusy('promote')
    setError('')
    try {
      await promoteApplicationCandidate(
        app.Config.tenant_id,
        app.Config.app_code,
        candidate.Config.config_version,
        rollout?.generation ?? 0,
      )
      await onRestored()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
      setPendingAction(null)
    }
  }

  const discard = async () => {
    if (!candidate) return
    setBusy('discard')
    setError('')
    try {
      await discardApplicationCandidate(app.Config.tenant_id, app.Config.app_code, candidate.Config.config_version)
      await refreshRelease()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
      setPendingAction(null)
    }
  }

  const toggleIngress = (key: string, checked: boolean) => {
    setIngresses((current) => checked ? [...new Set([...current, key])] : current.filter((entry) => entry !== key))
  }

  const toggleRolloutSettings = () => {
    if (rolloutSettingsOpen) {
      setRolloutSettingsOpen(false)
      return
    }
    setRolloutSettingsOpen(true)
    requestAnimationFrame(() => {
      rolloutPanelRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
      rolloutPanelRef.current?.focus({ preventScroll: true })
    })
  }

  return (
    <Dialog.Root open onOpenChange={(open) => { if (!open) onClose() }}>
      <Dialog.Portal>
        <Dialog.Overlay className="modal-backdrop" />
        <Dialog.Content className="modal version-history-modal">
          <DialogHeader
            title={`版本发布 · ${app.Config.app_code}`}
            description="稳定版本持续服务；候选版本可以先小范围验证，再逐步发布。"
            closeLabel="关闭版本发布"
          />
          {visibleError && <FeedbackBanner tone="error">{visibleError}</FeedbackBanner>}

          <div className="release-overview" aria-label="发布状态">
            <div className="release-version-card is-stable">
              <span>当前稳定</span>
              <strong>v{app.Config.config_version}</strong>
              <small>全部正常流量</small>
            </div>
            <div className={`release-version-card ${candidate ? 'is-candidate' : ''}`}>
              <span>候选版本</span>
              <strong>{candidate ? `v${candidate.Config.config_version}` : '—'}</strong>
              <small>{candidate ? (rollout ? `灰度 ${(rollout.basis_points / 100).toFixed(0)}%` : '待发布') : '暂无候选'}</small>
            </div>
          </div>

          {!candidate ? (
            <section className="release-empty">
              <strong>当前没有待发布版本</strong>
              <p>编辑机器人并选择“保存候选版本”，即可先验证指定成员或小比例流量，再决定是否全量发布。</p>
            </section>
          ) : rolloutSettingsOpen ? (
            <section ref={rolloutPanelRef} tabIndex={-1} className="release-panel">
              <div className="release-panel-head">
                <div>
                  <span className="release-eyebrow">{rollout ? '灰度进行中' : '候选已就绪'}</span>
                  <strong>v{candidate.Config.config_version}</strong>
                  <small>{formatVersionTime(candidate.PublishedAt)}</small>
                </div>
                <div className="release-panel-actions">
                  {rollout && <button type="button" className="secondary small-btn" disabled={busy !== ''} onClick={() => setPendingAction({ type: 'stop' })}>停止灰度</button>}
                  {!rollout && <button type="button" className="text-button danger" disabled={busy !== ''} onClick={() => setPendingAction({ type: 'discard' })}>放弃候选</button>}
                  <button type="button" className="secondary small-btn" disabled={busy !== ''} onClick={() => setPendingAction({ type: 'promote' })}>直接全量发布</button>
                </div>
              </div>

              <div className="release-config-section">
                <div className="release-config-title">
                  <strong>灰度比例</strong>
                  <span>{basisPoints === 0 ? '仅测试成员' : `${(basisPoints / 100).toFixed(0)}%`}</span>
                </div>
                <div className="release-percent-presets" role="group" aria-label="灰度比例快捷选择">
                  {PERCENT_PRESETS.map((value) => (
                    <button key={value} type="button" className={basisPoints === value ? 'is-active' : ''} disabled={busy !== ''} onClick={() => setBasisPoints(value)}>
                      {value === 0 ? '仅测试成员' : `${value / 100}%`}
                    </button>
                  ))}
                </div>
                <div className="release-slider-row">
                  <input aria-label="灰度比例" type="range" min={0} max={10000} step={100} value={basisPoints} disabled={busy !== ''} onChange={(event) => setBasisPoints(Number(event.target.value))} />
                  <output>{(basisPoints / 100).toFixed(0)}%</output>
                </div>
              </div>

              <div className="release-config-section">
                <div className="release-config-title">
                  <strong>发布入口</strong>
                  <span>{ingresses.length} / {ingressOptions.length}</span>
                </div>
                <p className="release-help">先限定哪些入口参与灰度；测试成员也不会绕过这里的范围。</p>
                <div className="release-ingress-grid">
                  {ingressOptions.map((entry) => (
                    <label key={entry.key} className={ingresses.includes(entry.key) ? 'is-selected' : ''}>
                      <input type="checkbox" checked={ingresses.includes(entry.key)} disabled={busy !== ''} onChange={(event) => toggleIngress(entry.key, event.target.checked)} />
                      <ChannelBrandIcon channel={entry.channel} size={18} />
                      <span>{entry.label}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="release-config-section">
                <div className="release-config-title">
                  <strong>测试成员</strong>
                  <span>{testUserIDs.length} 人</span>
                </div>
                <p className="release-help">指定成员在所选入口的私聊会优先进入候选版本；群聊始终按群会话参与比例分流。</p>
                <div className="release-member-picker">
                  <SelectControl
                    value={nextTestUser}
                    placeholder={availableMembers.length ? '选择租户成员' : '没有更多成员'}
                    disabled={busy !== '' || availableMembers.length === 0}
                    onValueChange={setNextTestUser}
                    options={availableMembers.map((member) => ({ value: member.platform_user_id, label: member.display_name || member.email || member.platform_user_id }))}
                  />
                  <button
                    type="button"
                    className="secondary small-btn"
                    disabled={busy !== '' || !nextTestUser}
                    onClick={() => {
                      if (!nextTestUser) return
                      setTestUserIDs((current) => [...new Set([...current, nextTestUser])])
                      setNextTestUser('')
                    }}
                  >添加</button>
                </div>
                {selectedMembers.length > 0 && (
                  <div className="release-member-list">
                    {selectedMembers.map((member) => (
                      <span key={member.platform_user_id}>
                        {member.display_name || member.platform_user_id}
                        <button type="button" aria-label={`移除 ${member.display_name || member.platform_user_id}`} disabled={busy !== ''} onClick={() => setTestUserIDs((current) => current.filter((id) => id !== member.platform_user_id))}>×</button>
                      </span>
                    ))}
                  </div>
                )}
              </div>

              <div className="release-submit-row">
                <div>
                  <strong>{rollout ? '当前灰度可随时调整' : '开始后仍可调整或停止'}</strong>
                  <small>已经进入执行队列的消息继续使用当时确定的版本。</small>
                </div>
                <button type="button" className="primary" disabled={busy !== '' || ingresses.length === 0} onClick={() => void saveRollout()}>
                  {busy === 'rollout' ? '保存中…' : rollout ? '保存灰度设置' : '开始灰度发布'}
                </button>
              </div>
            </section>
          ) : null}

          <section className="release-history">
            <div className="release-history-head">
              <div><strong>版本历史</strong><p>历史版本不可直接参与灰度；恢复会基于其内容创建新的稳定版本。</p></div>
            </div>
            <div className="table-scroll release-history-table-scroll">
              <table className="ui-table">
                <thead><tr><th>版本</th><th>创建时间</th><th>状态</th><th className="version-action-col">操作</th></tr></thead>
              <tbody>
                {versions.map((version) => {
                  const number = version.Config.config_version
                  const isStable = number === app.Config.config_version
                  const isCandidate = candidate?.Config.config_version === number
                  return (
                    <tr key={number} className={isCandidate ? 'is-candidate-version' : undefined}>
                      <td><span className="chip">v{number}</span></td>
                      <td className="time-cell">{formatVersionTime(version.PublishedAt)}</td>
                      <td>{isStable ? '当前稳定' : isCandidate ? (rollout ? `候选 · 灰度 ${(rollout.basis_points / 100).toFixed(0)}%` : '候选 · 待发布') : '历史'}</td>
                      <td className="version-action-col">
                        {isCandidate ? (
                          <button
                            type="button"
                            className="table-action"
                            disabled={busy !== ''}
                            aria-expanded={rolloutSettingsOpen}
                            onClick={toggleRolloutSettings}
                          >
                            {rolloutSettingsOpen ? '收起设置' : '展开设置'}
                          </button>
                        ) : !isStable && (
                          <button
                            type="button"
                            className="table-action"
                            disabled={busy !== '' || Boolean(candidate)}
                            title={candidate ? '请先处理当前候选版本' : undefined}
                            onClick={() => setPendingAction({ type: 'restore', version: number })}
                          >
                            {busy === `restore-${number}` ? '恢复中…' : '恢复此版本'}
                          </button>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
              </table>
            </div>
          </section>
        </Dialog.Content>
      </Dialog.Portal>

      <ConfirmDialog
        open={Boolean(pendingAction)}
        title={
          pendingAction?.type === 'restore' ? '恢复历史版本？'
            : pendingAction?.type === 'stop' ? '停止灰度？'
              : pendingAction?.type === 'discard' ? '放弃候选版本？'
                : '全量发布候选版本？'
        }
        description={
          pendingAction?.type === 'restore'
            ? `系统会复制 v${pendingAction.version} 的配置并创建一个新的稳定版本，历史记录不会被改写。`
            : pendingAction?.type === 'stop'
              ? `新请求将立即回到稳定版本 v${app.Config.config_version}；候选 v${candidate?.Config.config_version ?? ''} 会保留，可稍后重新灰度。`
              : pendingAction?.type === 'discard'
                ? `v${candidate?.Config.config_version ?? ''} 将不再作为候选版本，当前稳定版本不受影响。`
                : `v${candidate?.Config.config_version ?? ''} 将成为新的稳定版本并接管全部新流量。`
        }
        confirmLabel={pendingAction?.type === 'restore' ? '确认恢复' : pendingAction?.type === 'stop' ? '停止灰度' : pendingAction?.type === 'discard' ? '放弃候选' : '全量发布'}
        busy={busy !== ''}
        onConfirm={() => {
          if (pendingAction?.type === 'restore') void restore(pendingAction.version)
          else if (pendingAction?.type === 'stop') void stopRollout()
          else if (pendingAction?.type === 'discard') void discard()
          else if (pendingAction?.type === 'promote') void promote()
        }}
        onOpenChange={(open) => { if (!open && busy === '') setPendingAction(null) }}
      />
    </Dialog.Root>
  )
}
