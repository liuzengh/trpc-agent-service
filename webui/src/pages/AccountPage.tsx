import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getAccountProfile, updateAccountProfile } from '../api'
import { useAppContext } from '../context'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'

const IDENTITY_LABELS: Record<string, string> = {
  local: '普通账号',
  wecom: '企业微信',
  feishu: '飞书',
  oidc: '企业 SSO',
  mock: '测试登录',
}

export function AccountPage() {
  const { refreshUser } = useAppContext()
  const [displayName, setDisplayName] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const profileQuery = useQuery({
    queryKey: ['console', 'account-profile'],
    queryFn: ({ signal }) => getAccountProfile(signal),
  })
  const profile = profileQuery.data

  useEffect(() => {
    if (profile) setDisplayName(profile.display_name)
  }, [profile])

  const save = async () => {
    const name = displayName.trim()
    if (!name || name === profile?.display_name) return
    setSaving(true)
    setError('')
    setNotice('')
    try {
      const updated = await updateAccountProfile(name)
      setDisplayName(updated.display_name)
      await profileQuery.refetch()
      await refreshUser()
      setNotice('账号名称已更新')
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setSaving(false)
    }
  }

  if (profileQuery.isLoading) return <div className="account-page"><LoadingState label="正在读取账号…" /></div>

  return (
    <div className="account-page">
      {(error || profileQuery.error) && <FeedbackBanner tone="error">{error || (profileQuery.error as Error).message}</FeedbackBanner>}
      {notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}

      <section className="account-simple-card">
        <div className="account-simple-head">
          <div>
            <h2>账号</h2>
            <p>平台账号信息与登录身份。机器人不会与个人账号绑定。</p>
          </div>
          <span className="mono-value">{profile?.platform_user_id}</span>
        </div>
        <div className="account-simple-row">
          <label htmlFor="account-display-name">账号名称</label>
          <div className="account-name-editor">
            <input id="account-display-name" value={displayName} maxLength={80} onChange={(event) => setDisplayName(event.target.value)} />
            <button type="button" className="primary small" disabled={saving || !displayName.trim() || displayName.trim() === profile?.display_name} onClick={() => void save()}>
              {saving ? '保存中…' : '保存'}
            </button>
          </div>
        </div>
        {profile?.email && (
          <div className="account-simple-row">
            <span>邮箱</span>
            <strong>{profile.email}</strong>
          </div>
        )}
      </section>

      <section className="account-simple-card">
        <div className="account-simple-head">
          <div>
            <h2>登录身份</h2>
            <p>这些身份都用于登录同一个平台账号，与任何机器人无关。</p>
          </div>
          <span>{profile?.login_methods.length ?? 0} 个</span>
        </div>
        <div className="account-identity-list">
          {(profile?.login_methods ?? []).map((method) => (
            <div className="account-identity-row" key={`${method.provider_id}/${method.subject_id}`}>
              <div>
                <strong>{IDENTITY_LABELS[method.type] ?? method.display_name}</strong>
                <span>{method.display_name}</span>
              </div>
              <code title={method.subject_id}>{method.subject_id}</code>
            </div>
          ))}
          {(profile?.login_methods.length ?? 0) === 0 && <div className="account-simple-empty">暂无登录身份。</div>}
        </div>
      </section>
    </div>
  )
}
