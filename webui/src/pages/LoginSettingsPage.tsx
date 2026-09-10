import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getAuthConfiguration, verifyLoginProvider, type AuthProviderConfiguration } from '../api'
import { ChannelBrandIcon } from '../components/ChannelBrand'
import { CopyButton } from '../components/CopyButton'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { RefreshButton } from '../components/RefreshButton'
import { ShieldIcon } from '../components/Icons'
import { StatusIndicator, type StatusTone } from '../components/StatusIndicator'

type ProviderGuide = {
  required: string
  environment: string
  steps: string[]
  note?: string
  consoleLabel?: string
  consoleURL?: string
}

const VERIFY_ERRORS: Record<string, string> = {
  provider_unavailable: '登录方式在验证过程中已不可用，请重新读取配置。',
  exchange_failed: '官方授权已返回，但平台换取用户身份失败。请检查凭据、回调地址、应用权限和发布状态。',
  identity_link_failed: '该企业身份已经属于其他平台账号，无法关联到当前管理员账号。',
}

export function LoginSettingsPage() {
  const [busyProvider, setBusyProvider] = useState('')
  const [verifyError, setVerifyError] = useState('')
  const configurationQuery = useQuery({
    queryKey: ['console', 'auth-configuration'],
    queryFn: ({ signal }) => getAuthConfiguration(signal),
    staleTime: 10_000,
  })
  const configuration = configurationQuery.data
  const callbackURL = configuration?.callback_url ?? ''
  const formalProviders = configuration?.providers.filter((provider) => provider.type !== 'mock') ?? []
  const enabledCount = formalProviders.filter((provider) => provider.enabled).length
  const verifiedCount = formalProviders.filter((provider) => provider.last_successful_login_at).length
  const loopback = /^https?:\/\/(?:127\.0\.0\.1|localhost)(?::\d+)?\//i.test(callbackURL)
  const params = new URLSearchParams(window.location.search)
  const verifiedProviderID = params.get('provider_verified') ?? ''
  const callbackVerifyError = params.get('provider_verify_error') ?? ''

  const verify = async (provider: AuthProviderConfiguration) => {
    if (!provider.provider_id || provider.type === 'local' || provider.type === 'mock') return
    setBusyProvider(provider.provider_id)
    setVerifyError('')
    try {
      const result = await verifyLoginProvider(provider.provider_id)
      window.location.href = result.auth_url
    } catch (error) {
      setBusyProvider('')
      setVerifyError((error as Error).message)
    }
  }

  if (configurationQuery.isLoading) {
    return <div className="page-stack login-settings-page"><LoadingState label="正在读取登录配置…" /></div>
  }

  return (
    <div className="page-stack login-settings-page">
      {configurationQuery.error instanceof Error && (
        <FeedbackBanner tone="error">
          <span>{configurationQuery.error.message}</span>
          <RefreshButton onClick={() => void configurationQuery.refetch()} loading={configurationQuery.isFetching} label="重新读取登录配置" />
        </FeedbackBanner>
      )}
      {verifiedProviderID && (
        <FeedbackBanner tone="success">登录验证成功。该企业身份已经加入当前平台账号。</FeedbackBanner>
      )}
      {(verifyError || callbackVerifyError) && (
        <FeedbackBanner tone="error">{verifyError || VERIFY_ERRORS[callbackVerifyError] || '登录验证失败，请检查登录配置。'}</FeedbackBanner>
      )}

      <section className="login-settings-summary">
        <div>
          <span className="login-settings-eyebrow">登录方式</span>
          <h2>{enabledCount > 0 ? `已启用 ${enabledCount} 种正式登录方式` : '尚未启用正式登录'}</h2>
          <p>{verifiedCount > 0 ? `${verifiedCount} 种企业登录已经完成真实 OAuth 验证。` : '“已配置”不等于“可以登录”；企业登录至少完成一次真实 OAuth 后才标记为“已验证”。'}</p>
        </div>
        <span className="login-settings-summary-icon"><ShieldIcon size={26} /></span>
      </section>

      <section className="login-callback-panel">
        <div className="login-callback-copy">
          <span>统一登录回调地址</span>
          <strong>{callbackURL || '尚未配置企业登录回调地址'}</strong>
          <p>企业微信、飞书和 OIDC 后台登记的地址必须与这里完全一致。</p>
        </div>
        {callbackURL && <CopyButton value={callbackURL} label="复制地址" copiedLabel="已复制" className="secondary login-copy-button" iconSize={15} />}
      </section>

      {loopback && (
        <div className="login-settings-notice" role="note">
          当前使用本机回调地址，只适合本机浏览器开发验证。部署到服务器后应改成用户浏览器可访问的固定 HTTPS 地址，再同步更新企业微信、飞书或 OIDC 后台。
        </div>
      )}

      <section className="login-provider-settings-grid" aria-label="登录方式">
        {(configuration?.providers ?? []).map((provider) => (
          <ProviderCard
            key={provider.provider_id || provider.type}
            provider={provider}
            callbackURL={callbackURL}
            busy={busyProvider === provider.provider_id}
            onVerify={() => void verify(provider)}
          />
        ))}
      </section>
    </div>
  )
}

function ProviderCard({
  provider,
  callbackURL,
  busy,
  onVerify,
}: {
  provider: AuthProviderConfiguration
  callbackURL: string
  busy: boolean
  onVerify: () => void
}) {
  const isMock = provider.type === 'mock'
  const state = providerState(provider)
  const guide = providerGuide(provider.type, callbackURL)
  const metadata = provider.metadata ?? {}
  const identifiers = provider.type === 'wecom'
    ? [['CorpID', metadata.corp_id], ['AgentID', metadata.agent_id]]
    : provider.type === 'feishu'
      ? [['App ID', metadata.app_id], ['Tenant Key', metadata.tenant_key]]
      : provider.type === 'oidc'
        ? [['Issuer', metadata.issuer], ['Client ID', metadata.client_id]]
        : provider.type === 'mock'
          ? [['测试用户', metadata.subject_id]]
          : []
  const canVerify = provider.enabled && provider.provider_id && provider.type !== 'local' && provider.type !== 'mock'

  return (
    <article className={`login-provider-setting-card${isMock ? ' is-mock' : ''}`}>
      <div className="login-provider-setting-head">
        <span className={`login-provider-setting-icon login-provider-${provider.type}`}>
          {provider.type === 'local' || provider.type === 'oidc' || provider.type === 'mock'
            ? <ShieldIcon size={21} />
            : <ChannelBrandIcon channel={provider.type} size={21} />}
        </span>
        <StatusIndicator tone={state.tone} appearance="pill">{state.label}</StatusIndicator>
      </div>

      <div className="login-provider-setting-copy">
        <h3>{provider.display_name}</h3>
        <p>{providerDescription(provider.type)}</p>
      </div>

      <dl className="login-provider-setting-meta">
        <div><dt>所需配置</dt><dd>{guide.required}</dd></div>
        {provider.provider_id && <div><dt>配置标识</dt><dd>{provider.provider_id}</dd></div>}
        {identifiers.map(([label, value]) => value ? <div key={label}><dt>{label}</dt><dd>{value}</dd></div> : null)}
        {provider.last_successful_login_at && (
          <div><dt>最近验证</dt><dd>{formatDateTime(provider.last_successful_login_at)}</dd></div>
        )}
      </dl>

      {provider.type === 'local' && (
        <p className="login-provider-setting-hint">
          {provider.registration_enabled ? '当前允许用户自助注册；注册只创建平台账号，租户权限仍由管理员分配。' : '当前关闭自助注册。'} 密码只保存不可逆哈希。
        </p>
      )}
      {isMock && <p className="login-provider-setting-hint">仅用于开发和自动化测试，不应作为正式用户登录方式。</p>}

      {provider.type !== 'local' && !isMock && (
        <div className="login-provider-setting-actions">
          {canVerify && (
            <button type="button" className="secondary small-btn" disabled={busy} onClick={onVerify}>
              {busy ? '正在跳转…' : provider.last_successful_login_at ? '重新验证' : '验证登录'}
            </button>
          )}
          <details className="login-provider-guide">
            <summary>配置指南</summary>
            <div className="login-provider-guide-body">
              {guide.note && <p>{guide.note}</p>}
              <ol>{guide.steps.map((step) => <li key={step}>{step}</li>)}</ol>
              <div className="login-provider-guide-env">
                <span>平台配置</span>
                <code>{guide.environment}</code>
              </div>
              {callbackURL && (
                <div className="login-provider-guide-callback">
                  <span>回调地址</span>
                  <code>{callbackURL}</code>
                  <CopyButton value={callbackURL} label="复制" copiedLabel="已复制" className="text-button" iconSize={13} />
                </div>
              )}
              {guide.consoleURL && (
                <a href={guide.consoleURL} target="_blank" rel="noreferrer">打开{guide.consoleLabel}</a>
              )}
            </div>
          </details>
        </div>
      )}
    </article>
  )
}

function providerState(provider: AuthProviderConfiguration): { label: string; tone: StatusTone } {
  if (provider.type === 'local') return provider.enabled ? { label: '已启用', tone: 'success' } : { label: '已关闭', tone: 'neutral' }
  if (provider.type === 'mock') return provider.enabled ? { label: '开发测试', tone: 'info' } : { label: '已关闭', tone: 'neutral' }
  if (!provider.configured || !provider.enabled) return { label: '未配置', tone: 'neutral' }
  if (provider.last_successful_login_at) return { label: '已验证', tone: 'success' }
  return { label: '待验证', tone: 'warning' }
}

function providerDescription(type: AuthProviderConfiguration['type']): string {
  if (type === 'local') return '平台本地用户名和密码，不依赖外部身份服务。'
  if (type === 'wecom') return '企业微信自建应用扫码登录，与企业微信智能机器人 Bot 凭据是两套配置。'
  if (type === 'feishu') return '通过飞书自建应用完成 OAuth 登录；可以复用当前机器人所在的同一个飞书自建应用。'
  if (type === 'oidc') return '连接企业已有的标准 OIDC 身份服务。'
  return '显式开启的开发测试登录。'
}

function providerGuide(type: AuthProviderConfiguration['type'], callbackURL: string): ProviderGuide {
  const callbackStep = callbackURL ? `将回调地址登记为 ${callbackURL}` : '先在平台配置 LOGIN_CALLBACK_URL，再把完全相同的地址登记到身份服务后台'
  if (type === 'wecom') {
    return {
      required: 'CorpID · AgentID · Secret',
      environment: 'LOGIN_PROVIDERS_JSON + WECOM_LOGIN_SECRET',
      note: '企业微信智能机器人的 Bot ID / Secret 不能用于网页登录。登录需要企业内部“自建应用”的 CorpID、AgentID 和 Secret。',
      steps: [
        '进入企业微信管理后台，在“应用管理”创建或选择一个自建应用。',
        '从企业信息和自建应用中取得 CorpID、AgentID、Secret。',
        `在该应用的授权登录/网页授权设置中，${callbackStep}。`,
        '把 CorpID、AgentID 和 Secret 配到平台登录 Provider，重启服务。',
        '回到这里点击“验证登录”，用真实企业成员完成一次扫码授权。',
      ],
      consoleLabel: '企业微信管理后台',
      consoleURL: 'https://work.weixin.qq.com/wework_admin/frame#apps',
    }
  }
  if (type === 'feishu') {
    return {
      required: 'App ID · App Secret',
      environment: 'LOGIN_PROVIDERS_JSON + FEISHU_LOGIN_SECRET',
      note: '如果当前飞书机器人本身就是企业自建应用，可以复用同一 App ID / App Secret，不需要为了登录再创建一套应用。',
      steps: [
        '进入飞书开放平台，创建或打开当前企业自建应用。',
        '在“凭证与基础信息”取得 App ID 和 App Secret。',
        `在“安全设置”中${callbackStep}。`,
        '开启网页登录所需的用户身份权限，并发布一个可用版本。',
        '配置平台 Provider 后重启服务，再点击“验证登录”完成一次真实授权。',
      ],
      consoleLabel: '飞书开放平台',
      consoleURL: 'https://open.feishu.cn/app',
    }
  }
  if (type === 'oidc') {
    return {
      required: 'Issuer · Client ID · Client Secret',
      environment: 'LOGIN_PROVIDERS_JSON + OIDC_LOGIN_CLIENT_SECRET',
      steps: [
        '在企业身份服务中创建一个 OIDC Web Client。',
        `为该 Client ${callbackStep}。`,
        '确认 Issuer 支持 OIDC Discovery，并取得 Client ID / Client Secret。',
        '配置平台 Provider 后重启服务，再点击“验证登录”完成一次真实授权。',
      ],
    }
  }
  if (type === 'local') {
    return { required: '无需第三方凭据', environment: 'LOGIN_LOCAL_ENABLED / LOGIN_LOCAL_REGISTRATION_ENABLED', steps: [] }
  }
  return { required: '无需第三方凭据', environment: 'LOGIN_MOCK_ENABLED', steps: [] }
}

function formatDateTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(date)
}
