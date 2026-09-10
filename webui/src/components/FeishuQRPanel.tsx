import { useMemo } from 'react'
import type { LoginProvider } from '../api'
import { useFeishuQRLogin, type FeishuQRStatus } from '../hooks/useFeishuQRLogin'
import { ChannelBrandIcon } from './ChannelBrand'

const STATUS_TEXT: Record<FeishuQRStatus, string> = {
  idle: '正在准备二维码…',
  loading: '正在准备二维码…',
  ready: '请使用飞书 App「扫一扫」，在手机上确认授权',
  scanned: '已扫码，请在手机上确认',
  redirecting: '已确认，正在跳转…',
  expired: '二维码已失效',
  failed: '二维码不可用',
}

function formatCountdown(seconds: number): string {
  const safe = Math.max(0, seconds)
  return `${String(Math.floor(safe / 60)).padStart(2, '0')}:${String(safe % 60).padStart(2, '0')}`
}

/**
 * 飞书内嵌二维码登录面板（方案 A）。
 * 二维码由飞书二维码 SDK 渲染，扫码确认后跳转 goto&tmp_code，
 * 飞书再 302 回 /api/v1/auth/callback 完成会话签发。
 */
export function FeishuQRPanel({
  provider,
  onFallback,
  busy,
}: {
  provider: LoginProvider
  onFallback: () => void
  busy: boolean
}) {
  const providerID = provider.provider_id ?? ''
  const containerID = `feishu-qr-${providerID.replace(/[^a-zA-Z0-9_-]/g, '-')}`
  const { status, error, secondsLeft, start } = useFeishuQRLogin(providerID, containerID)
  const countdown = useMemo(() => formatCountdown(secondsLeft), [secondsLeft])
  const preparing = status === 'idle' || status === 'loading'
  const blocked = status === 'expired' || status === 'failed'

  return (
    <div className="login-qr-panel">
      <div className="login-qr-head">
        <span className="login-provider-icon login-provider-feishu">
          <ChannelBrandIcon channel={provider.type} size={20} />
        </span>
        <span className="login-provider-copy">
          <strong>{provider.display_name}</strong>
          <small>{STATUS_TEXT[status]}</small>
        </span>
      </div>

      <div className="login-qr-stage">
        <div id={containerID} className="login-qr-container" />
        {preparing && <div className="login-qr-overlay">正在准备二维码…</div>}
        {blocked && (
          <div className="login-qr-overlay">
            <p>{status === 'expired' ? '二维码已失效，请刷新后重新扫码' : error || '二维码加载失败'}</p>
            <button type="button" className="secondary small-btn" onClick={() => void start()}>
              刷新二维码
            </button>
          </div>
        )}
      </div>

      <div className="login-qr-foot">
        <span className="login-qr-countdown">有效期剩余 {countdown}</span>
        <button type="button" className="secondary small-btn" disabled={busy} onClick={onFallback}>
          无法扫码？使用跳转登录
        </button>
      </div>
    </div>
  )
}
