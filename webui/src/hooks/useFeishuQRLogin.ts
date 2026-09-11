import { useCallback, useEffect, useRef, useState } from 'react'
import { beginFeishuQR, type LoginQRBegin } from '../api'

/**
 * 飞书内嵌二维码登录状态机（方案 A）。
 *
 * idle → loading（申请会话/加载 SDK）→ ready（展示二维码）
 *      → scanned / redirecting（已在飞书 App 确认，跳转换取 code）
 * 异常分支：expired（二维码超时）、failed（申请或 SDK 失败）。
 *
 * 说明：飞书不会服务端回调本系统，用户确认后由浏览器拿到 tmp_code 并跳转
 * goto&tmp_code，飞书再 302 回 LOGIN_CALLBACK_URL，因此这里只做浏览器跳转。
 */
export type FeishuQRStatus = 'idle' | 'loading' | 'ready' | 'scanned' | 'redirecting' | 'expired' | 'failed'

interface QRLoginInstance {
  matchOrigin?: (origin: string) => boolean
  matchData?: (data: unknown) => boolean
}

interface QRLoginFactory {
  (options: { id: string; goto: string; width?: string; height?: string; style?: string }): QRLoginInstance
}

declare global {
  interface Window {
    QRLogin?: QRLoginFactory
  }
}

const SDK_LOAD_TIMEOUT_MS = 5_000
const SDK_SCRIPT_ATTR = 'data-feishu-qr-sdk'

// 模块级缓存：同一次页面生命周期内 SDK 只注入一次。
let sdkPromise: Promise<void> | null = null

export function loadFeishuQRSDK(url: string): Promise<void> {
  if (typeof window !== 'undefined' && window.QRLogin) return Promise.resolve()
  if (sdkPromise) return sdkPromise

  sdkPromise = new Promise<void>((resolve, reject) => {
    // 重试时丢弃上一次的 script，避免复用已触发过 load/error 的元素而空等超时。
    document.querySelector<HTMLScriptElement>(`script[${SDK_SCRIPT_ATTR}]`)?.remove()
    const script = document.createElement('script')

    let settled = false
    const timer = window.setTimeout(() => finish(new Error('二维码 SDK 加载超时')), SDK_LOAD_TIMEOUT_MS)

    function finish(error?: Error) {
      if (settled) return
      settled = true
      window.clearTimeout(timer)
      if (error) {
        reject(error)
        return
      }
      if (window.QRLogin) resolve()
      else reject(new Error('二维码 SDK 未就绪'))
    }

    script.src = url
    script.async = true
    script.setAttribute(SDK_SCRIPT_ATTR, '1')
    document.head.appendChild(script)
    if (window.QRLogin) {
      finish()
      return
    }
    script.addEventListener('load', () => finish(), { once: true })
    script.addEventListener('error', () => finish(new Error('二维码 SDK 加载失败')), { once: true })
  }).catch((error: unknown) => {
    // 允许后续重试重新注入。
    sdkPromise = null
    throw error
  })

  return sdkPromise
}

export function isFeishuQRMessageAccepted(instance: QRLoginInstance, event: Pick<MessageEvent, 'origin' | 'data'>): boolean {
  // Fail closed if the SDK does not expose its protocol validators. Guessing
  // from a domain suffix is weaker than the provider-owned validation seam.
  return typeof instance.matchOrigin === 'function'
    && typeof instance.matchData === 'function'
    && instance.matchOrigin(event.origin)
    && instance.matchData(event.data)
}

export function useFeishuQRLogin(providerID: string, containerID: string) {
  const [status, setStatus] = useState<FeishuQRStatus>('idle')
  const [error, setError] = useState('')
  const [secondsLeft, setSecondsLeft] = useState(0)
  const [totalSeconds, setTotalSeconds] = useState(0)

  const sessionRef = useRef<LoginQRBegin | null>(null)
  const instanceRef = useRef<QRLoginInstance | null>(null)
  const handledCodeRef = useRef('')
  const aliveRef = useRef(true)
  const requestRef = useRef<AbortController | null>(null)

  const start = useCallback(async () => {
    requestRef.current?.abort()
    const request = new AbortController()
    requestRef.current = request
    setStatus('loading')
    setError('')
    handledCodeRef.current = ''
    instanceRef.current = null
    document.getElementById(containerID)?.replaceChildren()
    try {
      const session = await beginFeishuQR(providerID, request.signal)
      if (!aliveRef.current) return
      sessionRef.current = session
      await loadFeishuQRSDK(session.sdk_url)
      if (!aliveRef.current) return
      if (!window.QRLogin) throw new Error('二维码 SDK 未就绪')
      instanceRef.current = window.QRLogin({
        id: containerID,
        goto: session.goto,
        width: '280',
        height: '280',
      })
      setTotalSeconds(session.expires_in || 0)
      setSecondsLeft(session.expires_in || 0)
      setStatus('ready')
    } catch (cause) {
      if ((cause as { name?: string }).name === 'AbortError') return
      if (!aliveRef.current) return
      setError((cause as Error).message || '二维码登录不可用')
      setStatus('failed')
    }
  }, [containerID, providerID])

  // 扫码成功后由浏览器跳转；飞书会 302 回 LOGIN_CALLBACK_URL。
  useEffect(() => {
    aliveRef.current = true
    const handleMessage = (event: MessageEvent) => {
      const session = sessionRef.current
      const instance = instanceRef.current
      if (!session || !instance) return
      if (!isFeishuQRMessageAccepted(instance, event)) return
      const tmpCode = String((event.data as { tmp_code?: unknown } | null)?.tmp_code ?? '')
      if (!tmpCode || handledCodeRef.current === tmpCode) return
      handledCodeRef.current = tmpCode
      setStatus('redirecting')
      window.location.href = `${session.goto}&tmp_code=${encodeURIComponent(tmpCode)}`
    }
    window.addEventListener('message', handleMessage)
    return () => {
      aliveRef.current = false
      window.removeEventListener('message', handleMessage)
    }
  }, [])

  // 倒计时：到期后必须让用户刷新，因为授权码只有 5 分钟有效期。
  useEffect(() => {
    if (status !== 'ready') return
    const timer = window.setInterval(() => {
      setSecondsLeft((left) => {
        if (left <= 1) {
          window.clearInterval(timer)
          setStatus((current) => (current === 'ready' ? 'expired' : current))
          return 0
        }
        return left - 1
      })
    }, 1000)
    return () => window.clearInterval(timer)
  }, [status])

  useEffect(() => {
    void start()
    return () => {
      aliveRef.current = false
      requestRef.current?.abort()
      document.getElementById(containerID)?.replaceChildren()
    }
  }, [containerID, start])

  return { status, error, secondsLeft, totalSeconds, start }
}
