// Login page: real token validation via GET /admin/v1/tenants (the cheapest
// authenticated endpoint), then tenant selection from the live registry.
import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { isAuthPersisted, useAuthStore } from '../stores/auth';
import CustomModelModal from '../components/CustomModelModal';
import type { CustomModelResponse, TenantInfo } from '../types';
import '../styles/login.css';

type Stage = 'idle' | 'verifying' | 'ready' | 'error';

export default function LoginPage() {
  const navigate = useNavigate();
  const login = useAuthStore((s) => s.login);
  const existingToken = useAuthStore((s) => s.token);
  const existingTenant = useAuthStore((s) => s.tenant);
  const existingUserId = useAuthStore((s) => s.userId);

  const [token, setToken] = useState(existingToken);
  const [showToken, setShowToken] = useState(false);
  const [userId, setUserId] = useState(existingUserId || 'alice');
  const [keepLogin, setKeepLogin] = useState(isAuthPersisted());
  const [stage, setStage] = useState<Stage>('idle');
  const [verifiedToken, setVerifiedToken] = useState('');
  const [tenants, setTenants] = useState<TenantInfo[] | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [hint, setHint] = useState('输入管理令牌后加载租户列表');
  const [submitting, setSubmitting] = useState(false);
  const [showCustomModal, setShowCustomModal] = useState(false);
  const verificationSequence = useRef(0);

  async function verify(inputToken: string): Promise<TenantInfo[] | null> {
    // Validate against the real admin endpoint by fetching the tenant list.
    const sequence = ++verificationSequence.current;
    try {
      const res = await fetch('/admin/v1/tenants', {
        headers: { Authorization: `Bearer ${inputToken}` },
      });
      if (sequence !== verificationSequence.current) return null;
      if (!res.ok) {
        const msg = res.status === 401 ? '管理令牌不正确，请重新输入。' : `服务返回 ${res.status}`;
        setError(msg);
        setStage('error');
        setVerifiedToken('');
        setTenants(null);
        setSelected(null);
        setHint(msg);
        return null;
      }
      const data = (await res.json()) as { tenants: TenantInfo[] };
      if (sequence !== verificationSequence.current) return null;
      setTenants(data.tenants);
      setSelected((current) => {
        if (current && data.tenants.some((tenant) => tenant.tenant_id === current)) return current;
        if (existingTenant && data.tenants.some((tenant) => tenant.tenant_id === existingTenant.tenant_id)) {
          return existingTenant.tenant_id;
        }
        return data.tenants[0]?.tenant_id ?? null;
      });
      setStage('ready');
      setVerifiedToken(inputToken);
      setError('');
      setHint(`已加载 ${data.tenants.length} 个租户，选择一个进入控制台`);
      return data.tenants;
    } catch {
      if (sequence !== verificationSequence.current) return null;
      setError('无法连接服务，请确认后端已启动。');
      setStage('error');
      setVerifiedToken('');
      setTenants(null);
      setSelected(null);
      setHint('无法连接服务');
      return null;
    }
  }

  useEffect(() => {
    if (!existingToken) return;
    setStage('verifying');
    setHint('正在恢复已保存的登录态…');
    void verify(existingToken);
    // The persisted values are intentionally sampled once on page entry.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function onTokenBlur() {
    if (!token.trim() || stage === 'verifying') return;
    setStage('verifying');
    setHint('正在校验管理令牌…');
    await verify(token.trim());
  }

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (submitting) return;
    const trimmed = token.trim();
    if (!trimmed || !userId.trim()) return;
    setSubmitting(true);
    setError('');
    let list = tenants;
    if (stage !== 'ready' || verifiedToken !== trimmed || !list) {
      setStage('verifying');
      list = await verify(trimmed);
    }
    if (!list) {
      setSubmitting(false);
      return;
    }
    const tenantId = selected ?? list[0]?.tenant_id;
    const tenant = list.find((t) => t.tenant_id === tenantId);
    if (!tenant) {
      setError('请选择一个租户。');
      setSubmitting(false);
      return;
    }
    login(trimmed, tenant, userId.trim(), keepLogin);
    navigate('/console');
  }

  const submitEnabled = token.trim() !== '' && userId.trim() !== '' && !submitting;
  const service = (window as unknown as { __SERVICE__?: { version: string; commit: string } }).__SERVICE__;

  return (
    <main className="login-main">
      <section className="login-card">
        <header className="login-brand">
          <div className="login-brand-mark" aria-hidden="true">A</div>
          <div className="login-brand-text">
            <h1 className="login-title">IM 接入模拟控制台</h1>
            <p className="login-subtitle">多租户 Agent 平台 · 会话 / Trace / 指标</p>
          </div>
        </header>
        <p className="login-version">
          service v{service?.version ?? 'dev'} · commit {service?.commit ?? 'dev'}
        </p>

        <form className="form login-form" onSubmit={onSubmit} noValidate>
          <div className="form-field">
            <label className="form-label" htmlFor="admin-token">管理令牌</label>
            <div className="pwd-field">
              <input
                className="ui-input input-md pwd-input"
                id="admin-token"
                type={showToken ? 'text' : 'password'}
                placeholder="请输入本地 ADMIN_TOKEN"
                autoComplete="current-password"
                value={token}
                onChange={(e) => {
                  const next = e.target.value;
                  setToken(next);
                  if (next.trim() !== verifiedToken) {
                    verificationSequence.current += 1;
                    setVerifiedToken('');
                    setTenants(null);
                    setSelected(null);
                    setStage('idle');
                    setError('');
                    setHint('输入管理令牌后加载租户列表');
                  }
                }}
                onBlur={onTokenBlur}
              />
              <button
                className="pwd-toggle"
                type="button"
                aria-label="显示或隐藏管理令牌"
                tabIndex={-1}
                onClick={() => setShowToken(!showToken)}
              >
                {showToken ? '隐藏' : '显示'}
              </button>
            </div>
          </div>

          <div className="form-field">
            <label className="form-label" htmlFor="mock-user">模拟用户 ID</label>
            <input
              className="ui-input input-md"
              id="mock-user"
              type="text"
              placeholder="例如 alice"
              autoComplete="username"
              value={userId}
              onChange={(e) => setUserId(e.target.value)}
            />
          </div>

          <div className="form-field">
            <label
              className={keepLogin ? 'checkbox checked' : 'checkbox'}
              onClick={() => setKeepLogin(!keepLogin)}
            >
              <span className="checkbox-box">
                {keepLogin && (
                  <svg className="checkbox-icon" viewBox="0 0 14 14" fill="none">
                    <path d="M3 7l3 3 5-5" stroke="var(--color-white)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </span>
              <span className="checkbox-label">在这台设备上保持登录（保存于 localStorage）</span>
            </label>
          </div>

          <div className="login-error" role="alert" hidden={!error}>
            <span>{error}</span>
          </div>

          <div className="form-actions login-actions">
            <button type="submit" className="btn btn-primary btn-lg login-submit" disabled={!submitEnabled}>
              <span className="btn-label">进入控制台</span>
              <span aria-hidden="true">→</span>
            </button>
          </div>
        </form>

        <div className="tenant-section">
          <div className="tenant-head">
            <h2 className="tenant-title">选择租户</h2>
            <div className="tenant-head-actions">
              <button
                className="btn btn-primary btn-sm tenant-add-model"
                type="button"
                disabled={stage !== 'ready' || verifiedToken !== token.trim()}
                title={stage === 'ready' ? '接入 OpenAI 兼容的自定义模型' : '请先验证管理令牌'}
                onClick={() => setShowCustomModal(true)}
              >
                <span>添加自定义模型</span>
              </button>
              <button
                className="btn btn-secondary btn-sm tenant-refresh"
                type="button"
                disabled={stage === 'verifying' || !token.trim()}
                onClick={async () => {
                  setStage('verifying');
                  setHint('正在刷新租户列表…');
                  await verify(token.trim());
                }}
              >
                <span>刷新</span>
              </button>
            </div>
          </div>
          <p className="tenant-hint">{hint}</p>

          <div className="tenant-grid">
            {tenants?.map((tenant) => {
              const isSelected = selected === tenant.tenant_id;
              return (
                <div
                  key={tenant.tenant_id}
                  className={isSelected ? 'tenant-card selected' : 'tenant-card'}
                  tabIndex={0}
                  role="button"
                  aria-pressed={isSelected}
                  onClick={() => setSelected(tenant.tenant_id)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') setSelected(tenant.tenant_id);
                  }}
                >
                  <div className="tenant-name">{tenant.app.name}</div>
                  <div className="tenant-meta">
                    {tenant.tenant_id} · {tenant.model.provider}/{tenant.model.name}
                  </div>
                  <div className="tenant-channels">
                    {tenant.channels.map((ch) => (
                      <span
                        key={ch.type + ch.binding_id}
                        className={
                          ch.enabled
                            ? 'status-tag status-tag-fill status-tag-success'
                            : 'status-tag status-tag-fill status-tag-stop'
                        }
                      >
                        {ch.type}
                      </span>
                    ))}
                  </div>
                  <span className="tenant-check" aria-hidden="true">✓</span>
                </div>
              );
            })}
            {stage === 'verifying' && (
              <div className="tenant-hint">加载中…</div>
            )}
          </div>
        </div>

        {existingTenant && stage === 'ready' && verifiedToken === token.trim() && (
          <p className="tenant-hint" style={{ marginTop: 12 }}>
            当前登录态已验证（{existingTenant.app.name}），可直接进入控制台或切换租户。
          </p>
        )}
      </section>

      {showCustomModal && (
        <CustomModelModal
          token={token.trim()}
          onClose={() => setShowCustomModal(false)}
          onCreated={async (result: CustomModelResponse) => {
            setShowCustomModal(false);
            setHint(`模型 ${result.display_name} 配置已保存，正在刷新租户列表…`);
            setStage('verifying');
            const list = await verify(token.trim());
            if (list) {
              setSelected(result.tenant_id);
              setHint(`已选择 ${result.display_name}（${result.tenant_id}）；进入控制台发送消息时会实际调用该模型。`);
            }
          }}
        />
      )}
    </main>
  );
}
