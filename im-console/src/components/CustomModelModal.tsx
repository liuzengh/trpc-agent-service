// Custom model modal: provisions an OpenAI-compatible model as a new tenant
// via POST /admin/v1/tenants/custom-model. Called from the login page before
// the auth token reaches the store, so the token is passed in as a prop.
import { useEffect, useRef, useState } from 'react';
import type { CustomModelRequest, CustomModelResponse } from '../types';
import '../styles/custom-model.css';

const API_FORMATS = [
  { value: 'openai-chat-completions', label: 'OpenAI Chat Completions 格式' },
];

const MAX_DISPLAY_NAME = 32;

interface Props {
  token: string;
  onClose: () => void;
  onCreated: (result: CustomModelResponse) => void;
}

function EyeIcon({ open }: { open: boolean }) {
  return open ? (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
      <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z" />
      <circle cx="12" cy="12" r="3" />
    </svg>
  ) : (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
      <path d="M17.94 17.94A10.5 10.5 0 0 1 12 19c-7 0-11-7-11-7a19.8 19.8 0 0 1 5.06-5.94M9.9 4.24A9.6 9.6 0 0 1 12 4c7 0 11 7 11 7a19.8 19.8 0 0 1-3.22 4.31M1 1l22 22" />
    </svg>
  );
}

export default function CustomModelModal({ token, onClose, onCreated }: Props) {
  const [apiFormat, setApiFormat] = useState(API_FORMATS[0].value);
  const [baseUrl, setBaseUrl] = useState('');
  const [fullUrl, setFullUrl] = useState(false);
  const [modelId, setModelId] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [apiKeyEnv, setApiKeyEnv] = useState('');
  const [showKey, setShowKey] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const firstInputRef = useRef<HTMLSelectElement>(null);

  useEffect(() => {
    firstInputRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const trimmedBase = baseUrl.trim();
  const trimmedModelId = modelId.trim();
  const trimmedKey = apiKeyEnv.trim();
  const submitEnabled =
    !submitting && trimmedBase !== '' && trimmedModelId !== '' && trimmedKey !== '';

  function validate(): string | null {
    if (trimmedBase === '') return '请填写自定义请求地址。';
    try {
      const parsed = new URL(trimmedBase);
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        return '请求地址必须是 http(s) 开头的完整地址。';
      }
      if (parsed.username || parsed.password || trimmedBase.includes('?') || trimmedBase.includes('#')) {
        return '请求地址不能包含账号、密码、查询参数或锚点。';
      }
      const path = parsed.pathname.replace(/\/+$/, '');
      if (fullUrl && !path.endsWith('/chat/completions')) {
        return '完整 URL 必须以 /chat/completions 结尾。';
      }
      if (!fullUrl && path.endsWith('/chat/completions')) {
        return '该地址是完整调用 URL，请开启「完整URL」。';
      }
    } catch {
      return '请求地址格式不正确，请填写完整的服务端点地址。';
    }
    if (trimmedModelId === '') return '请填写模型 ID。';
    if ([...displayName].length > MAX_DISPLAY_NAME) {
      return `模型展示名称不能超过 ${MAX_DISPLAY_NAME} 个字符。`;
    }
    if (trimmedKey === '') return '请填写 API Key 的环境变量/secret 引用。';
    return null;
  }

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (submitting) return;
    const problem = validate();
    if (problem) {
      setError(problem);
      return;
    }
    setError('');
    setSubmitting(true);
    try {
      const body: CustomModelRequest = {
        api_format: apiFormat,
        base_url: trimmedBase.replace(/\/+$/, ''),
        full_url: fullUrl,
        model_id: trimmedModelId,
        display_name: displayName.trim(),
        api_key_env: trimmedKey,
      };
      const res = await fetch('/admin/v1/tenants/custom-model', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        let detail = `服务返回 ${res.status}`;
        if (res.status === 401) detail = '管理令牌无效，请重新输入令牌。';
        if (res.status === 409) detail = '该模型已接入，请勿重复添加。';
        try {
          const text = await res.text();
          if (text) detail = detail === `服务返回 ${res.status}` ? text : detail;
        } catch {
          /* ignore non-text body */
        }
        setError(detail);
        return;
      }
      onCreated((await res.json()) as CustomModelResponse);
    } catch {
      setError('无法连接服务，请确认后端已启动。');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      className="cm-overlay"
      role="dialog"
      aria-modal="true"
      aria-label="自定义模型"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <form className="modal modal-md cm-modal" onSubmit={onSubmit} noValidate>
        <header className="modal-header cm-header">
          <h2 className="modal-title">自定义模型</h2>
          <button
            className="modal-close cm-close"
            type="button"
            aria-label="关闭"
            onClick={onClose}
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
              <path d="M18 6L6 18M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </header>

        <div className="modal-body cm-body">
          <div className="form-field">
            <label className="form-label" htmlFor="cm-api-format">
              <span className="cm-required" aria-hidden="true">*</span>API 格式
            </label>
            <select
              ref={firstInputRef}
              className="ui-input input-md cm-select"
              id="cm-api-format"
              value={apiFormat}
              onChange={(e) => setApiFormat(e.target.value)}
            >
              {API_FORMATS.map((f) => (
                <option key={f.value} value={f.value}>{f.label}</option>
              ))}
            </select>
          </div>

          <div className="form-field">
            <label className="form-label" htmlFor="cm-base-url">
              <span className="cm-required" aria-hidden="true">*</span>自定义请求地址
            </label>
            <div className="cm-url-row">
              <input
                className="ui-input input-md cm-url-input"
                id="cm-base-url"
                type="text"
                placeholder="https://api.openai.com/v1"
                autoComplete="off"
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
              />
              <label className="switch cm-url-switch" title="完整 URL">
                <input
                  type="checkbox"
                  checked={fullUrl}
                  onChange={(e) => setFullUrl(e.target.checked)}
                />
                <span className="switch-slider" aria-hidden="true" />
                <span className="cm-url-switch-label">完整URL</span>
              </label>
            </div>
            <p className="cm-hint">
              {fullUrl
                ? '完整 URL 必须以 /chat/completions 结尾，后端会按原端点调用。'
                : '请填写兼容 OpenAI API 的基础地址；/chat/completions 会自动补充。'}
            </p>
          </div>

          <div className="form-field">
            <label className="form-label" htmlFor="cm-model-id">
              <span className="cm-required" aria-hidden="true">*</span>模型 ID
            </label>
            <input
              className="ui-input input-md"
              id="cm-model-id"
              type="text"
              placeholder="输入模型ID"
              autoComplete="off"
              value={modelId}
              onChange={(e) => setModelId(e.target.value)}
            />
          </div>

          <div className="form-field">
            <div className="cm-name-row">
              <label className="form-label" htmlFor="cm-display-name">模型展示名称</label>
              <span className="cm-counter">{[...displayName].length}/{MAX_DISPLAY_NAME}</span>
            </div>
            <input
              className="ui-input input-md"
              id="cm-display-name"
              type="text"
              placeholder="可选"
              autoComplete="off"
              maxLength={MAX_DISPLAY_NAME * 2}
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
            />
            <p className="cm-hint">在模型列表中展示的名称，未设置时默认显示 Model ID。</p>
          </div>

          <div className="form-field">
            <label className="form-label" htmlFor="cm-api-key">
              <span className="cm-required" aria-hidden="true">*</span>API Key 引用
            </label>
            <div className="pwd-field cm-key-field">
              <input
                className="ui-input input-md cm-key-input"
                id="cm-api-key"
                type="text"
                placeholder="例如 CUSTOM_MODEL_API_KEY"
                autoComplete="off"
                value={apiKeyEnv}
                onChange={(e) => setApiKeyEnv(e.target.value)}
              />
              <button
                className="pwd-toggle cm-key-toggle"
                type="button"
                aria-label="API Key 环境变量"
                tabIndex={-1}
                onClick={() => setShowKey(!showKey)}
              >
                <EyeIcon open={showKey} />
              </button>
            </div>
          </div>

          <div className="login-error cm-error" role="alert" hidden={!error}>
            <span>{error}</span>
          </div>
        </div>

        <footer className="modal-footer cm-footer">
          <button type="button" className="btn btn-secondary btn-md" onClick={onClose} disabled={submitting}>
            取消
          </button>
          <button type="submit" className="btn btn-primary btn-md" disabled={!submitEnabled}>
            {submitting ? '接入中…' : '添加模型'}
          </button>
        </footer>
      </form>
    </div>
  );
}
