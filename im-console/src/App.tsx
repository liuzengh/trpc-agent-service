// Auth-guarded application shell: topbar + routed content area.
import { useEffect, useState } from 'react';
import { Link, Navigate, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from './stores/auth';
import { useChatStore } from './stores/chat';
import './styles/console.css';

export default function App() {
  const token = useAuthStore((s) => s.token);
  const tenant = useAuthStore((s) => s.tenant);
  const userId = useAuthStore((s) => s.userId);
  const logout = useAuthStore((s) => s.logout);
  const load = useChatStore((s) => s.load);
  const navigate = useNavigate();
  const location = useLocation();
  const [health, setHealth] = useState<'ok' | 'down'>('ok');
  const [sidebarOpen, setSidebarOpen] = useState(false);

  useEffect(() => {
    if (tenant && userId) load(tenant.tenant_id, userId);
  }, [tenant, userId, load]);

  useEffect(() => {
    const check = () => {
      fetch('/healthz')
        .then((res) => setHealth(res.ok ? 'ok' : 'down'))
        .catch(() => setHealth('down'));
    };
    check();
    const timer = setInterval(check, 30_000);
    return () => clearInterval(timer);
  }, []);

  if (!token || !tenant || !userId) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }

  const modelLabel = `${tenant.model.provider}/${tenant.model.name}`;
  const onMonitor = location.pathname.startsWith('/monitor');

  return (
    <main className="im-app">
      <header className="im-topbar">
        <div className="im-topbar__left">
          <button
            className="im-hamburger"
            aria-label="菜单"
            onClick={() => setSidebarOpen(!sidebarOpen)}
          >
            <span className="im-hamburger__line" />
            <span className="im-hamburger__line" />
            <span className="im-hamburger__line" />
          </button>
          <div className="im-brand">
            <div className="im-brand__mark">IM</div>
            <span className="im-brand__title">IM 接入模拟控制台</span>
          </div>
        </div>
        <div className="im-topbar__center">
          <span className="im-tenant-badge">{tenant.app.name}</span>
          <span className="im-tenant-meta">
            {tenant.tenant_id} · {modelLabel}
          </span>
        </div>
        <div className="im-topbar__right">
          <div className="im-health" data-state={health}>
            <span className="im-health__dot" />
            <span className="im-health__text">{health === 'ok' ? '服务正常' : '服务异常'}</span>
          </div>
          <Link className="btn btn-text btn-md" to="/console">
            会话控制台
          </Link>
          <button
            className="btn btn-text btn-md"
            type="button"
            onClick={() => (onMonitor ? navigate('/console') : navigate('/monitor'))}
          >
            {onMonitor ? '返回控制台' : '监控面板'}
          </button>
          <button className="btn btn-text btn-md" type="button" onClick={() => navigate('/login')}>
            切换租户
          </button>
          <button
            className="btn btn-text btn-md"
            type="button"
            onClick={() => {
              logout();
              navigate('/login');
            }}
          >
            退出登录
          </button>
        </div>
      </header>
      <div className="im-body">
        <div className={sidebarOpen ? 'im-overlay show' : 'im-overlay'} onClick={() => setSidebarOpen(false)} />
        <Outlet context={{ sidebarOpen, closeSidebar: () => setSidebarOpen(false) }} />
      </div>
    </main>
  );
}
