// Monitor page: live /metrics aggregates + dynamic SVG charts driven by the
// console session's real message history.
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchMetrics, sumMetric } from '../api/metrics';
import { useAuthStore } from '../stores/auth';
import { useChatStore } from '../stores/chat';
import type { ChatMessage, MetricRow } from '../types';
import { formatTokens } from '../utils/format';
import '../styles/monitor.css';

const AUTO_REFRESH_MS = 15_000;

interface SessionStats {
  agentMessages: ChatMessage[];
  perConversation: { title: string; count: number }[];
}

function collectSessionStats(messages: ChatMessage[], conversations: { title: string; messages: ChatMessage[] }[]): SessionStats {
  const perConversation = conversations
    .map((c) => ({ title: c.title, count: c.messages.length }))
    .filter((c) => c.count > 0)
    .sort((a, b) => b.count - a.count)
    .slice(0, 6);
  return { agentMessages: messages.filter((m) => m.result), perConversation };
}

export default function MonitorPage() {
  const navigate = useNavigate();
  const tenant = useAuthStore((s) => s.tenant);
  const conversations = useChatStore((s) => s.conversations);

  const [rows, setRows] = useState<MetricRow[] | null>(null);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    try {
      const data = await fetchMetrics();
      setRows(data);
      setUpdatedAt(Date.now());
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (!autoRefresh) return;
    const timer = setInterval(() => void refresh(), AUTO_REFRESH_MS);
    return () => clearInterval(timer);
  }, [autoRefresh, refresh]);

  const agentMessages = useMemo(
    () => conversations.flatMap((c) => c.messages).filter((m) => m.result),
    [conversations],
  );
  const sessionStats = useMemo(
    () => collectSessionStats(agentMessages, conversations),
    [agentMessages, conversations],
  );

  if (!tenant) return null;
  const tenantId = tenant.tenant_id;

  // Server-side aggregates for the metric cards.
  const requestsTenant = rows ? sumMetric(rows, 'agent_requests_total', { tenant: tenantId }) : 0;
  const requestsGlobal = rows ? sumMetric(rows, 'agent_requests_total') : 0;
  const failuresTenant = rows ? sumMetric(rows, 'agent_requests_total', { tenant: tenantId, result: 'error' }) : 0;
  const tokensTenant = rows ? sumMetric(rows, 'model_tokens_total', { tenant: tenantId }) : 0;
  const costTenant = rows ? sumMetric(rows, 'tenant_cost_usd_total', { tenant: tenantId }) : 0;
  const latencySum = rows ? sumMetric(rows, 'agent_request_latency_seconds_sum', { tenant: tenantId }) : 0;
  const latencyCount = rows ? sumMetric(rows, 'agent_request_latency_seconds_count', { tenant: tenantId }) : 0;
  const cacheHits = rows ? sumMetric(rows, 'result_cache_hits_total', { tenant: tenantId }) : 0;
  const avgLatencyMS = latencyCount > 0 ? (latencySum / latencyCount) * 1000 : 0;
  const hitRate = requestsTenant > 0 ? cacheHits / requestsTenant : 0;

  const updatedText = updatedAt
    ? new Date(updatedAt).toLocaleTimeString('zh-CN', { hour12: false })
    : '刚刚';

  return (
    <main>
      <div className="monitor-page">
        <header className="monitor-header">
          <div className="monitor-header__left">
            <div className="monitor-title-row">
              <h1 className="monitor-title">性能监控</h1>
              <span className="status-tag status-tag-fill status-tag-processing">实时</span>
              <span className="monitor-tenant">{tenant.app.name}</span>
            </div>
            <p className="monitor-caption">
              已更新 · {updatedText} · 数据来源 /metrics 端点{error ? ` · 拉取失败：${error}` : ''}
            </p>
          </div>
          <div className="monitor-header__actions">
            <label
              className={autoRefresh ? 'checkbox checked' : 'checkbox'}
              onClick={() => setAutoRefresh(!autoRefresh)}
            >
              <span className="checkbox-box">
                {autoRefresh && (
                  <svg className="checkbox-icon" viewBox="0 0 14 14" fill="none">
                    <path d="M3 7l3 3 5-5" stroke="var(--color-white)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </span>
              <span className="checkbox-label">自动刷新（15s）</span>
            </label>
            <button className="btn btn-text btn-sm" type="button" onClick={() => void refresh()}>
              <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9" />
                <path d="M13.5 2.5V5H11" />
              </svg>
              刷新
            </button>
            <button className="btn btn-icon btn-sm" type="button" aria-label="返回" onClick={() => navigate('/console')}>
              <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M4 4l8 8M12 4l-8 8" />
              </svg>
            </button>
          </div>
        </header>

        <section className="metric-grid">
          <div className="metric-card">
            <div className="metric-card__top">
              <span className="metric-label">消息总数（租户）</span>
              <span className="metric-icon metric-icon--primary">
                <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M3 5.5A1.5 1.5 0 0 1 4.5 4h7A1.5 1.5 0 0 1 13 5.5v3A1.5 1.5 0 0 1 11.5 10H7l-2.5 2.5V10H4.5A1.5 1.5 0 0 1 3 8.5v-3z" />
                </svg>
              </span>
            </div>
            <div className="metric-value">{Math.round(requestsTenant)}</div>
            <div className="metric-sub">全局 {Math.round(requestsGlobal)} 次 · 本地会话 {agentMessages.length} 条回复</div>
          </div>

          <div className="metric-card">
            <div className="metric-card__top">
              <span className="metric-label">token 消耗</span>
              <span className="metric-icon metric-icon--primary">
                <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="4" y="4" width="8" height="8" rx="1.5" />
                  <path d="M6.5 2v2M9.5 2v2M6.5 12v2M9.5 12v2M2 6.5h2M2 9.5h2M12 6.5h2M12 9.5h2" />
                </svg>
              </span>
            </div>
            <div className="metric-value">
              {tokensTenant >= 1000 ? formatTokens(tokensTenant) : Math.round(tokensTenant)}
            </div>
            <div className="metric-sub">服务端累计 · 租户 {tenantId}</div>
          </div>

          <div className="metric-card">
            <div className="metric-card__top">
              <span className="metric-label">累计成本</span>
              <span className="metric-icon metric-icon--primary">
                <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M8 2v12" />
                  <path d="M11 5c0-1.2-1.3-2-3-2s-3 .8-3 2 1.3 1.8 3 2.2 3 1 3 2.2-1.3 2-3 2-3-.8-3-2" />
                </svg>
              </span>
            </div>
            <div className="metric-value">${costTenant.toFixed(3)}</div>
            <div className="metric-sub">按租户单价估算</div>
          </div>

          <div className="metric-card">
            <div className="metric-card__top">
              <span className="metric-label">平均延迟</span>
              <span className="metric-icon metric-icon--warning">
                <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="8" cy="8" r="5.5" />
                  <path d="M8 5v3.2l2 1.3" />
                </svg>
              </span>
            </div>
            <div className="metric-value">
              {Math.round(avgLatencyMS)}
              <span className="metric-unit">ms</span>
            </div>
            <div className="metric-sub">{Math.round(latencyCount)} 次请求</div>
          </div>

          <div className="metric-card">
            <div className="metric-card__top">
              <span className="metric-label">缓存命中率</span>
              <span className="metric-icon metric-icon--success">
                <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <ellipse cx="8" cy="4.2" rx="4.2" ry="1.7" />
                  <path d="M3.8 4.2v3.6c0 .9 1.9 1.7 4.2 1.7s4.2-.8 4.2-1.7V4.2" />
                  <path d="M3.8 7.8v3.6c0 .9 1.9 1.7 4.2 1.7s4.2-.8 4.2-1.7V7.8" />
                </svg>
              </span>
            </div>
            <div className="metric-value">
              {Math.round(hitRate * 100)}
              <span className="metric-unit">%</span>
            </div>
            <div className="metric-sub">
              {Math.round(cacheHits)}/{Math.round(requestsTenant)} 次命中
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-card__top">
              <span className="metric-label">失败消息</span>
              <span className="metric-icon metric-icon--danger">
                <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M8 2.6 14.2 13H1.8L8 2.6z" />
                  <path d="M8 6.8v2.6" />
                  <path d="M8 11v.4" />
                </svg>
              </span>
            </div>
            <div className="metric-value metric-value--danger">{Math.round(failuresTenant)}</div>
            <div className="metric-sub">agent_requests_total{`{result="error"}`}</div>
          </div>
        </section>

        <section className="chart-grid">
          <TokenStackChart messages={sessionStats.agentMessages} />
          <LatencyTrendChart messages={sessionStats.agentMessages} />
          <CacheDonut hits={Math.round(cacheHits)} total={Math.round(requestsTenant)} />
          <ConversationBars data={sessionStats.perConversation} />
        </section>

        <section className="server-metrics">
          <div className="server-metrics__head">
            <h3 className="section-title">服务端指标（/metrics 摘要）</h3>
            <span className="section-sub">Prometheus 原始指标 · 当前租户与全局聚合</span>
          </div>
          <div className="table-wrapper">
            <table className="table table-striped">
              <thead>
                <tr>
                  <th>指标名</th>
                  <th className="col-scope">范围</th>
                  <th className="col-value">数值</th>
                </tr>
              </thead>
              <tbody>
                {rows === null && (
                  <tr>
                    <td colSpan={3} style={{ padding: 16, color: 'var(--color-text-3)' }}>
                      {error ? `拉取失败：${error}` : '加载中…'}
                    </td>
                  </tr>
                )}
                {rows !== null && <MetricTableRows rows={rows} tenantId={tenantId} />}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </main>
  );
}

function MetricTableRows({ rows, tenantId }: { rows: MetricRow[]; tenantId: string }) {
  // Show the most relevant series: per-tenant rows first, then unlabeled globals.
  const interesting = rows.filter(
    (row) =>
      row.labels.tenant === tenantId ||
      (!row.labels.tenant && !row.name.startsWith('agent_request_latency')),
  );
  if (interesting.length === 0) {
    return (
      <tr>
        <td colSpan={3} style={{ padding: 16, color: 'var(--color-text-3)' }}>
          暂无指标（服务刚启动或还没有请求）。
        </td>
      </tr>
    );
  }
  return (
    <>
      {interesting.slice(0, 20).map((row, i) => {
        const isTenant = row.labels.tenant === tenantId;
        const labelText = Object.entries(row.labels)
          .map(([k, v]) => `${k}="${v}"`)
          .join(',');
        return (
          <tr key={`${row.name}{${labelText}}-${i}`}>
            <td className="col-name">
              {row.name}
              {labelText && <span className="col-args">{`{${labelText}}`}</span>}
            </td>
            <td className="col-scope">
              <span
                className={
                  isTenant
                    ? 'status-tag status-tag-fill status-tag-processing'
                    : 'status-tag status-tag-fill status-tag-stop'
                }
              >
                {isTenant ? '租户' : '全局'}
              </span>
            </td>
            <td className="col-value">{Number.isInteger(row.value) ? row.value : row.value.toFixed(3)}</td>
          </tr>
        );
      })}
    </>
  );
}

/* ============ SVG charts (plot area: x 36..468, y 16..232) ============ */

const PLOT = { left: 36, right: 468, top: 16, bottom: 232 };

function TokenStackChart({ messages }: { messages: ChatMessage[] }) {
  const recent = messages.slice(-12);
  const max = Math.max(
    1,
    ...recent.map((m) => (m.result?.prompt_tokens ?? 0) + (m.result?.completion_tokens ?? 0)),
  );
  const step = recent.length > 0 ? (PLOT.right - PLOT.left) / recent.length : 0;
  const barW = Math.min(22, step * 0.6);
  return (
    <div className="chart-card">
      <div className="chart-card__head">
        <h3 className="chart-card__title">最近消息 token 消耗</h3>
        <div className="chart-legend">
          <span className="chart-legend__item"><i className="dot dot--input" />输入</span>
          <span className="chart-legend__item"><i className="dot dot--output" />输出</span>
        </div>
      </div>
      <div className="chart-card__body">
        {recent.length === 0 ? (
          <EmptyChart text="控制台还没有消息，去会话页发一条吧" />
        ) : (
          <svg viewBox="0 0 480 260" role="img" aria-label="最近消息 token 消耗堆叠柱状图">
            <g className="ch-grid">
              <line x1={PLOT.left} y1={PLOT.top} x2={PLOT.right} y2={PLOT.top} />
              <line x1={PLOT.left} y1={88} x2={PLOT.right} y2={88} />
              <line x1={PLOT.left} y1={160} x2={PLOT.right} y2={160} />
            </g>
            <line className="ch-baseline" x1={PLOT.left} y1={PLOT.bottom} x2={PLOT.right} y2={PLOT.bottom} />
            <g className="ch-label" textAnchor="end">
              <text x={30} y={20}>{Math.round(max)}</text>
              <text x={30} y={92}>{Math.round((max * 2) / 3)}</text>
              <text x={30} y={164}>{Math.round(max / 3)}</text>
              <text x={30} y={236}>0</text>
            </g>
            <g className="ch-yunit">
              <text x={30} y={8} className="ch-label" textAnchor="end">tokens</text>
            </g>
            {recent.map((message, i) => {
              const cx = PLOT.left + step * i + step / 2;
              const input = message.result?.prompt_tokens ?? 0;
              const output = message.result?.completion_tokens ?? 0;
              const inputH = (input / max) * (PLOT.bottom - PLOT.top);
              const outputH = (output / max) * (PLOT.bottom - PLOT.top);
              const x = cx - barW / 2;
              return (
                <g key={message.id}>
                  <path
                    d={`M${x},${PLOT.bottom - inputH} H${x + barW} V${PLOT.bottom - 3} Q${x + barW},${PLOT.bottom} ${x + barW - 3},${PLOT.bottom} H${x + 3} Q${x},${PLOT.bottom} ${x},${PLOT.bottom - 3} Z`}
                    fill="var(--chart-1)"
                  >
                    <title>{`消息${i + 1} · 输入 ${input}`}</title>
                  </path>
                  <path
                    d={`M${x},${PLOT.bottom - inputH - outputH + 3} Q${x},${PLOT.bottom - inputH - outputH} ${x + 3},${PLOT.bottom - inputH - outputH} H${x + barW - 3} Q${x + barW},${PLOT.bottom - inputH - outputH} ${x + barW},${PLOT.bottom - inputH - outputH + 3} V${PLOT.bottom - inputH} H${x} Z`}
                    fill="var(--violet-6)"
                  >
                    <title>{`消息${i + 1} · 输入 ${input} · 输出 ${output}`}</title>
                  </path>
                </g>
              );
            })}
            <g className="ch-xlabel" textAnchor="middle">
              {recent.map((_, i) => (
                <text key={i} x={PLOT.left + step * i + step / 2} y={248}>
                  {i + 1}
                </text>
              ))}
            </g>
          </svg>
        )}
      </div>
    </div>
  );
}

function LatencyTrendChart({ messages }: { messages: ChatMessage[] }) {
  const recent = messages.slice(-12);
  const max = Math.max(1, ...recent.map((m) => m.result?.latency_ms ?? 0));
  const avg = recent.length
    ? recent.reduce((s, m) => s + (m.result?.latency_ms ?? 0), 0) / recent.length
    : 0;
  const step = recent.length > 1 ? (PLOT.right - PLOT.left) / (recent.length - 1) : 0;
  const yOf = (v: number) => PLOT.bottom - (v / max) * (PLOT.bottom - PLOT.top);
  const points = recent.map((m, i) => ({ x: PLOT.left + step * i, y: yOf(m.result?.latency_ms ?? 0), ms: m.result?.latency_ms ?? 0 }));
  const avgY = yOf(avg);
  return (
    <div className="chart-card">
      <div className="chart-card__head">
        <h3 className="chart-card__title">端到端延迟趋势</h3>
        <div className="chart-legend">
          <span className="chart-legend__item"><i className="dot dot--input" />延迟</span>
          <span className="chart-legend__item"><i className="dot dot--avg" />均值</span>
        </div>
      </div>
      <div className="chart-card__body">
        {recent.length === 0 ? (
          <EmptyChart text="控制台还没有消息，去会话页发一条吧" />
        ) : (
          <svg viewBox="0 0 480 260" role="img" aria-label="端到端延迟趋势折线图">
            <g className="ch-grid">
              <line x1={PLOT.left} y1={PLOT.top} x2={PLOT.right} y2={PLOT.top} />
              <line x1={PLOT.left} y1={88} x2={PLOT.right} y2={88} />
              <line x1={PLOT.left} y1={160} x2={PLOT.right} y2={160} />
            </g>
            <line className="ch-baseline" x1={PLOT.left} y1={PLOT.bottom} x2={PLOT.right} y2={PLOT.bottom} />
            <g className="ch-label" textAnchor="end">
              <text x={30} y={20}>{Math.round(max)}</text>
              <text x={30} y={92}>{Math.round((max * 2) / 3)}</text>
              <text x={30} y={164}>{Math.round(max / 3)}</text>
              <text x={30} y={236}>0</text>
            </g>
            <g className="ch-yunit">
              <text x={30} y={8} className="ch-label" textAnchor="end">ms</text>
            </g>
            <line className="ch-avg" x1={PLOT.left} y1={avgY} x2={PLOT.right} y2={avgY} strokeDasharray="4 4" />
            <text className="ch-avg-label" x={464} y={Math.max(avgY - 6, 12)} textAnchor="end">
              均值 {Math.round(avg)} ms
            </text>
            <path
              className="ch-area"
              d={`${points.map((p) => `${p.x},${p.y}`).join(' L')} L${points[points.length - 1].x},${PLOT.bottom} L${points[0].x},${PLOT.bottom} Z`}
            />
            <polyline className="ch-line" points={points.map((p) => `${p.x},${p.y}`).join(' ')} />
            <g className="ch-dots">
              {points.map((p, i) => (
                <circle key={i} cx={p.x} cy={p.y} r={3}>
                  <title>{`消息${i + 1} · ${p.ms} ms`}</title>
                </circle>
              ))}
            </g>
            <g className="ch-xlabel" textAnchor="middle">
              {points.map((_, i) => (
                <text key={i} x={PLOT.left + step * i} y={248}>
                  {i + 1}
                </text>
              ))}
            </g>
          </svg>
        )}
      </div>
    </div>
  );
}

function CacheDonut({ hits, total }: { hits: number; total: number }) {
  const misses = Math.max(0, total - hits);
  const rate = total > 0 ? hits / total : 0;
  const circumference = 2 * Math.PI * 65;
  const hitArc = circumference * rate;
  return (
    <div className="chart-card">
      <div className="chart-card__head">
        <h3 className="chart-card__title">结果缓存命中</h3>
        <div className="chart-legend">
          <span className="chart-legend__item"><i className="dot dot--hit" />命中</span>
          <span className="chart-legend__item"><i className="dot dot--miss" />未命中</span>
        </div>
      </div>
      <div className="chart-card__body">
        <svg viewBox="0 0 480 240" role="img" aria-label="结果缓存命中环形图">
          <g transform="rotate(-90 110 120)">
            <circle cx="110" cy="120" r="65" fill="none" stroke="var(--color-border)" strokeWidth="30" strokeDasharray={`${circumference - hitArc} ${circumference}`} strokeDashoffset={-hitArc} />
            <circle cx="110" cy="120" r="65" fill="none" stroke="var(--color-success)" strokeWidth="30" strokeDasharray={`${hitArc} ${circumference}`} />
          </g>
          <text x="110" y="118" textAnchor="middle" className="donut-pct">
            {Math.round(rate * 100)}%
          </text>
          <text x="110" y="140" textAnchor="middle" className="donut-sub">
            命中率
          </text>
          <g className="ch-legend">
            <rect x="240" y="80" width="12" height="12" rx="2" fill="var(--color-success)" />
            <text x="260" y="90" className="ch-legend-label">命中</text>
            <text x="456" y="90" className="ch-legend-value" textAnchor="end">{hits} 次</text>
            <rect x="240" y="110" width="12" height="12" rx="2" fill="var(--color-border)" />
            <text x="260" y="120" className="ch-legend-label">未命中</text>
            <text x="456" y="120" className="ch-legend-value" textAnchor="end">{misses} 次</text>
            <line x1="240" y1="132" x2="456" y2="132" stroke="var(--color-border)" strokeWidth="1" />
            <text x="260" y="150" className="ch-legend-label ch-legend-muted">总计</text>
            <text x="456" y="150" className="ch-legend-value ch-legend-muted" textAnchor="end">
              {total} 次
            </text>
          </g>
        </svg>
      </div>
    </div>
  );
}

function ConversationBars({ data }: { data: { title: string; count: number }[] }) {
  const max = Math.max(1, ...data.map((d) => d.count));
  return (
    <div className="chart-card">
      <div className="chart-card__head">
        <h3 className="chart-card__title">各会话消息量</h3>
        <span className="chart-card__hint">按会话聚合 · 单位 条</span>
      </div>
      <div className="chart-card__body">
        {data.length === 0 ? (
          <EmptyChart text="暂无会话数据" />
        ) : (
          <svg viewBox="0 0 480 240" role="img" aria-label="各会话消息量横向柱状图">
            <g className="hbar-track">
              {data.map((_, i) => (
                <rect key={i} x="140" y={10 + i * 40} width="300" height="20" rx="4" />
              ))}
            </g>
            <g fill="var(--chart-1)">
              {data.map((d, i) => (
                <rect key={i} x="140" y={10 + i * 40} width={(d.count / max) * 300} height="20" rx="4">
                  <title>{`${d.title} · ${d.count} 条`}</title>
                </rect>
              ))}
            </g>
            <g className="hbar-name" textAnchor="end">
              {data.map((d, i) => (
                <text key={i} x="132" y={24 + i * 40}>
                  {d.title.length > 8 ? d.title.slice(0, 8) + '…' : d.title}
                </text>
              ))}
            </g>
            <g className="hbar-value" textAnchor="start">
              {data.map((d, i) => (
                <text key={i} x={144 + (d.count / max) * 300} y={24 + i * 40}>
                  {d.count}
                </text>
              ))}
            </g>
          </svg>
        )}
      </div>
    </div>
  );
}

function EmptyChart({ text }: { text: string }) {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: 200,
        color: 'var(--color-text-3)',
        fontSize: 12,
      }}
    >
      {text}
    </div>
  );
}
