// Message detail page: renders a single agent reply with its real trace
// timeline, tool governance decisions, request/response JSON trees and metrics.
import { useMemo, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { JsonTree } from '../components/JsonTree';
import { useAuthStore } from '../stores/auth';
import { findMessage, useChatStore } from '../stores/chat';
import type { StageTrace } from '../types';
import { formatCost, formatDateTime } from '../utils/format';
import '../styles/detail.css';

type TabKey = 'overview' | 'request' | 'response' | 'trace' | 'metrics';

const TABS: { key: TabKey; label: string }[] = [
  { key: 'overview', label: '概览' },
  { key: 'request', label: '请求' },
  { key: 'response', label: '响应' },
  { key: 'trace', label: 'Trace' },
  { key: 'metrics', label: '指标' },
];

/** Longest-stage highlight threshold (primary bar). */
function isPrimaryStage(stages: StageTrace[], stage: StageTrace): boolean {
  const max = stages.reduce((m, s) => Math.max(m, s.duration_ms), 0);
  return stage.duration_ms === max && max > 0;
}

function TimelineChart({ stages }: { stages: StageTrace[] }) {
  const total = stages.reduce(
    (max, s) => Math.max(max, s.start_ms + s.duration_ms),
    0,
  );
  const primary = stages.reduce((m, s) => Math.max(m, s.duration_ms), 0);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((p) => Math.round(total * p));
  return (
    <div className="timeline-chart">
      <div className="timeline-ruler-row">
        <div className="stage-name-col" />
        <div className="ruler-track">
          {ticks.map((tick, i) => (
            <span key={i} className="ruler-tick" style={{ left: `${i * 25}%` }}>
              {tick}ms
            </span>
          ))}
        </div>
        <div className="stage-dur-col" />
      </div>
      {stages.map((stage) => {
        const left = total > 0 ? (stage.start_ms / total) * 100 : 0;
        const width = total > 0 ? (stage.duration_ms / total) * 100 : 0;
        return (
          <div key={stage.name} className="timeline-row">
            <div className="stage-name-col">
              <span className="stage-name">{stage.name}</span>
            </div>
            <div className="stage-bar-track">
              <div
                className={
                  isPrimaryStage(stages, stage) ? 'stage-bar stage-bar-primary' : 'stage-bar'
                }
                style={{
                  // Keep sub-pixel stages visible with a 0.16% minimum.
                  ['--bar-left' as string]: `${left}%`,
                  ['--bar-width' as string]: `${Math.max(width, 0.16)}%`,
                }}
                title={`start: ${stage.start_ms}ms, duration: ${stage.duration_ms}ms`}
              />
            </div>
            <div className="stage-dur-col">
              <span className="stage-duration">{stage.duration_ms} ms</span>
            </div>
          </div>
        );
      })}
      <p className="stage-note">
        总计 {total} ms · 最长阶段 {primary} ms · {stages.length} 个处理阶段
      </p>
    </div>
  );
}

export default function MessageDetailPage() {
  const { messageId } = useParams<{ messageId: string }>();
  const conversations = useChatStore((s) => s.conversations);
  const tenant = useAuthStore((s) => s.tenant);
  const userId = useAuthStore((s) => s.userId);
  const [tab, setTab] = useState<TabKey>('trace');

  const found = useMemo(
    () => (messageId ? findMessage(conversations, messageId) : null),
    [conversations, messageId],
  );

  if (!found) {
    return (
      <main>
        <div className="page-container">
          <header className="page-header">
            <div className="page-title-row">
              <div className="page-title-left">
                <h1 className="page-title">消息详情</h1>
              </div>
            </div>
          </header>
          <p style={{ padding: 24, color: 'var(--color-text-3)' }}>
            未找到该消息，可能已被清空。返回 <Link to="/console">会话控制台</Link>。
          </p>
        </div>
      </main>
    );
  }

  const { conversation, message } = found;
  const result = message.result;
  const stages = result?.timeline ?? [];
  const totalMS = stages.reduce((max, s) => Math.max(max, s.start_ms + s.duration_ms), 0);

  const messageIndex = conversation.messages.findIndex((candidate) => candidate.id === message.id);
  const precedingRequest = messageIndex > 0
    ? [...conversation.messages.slice(0, messageIndex)].reverse().find((candidate) => candidate.request)?.request
    : undefined;
  const requestPayload = message.request ?? precedingRequest ?? {
    note: '该消息没有关联请求体（数据来自旧版本会话记录）',
  };
  const responsePayload = result ?? { note: '该消息没有关联响应体' };

  const overviewItems: { label: string; value: string }[] = [
    { label: '消息 ID', value: message.id },
    { label: '角色', value: message.role === 'agent' ? 'Agent 回复' : '用户消息' },
    { label: '会话', value: `${conversation.title}（${conversation.id}）` },
    { label: '时间', value: formatDateTime(message.createdAt) },
    { label: 'request_id', value: result?.request_id ?? '-' },
    { label: 'session_id', value: result?.session_id ?? '-' },
    { label: 'trace_id', value: result?.trace_id ?? '-' },
    { label: '端到端延迟', value: result ? `${result.latency_ms} ms` : '-' },
    { label: 'token', value: result ? `${result.prompt_tokens} 输入 + ${result.completion_tokens} 输出` : '-' },
    { label: '成本', value: result ? formatCost(result.cost_usd) : '-' },
    { label: '缓存命中', value: result ? (result.cache_hit ? '是' : '否') : '-' },
  ];

  const tokenTotal = (result?.prompt_tokens ?? 0) + (result?.completion_tokens ?? 0);
  const inputPct = tokenTotal > 0 ? (result!.prompt_tokens / tokenTotal) * 100 : 0;

  return (
    <main>
      <div className="page-container">
        <header className="page-header">
          <nav className="breadcrumb breadcrumb-md" aria-label="面包屑">
            <Link className="breadcrumb-item" to="/console">
              会话列表
            </Link>
            <span className="breadcrumb-separator">/</span>
            <Link className="breadcrumb-item" to="/console">
              {conversation.title}
            </Link>
            <span className="breadcrumb-separator">/</span>
            <span className="breadcrumb-item current">消息详情</span>
          </nav>
          <div className="page-title-row">
            <div className="page-title-left">
              <h1 className="page-title">消息详情</h1>
              <span
                className={
                  message.error
                    ? 'status-tag status-tag-fill status-tag-error'
                    : 'status-tag status-tag-fill status-tag-success'
                }
              >
                {message.error ? '失败' : '完成'}
              </span>
            </div>
            <Link className="btn-icon-close" to="/console" aria-label="返回" title="返回控制台">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
              </svg>
            </Link>
          </div>
        </header>

        <div className="tabs-line tab-nav">
          {TABS.map((t) => (
            <span
              key={t.key}
              className={tab === t.key ? 'tab-item tab-size-md active' : 'tab-item tab-size-md'}
              onClick={() => setTab(t.key)}
            >
              {t.label}
            </span>
          ))}
        </div>

        <div className="tab-content">
          {tab === 'trace' && (
            <div className="tab-pane">
              <section className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">
                    处理时间线{totalMS > 0 ? `（总 ${totalMS} ms）` : ''}
                  </h2>
                  <span className="panel-subtitle">{stages.length} 个处理阶段</span>
                </div>
                {stages.length > 0 ? (
                  <TimelineChart stages={stages} />
                ) : (
                  <p style={{ padding: 16, color: 'var(--color-text-3)' }}>
                    该消息没有时间线数据（旧版本会话记录或请求失败）。
                  </p>
                )}
              </section>

              <section className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">工具调用与治理决策</h2>
                </div>
                <div className="tool-list">
                  {(result?.tools ?? []).map((tool, i) => (
                    <div key={i} className="tool-item">
                      <span className={`decision-tag decision-${tool.decision}`}>
                        {tool.decision}
                      </span>
                      <span className="tool-name">{tool.name}</span>
                      <span className="tool-reason">{tool.reason ?? ''}</span>
                    </div>
                  ))}
                  {!result?.tools?.length && (
                    <p style={{ padding: 16, color: 'var(--color-text-3)' }}>
                      本次执行没有工具调用。
                    </p>
                  )}
                </div>
              </section>

              <div className="trace-note">
                <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
                  <circle cx="8" cy="8" r="6.5" stroke="currentColor" strokeWidth="1.2" fill="none" />
                  <path d="M8 7.5v3.5M8 5.5v.01" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
                </svg>
                <span>
                  时间线由 Worker 各处理阶段真实埋点生成：会话锁 → 去重声明 → 结果重放查询 → 入站治理
                  → Runtime 获取 → Agent 执行（模型+工具）→ 结果持久化。
                </span>
              </div>
            </div>
          )}

          {tab === 'overview' && (
            <div className="tab-pane">
              <section className="descriptions descriptions-stacked">
                <h3 className="descriptions-title">基本信息</h3>
                <div className="descriptions-grid descriptions-grid-2">
                  {overviewItems.map((item) => (
                    <div key={item.label} className="desc-item desc-item-stacked">
                      <span className="desc-label">{item.label}</span>
                      <span className="desc-value">{item.value}</span>
                    </div>
                  ))}
                </div>
              </section>
              {tenant && (
                <section className="descriptions descriptions-stacked">
                  <h3 className="descriptions-title">上下文</h3>
                  <div className="descriptions-grid descriptions-grid-2">
                    <div className="desc-item desc-item-stacked">
                      <span className="desc-label">租户</span>
                      <span className="desc-value">
                        {tenant.app.name}（{tenant.tenant_id}）
                      </span>
                    </div>
                    <div className="desc-item desc-item-stacked">
                      <span className="desc-label">模型</span>
                      <span className="desc-value">
                        {tenant.model.provider}/{tenant.model.name}
                      </span>
                    </div>
                    <div className="desc-item desc-item-stacked">
                      <span className="desc-label">模拟用户</span>
                      <span className="desc-value">{userId || '-'}</span>
                    </div>
                    <div className="desc-item desc-item-stacked">
                      <span className="desc-label">通道</span>
                      <span className="desc-value">api/admin-api</span>
                    </div>
                  </div>
                </section>
              )}
            </div>
          )}

          {tab === 'request' && (
            <div className="tab-pane">
              <JsonTree data={requestPayload} />
            </div>
          )}

          {tab === 'response' && (
            <div className="tab-pane">
              <JsonTree data={responsePayload} />
            </div>
          )}

          {tab === 'metrics' && (
            <div className="tab-pane">
              <section className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">Token 构成</h2>
                </div>
                <div className="token-bar-container">
                  <div className="token-bar">
                    <div className="token-segment token-input" style={{ width: `${inputPct}%` }}>
                      <span className="token-segment-label">
                        输入 {result?.prompt_tokens ?? 0}
                      </span>
                    </div>
                    <div className="token-segment token-output" style={{ width: `${100 - inputPct}%` }}>
                      <span className="token-segment-label">
                        输出 {result?.completion_tokens ?? 0}
                      </span>
                    </div>
                  </div>
                </div>
                <div className="token-legend">
                  <span className="legend-item">
                    <span className="legend-dot legend-input" />输入 {result?.prompt_tokens ?? 0}
                  </span>
                  <span className="legend-item">
                    <span className="legend-dot legend-output" />输出 {result?.completion_tokens ?? 0}
                  </span>
                  <span className="legend-total">总计 {tokenTotal}</span>
                </div>
              </section>
              <section className="descriptions descriptions-label-left">
                <h3 className="descriptions-title">性能指标</h3>
                <div className="descriptions-list">
                  <div className="desc-item desc-item-horizontal">
                    <span className="desc-label">端到端延迟</span>
                    <span className="desc-value">{result?.latency_ms ?? '-'} ms</span>
                  </div>
                  <div className="desc-item desc-item-horizontal">
                    <span className="desc-label">Trace 总时长</span>
                    <span className="desc-value">{totalMS} ms</span>
                  </div>
                  <div className="desc-item desc-item-horizontal">
                    <span className="desc-label">阶段数</span>
                    <span className="desc-value">{stages.length}</span>
                  </div>
                  <div className="desc-item desc-item-horizontal">
                    <span className="desc-label">工具调用次数</span>
                    <span className="desc-value">{result?.tools?.length ?? 0}</span>
                  </div>
                  <div className="desc-item desc-item-horizontal">
                    <span className="desc-label">成本</span>
                    <span className="desc-value">
                      {result ? formatCost(result.cost_usd) : '-'}
                    </span>
                  </div>
                  <div className="desc-item desc-item-horizontal">
                    <span className="desc-label">缓存命中</span>
                    <span className="desc-value">{result?.cache_hit ? '是' : '否'}</span>
                  </div>
                </div>
              </section>
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
