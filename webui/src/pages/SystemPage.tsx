import { useQuery } from '@tanstack/react-query'
import { getSystem } from '../api'
import { CopyButton } from '../components/CopyButton'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { StatusIndicator } from '../components/StatusIndicator'
import { BackendDriverIcon } from '../components/BackendDriverIcon'
import {
  AlertIcon,
  KafkaBrandIcon,
  ServerIcon,
} from '../components/Icons'
import { LayersIcon, RouteIcon } from '../components/PageIcons'

export function SystemPage() {
  const systemQuery = useQuery({
    queryKey: ['console', 'system-status'],
    queryFn: ({ signal }) => getSystem(signal),
    staleTime: 10_000,
  })
  const status = systemQuery.data ?? null
  const error = systemQuery.error instanceof Error ? systemQuery.error.message : ''
  const lastProbe = systemQuery.dataUpdatedAt
    ? new Date(systemQuery.dataUpdatedAt).toLocaleTimeString('zh-CN', { hour12: false })
    : ''
  const load = () => systemQuery.refetch()

  const dependencies = status ? Object.entries(status.status).filter(([name]) => name.toLowerCase() !== 'node') : []
  const nodes = status?.nodes ?? []
  const okCount = dependencies.filter(([, value]) => value === 'ok').length
  const degraded = dependencies.filter(([, value]) => value !== 'ok')

  if (systemQuery.isLoading) {
    return <div className="page-stack system-page"><LoadingState label="正在探测系统状态…" /></div>
  }

  return (
    <div className="page-stack system-page">
      {error && (
        <FeedbackBanner tone="error">
          <span>{error}</span>
          <RefreshButton onClick={() => void load()} loading={systemQuery.isFetching} label="重新探测系统状态" />
        </FeedbackBanner>
      )}

      {status && (
        <>
          {degraded.length > 0 && (
            <div className="system-degraded-banner" role="status">
              <AlertIcon size={18} />
              <div>
                <strong>部分服务异常</strong>
                <p>{degraded.map(([name]) => dependencyLabel(name)).join('、')} 当前不可用，请检查服务连接。</p>
              </div>
            </div>
          )}

          <dl className="system-summary-cards" aria-label="系统状态汇总">
            <div className="system-summary-card system-version-card">
              <span className="system-summary-icon"><LayersIcon size={25} /></span>
              <div><dt>服务版本</dt><dd>{status.info.version || '—'}</dd></div>
            </div>
            <div className="system-summary-card system-health-card">
              <span className="system-summary-icon"><ServerIcon size={25} /></span>
              <div><dt>运行服务</dt><dd>{okCount} / {dependencies.length}</dd></div>
              <span className="system-health-bars" aria-hidden="true"><i /><i /><i /><i /></span>
            </div>
          </dl>

          <section className="system-section system-services-panel">
            <PanelHeader
              icon={<LayersIcon size={19} />}
              title="基础服务"
              description="核心依赖服务运行状态"
              actions={
                <>
                  {lastProbe && <span className="system-probe-time">最近探测 {lastProbe}</span>}
                  <RefreshButton onClick={() => void load()} loading={systemQuery.isFetching} label="重新探测系统状态" />
                </>
              }
            />
            <div className="table-scroll system-dependency-table-wrap">
              <table className="ui-table dependency-table">
                <thead><tr><th>服务名称</th><th>状态</th></tr></thead>
                <tbody>
                  {dependencies.map(([name, value]) => (
                    <tr key={name}>
                      <td>
                        <span className="dependency-service">
                          <span className={`dependency-brand dependency-brand-${name.toLowerCase()}`}>
                            <DependencyIcon name={name} />
                          </span>
                          <strong>{dependencyLabel(name)}</strong>
                        </span>
                      </td>
                      <td className="dependency-state-cell">
                        <StatusIndicator tone={value === 'ok' ? 'success' : 'danger'} appearance="pill">
                          {value === 'ok' ? '正常' : '异常'}
                        </StatusIndicator>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {nodes.length > 0 && (
            <section className="system-section system-services-panel">
              <PanelHeader
                icon={<ServerIcon size={19} />}
                title="运行节点"
                description="网关与执行节点的实时注册和排空状态"
              />
              <div className="table-scroll system-dependency-table-wrap">
                <table className="ui-table dependency-table system-node-table">
                  <thead><tr><th>节点</th><th>角色</th><th>处理中</th><th>最近心跳</th><th>状态</th></tr></thead>
                  <tbody>
                    {nodes.map((node) => (
                      <tr key={`${node.node_id}:${node.boot_id}`}>
                        <td><strong className="mono-value">{node.node_id}</strong></td>
                        <td><NodeRole role={node.role} /></td>
                        <td>{node.inflight}</td>
                        <td>{formatHeartbeat(node.last_heartbeat)}</td>
                        <td className="dependency-state-cell">
                          <StatusIndicator
                            tone={node.state === 'ready' ? 'success' : node.state === 'draining' ? 'info' : 'danger'}
                            appearance="pill"
                          >
                            {nodeStateLabel(node.state)}
                          </StatusIndicator>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          <div className="system-config-grid">
            <section className="flat-config-panel system-config-card">
              <PanelHeader icon={<ServerIcon size={19} />} title="服务配置" description="服务监听与基础运行环境" />
              <dl className="definition-list system-definition-list">
                <SystemConfigItem label="监听地址" value={status.info.listen_address} />
                <SystemConfigItem label="服务版本" value={status.info.version} />
                <SystemConfigItem label="Redis 地址" value={status.info.redis_address} />
              </dl>
            </section>
            <section className="flat-config-panel system-config-card">
              <PanelHeader icon={<KafkaBrandIcon size={19} />} title="消息配置" description="异步消息与事件队列配置" />
              <dl className="definition-list system-definition-list">
                <SystemConfigItem label="Kafka 地址" value={status.info.kafka_brokers} />
                <SystemConfigItem label="Kafka 主题" value={status.info.kafka_topic} />
              </dl>
            </section>
          </div>
        </>
      )}
    </div>
  )
}

function SystemConfigItem({ label, value }: { label: string; value: string }) {
  const display = value || '—'
  return (
    <div>
      <dt>{label}</dt>
      <dd>
        <span className="mono-value">{display}</span>
        {value && (
          <CopyButton
            value={value}
            label={`复制${label}`}
            copiedLabel={`已复制${label}`}
            className="system-copy-button"
            iconOnly
          />
        )}
      </dd>
    </div>
  )
}

function dependencyLabel(name: string) {
  const labels: Record<string, string> = {
    kafka: 'Apache Kafka',
    redis: 'Redis',
    postgres: 'PostgreSQL',
    postgresql: 'PostgreSQL',
    pgvector: 'pgvector',
    qdrant: 'Qdrant',
    s3: 'S3 兼容对象存储',
    cos: '腾讯云 COS',
    mem0: 'Mem0',
  }
  return labels[name.toLowerCase()] ?? name
}

function DependencyIcon({ name }: { name: string }) {
  if (name.toLowerCase() === 'kafka') return <KafkaBrandIcon size={25} />
  if (['postgres', 'postgresql', 'pgvector', 'redis', 'qdrant', 's3', 'cos', 'mem0'].includes(name.toLowerCase())) {
    return <BackendDriverIcon driver={name} size={25} />
  }
  return <ServerIcon size={22} />
}

function nodeRoleLabel(role: string) {
  return ({ gateway: '网关', worker: '执行节点', all: '一体模式' } as Record<string, string>)[role] ?? role
}

function NodeRole({ role }: { role: string }) {
  const icon = role === 'gateway'
    ? <RouteIcon size={15} />
    : <ServerIcon size={15} />
  return <span className="dependency-service">{icon}<span>{nodeRoleLabel(role)}</span></span>
}

function nodeStateLabel(state: string) {
  return ({ ready: '正常', draining: '排空中', offline: '离线' } as Record<string, string>)[state] ?? state
}

function formatHeartbeat(value: string) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleTimeString('zh-CN', { hour12: false })
}
