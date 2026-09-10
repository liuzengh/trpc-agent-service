import { useState, type ReactNode } from 'react'
import { useFieldArray, useFormContext, useWatch } from 'react-hook-form'

import type { BotDraft } from '../../botDraft'
import type { ToolInfo } from '../../types'
import { ActivityIcon, ShieldIcon } from '../Icons'
import { BrowserCodeIcon, PlugIcon, PlusIcon } from '../PageIcons'
import { PanelHeader } from '../PanelHeader'
import { SelectControl } from '../SelectControl'

export function BotToolsSection({
  toolCatalog,
  toolCredentialRefs,
}: {
  toolCatalog: ToolInfo[]
  toolCredentialRefs: string[]
}) {
  const { control, register } = useFormContext<BotDraft>()
  const httpTools = useWatch({ control, name: 'tools_http' }) ?? []
  const mcpTools = useWatch({ control, name: 'tools_mcp' }) ?? []
  const credentialValues = new Set(toolCredentialRefs)
  for (const tool of httpTools) if (tool.credential_ref.trim()) credentialValues.add(tool.credential_ref.trim())
  for (const server of mcpTools) if (server.credential_ref.trim()) credentialValues.add(server.credential_ref.trim())

  return (
    <section className="bot-form-section compact">
      <PanelHeader level={3} icon={<ActivityIcon size={18} />} title="工具与调用策略" description="管理平台工具、自定义 HTTP、MCP 与单次调用限制。" />
      <div className="bot-section-content">
        <BuiltInTools toolCatalog={toolCatalog} />
        <HTTPToolsEditor />
        <MCPToolsEditor />
        <datalist id="tool-credential-refs">
          {[...credentialValues].map((value) => <option key={value} value={value} />)}
        </datalist>
        <div className="bot-three-column">
          <label>单次最大工具调用<input type="number" min={0} {...register('max_tool_calls', { valueAsNumber: true })} /></label>
          <label>单次预算单位<input type="number" min={0} {...register('budget_units', { valueAsNumber: true })} /></label>
          <label>审计保留天数<input type="number" min={0} {...register('retention_days', { valueAsNumber: true })} /></label>
        </div>
      </div>
    </section>
  )
}

function BuiltInTools({ toolCatalog }: { toolCatalog: ToolInfo[] }) {
  const { control, setValue } = useFormContext<BotDraft>()
  const allowed = useWatch({ control, name: 'tools_allowed' }) ?? []
  const confirmations = useWatch({ control, name: 'tools_require_confirmation' }) ?? []

  return (
    <div className="bot-tool-policy">
      <div className="bot-tool-policy-head">
        <span className="bot-tool-policy-icon" aria-hidden="true"><ShieldIcon size={17} /></span>
        <div><strong>工具权限与审批</strong><p>从平台已注册工具中选择；敏感工具可要求执行前审批。</p></div>
        {toolCatalog.length > 0 && <span className="bot-tool-policy-count">已允许 {allowed.length} / {toolCatalog.length}</span>}
      </div>
      {toolCatalog.length === 0 ? (
        <div className="bot-tool-policy-empty">当前平台未开放可配置工具。知识检索、记忆和当前时间等平台基础能力由系统统一管理，无需手动配置。</div>
      ) : (
        <div className="bot-tool-list">
          {toolCatalog.map((tool) => {
            const isAllowed = allowed.includes(tool.name)
            const requiresConfirmation = confirmations.includes(tool.name)
            const displayName = toolDisplayName(tool.name)
            const description = toolDisplayDescription(tool)
            return (
              <div className="bot-tool-row" key={tool.name}>
                <div className="bot-tool-choice">
                  <span><strong>{displayName}</strong><small>{description}</small></span>
                </div>
                <div className="bot-tool-row-controls">
                  <label className="bot-tool-check">
                    <input
                      type="checkbox"
                      checked={isAllowed}
                      aria-label={`启用 ${displayName}`}
                      onChange={(event) => {
                        if (event.target.checked) {
                          setValue('tools_allowed', [...new Set([...allowed, tool.name])], { shouldDirty: true })
                          return
                        }
                        setValue('tools_require_confirmation', confirmations.filter((name) => name !== tool.name), { shouldDirty: true })
                        setValue('tools_allowed', allowed.filter((name) => name !== tool.name), { shouldDirty: true })
                      }}
                    />
                    <span>启用</span>
                  </label>
                  <label className={`bot-tool-check ${isAllowed ? '' : 'is-disabled'}`}>
                    <input
                      type="checkbox"
                      disabled={!isAllowed}
                      checked={requiresConfirmation}
                      aria-label={`${displayName} 执行前审批`}
                      onChange={(event) => setValue(
                        'tools_require_confirmation',
                        event.target.checked ? [...new Set([...confirmations, tool.name])] : confirmations.filter((name) => name !== tool.name),
                        { shouldDirty: true },
                      )}
                    />
                    <span>执行前审批</span>
                  </label>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

function toolDisplayName(name: string) {
  if (name === 'duckduckgo_search') return '网络搜索'
  return name
}

function toolDisplayDescription(tool: ToolInfo) {
  if (tool.name === 'duckduckgo_search') return '搜索公开网页并返回结果摘要。'
  return tool.description || '平台工具'
}

function HTTPToolsEditor() {
  const { control, register } = useFormContext<BotDraft>()
  const tools = useWatch({ control, name: 'tools_http' }) ?? []
  const fields = useFieldArray({ control, name: 'tools_http' })

  return (
    <div className="bot-tool-policy">
      <div className="bot-tool-policy-head">
        <span className="bot-tool-policy-icon" aria-hidden="true"><BrowserCodeIcon size={17} /></span>
        <div><strong>自定义 HTTP 工具</strong><p>固定调用一个 HTTPS JSON 接口。URL 和凭据由配置持有，模型只能填写输入参数。</p></div>
        <button
          type="button"
          className="secondary small-btn"
          onClick={() => fields.append({ name: '', description: '', url: '', credential_ref: '', input_schema: '', enabled: true, require_confirmation: false })}
        >
          <PlusIcon size={13} /> 添加
        </button>
      </div>
      {fields.fields.length === 0 ? <div className="bot-tool-policy-empty">未配置 HTTP 工具。</div> : (
        <div className="bot-tool-editor-list">
          {fields.fields.map((field, index) => {
            const tool = tools[index] ?? field
            return (
              <ToolEditorCard
                key={field.id}
                initiallyOpen={!tool.name.trim()}
                title={tool.name.trim() || `HTTP 工具 ${index + 1}`}
                subtitle={tool.url.trim() || tool.description.trim() || '尚未配置接口地址'}
                meta={<><span>{tool.enabled ? '已启用' : '已停用'}</span>{tool.require_confirmation && <span>需审批</span>}</>}
              >
                  <div className="bot-two-column">
                    <label>工具名称<input placeholder="例如 query_order" {...register(`tools_http.${index}.name`)} /></label>
                    <label>调用凭据<input list="tool-credential-refs" placeholder="可选" {...register(`tools_http.${index}.credential_ref`)} /></label>
                  </div>
                  <label>固定 HTTPS URL<input placeholder="https://api.example.com/query" {...register(`tools_http.${index}.url`)} /></label>
                  <label>工具说明<input placeholder="告诉模型什么时候调用" {...register(`tools_http.${index}.description`)} /></label>
                  <label>
                    输入 JSON Schema
                    <textarea rows={6} placeholder={'例如 {"type":"object","properties":{"order_id":{"type":"string"}},"required":["order_id"]}'} {...register(`tools_http.${index}.input_schema`)} />
                  </label>
                  <div className="bot-tool-editor-actions">
                    <div className="bot-tool-editor-toggles">
                      <label className="checkbox bot-setting-checkbox"><input type="checkbox" {...register(`tools_http.${index}.enabled`)} /><span><strong>允许模型调用</strong></span></label>
                      <label className={`checkbox bot-setting-checkbox ${tool.enabled ? '' : 'is-disabled'}`}>
                        <input type="checkbox" disabled={!tool.enabled} {...register(`tools_http.${index}.require_confirmation`)} />
                        <span><strong>执行前审批</strong></span>
                      </label>
                    </div>
                    <button type="button" className="text-button danger" onClick={() => fields.remove(index)}>删除工具</button>
                  </div>
              </ToolEditorCard>
            )
          })}
        </div>
      )}
    </div>
  )
}

function MCPToolsEditor() {
  const { control, register, setValue } = useFormContext<BotDraft>()
  const servers = useWatch({ control, name: 'tools_mcp' }) ?? []
  const fields = useFieldArray({ control, name: 'tools_mcp' })

  return (
    <div className="bot-tool-policy">
      <div className="bot-tool-policy-head">
        <span className="bot-tool-policy-icon" aria-hidden="true"><PlugIcon size={17} /></span>
        <div><strong>MCP 服务</strong><p>直接使用框架 MCP ToolSet。仅支持 Streamable HTTP / SSE，不启动本机 stdio 进程。</p></div>
        <button
          type="button"
          className="secondary small-btn"
          onClick={() => fields.append({ name: '', description: '', transport: 'streamable', url: '', credential_ref: '', allowed_tools: '', confirmation_tools: '' })}
        >
          <PlusIcon size={13} /> 添加
        </button>
      </div>
      {fields.fields.length === 0 ? <div className="bot-tool-policy-empty">未配置 MCP 服务。</div> : (
        <div className="bot-tool-editor-list">
          {fields.fields.map((field, index) => {
            const server = servers[index] ?? field
            return (
              <ToolEditorCard
                key={field.id}
                initiallyOpen={!server.name.trim()}
                title={server.name.trim() || `MCP 服务 ${index + 1}`}
                subtitle={server.url.trim() || server.description.trim() || '尚未配置服务地址'}
                meta={<><span>{server.transport === 'sse' ? 'SSE' : 'Streamable HTTP'}</span>{server.allowed_tools.trim() && <span>{server.allowed_tools.split(',').filter(Boolean).length} 个工具</span>}</>}
              >
                  <div className="bot-two-column">
                    <label>服务名称<input placeholder="例如 crm" {...register(`tools_mcp.${index}.name`)} /></label>
                    <label htmlFor={`bot-mcp-transport-${field.id}`}>
                      传输协议
                      <SelectControl
                        id={`bot-mcp-transport-${field.id}`}
                        value={server.transport}
                        onValueChange={(value) => setValue(`tools_mcp.${index}.transport`, value === 'sse' ? 'sse' : 'streamable', { shouldDirty: true })}
                        options={[{ value: 'streamable', label: 'Streamable HTTP' }, { value: 'sse', label: 'SSE（兼容）' }]}
                      />
                    </label>
                  </div>
                  <label>MCP URL<input placeholder="https://mcp.example.com/mcp" {...register(`tools_mcp.${index}.url`)} /></label>
                  <label>调用凭据<input list="tool-credential-refs" placeholder="可选" {...register(`tools_mcp.${index}.credential_ref`)} /></label>
                  <label>服务说明<input placeholder="例如客户资料查询与工单工具" {...register(`tools_mcp.${index}.description`)} /></label>
                  <div className="bot-two-column">
                    <label>允许的远端工具<input placeholder="find_customer, create_ticket" {...register(`tools_mcp.${index}.allowed_tools`)} /><small className="field-help">逗号分隔。仅这些工具会进入模型工具面。</small></label>
                    <label>需要审批的远端工具<input placeholder="create_ticket" {...register(`tools_mcp.${index}.confirmation_tools`)} /><small className="field-help">必须是左侧允许工具的子集。</small></label>
                  </div>
                  <div className="bot-tool-editor-actions">
                    <span />
                    <button type="button" className="text-button danger" onClick={() => fields.remove(index)}>删除服务</button>
                  </div>
              </ToolEditorCard>
            )
          })}
        </div>
      )}
    </div>
  )
}

function ToolEditorCard({
  initiallyOpen,
  title,
  subtitle,
  meta,
  children,
}: {
  initiallyOpen: boolean
  title: string
  subtitle: string
  meta: ReactNode
  children: ReactNode
}) {
  const [open, setOpen] = useState(initiallyOpen)
  return (
    <details className="bot-tool-editor-card" open={open} onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>
        <span className="bot-tool-editor-summary-main">
          <strong>{title}</strong>
          <small>{subtitle}</small>
        </span>
        <span className="bot-tool-editor-summary-meta">{meta}</span>
      </summary>
      <div className="bot-tool-editor-body">{children}</div>
    </details>
  )
}
