import { useEffect, useMemo, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useQuery } from '@tanstack/react-query'
import {
  deleteKnowledgeDocument,
  ingestKnowledgeDocument,
  uploadKnowledgeDocument,
  listKnowledgeDocuments,
} from '../api'
import { useAppContext } from '../context'
import type { KnowledgeDocument } from '../types'
import { SegmentedControl } from '../components/SegmentedControl'
import { PanelHeader } from '../components/PanelHeader'
import { StatusIndicator } from '../components/StatusIndicator'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { DialogHeader } from '../components/DialogHeader'
import { EmptyState } from '../components/EmptyState'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { RefreshButton } from '../components/RefreshButton'
import './PlatformDataPage.css'
import { FileDropzone } from '../components/FileDropzone'
import {
  FileTextIcon,
  GlobeIcon,
} from '../components/Icons'
import { GitBranchIcon, PlusIcon } from '../components/PageIcons'

const MAX_KNOWLEDGE_FILE_BYTES = 16 << 20
const KNOWLEDGE_FILE_EXTENSIONS = ['.txt', '.text', '.md', '.markdown', '.json', '.csv', '.doc', '.docx', '.go', '.py', '.proto', '.pdf', '.pptx', '.xlsx', '.html', '.png', '.jpg', '.jpeg', '.tiff', '.tif', '.bmp', '.webp', '.asciidoc']

export function PlatformDataPage() {
  const { apps, appsLoading, tenant, appsError, activeAppKey } = useAppContext()
  const selectedApp = useMemo(
    () =>
      apps.find(
        (entry) =>
          (`${entry.Config.tenant_id}/${entry.Config.app_code}` === activeAppKey || entry.Config.app_code === activeAppKey) &&
          entry.Config.tenant_id === tenant,
      ),
    [activeAppKey, apps, tenant],
  )
  const appCode = selectedApp?.Config.app_code ?? ''
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')

  type KnowledgeSourceMode = 'file' | 'text' | 'url' | 'repo'
  const [sourceMode, setSourceMode] = useState<KnowledgeSourceMode>('file')
  const [sourceUrl, setSourceUrl] = useState('')
  const [branch, setBranch] = useState('')
  const [showCreateForm, setShowCreateForm] = useState(false)
  const [docId, setDocId] = useState('')
  const [docName, setDocName] = useState('')
  const [docContent, setDocContent] = useState('')
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [isIngesting, setIsIngesting] = useState(false)
  const [documentToDelete, setDocumentToDelete] = useState('')
  const [deletingDocument, setDeletingDocument] = useState(false)

  const documentsQuery = useQuery({
    queryKey: ['console', 'knowledge-documents', tenant, appCode],
    queryFn: ({ signal }) => listKnowledgeDocuments(tenant, appCode, signal),
    enabled: Boolean(tenant && appCode),
    refetchInterval: (query) => query.state.data?.some((document) => document.status === 'indexing') ? 2000 : false,
  })
  const documentList: KnowledgeDocument[] = documentsQuery.data ?? []
  const isLoadingDocs = documentsQuery.isFetching
  const visibleError = error || (documentsQuery.error instanceof Error ? documentsQuery.error.message : '')

  // biome-ignore lint/correctness/useExhaustiveDependencies: local page state belongs to one tenant/application scope and must reset when that scope changes.
  useEffect(() => {
    setError('')
    setNotice('')
    setShowCreateForm(false)
    setDocumentToDelete('')
  }, [tenant, appCode])

  const loadDocuments = async () => {
    if (!tenant || !appCode) return
    setError('')
    await documentsQuery.refetch()
  }

  const handleDeleteDoc = async (documentId: string) => {
    setError('')
    setNotice('')
    setDeletingDocument(true)
    try {
      await deleteKnowledgeDocument(tenant, appCode, documentId)
      setNotice(`文档「${documentId}」已删除`)
      setDocumentToDelete('')
      await loadDocuments()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setDeletingDocument(false)
    }
  }

  const handleClearFile = () => {
    setDocName('')
    setDocId('')
    setDocContent('')
    setSelectedFile(null)
    setSourceUrl('')
    setBranch('')
  }

  const handleFileUpload = (file?: File) => {
    if (!file) return
    setError('')
    if (file.size > MAX_KNOWLEDGE_FILE_BYTES) {
      setError('文件大小不能超过 16 MiB')
      return
    }
    setSelectedFile(file)
    setDocName(file.name)
    setDocId((current) => current || file.name.replace(/\.[^.]+$/, '').toLowerCase().replace(/[^a-z0-9_-]/g, '-'))
  }

  const handleIngest = async () => {
    if (!docId.trim()) {
      setError('文档 ID 不能为空')
      return
    }
    if (sourceMode === 'file' && !selectedFile) {
      setError('请选择要上传的文件')
      return
    }
    if (sourceMode === 'text' && !docContent.trim()) {
      setError('请输入文档正文')
      return
    }
    if ((sourceMode === 'url' || sourceMode === 'repo') && !sourceUrl.trim()) {
      setError(sourceMode === 'url' ? '请输入网页 URL' : '请输入 Git 仓库地址')
      return
    }
    if (sourceMode === 'url' && !/^https?:\/\//i.test(sourceUrl.trim())) {
      setError('网页 URL 必须以 http:// 或 https:// 开头')
      return
    }

    setIsIngesting(true)
    setError('')
    setNotice('')
    try {
      if (sourceMode === 'file' && selectedFile) {
        await uploadKnowledgeDocument({
          tenant_id: tenant,
          app_code: appCode,
          document_id: docId.trim(),
          name: docName.trim() || selectedFile.name,
          file: selectedFile,
          chunk_size: 800,
          overlap: 100,
        })
        setNotice(`文档「${docName || docId}」已提交索引`)
      } else if (sourceMode === 'text') {
        await ingestKnowledgeDocument({
          tenant_id: tenant,
          app_code: appCode,
          document_id: docId.trim(),
          name: docName.trim() || docId.trim(),
          content: docContent,
          chunk_size: 800,
          overlap: 100,
        })
        setNotice(`文档「${docName || docId}」已发布`)
      } else if (sourceMode === 'url') {
        await ingestKnowledgeDocument({
          tenant_id: tenant,
          app_code: appCode,
          document_id: docId.trim(),
          name: docName.trim() || sourceUrl.trim(),
          source_type: 'url',
          source_url: sourceUrl.trim(),
          chunk_size: 800,
          overlap: 100,
        })
        setNotice(`网页抓取任务「${docName || docId}」已提交索引`)
      } else if (sourceMode === 'repo') {
        await ingestKnowledgeDocument({
          tenant_id: tenant,
          app_code: appCode,
          document_id: docId.trim(),
          name: docName.trim() || sourceUrl.trim(),
          source_type: 'repo',
          source_url: sourceUrl.trim(),
          branch: branch.trim() || undefined,
          chunk_size: 800,
          overlap: 100,
        })
        setNotice(`代码库索引任务「${docName || docId}」已提交索引`)
      }
      setShowCreateForm(false)
      handleClearFile()
      await loadDocuments()
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setIsIngesting(false)
    }
  }

  if (appsLoading) {
    return <div className="page-stack"><LoadingState label="正在读取机器人…" /></div>
  }
  if (!appCode) {
    return (
      <div className="page-stack">
        <div className="empty-block">当前租户没有可管理的机器人。</div>
      </div>
    )
  }

  return (
    <div className="page-stack platform-data-page">
      {appsError && <FeedbackBanner tone="error">{appsError}</FeedbackBanner>}
      {visibleError && !showCreateForm && <FeedbackBanner tone="error">{visibleError}</FeedbackBanner>}
      {notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}

      <section className="data-panel-card">
          <PanelHeader
            title="知识文档"
            description="管理该机器人可使用的知识文档，上传后可在对话中基于文档内容回答。"
            actions={(
              <>
              <RefreshButton onClick={() => void loadDocuments()} loading={isLoadingDocs} label="刷新知识文档" />
              <button type="button" className="primary" onClick={() => setShowCreateForm(true)}>
                  <PlusIcon size={15} /> 上传文档
              </button>
              </>
            )}
          />

          {documentsQuery.isLoading ? (
            <LoadingState label="正在读取知识文档…" />
          ) : documentList.length === 0 ? (
            <EmptyState
              icon={<FileTextIcon size={50} />}
              title="暂无知识文档"
              description="点击右上角「上传文档」添加内容，让机器人基于您的知识进行更准确的回答。"
            />
          ) : (
            <div className="table-scroll data-table-scroll"><table className="ui-table knowledge-docs-table">
              <thead>
                <tr>
                  <th>文档名称</th>
                  <th>来源</th>
                  <th>ID</th>
                  <th>状态</th>
                  <th>更新时间</th>
                  <th className="knowledge-actions-col">操作</th>
                </tr>
              </thead>
              <tbody>
                {documentList.map((doc) => {
                  const sourceType = doc.metadata?.source_type
                  const sourceUrl = doc.metadata?.source_url
                  const branch = doc.metadata?.branch
                  return (
                    <tr key={doc.document_id}>
                      <td><strong>{doc.name || doc.document_id}</strong></td>
                      <td>
                        {sourceType === 'url' ? (
                          <span title={sourceUrl || ''} className="knowledge-source is-linked">
                            <GlobeIcon size={12} /> 网页
                          </span>
                        ) : sourceType === 'repo' ? (
                          <span title={`${sourceUrl || ''}${branch ? ` (${branch})` : ''}`} className="knowledge-source is-linked">
                            <GitBranchIcon size={12} /> 代码库
                          </span>
                        ) : sourceType === 'file' ? (
                          <span className="knowledge-source is-muted">
                            <FileTextIcon size={12} /> 文件
                          </span>
                        ) : (
                          <span className="knowledge-source is-muted">
                            <FileTextIcon size={12} /> 文本
                          </span>
                        )}
                      </td>
                      <td><span className="mono-value knowledge-document-id">{doc.document_id}</span></td>
                      <td>
                        <StatusIndicator
                          appearance="pill"
                          tone={doc.status === 'failed' ? 'danger' : doc.status === 'indexing' ? 'info' : 'success'}
                        >
                          {doc.status === 'indexing' ? '索引中' : doc.status === 'failed' ? '失败' : '可用'}
                        </StatusIndicator>
                      </td>
                      <td className="knowledge-updated">
                        {new Date(doc.updated_at).toLocaleString()}
                      </td>
                      <td className="knowledge-actions-col">
                        <button
                          type="button"
                          className="table-action danger knowledge-delete-button"
                          onClick={() => setDocumentToDelete(doc.document_id)}
                        >
                          删除
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table></div>
          )}

          <Dialog.Root open={showCreateForm} onOpenChange={(open) => {
            setShowCreateForm(open)
            if (!open) handleClearFile()
          }}>
            <Dialog.Portal>
              <Dialog.Overlay className="modal-backdrop" />
              <Dialog.Content className="modal knowledge-ingest-modal">
                <DialogHeader
                  title="添加知识文档"
                  description="添加文件、文本、网页或代码仓库到当前机器人的知识库。"
                  descriptionClassName="sr-only"
                  closeLabel="关闭知识文档弹窗"
                />

                {visibleError && <FeedbackBanner tone="error">{visibleError}</FeedbackBanner>}

                <SegmentedControl
                  ariaLabel="知识来源类型"
                  value={sourceMode}
                  items={[
                    { value: 'file', label: '本地文件' },
                    { value: 'text', label: '直接文本' },
                    { value: 'url', label: '网页抓取 (URL)' },
                    { value: 'repo', label: 'Git 仓库' },
                  ]}
                  onValueChange={(val) => {
                    setSourceMode(val as KnowledgeSourceMode)
                    setError('')
                  }}
                />

                {sourceMode === 'file' && (
                  <FileDropzone
                    file={selectedFile}
                    acceptedExtensions={KNOWLEDGE_FILE_EXTENSIONS}
                    maxBytes={MAX_KNOWLEDGE_FILE_BYTES}
                    hint="支持常见文本、代码与文档；PDF、Office 和图片需 Docling · 最大 16 MiB"
                    onFile={handleFileUpload}
                    onError={setError}
                  />
                )}

                {sourceMode === 'url' && (
                  <div className="knowledge-source-fields">
                    <label>
                      网页 URL 地址
                      <input
                        value={sourceUrl}
                        onChange={(e) => {
                          const val = e.target.value
                          setSourceUrl(val)
                          if (!docName) setDocName(val)
                          if (!docId) {
                            const slug = val.replace(/^https?:\/\//i, '').replace(/[^a-z0-9_-]/gi, '-').toLowerCase().slice(0, 32)
                            setDocId(slug)
                          }
                        }}
                        placeholder="例如：https://docs.example.com/guide"
                      />
                    </label>
                    <small className="knowledge-field-help">
                      服务端会抓取网页正文并自动建立知识索引。
                    </small>
                  </div>
                )}

                {sourceMode === 'repo' && (
                  <div className="knowledge-source-fields">
                    <div className="knowledge-source-grid">
                      <label>
                        Git 仓库地址
                        <input
                          value={sourceUrl}
                          onChange={(e) => {
                            const val = e.target.value
                            setSourceUrl(val)
                            if (!docName) setDocName(val)
                            if (!docId) {
                              const parts = val.trim().replace(/\.git$/, '').split('/')
                              const repoSlug = parts[parts.length - 1]?.toLowerCase().replace(/[^a-z0-9_-]/g, '-') || ''
                              setDocId(repoSlug)
                            }
                          }}
                          placeholder="例如：https://github.com/org/repo.git"
                        />
                      </label>
                      <label>
                        分支 / Tag (可选)
                        <input
                          value={branch}
                          onChange={(e) => setBranch(e.target.value)}
                          placeholder="如 main 或 master"
                        />
                      </label>
                    </div>
                    <small className="knowledge-field-help">
                      服务端会读取代码仓库中的代码与文档，并自动建立知识索引。
                    </small>
                  </div>
                )}

                <div className="knowledge-metadata-grid">
                  <label>
                    文档名称
                    <input
                      value={docName}
                      onChange={(e) => setDocName(e.target.value)}
                      placeholder="如：产品说明.md"
                    />
                  </label>
                  <label>
                    文档 ID
                    <input
                      value={docId}
                      onChange={(e) => setDocId(e.target.value)}
                      placeholder="如：product-guide"
                    />
                  </label>
                </div>

                {sourceMode === 'text' && (
                  <label className="document-content-field">
                    文档正文 ({docContent.length} 字符)
                    <textarea
                      rows={8}
                      value={docContent}
                      onChange={(e) => setDocContent(e.target.value)}
                      placeholder="在此直接输入或粘贴知识文档正文…"
                    />
                  </label>
                )}

                <div className="modal-actions">
                  <button type="button" className="secondary" onClick={() => setShowCreateForm(false)}>
                    取消
                  </button>
                  <button
                    type="button"
                    className="primary"
                    disabled={
                      isIngesting ||
                      !docId.trim() ||
                      (sourceMode === 'file' ? !selectedFile :
                       sourceMode === 'text' ? !docContent.trim() :
                       sourceMode === 'url' ? !sourceUrl.trim() :
                       sourceMode === 'repo' ? !sourceUrl.trim() : true)
                    }
                    onClick={() => void handleIngest()}
                  >
                    {isIngesting
                      ? (sourceMode === 'file' || sourceMode === 'text' ? '正在发布…' : '正在提交索引…')
                      : (sourceMode === 'file' || sourceMode === 'text' ? '发布' : '开始抓取并索引')}
                  </button>
                </div>
              </Dialog.Content>
            </Dialog.Portal>
          </Dialog.Root>

      </section>

      <ConfirmDialog
        open={Boolean(documentToDelete)}
        title="删除知识文档？"
        description={documentToDelete ? `文档「${documentToDelete}」删除后需要重新上传或抓取才能恢复。` : ''}
        confirmLabel="删除文档"
        busy={deletingDocument}
        onConfirm={() => { if (documentToDelete) void handleDeleteDoc(documentToDelete) }}
        onOpenChange={(open) => { if (!open && !deletingDocument) setDocumentToDelete('') }}
      />

    </div>
  )
}
