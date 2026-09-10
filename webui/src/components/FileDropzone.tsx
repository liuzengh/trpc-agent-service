import { useRef, useState } from 'react'

import { FileTextIcon } from './Icons'
import { UploadCloudIcon } from './PageIcons'
import './FileDropzone.css'

function formatBytes(bytes: number): string {
  if (bytes <= 0) return '0 B'
  const unit = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(unit)), sizes.length - 1)
  return `${Number((bytes / unit ** index).toFixed(1))} ${sizes[index]}`
}

export function FileDropzone({
  file,
  acceptedExtensions,
  maxBytes,
  hint,
  onFile,
  onError,
}: {
  file: File | null
  acceptedExtensions: string[]
  maxBytes: number
  hint: string
  onFile: (file: File) => void
  onError: (message: string) => void
}) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [dragging, setDragging] = useState(false)
  const accept = acceptedExtensions.join(',')

  const openPicker = () => {
    if (!inputRef.current) return
    inputRef.current.value = ''
    inputRef.current.click()
  }

  const acceptFile = (candidate?: File) => {
    if (!candidate) return
    if (candidate.size > maxBytes) {
      onError(`文件大小不能超过 ${formatBytes(maxBytes)}`)
      return
    }
    const name = candidate.name.toLowerCase()
    if (!acceptedExtensions.some((extension) => name.endsWith(extension))) {
      onError('不支持该文件类型')
      return
    }
    onError('')
    onFile(candidate)
  }

  return (
    <>
      <input
        ref={inputRef}
        type="file"
        hidden
        accept={accept}
        onChange={(event) => acceptFile(event.target.files?.[0])}
      />
      {file ? (
        <div className="file-preview-card">
          <div className="file-preview-info">
            <div className="file-preview-icon"><FileTextIcon size={20} /></div>
            <div className="file-preview-meta">
              <strong className="file-preview-name">{file.name}</strong>
              <span className="file-preview-size">{formatBytes(file.size)} · 已选择</span>
            </div>
          </div>
          <button type="button" className="secondary file-dropzone-change" onClick={openPicker}>更换文件</button>
        </div>
      ) : (
        <button
          type="button"
          className={`file-dropzone ${dragging ? 'is-dragging' : ''}`}
          aria-label="上传文件"
          onClick={openPicker}
          onDragOver={(event) => {
            event.preventDefault()
            setDragging(true)
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={(event) => {
            event.preventDefault()
            setDragging(false)
            acceptFile(event.dataTransfer.files?.[0])
          }}
        >
          <div className="file-dropzone-icon-wrap"><UploadCloudIcon size={24} /></div>
          <div className="file-dropzone-text">
            <div className="file-dropzone-title">拖拽文件至此处，或 <span>点击浏览本地文件</span></div>
            <div className="file-dropzone-hint">{hint}</div>
          </div>
        </button>
      )}
    </>
  )
}
