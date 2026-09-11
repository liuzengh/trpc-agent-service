"use client";
import { useEffect, useRef, useState } from "react";
import { controlApi } from "../../lib/control-api";
import { ARTIFACT_MAX_BYTES, artifactApi, validArtifactName, type ArtifactReceipt } from "../../lib/artifact-api";
import { Button } from "../ui";
import styles from "./deployment.module.css";

export function ArtifactPanel({ tenantId, deploymentId, revisionNumber }: { tenantId: string; deploymentId: string; revisionNumber: number }) {
  const [owner, setOwner] = useState<boolean | null>(null);
  const [runId, setRunId] = useState("");
  const [name, setName] = useState("");
  const [version, setVersion] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [receipt, setReceipt] = useState<ArtifactReceipt | null>(null);
  const [read, setRead] = useState<{ url: string; mime: string; size: number; preview: string | null; name: string; version: number } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const operation = useRef<AbortController | null>(null);
  const downloadURL = useRef<string | null>(null);
  function revokeDownload() { if (downloadURL.current) { URL.revokeObjectURL(downloadURL.current); downloadURL.current = null; } }
  function clearFacts() { setReceipt(null); setRead(null); setError(""); revokeDownload(); }
  useEffect(() => {
    let current = true;
    setOwner(null);
    void controlApi.getTenant(tenantId).then((tenant) => { if (current) setOwner(tenant.role === "OWNER"); }).catch(() => { if (current) { setOwner(false); setError("读取租户权限失败，请重新进入本发布版本。"); } });
    return () => { current = false; operation.current?.abort(); revokeDownload(); };
  }, [tenantId]);
  const named = !!runId.trim() && validArtifactName(name);
  const readable = /^(0|[1-9][0-9]*)$/.test(version) && Number.isSafeInteger(Number(version));
  async function perform(write: boolean) {
    if (!owner || busy || !named || (write ? !file || file.size > ARTIFACT_MAX_BYTES : !readable)) return;
    const controller = new AbortController();operation.current = controller;
    setBusy(true);setError("");setRead(null);revokeDownload();
    const scope = { tenantId, deploymentId, revisionNumber, runId };
    try {
      if (write) {
        setReceipt(null);
        const result = await artifactApi.upload(scope, name, file!, controller.signal);
        if (!controller.signal.aborted) { setReceipt(result); setVersion(String(result.version)); }
      } else {
        const result = await artifactApi.read(scope, name, Number(version), controller.signal);
        if (!controller.signal.aborted) {
          const url = URL.createObjectURL(new Blob([result.bytes], { type: "application/octet-stream" }));
          downloadURL.current = url;
          const preview = result.mimeType.startsWith("text/") || result.mimeType.startsWith("application/json") ? new TextDecoder().decode(result.bytes.slice(0, 4096)) : null;
          setRead({ url, mime: result.mimeType, size: result.bytes.byteLength, preview, name, version: Number(version) });
        }
      }
    } catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : "Artifact 操作未完成。"); }
    finally { if (!controller.signal.aborted) setBusy(false); if (operation.current === controller) operation.current = null; }
  }
  return <section className={styles.card} aria-label="Artifact 文件">
    <h2>Artifact 文件</h2>
    <p>仅操作此固定发布版本的正式 Run 所属 Session。Run ID 是查找条件，服务端仍验证租户权限与 Manifest；不会由浏览器指定用户或存储路径。</p>
    <p className={styles.small}>上传每次创建新版本，不自动重试。HTTP 传输上限 16 MiB；后端还可能有更小限额。版本从 0 开始，读取使用显式版本。</p>
    {owner === null ? <p role="status">正在确认租户权限…</p> : !owner && <p>上传和读取 Artifact 需要当前租户 OWNER。</p>}
    <div className={styles.sources}>
      <label className={styles.field}><span>正式 Run ID</span><input aria-label="正式 Run ID" value={runId} disabled={busy || !owner} onChange={(e) => { setRunId(e.target.value); clearFacts(); }} /></label>
      <label className={styles.field}><span>文件名</span><input aria-label="Artifact 文件名" value={name} disabled={busy || !owner} onChange={(e) => { setName(e.target.value); clearFacts(); }} placeholder="仅文件名，不含目录" /></label>
      <label className={styles.field}><span>上传文件</span><input aria-label="Artifact 上传文件" type="file" disabled={busy || !owner} onChange={(e) => { const next = e.target.files?.[0] ?? null; setFile(next); if (next) setName(next.name); clearFacts(); }} /></label>
      <label className={styles.field}><span>读取版本</span><input aria-label="Artifact 读取版本" type="number" min={0} step={1} value={version} disabled={busy || !owner} onChange={(e) => { setVersion(e.target.value); setRead(null); revokeDownload(); }} placeholder="例如 0" /></label>
    </div>
    {name && !validArtifactName(name) && <p role="alert">文件名必须是 basename，不含路径分隔符或控制字符。</p>}
    {file && file.size > ARTIFACT_MAX_BYTES && <p role="alert">文件超过 16 MiB HTTP 传输上限。</p>}
    <div className={styles.actions}><Button disabled={!owner || busy || !named || !file || file.size > ARTIFACT_MAX_BYTES} onClick={() => void perform(true)}>上传新版本</Button><Button variant="secondary" disabled={!owner || busy || !named || !readable} onClick={() => void perform(false)}>读取指定版本</Button></div>
    {busy && <p role="status">正在等待 Artifact 服务响应…</p>}
    {error && <p role="alert" className={styles.error}>{error}</p>}
    {receipt && <div role="status" className={styles.small}><strong>已保存版本 {receipt.version}</strong><p>{receipt.name} · {receipt.mime_type} · {receipt.size_bytes} bytes</p><p>引用：<code>{receipt.ref}</code></p><p>SHA-256：<code>{receipt.sha256}</code></p></div>}
    {read && <div className={styles.small}><p>已读取 {read.name} · 版本 {read.version} · {read.mime} · {read.size} bytes</p><a className="button secondary" href={read.url} download={read.name}>下载已读取文件</a>{read.preview !== null && <><p>文本预览（最多 4096 bytes，按纯文本显示）：</p><pre className={styles.json}>{read.preview}</pre></>}</div>}
  </section>;
}
