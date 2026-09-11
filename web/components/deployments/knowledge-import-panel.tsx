"use client";
import { useEffect, useRef, useState } from "react";
import { controlApi } from "../../lib/control-api";
import { knowledgeApi, knowledgeInputError, KNOWLEDGE_MAX_TEXT_BYTES } from "../../lib/knowledge-api";
import { Button } from "../ui";
import styles from "./deployment.module.css";

export function KnowledgeImportPanel({ tenantId, deploymentId, revisionNumber, resources }: { tenantId: string; deploymentId: string; revisionNumber: number; resources: string[] }) {
  const [owner, setOwner] = useState<boolean | null>(null);
  const [resource, setResource] = useState(resources[0] ?? "");
  const [name, setName] = useState("");
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [reading, setReading] = useState(false);
  const [error, setError] = useState("");
  const [receipt, setReceipt] = useState<{ name: string; resource: string; documents: number } | null>(null);
  const operation = useRef<AbortController | null>(null);
  const reader = useRef<FileReader | null>(null);
  useEffect(() => {
    let current = true;
    void controlApi.getTenant(tenantId).then((tenant) => { if (current) setOwner(tenant.role === "OWNER"); }).catch(() => { if (current) { setOwner(false); setError("读取租户权限失败，请重新进入本发布版本。"); } });
    return () => { current = false; operation.current?.abort(); reader.current?.abort(); };
  }, [tenantId]);
  function clear() { setError(""); setReceipt(null); }
  function chooseFile(file?: File) {
    reader.current?.abort(); reader.current = null; setReading(false); clear(); setText("");
    if (!file) return;
    if (file.size > KNOWLEDGE_MAX_TEXT_BYTES) { setError("文本文件超过 1 MiB；后端还可能有更小限额。"); return; }
    if (!(file.type.startsWith("text/") || (!file.type && /\.(txt|md)$/i.test(file.name)))) { setError("只导入 UTF-8 纯文本文件；不解析 PDF 或其他二进制格式。"); return; }
    const next = new FileReader(); reader.current = next; setReading(true);
    next.onload = () => {
      if (reader.current !== next) return;
      try { const value = new TextDecoder("utf-8", { fatal: true }).decode(next.result as ArrayBuffer); setName(file.name); setText(value); }
      catch { setError("文件不是有效 UTF-8 纯文本。"); }
      setReading(false); reader.current = null;
    };
    next.onerror = () => { if (reader.current === next) { setError("读取本地文本文件失败。"); setReading(false); reader.current = null; } };
    next.readAsArrayBuffer(file);
  }
  const validation = knowledgeInputError(name, text);
  async function submit() {
    if (!owner || busy || reading || validation || !resources.includes(resource)) return;
    const controller = new AbortController(); operation.current = controller; setBusy(true); clear();
    try { const result = await knowledgeApi.importText({ tenantId, deploymentId, revisionNumber, resource }, name, text, controller.signal); if (!controller.signal.aborted) setReceipt({ name, resource, documents: result.documents }); }
    catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : "导入未确认，部分 chunks 可能已写入；不会自动重试。"); }
    finally { if (!controller.signal.aborted) setBusy(false); if (operation.current === controller) operation.current = null; }
  }
  return <section className={styles.card} aria-label="Knowledge 文本导入">
    <h2>Knowledge 文本导入</h2>
    <p>只向此不可变 Manifest 显式选用的 managed Knowledge 资源同步导入纯文本。隔离范围由服务端按租户、Profile、资源、固定后端和 Embedding 配置派生，不按 Session 隔离，也不接受浏览器指定存储命名空间。</p>
    <p className={styles.small}>文本上限 1 MiB、JSON HTTP 请求上限 2 MiB；后端可能有更小限额。不解析 PDF。失败时部分 chunks 可能已写入，不会自动重试，也不表示已回滚。</p>
    {owner === null ? <p role="status">正在确认租户权限…</p> : !owner && <p>导入 Knowledge 需要当前租户 OWNER。</p>}
    <div className={styles.sources}>
      <label className={styles.field}><span>固定 Knowledge 资源</span><select aria-label="Knowledge 资源" value={resource} disabled={busy || reading || !owner} onChange={(e) => { setResource(e.target.value); clear(); }}>{resources.map((item) => <option key={item} value={item}>{item}</option>)}</select></label>
      <label className={styles.field}><span>文档名</span><input aria-label="Knowledge 文档名" value={name} disabled={busy || reading || !owner} onChange={(e) => { setName(e.target.value); clear(); }} placeholder="note.txt" /></label>
      <label className={styles.field}><span>可选：载入本地文本文件</span><input aria-label="Knowledge 文本文件" type="file" accept="text/*,.txt,.md" disabled={busy || !owner} onChange={(e) => chooseFile(e.target.files?.[0])} /></label>
    </div>
    <label className={styles.field}><span>文本内容</span><textarea aria-label="Knowledge 文本内容" rows={8} value={text} disabled={busy || reading || !owner} onChange={(e) => { setText(e.target.value); clear(); }} /></label>
    {name && text && validation && <p role="alert">{validation}</p>}
    {error && <p role="alert" className={styles.error}>{error}</p>}
    <Button disabled={!owner || busy || reading || !!validation || !resources.includes(resource)} onClick={() => void submit()}>同步导入文本</Button>
    {(busy || reading) && <p role="status">{reading ? "正在读取本地文件…" : "正在等待导入响应…"}</p>}
    {receipt && <p role="status">已导入 {receipt.documents} 个 documents / chunks · {receipt.resource} · {receipt.name}</p>}
  </section>;
}
