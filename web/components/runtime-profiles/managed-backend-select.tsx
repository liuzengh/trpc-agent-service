"use client";
import { useEffect, useId, useState, type ReactNode } from "react";
import { backendDirectoryMessage, listRuntimeBackends, selectableBackend, type BackendRole, type BackendSelection, type RuntimeBackend } from "../../lib/runtime-backend-api";

type DirectoryState = { tenant: string; status: "loading" | "ready" | "error"; items: RuntimeBackend[]; message?: string };
/** Only a catalog-backed explicit selection; never implies adapter/runtime health. */
export function ManagedBackendSelect({ tenantId, role, value, onChange, disabled = false, readOnly = false, renderSelection }: {
  tenantId: string; role: BackendRole; value: Partial<BackendSelection>; onChange(value: BackendSelection): void; disabled?: boolean; readOnly?: boolean; renderSelection?(selected: RuntimeBackend | undefined): ReactNode;
}) {
  const id = useId();
  const [retry, setRetry] = useState(0);
  const [directory, setDirectory] = useState<DirectoryState>({ tenant: tenantId, status: "loading", items: [] });
  useEffect(() => {
    if (readOnly) return;
    const controller = new AbortController();
    setDirectory({ tenant: tenantId, status: "loading", items: [] });
    listRuntimeBackends(tenantId, controller.signal).then((items) => {
      if (!controller.signal.aborted) setDirectory({ tenant: tenantId, status: "ready", items });
    }).catch((error: unknown) => {
      if (!controller.signal.aborted) setDirectory({ tenant: tenantId, status: "error", items: [], message: backendDirectoryMessage(error) });
    });
    return () => controller.abort();
  }, [tenantId, retry, readOnly]);
  const ready = directory.tenant === tenantId && directory.status === "ready";
  const options = ready ? directory.items.filter((item) => selectableBackend(item, role)) : [];
  const selected = options.find((item) => item.id === value.backend_id && item.revision === value.backend_revision);
  const selectedKey = selected ? `${selected.id}@${selected.revision}` : "";
  return <section aria-label={`${role} managed 后端`}>
    {value.backend_id && <p>当前绑定：<code>{value.backend_id}</code> · 修订 {value.backend_revision ?? "未设置"}</p>}
    {!readOnly && <>
      <label htmlFor={id}>平台后端（{role}）</label>
      <select id={id} value={selectedKey} disabled={disabled || !ready || options.length === 0} onChange={(event) => {
        const item = options.find((candidate) => `${candidate.id}@${candidate.revision}` === event.target.value);
        if (!disabled && item) onChange({ backend_id: item.id, backend_revision: item.revision });
      }}>
        <option value="">{selected ? "请选择" : value.backend_id ? "当前绑定不可选或修订已变化" : "请选择平台后端"}</option>
        {options.map((item) => <option key={item.id} value={`${item.id}@${item.revision}`}>{item.label} · {item.kind} · r{item.revision}</option>)}
      </select>
      {directory.tenant !== tenantId || directory.status === "loading" ? <p role="status">正在读取后端目录…</p>
        : directory.status === "error" ? <p role="alert">{directory.message}</p>
          : options.length === 0 ? <p role="status">没有支持 {role} 的可选后端；请联系平台管理员。</p>
            : value.backend_id && !selected ? <p role="alert">当前绑定不在可选目录中或修订已变化，请显式重新选择；已保存的绑定不会自动升级。</p> : null}
      <button type="button" disabled={disabled || directory.status === "loading"} onClick={() => setRetry((n) => n + 1)}>刷新后端目录</button>
    </>}
    {renderSelection?.(selected)}
    <small>平台连接目标不进入 Profile config；凭据通过独立只写操作配置。目录可选不等于 Worker 运行就绪；当前发布能力以服务端校验为准。</small>
  </section>;
}
