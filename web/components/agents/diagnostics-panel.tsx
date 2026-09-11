"use client";

import type { CSSProperties } from "react";
import { AlertCircle, AlertTriangle, CheckCircle2 } from "lucide-react";

import { nodeIDFromDiagnostic, type AgentSpecDiagnostic } from "../../lib/agent-spec-v1";

export function DiagnosticsPanel({
  diagnostics,
  onSelectNode,
}: {
  diagnostics: readonly AgentSpecDiagnostic[];
  onSelectNode?: (nodeID: string) => void;
}) {
  if (diagnostics.length === 0) {
    return (
      <div style={{ ...panelStyle, color: "var(--success)", background: "var(--success-soft)" }} data-testid="diagnostics-empty">
        <CheckCircle2 size={16} />
        <span>本地结构检查通过；发布仍以服务端校验结果为准。</span>
      </div>
    );
  }

  return (
    <section aria-label="AgentSpec 诊断" style={panelStyle}>
      <header style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
        <strong style={{ fontSize: 13 }}>诊断</strong>
        <span style={{ color: "var(--muted)", fontSize: 11 }}>
          {diagnostics.filter((item) => item.severity === "error").length} 个错误 · {diagnostics.filter((item) => item.severity === "warning").length} 个警告
        </span>
      </header>
      <div style={{ display: "grid", gap: 8, marginTop: 10 }}>
        {diagnostics.map((item, index) => {
          const nodeID = nodeIDFromDiagnostic(item);
          const warning = item.severity === "warning";
          const content = (
            <>
              {warning ? <AlertTriangle size={15} /> : <AlertCircle size={15} />}
              <span style={{ minWidth: 0, flex: 1 }}>
                <strong style={{ display: "block", fontSize: 11 }}>{item.message}</strong>
                <code style={{ display: "block", color: "var(--muted)", fontSize: 9, marginTop: 3, overflowWrap: "anywhere" }}>
                  {item.code} · {item.pointer || "/"}
                </code>
              </span>
            </>
          );
          const style: CSSProperties = {
            alignItems: "flex-start",
            background: warning ? "var(--warning-soft)" : "var(--danger-soft)",
            border: `1px solid ${warning ? "var(--warning-border)" : "var(--danger-border)"}`,
            borderRadius: 8,
            color: warning ? "var(--warning)" : "var(--danger)",
            display: "flex",
            gap: 8,
            padding: "9px 10px",
            textAlign: "left",
            width: "100%",
          };
          return nodeID && onSelectNode ? (
            <button key={`${item.code}-${item.pointer}-${index}`} type="button" style={style} onClick={() => onSelectNode(nodeID)}>
              {content}
            </button>
          ) : (
            <div key={`${item.code}-${item.pointer}-${index}`} style={style}>{content}</div>
          );
        })}
      </div>
    </section>
  );
}

const panelStyle: CSSProperties = {
  alignItems: "center",
  background: "var(--surface)",
  border: "1px solid var(--line)",
  borderRadius: 10,
  display: "flex",
  flexDirection: "column",
  gap: 6,
  padding: 12,
};
