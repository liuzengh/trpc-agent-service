"use client";

import { Braces, Boxes, Plus, Trash2 } from "lucide-react";
import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";

import type { AgentSpecDocument, ValidationDiagnostic } from "../../lib/control-api";
import {
  createSingleLLMAgentSpec,
  isRenderableAgentSpecV1,
  isEmptyAgentSpec,
  type AgentNodeV1,
  type AgentSpecDiagnostic,
} from "../../lib/agent-spec-v1";
import {
  agentEditorReducer,
  createAgentEditorState,
  mergeAgentSpecDiagnostics,
  parseEditorPositions,
  serializeEditorPositions,
  validateAgentSpecLocally,
  type AgentEditorAction,
  type AgentEditorState,
} from "../../lib/agent-editor-state";
import { Button } from "../ui";
import { WorkspaceFields } from "./workspace-fields";
import { NodeDataFields, RuntimeSummaryFields } from "./runtime-data-fields";
import { AgentCanvas } from "./agent-canvas";
import { DiagnosticsPanel } from "./diagnostics-panel";
import { NumberInput } from "./editor-inputs";
import { SlotSelect } from "./slot-select";
import { RequirementsEditor } from "./requirements-editor";
import { NodeToolbar, NodeStructureFields } from "./node-structure-editor";
import styles from "./agent-spec-editor.module.css";

export function AgentSpecEditor({
  value,
  onChange,
  diagnostics = [],
  disabled = false,
  storageKey,
}: {
  value: AgentSpecDocument;
  onChange(next: AgentSpecDocument): void;
  diagnostics?: readonly ValidationDiagnostic[];
  disabled?: boolean;
  storageKey?: string;
}) {
  const valueKey = useMemo(() => JSON.stringify(value), [value]);
  const [mode, setMode] = useState<"canvas" | "json">(
    isRenderableAgentSpecV1(value) || isEmptyAgentSpec(value) ? "canvas" : "json",
  );
  const [state, setState] = useState<AgentEditorState | null>(() =>
    isRenderableAgentSpecV1(value) ? createAgentEditorState(value) : null,
  );
  const [jsonSource, setJSONSource] = useState(() => JSON.stringify(value, null, 2));
  const [jsonError, setJSONError] = useState("");

  useEffect(() => {
    setJSONSource(JSON.stringify(value, null, 2));
    setJSONError("");
    if (isRenderableAgentSpecV1(value)) {
      setState((current) => current
        ? agentEditorReducer(current, { type: "spec.replace", spec: value })
        : createAgentEditorState(value));
    } else {
      setState(null);
      if (!isEmptyAgentSpec(value)) setMode("json");
    }
  }, [valueKey]);

  useEffect(() => {
    if (!state || !storageKey) return;
    const stored = window.localStorage.getItem(storageKey);
    if (!stored) return;
    const positions = parseEditorPositions(stored, Object.keys(state.spec.nodes));
    if (Object.keys(positions).length > 0) {
      setState((current) => current
        ? { ...current, positions: { ...current.positions, ...positions } }
        : current);
    }
  }, [storageKey]);

  const positionsJSON = state ? serializeEditorPositions(state.positions) : "";
  useEffect(() => {
    if (storageKey && positionsJSON) window.localStorage.setItem(storageKey, positionsJSON);
  }, [positionsJSON, storageKey]);

  const allDiagnostics = useMemo(
    () => mergeAgentSpecDiagnostics(
      validateAgentSpecLocally(value),
      diagnostics as readonly AgentSpecDiagnostic[],
    ),
    [diagnostics, valueKey],
  );

  const dispatch = useCallback((action: AgentEditorAction) => {
    if (!state || disabled) return;
    const next = agentEditorReducer(state, action);
    setState(next);
    if (next.spec !== state.spec) {
      setJSONSource(JSON.stringify(next.spec, null, 2));
      onChange(next.spec);
    }
  }, [disabled, onChange, state]);

  function initializeCanvas() {
    const spec = createSingleLLMAgentSpec();
    setState(createAgentEditorState(spec));
    setJSONSource(JSON.stringify(spec, null, 2));
    setJSONError("");
    setMode("canvas");
    onChange(spec);
  }

  function applyJSON() {
    try {
      const parsed: unknown = JSON.parse(jsonSource);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("AgentSpec 顶层必须是 JSON Object");
      }
      const sensitiveField = findSensitiveAgentSpecField(parsed);
      if (sensitiveField) {
        throw new Error(`敏感字段 ${sensitiveField} 不能写入 Draft`);
      }
      setJSONError("");
      onChange(parsed as Record<string, unknown>);
      if (isRenderableAgentSpecV1(parsed)) {
        setState((current) => current
          ? agentEditorReducer(current, { type: "spec.replace", spec: parsed })
          : createAgentEditorState(parsed));
        setMode("canvas");
      } else {
        setState(null);
      }
    } catch (error) {
      setJSONError(error instanceof Error ? error.message : "JSON 无法解析");
    }
  }

  if (isEmptyAgentSpec(value) && !state) {
    return (
      <section aria-label="初始化 AgentSpec 画布" style={emptyStyle}>
        <Boxes color="var(--primary)" size={30} />
        <h2 style={{ fontSize: 18, margin: 0 }}>Draft 尚未初始化</h2>
        <p style={{ color: "var(--muted)", fontSize: 12, lineHeight: 1.65, margin: 0, maxWidth: 560 }}>
          当前是后端创建的 revision 1、spec={"{}"}。模板只生成浏览器工作副本；点击页面上方“保存 Draft”后才写入 Control API。
        </p>
        <Button disabled={disabled} onClick={initializeCanvas}><Plus size={14} />使用 Single LLM 模板初始化画布</Button>
        <Button disabled={disabled} onClick={() => setMode(mode === "json" ? "canvas" : "json")} variant="ghost"><Braces size={13} />直接编辑 JSON</Button>
        {mode === "json" && <JSONEditor disabled={disabled} error={jsonError} onApply={applyJSON} onChange={setJSONSource} source={jsonSource} />}
      </section>
    );
  }

  return (
    <section className={styles.shell} style={shellStyle}>
      <header style={headerStyle}>
        <div><strong style={{ display: "block", fontSize: 14 }}>AgentSpec V1 编辑器</strong><small style={{ color: "var(--muted)" }}>画布生成 Spec；后端负责最终校验、规范化和发布。</small></div>
        <div style={{ display: "flex", gap: 6 }}>
          <Button disabled={!state} onClick={() => setMode("canvas")} variant={mode === "canvas" ? "primary" : "secondary"}><Boxes size={13} />画布</Button>
          <Button onClick={() => { setJSONSource(JSON.stringify(value, null, 2)); setMode("json"); }} variant={mode === "json" ? "primary" : "secondary"}><Braces size={13} />JSON</Button>
        </div>
      </header>
      {mode === "json" || !state ? (
        <div style={{ display: "grid", gap: 12, padding: 14 }}>
          <JSONEditor disabled={disabled} error={jsonError} onApply={applyJSON} onChange={setJSONSource} source={jsonSource} />
          <DiagnosticsPanel diagnostics={allDiagnostics} />
        </div>
      ) : (
        <div className={`agent-spec-editor-grid ${styles.grid}`}>
          <div style={{ display: "grid", gap: 12, minWidth: 0 }}>
            <NodeToolbar disabled={disabled} dispatch={dispatch} state={state} />
            <AgentCanvas disabled={disabled} diagnostics={allDiagnostics} onAction={dispatch} state={state} />
            <DiagnosticsPanel diagnostics={allDiagnostics} onSelectNode={(nodeID) => dispatch({ type: "node.select", nodeID })} />
          </div>
          <aside style={{ alignContent: "start", display: "grid", gap: 12, minWidth: 0 }}>
            <NodeInspector key={state.selectedNodeID} disabled={disabled} dispatch={dispatch} state={state} />
            <RequirementsEditor disabled={disabled} dispatch={dispatch} state={state} />
            <RuntimeSummaryFields disabled={disabled} dispatch={dispatch} state={state} />
          </aside>
        </div>
      )}
    </section>
  );
}

function JSONEditor({ source, onChange, onApply, error, disabled }: {
  source: string;
  onChange(value: string): void;
  onApply(): void;
  error: string;
  disabled: boolean;
}) {
  return (
    <div style={{ display: "grid", gap: 9, width: "100%" }}>
      <textarea aria-label="AgentSpec JSON" disabled={disabled} onChange={(event) => onChange(event.target.value)} spellCheck={false} style={jsonStyle} value={source} />
      {error && <div role="alert" style={{ color: "var(--danger)", fontSize: 11 }}>{error}</div>}
      <div style={{ alignItems: "center", display: "flex", gap: 10, justifyContent: "space-between" }}>
        <small style={{ color: "var(--muted)" }}>不完整但可解析的 Object 仍可保存为 Draft。</small>
        <Button disabled={disabled} onClick={onApply}>应用 JSON 到工作副本</Button>
      </div>
    </div>
  );
}

function NodeInspector({ state, dispatch, disabled }: {
  state: AgentEditorState;
  dispatch(action: AgentEditorAction): void;
  disabled: boolean;
}) {
  const nodeID = state.selectedNodeID;
  const node = nodeID ? state.spec.nodes[nodeID] : undefined;
  if (!nodeID || !node) return <section style={panelStyle}>请选择画布节点。</section>;
  const replace = (next: AgentNodeV1) => dispatch({ type: "node.replace", nodeID, node: next });
  return (
    <section aria-label="节点属性" style={panelStyle}>
      <header style={panelHeaderStyle}>
        <div><strong>{nodeID}</strong><small style={{ color: "var(--muted)", display: "block", marginTop: 3 }}>{node.kind}</small></div>
        <Button aria-label={`删除节点 ${nodeID}`} disabled={disabled || Object.keys(state.spec.nodes).length <= 1} onClick={() => dispatch({ type: "node.delete", nodeID })} variant="danger"><Trash2 size={13} /></Button>
      </header>
      <label className="field"><span>显示名称</span><input disabled={disabled} onChange={(event) => replace({ ...node, name: event.target.value || undefined } as AgentNodeV1)} value={node.name ?? ""} /></label>
      {node.kind === "llm" && <LLMFields disabled={disabled} node={node} replace={replace} state={state} />}
      <NodeStructureFields disabled={disabled} dispatch={dispatch} nodeID={nodeID} state={state} />
    </section>
  );
}

function LLMFields({ node, state, replace, disabled }: {
  node: Extract<AgentNodeV1, { kind: "llm" }>;
  state: AgentEditorState;
  replace(next: AgentNodeV1): void;
  disabled: boolean;
}) {
  return <>
    <label className="field"><span>Instruction</span><textarea disabled={disabled} onChange={(event) => replace({ ...node, instruction: event.target.value })} rows={5} style={{ font: "inherit" }} value={node.instruction} /></label>
    <label className="field"><span>Model Slot</span><select disabled={disabled} onChange={(event) => replace({ ...node, model_slot: event.target.value })} value={node.model_slot}>{Object.keys(state.spec.requirements.models).map((slot) => <option key={slot}>{slot}</option>)}</select></label>
    <SlotSelect label="Tool Slots" requirement="Tools" declared={state.spec.requirements.tools ?? {}} selected={node.tool_slots} replace={(tool_slots) => replace({ ...node, tool_slots })} disabled={disabled} />
    <NodeDataFields disabled={disabled} node={node} replace={replace} />
    <WorkspaceFields key={state.selectedNodeID} node={node} executors={state.spec.requirements.executors ?? {}} replace={replace} disabled={disabled} />
    <SlotSelect label="Knowledge Slots" requirement="Knowledge" declared={state.spec.requirements.knowledge ?? {}} selected={node.knowledge_slots} replace={(knowledge_slots) => replace({ ...node, knowledge_slots })} disabled={disabled} />
    <div style={{ display: "grid", gap: 7, gridTemplateColumns: "minmax(0,1fr) minmax(0,1fr)" }}>
      <label className="field"><span>Temperature</span><NumberInput disabled={disabled} max={2} min={0} onValueChange={(temperature) => replace({ ...node, generation: generation(temperature, node.generation?.max_output_tokens) })} step={0.1} value={node.generation?.temperature} /></label>
      <label className="field"><span>Max Tokens</span><NumberInput disabled={disabled} max={262144} min={1} onValueChange={(tokens) => replace({ ...node, generation: generation(node.generation?.temperature, tokens) })} value={node.generation?.max_output_tokens} /></label>
    </div>
  </>;
}

function generation(temperature: number | undefined, max_output_tokens: number | undefined) {
  return temperature === undefined && max_output_tokens === undefined ? undefined : { temperature, max_output_tokens };
}

function findSensitiveAgentSpecField(value: unknown, pointer = ""): string | null {
  if (!value || typeof value !== "object") return null;
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      const found = findSensitiveAgentSpecField(value[index], `${pointer}/${index}`);
      if (found) return found;
    }
    return null;
  }
  // Keep this list in lockstep with domain.sensitiveFields. Unknown and legacy
  // fields are publication-invalid, but the backend deliberately permits them
  // at Draft L0 so users can persist incomplete work and inspect diagnostics.
  const sensitive = new Set([
    "api_key", "password", "token",
    "authorization", "credential", "client_secret",
  ]);
  for (const [field, child] of Object.entries(value as Record<string, unknown>)) {
    const fieldPointer = `${pointer}/${field}`;
    if (sensitive.has(field.toLowerCase())) return fieldPointer;
    const found = findSensitiveAgentSpecField(child, fieldPointer);
    if (found) return found;
  }
  return null;
}

const shellStyle: CSSProperties = { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 11, overflow: "hidden" };
const headerStyle: CSSProperties = { alignItems: "center", borderBottom: "1px solid var(--line)", display: "flex", flexWrap: "wrap", gap: 12, justifyContent: "space-between", padding: "13px 14px" };
const emptyStyle: CSSProperties = { alignItems: "center", background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 11, display: "flex", flexDirection: "column", gap: 13, justifyContent: "center", minHeight: 430, padding: 30, textAlign: "center" };
const panelStyle: CSSProperties = { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 10, display: "grid", gap: 12, minWidth: 0, padding: 13 };
const panelHeaderStyle: CSSProperties = { alignItems: "center", borderBottom: "1px solid var(--line)", display: "flex", justifyContent: "space-between", paddingBottom: 10 };
const jsonStyle: CSSProperties = { background: "var(--code-bg)", border: "1px solid var(--code-border)", borderRadius: 9, color: "var(--code-ink)", font: "11px/1.65 ui-monospace,SFMono-Regular,Menlo,monospace", minHeight: 420, padding: 15, resize: "vertical", width: "100%" };
