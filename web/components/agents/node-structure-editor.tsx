"use client";

import { ArrowDown, ArrowUp, LayoutGrid, Plus } from "lucide-react";
import { useEffect, useState, type CSSProperties } from "react";

import {
  getNodeAddError,
  getNodeParentIDs,
  getNodeReparentError,
  type AgentEditorAction,
  type AgentEditorState,
  type NodeAddMode,
} from "../../lib/agent-editor-state";
import { AGENT_NODE_KINDS, type AgentNodeKind } from "../../lib/agent-spec-v1";
import { Button } from "../ui";

interface StructureProps {
  state: AgentEditorState;
  dispatch(action: AgentEditorAction): void;
  disabled: boolean;
}

export function NodeToolbar({ state, dispatch, disabled }: StructureProps) {
  const containers = Object.keys(state.spec.nodes).filter((id) => canOwnChildren(state, id));
  const selectedParent = state.selectedNodeID && canOwnChildren(state, state.selectedNodeID) ? state.selectedNodeID : "";
  const [nodeID, setNodeID] = useState("");
  const [kind, setKind] = useState<AgentNodeKind>(containers.length ? "llm" : "sequence");
  const [mode, setMode] = useState<NodeAddMode>(containers.length ? "child" : "wrap-selected");
  const [chosenParent, setChosenParent] = useState(selectedParent);
  const parentID = containers.includes(chosenParent) ? chosenParent : selectedParent || containers[0] || "";
  const action: Extract<AgentEditorAction, { type: "node.add" }> = {
    type: "node.add", nodeID, kind, mode, parentID, targetID: state.selectedNodeID,
  };
  const error = getNodeAddError(state, action);
  return <section aria-label="节点操作工具栏" style={toolbarStyle}>
    <div style={toolbarFieldsStyle}>
      <label className="field"><span>节点操作</span><select disabled={disabled} onChange={(event) => setMode(event.target.value as NodeAddMode)} value={mode}>
        <option value="child">添加子节点</option>
        <option value="wrap-selected">包装选中节点</option>
        <option value="wrap-root">包装 Root</option>
      </select></label>
      <label className="field"><span>新节点 ID</span><input disabled={disabled} onChange={(event) => setNodeID(event.target.value)} placeholder="例如 review" value={nodeID} /></label>
      <label className="field"><span>新节点类型</span><select disabled={disabled} onChange={(event) => setKind(event.target.value as AgentNodeKind)} value={kind}>{AGENT_NODE_KINDS.map((item) => <option key={item}>{item}</option>)}</select></label>
      {mode === "child" && <label className="field"><span>新节点父节点</span><select disabled={disabled} onChange={(event) => setChosenParent(event.target.value)} value={parentID}>
        {!parentID && <option value="">请先包装一个组合节点</option>}
        {containers.map((id) => <option key={id} value={id}>{id}</option>)}
      </select></label>}
    </div>
    <div style={{ alignItems: "center", display: "flex", flexWrap: "wrap", gap: 8 }}>
      <Button disabled={disabled || Boolean(error)} onClick={() => {
        dispatch(action);
        if (mode === "child") setChosenParent(parentID);
        setNodeID("");
      }}><Plus size={13} />添加节点</Button>
      <Button disabled={disabled} onClick={() => dispatch({ type: "layout.reset" })} variant="secondary"><LayoutGrid size={13} />自动布局</Button>
      <small style={hintStyle}>{mode === "child" ? `添加到 ${parentID || "指定父节点"}；保留此父节点，便于连续添加兄弟节点。` : `只包装 ${mode === "wrap-root" ? `Root ${state.spec.root}` : `选中节点 ${state.selectedNodeID ?? "（未选择）"}`}，保留原有子树。`}</small>
    </div>
    {nodeID && error && <small role="alert" style={{ color: "var(--danger)" }}>{error}</small>}
    {mode === "child" && kind === "loop" && !nodeID && <small style={hintStyle}>Loop 需要已有 Body；请选择包装选中节点或包装 Root。</small>}
  </section>;
}

export function NodeStructureFields({ nodeID, state, dispatch, disabled }: StructureProps & { nodeID: string }) {
  const node = state.spec.nodes[nodeID];
  if (!node) return null;
  const parents = getNodeParentIDs(state.spec, nodeID);
  const destinations = Object.keys(state.spec.nodes).filter((parentID) => !getNodeReparentError(state.spec, nodeID, parentID));
  return <div style={{ display: "grid", gap: 12 }}>
    <div style={{ display: "grid", gap: 8 }}>
      <small style={hintStyle}>当前父节点：{nodeID === state.spec.root ? "Root（无父节点）" : parents.join(", ") || "未连接"}</small>
      {nodeID !== state.spec.root && <NodeConnection key={`parent-${nodeID}`} disabled={disabled} label="移动到父节点" candidates={destinations} buttonLabel="移动节点" onConnect={(parentID) => dispatch({ type: "node.reparent", nodeID, parentID })} />}
    </div>
    {(node.kind === "sequence" || node.kind === "parallel") && <>
      <div style={{ display: "grid", gap: 7 }}>
        <strong style={{ fontSize: 12 }}>Children（有序）</strong>
        {node.children.length === 0 && <small style={hintStyle}>尚无子节点；可在工具栏添加新子节点，或连接下方已有节点。</small>}
        <ol style={{ display: "grid", gap: 6, listStyle: "none", margin: 0, padding: 0 }}>
          {node.children.map((childID, index) => <li key={`${childID}-${index}`} style={childRowStyle}>
            <span style={{ color: "var(--muted)", fontSize: 11 }}>{index + 1}</span>
            <Button disabled={!state.spec.nodes[childID]} onClick={() => dispatch({ type: "node.select", nodeID: childID })} style={{ justifyContent: "flex-start", minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }} variant="ghost">{childID}</Button>
            <Button aria-label={`上移 ${childID}`} disabled={disabled || index === 0} onClick={() => dispatch({ type: "child.move", parentID: nodeID, from: index, to: index - 1 })} style={iconStyle} variant="secondary"><ArrowUp size={13} /></Button>
            <Button aria-label={`下移 ${childID}`} disabled={disabled || index === node.children.length - 1} onClick={() => dispatch({ type: "child.move", parentID: nodeID, from: index, to: index + 1 })} style={iconStyle} variant="secondary"><ArrowDown size={13} /></Button>
          </li>)}
        </ol>
      </div>
      <ExistingChild key={`child-${nodeID}`} disabled={disabled} dispatch={dispatch} nodeID={nodeID} state={state} />
    </>}
    {node.kind === "loop" && <>
      <div style={{ display: "grid", gap: 6 }}><strong style={{ fontSize: 12 }}>Body</strong>
        {node.body ? <Button disabled={!state.spec.nodes[node.body]} onClick={() => dispatch({ type: "node.select", nodeID: node.body })} variant="secondary">{node.body}</Button> : <small style={hintStyle}>Body 暂为空，请连接一个已有节点。</small>}
        {node.body ? <small style={hintStyle}>替换 Body 前，先选中当前 Body 并将其移到其他父节点，避免原子树丢失。</small> : <ExistingChild key={`body-${nodeID}`} disabled={disabled} dispatch={dispatch} nodeID={nodeID} state={state} />}
      </div>
      <IterationField key={nodeID} disabled={disabled} value={node.max_iterations} onChange={(max_iterations) => dispatch({ type: "node.replace", nodeID, node: { ...node, max_iterations } })} />
    </>}
  </div>;
}

function ExistingChild({ nodeID, state, dispatch, disabled }: StructureProps & { nodeID: string }) {
  const candidates = Object.keys(state.spec.nodes).filter((childID) => !getNodeReparentError(state.spec, childID, nodeID));
  return <div style={{ display: "grid", gap: 5 }}>
    <NodeConnection disabled={disabled} label="连接已有子节点" candidates={candidates} buttonLabel="连接子节点" onConnect={(childID) => dispatch({ type: "node.reparent", nodeID: childID, parentID: nodeID })} />
    <small style={hintStyle}>连接时自动从旧父节点移除；根节点、后代环和重复连接不会出现在选项中。</small>
  </div>;
}

function NodeConnection({ candidates, label, buttonLabel, onConnect, disabled }: {
  candidates: string[]; label: string; buttonLabel: string; onConnect(value: string): void; disabled: boolean;
}) {
  const [choice, setChoice] = useState("");
  const value = candidates.includes(choice) ? choice : "";
  return <div style={{ alignItems: "end", display: "grid", gap: 7, gridTemplateColumns: "minmax(0,1fr) auto" }}>
    <label className="field"><span>{label}</span><select disabled={disabled || candidates.length === 0} onChange={(event) => setChoice(event.target.value)} value={value}>
      <option value="">{candidates.length ? "选择节点" : "暂无可连接节点"}</option>
      {candidates.map((id) => <option key={id}>{id}</option>)}
    </select></label>
    <Button disabled={disabled || !value} onClick={() => { onConnect(value); setChoice(""); }} style={{ height: 34 }} variant="secondary">{buttonLabel}</Button>
  </div>;
}

function IterationField({ value, onChange, disabled }: { value: number; onChange(value: number): void; disabled: boolean }) {
  const [draft, setDraft] = useState(String(value));
  useEffect(() => {
    if (Number(draft) !== value) setDraft(String(value));
  }, [value]);
  return <label className="field"><span>最大迭代次数</span><input aria-label="最大迭代次数" disabled={disabled} min={1} max={32} onChange={(event) => {
    setDraft(event.target.value);
    const parsed = Number(event.target.value);
    if (Number.isFinite(parsed)) onChange(parsed);
  }} type="number" value={draft} /><small>允许暂时留空；发布前请填写 1–32 的整数。</small></label>;
}

function canOwnChildren(state: AgentEditorState, nodeID: string): boolean {
  const node = state.spec.nodes[nodeID];
  return node?.kind === "sequence" || node?.kind === "parallel";
}

const hintStyle: CSSProperties = { color: "var(--muted)", fontSize: 11, lineHeight: 1.5 };
const toolbarStyle: CSSProperties = { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 11, display: "grid", gap: 10, padding: 12 };
const toolbarFieldsStyle: CSSProperties = { display: "grid", gap: 8, gridTemplateColumns: "repeat(auto-fit,minmax(135px,1fr))" };
const childRowStyle: CSSProperties = { alignItems: "center", background: "var(--surface-subtle)", border: "1px solid var(--line)", borderRadius: 7, display: "grid", gap: 4, gridTemplateColumns: "18px minmax(0,1fr) 28px 28px", padding: 5 };
const iconStyle: CSSProperties = { height: 28, padding: 0, width: 28 };
