import {
  AGENT_SPEC_LIMITS,
  cloneAgentSpec,
  escapeJSONPointer,
  getNodeReferences,
  isAgentSpecIdentifier,
  type AgentNodeKind,
  type AgentNodeV1,
  type AgentSpecDiagnostic,
  type AgentSpecV1,
  validateAgentSpecShape,
} from "./agent-spec-v1";

export interface CanvasPoint {
  x: number;
  y: number;
}

export interface AgentEditorState {
  spec: AgentSpecV1;
  selectedNodeID: string | null;
  positions: Record<string, CanvasPoint>;
  focusRevision?: number;
}

export type RequirementKind = "models" | "tools" | "knowledge" | "executors";
export type NodeAddMode = "child" | "wrap-selected" | "wrap-root";

export type AgentEditorAction =
  | { type: "spec.replace"; spec: AgentSpecV1 }
  | { type: "runtime.set"; runtime: AgentSpecV1["runtime"] }
  | { type: "node.select"; nodeID: string | null }
  | { type: "node.move"; nodeID: string; position: CanvasPoint }
  | { type: "node.add"; nodeID: string; kind: AgentNodeKind; parentID?: string | null; mode?: NodeAddMode; targetID?: string | null }
  | { type: "node.reparent"; nodeID: string; parentID: string }
  | { type: "layout.reset" }
  | { type: "node.delete"; nodeID: string }
  | { type: "node.replace"; nodeID: string; node: AgentNodeV1 }
  | { type: "root.set"; nodeID: string }
  | { type: "child.add"; parentID: string; childID: string }
  | { type: "child.remove"; parentID: string; index: number }
  | { type: "child.move"; parentID: string; from: number; to: number }
  | { type: "loop.body.set"; nodeID: string; body: string }
  | { type: "requirement.model.set"; slot: string; capabilities: string[] }
  | { type: "requirement.capability.set"; kind: "tools" | "knowledge" | "executors"; slot: string; capability: string }
  | { type: "requirement.delete"; kind: RequirementKind; slot: string };

export function createAgentEditorState(spec: AgentSpecV1): AgentEditorState {
  return {
    spec: cloneAgentSpec(spec),
    selectedNodeID: spec.nodes[spec.root] ? spec.root : Object.keys(spec.nodes)[0] ?? null,
    positions: createDefaultNodePositions(spec),
  };
}

export function reconcileAgentEditorState(state: AgentEditorState, spec: AgentSpecV1): AgentEditorState {
  const defaults = createDefaultNodePositions(spec);
  const positions = Object.fromEntries(
    Object.keys(spec.nodes).map((nodeID) => [nodeID, state.positions[nodeID] ?? defaults[nodeID]]),
  );
  return {
    ...state,
    spec: cloneAgentSpec(spec),
    selectedNodeID: state.selectedNodeID && spec.nodes[state.selectedNodeID]
      ? state.selectedNodeID
      : spec.nodes[spec.root]
        ? spec.root
        : Object.keys(spec.nodes)[0] ?? null,
    positions,
  };
}

export function agentEditorReducer(state: AgentEditorState, action: AgentEditorAction): AgentEditorState {
  switch (action.type) {
    case "spec.replace":
      return reconcileAgentEditorState(state, action.spec);
    case "runtime.set": {
      const spec = cloneAgentSpec(state.spec);
      if (action.runtime === undefined) delete spec.runtime;
      else spec.runtime = structuredClone(action.runtime);
      return { ...state, spec };
    }
    case "node.select":
      return action.nodeID === null || state.spec.nodes[action.nodeID]
        ? { ...state, selectedNodeID: action.nodeID, focusRevision: (state.focusRevision ?? 0) + 1 }
        : state;
    case "node.move":
      if (!state.spec.nodes[action.nodeID]) return state;
      return {
        ...state,
        positions: {
          ...state.positions,
          [action.nodeID]: {
            x: Math.max(0, Math.round(action.position.x)),
            y: Math.max(0, Math.round(action.position.y)),
          },
        },
      };
    case "node.add":
      return action.mode ? addNodeExplicitly(state, action) : addNode(state, action.nodeID, action.kind, action.parentID ?? null);
    case "node.reparent":
      return reparentNode(state, action.nodeID, action.parentID);
    case "layout.reset":
      return { ...state, positions: createDefaultNodePositions(state.spec), focusRevision: (state.focusRevision ?? 0) + 1 };
    case "node.delete":
      return deleteNode(state, action.nodeID);
    case "node.replace": {
      if (!state.spec.nodes[action.nodeID]) return state;
      return withSpec(state, {
        ...state.spec,
        nodes: { ...state.spec.nodes, [action.nodeID]: cloneNode(action.node) },
      });
    }
    case "root.set":
      return state.spec.nodes[action.nodeID] ? withSpec(state, { ...state.spec, root: action.nodeID }) : state;
    case "child.add":
      return updateChildren(state, action.parentID, (children) =>
        action.childID === action.parentID || children.includes(action.childID) ? children : [...children, action.childID],
      );
    case "child.remove":
      return updateChildren(state, action.parentID, (children) => children.filter((_, index) => index !== action.index));
    case "child.move":
      return updateChildren(state, action.parentID, (children) => moveArrayItem(children, action.from, action.to));
    case "loop.body.set": {
      const node = state.spec.nodes[action.nodeID];
      if (!node || node.kind !== "loop" || action.body === action.nodeID) return state;
      return withSpec(state, {
        ...state.spec,
        nodes: { ...state.spec.nodes, [action.nodeID]: { ...node, body: action.body } },
      });
    }
    case "requirement.model.set": {
      if (!isAgentSpecIdentifier(action.slot)) return state;
      return withSpec(state, {
        ...state.spec,
        requirements: {
          ...state.spec.requirements,
          models: {
            ...state.spec.requirements.models,
            [action.slot]: { capabilities: unique(action.capabilities) },
          },
        },
      });
    }
    case "requirement.capability.set": {
      if (!isAgentSpecIdentifier(action.slot)) return state;
      return withSpec(state, {
        ...state.spec,
        requirements: {
          ...state.spec.requirements,
          [action.kind]: {
            ...state.spec.requirements[action.kind],
            [action.slot]: { capability: action.capability },
          },
        },
      });
    }
    case "requirement.delete": {
      const current = state.spec.requirements[action.kind] ?? {};
      if (!(action.slot in current)) return state;
      const next = { ...current };
      delete next[action.slot];
      return withSpec(state, {
        ...state.spec,
        requirements: { ...state.spec.requirements, [action.kind]: next },
      });
    }
  }
}

export function createDefaultNodePositions(spec: AgentSpecV1): Record<string, CanvasPoint> {
  const depth = new Map<string, number>();
  const queue: string[] = [];
  if (spec.nodes[spec.root]) {
    depth.set(spec.root, 0);
    queue.push(spec.root);
  }
  while (queue.length > 0) {
    const current = queue.shift() as string;
    const currentDepth = depth.get(current) ?? 0;
    for (const child of getNodeReferences(spec.nodes[current])) {
      if (!spec.nodes[child] || depth.has(child)) continue;
      depth.set(child, currentDepth + 1);
      queue.push(child);
    }
  }
  let unreachableDepth = Math.max(0, ...depth.values()) + 1;
  for (const nodeID of Object.keys(spec.nodes).sort()) {
    if (!depth.has(nodeID)) depth.set(nodeID, unreachableDepth++);
  }
  const levels = new Map<number, string[]>();
  for (const [nodeID, nodeDepth] of depth.entries()) {
    const level = levels.get(nodeDepth) ?? [];
    level.push(nodeID);
    levels.set(nodeDepth, level);
  }
  const result: Record<string, CanvasPoint> = {};
  for (const [nodeDepth, nodeIDs] of [...levels.entries()].sort(([left], [right]) => left - right)) {
    nodeIDs.forEach((nodeID, index) => {
      result[nodeID] = { x: 34 + index * 218, y: 30 + nodeDepth * 146 };
    });
  }
  return result;
}

export function validateAgentSpecLocally(value: unknown): AgentSpecDiagnostic[] {
  const shapeDiagnostics = validateAgentSpecShape(value);
  if (shapeDiagnostics.some((item) => item.severity === "error")) return shapeDiagnostics;
  const spec = value as AgentSpecV1;
  const diagnostics: AgentSpecDiagnostic[] = [];
  const summary = spec.runtime?.summary;
  if (summary?.enabled) {
    const slot = summary.model_slot ?? "";
    const model = Object.hasOwn(spec.requirements.models, slot) ? spec.requirements.models[slot] : undefined;
    if (!model) diagnostics.push(error("AGENT_SPEC_MODEL_SLOT_NOT_FOUND", "/runtime/summary/model_slot", "Summary 模型槽未声明。"));
    else if (!model.capabilities.includes("chat")) diagnostics.push(error("AGENT_SPEC_SUMMARY_MODEL_CAPABILITY", "/runtime/summary/model_slot", "Summary 模型必须声明 chat 能力。"));
  }
  for (const [id, node] of Object.entries(spec.nodes)) {
    if (node.kind === "llm" && node.add_session_summary && !summary?.enabled) diagnostics.push(error("AGENT_SPEC_SUMMARY_NOT_ENABLED", `/nodes/${escapeJSONPointer(id)}/add_session_summary`, "消费摘要前必须启用 Agent 级 Summary。", id));
  }
  const nodeIDs = Object.keys(spec.nodes).sort();
  const parents = new Map<string, string[]>();
  const adjacency = new Map<string, string[]>();

  if (!spec.nodes[spec.root]) {
    diagnostics.push(error("AGENT_SPEC_ROOT_NOT_FOUND", "/root", "Root 节点不存在。"));
  }

  for (const nodeID of nodeIDs) {
    const node = spec.nodes[nodeID];
    const refs = getNodeReferences(node);
    adjacency.set(nodeID, refs.filter((ref) => Boolean(spec.nodes[ref])));
    refs.forEach((ref, index) => {
      const field = node.kind === "loop" ? "body" : `children/${index}`;
      if (!spec.nodes[ref]) {
        diagnostics.push(
          error(
            "AGENT_SPEC_NODE_REFERENCE_NOT_FOUND",
            `/nodes/${escapeJSONPointer(nodeID)}/${field}`,
            `引用的节点 ${ref} 不存在。`,
            nodeID,
          ),
        );
      } else {
        const owners = parents.get(ref) ?? [];
        owners.push(nodeID);
        parents.set(ref, owners);
      }
    });
    if (node.kind === "llm") validateSlots(nodeID, node, spec, diagnostics);
  }

  if ((parents.get(spec.root) ?? []).length > 0) {
    diagnostics.push(error("AGENT_SPEC_ROOT_HAS_PARENT", "/root", "Root 节点不能被其他节点引用。"));
  }
  for (const nodeID of nodeIDs) {
    if ((parents.get(nodeID) ?? []).length > 1) {
      diagnostics.push(
        error(
          "AGENT_SPEC_NODE_MULTIPLE_PARENTS",
          `/nodes/${escapeJSONPointer(nodeID)}`,
          "节点存在多个父节点。",
          nodeID,
        ),
      );
    }
  }

  const visitState = new Map<string, 0 | 1 | 2>();
  let reportedCycle = false;
  const visit = (nodeID: string, depth: number): void => {
    if (visitState.get(nodeID) === 1) {
      if (!reportedCycle) {
        diagnostics.push(
          error(
            "AGENT_SPEC_STRUCTURE_CYCLE",
            `/nodes/${escapeJSONPointer(nodeID)}`,
            "节点结构中存在环。",
            nodeID,
          ),
        );
        reportedCycle = true;
      }
      return;
    }
    if (visitState.get(nodeID) === 2) return;
    visitState.set(nodeID, 1);
    if (depth > AGENT_SPEC_LIMITS.depth) {
      diagnostics.push(
        error(
          "AGENT_SPEC_LIMIT_EXCEEDED",
          `/nodes/${escapeJSONPointer(nodeID)}`,
          `节点深度超过 ${AGENT_SPEC_LIMITS.depth}。`,
          nodeID,
        ),
      );
    }
    for (const child of adjacency.get(nodeID) ?? []) visit(child, depth + 1);
    visitState.set(nodeID, 2);
  };
  if (spec.nodes[spec.root]) visit(spec.root, 1);
  for (const nodeID of nodeIDs) {
    if (!visitState.has(nodeID)) {
      diagnostics.push(
        error(
          "AGENT_SPEC_NODE_UNREACHABLE",
          `/nodes/${escapeJSONPointer(nodeID)}`,
          "节点无法从 Root 到达。",
          nodeID,
        ),
      );
    }
  }
  appendUnusedSlotWarnings(spec, diagnostics);
  return sortDiagnostics(diagnostics);
}

export function mergeAgentSpecDiagnostics(
  local: readonly AgentSpecDiagnostic[],
  server: readonly AgentSpecDiagnostic[],
): AgentSpecDiagnostic[] {
  const uniqueDiagnostics = new Map<string, AgentSpecDiagnostic>();
  for (const item of [...local, ...server]) {
    // The server report is authoritative and intentionally overwrites a local
    // hint for the same stable diagnostic identity (including node_id shape).
    uniqueDiagnostics.set(`${item.code}\u0000${item.severity}\u0000${item.pointer}`, { ...item });
  }
  return sortDiagnostics([...uniqueDiagnostics.values()]);
}

export function serializeEditorPositions(positions: Record<string, CanvasPoint>): string {
  return JSON.stringify({ positions });
}

export function parseEditorPositions(value: string, nodeIDs: readonly string[]): Record<string, CanvasPoint> {
  try {
    const parsed = JSON.parse(value) as { positions?: unknown };
    if (!parsed || typeof parsed !== "object" || !parsed.positions || typeof parsed.positions !== "object") return {};
    const allowed = new Set(nodeIDs);
    const result: Record<string, CanvasPoint> = {};
    for (const [nodeID, position] of Object.entries(parsed.positions as Record<string, unknown>)) {
      if (!allowed.has(nodeID) || !position || typeof position !== "object") continue;
      const candidate = position as Partial<CanvasPoint>;
      if (typeof candidate.x === "number" && Number.isFinite(candidate.x) && typeof candidate.y === "number" && Number.isFinite(candidate.y)) {
        result[nodeID] = { x: Math.max(0, Math.round(candidate.x)), y: Math.max(0, Math.round(candidate.y)) };
      }
    }
    return result;
  } catch {
    return {};
  }
}

function addNode(state: AgentEditorState, nodeID: string, kind: AgentNodeKind, parentID: string | null): AgentEditorState {
  if (!isAgentSpecIdentifier(nodeID) || state.spec.nodes[nodeID] || Object.keys(state.spec.nodes).length >= AGENT_SPEC_LIMITS.nodes) {
    return state;
  }
  let spec = cloneAgentSpec(state.spec);
  if (kind === "llm") {
    let modelSlot = Object.keys(spec.requirements.models)[0];
    if (!modelSlot) {
      modelSlot = "primary";
      spec.requirements.models.primary = { capabilities: ["chat"] };
    }
    spec.nodes[nodeID] = {
      kind: "llm",
      name: "新 LLM 节点",
      instruction: "描述该节点需要完成的任务。",
      model_slot: modelSlot,
      tool_slots: [],
      knowledge_slots: [],
    };
    const parent = parentID ? spec.nodes[parentID] : undefined;
    if (parent?.kind === "sequence" || parent?.kind === "parallel") {
      parent.children = unique([...parent.children, nodeID]);
    }
  } else if (kind === "sequence" || kind === "parallel") {
    spec.nodes[nodeID] = { kind, name: kind === "sequence" ? "顺序编排" : "并行编排", children: [spec.root] };
    spec.root = nodeID;
  } else {
    spec.nodes[nodeID] = { kind: "loop", name: "循环编排", body: spec.root, max_iterations: 3 };
    spec.root = nodeID;
  }
  const y = Math.max(0, ...Object.values(state.positions).map((position) => position.y)) + 146;
  return {
    spec,
    selectedNodeID: nodeID,
    positions: { ...state.positions, [nodeID]: { x: 34, y } },
  };
}

export function getNodeParentIDs(spec: AgentSpecV1, nodeID: string): string[] {
  return Object.entries(spec.nodes).filter(([, node]) => getNodeReferences(node).includes(nodeID)).map(([id]) => id);
}

export function getNodeReparentError(spec: AgentSpecV1, nodeID: string, parentID: string): string | null {
  if (!spec.nodes[nodeID]) return "节点不存在。";
  if (nodeID === spec.root) return "Root 不能移动到其他节点下；请使用包装 Root。";
  const parent = spec.nodes[parentID];
  if (!parent || parent.kind === "llm") return "目标父节点必须是 sequence、parallel 或空 loop。";
  if (isNodeReachable(spec, nodeID, parentID)) return "目标是当前节点或其后代，会产生结构环。";
  if (getNodeParentIDs(spec, nodeID).includes(parentID)) return "节点已经属于这个父节点。";
  if (parent.kind === "loop" && parent.body) return "Loop 已有 Body；请先把原 Body 移到其他父节点。";
  if ((parent.kind === "sequence" || parent.kind === "parallel") && parent.children.length >= AGENT_SPEC_LIMITS.children) {
    return `父节点最多包含 ${AGENT_SPEC_LIMITS.children} 个子节点。`;
  }
  return null;
}

export function getNodeAddError(state: AgentEditorState, action: Extract<AgentEditorAction, { type: "node.add" }>): string | null {
  const { spec } = state;
  if (!isAgentSpecIdentifier(action.nodeID)) return "节点 ID 必须以小写字母开头，只含小写字母、数字、下划线或连字符，最多 64 字符。";
  if (spec.nodes[action.nodeID]) return "节点 ID 已存在，请使用新的 ID。";
  if (Object.keys(spec.nodes).length >= AGENT_SPEC_LIMITS.nodes) return `最多添加 ${AGENT_SPEC_LIMITS.nodes} 个节点。`;
  if (action.mode === "child") {
    const parent = action.parentID ? spec.nodes[action.parentID] : undefined;
    if (!parent || (parent.kind !== "sequence" && parent.kind !== "parallel")) return "添加子节点需要选择 sequence 或 parallel 父节点。";
    if (action.kind === "loop") return "Loop 需要已有 Body，请选择包装选中节点或包装 Root。";
    if (parent.children.length >= AGENT_SPEC_LIMITS.children) return `父节点最多包含 ${AGENT_SPEC_LIMITS.children} 个子节点。`;
  } else {
    if (action.kind === "llm") return "LLM 不能包装节点，请选择 sequence、parallel 或 loop。";
    const targetID = action.mode === "wrap-root" ? spec.root : action.targetID ?? state.selectedNodeID;
    if (!targetID || !spec.nodes[targetID]) return "请选择要包装的已有节点。";
    const parents = getNodeParentIDs(spec, targetID);
    if ((targetID === spec.root && parents.length > 0) || (targetID !== spec.root && parents.length !== 1)) {
      return "请先将待包装节点连接到唯一父节点。";
    }
  }
  return null;
}

function addNodeExplicitly(state: AgentEditorState, action: Extract<AgentEditorAction, { type: "node.add" }>): AgentEditorState {
  if (getNodeAddError(state, action)) return state;
  const spec = cloneAgentSpec(state.spec);
  const { nodeID, kind } = action;
  if (action.mode === "child") {
    const parent = spec.nodes[action.parentID!];
    if (parent.kind !== "sequence" && parent.kind !== "parallel") return state;
    if (kind === "llm") {
      const modelSlot = Object.keys(spec.requirements.models)[0] ?? "primary";
      spec.requirements.models[modelSlot] ??= { capabilities: ["chat"] };
      spec.nodes[nodeID] = { kind, name: "新 LLM 节点", instruction: "描述该节点需要完成的任务。", model_slot: modelSlot, tool_slots: [], knowledge_slots: [] };
    } else if (kind === "sequence" || kind === "parallel") {
      spec.nodes[nodeID] = { kind, name: kind === "sequence" ? "顺序编排" : "并行编排", children: [] };
    }
    parent.children.push(nodeID);
  } else {
    const targetID = action.mode === "wrap-root" ? spec.root : action.targetID ?? state.selectedNodeID!;
    if (kind === "loop") spec.nodes[nodeID] = { kind, name: "循环编排", body: targetID, max_iterations: 3 };
    else if (kind === "sequence" || kind === "parallel") spec.nodes[nodeID] = { kind, name: kind === "sequence" ? "顺序编排" : "并行编排", children: [targetID] };
    if (targetID === spec.root) spec.root = nodeID;
    else {
      const parent = spec.nodes[getNodeParentIDs(state.spec, targetID)[0]];
      if (parent.kind === "loop") parent.body = nodeID;
      else if (parent.kind === "sequence" || parent.kind === "parallel") parent.children = parent.children.map((id) => id === targetID ? nodeID : id);
    }
  }
  const y = Math.max(0, ...Object.values(state.positions).map((position) => position.y)) + 146;
  return { spec, selectedNodeID: nodeID, positions: { ...state.positions, [nodeID]: { x: 34, y } } };
}

function reparentNode(state: AgentEditorState, nodeID: string, parentID: string): AgentEditorState {
  if (getNodeReparentError(state.spec, nodeID, parentID)) return state;
  const spec = cloneAgentSpec(state.spec);
  for (const node of Object.values(spec.nodes)) {
    if (node.kind === "sequence" || node.kind === "parallel") node.children = node.children.filter((id) => id !== nodeID);
    else if (node.kind === "loop" && node.body === nodeID) node.body = "";
  }
  const parent = spec.nodes[parentID];
  if (parent.kind === "loop") parent.body = nodeID;
  else if (parent.kind === "sequence" || parent.kind === "parallel") parent.children.push(nodeID);
  return withSpec(state, spec);
}

function isNodeReachable(spec: AgentSpecV1, from: string, target: string): boolean {
  const pending = [from];
  const visited = new Set<string>();
  while (pending.length > 0) {
    const current = pending.pop()!;
    if (current === target) return true;
    if (visited.has(current)) continue;
    visited.add(current);
    if (spec.nodes[current]) pending.push(...getNodeReferences(spec.nodes[current]));
  }
  return false;
}

function deleteNode(state: AgentEditorState, nodeID: string): AgentEditorState {
  if (!state.spec.nodes[nodeID] || Object.keys(state.spec.nodes).length <= 1) return state;
  const spec = cloneAgentSpec(state.spec);
  delete spec.nodes[nodeID];
  const nextRoot = spec.root === nodeID ? Object.keys(spec.nodes).sort()[0] : spec.root;
  spec.root = nextRoot;
  for (const [parentID, node] of Object.entries(spec.nodes)) {
    if (node.kind === "sequence" || node.kind === "parallel") {
      node.children = node.children.filter((child) => child !== nodeID);
    } else if (node.kind === "loop" && node.body === nodeID) {
      node.body = Object.keys(spec.nodes).find((candidate) => candidate !== parentID) ?? nodeID;
    }
  }
  const positions = { ...state.positions };
  delete positions[nodeID];
  return {
    spec,
    positions,
    selectedNodeID: state.selectedNodeID === nodeID ? spec.root : state.selectedNodeID,
  };
}

function updateChildren(
  state: AgentEditorState,
  parentID: string,
  update: (children: string[]) => string[],
): AgentEditorState {
  const node = state.spec.nodes[parentID];
  if (!node || (node.kind !== "sequence" && node.kind !== "parallel")) return state;
  const children = update([...node.children]);
  return withSpec(state, {
    ...state.spec,
    nodes: { ...state.spec.nodes, [parentID]: { ...node, children } },
  });
}

function withSpec(state: AgentEditorState, spec: AgentSpecV1): AgentEditorState {
  return { ...state, spec };
}

function cloneNode(node: AgentNodeV1): AgentNodeV1 {
  return JSON.parse(JSON.stringify(node)) as AgentNodeV1;
}

function moveArrayItem(values: string[], from: number, to: number): string[] {
  if (from < 0 || from >= values.length || to < 0 || to >= values.length || from === to) return values;
  const result = [...values];
  const [item] = result.splice(from, 1);
  result.splice(to, 0, item);
  return result;
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

function validateSlots(
  nodeID: string,
  node: Extract<AgentNodeV1, { kind: "llm" }>,
  spec: AgentSpecV1,
  diagnostics: AgentSpecDiagnostic[],
): void {
  const pointer = `/nodes/${escapeJSONPointer(nodeID)}`;
  if (node.workspace) {
    if (!Object.hasOwn(spec.requirements.executors ?? {}, node.workspace.executor_slot)) diagnostics.push(error("AGENT_SPEC_EXECUTOR_SLOT_NOT_FOUND", `${pointer}/workspace/executor_slot`, "Executor Slot 未声明。", nodeID));
    if (node.workspace.tools.includes("workspace_save_artifact") && node.artifact?.enabled !== true) diagnostics.push(error("AGENT_SPEC_WORKSPACE_ARTIFACT_REQUIRED", `${pointer}/workspace/tools`, "workspace_save_artifact 需要同节点显式启用 Artifact 服务。", nodeID));
  }
  if (!spec.requirements.models[node.model_slot]) {
    diagnostics.push(error("AGENT_SPEC_MODEL_SLOT_NOT_FOUND", `${pointer}/model_slot`, "Model Slot 未声明。", nodeID));
  }
  node.tool_slots.forEach((slot, index) => {
    if (!spec.requirements.tools[slot]) {
      diagnostics.push(error("AGENT_SPEC_TOOL_SLOT_NOT_FOUND", `${pointer}/tool_slots/${index}`, "Tool Slot 未声明。", nodeID));
    }
  });
  node.knowledge_slots.forEach((slot, index) => {
    if (!spec.requirements.knowledge[slot]) {
      diagnostics.push(
        error("AGENT_SPEC_KNOWLEDGE_SLOT_NOT_FOUND", `${pointer}/knowledge_slots/${index}`, "Knowledge Slot 未声明。", nodeID),
      );
    }
  });
}

function appendUnusedSlotWarnings(spec: AgentSpecV1, diagnostics: AgentSpecDiagnostic[]): void {
  const usedModels = new Set<string>();
  if (spec.runtime?.summary?.enabled && spec.runtime.summary.model_slot) usedModels.add(spec.runtime.summary.model_slot);
  const usedTools = new Set<string>();
  const usedKnowledge = new Set<string>();
  for (const node of Object.values(spec.nodes)) {
    if (node.kind !== "llm") continue;
    usedModels.add(node.model_slot);
    node.tool_slots.forEach((slot) => usedTools.add(slot));
    node.knowledge_slots.forEach((slot) => usedKnowledge.add(slot));
  }
  appendWarnings(spec.requirements.models, usedModels, "models", "AGENT_SPEC_UNUSED_MODEL_SLOT", diagnostics);
  appendWarnings(spec.requirements.tools, usedTools, "tools", "AGENT_SPEC_UNUSED_TOOL_SLOT", diagnostics);
  appendWarnings(spec.requirements.knowledge, usedKnowledge, "knowledge", "AGENT_SPEC_UNUSED_KNOWLEDGE_SLOT", diagnostics);
}

function appendWarnings(
  slots: Record<string, unknown>,
  used: Set<string>,
  kind: RequirementKind,
  code: string,
  diagnostics: AgentSpecDiagnostic[],
): void {
  for (const slot of Object.keys(slots).sort()) {
    if (used.has(slot)) continue;
    diagnostics.push({
      code,
      severity: "warning",
      pointer: `/requirements/${kind}/${escapeJSONPointer(slot)}`,
      node_id: null,
      message: `${kind} Slot ${slot} 已声明但未使用。`,
    });
  }
}

function error(code: string, pointer: string, message: string, nodeID: string | null = null): AgentSpecDiagnostic {
  return { code, severity: "error", pointer, node_id: nodeID, message };
}

function sortDiagnostics(diagnostics: AgentSpecDiagnostic[]): AgentSpecDiagnostic[] {
  return diagnostics.sort((left, right) =>
    left.pointer.localeCompare(right.pointer) ||
    left.severity.localeCompare(right.severity) ||
    left.code.localeCompare(right.code),
  );
}
