export const AGENT_SPEC_SCHEMA_VERSION = "v1" as const;

export const AGENT_NODE_KINDS = ["llm", "sequence", "parallel", "loop"] as const;

export type AgentNodeKind = (typeof AGENT_NODE_KINDS)[number];

export interface ModelRequirementV1 {
  capabilities: string[];
}

export interface CapabilityRequirementV1 {
  capability: string;
}

export interface AgentRequirementsV1 {
  executors?: Record<string, CapabilityRequirementV1>;
  models: Record<string, ModelRequirementV1>;
  tools: Record<string, CapabilityRequirementV1>;
  knowledge: Record<string, CapabilityRequirementV1>;
}

export interface GenerationOptionsV1 {
  temperature?: number;
  max_output_tokens?: number;
}

export const WORKSPACE_TOOLS = ["workspace_exec", "workspace_save_artifact"] as const;
export type WorkspaceTool = (typeof WORKSPACE_TOOLS)[number];

export const MEMORY_TOOLS = ["memory_add", "memory_update", "memory_delete", "memory_clear", "memory_search", "memory_load"] as const;
export type MemoryTool = (typeof MEMORY_TOOLS)[number];
export type AgentRuntimeV1 = { summary?: { enabled: boolean; model_slot?: string; event_threshold?: number } };

interface NamedNodeV1 {
  name?: string;
}

export interface LLMNodeV1 extends NamedNodeV1 {
  kind: "llm";
  instruction: string;
  model_slot: string;
  tool_slots: string[];
  knowledge_slots: string[];
  generation?: GenerationOptionsV1;
  workspace?: { executor_slot: string; tools: WorkspaceTool[] };
  memory?: { tools: MemoryTool[]; preload_limit?: number };
  artifact?: { enabled: boolean };
  add_session_summary?: boolean;
}

export interface SequenceNodeV1 extends NamedNodeV1 {
  kind: "sequence";
  children: string[];
}

export interface ParallelNodeV1 extends NamedNodeV1 {
  kind: "parallel";
  children: string[];
}

export interface LoopNodeV1 extends NamedNodeV1 {
  kind: "loop";
  body: string;
  max_iterations: number;
}

export type AgentNodeV1 = LLMNodeV1 | SequenceNodeV1 | ParallelNodeV1 | LoopNodeV1;

export interface AgentSpecV1 {
  schema_version: typeof AGENT_SPEC_SCHEMA_VERSION;
  root: string;
  runtime?: AgentRuntimeV1;
  requirements: AgentRequirementsV1;
  nodes: Record<string, AgentNodeV1>;
}

/** The API deliberately permits an empty or otherwise incomplete JSON object as a Draft. */
export type EmptyAgentSpec = Record<string, never>;
export type AgentSpecDraft = AgentSpecV1 | EmptyAgentSpec | Record<string, unknown>;

export type AgentSpecDiagnosticSeverity = "error" | "warning";

/** Structural match for the diagnostic objects returned by Control API. */
export interface AgentSpecDiagnostic {
  code: string;
  severity: AgentSpecDiagnosticSeverity;
  pointer: string;
  node_id?: string | null;
  message: string;
}

export const AGENT_SPEC_LIMITS = {
  nodes: 128,
  depth: 16,
  children: 64,
  modelSlots: 16,
  executorSlots: 16,
  toolSlots: 64,
  knowledgeSlots: 32,
  capabilitiesPerModel: 16,
  loopIterations: 32,
  instructionBytes: 65_536,
  maxOutputTokens: 262_144,
} as const;

const IDENTIFIER_PATTERN = /^[a-z][a-z0-9_-]{0,63}$/;
const CAPABILITY_PATTERN = /^[a-z][a-z0-9_.-]{0,127}$/;

export function isAgentSpecIdentifier(value: string): boolean {
  return IDENTIFIER_PATTERN.test(value);
}

export function isAgentCapability(value: string): boolean {
  return CAPABILITY_PATTERN.test(value);
}

export function createSingleLLMAgentSpec(): AgentSpecV1 {
  return {
    schema_version: AGENT_SPEC_SCHEMA_VERSION,
    root: "assistant",
    requirements: {
      models: { primary: { capabilities: ["chat"] } },
      tools: {},
      knowledge: {},
    },
    nodes: {
      assistant: {
        kind: "llm",
        name: "通用助手",
        instruction: "准确回答用户问题。",
        model_slot: "primary",
        tool_slots: [],
        knowledge_slots: [],
      },
    },
  };
}

export function cloneAgentSpec(spec: AgentSpecV1): AgentSpecV1 {
  return JSON.parse(JSON.stringify(spec)) as AgentSpecV1;
}

export function isEmptyAgentSpec(value: unknown): value is EmptyAgentSpec {
  return isRecord(value) && Object.keys(value).length === 0;
}

export function isAgentSpecV1(value: unknown): value is AgentSpecV1 {
  return validateAgentSpecShape(value).length === 0;
}

/**
 * Checks only the typed V1 structure the canvas can preserve and render. Empty
 * or temporarily invalid field values remain editable; publishing still uses
 * the strict shape and domain-semantic validators. Unknown fields stay in JSON
 * so visual edits cannot silently remove data the editor does not understand.
 */
export function isRenderableAgentSpecV1(value: unknown): value is AgentSpecV1 {
  return hasRenderableFields(value, ["schema_version", "root", "requirements", "nodes"], ["runtime"])
    && (!("runtime" in value) || renderableRuntime(value.runtime))
    && value.schema_version === AGENT_SPEC_SCHEMA_VERSION
    && typeof value.root === "string"
    && isRenderableRequirements(value.requirements)
    && isRenderableMap(value.nodes, isRenderableNode);
}

function isRenderableStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && Array.from(value).every((item) => typeof item === "string");
}

function isRenderableRecord(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasRenderableFields(
  value: unknown,
  required: readonly string[],
  optional: readonly string[] = [],
): value is Record<string, unknown> {
  return isRenderableRecord(value)
    && required.every((field) => Object.prototype.hasOwnProperty.call(value, field))
    && Object.keys(value).every((field) => required.includes(field) || optional.includes(field));
}

function isRenderableMap(value: unknown, isEntry: (entry: unknown) => boolean): boolean {
  return isRenderableRecord(value)
    && Object.values(value).every(isEntry);
}

function isRenderableRequirements(value: unknown): boolean {
  const isCapability = (entry: unknown) => hasRenderableFields(entry, ["capability"])
    && typeof entry.capability === "string";
  return hasRenderableFields(value, ["models", "tools", "knowledge"], ["executors"])
    && (!("executors" in value) || isRenderableMap(value.executors, isCapability))
    && isRenderableMap(value.models, (entry) => hasRenderableFields(entry, ["capabilities"])
      && isRenderableStringArray(entry.capabilities))
    && isRenderableMap(value.tools, isCapability)
    && isRenderableMap(value.knowledge, isCapability);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isRenderableNode(value: unknown): boolean {
  if (!isRenderableRecord(value) || ("name" in value && typeof value.name !== "string")) return false;
  switch (value.kind) {
    case "llm":
      return hasRenderableFields(value, ["kind", "instruction", "model_slot", "tool_slots", "knowledge_slots"], ["name", "generation", "memory", "artifact", "add_session_summary", "workspace"])
        && renderableNodeData(value)
        && typeof value.instruction === "string"
        && typeof value.model_slot === "string"
        && isRenderableStringArray(value.tool_slots)
        && isRenderableStringArray(value.knowledge_slots)
        && (!("generation" in value) || (
          hasRenderableFields(value.generation, [], ["temperature", "max_output_tokens"])
          && (!("temperature" in value.generation) || isFiniteNumber(value.generation.temperature))
          && (!("max_output_tokens" in value.generation) || isFiniteNumber(value.generation.max_output_tokens))
        ));
    case "sequence":
    case "parallel":
      return hasRenderableFields(value, ["kind", "children"], ["name"])
        && isRenderableStringArray(value.children);
    case "loop":
      return hasRenderableFields(value, ["kind", "body", "max_iterations"], ["name"])
        && typeof value.body === "string"
        && isFiniteNumber(value.max_iterations);
    default:
      return false;
  }
}

/**
 * Mirrors the versioned JSON Schema for strict publish validation. Canvas
 * eligibility uses isRenderableAgentSpecV1 so field edits can remain incomplete.
 * Domain-semantic diagnostics live in agent-editor-state.ts.
 */
export function validateAgentSpecShape(value: unknown): AgentSpecDiagnostic[] {
  const diagnostics: AgentSpecDiagnostic[] = [];
  if (!isRecord(value)) {
    return [diagnostic("AGENT_SPEC_DOCUMENT_REQUIRED", "", "AgentSpec 顶层值必须是对象。")];
  }

  allowedFields(value, "", ["schema_version", "root", "requirements", "nodes", "runtime"], diagnostics);
  if ("runtime" in value) validateRuntime(value.runtime, diagnostics);
  requiredFields(value, "", ["schema_version", "root", "requirements", "nodes"], diagnostics);

  if ("schema_version" in value && value.schema_version !== AGENT_SPEC_SCHEMA_VERSION) {
    diagnostics.push(diagnostic("AGENT_SPEC_UNSUPPORTED_VERSION", "/schema_version", "schema_version 必须是 v1。"));
  }
  if ("root" in value) validateIdentifier(value.root, "/root", diagnostics);
  if ("requirements" in value) validateRequirements(value.requirements, diagnostics);
  if ("nodes" in value) validateNodes(value.nodes, diagnostics);

  return sortDiagnostics(diagnostics);
}

export function getNodeReferences(node: AgentNodeV1): string[] {
  if (node.kind === "sequence" || node.kind === "parallel") return node.children;
  if (node.kind === "loop") return [node.body];
  return [];
}

export function escapeJSONPointer(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

export function unescapeJSONPointer(value: string): string {
  return value.replaceAll("~1", "/").replaceAll("~0", "~");
}

export function nodeIDFromDiagnostic(diagnosticValue: Pick<AgentSpecDiagnostic, "node_id" | "pointer">): string | null {
  if (diagnosticValue.node_id) return diagnosticValue.node_id;
  const match = diagnosticValue.pointer.match(/^\/nodes\/([^/]+)/);
  return match ? unescapeJSONPointer(match[1]) : null;
}

function validateRequirements(value: unknown, diagnostics: AgentSpecDiagnostic[]): void {
  if (!isRecord(value)) {
    diagnostics.push(invalidType("/requirements"));
    return;
  }
  allowedFields(value, "/requirements", ["models", "tools", "knowledge", "executors"], diagnostics);
  if ("executors" in value) {
    validateRequirementMap(value.executors, "/requirements/executors", AGENT_SPEC_LIMITS.executorSlots, false, diagnostics);
    if (isRecord(value.executors)) for (const [slot, entry] of Object.entries(value.executors)) {
      if (isRecord(entry) && entry.capability !== "workspace") diagnostics.push(diagnostic("AGENT_SPEC_EXECUTOR_CAPABILITY_UNSUPPORTED", `/requirements/executors/${escapeJSONPointer(slot)}/capability`, "Executor capability 必须为 workspace。"));
    }
  }
  requiredFields(value, "/requirements", ["models", "tools", "knowledge"], diagnostics);
  if ("models" in value) {
    validateRequirementMap(value.models, "/requirements/models", AGENT_SPEC_LIMITS.modelSlots, true, diagnostics);
  }
  if ("tools" in value) {
    validateRequirementMap(value.tools, "/requirements/tools", AGENT_SPEC_LIMITS.toolSlots, false, diagnostics);
  }
  if ("knowledge" in value) {
    validateRequirementMap(value.knowledge, "/requirements/knowledge", AGENT_SPEC_LIMITS.knowledgeSlots, false, diagnostics);
  }
}

function validateRequirementMap(
  value: unknown,
  pointer: string,
  maximum: number,
  model: boolean,
  diagnostics: AgentSpecDiagnostic[],
): void {
  if (!isRecord(value)) {
    diagnostics.push(invalidType(pointer));
    return;
  }
  if (Object.keys(value).length > maximum) diagnostics.push(limitExceeded(pointer));
  for (const [slot, requirement] of Object.entries(value)) {
    const slotPointer = `${pointer}/${escapeJSONPointer(slot)}`;
    if (!isAgentSpecIdentifier(slot)) diagnostics.push(invalidIdentifier(slotPointer));
    if (!isRecord(requirement)) {
      diagnostics.push(invalidType(slotPointer));
      continue;
    }
    if (model) {
      allowedFields(requirement, slotPointer, ["capabilities"], diagnostics);
      requiredFields(requirement, slotPointer, ["capabilities"], diagnostics);
      if ("capabilities" in requirement) {
        validateStringArray(
          requirement.capabilities,
          `${slotPointer}/capabilities`,
          1,
          AGENT_SPEC_LIMITS.capabilitiesPerModel,
          isAgentCapability,
          diagnostics,
        );
      }
      continue;
    }
    allowedFields(requirement, slotPointer, ["capability"], diagnostics);
    requiredFields(requirement, slotPointer, ["capability"], diagnostics);
    if ("capability" in requirement) validateCapability(requirement.capability, `${slotPointer}/capability`, diagnostics);
  }
}

function validateNodes(value: unknown, diagnostics: AgentSpecDiagnostic[]): void {
  if (!isRecord(value)) {
    diagnostics.push(invalidType("/nodes"));
    return;
  }
  const entries = Object.entries(value);
  if (entries.length === 0 || entries.length > AGENT_SPEC_LIMITS.nodes) diagnostics.push(limitExceeded("/nodes"));
  for (const [nodeID, candidate] of entries) {
    const pointer = `/nodes/${escapeJSONPointer(nodeID)}`;
    if (!isAgentSpecIdentifier(nodeID)) diagnostics.push(invalidIdentifier(pointer));
    if (!isRecord(candidate)) {
      diagnostics.push(invalidType(pointer));
      continue;
    }
    validateNode(candidate, pointer, nodeID, diagnostics);
  }
}

function validateNode(node: Record<string, unknown>, pointer: string, nodeID: string, diagnostics: AgentSpecDiagnostic[]): void {
  if (!("kind" in node)) {
    diagnostics.push(requiredField(`${pointer}/kind`));
    return;
  }
  if (typeof node.kind !== "string") {
    diagnostics.push(invalidType(`${pointer}/kind`));
    return;
  }
  if (!AGENT_NODE_KINDS.includes(node.kind as AgentNodeKind)) {
    diagnostics.push(
      diagnostic("AGENT_SPEC_UNSUPPORTED_NODE_KIND", `${pointer}/kind`, "节点类型不属于 AgentSpec V1。", nodeID),
    );
    return;
  }

  if (node.kind === "llm") {
    allowedFields(node, pointer, ["kind", "name", "instruction", "model_slot", "tool_slots", "knowledge_slots", "generation", "memory", "artifact", "add_session_summary", "workspace"], diagnostics);
    validateNodeData(node, pointer, diagnostics);
    requiredFields(node, pointer, ["kind", "instruction", "model_slot", "tool_slots", "knowledge_slots"], diagnostics);
    validateOptionalName(node, pointer, diagnostics);
    if ("instruction" in node) {
      if (typeof node.instruction !== "string") diagnostics.push(invalidType(`${pointer}/instruction`, nodeID));
      else if (node.instruction.trim() === "" || utf8Length(node.instruction) > AGENT_SPEC_LIMITS.instructionBytes) {
        diagnostics.push(limitExceeded(`${pointer}/instruction`, nodeID));
      }
    }
    if ("model_slot" in node) validateIdentifier(node.model_slot, `${pointer}/model_slot`, diagnostics, nodeID);
    if ("tool_slots" in node) {
      validateStringArray(node.tool_slots, `${pointer}/tool_slots`, 0, AGENT_SPEC_LIMITS.toolSlots, isAgentSpecIdentifier, diagnostics, nodeID);
    }
    if ("knowledge_slots" in node) {
      validateStringArray(
        node.knowledge_slots,
        `${pointer}/knowledge_slots`,
        0,
        AGENT_SPEC_LIMITS.knowledgeSlots,
        isAgentSpecIdentifier,
        diagnostics,
        nodeID,
      );
    }
    if ("generation" in node) validateGeneration(node.generation, `${pointer}/generation`, diagnostics, nodeID);
    return;
  }

  if (node.kind === "sequence" || node.kind === "parallel") {
    allowedFields(node, pointer, ["kind", "name", "children"], diagnostics);
    requiredFields(node, pointer, ["kind", "children"], diagnostics);
    validateOptionalName(node, pointer, diagnostics);
    if ("children" in node) {
      validateStringArray(node.children, `${pointer}/children`, 1, AGENT_SPEC_LIMITS.children, isAgentSpecIdentifier, diagnostics, nodeID);
    }
    return;
  }

  allowedFields(node, pointer, ["kind", "name", "body", "max_iterations"], diagnostics);
  requiredFields(node, pointer, ["kind", "body", "max_iterations"], diagnostics);
  validateOptionalName(node, pointer, diagnostics);
  if ("body" in node) validateIdentifier(node.body, `${pointer}/body`, diagnostics, nodeID);
  if ("max_iterations" in node) {
    if (!Number.isInteger(node.max_iterations)) diagnostics.push(invalidType(`${pointer}/max_iterations`, nodeID));
    else if ((node.max_iterations as number) < 1 || (node.max_iterations as number) > AGENT_SPEC_LIMITS.loopIterations) {
      diagnostics.push(limitExceeded(`${pointer}/max_iterations`, nodeID));
    }
  }
}

function validateOptionalName(node: Record<string, unknown>, pointer: string, diagnostics: AgentSpecDiagnostic[]): void {
  if (!("name" in node)) return;
  if (typeof node.name !== "string") diagnostics.push(invalidType(`${pointer}/name`));
  else if (node.name.trim() === "" || Array.from(node.name).length > 128) diagnostics.push(limitExceeded(`${pointer}/name`));
}

function validateGeneration(value: unknown, pointer: string, diagnostics: AgentSpecDiagnostic[], nodeID: string): void {
  if (!isRecord(value)) {
    diagnostics.push(invalidType(pointer, nodeID));
    return;
  }
  allowedFields(value, pointer, ["temperature", "max_output_tokens"], diagnostics);
  if ("temperature" in value) {
    if (typeof value.temperature !== "number" || !Number.isFinite(value.temperature)) diagnostics.push(invalidType(`${pointer}/temperature`, nodeID));
    else if (value.temperature < 0 || value.temperature > 2) diagnostics.push(limitExceeded(`${pointer}/temperature`, nodeID));
  }
  if ("max_output_tokens" in value) {
    if (!Number.isInteger(value.max_output_tokens)) diagnostics.push(invalidType(`${pointer}/max_output_tokens`, nodeID));
    else if ((value.max_output_tokens as number) < 1 || (value.max_output_tokens as number) > AGENT_SPEC_LIMITS.maxOutputTokens) {
      diagnostics.push(limitExceeded(`${pointer}/max_output_tokens`, nodeID));
    }
  }
}

function validateStringArray(
  value: unknown,
  pointer: string,
  minimum: number,
  maximum: number,
  predicate: (item: string) => boolean,
  diagnostics: AgentSpecDiagnostic[],
  nodeID?: string,
): void {
  if (!Array.isArray(value)) {
    diagnostics.push(invalidType(pointer, nodeID));
    return;
  }
  if (value.length < minimum || value.length > maximum) diagnostics.push(limitExceeded(pointer, nodeID));
  const seen = new Set<string>();
  value.forEach((item, index) => {
    const itemPointer = `${pointer}/${index}`;
    if (typeof item !== "string") diagnostics.push(invalidType(itemPointer, nodeID));
    else {
      if (!predicate(item)) diagnostics.push(invalidIdentifier(itemPointer, nodeID));
      if (seen.has(item)) {
        const duplicateCode = pointer.endsWith("/children")
          ? "AGENT_SPEC_DUPLICATE_CHILD"
          : pointer.endsWith("/workspace/tools") ? "AGENT_SPEC_DUPLICATE_WORKSPACE_TOOL" : "AGENT_SPEC_LIMIT_EXCEEDED";
        diagnostics.push(diagnostic(duplicateCode, itemPointer, "数组元素必须唯一。", nodeID));
      }
      seen.add(item);
    }
  });
}

function validateIdentifier(value: unknown, pointer: string, diagnostics: AgentSpecDiagnostic[], nodeID?: string): void {
  if (typeof value !== "string") diagnostics.push(invalidType(pointer, nodeID));
  else if (!isAgentSpecIdentifier(value)) diagnostics.push(invalidIdentifier(pointer, nodeID));
}

function validateCapability(value: unknown, pointer: string, diagnostics: AgentSpecDiagnostic[]): void {
  if (typeof value !== "string") diagnostics.push(invalidType(pointer));
  else if (!isAgentCapability(value)) diagnostics.push(invalidIdentifier(pointer));
}

function allowedFields(
  value: Record<string, unknown>,
  pointer: string,
  allowed: readonly string[],
  diagnostics: AgentSpecDiagnostic[],
): void {
  for (const field of Object.keys(value)) {
    if (!allowed.includes(field)) {
      diagnostics.push(
        diagnostic(
          "AGENT_SPEC_UNKNOWN_FIELD",
          `${pointer}/${escapeJSONPointer(field)}`,
          "字段不属于 AgentSpec V1。",
        ),
      );
    }
  }
}

function requiredFields(
  value: Record<string, unknown>,
  pointer: string,
  required: readonly string[],
  diagnostics: AgentSpecDiagnostic[],
): void {
  for (const field of required) {
    if (!(field in value)) diagnostics.push(requiredField(`${pointer}/${field}`));
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).length;
}

function diagnostic(
  code: string,
  pointer: string,
  message: string,
  nodeID: string | null = null,
  severity: AgentSpecDiagnosticSeverity = "error",
): AgentSpecDiagnostic {
  return { code, severity, pointer, node_id: nodeID, message };
}

function invalidType(pointer: string, nodeID?: string): AgentSpecDiagnostic {
  return diagnostic("AGENT_SPEC_INVALID_TYPE", pointer, "字段 JSON 类型无效。", nodeID ?? null);
}

function invalidIdentifier(pointer: string, nodeID?: string): AgentSpecDiagnostic {
  return diagnostic("AGENT_SPEC_INVALID_IDENTIFIER", pointer, "标识符不符合 V1 格式。", nodeID ?? null);
}

function requiredField(pointer: string): AgentSpecDiagnostic {
  return diagnostic("AGENT_SPEC_REQUIRED_FIELD", pointer, "缺少 AgentSpec V1 必填字段。");
}

function limitExceeded(pointer: string, nodeID?: string): AgentSpecDiagnostic {
  return diagnostic("AGENT_SPEC_LIMIT_EXCEEDED", pointer, "字段超出 AgentSpec V1 限制。", nodeID ?? null);
}

function sortDiagnostics(diagnostics: AgentSpecDiagnostic[]): AgentSpecDiagnostic[] {
  return diagnostics.sort((left, right) =>
    left.pointer.localeCompare(right.pointer) ||
    left.severity.localeCompare(right.severity) ||
    left.code.localeCompare(right.code),
  );
}

function renderableRuntime(value: unknown): boolean {
  return hasRenderableFields(value, [], ["summary"]) && (!("summary" in value) || (
    hasRenderableFields(value.summary, ["enabled"], ["model_slot", "event_threshold"])
    && typeof value.summary.enabled === "boolean"
    && (!("model_slot" in value.summary) || typeof value.summary.model_slot === "string")
    && (!("event_threshold" in value.summary) || isFiniteNumber(value.summary.event_threshold))
  ));
}
function renderableNodeData(value: Record<string, unknown>): boolean {
  return (!("workspace" in value) || (hasRenderableFields(value.workspace, ["executor_slot", "tools"])
    && typeof value.workspace.executor_slot === "string" && isRenderableStringArray(value.workspace.tools)
    && value.workspace.tools.every((tool) => WORKSPACE_TOOLS.includes(tool as WorkspaceTool))))
    && (!("memory" in value) || (hasRenderableFields(value.memory, ["tools"], ["preload_limit"])
    && isRenderableStringArray(value.memory.tools) && value.memory.tools.every((tool) => MEMORY_TOOLS.includes(tool as MemoryTool))
    && (!("preload_limit" in value.memory) || isFiniteNumber(value.memory.preload_limit))))
    && (!("artifact" in value) || (hasRenderableFields(value.artifact, ["enabled"]) && typeof value.artifact.enabled === "boolean"))
    && (!("add_session_summary" in value) || typeof value.add_session_summary === "boolean");
}
function dataObject(value: unknown, pointer: string, allowed: string[], required: string[], d: AgentSpecDiagnostic[]): value is Record<string, unknown> {
  if (!isRecord(value)) { d.push(invalidType(pointer)); return false; }
  allowedFields(value, pointer, allowed, d); requiredFields(value, pointer, required, d); return true;
}
function dataInteger(value: unknown, pointer: string, min: number, d: AgentSpecDiagnostic[]) {
  if (typeof value !== "number" || !Number.isInteger(value)) d.push(invalidType(pointer));
  else if (!Number.isSafeInteger(value) || value < min) d.push(limitExceeded(pointer));
}
function validateRuntime(value: unknown, d: AgentSpecDiagnostic[]) {
  if (!dataObject(value, "/runtime", ["summary"], [], d) || !("summary" in value)) return;
  const s = value.summary, p = "/runtime/summary";
  if (!dataObject(s, p, ["enabled", "model_slot", "event_threshold"], ["enabled"], d)) return;
  if (!("enabled" in s)) return;
  if (typeof s.enabled !== "boolean") { d.push(invalidType(`${p}/enabled`)); return; }
  if (!s.enabled) { allowedFields(s, p, ["enabled"], d); return; }
  requiredFields(s, p, ["model_slot", "event_threshold"], d);
  if ("model_slot" in s) validateIdentifier(s.model_slot, `${p}/model_slot`, d);
  if ("event_threshold" in s) dataInteger(s.event_threshold, `${p}/event_threshold`, 1, d);
}
function validateNodeData(node: Record<string, unknown>, p: string, d: AgentSpecDiagnostic[]) {
  if ("workspace" in node && dataObject(node.workspace, `${p}/workspace`, ["executor_slot", "tools"], ["executor_slot", "tools"], d)) {
    const w = node.workspace;
    if ("executor_slot" in w) validateIdentifier(w.executor_slot, `${p}/workspace/executor_slot`, d);
    if ("tools" in w) {
      validateStringArray(w.tools, `${p}/workspace/tools`, 1, 2, isAgentCapability, d);
      if (Array.isArray(w.tools)) w.tools.forEach((tool, index) => {
        if (typeof tool === "string" && !WORKSPACE_TOOLS.includes(tool as WorkspaceTool)) d.push(diagnostic("AGENT_SPEC_WORKSPACE_TOOL_UNSUPPORTED", `${p}/workspace/tools/${index}`, "Workspace 工具不受支持。"));
      });
    }
  }
  if ("memory" in node && dataObject(node.memory, `${p}/memory`, ["tools", "preload_limit"], ["tools"], d)) {
    const m = node.memory;
    if ("tools" in m) {
      validateStringArray(m.tools, `${p}/memory/tools`, 0, 6, isAgentCapability, d);
      if (Array.isArray(m.tools)) m.tools.forEach((tool, index) => {
        if (typeof tool === "string" && !MEMORY_TOOLS.includes(tool as MemoryTool)) d.push(diagnostic("AGENT_SPEC_MEMORY_TOOL_UNSUPPORTED", `${p}/memory/tools/${index}`, "Memory 工具不受支持。"));
      });
    }
    if ("preload_limit" in m) dataInteger(m.preload_limit, `${p}/memory/preload_limit`, -1, d);
  }
  if ("artifact" in node && dataObject(node.artifact, `${p}/artifact`, ["enabled"], ["enabled"], d)
    && "enabled" in node.artifact && typeof node.artifact.enabled !== "boolean") d.push(invalidType(`${p}/artifact/enabled`));
  if ("add_session_summary" in node && typeof node.add_session_summary !== "boolean") d.push(invalidType(`${p}/add_session_summary`));
}
