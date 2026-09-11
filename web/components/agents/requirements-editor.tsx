import { Plus, Trash2 } from "lucide-react";
import { useId, useState, type CSSProperties } from "react";

import { AGENT_SPEC_LIMITS, isAgentCapability, isAgentSpecIdentifier } from "../../lib/agent-spec-v1";
import type { AgentEditorAction, AgentEditorState, RequirementKind } from "../../lib/agent-editor-state";
import { Button } from "../ui";
import { CsvInput } from "./editor-inputs";

export function RequirementsEditor({ state, dispatch, disabled }: {
  state: AgentEditorState;
  dispatch(action: AgentEditorAction): void;
  disabled: boolean;
}) {
  return (
    <section aria-label="资源需求槽位" style={panelStyle}>
      <header style={panelHeaderStyle}>
        <div><strong>Requirements</strong><small style={{ color: "var(--muted)", display: "block", marginTop: 3 }}>只声明逻辑槽位，不填写密钥</small></div>
      </header>
      <RequirementGroup disabled={disabled} dispatch={dispatch} entries={state.spec.requirements.models} kind="models" />
      <RequirementGroup disabled={disabled} dispatch={dispatch} entries={state.spec.requirements.tools} kind="tools" />
      <RequirementGroup disabled={disabled} dispatch={dispatch} entries={state.spec.requirements.knowledge} kind="knowledge" />
      <RequirementGroup disabled={disabled} dispatch={dispatch} entries={state.spec.requirements.executors ?? {}} kind="executors" />
    </section>
  );
}

function RequirementGroup({ kind, entries, dispatch, disabled }: {
  kind: RequirementKind;
  entries: Record<string, { capabilities: string[] } | { capability: string }>;
  dispatch(action: AgentEditorAction): void;
  disabled: boolean;
}) {
  const [slot, setSlot] = useState("");
  const [capability, setCapability] = useState(kind === "models" ? "chat" : kind === "executors" ? "workspace" : "");
  const [capabilityTouched, setCapabilityTouched] = useState(false);
  const errorID = useId();
  const title = kind === "models" ? "Models" : kind === "tools" ? "Tools" : kind === "executors" ? "Executors" : "Knowledge";
  const limit = kind === "models" ? AGENT_SPEC_LIMITS.modelSlots : kind === "tools" ? AGENT_SPEC_LIMITS.toolSlots : kind === "executors" ? AGENT_SPEC_LIMITS.executorSlots : AGENT_SPEC_LIMITS.knowledgeSlots;
  const slotError = Object.keys(entries).length >= limit
    ? `${title} 最多声明 ${limit} 个槽位。`
    : slot && !isAgentSpecIdentifier(slot)
      ? "Slot ID 须以小写字母开头，仅含小写字母、数字、_ 或 -，最长 64 个字符。"
      : Object.hasOwn(entries, slot)
        ? `Slot ID「${slot}」已存在，请使用其他 ID；修改现有能力请编辑对应行。`
        : "";
  const capabilities = kind === "models" ? parseCapabilities(capability) : [capability];
  const capabilityError = validateCapabilities(kind, capabilities);
  const visibleCapabilityError = slot || capabilityTouched ? capabilityError : "";
  const canAdd = !disabled && Boolean(slot) && !slotError && !capabilityError;

  function add() {
    // The same guard covers mouse, keyboard, and stale submit attempts.
    if (!canAdd) return;
    if (kind === "models") dispatch({ type: "requirement.model.set", slot, capabilities });
    else dispatch({ type: "requirement.capability.set", kind, slot, capability });
    setSlot("");
    setCapability(kind === "models" ? "chat" : kind === "executors" ? "workspace" : "");
    setCapabilityTouched(false);
  }

  return (
    <div data-resource-category={kind} style={{ display: "grid", gap: 8 }}>
      <span style={{ color: "var(--resource-color)", fontSize: 12, fontWeight: 700 }}>{title}</span>
      {Object.entries(entries).map(([slotID, item]) => {
        const existingError = validateCapabilities(kind, "capabilities" in item ? item.capabilities : [item.capability]);
        const existingErrorID = `${errorID}-${slotID}`;
        return (
          <div key={slotID} style={{ display: "grid", gap: 4 }}>
            <div style={requirementStyle}>
              <code style={requirementSlotStyle} title={slotID}>{slotID}</code>
              {"capabilities" in item ? (
                <CsvInput
                  aria-describedby={existingError ? existingErrorID : undefined}
                  aria-invalid={Boolean(existingError)}
                  aria-label={`${slotID} capability`}
                  disabled={disabled}
                  onValueChange={(next) => dispatch({ type: "requirement.model.set", slot: slotID, capabilities: next })}
                  style={requirementInputStyle}
                  value={item.capabilities}
                />
              ) : (
                <input
                  aria-describedby={existingError ? existingErrorID : undefined}
                  aria-invalid={Boolean(existingError)}
                  aria-label={`${slotID} capability`}
                  disabled={disabled}
                  onChange={(event) => {
                    if (kind !== "models") dispatch({ type: "requirement.capability.set", kind, slot: slotID, capability: event.target.value });
                  }}
                  style={requirementInputStyle}
                  readOnly={kind === "executors"}
                  value={item.capability}
                />
              )}
              <Button aria-label={`删除 ${slotID}`} disabled={disabled} onClick={() => dispatch({ type: "requirement.delete", kind, slot: slotID })} style={requirementActionStyle} type="button" variant="danger"><Trash2 size={12} /></Button>
            </div>
            {existingError && <small id={existingErrorID} role="status" style={errorStyle}>{existingError}</small>}
          </div>
        );
      })}
      <div style={requirementStyle}>
        <input
          aria-describedby={slotError ? `${errorID}-slot` : undefined}
          aria-invalid={Boolean(slotError)}
          aria-label={`${title} 新 Slot ID`}
          disabled={disabled}
          onChange={(event) => setSlot(event.target.value)}
          placeholder="slot_id"
          style={requirementInputStyle}
          value={slot}
        />
        <input
          aria-describedby={visibleCapabilityError ? `${errorID}-capability` : undefined}
          aria-invalid={Boolean(visibleCapabilityError)}
          aria-label={`${title} 新 Capability`}
          disabled={disabled}
          onBlur={() => setCapabilityTouched(true)}
          onChange={(event) => { setCapability(event.target.value); setCapabilityTouched(true); }}
          placeholder={kind === "models" ? "chat, tool_call" : "web.search"}
          style={requirementInputStyle}
          readOnly={kind === "executors"}
          value={capability}
        />
        <Button aria-label={`添加 ${title} Slot`} disabled={!canAdd} onClick={add} style={requirementActionStyle} type="button" variant="secondary"><Plus size={12} /></Button>
      </div>
      {slotError && <small id={`${errorID}-slot`} role="status" style={errorStyle}>{slotError}</small>}
      {visibleCapabilityError && <small id={`${errorID}-capability`} role="status" style={errorStyle}>{visibleCapabilityError}</small>}
    </div>
  );
}

function parseCapabilities(value: string): string[] {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function validateCapabilities(kind: RequirementKind, capabilities: readonly string[]): string {
  if (kind === "executors" && (capabilities.length !== 1 || capabilities[0] !== "workspace")) return "Executor capability 必须为 workspace。";
  if (capabilities.length === 0 || capabilities.some((capability) => !capability)) return "Capability 必填。";
  if (kind === "models" && capabilities.length > AGENT_SPEC_LIMITS.capabilitiesPerModel) return `每个 Model 最多声明 ${AGENT_SPEC_LIMITS.capabilitiesPerModel} 个 Capability。`;
  if (new Set(capabilities).size !== capabilities.length) return "Capability 不可重复。";
  if (capabilities.some((capability) => !isAgentCapability(capability))) return "Capability 须以小写字母开头，仅含小写字母、数字、_、. 或 -，最长 128 个字符。";
  return "";
}

const panelStyle: CSSProperties = { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 10, display: "grid", gap: 12, minWidth: 0, padding: 13 };
const panelHeaderStyle: CSSProperties = { alignItems: "center", borderBottom: "1px solid var(--line)", display: "flex", justifyContent: "space-between", paddingBottom: 10 };
const requirementStyle: CSSProperties = { alignItems: "center", display: "grid", gap: 6, gridTemplateColumns: "minmax(82px,.7fr) minmax(0,1.3fr) 34px", minWidth: 0 };
const requirementInputStyle: CSSProperties = { background: "var(--surface)", border: "1px solid var(--line-strong)", borderRadius: 7, color: "var(--ink)", fontSize: 11, fontWeight: 500, height: 34, minWidth: 0, padding: "0 9px", width: "100%" };
const requirementSlotStyle: CSSProperties = { alignItems: "center", background: "var(--bg)", border: "1px solid var(--line)", borderRadius: 7, color: "var(--ink-soft)", display: "flex", fontSize: 11, height: 34, minWidth: 0, overflow: "hidden", padding: "0 9px", textOverflow: "ellipsis", whiteSpace: "nowrap" };
const requirementActionStyle: CSSProperties = { height: 34, minHeight: 34, padding: 0, width: 34 };
const errorStyle: CSSProperties = { color: "var(--danger)", fontSize: 11, lineHeight: 1.5, overflowWrap: "anywhere" };
