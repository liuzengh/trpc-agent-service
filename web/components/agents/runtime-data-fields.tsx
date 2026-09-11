"use client";

import { MEMORY_TOOLS, type AgentRuntimeV1, type LLMNodeV1 } from "../../lib/agent-spec-v1";
import type { AgentEditorAction, AgentEditorState } from "../../lib/agent-editor-state";
import { NumberInput } from "./editor-inputs";
import { Tooltip } from "../ui";

const unset = "absent";
function OptionalBoolean({ label, value, disabled, onChange }: {
  label: string; value: boolean | undefined; disabled: boolean; onChange(value: boolean | undefined): void;
}) {
  return <label className="field"><span>{label}</span><select value={value === undefined ? unset : String(value)} disabled={disabled}
    onChange={(event) => onChange(event.target.value === unset ? undefined : event.target.value === "true")}>
    <option value={unset}>未声明（不启用）</option><option value="false">显式关闭</option><option value="true">启用</option>
  </select></label>;
}

export function RuntimeSummaryFields({ state, dispatch, disabled }: {
  state: AgentEditorState; dispatch(action: AgentEditorAction): void; disabled: boolean;
}) {
  const summary = state.spec.runtime?.summary;
  const update = (next: AgentRuntimeV1["summary"]) => {
    const runtime = { ...state.spec.runtime };
    if (next === undefined) delete runtime.summary; else runtime.summary = next;
    dispatch({ type: "runtime.set", runtime });
  };
  return <section aria-label="Agent 运行能力" style={{ display: "grid", gap: 10 }}>
    <h3>Session Summary<Tooltip label="Session Summary 说明">摘要跟随 Session 存储；阈值是事件数量，不是 token 预算。关闭生成不会自动清除节点的摘要消费意图，请处理相应诊断。Session Summary 已支持配置、发布和 Worker 执行；需绑定 Session 存储及具备 chat 能力的摘要模型，实际发布能力以服务端校验为准。新的 DeploymentRevision 对应新的 Session，不继承旧版本的会话历史或摘要。同 Session 内部 overlay 保留摘要元数据，不等于跨发布版本的历史迁移。</Tooltip></h3>
    <OptionalBoolean label="生成会话摘要" disabled={disabled} value={summary?.enabled} onChange={(enabled) => update(enabled === undefined ? undefined : enabled ? { enabled: true, model_slot: "" } : { enabled: false })} />
    {summary?.enabled && <>
      <label className="field"><span>Summary 模型槽</span><select value={summary.model_slot ?? ""} disabled={disabled}
        onChange={(event) => update({ ...summary, model_slot: event.target.value })}>
        <option value="">请选择已声明的 chat 模型槽</option>
        {summary.model_slot && !Object.hasOwn(state.spec.requirements.models, summary.model_slot) && <option value={summary.model_slot}>{summary.model_slot} · 未声明</option>}
        {Object.entries(state.spec.requirements.models).map(([slot, requirement]) => <option key={slot} value={slot}>{slot}{requirement.capabilities.includes("chat") ? "" : " · 缺少 chat 能力"}</option>)}
      </select></label>
      <label className="field"><span>摘要事件触发阈值</span><NumberInput disabled={disabled} min={1} max={Number.MAX_SAFE_INTEGER} step={1}
        value={summary.event_threshold} onValueChange={(value) => { const next = { ...summary }; if (value === undefined) delete next.event_threshold; else next.event_threshold = value; update(next); }} /></label>
    </>}
    {state.spec.runtime && <button type="button" disabled={disabled} onClick={() => dispatch({ type: "runtime.set", runtime: undefined })}>移除 runtime 声明</button>}
  </section>;
}

export function NodeDataFields({ node, replace, disabled }: { node: LLMNodeV1; replace(node: LLMNodeV1): void; disabled: boolean }) {
  const memory = node.memory;
  const setOptional = (key: "artifact" | "add_session_summary", enabled: boolean | undefined) => {
    const next = { ...node };
    if (enabled === undefined) delete next[key];
    else if (key === "artifact") next.artifact = { enabled }; else next.add_session_summary = enabled;
    replace(next);
  };
  return <fieldset disabled={disabled} style={{ minWidth: 0, display: "grid", gap: 10 }}>
    <legend>节点数据能力<Tooltip label="节点数据能力说明">空工具列表且未预加载时不启用 Memory。隔离身份由服务端可信身份确定，跨 Session 复用，按 Tenant、主体及 Agent 隔离。预加载条目留空时不预加载，0 为显式关闭，-1 为全加载，正整数是 SDK adaptive 条目预算，不是 token 预算。显式启用 Artifact 后装配 SDK artifact.Service，并提供 Worker 薄文件工具 artifact_save、artifact_load、artifact_list、artifact_delete（不是 SDK 内建工具）。内容存入 S3，元数据存入 Worker PostgreSQL；工具保存立即持久化，不随失败 Run 回滚。</Tooltip></legend>
    <label><input type="checkbox" checked={memory !== undefined} onChange={(event) => {
      const next = { ...node }; if (event.target.checked) next.memory = { tools: [] }; else delete next.memory; replace(next);
    }} />声明 Memory 配置</label>
    {memory && <>
      {MEMORY_TOOLS.map((tool) => <label key={tool}><input type="checkbox" checked={memory.tools.includes(tool)} onChange={(event) => replace({ ...node, memory: { ...memory, tools: event.target.checked ? [...memory.tools.filter((name) => name !== tool), tool] : memory.tools.filter((name) => name !== tool) } })} />{tool}</label>)}
      <label className="field"><span>Memory 预加载条目</span><NumberInput min={-1} max={Number.MAX_SAFE_INTEGER} step={1} value={memory.preload_limit}
        onValueChange={(value) => { const next = { ...memory }; if (value === undefined) delete next.preload_limit; else next.preload_limit = value; replace({ ...node, memory: next }); }} /></label>
    </>}
    <OptionalBoolean label="Artifact 服务" value={node.artifact?.enabled} disabled={disabled} onChange={(value) => setOptional("artifact", value)} />
    <OptionalBoolean label="消费会话摘要" value={node.add_session_summary} disabled={disabled} onChange={(value) => setOptional("add_session_summary", value)} />
  </fieldset>;
}
