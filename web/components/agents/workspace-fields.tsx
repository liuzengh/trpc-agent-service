"use client";

import { useState } from "react";
import { WORKSPACE_TOOLS, type CapabilityRequirementV1, type LLMNodeV1 } from "../../lib/agent-spec-v1";
import { Tooltip } from "../ui";

/** A slot selection alone is local UI state; only explicit tool selection declares capability. */
export function WorkspaceFields({ node, executors, replace, disabled }: {
  node: LLMNodeV1;
  executors: Record<string, CapabilityRequirementV1>;
  replace(node: LLMNodeV1): void;
  disabled: boolean;
}) {
  const [pendingSlot, setPendingSlot] = useState("");
  const slot = node.workspace?.executor_slot ?? pendingSlot;
  const declared = Object.hasOwn(executors, slot);
  const clear = () => { const next = { ...node }; delete next.workspace; replace(next); };
  return <fieldset disabled={disabled} style={{ display: "grid", gap: 8, minWidth: 0 }}>
    <legend>Workspace 工作区工具<Tooltip label="Workspace 说明">先在 Requirements / Executors 声明槽位，再在此逐项选择工具；未勾选工具不启用，取消最后一项会移除 workspace 声明，模型需声明 tool_call。workspace_exec 在 SDK Sandbox 工作区执行；workspace_save_artifact 将工作区文件保存到已配置的 Artifact 存储。这里不填写主机路径、URL 或环境秘密。</Tooltip></legend>
    <label className="field"><span>Workspace Executor Slot</span><select value={slot} onChange={(event) => {
      const next = event.target.value; setPendingSlot(next);
      if (!next) clear();
      else if (node.workspace) replace({ ...node, workspace: { ...node.workspace, executor_slot: next } });
    }}>
      <option value="">未启用 · 请选择已声明的执行器槽位</option>
      {slot && !declared && <option value={slot}>{slot} · 未声明</option>}
      {Object.keys(executors).map((name) => <option key={name} value={name}>{name}</option>)}
    </select></label>
    {WORKSPACE_TOOLS.map((tool) => <label key={tool}><input type="checkbox" disabled={disabled || !declared} checked={node.workspace?.tools.includes(tool) ?? false} onChange={(event) => {
      const tools = event.target.checked ? [...(node.workspace?.tools ?? []).filter((item) => item !== tool), tool] : (node.workspace?.tools ?? []).filter((item) => item !== tool);
      if (!tools.length) clear(); else replace({ ...node, workspace: { executor_slot: slot, tools } });
    }} />{tool}</label>)}
    {node.workspace?.tools.includes("workspace_save_artifact") && node.artifact?.enabled !== true && <p role="alert">workspace_save_artifact 需要同节点显式启用 Artifact 服务；不会自动开启。</p>}
    {node.workspace && <button type="button" disabled={disabled} onClick={() => { setPendingSlot(""); clear(); }}>关闭 Workspace</button>}
  </fieldset>;
}
