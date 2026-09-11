"use client";

import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { Boxes, BrainCircuit, Database, KeyRound, Plus, Search, Trash2 } from "lucide-react";
import type { CredentialAction, CredentialActions, CredentialState, CredentialStates, KnowledgeConfig, ModelConfig, ProfileConfig, ResourceCategory, StorageConfig, ToolConfig } from "../../lib/runtime-profile-api";
import { isAgentCapability } from "../../lib/agent-spec-v1";
import { managedProfileIssues } from "../../lib/managed-profile";
import { NumberInput } from "../agents/editor-inputs";
import { Button, Tooltip } from "../ui";
import { ManagedBackendSelect } from "./managed-backend-select";
import { ProfileDialog } from "./profile-dialog";
import styles from "./resource-editor.module.css";

export type ResourceFocusTarget = { category: ResourceCategory; name: string; pointer?: string };
export type ProfileResourceEditorProps = {
  tenantId?: string;
  config: ProfileConfig;
  credentials: CredentialActions;
  credentialStates: CredentialStates;
  onChange(config: ProfileConfig, credentials: CredentialActions): void;
  isOwner: boolean;
  disabled?: boolean;
  readOnly?: boolean;
  focusTarget?: ResourceFocusTarget | null;
};
type ResourceConfig = ModelConfig | ToolConfig | KnowledgeConfig | StorageConfig;
const categories: { key: ResourceCategory; label: string; detail: string; limit: number; kind: string; icon: typeof Boxes }[] = [
  { key: "models", label: "Models", detail: "模型服务", limit: 16, kind: "openai_compatible", icon: BrainCircuit },
  { key: "tools", label: "Tools", detail: "MCP 工具", limit: 64, kind: "mcp_streamable_http", icon: Boxes },
  { key: "knowledge", label: "Knowledge", detail: "知识检索", limit: 32, kind: "qdrant_openai", icon: Search },
  { key: "executors", label: "Executors", detail: "工作区执行器", limit: 16, kind: "sdk_sandbox", icon: Boxes },
  { key: "storage", label: "Storage", detail: "状态存储", limit: 16, kind: "postgres_state", icon: Database },
];
const namePattern = /^[a-z][a-z0-9_-]{0,63}$/;

function own<T>(map: Record<string, T> | undefined, key: string): T | undefined {
  return map && Object.prototype.hasOwnProperty.call(map, key) ? map[key] : undefined;
}
function initialResource(category: ResourceCategory): ResourceConfig {
  switch (category) {
    case "executors": return { kind: "sdk_sandbox" };
    case "models": return { kind: "openai_compatible", model: "", base_url: "", capabilities: ["chat"] };
    case "tools": return { kind: "mcp_streamable_http", server_url: "", toolset_name: "", tool_name: "", auth: { kind: "none" }, capability: "web.search" };
    case "knowledge": return { kind: "qdrant_openai", host: "", port: 6334, tls: true, collection: "", embedding: { model: "", base_url: "", dimensions: undefined } };
    case "storage": return { kind: "postgres_state", destination: { host: "", port: 5432, database: "", username: "", sslmode: "require" } };
  }
}
function dropResourceActions(actions: CredentialActions, category: ResourceCategory, name: string): CredentialActions {
  const next = { ...actions };
  const resources = { ...actions[category] };
  delete resources[name];
  if (Object.keys(resources).length) next[category] = resources;
  else delete next[category];
  return next;
}
function credentialAssociated(state: CredentialState | undefined): boolean {
  // A live-cleared credential still has an association in an immutable/draft config.
  return !!state && (state.configured || state.credential_revision > 0 || state.status === "active" || state.status === "cleared");
}
function statusLabel(state?: CredentialState): string {
  const label = state?.status === "cleared" ? "已清除" : state?.configured ? "已配置" : "未配置";
  return state?.credential_revision ? `${label} · 凭证 r${state.credential_revision}` : label;
}

function FormSection({ title, description, children }: { title: string; description?: string; children: ReactNode }) {
  return <section className={styles.section}><header><h3>{title}{description && <Tooltip label={`${title} 说明`}>{description}</Tooltip>}</h3></header>{children}</section>;
}
function TextField({ id, label, value, onChange, readOnly, disabled, hint, placeholder }: { id: string; label: string; value?: string; onChange(value: string): void; readOnly?: boolean; disabled?: boolean; hint?: string; placeholder?: string }) {
  return <label className={styles.field} htmlFor={id}><span>{label}</span><input id={id} aria-label={label} aria-describedby={hint ? `${id}-hint` : undefined} value={value ?? ""} onChange={(event) => onChange(event.target.value)} readOnly={readOnly} disabled={disabled} placeholder={placeholder} autoComplete="off" spellCheck={false} />{hint && <small id={`${id}-hint`}>{hint}</small>}</label>;
}
function NumericField({ id, label, value, onChange, readOnly, disabled, min, max }: { id: string; label: string; value?: number; onChange(value: number | undefined): void; readOnly?: boolean; disabled?: boolean; min?: number; max?: number }) {
  return <label className={styles.field} htmlFor={id}><span>{label}</span><NumberInput id={id} aria-label={label} value={value} onValueChange={onChange} readOnly={readOnly} disabled={disabled} min={min} max={max} step={1} /></label>;
}
function CredentialField({ id, label, state, action, onChange, isOwner, disabled, readOnly, hint }: { id: string; label: string; state?: CredentialState; action?: CredentialAction; onChange(action?: CredentialAction): void; isOwner: boolean; disabled?: boolean; readOnly?: boolean; hint?: string }) {
  const mode = action?.action ?? "keep";
  return <section id={`${id}-section`} tabIndex={-1} className={styles.credential} aria-label={label}>
    <header><span><KeyRound size={15} aria-hidden="true" />{label}</span><span className={`${styles.credentialStatus} ${state?.configured ? styles.configured : ""}`}>{statusLabel(state)}</span></header>
    {!readOnly && isOwner ? <>
      <label className={styles.field} htmlFor={`${id}-action`}><span>凭证操作</span><select id={`${id}-action`} aria-label={`${label} 操作`} value={mode} disabled={disabled} onChange={(event) => {
        const selected = event.target.value;
        if (selected === "keep") onChange(undefined);
        else if (selected === "replace") onChange({ action: "replace", value: "" });
        else onChange({ action: "clear" });
      }}><option value="keep">保持现有凭证</option><option value="replace">替换凭证</option><option value="clear">清除草稿关联</option></select></label>
      {mode === "replace" && <label className={styles.field} htmlFor={id}><span>新凭证值</span><input id={id} aria-label={`${label} 新值`} type="password" autoComplete="new-password" spellCheck={false} value={action?.action === "replace" ? action.value : ""} disabled={disabled} onChange={(event) => onChange({ action: "replace", value: event.target.value })} /><small>{hint ?? "只写入新值，现有值不会回填。凭证只暂存在当前页面内存中。"}</small></label>}
      {mode === "clear" && <p className={styles.warning}>保存后移除这个 Draft 的凭证关联，不撤销已发布版本的凭证。</p>}
    </> : <p className={styles.hint}>{readOnly ? "当前凭证状态与不可变配置分开展示；原值永不回填。" : "凭证变更需要 OWNER；你仍可编辑不改变凭证关联的配置。"}</p>}
  </section>;
}

export function ProfileResourceEditor({ tenantId = "", config, credentials, credentialStates, onChange, isOwner, disabled = false, readOnly = false, focusTarget }: ProfileResourceEditorProps) {
  const prefix = useId();
  const [newManaged, setNewManaged] = useState(false);
  const [category, setCategory] = useState<ResourceCategory>("models");
  const [chosenName, setChosenName] = useState<string>(Object.keys(config.models ?? {})[0] ?? "");
  const [newName, setNewName] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<ResourceFocusTarget | null>(null);
  const addNameRef = useRef<HTMLInputElement>(null);
  const resourceNames = Object.keys(config[category] ?? {});
  const name = resourceNames.includes(chosenName) ? chosenName : resourceNames[0] ?? "";
  const meta = categories.find((entry) => entry.key === category)!;
  const resourceStates = own(credentialStates[category], name);
  const resourceActions = own(credentials[category], name);
  const formDisabled = disabled;
  const associated = (purpose: string) => credentialAssociated(own(resourceStates, purpose));
  const protectedAudience = (purpose: string) => formDisabled || (!isOwner && associated(purpose));
  const hasAssociation = Object.values(resourceStates ?? {}).some(credentialAssociated) || Object.values(resourceActions ?? {}).some((action) => action.action !== "keep");
  const protectDelete = !isOwner && hasAssociation;
  const countError = resourceNames.length >= meta.limit ? `${meta.label} 最多 ${meta.limit} 个资源` : "";
  const addError = (newManaged && category === "storage" && newName && !["session", "memory", "artifact"].includes(newName) ? "Managed Storage 名称必须为 session、memory 或 artifact" : "") || countError || (newName && !namePattern.test(newName) ? "名称以小写字母开头，仅含小写字母、数字、_ 或 -，最多 64 个字符" : "") || (newName && Object.prototype.hasOwnProperty.call(config[category] ?? {}, newName) ? "当前分类中已存在同名资源" : "");
  const fieldID = (field: string) => `${prefix}-${category}-${name}-${field.replaceAll("/", "-")}`;

  useEffect(() => {
    if (!focusTarget) return;
    setCategory(focusTarget.category);
    setChosenName(focusTarget.name);
    setDeleteTarget(null);
    setNewName(Object.hasOwn(config[focusTarget.category] ?? {}, focusTarget.name) ? "" : focusTarget.name);
  }, [focusTarget]);
  useEffect(() => {
    if (!focusTarget || focusTarget.category !== category) return;
    if (!Object.hasOwn(config[category] ?? {}, focusTarget.name)) {
      addNameRef.current?.focus(); addNameRef.current?.scrollIntoView?.({ block: "nearest" }); return;
    }
    if (focusTarget.name !== name) return;
    const segments = (focusTarget.pointer ?? "").split("/").filter(Boolean).map((part) => part.replaceAll("~1", "/").replaceAll("~0", "~"));
    const resourceIndex = segments.indexOf(name, segments.indexOf(category) + 1);
    const fieldParts = resourceIndex < 0 ? [] : segments.slice(resourceIndex + 1);
    // Publication pointers refer to canonical credential fields; focus their public write-only controls.
    const credentialFields: Record<string, string> = { api_key_credential_id: "api_key", bearer_token_credential_id: "bearer_token", qdrant_api_key_credential_id: "qdrant_api_key", access_key_id_credential_id: "access_key_id", secret_access_key_credential_id: "secret_access_key", dsn_credential_id: category === "storage" && ["memory", "session"].includes(name) && own(config.storage, name)?.kind === `managed_${name}` ? "dsn_password" : "dsn" };
    let field = fieldParts.join("-");
    if (field === "auth-credential_id") field = "bearer_token";
    else if (field === "embedding-api_key_credential_id") field = "embedding_api_key";
    else field = own(credentialFields, field) ?? field;
    let target = document.getElementById(`${prefix}-${category}-${name}-${field}`)
      ?? document.getElementById(`${prefix}-${category}-${name}-${field}-action`)
      ?? document.getElementById(`${prefix}-${category}-${name}-${field}-section`);
    while (!target && fieldParts.length > 1) {
      fieldParts.pop();
      target = document.getElementById(`${prefix}-${category}-${name}-${fieldParts.join("-")}`);
    }
    if (!target || target.matches(":disabled")) target = document.getElementById(`${prefix}-${category}-${name}-heading`);
    target?.focus();
    target?.scrollIntoView?.({ block: "nearest", behavior: "smooth" });
  }, [focusTarget, category, name, prefix]);

  const updateResource = (resource: ResourceConfig) => {
    if (readOnly || disabled || !name) return;
    onChange({ ...config, [category]: { ...config[category], [name]: resource } }, credentials);
  };
  const updateCredential = (purpose: string, action?: CredentialAction) => {
    if (readOnly || disabled || !isOwner || !name) return;
    const purposes = { ...resourceActions };
    if (action) purposes[purpose] = action;
    else delete purposes[purpose];
    const next = Object.keys(purposes).length
      ? { ...credentials, [category]: { ...credentials[category], [name]: purposes } }
      : dropResourceActions(credentials, category, name);
    onChange(config, next);
  };
  const credential = (purpose: string, label: string, hint?: string) => <CredentialField key={purpose} id={fieldID(purpose)} label={label} state={own(resourceStates, purpose)} action={own(resourceActions, purpose)} onChange={(action) => updateCredential(purpose, action)} isOwner={isOwner} disabled={disabled} readOnly={readOnly} hint={hint} />;
  const textProps = { readOnly, disabled: formDisabled };
  const audienceHint = (purpose: string) => associated(purpose) ? isOwner ? "更改连接目标后，请替换凭证或清除草稿关联，再保存。" : "这个连接目标已有凭证关联，需要 OWNER 修改。" : undefined;
  const navigate = (nextCategory: ResourceCategory, nextName = Object.keys(config[nextCategory] ?? {})[0] ?? "") => { setCategory(nextCategory); setChosenName(nextName); setNewName(""); setDeleteTarget(null); setNewManaged(false); };
  const cancelDelete = () => setDeleteTarget(null);

  let form: ReactNode = null;
  if (name && category === "executors") {
    form = <FormSection title="SDK Sandbox" description="仅声明 sdk_sandbox 执行器；执行环境由部署管理，不接受 URL、主机路径或环境秘密。">
      <p>使用与 Agent Executors Requirement 相同的槽位名称。在 LLM 节点显式选择执行器及 workspace 工具；添加资源不会自动启用任何节点能力。</p>
      <p>workspace_save_artifact 还需同节点启用 Artifact，并在此 Profile 配置对应的 Artifact 存储。</p>
    </FormSection>;
  } else if (name && category === "models") {
    const model = own(config.models, name) ?? {};
    form = <>
      <FormSection title="模型连接" description="手动填写 OpenAI-compatible 服务参数，不执行模型发现或连接测试。">
        <TextField id={fieldID("model")} label="模型名称" value={model.model} onChange={(modelName) => updateResource({ ...model, model: modelName })} {...textProps} placeholder="例如 example-chat" />
        <TextField id={fieldID("base_url")} label="Base URL" value={model.base_url} onChange={(base_url) => updateResource({ ...model, base_url })} {...textProps} disabled={protectedAudience("api_key")} hint={audienceHint("api_key")} placeholder="https://model.example.com/v1" />
        <fieldset id={fieldID("capabilities")} tabIndex={-1} className={styles.capabilities} disabled={disabled || readOnly}><legend>Capabilities</legend><div>{["chat", "tool_call"].map((capability) => <label key={capability}><input type="checkbox" id={fieldID(`capabilities-${capability}`)} checked={(model.capabilities ?? []).includes(capability)} onChange={(event) => updateResource({ ...model, capabilities: event.target.checked ? [...new Set([...(model.capabilities ?? []), capability])] : (model.capabilities ?? []).filter((item) => item !== capability) })} />{capability}</label>)}{(model.capabilities ?? []).filter((item) => !["chat", "tool_call"].includes(item)).map((item, index) => <label key={`${index}:${item}`}><input type="checkbox" checked aria-label={`${item} · 不支持`} onChange={() => updateResource({ ...model, capabilities: (model.capabilities ?? []).filter((capability) => capability !== item) })} />{item} · 不支持</label>)}</div><small>发布至少需要 chat；调用工具时声明 tool_call。</small></fieldset>
      </FormSection>{credential("api_key", "API Key")}
    </>;
  } else if (name && category === "tools") {
    const tool = own(config.tools, name) ?? {};
    form = <>
      <FormSection title="MCP Streamable HTTP" description="填写已有 MCP 服务的工具标识，不在此页面发现或执行远程工具。">
        <TextField id={fieldID("server_url")} label="Server URL" value={tool.server_url} onChange={(server_url) => updateResource({ ...tool, server_url })} {...textProps} disabled={protectedAudience("bearer_token")} hint={audienceHint("bearer_token")} placeholder="https://mcp.example.com/mcp" />
        <div className={styles.row}><TextField id={fieldID("toolset_name")} label="Toolset Name" value={tool.toolset_name} onChange={(toolset_name) => updateResource({ ...tool, toolset_name })} {...textProps} /><TextField id={fieldID("tool_name")} label="Tool Name" value={tool.tool_name} onChange={(tool_name) => updateResource({ ...tool, tool_name })} {...textProps} /></div>
        <div className={styles.row}><label className={styles.field} htmlFor={fieldID("auth-kind")}><span>认证方式</span><select id={fieldID("auth-kind")} value={tool.auth?.kind ?? ""} disabled={readOnly || protectedAudience("bearer_token")} onChange={(event) => {
          const kind = event.target.value;
          onChange({ ...config, tools: { ...config.tools, [name]: { ...tool, auth: { kind } } } }, kind === "none" ? dropResourceActions(credentials, "tools", name) : credentials);
        }}><option value="">请选择认证方式</option><option value="none">none · 无认证</option><option value="bearer">bearer · Token 认证</option>{tool.auth?.kind && !["none", "bearer"].includes(tool.auth.kind) && <option value={tool.auth.kind}>{tool.auth.kind} · 不支持</option>}</select></label><TextField id={fieldID("capability")} label="Capability" value={tool.capability} onChange={(capability) => updateResource({ ...tool, capability })} {...textProps} hint={isAgentCapability(tool.capability ?? "") ? "必须与 Agent 对应 Tool Requirement 的 capability 完全一致；不是远端工具名称。" : "Capability 须以小写字母开头，仅含小写字母、数字、_、. 或 -，最长 128 个字符；发布仍以服务端校验为准。"} placeholder="例如 mcp.search" /></div>
      </FormSection>{tool.auth?.kind === "bearer" && credential("bearer_token", "Bearer Token")}
    </>;
  } else if (name && category === "knowledge") {
    const knowledge = own(config.knowledge, name) ?? {};
    const embedding = knowledge.embedding ?? {};
    form = <>
      {knowledge.kind === "managed_knowledge" ? <ManagedBackendSelect tenantId={tenantId} role="knowledge" value={knowledge} disabled={disabled} readOnly={readOnly} onChange={(selection) => updateResource({ ...knowledge, ...selection })}
        renderSelection={(selected) => {
          const editable = selected?.kind === "qdrant";
          const state = own(resourceStates, "qdrant_api_key");
          return <>
            {(editable || state) && <CredentialField id={fieldID("qdrant_api_key")} label="Qdrant API Key" state={state} action={own(resourceActions, "qdrant_api_key")} onChange={(action) => updateCredential("qdrant_api_key", action)} isOwner={isOwner} disabled={disabled} readOnly={readOnly || !editable} />}
            {editable ? <p>更换固定 backend 或 revision 后需显式替换 Qdrant 凭据。Embedding 凭据独立配置；连接目标由平台目录固定。</p> : !readOnly && <p>当前目录未确认该绑定为可选 Qdrant 后端，草稿 Qdrant 凭据编辑暂停，已有状态保留；已发布版本轮换沿用固定关联。</p>}
          </>;
        }} /> : <FormSection title="Qdrant 连接" description="使用已有 Collection；此页面不上传文件或创建索引。">
        <div className={styles.row}><TextField id={fieldID("host")} label="Qdrant 主机" value={knowledge.host} onChange={(host) => updateResource({ ...knowledge, host })} {...textProps} disabled={protectedAudience("qdrant_api_key")} /><NumericField id={fieldID("port")} label="Qdrant 端口" value={knowledge.port} onChange={(port) => updateResource({ ...knowledge, port })} {...textProps} disabled={protectedAudience("qdrant_api_key")} min={1} max={65535} /></div>
        <label className={styles.toggle}><input id={fieldID("tls")} type="checkbox" checked={knowledge.tls ?? false} disabled={readOnly || protectedAudience("qdrant_api_key")} onChange={(event) => updateResource({ ...knowledge, tls: event.target.checked })} />启用 Qdrant TLS</label>
        {audienceHint("qdrant_api_key") && <p className={styles.hint}>{audienceHint("qdrant_api_key")}</p>}
        <TextField id={fieldID("collection")} label="Collection" value={knowledge.collection} onChange={(collection) => updateResource({ ...knowledge, collection })} {...textProps} />
        {credential("qdrant_api_key", "Qdrant API Key")}
      </FormSection>}<FormSection title="Embedding 配置" description="配置用于向量检索的 Embedding 模型和固定维度。">
        <div className={styles.row}><TextField id={fieldID("embedding-model")} label="Embedding 模型" value={embedding.model} onChange={(model) => updateResource({ ...knowledge, embedding: { ...embedding, model } })} {...textProps} /><NumericField id={fieldID("embedding-dimensions")} label="Embedding 维度" value={embedding.dimensions} onChange={(dimensions) => updateResource({ ...knowledge, embedding: { ...embedding, dimensions } })} {...textProps} min={1} /></div>
        <TextField id={fieldID("embedding-base_url")} label="Embedding Base URL" value={embedding.base_url} onChange={(base_url) => updateResource({ ...knowledge, embedding: { ...embedding, base_url } })} {...textProps} disabled={protectedAudience("embedding_api_key")} hint={audienceHint("embedding_api_key")} />
        {credential("embedding_api_key", "Embedding API Key")}
      </FormSection>
    </>;
  } else if (name && category === "storage" && own(config.storage, name)?.kind?.startsWith("managed_")) {
    const storage = own(config.storage, name)!;
    const validRole = ["session", "memory", "artifact"].includes(name) && storage.kind === `managed_${name}`;
    form = <>
      {validRole ? <ManagedBackendSelect tenantId={tenantId} role={name as "session" | "memory" | "artifact"} value={storage} disabled={disabled} readOnly={readOnly} onChange={(selection) => updateResource({ ...storage, ...selection })}
        renderSelection={(selected) => {
          if (name === "artifact") {
            const editable = selected?.kind === "s3";
            return <>
              {[["access_key_id", "S3 Access Key ID"], ["secret_access_key", "S3 Secret Access Key"]].map(([purpose, label]) => {
                const state = own(resourceStates, purpose);
                return editable || state ? <CredentialField key={purpose} id={fieldID(purpose)} label={label} state={state} action={own(resourceActions, purpose)}
                  onChange={(action) => updateCredential(purpose, action)} isOwner={isOwner} disabled={disabled} readOnly={readOnly || !editable} /> : null;
              })}
              {editable && <p>首次关联需同时配置两项 S3 凭据；只写入新值，不回填已有值。更换 backend 或 revision 后需显式替换凭据。</p>}
              {!editable && !readOnly && <p>当前目录未确认该绑定为可选 S3 Artifact 后端，草稿凭据编辑暂停，已有状态保留；已发布版本轮换沿用固定关联。</p>}
            </>;
          }
          const state = own(resourceStates, "dsn_password");
          const passwordBackend = selected?.kind === "redis" || (name === "memory" && selected?.kind === "postgresql");
          const passwordLabel = name === "session" ? "Session 后端密码" : "Memory 后端密码";
          return passwordBackend || state ? <>
            <CredentialField id={fieldID("dsn_password")} label={passwordLabel} state={state} action={own(resourceActions, "dsn_password")}
              onChange={(action) => updateCredential("dsn_password", action)} isOwner={isOwner} disabled={disabled} readOnly={readOnly || !passwordBackend}
              hint="只填写原始密码，不是完整 DSN。现有密码不回填；更换后端或修订后必须显式替换密码。" />
            {!passwordBackend && !readOnly && <p>当前目录未确认该绑定为可选 {name === "session" ? "Redis Session" : "PostgreSQL 或 Redis Memory"} 后端，草稿密码编辑暂停，已有状态保留；已发布版本的轮换不受当前目录影响。</p>}
            {passwordBackend && <p>更换 backend 或 revision 后，请选择替换凭证；保持旧凭证不能重新绑定目标。</p>}
          </> : null;
        }} /> : <p role="alert">Managed Storage 类型必须与固定角色名 session、memory、artifact 一致。</p>}
      <p>平台托管资源不填写连接目标；后端凭据使用独立只写操作。跨类型替换请通过既有删除确认流程后重新创建，不自动迁移凭据。</p>
    </>;
  } else if (name && category === "storage") {
    const storage = own(config.storage, name) ?? {};
    const destination = storage.destination ?? {};
    const destinationDisabled = protectedAudience("dsn");
    form = <>
      <FormSection title="PostgreSQL 固定连接目标" description="非密钥连接目标与 DSN 凭证分开存储。修改目标后需要新的凭证关联。">
        <div className={styles.row}><TextField id={fieldID("destination-host")} label="数据库主机" value={destination.host} onChange={(host) => updateResource({ ...storage, destination: { ...destination, host } })} {...textProps} disabled={destinationDisabled} /><NumericField id={fieldID("destination-port")} label="数据库端口" value={destination.port} onChange={(port) => updateResource({ ...storage, destination: { ...destination, port } })} {...textProps} disabled={destinationDisabled} min={1} max={65535} /></div>
        <div className={styles.row}><TextField id={fieldID("destination-database")} label="数据库名称" value={destination.database} onChange={(database) => updateResource({ ...storage, destination: { ...destination, database } })} {...textProps} disabled={destinationDisabled} /><TextField id={fieldID("destination-username")} label="数据库用户名" value={destination.username} onChange={(username) => updateResource({ ...storage, destination: { ...destination, username } })} {...textProps} disabled={destinationDisabled} /></div>
        <label className={styles.field} htmlFor={fieldID("destination-sslmode")}><span>SSL 模式</span><select id={fieldID("destination-sslmode")} value={destination.sslmode ?? ""} disabled={readOnly || destinationDisabled} onChange={(event) => updateResource({ ...storage, destination: { ...destination, sslmode: event.target.value } })}><option value="">请选择</option><option value="disable">disable</option><option value="require">require</option><option value="verify-full">verify-full</option>{destination.sslmode && !["disable", "require", "verify-full"].includes(destination.sslmode) && <option value={destination.sslmode}>{destination.sslmode} · 不支持</option>}</select></label>
        {audienceHint("dsn") && <p className={styles.hint}>{audienceHint("dsn")}</p>}
      </FormSection>{credential("dsn", "PostgreSQL DSN", "使用包含密码、单主机及明确 sslmode 的 PostgreSQL URI，且连接目标必须与上方一致。只有密码被后端加密保存；原值不会回填。")}
    </>;
  }

  return <div className={styles.editor}>
    <aside className={styles.navigation} aria-label="资源导航">
      <header><strong>资源配置</strong><span>四类运行依赖</span></header>
      {categories.map(({ key, label, detail, icon: Icon }) => <div key={key} className={styles.category} data-resource-category={key}>
        <button className={`${styles.categoryButton} ${category === key ? styles.activeCategory : ""}`} type="button" aria-label={`${label} · ${Object.keys(config[key] ?? {}).length}`} aria-expanded={category === key} onClick={() => navigate(key)}><Icon size={16} aria-hidden="true" /><span><strong>{label}</strong><small>{detail}</small></span><span className={styles.count}>{Object.keys(config[key] ?? {}).length}</span></button>
        {category === key && <div className={styles.resourceList}>{Object.keys(config[key] ?? {}).length ? Object.keys(config[key] ?? {}).map((resourceName) => <button type="button" key={resourceName} aria-pressed={name === resourceName} className={`${styles.resource} ${name === resourceName ? styles.activeResource : ""}`} onClick={() => navigate(key, resourceName)} title={resourceName}><span className={styles.dot} />{resourceName}</button>) : <p>暂无资源</p>}</div>}
      </div>)}
      {!readOnly && <section className={styles.addResource}>
        {(category === "storage" || category === "knowledge") && <label><input type="checkbox" checked={newManaged} disabled={disabled} onChange={(event) => setNewManaged(event.target.checked)} />新增平台托管资源（managed）</label>}
        <label className={styles.field} htmlFor={`${prefix}-new-resource`}><span>新增 {meta.label} 资源</span><input ref={addNameRef} id={`${prefix}-new-resource`} aria-label="新资源名称" value={newName} disabled={disabled} maxLength={64} aria-invalid={!!addError} aria-describedby={`${prefix}-name-help`} onChange={(event) => setNewName(event.target.value)} placeholder={category === "storage" ? "session、memory 或 artifact" : "resource_name"} /></label>
        <small id={`${prefix}-name-help`} className={addError ? styles.error : styles.hint}>{addError || "名称在当前分类内唯一"}</small>
        <Button type="button" variant="secondary" disabled={disabled || !newName || !!addError} onClick={() => {
          if (!namePattern.test(newName) || addError) return;
          onChange({ ...config, [category]: { ...config[category], [newName]: newManaged && (category === "storage" || category === "knowledge") ? { kind: category === "knowledge" ? "managed_knowledge" : `managed_${newName}`, ...(category === "knowledge" ? { embedding: {} } : {}) } : initialResource(category) } }, credentials);
          setChosenName(newName); setNewName("");
        }}><Plus size={14} aria-hidden="true" />添加资源</Button>
      </section>}
    </aside>
    <div className={styles.content} data-resource-category={category}>
      {name ? <>
        <header className={styles.resourceHeading}><div><span className={styles.eyebrow}>{meta.label} / {readOnly ? "不可变配置" : "Draft 配置"}</span><h2 id={fieldID("heading")} tabIndex={-1}>{name}</h2><div className={styles.kindRow}><code id={fieldID("kind")} tabIndex={-1}>{own(config[category] as Record<string, ResourceConfig> | undefined, name)?.kind || "类型未设置"}</code>{!readOnly && own(config[category] as Record<string, ResourceConfig> | undefined, name)?.kind !== meta.kind && !own(config[category] as Record<string, ResourceConfig> | undefined, name)?.kind?.startsWith("managed_") && <Button type="button" variant="secondary" disabled={disabled || protectDelete} onClick={() => updateResource({ ...own(config[category] as Record<string, ResourceConfig> | undefined, name), kind: meta.kind })}>使用 {meta.kind}</Button>}</div></div>{!readOnly && <Button type="button" variant="ghost" disabled={disabled || protectDelete} aria-label={`删除资源 ${name}`} title={protectDelete ? "已有凭证关联，需要 OWNER 删除" : "删除此 Draft 资源"} onClick={() => setDeleteTarget({ category, name })}><Trash2 size={16} aria-hidden="true" />删除</Button>}</header>
        {protectDelete && !readOnly && <p className={styles.ownerNotice}>资源持有凭证关联；删除或修改关联目标需要 OWNER。其余非密钥字段仍可编辑。</p>}
        {managedProfileIssues(config).filter((issue) => issue.pointer.startsWith(`/${category}/${name.replaceAll("~", "~0").replaceAll("/", "~1")}/`)).map((issue) => <p role="alert" key={issue.pointer}>{issue.message} <code>{issue.pointer}</code></p>)}
        <div className={styles.form} key={`${category}:${name}`}>{form}</div>
      </> : <div className={styles.empty}><meta.icon size={30} aria-hidden="true" /><h2>尚无 {meta.label} 资源</h2><p>{readOnly ? "此版本未声明这一类资源。" : "在左侧输入唯一名称，添加一个运行资源。"}</p></div>}
    </div>
    {deleteTarget && <ProfileDialog title={`删除资源 ${deleteTarget.name}`} onClose={cancelDelete} busy={disabled} footer={<><Button type="button" variant="secondary" onClick={cancelDelete} disabled={disabled}>取消</Button><Button type="button" variant="danger" disabled={disabled || readOnly || protectDelete} onClick={() => {
      if (readOnly || disabled || protectDelete) return;
      const resources: Record<string, ResourceConfig> = { ...config[deleteTarget.category] };
      delete resources[deleteTarget.name];
      onChange({ ...config, [deleteTarget.category]: resources }, dropResourceActions(credentials, deleteTarget.category, deleteTarget.name));
      setDeleteTarget(null); setChosenName(Object.keys(resources)[0] ?? ""); addNameRef.current?.focus();
    }}>确认删除</Button></>}><p className={styles.deleteDescription}>仅从当前 Draft 移除 {deleteTarget.category} 中的 {deleteTarget.name} 及待提交凭证操作。已发布配置保持不变。</p></ProfileDialog>}
  </div>;
}
