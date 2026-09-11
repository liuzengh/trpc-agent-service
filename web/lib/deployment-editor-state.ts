import type { AgentVersion } from "./control-api";
import type { ProfileRevision } from "./runtime-profile-api";
import type { DeploymentInput, PublishDeploymentInput } from "./deployment-api";
export type Selection = { agentId: string; agentVersion: number; profileId: string; profileRevision: number };
export type PendingOperation = { kind: "create"; key: string; body: { name: string; description: string } } | { kind: "publish"; key: string; body: PublishDeploymentInput };
export type Preparation = { version: 1; selection: Selection; name: string; description: string; pending: PendingOperation | null; sourceQuery?: string };
export const emptySelection = (): Selection => ({ agentId: "", agentVersion: 0, profileId: "", profileRevision: 0 });
export const deploymentHref = (tenant: string, id?: string, revision?: number) => `/tenants/${encodeURIComponent(tenant)}/deployments${id ? `/${encodeURIComponent(id)}` : ""}${revision ? `/revisions/${revision}` : ""}`;
export function inputFrom(selection: Selection): DeploymentInput | null {
  if (!selection.agentId || !selection.profileId || !Number.isSafeInteger(selection.agentVersion) || selection.agentVersion < 1 || !Number.isSafeInteger(selection.profileRevision) || selection.profileRevision < 1) return null;
  return { schema_version: "v1", agent: { agent_id: selection.agentId, version_number: selection.agentVersion }, profile: { profile_id: selection.profileId, revision_number: selection.profileRevision } };
}
export const selectionFrom = (input: DeploymentInput): Selection => ({ agentId: input.agent.agent_id, agentVersion: input.agent.version_number, profileId: input.profile.profile_id, profileRevision: input.profile.revision_number });
export const inputFingerprint = (input: DeploymentInput) => JSON.stringify(inputFrom(selectionFrom(input)));
export function selectionQuery(selection: Selection): string {
  return new URLSearchParams({ agent: selection.agentId, version: String(selection.agentVersion), profile: selection.profileId, revision: String(selection.profileRevision) }).toString();
}
export function selectionFromQuery(query: URLSearchParams): Selection {
  const num = (s: string | null) => { const n = Number(s); return Number.isSafeInteger(n) && n > 0 ? n : 0; };
  return { agentId: (query.get("agent") ?? "").slice(0, 128), agentVersion: num(query.get("version")), profileId: (query.get("profile") ?? "").slice(0, 128), profileRevision: num(query.get("revision")) };
}
const prefix = "deployment-preparation:v1:";
export const preparationKey = (user: string, tenant: string, id = "new") => prefix + [user, tenant, id].map(encodeURIComponent).join(":");
export function clearDeploymentPreparations(storage: Storage) {
  for (const key of Object.keys(storage)) if (key.startsWith(prefix)) storage.removeItem(key);
}
export function savePreparation(storage: Storage, key: string, state: Preparation): boolean {
  try { storage.setItem(key, JSON.stringify(state)); return true; } catch { return false; }
}
export function loadPreparation(storage: Storage, key: string): Preparation | null {
  try {
    const raw: unknown = JSON.parse(storage.getItem(key) ?? "null");
    if (!raw || typeof raw !== "object") return null;
    const p = raw as Preparation;
    if (p.version !== 1 || !p.selection || typeof p.name !== "string" || typeof p.description !== "string") return null;
    const { agentId, agentVersion, profileId, profileRevision } = p.selection;
    if (typeof agentId !== "string" || typeof profileId !== "string" || !Number.isSafeInteger(agentVersion) || !Number.isSafeInteger(profileRevision)) return null;
    if (p.pending) {
      if (!p.pending.key || p.pending.key.length > 128) return null;
      if (p.pending.kind === "publish") {
        const b = p.pending.body;
        if (!b?.input || !inputFrom(selectionFrom(b.input)) || (b.expected_latest_revision_number !== null && (!Number.isSafeInteger(b.expected_latest_revision_number) || b.expected_latest_revision_number < 1))) return null;
      } else if (p.pending.kind !== "create" || typeof p.pending.body?.name !== "string" || typeof p.pending.body?.description !== "string") return null;
    }
    const selection = { agentId, agentVersion, profileId, profileRevision };
    if (agentId.length > 128 || profileId.length > 128 || agentVersion < 0 || profileRevision < 0 || p.name.length > 1024 || p.description.length > 16384) return null;
    let pending: PendingOperation | null = null;
    if (p.pending) {
      if (typeof p.pending.key !== "string" || !/^[\x21-\x7e]{1,128}$/.test(p.pending.key)) return null;
      if (p.pending.kind === "create") pending = { kind: "create", key: p.pending.key, body: { name: p.pending.body.name, description: p.pending.body.description } };
      else {
        if (p.pending.body.input.schema_version !== "v1") return null;
        const input = inputFrom(selectionFrom(p.pending.body.input));
        if (!input) return null;
        pending = { kind: "publish", key: p.pending.key, body: { expected_latest_revision_number: p.pending.body.expected_latest_revision_number, input } };
        Object.assign(selection, selectionFrom(input));
      }
    }
    return { version: 1, selection, name: p.name, description: p.description, pending, ...(typeof p.sourceQuery === "string" && p.sourceQuery.length <= 2048 ? { sourceQuery: p.sourceQuery } : {}) };
  } catch { return null; }
}
export type PairingRow = { category: string; name: string; nodes: string[]; present: boolean; note: string };
export function pairingRows(agent: AgentVersion, profile: ProfileRevision): PairingRow[] {
  const rows: PairingRow[] = [];
  for (const category of ["models", "tools", "knowledge"] as const) {
    const requirements = agent.spec.requirements?.[category] ?? {};
    for (const name of Object.keys(requirements).sort()) {
      const nodes = Object.entries(agent.spec.nodes).filter(([, node]) => {
        if (node.kind !== "llm") return false;
        return category === "models" ? node.model_slot === name : (category === "tools" ? node.tool_slots : node.knowledge_slots)?.includes(name);
      }).map(([id]) => id);
      const present = Object.hasOwn(profile.config[category], name);
      rows.push({ category, name, nodes, present, note: !present ? "缺少同名资源" : !nodes.length ? "名称匹配 · 声明未使用" : "名称匹配 · 能力待服务端校验" });
    }
  }
  for (const name of ["session", "memory"]) {
    const present = Object.hasOwn(profile.config.storage, name);
    rows.push({ category: "storage", name, nodes: [], present: present || name === "memory", note: present ? "固定运行角色" : name === "memory" ? "未声明 · 不启用" : "缺少必需 storage.session" });
  }
  return rows;
}
export function safeDeploymentReturn(value: string | null, tenant: string): string | null {
  if (!value || !value.startsWith("/") || value.startsWith("//") || /[\\\r\n]/.test(value)) return null;
  try {
    const url = new URL(value, "https://deployment.invalid");
    const root = deploymentHref(tenant);
    if (url.origin !== "https://deployment.invalid" || !(url.pathname === root || url.pathname.startsWith(root + "/"))) return null;
    if (decodeURIComponent(url.pathname).split("/").some((part) => part === ".." || part === ".")) return null;
    return url.pathname + url.search;
  } catch { return null; }
}
export function establishPreparationIdentity(storage: Storage, user: string) {
  const key = "deployment-preparation-user:v1";
  const previous = storage.getItem(key);
  if (previous && previous !== user) clearDeploymentPreparations(storage);
  storage.setItem(key, user);
}
