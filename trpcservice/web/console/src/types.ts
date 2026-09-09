export type Dict = Record<string, any>;
export interface Principal {
  name: string;
  role: string;
  tenant_ids?: string[];
  csrf_token: string;
}
export interface Tenant {
  tenant_id: string;
  display_name: string;
  status: string;
  version: number;
  quota_config: Dict;
  audit_policy: Dict;
}
export interface AgentApp {
  app_id: string;
  tenant_id: string;
  name: string;
  description: string;
  status: string;
  stable_revision_id?: string;
  version: number;
  rollout_policy: Dict;
  updated_at: string;
}
export interface Revision {
  revision_id: string;
  tenant_id: string;
  app_id: string;
  revision_no: number;
  agent_type: string;
  agent_config: Dict;
  model_config: Dict;
  tool_policy: Dict;
  knowledge_config: Dict;
  memory_config: Dict;
  guardrail_config: Dict;
  checksum?: string;
  created_at?: string;
  created_by?: string;
}
export interface Stored<T = Dict> {
  id: string;
  tenant_id: string;
  app_id: string;
  owner_id: string;
  status: string;
  version: number;
  data: T;
  created_at: string;
  updated_at: string;
  expires_at: string;
}
export interface Draft {
  config: Revision;
  source_revision_id: string;
  last_editor: string;
  published_revision_id?: string;
}
export interface Skill {
  name: string;
  version: string;
  checksum: string;
  description: string;
}
export interface Tool {
  name: string;
  description: string;
  requires_approval: boolean;
}
export interface Workspace {
  app: AgentApp;
  draft: Stored<Draft>;
  stable: Revision;
  skills: Skill[];
  tools: Tool[];
  startup_model_name: string;
}
export interface Issue {
  code: string;
  field: string;
  severity: string;
  message: string;
  suggestion: string;
}
export interface Validation {
  valid: boolean;
  check_id: string;
  runtime_status: string;
  issues: Issue[];
  dependencies: Dict[];
}
export interface Page<T> {
  items: T[];
  next?: string;
}
export const writable = (p: Principal) =>
  ["superadmin", "tenant_admin"].includes(p.role);
export const operable = (p: Principal) =>
  ["superadmin", "tenant_admin", "operator"].includes(p.role);
export const date = (value?: string) =>
  value && !value.startsWith("0001")
    ? new Date(value).toLocaleString("zh-CN", { hour12: false })
    : "—";
export const roleName = (role: string) =>
  ({
    superadmin: "平台管理员",
    tenant_admin: "租户管理员",
    operator: "运维操作员",
    auditor: "只读审计员",
  })[role] || role;
