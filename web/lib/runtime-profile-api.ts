/** Public authoring DTOs. These are config projections, never canonical Specs or credential IDs. */
export type ResourceCategory = "models" | "tools" | "knowledge" | "storage" | "executors";
export type ProfilePageInput = { offset: number; limit: number };

export type RuntimeProfile = {
  id: string;
  tenant_id: string;
  name: string;
  description: string;
  latest_revision_number: number | null;
  created_by: string;
  created_at: string;
  updated_at: string;
};

// Drafts intentionally allow incomplete resource fields. Publication applies
// the backend's closed V1 kinds/capabilities and completeness checks.
export type ModelConfig = {
  kind?: string;
  model?: string;
  base_url?: string;
  capabilities?: string[];
};
export type ToolConfig = {
  kind?: string;
  server_url?: string;
  toolset_name?: string;
  tool_name?: string;
  auth?: { kind?: string };
  capability?: string;
};
export type KnowledgeConfig = {
  kind?: string;
  backend_id?: string;
  backend_revision?: number;
  host?: string;
  port?: number;
  tls?: boolean;
  collection?: string;
  embedding?: { model?: string; base_url?: string; dimensions?: number };
};
export type StorageDestination = {
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  sslmode?: string;
};
export type StorageConfig = { kind?: string; backend_id?: string; backend_revision?: number; destination?: StorageDestination };
export type ExecutorConfig = { kind?: string };
export type ProfileConfig = {
  executors?: Record<string, ExecutorConfig>;
  models: Record<string, ModelConfig>;
  tools: Record<string, ToolConfig>;
  knowledge: Record<string, KnowledgeConfig>;
  storage: Record<string, StorageConfig>;
};

export type CredentialState = {
  configured: boolean;
  status: "unconfigured" | "active" | "cleared";
  credential_revision: number;
  association_token?: string;
};
export type CredentialStates = Partial<Record<ResourceCategory, Record<string, Record<string, CredentialState>>>>;
export type CredentialAction = (
  | { action: "keep" | "clear"; value?: never }
  | { action: "replace"; value: string }
) & { expected_credential_revision?: 0 };
export type CredentialActions = Partial<Record<ResourceCategory, Record<string, Record<string, CredentialAction>>>>;

export type ProfileDraft = {
  tenant_id: string;
  profile_id: string;
  draft_revision: number;
  schema_version: string;
  credential_protocol_version: "v1";
  config: ProfileConfig;
  credential_states?: CredentialStates;
  updated_by: string;
  updated_at: string;
};
export type ProfileRevisionSummary = {
  id: string;
  tenant_id: string;
  profile_id: string;
  revision_number: number;
  source_draft_revision: number;
  schema_version: string;
  spec_digest: string;
  published_by: string;
  published_at: string;
};
export type ProfileRevision = ProfileRevisionSummary & {
  credential_protocol_version: "v1";
  config: ProfileConfig;
  // Publication is immutable; GET adds the current, potentially changed states.
  credential_states?: CredentialStates;
};
export type RuntimeProfilePage = ProfilePageInput & { runtime_profiles: RuntimeProfile[]; total: number };
export type ProfileRevisionPage = ProfilePageInput & { revisions: ProfileRevisionSummary[]; total: number };
export type ProfileWrite = {
  expected_draft_revision: number;
  credential_protocol_version: "v1";
  config: ProfileConfig;
  credentials?: CredentialActions;
};
/** A successful write receipt is not a Draft. Re-read the Draft to obtain current credential states. */
export type DraftWriteReceipt = { profile_id: string; draft_revision: number; updated_at: string };
export type PublishedCredentialTarget = {
  profile_revision_number: number;
  category: ResourceCategory;
  resource_name: string;
  purpose_field: string;
  association_token: string;
};
export type CredentialUpdate = {
  target: PublishedCredentialTarget;
  expected_credential_revision: number;
} & ({ action: "clear"; value?: never } | { action: "replace"; value: string });
export type CredentialUpdateReceipt = { credential_revision: number; status: "active" | "cleared" };
export type ProfileValidationDiagnostic = {
  code: string;
  severity: "error" | "warning";
  pointer: string;
  // These are singular kinds, unlike the config's category keys.
  resource_kind: "model" | "tool" | "knowledge" | "storage" | "executor" | null;
  resource_key: string | null;
  message: string;
};
export type ProfileValidationReport = {
  valid: boolean;
  schema_version: string;
  draft_revision: number;
  diagnostics: ProfileValidationDiagnostic[];
};

export class RuntimeProfileApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly validation?: ProfileValidationReport,
  ) {
    super(message);
    this.name = "RuntimeProfileApiError";
  }
}

const base = "/api/control";
function profilePath(tenantId: string, profileId?: string) {
  const collection = `/v1/tenants/${encodeURIComponent(tenantId)}/runtime-profiles`;
  return profileId === undefined ? collection : `${collection}/${encodeURIComponent(profileId)}`;
}
function pageQuery(page: ProfilePageInput) {
  return new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
}
function json(method: string, body: unknown, idempotencyKey?: string): RequestInit {
  return {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(idempotencyKey === undefined ? {} : { "Idempotency-Key": idempotencyKey }),
    },
    body: JSON.stringify(body),
  };
}
async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${base}${path}`, { ...init, credentials: "include", cache: "no-store" });
  if (!response.ok) {
    const body = await response.json().catch(() => null) as {
      error?: { code?: string; message?: string };
      validation?: ProfileValidationReport;
    } | null;
    throw new RuntimeProfileApiError(
      response.status,
      body?.error?.code ?? "HTTP_ERROR",
      body?.error?.message ?? `Control API returned HTTP ${response.status}`,
      body?.validation,
    );
  }
  return response.json() as Promise<T>;
}

// Mutation retries and idempotency keys belong to the page's logical operation.
// This transport never generates keys, retries writes, or replaces receipts with hidden GETs.
export const runtimeProfileApi = {
  listProfiles(tenantId: string, page: ProfilePageInput) {
    return request<RuntimeProfilePage>(`${profilePath(tenantId)}?${pageQuery(page)}`);
  },
  createProfile(tenantId: string, input: { name: string; description: string }) {
    return request<{ profile: RuntimeProfile; draft: ProfileDraft }>(profilePath(tenantId), json("POST", input));
  },
  getProfile(tenantId: string, profileId: string) {
    return request<RuntimeProfile>(profilePath(tenantId, profileId));
  },
  updateProfile(tenantId: string, profileId: string, input: { name?: string; description?: string }) {
    return request<RuntimeProfile>(profilePath(tenantId, profileId), json("PATCH", input));
  },
  getDraft(tenantId: string, profileId: string) {
    return request<ProfileDraft>(`${profilePath(tenantId, profileId)}/draft`);
  },
  saveDraft(tenantId: string, profileId: string, input: ProfileWrite, idempotencyKey: string) {
    return request<DraftWriteReceipt>(`${profilePath(tenantId, profileId)}/draft`, json("PUT", input, idempotencyKey));
  },
  validateDraft(tenantId: string, profileId: string, input: { expected_revision: number }) {
    return request<ProfileValidationReport>(`${profilePath(tenantId, profileId)}/draft/validate`, json("POST", input));
  },
  publishRevision(tenantId: string, profileId: string, input: { expected_revision: number }) {
    return request<{ revision: ProfileRevision }>(`${profilePath(tenantId, profileId)}/revisions`, json("POST", input));
  },
  listRevisions(tenantId: string, profileId: string, page: ProfilePageInput) {
    return request<ProfileRevisionPage>(`${profilePath(tenantId, profileId)}/revisions?${pageQuery(page)}`);
  },
  getRevision(tenantId: string, profileId: string, revisionNumber: number) {
    return request<ProfileRevision>(`${profilePath(tenantId, profileId)}/revisions/${encodeURIComponent(String(revisionNumber))}`);
  },
  updateCredential(tenantId: string, profileId: string, input: CredentialUpdate, idempotencyKey: string) {
    return request<CredentialUpdateReceipt>(`${profilePath(tenantId, profileId)}/credentials/update`, json("POST", input, idempotencyKey));
  },
};
