import type { ProfileConfig } from "./runtime-profile-api";
import { BACKEND_ID_PATTERN } from "./runtime-backend-api";
export type ManagedProfileIssue = { pointer: string; message: string };
/** Public config projection checks, not a compiler or a credential-ID decoder. */
export function managedProfileIssues(config: ProfileConfig): ManagedProfileIssue[] {
  const issues: ManagedProfileIssue[] = [];
  for (const category of ["storage", "knowledge"] as const) {
    for (const [name, value] of Object.entries(config[category] ?? {})) {
      if (!value?.kind?.startsWith("managed_")) continue;
      const p = `/${category}/${name.replaceAll("~", "~0").replaceAll("/", "~1")}`;
      const add = (field: string, message: string) => issues.push({ pointer: `${p}/${field}`, message });
      if (category === "knowledge" ? value.kind !== "managed_knowledge" : !["session", "memory", "artifact"].includes(name) || value.kind !== `managed_${name}`) add("kind", "托管类型必须匹配固定资源角色。");
      for (const key of Object.keys(value)) if (!["kind", "backend_id", "backend_revision", ...(category === "knowledge" ? ["embedding"] : [])].includes(key)) add(key, "托管配置不接受平台连接目标、凭据或其他分支字段。");
      if (!value.backend_id || !BACKEND_ID_PATTERN.test(value.backend_id)) add("backend_id", "请选择平台后端。");
      if (!Number.isSafeInteger(value.backend_revision) || (value.backend_revision ?? 0) < 1) add("backend_revision", "请选择有效的后端修订（正安全整数）。");
      if (category === "knowledge") {
        const embedding = config.knowledge[name].embedding;
        if (!embedding) { add("embedding", "托管 Knowledge 必须配置 Embedding。"); continue; }
        for (const key of Object.keys(embedding)) if (!["model", "base_url", "dimensions"].includes(key)) add(`embedding/${key}`, "Embedding 公共配置不接受凭据 ID 或秘密值。");
        if (!embedding.model?.trim()) add("embedding/model", "请填写 Embedding 模型。");
        try { const u = new URL(embedding.base_url ?? ""); if (!["http:", "https:"].includes(u.protocol) || u.username || u.password) throw new Error(); } catch { add("embedding/base_url", "请填写不含凭据的 HTTP(S) Embedding 地址。"); }
        if (!Number.isInteger(embedding.dimensions) || (embedding.dimensions ?? 0) < 1 || (embedding.dimensions ?? 0) > 65536) add("embedding/dimensions", "Embedding 维度必须为 1–65536 的整数。");
      }
    }
  }
  return issues;
}
