"use client";
import Link from "next/link";
import { useEffect, useState } from "react";
import { safeDeploymentReturn } from "../../lib/deployment-editor-state";
import type { ResourceCategory } from "../../lib/runtime-profile-api";
import styles from "./deployment.module.css";

export function DeploymentReturnLink({ tenantId, onFocus }: { tenantId: string; onFocus?(target: { category: ResourceCategory; name: string }): void }) {
  const [href, setHref] = useState<string | null>(null);
  useEffect(() => {
    const query = new URLSearchParams(window.location.search);
    const target = safeDeploymentReturn(query.get("returnTo"), tenantId);
    setHref(target);
    const category = query.get("focusCategory"); const name = query.get("focusName");
    if (target && category && ["models", "tools", "knowledge", "storage"].includes(category) && name && /^[a-z][a-z0-9_-]{0,63}$/.test(name)) onFocus?.({ category: category as ResourceCategory, name });
  }, [tenantId, onFocus]);
  if (!href) return null;
  return <div className={styles.banner}>你正在修复部署来源。配置结构修改后请保存并发布新的 Profile Revision；凭据更新后也需回到部署重新校验。 <Link className={styles.link} href={href}>返回部署准备并重新选择 →</Link></div>;
}
