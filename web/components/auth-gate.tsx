"use client";

import { usePathname, useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import { controlApi, ControlApiError, type Session } from "../lib/control-api";
import { AppShell } from "./app-shell";

export function AuthGate({ children, requireOperator = false }: { children: ReactNode; requireOperator?: boolean }) {
  const pathname = usePathname();
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [capabilities, setCapabilities] = useState<string[]>([]);
  const [failure, setFailure] = useState("");
  useEffect(() => {
    let cancelled = false;
    void controlApi.getMe().then(async (current) => {
      if (cancelled) return;
      if (current.password_change_required) return router.replace("/change-password");
      try {
        const result = await controlApi.getCapabilities();
        if (!cancelled) setCapabilities(result.capabilities);
      } catch (error) {
        if (requireOperator && error instanceof ControlApiError && error.status === 403) return router.replace("/tenants");
        if (!(error instanceof ControlApiError) || error.status !== 403) throw error;
      }
      if (!cancelled) setSession(current);
    }).catch((error) => {
      if (error instanceof ControlApiError && error.status === 401) router.replace(`/login?next=${encodeURIComponent(pathname)}`);
      else setFailure(error instanceof Error ? error.message : "会话恢复失败");
    });
    return () => { cancelled = true; };
  }, [pathname, requireOperator, router]);
  if (failure) return <main className="center-page"><h1>控制台暂时不可用</h1><p>{failure}</p></main>;
  if (!session) return <main className="center-page"><div className="loading-mark" /><h1>正在验证身份</h1><p>正在连接本地 Control API…</p></main>;
  return <AppShell user={session.user} capabilities={capabilities}>{children}</AppShell>;
}
