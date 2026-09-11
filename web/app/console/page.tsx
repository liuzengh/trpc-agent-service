"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { Button } from "../../components/ui";
import { controlApi, ControlApiError } from "../../lib/control-api";

const bootstrapTimeoutMs = 10_000;
const bootstrapTimeoutMessage = "恢复会话超过 10 秒，请确认 Web 服务与 Control API 均可访问后重试。";

export default function ConsoleEntry() {
  const router = useRouter();
  const [failure, setFailure] = useState("");
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    let cancelled = false;
    let timedOut = false;
    const timeout = window.setTimeout(() => {
      timedOut = true;
      if (!cancelled) setFailure(bootstrapTimeoutMessage);
    }, bootstrapTimeoutMs);
    setFailure("");
    void controlApi.getMe().then(async (session) => {
      if (cancelled || timedOut) return;
      if (session.password_change_required) {
        router.replace("/change-password");
        return;
      }
      try {
        await controlApi.getCapabilities();
        if (cancelled || timedOut) return;
        router.replace("/admin");
      } catch (error) {
        if (!(error instanceof ControlApiError) || error.status !== 403) throw error;
        await controlApi.listMyTenants();
        if (cancelled || timedOut) return;
        router.replace("/tenants");
      }
    }).catch((error) => {
      if (cancelled || timedOut) return;
      if (error instanceof ControlApiError && error.status === 401) {
        router.replace("/login");
        return;
      }
      setFailure(error instanceof Error ? error.message : "控制台入口加载失败");
    }).finally(() => {
      window.clearTimeout(timeout);
    });
    return () => {
      cancelled = true;
      window.clearTimeout(timeout);
    };
  }, [attempt, router]);
  if (failure) return <main className="center-page"><h1>{failure === bootstrapTimeoutMessage ? "控制台连接超时" : "控制台暂时不可用"}</h1><p>{failure}</p><Button onClick={() => setAttempt((value) => value + 1)} variant="secondary">重试</Button></main>;
  return <main className="center-page"><div className="loading-mark" /><h1>正在打开控制台</h1><p>正在恢复安全会话与工作区…</p></main>;
}
