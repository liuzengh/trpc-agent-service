import { AlertTriangle, Inbox, LoaderCircle, ShieldX } from "lucide-react";

export type StateKind = "loading" | "empty" | "forbidden" | "error";

const content: Record<StateKind, { title: string; detail: string }> = {
  loading: { title: "正在加载", detail: "正在从服务端读取最新数据" },
  empty: { title: "暂无数据", detail: "当前租户尚未创建此类资源" },
  forbidden: { title: "没有访问权限", detail: "当前角色不能执行此操作" },
  error: { title: "服务暂时不可用", detail: "请稍后重试" },
};

export function AsyncState({ kind, retry }: { kind: StateKind; retry?: () => void }) {
  const Icon = kind === "loading" ? LoaderCircle : kind === "empty" ? Inbox : kind === "forbidden" ? ShieldX : AlertTriangle;
  return (
    <div className="async-state" role={kind === "error" ? "alert" : "status"}>
      <Icon className={kind === "loading" ? "spin" : ""} aria-hidden="true" />
      <strong>{content[kind].title}</strong>
      <span>{content[kind].detail}</span>
      {retry && <button onClick={retry}>重试</button>}
    </div>
  );
}
