import { Alert, Button, Empty, Tag } from "antd";
import { Component, type CSSProperties, type ReactNode } from "react";
import type { Validation } from "./types";

export class PageBoundary extends Component<
  { children: ReactNode },
  { failed: boolean }
> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    if (this.state.failed)
      return (
        <Alert
          type="error"
          showIcon
          title="页面未能正常显示"
          description="已保存的草稿和后端任务不受页面异常影响。可以切换页面，或重新加载当前页面。"
          action={<Button onClick={() => location.reload()}>重新加载</Button>}
        />
      );
    return this.props.children;
  }
}

const paths: Record<string, ReactNode> = {
  grid: (
    <>
      <rect x="3" y="3" width="7" height="7" rx="2" />
      <rect x="14" y="3" width="7" height="7" rx="2" />
      <rect x="3" y="14" width="7" height="7" rx="2" />
      <rect x="14" y="14" width="7" height="7" rx="2" />
    </>
  ),
  agent: (
    <>
      <rect x="4" y="7" width="16" height="13" rx="5" />
      <path d="M12 3v4M8 12v2m8-2v2m-7 3h6M1 11v5m22-5v5" />
    </>
  ),
  layers: (
    <>
      <path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5" />
    </>
  ),
  channel: (
    <>
      <rect x="3" y="4" width="18" height="13" rx="3" />
      <path d="m7 17-2 4 7-4m-5-7h10" />
    </>
  ),
  activity: <path d="M3 12h4l3-8 4 16 3-8h4" />,
  settings: (
    <>
      <path d="M4 6h16M4 12h16M4 18h16" />
      <circle cx="8" cy="6" r="2" />
      <circle cx="16" cy="12" r="2" />
      <circle cx="10" cy="18" r="2" />
    </>
  ),
  users: (
    <>
      <circle cx="9" cy="8" r="3" />
      <path d="M3 21v-3a6 6 0 0 1 12 0v3m1-16a3 3 0 0 1 0 6m2 4a5 5 0 0 1 3 5" />
    </>
  ),
  arrow: <path d="M4 12h16m-6-6 6 6-6 6" />,
  back: <path d="M20 12H4m6-6-6 6 6 6" />,
  plus: <path d="M12 5v14M5 12h14" />,
  send: (
    <>
      <path d="m3 3 19 9-19 9 4-9-4-9Zm4 9h15" />
    </>
  ),
  refresh: (
    <>
      <path d="M20 7v5h-5M4 17v-5h5M5 8a8 8 0 0 1 14-2l1 1M4 17l1 1a8 8 0 0 0 14-2" />
    </>
  ),
  check: <path d="m5 12 4 4L19 6" />,
  code: (
    <>
      <path d="m8 7-5 5 5 5m8-10 5 5-5 5M14 4l-4 16" />
    </>
  ),
};
export function Icon({ name, size = 18 }: { name: string; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.65"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {paths[name] || paths.agent}
    </svg>
  );
}
export function Brand({ small = false }: { small?: boolean }) {
  return (
    <div className={"brand " + (small ? "small" : "")}>
      <span className="brand-mark">
        <Icon name="layers" size={23} />
      </span>
      <span>
        Agent<span className="brand-light"> Studio</span>
        <small>tRPC-Agent-Go</small>
      </span>
    </div>
  );
}
export function PageHeading({
  eyebrow,
  title,
  subtitle,
  action,
}: {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  action?: ReactNode;
}) {
  return (
    <div className="page-heading">
      <div>
        {eyebrow && <div className="eyebrow">{eyebrow}</div>}
        <h1>{title}</h1>
        {subtitle && <p>{subtitle}</p>}
      </div>
      <div>{action}</div>
    </div>
  );
}
const labels: Record<string, string> = {
  configured: "已配置 · 待验证",
  active: "已启用",
  disabled: "已停用",
  suspended: "已暂停",
  queued: "排队中",
  running: "执行中",
  succeeded: "成功",
  completed: "已完成",
  completed_with_issues: "处理结束 · 有限制",
  expired: "已过期 · 未执行",
  dead: "已停止重试",
  failed: "失败",
  awaiting_approval: "等待审批",
  cancel_requested: "正在取消",
  cancelled: "已取消",
  unknown: "待核对",
  ready: "就绪",
  unavailable: "不可用",
  draft: "草稿",
  published: "已发布",
};
export function Status({ value }: { value?: string }) {
  const color = [
    "active",
    "succeeded",
    "completed",
    "ready",
    "published",
  ].includes(value || "")
    ? "success"
    : ["failed", "unavailable", "dead"].includes(value || "")
      ? "error"
      : ["running", "queued"].includes(value || "")
        ? "processing"
        : [
              "unknown",
              "awaiting_approval",
              "cancel_requested",
              "completed_with_issues",
            ].includes(value || "")
          ? "warning"
          : "default";
  return <Tag color={color}>{labels[value || ""] || value || "未发布"}</Tag>;
}
export function Failure({
  error,
  retry,
}: {
  error: string;
  retry?: () => void;
}) {
  return (
    <Alert
      type="error"
      showIcon
      title={error}
      action={
        retry && (
          <Button size="small" onClick={retry}>
            重新加载
          </Button>
        )
      }
    />
  );
}
export function Blank({
  title,
  children,
}: {
  title: string;
  children?: ReactNode;
}) {
  return (
    <div className="blank">
      <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={title}>
        {children}
      </Empty>
    </div>
  );
}
export function Panel({
  title,
  subtitle,
  children,
  action,
  style,
}: {
  title: string;
  subtitle?: string;
  children: ReactNode;
  action?: ReactNode;
  style?: CSSProperties;
}) {
  return (
    <section className="panel" style={style}>
      <div className="panel-heading">
        <div>
          <h3>{title}</h3>
          {subtitle && <p>{subtitle}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}
export function ValidationView({ report }: { report: Validation }) {
  return (
    <div className="validation">
      <Alert
        type={report.valid ? "success" : "error"}
        showIcon
        title={report.valid ? "配置检查通过" : "请先修正配置"}
        description={
          report.runtime_status === "ready"
            ? "已收到执行节点的就绪观测。"
            : "配置检查不调用模型。执行依赖尚未完全验证，不等于真实调用已经成功。"
        }
      />
      {report.issues.map((issue, i) => (
        <div className={"validation-issue " + issue.severity} key={i}>
          <Tag color={issue.severity === "error" ? "error" : "warning"}>
            {issue.severity === "error" ? "需修正" : "提示"}
          </Tag>
          <div>
            <strong>{issue.message}</strong>
            <p>{issue.suggestion}</p>
            <code>
              {issue.field} · {issue.code}
            </code>
          </div>
        </div>
      ))}
    </div>
  );
}
