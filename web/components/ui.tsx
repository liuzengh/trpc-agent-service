import { X } from "lucide-react";
import { useId } from "react";
import type { ButtonHTMLAttributes, InputHTMLAttributes, ReactNode } from "react";

/** Short inline hint. Long prose belongs here, not in the form flow. */
export function Tooltip({ label, children }: { label: string; children: ReactNode }) {
  const id = useId();
  return <span className="tooltip"><button type="button" className="tooltip-trigger" aria-label={label} aria-describedby={id}>?</button><span id={id} role="tooltip" className="tooltip-body">{children}</span></span>;
}

export function PageHeader({ eyebrow, title, description, action }: { eyebrow?: string; title: string; description: string; action?: ReactNode }) {
  return <div className="page-header"><div>{eyebrow && <span className="eyebrow">{eyebrow}</span>}<h1>{title}</h1><p>{description}</p></div>{action}</div>;
}
export function Button({ variant = "primary", className = "", ...props }: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "secondary" | "danger" | "ghost" }) {
  return <button className={`button ${variant} ${className}`.trim()} {...props} />;
}
export function Field({ label, hint, error, ...props }: InputHTMLAttributes<HTMLInputElement> & { label: string; hint?: string; error?: string }) {
  return <label className="field"><span>{label}</span><input {...props} />{hint && <small>{hint}</small>}{error && <small className="error-text">{error}</small>}</label>;
}
export function StatusBadge({ children, tone = "green" }: { children: ReactNode; tone?: "green" | "blue" | "amber" | "gray" }) {
  return <span className={`status-badge ${tone}`}>{children}</span>;
}
export function StatCard({ label, value, detail }: { label: string; value: string | number; detail: string }) {
  return <div className="stat-card"><span>{label}</span><strong>{value}</strong><small>{detail}</small></div>;
}
export function EmptyState({ title, detail }: { title: string; detail: string }) {
  return <div className="empty-state"><strong>{title}</strong><p>{detail}</p></div>;
}
export function Drawer({ title, description, onClose, children }: { title: string; description?: string; onClose: () => void; children: ReactNode }) {
  return <><button aria-label="关闭抽屉" className="drawer-backdrop" onClick={onClose} /><aside className="drawer" aria-label={title}><header><div><h2>{title}</h2>{description && <p>{description}</p>}</div><button aria-label="关闭" className="icon-button" onClick={onClose}><X size={18} /></button></header><div className="drawer-body">{children}</div></aside></>;
}
export function ApiNotice({ error }: { error: unknown }) {
  if (!error) return null;
  return <div className="api-notice" role="alert">{error instanceof Error ? error.message : "请求失败"}</div>;
}
