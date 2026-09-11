"use client";
import { useEffect, useId, useRef, type ReactNode } from "react";
import { Button } from "../ui";
import styles from "./channel.module.css";
export function ChannelDialog({ title, children, busy, disabled, confirmLabel, onConfirm, onClose }: { title: string; children: ReactNode; busy: boolean; disabled?: boolean; confirmLabel: string; onConfirm: () => void; onClose: () => void }) {
  const ref = useRef<HTMLDivElement>(null); const titleId = useId();
  const latest = useRef({ busy, onClose }); latest.current = { busy, onClose };
  useEffect(() => {
    const prior = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    ref.current?.focus();
    function keys(event: KeyboardEvent) {
      if (event.key === "Escape" && !latest.current.busy) { event.preventDefault(); latest.current.onClose(); }
      if (event.key !== "Tab") return;
      const nodes = [...(ref.current?.querySelectorAll<HTMLElement>('button:not([disabled]),a[href],input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex="0"]') ?? [])];
      const first = nodes[0], last = nodes.at(-1);
      if (!first) { event.preventDefault(); return; }
      if (event.shiftKey && (document.activeElement === first || document.activeElement === ref.current)) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && (document.activeElement === last || document.activeElement === ref.current)) { event.preventDefault(); first.focus(); }
    }
    document.addEventListener("keydown", keys);
    return () => { document.removeEventListener("keydown", keys); if (prior?.isConnected) prior.focus(); };
  }, []);
  return <div className={styles.overlay}><div ref={ref} className={styles.dialog} role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1}><h2 id={titleId}>{title}</h2>{children}<div className={styles.actions}><Button variant="secondary" disabled={busy} onClick={onClose}>取消</Button><Button disabled={busy || disabled} onClick={onConfirm}>{busy ? "正在提交…" : confirmLabel}</Button></div></div></div>;
}
