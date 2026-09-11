import { SiteLink } from "./site-link";
import { ArrowRight, Hexagon } from "lucide-react";
import type { ReactNode } from "react";
import styles from "./site.module.css";

export const guideUrl = "/docs/guide.html";
export function SiteShell({ children, active }: { children: ReactNode; active?: "home" | "docs" }) {
  return <div className={styles.site}>
    <SiteLink className={styles.skipLink} href="#main-content">跳到主要内容</SiteLink>
    <header className={styles.header}>
      <SiteLink className={styles.brand} href="/" aria-label="Agent tRPC 主页"><span className={styles.brandIcon}><Hexagon size={23} strokeWidth={2.5} /></span>Agent <strong>tRPC</strong><span className={styles.version}>V1</span></SiteLink>
      <nav className={styles.nav} aria-label="主导航"><SiteLink href="/" aria-current={active === "home" ? "page" : undefined}>概览</SiteLink><SiteLink href={guideUrl}>使用教程</SiteLink><SiteLink href="/docs" aria-current={active === "docs" ? "page" : undefined}>文档</SiteLink></nav>
      <SiteLink className={styles.navCta} href={`${guideUrl}#chapter-13`}>开始部署 <ArrowRight size={15} /></SiteLink>
    </header>{children}
    <footer className={styles.footer}><div><SiteLink className={styles.brand} href="/"><Hexagon size={21} />Agent <strong>tRPC</strong></SiteLink><p>从一个想法，到一次真实的对话。</p></div><nav aria-label="页脚导航"><SiteLink href={guideUrl}>使用教程</SiteLink><SiteLink href="/docs">文档中心</SiteLink><SiteLink href={`${guideUrl}#chapter-13`}>开始部署</SiteLink></nav><span>BUILD WITH INTENT. SHIP WITH CONFIDENCE.</span></footer>
  </div>;
}
