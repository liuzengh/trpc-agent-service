import { SiteLink } from "../../components/site/site-link";
import type { Metadata } from "next";
import { ArrowRight, BookOpen, CircleHelp, Terminal } from "lucide-react";
import { DocCards } from "../../components/site/doc-cards";
import { guideUrl, SiteShell } from "../../components/site/site-shell";
import styles from "../../components/site/site.module.css";
export const metadata: Metadata = { title: "文档中心 · Agent tRPC", description: "Agent tRPC V1 使用教程与参考文档：创建 Agent、配置资源、发布部署与接入机器人。" };
export default function DocsPage() {
  return <SiteShell active="docs"><main id="main-content">
    <section className={styles.docsHero}><span className={styles.eyebrow}>THE FIELD GUIDE / V1</span><h1>少一点摸索，<br /><span>多一点开始。</span></h1><p>先跟着教程走通一遍，再用参考文档理解每一个选择。</p><SiteLink className={styles.primaryButton} href={guideUrl}>打开完整图文教程 <ArrowRight size={17} /></SiteLink></section>
    <section className={styles.section} aria-labelledby="reading-paths"><div className={styles.sectionHeading}><div><span className={styles.eyebrow}>CHOOSE YOUR PATH</span><h2 id="reading-paths">你现在想做什么？</h2></div></div><div className={styles.readingPaths}>
      <SiteLink href={`${guideUrl}#chapter-4`}><BookOpen size={23} /><h3>部署第一个 Agent</h3><p>已有平台账号，从创建助手开始。</p><span>跟着操作 <ArrowRight size={16} /></span></SiteLink>
      <SiteLink href={`${guideUrl}#chapter-13`}><Terminal size={23} /><h3>安装自己的平台</h3><p>准备环境、启动服务并检查安装结果。</p><span>安装说明 <ArrowRight size={16} /></span></SiteLink>
      <SiteLink href={`${guideUrl}#chapter-11`}><CircleHelp size={23} /><h3>解决使用中的问题</h3><p>定位登录、配置、预检与收发消息问题。</p><span>排查问题 <ArrowRight size={16} /></span></SiteLink>
    </div></section>
    <section className={styles.section} aria-labelledby="reference-docs"><div className={styles.sectionHeading}><div><span className={styles.eyebrow}>REFERENCE LIBRARY</span><h2 id="reference-docs">按主题查阅</h2></div><p>操作步骤之外，也把规则讲清楚。</p></div><DocCards /></section>
    <aside className={styles.versionNote}><span>版本说明</span><p>文档面向 V1 使用者。首个可运行方案采用单 LLM、模型与 session 会话存储；工具、知识调用与组合节点不属于当前 Worker V1 执行路径。接入预检通过，也仍需用真实消息确认回复。</p><SiteLink href={`${guideUrl}#chapter-14`}>查看版本与插图说明 <ArrowRight size={16} /></SiteLink></aside>
  </main></SiteShell>;
}
