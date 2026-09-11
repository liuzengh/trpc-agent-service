import { SiteLink } from "../components/site/site-link";
import type { Metadata } from "next";
import { ArrowDown, ArrowRight, ArrowUpRight, BookOpen, Bot, Check, CheckCheck, CircleCheck, GitBranch, Layers3, MessageCircle, Radio, Rocket, Send, Settings2, ShieldCheck, Sparkles, Terminal, Workflow } from "lucide-react";
import { DocCards } from "../components/site/doc-cards";
import { guideUrl, SiteShell } from "../components/site/site-shell";
import styles from "../components/site/site.module.css";
export const metadata: Metadata = { title: "Agent tRPC · 从配置到对话", description: "定义你的 Agent，连接模型与会话资源，发布确定的部署版本，再接入 Telegram 或企业微信。教程、文档与控制台，从这里开始。" };
function WorkflowPreview() {
  return <div className={styles.workflowPreview} aria-label="从 Agent 和运行配置，到部署与渠道对话的流程示意">
    <div className={styles.previewHeading}><span><span className={styles.liveDot} />从配置到对话</span><span>流程示意 · 非运行状态</span></div>
    <div className={styles.sourceNodes}>
      <div className={styles.flowNode}><span className={styles.nodeIcon}><Bot size={23} /></span><div><small>01 / DEFINE</small><strong>你的 Agent</strong></div><span className={styles.nodeVersion}>v1</span><p>角色与指令，定义工作方式。</p></div>
      <div className={styles.flowNode}><span className={styles.nodeIconAlt}><Settings2 size={23} /></span><div><small>02 / CONNECT</small><strong>运行配置</strong></div><span className={styles.nodeVersion}>r1</span><p>模型与会话，连接真实资源。</p></div>
    </div>
    <div className={styles.connector} aria-hidden="true"><span /><ArrowDown size={18} /></div>
    <div className={styles.deployNode}><span className={styles.deployIcon}><Layers3 size={25} /></span><div><small>03 / PUBLISH</small><strong>一份确定的部署</strong><p>Agent v1 + Profile r1</p></div><span className={styles.immutable}><ShieldCheck size={14} />固定版本</span></div>
    <div className={styles.singleConnector} aria-hidden="true"><ArrowDown size={18} /></div>
    <div className={styles.conversation}><div className={styles.conversationHeading}><span><Send size={17} />渠道对话</span><span>04 / GO LIVE</span></div><div className={styles.userBubble}>你好，介绍一下自己吧。</div><div className={styles.botReply}><span className={styles.avatar}><Bot size={19} /></span><div><strong>我的第一个助手</strong><p>你好！我已经准备好了。<br />我们从你的第一个问题开始。</p><span>示例回复 <CheckCheck size={13} /></span></div></div></div>
    <div className={styles.previewFooter}><GitBranch size={14} /> 每次发布有版本，每次切换有目标。</div>
  </div>;
}
export default function Home() {
  return <SiteShell active="home"><main id="main-content">
    <section className={styles.hero} aria-labelledby="hero-title"><div className={styles.heroCopy}><SiteLink className={styles.releaseBadge} href={`${guideUrl}#chapter-1`}><span>V1</span> 从搭建到接入，一条完整路径 <ArrowUpRight size={14} /></SiteLink><h1 id="hero-title">让你的 Agent，<br /><span>真正开始工作。</span></h1><p className={styles.heroDescription}>定义它的角色，连接你的模型。<br />把经过校验的版本，带到用户的下一次对话里。</p><div className={styles.heroActions}><SiteLink className={styles.primaryButton} href={`${guideUrl}#chapter-4`}>开始搭建我的 Agent <ArrowRight size={17} /></SiteLink><SiteLink className={styles.secondaryButton} href="/docs"><BookOpen size={18} />阅读文档</SiteLink></div><div className={styles.heroFootnote}><span><Check size={14} />可视化配置</span><span><Check size={14} />版本化发布</span><span><Check size={14} />真实渠道接入</span></div><SiteLink className={styles.scrollCue} href="#start-here"><span>从这里开始探索</span><ArrowDown size={15} /></SiteLink></div><WorkflowPreview /></section>
    <div className={styles.capabilityStrip}><span>为完整的使用流程而设计</span><div><Workflow size={18} />Agent 工作台</div><div><Settings2 size={18} />运行配置</div><div><Layers3 size={18} />部署管理</div><div><Radio size={18} />Telegram / 企业微信</div></div>
    <section className={styles.section} id="start-here" aria-labelledby="tutorial-title"><div className={styles.sectionHeading}><div><span className={styles.eyebrow}>FROM ZERO TO YOUR FIRST REPLY</span><h2 id="tutorial-title">不用猜下一步，<br className={styles.mobileBreak} />跟着做就好。</h2></div><SiteLink className={styles.textLink} href={guideUrl}>全部使用教程 <ArrowRight size={16} /></SiteLink></div><div className={styles.tutorialGrid}>
      <SiteLink className={styles.featuredTutorial} href={`${guideUrl}#chapter-4`}><span className={styles.tutorialLabel}><BookOpen size={16} />新手完整教程</span><h3>部署你的<br />第一个 Agent</h3><p>从一个 Single LLM 助手开始，<br />直到机器人里出现第一条真实回复。</p><div className={styles.miniSteps}><span><Bot size={19} />创建</span><ArrowRight size={16} /><span><Settings2 size={19} />配置</span><ArrowRight size={16} /><span><Rocket size={19} />发布</span><ArrowRight size={16} /><span><MessageCircle size={19} />对话</span></div><div className={styles.tutorialBottom}><span>图文指南 · 逐步操作</span><span className={styles.roundArrow}><ArrowUpRight size={23} /></span></div></SiteLink>
      <div className={styles.sideTutorials}><SiteLink className={styles.smallTutorial} href={`${guideUrl}#chapter-13`}><span className={styles.tutorialIcon}><Terminal size={24} /></span><span className={styles.docTag}>安装与准备</span><h3>先拥有自己的平台</h3><p>环境准备、服务启动、首次登录。<br />从这里搭好工作的起点。</p><span className={styles.textLink}>阅读部署指南 <ArrowUpRight size={17} /></span></SiteLink><SiteLink className={styles.smallTutorial} href={`${guideUrl}#chapter-7`}><span className={styles.tutorialIcon}><Send size={24} /></span><span className={styles.docTag}>TELEGRAM 接入</span><h3>没有公网 IP，也能开始</h3><p>使用长轮询，完成接入预检，<br />再验证一次真正的收发。</p><span className={styles.textLink}>连接我的机器人 <ArrowUpRight size={17} /></span></SiteLink></div>
    </div></section>
    <section className={`${styles.section} ${styles.principles}`} aria-labelledby="principles-title"><div><span className={styles.eyebrow}>LESS GUESSWORK. MORE CONTROL.</span><h2 id="principles-title">上线不靠猜测，<br />每一步都有依据。</h2><p>把“保存好了”和“真正可用”分开。<br />让配置、发布与接入各自清晰。</p></div><div className={styles.principleList}>{[
      { icon: GitBranch, title: "发布一个版本，而不是一份变化中的草稿", text: "部署固定 Agent 与运行配置版本。升级时选择新版本，回退时也有明确目标。" },
      { icon: ShieldCheck, title: "先检查配置，再连接用户", text: "部署校验与渠道预检各司其职；最后用真实消息确认回复，而非只看绿色状态。" },
      { icon: CircleCheck, title: "接入和路由，分别掌握", text: "区分机器人是否接入、消息是否进入部署。维护与恢复时，清楚知道会影响什么。" },
    ].map(({ icon: Icon, title, text }, i) => <div key={title}><span><Icon size={23} /></span><div><small>0{i + 1}</small><h3>{title}</h3><p>{text}</p></div></div>)}</div></section>
    <section className={styles.section} aria-labelledby="docs-title"><div className={styles.sectionHeading}><div><span className={styles.eyebrow}>A PLACE FOR EVERY ANSWER</span><h2 id="docs-title">需要时，文档就在这里。</h2></div><SiteLink className={styles.textLink} href="/docs">进入文档中心 <ArrowRight size={16} /></SiteLink></div><DocCards /></section>
    <section className={styles.finalCta}><span className={styles.ctaSymbol}><Sparkles size={33} strokeWidth={1.3} /></span><div><h2>你的下一个助手，从这里开始。</h2><p>先完成一个简单、可用的 Agent，再逐步打磨它。</p></div><SiteLink className={styles.primaryButton} href={`${guideUrl}#chapter-13`}>开始部署 <ArrowRight size={17} /></SiteLink></section>
    <aside className={styles.scopeNote}>V1 执行路径：单 LLM + 模型 + session 会话存储 + 文本回复。工具、知识与组合节点的配置入口不代表当前 Worker 已支持执行。<SiteLink href={`${guideUrl}#chapter-1`}>了解当前版本 <ArrowUpRight size={13} /></SiteLink></aside>
  </main></SiteShell>;
}
