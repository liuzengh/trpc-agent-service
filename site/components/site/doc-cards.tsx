import { SiteLink } from "./site-link";
import { ArrowUpRight, Bot, Layers3, MessagesSquare, Rocket, Settings2, Users } from "lucide-react";
import { docTopics, referenceUrl } from "./doc-topics";
import styles from "./site.module.css";
const icons = { layers: Layers3, agent: Bot, settings: Settings2, deploy: Rocket, channel: MessagesSquare, users: Users };
export function DocCards() {
  return <div className={styles.docGrid}>{docTopics.map((topic) => {
    const Icon = icons[topic.icon];
    return <SiteLink className={styles.docCard} href={referenceUrl(topic.slug)} key={topic.slug}><div className={styles.docCardTop}><span className={styles.docIcon}><Icon size={22} strokeWidth={1.6} /></span><ArrowUpRight size={19} /></div><span className={styles.docTag}>{topic.tag}</span><h3>{topic.title}</h3><p>{topic.description}</p><span className={styles.docNumber}>DOC / {topic.number}</span></SiteLink>;
  })}</div>;
}
