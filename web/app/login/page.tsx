"use client";

import { ArrowRight, BookOpen, Bot, Hexagon, Layers3, Radio, ShieldCheck, Wrench } from "lucide-react";
import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, Suspense, useState } from "react";
import { HelpLink } from "../../components/help-link";
import { Button, Field } from "../../components/ui";
import { controlApi, ControlApiError } from "../../lib/control-api";
import styles from "./login.module.css";

export default function LoginPage() {
  return <Suspense><LoginForm /></Suspense>;
}

function LoginForm() {
  const router = useRouter();
  const search = useSearchParams();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault(); setError(""); setSubmitting(true);
    try {
      const session = await controlApi.login({ username, password });
      if (session.password_change_required) router.replace("/change-password");
      else {
        const requested = search.get("next");
        router.replace(requested?.startsWith("/") && !requested.startsWith("//") ? requested : "/console");
      }
    } catch (caught) {
      setError(caught instanceof ControlApiError && caught.code === "INVALID_CREDENTIALS" ? "用户名或密码错误" : caught instanceof Error ? caught.message : "登录失败");
    } finally { setSubmitting(false); }
  }
  return (
    <main className={styles.page}>
      <div className={styles.frame}>
        <section className={styles.story} aria-label="Agent tRPC 平台介绍">
          <div className={styles.brand}><Hexagon size={30} strokeWidth={2.2} aria-hidden="true" />Agent tRPC<span>CONTROL</span></div>
          <div className={styles.introduction}>
            <span className={styles.eyebrow}>从配置，到对话</span>
            <h2>让你的 Agent，<br />在这里开始。</h2>
            <p>连接模型与资源，管理部署和渠道。<br />一个工作区，让每一步清晰有序。</p>
          </div>
          <div className={styles.journey}>
            <div className={styles.journeyTitle}><Bot size={20} aria-hidden="true" /><strong>你的 Agent 工作区</strong><span>01 — 03</span></div>
            <ol>
              <li><span className={styles.step}>01</span><div><strong>定义 Agent</strong><small>从逻辑槽位与行为开始</small></div></li>
              <li><span className={styles.step}>02</span><div><strong>配置运行资源</strong><div className={styles.resources}><span data-resource-category="models"><Layers3 size={13} aria-hidden="true" />Models</span><span data-resource-category="tools"><Wrench size={13} aria-hidden="true" />Tools</span><span data-resource-category="knowledge"><BookOpen size={13} aria-hidden="true" />Knowledge</span></div></div></li>
              <li><span className={styles.step}>03</span><div><strong>部署与渠道接入</strong><small>发布版本，连接你的对话入口</small></div><Radio size={18} aria-hidden="true" /></li>
            </ol>
          </div>
          <p className={styles.storyFoot}>构建有序，让对话自然发生。</p>
        </section>
        <section className={styles.access} aria-label="账号登录">
          <div className={styles.accessHeading}><span>管理控制台</span><HelpLink /></div>
          <form className={styles.form} onSubmit={(event) => void submit(event)} aria-busy={submitting}>
            <span className={styles.formEyebrow}>WELCOME BACK</span>
            <h1>登录控制台</h1>
            <p>欢迎回来，继续管理你的 Agent 工作区。</p>
            {error && <div className="api-notice" role="alert">{error}</div>}
            <div className="form-stack">
              <Field autoComplete="username" label="用户名" onChange={(e) => setUsername(e.target.value)} placeholder="请输入用户名" required value={username} />
              <Field autoComplete="current-password" label="密码" onChange={(e) => setPassword(e.target.value)} placeholder="请输入密码" required type="password" value={password} />
              <Button disabled={submitting} type="submit">{submitting ? "正在登录…" : "登录"}<ArrowRight size={17} aria-hidden="true" /></Button>
            </div>
            <p className={styles.accountHint}>使用平台账号登录。需要账号？请联系平台管理员。</p>
          </form>
          <p className={styles.sessionNote}><ShieldCheck size={15} aria-hidden="true" />Session 由 HttpOnly Cookie 安全保存</p>
        </section>
      </div>
      <footer className={styles.footer}><span>Agent tRPC</span><span>定义 · 配置 · 部署 · 对话</span></footer>
    </main>
  );
}
