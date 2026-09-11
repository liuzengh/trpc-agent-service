"use client";

import { Hexagon } from "lucide-react";
import { useRouter } from "next/navigation";
import { FormEvent, useState } from "react";
import { Button, Field } from "../../components/ui";
import { controlApi, ControlApiError } from "../../lib/control-api";

export default function ChangePasswordPage() {
  const router = useRouter();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault(); setError("");
    if (next !== confirm) return setError("两次输入的新密码不一致");
    setSubmitting(true);
    try { await controlApi.changePassword({ current_password: current, new_password: next }); router.replace("/console"); }
    catch (caught) { setError(caught instanceof ControlApiError && caught.code === "WEAK_PASSWORD" ? "新密码不符合密码策略" : caught instanceof Error ? caught.message : "修改失败"); }
    finally { setSubmitting(false); }
  }
  return <main className="auth-page"><form className="auth-card" onSubmit={(event) => void submit(event)}><div className="auth-brand"><span className="brand-mark"><Hexagon size={18} /></span>Agent tRPC</div><h1>设置新密码</h1><p>临时密码只能用于首次登录，继续前必须设置新密码。</p>{error && <div className="api-notice" role="alert">{error}</div>}<div className="form-stack"><Field label="当前密码" onChange={(e) => setCurrent(e.target.value)} required type="password" value={current} /><Field hint="至少 12 位，建议包含大小写字母、数字和符号" label="新密码" onChange={(e) => setNext(e.target.value)} required type="password" value={next} /><Field label="确认新密码" onChange={(e) => setConfirm(e.target.value)} required type="password" value={confirm} /><Button disabled={submitting} type="submit">{submitting ? "正在保存…" : "保存并继续"}</Button></div></form></main>;
}
