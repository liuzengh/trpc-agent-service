"use client";

import { ArrowLeft, Bot } from "lucide-react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { type FormEvent, useState } from "react";

import { ApiNotice, Button, Field, PageHeader } from "../../../../../components/ui";
import { controlApi } from "../../../../../lib/control-api";

export default function NewAgentPage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  const router = useRouter();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<unknown>();

  const tenantSegment = encodeURIComponent(tenantId);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(undefined);
    try {
      const created = await controlApi.createAgent(tenantId, {
        name: name.trim(),
        description: description.trim(),
      });
      router.push(`/tenants/${tenantSegment}/agents/${encodeURIComponent(created.agent.id)}`);
    } catch (caught) {
      setError(caught);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <>
      <PageHeader
        eyebrow="CREATE AGENT"
        title="创建 Agent"
        description="后端会原子创建 Agent 和 revision 1 的空 Draft；随后在画布中初始化 AgentSpec。"
        action={<Link className="button secondary" href={`/tenants/${tenantSegment}/agents`}><ArrowLeft size={15} />返回列表</Link>}
      />
      <div className="agent-create-layout">
        <form className="panel agent-create-form" onSubmit={(event) => void submit(event)}>
          <div className="panel-section-heading"><span className="feature-icon"><Bot size={18} /></span><div><h2>Agent 元数据</h2><p>这里只创建管理对象，不会伪造 Draft 或本地版本。</p></div></div>
          <ApiNotice error={error} />
          <div className="form-stack">
            <Field autoFocus label="名称" maxLength={128} onChange={(event) => setName(event.target.value)} placeholder="例如：研究助理" required value={name} />
            <label className="field">
              <span>描述</span>
              <textarea maxLength={4096} onChange={(event) => setDescription(event.target.value)} placeholder="说明 Agent 的职责和使用场景" rows={6} value={description} />
              <small>{description.length}/4096</small>
            </label>
            <div className="form-actions">
              <Link className="button secondary" href={`/tenants/${tenantSegment}/agents`}>取消</Link>
              <Button disabled={submitting || name.trim().length === 0} type="submit">{submitting ? "正在创建…" : "创建并打开画布"}</Button>
            </div>
          </div>
        </form>
        <aside className="panel agent-create-note">
          <h3>创建后的真实状态</h3>
          <ol>
            <li>Control API 返回 Agent 与 revision 1 Draft。</li>
            <li>空 <code>{"{}"}</code> Draft 会显示模板初始化入口。</li>
            <li>模板只更新浏览器工作副本，点击保存后才写入服务端。</li>
          </ol>
        </aside>
      </div>
    </>
  );
}
