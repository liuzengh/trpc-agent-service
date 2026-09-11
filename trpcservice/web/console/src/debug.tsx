import { useEffect, useRef, useState } from "react";
import { Alert, App, Button, Input, Space, Tag } from "antd";
import { api, errorText, streamState } from "./api";
import { Icon, Status } from "./components";
import {
  date,
  operable,
  writable,
  type Dict,
  type Draft,
  type Principal,
  type Stored,
} from "./types";

const working = (run?: Stored) =>
  !!run && ["queued", "running", "cancel_requested"].includes(run.status);
export function DebugPanel({
  tenant,
  appID,
  principal,
  draft,
  dirty,
  onSave,
  onError,
}: {
  tenant: string;
  appID: string;
  principal: Principal;
  draft: Stored<Draft>;
  dirty: boolean;
  onSave: () => Promise<Stored<Draft>>;
  onError: (e: unknown) => void;
}) {
  const { message } = App.useApp();
  const [session, setSession] = useState<Stored | null>(null);
  const [state, setState] = useState<Dict | null>(null);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const bottom = useRef<HTMLDivElement>(null);
  const generation = useRef(0);
  const current = state?.current_run as Stored | undefined;
  const pending = current?.status === "awaiting_approval";
  useEffect(() => {
    if (!operable(principal)) return;
    let live = true;
    api<{ session: Stored | null }>("debug/latest", {
      tenant_id: tenant,
      app_id: appID,
    })
      .then((data) => {
        if (live && data.session) setSession(data.session);
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant, appID, principal.name]);
  useEffect(() => {
    bottom.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [state?.runs?.length, current?.status]);
  useEffect(() => {
    if (!session) return;
    let live = true;
    let timer: ReturnType<typeof setTimeout>;
    const controller = new AbortController();
    let cursor = "";
    let terminal = false;
    const gen = ++generation.current;
    async function poll() {
      try {
        const result = await api<Dict>("debug/get", {
          tenant_id: tenant,
          session_id: session!.id,
        });
        if (live && generation.current === gen) {
          setState(result);
          setError("");
        }
      } catch (e) {
        if (live) setError(errorText(e));
      } finally {
        if (live) void watch();
      }
    }
    async function watch() {
      try {
        await streamState(
          { tenant_id: tenant, session_id: session!.id, cursor },
          controller.signal,
          (result, id) => {
            if (!live || generation.current !== gen) return;
            cursor = id;
            terminal =
              !result.current_run ||
              ![
                "queued",
                "running",
                "cancel_requested",
                "awaiting_approval",
              ].includes(result.current_run.status);
            setState((old) => {
              const runs: Stored[] = old?.runs || [];
              const current = result.current_run as Stored | undefined;
              return {
                ...old,
                ...result,
                runs: current
                  ? runs.some((r) => r.id === current.id)
                    ? runs.map((r) => (r.id === current.id ? current : r))
                    : [...runs, current]
                  : runs,
              };
            });
            setError("");
          },
        );
      } catch (e) {
        if (live && !controller.signal.aborted) setError(errorText(e));
      } finally {
        if (live && !terminal) timer = setTimeout(watch, 1000);
      }
    }
    void poll();
    return () => {
      live = false;
      controller.abort();
      clearTimeout(timer);
    };
  }, [session?.id, tenant, current?.id]);
  async function start() {
    setBusy(true);
    setError("");
    try {
      const saved =
        dirty || (draft.version === 0 && writable(principal))
          ? await onSave()
          : draft;
      const result = await api<{ session: Stored; disabled_tools: string[] }>(
        "debug/sessions",
        {
          tenant_id: tenant,
          app_id: appID,
          draft_version: saved.version,
          previous_session_id: session?.id || "",
        },
      );
      setSession(result.session);
      setState({ runs: [], disabled_tools: result.disabled_tools });
      message.success("已创建独立调试会话，使用保存的草稿快照");
      return result.session;
    } catch (e) {
      onError(e);
      setError(errorText(e));
      throw e;
    } finally {
      setBusy(false);
    }
  }
  async function send() {
    if (!input.trim() || busy || working(current) || pending) return;
    setBusy(true);
    setError("");
    try {
      const target = session || (await start());
      setBusy(true);
      const result = await api<{ run: Stored }>("debug/send", {
        tenant_id: tenant,
        session_id: target.id,
        message: input,
        message_id: crypto.randomUUID(),
      });
      setInput("");
      setState((old) => ({
        ...old,
        current_run: result.run,
        runs: [...(old?.runs || []), result.run],
      }));
    } catch (e) {
      onError(e);
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  async function decide(id: string, decision: string) {
    if (!session) return;
    setBusy(true);
    try {
      await api("debug/decision", {
        tenant_id: tenant,
        session_id: session.id,
        approval_id: id,
        decision,
      });
      setState(
        await api<Dict>("debug/get", {
          tenant_id: tenant,
          session_id: session.id,
        }),
      );
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  async function cancel() {
    if (!session || !current) return;
    setBusy(true);
    try {
      await api("debug/cancel", {
        tenant_id: tenant,
        session_id: session.id,
        run_id: current.id,
      });
      message.info("已请求取消；实际执行结果以运行记录为准");
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  const runs: Stored[] = state?.runs || [];
  return (
    <aside className="debug-panel">
      <div className="debug-heading">
        <div>
          <strong>
            <Icon name="channel" /> 调试对话
          </strong>
          <span>独立会话 · 不发送到 IM</span>
        </div>
        <Button
          size="small"
          icon={<Icon name="plus" />}
          disabled={!operable(principal) || busy || working(current) || pending}
          onClick={() => {
            void start().catch(() => {});
          }}
        >
          新调试
        </Button>
      </div>
      <div className="debug-meta">
        <Tag color="purple">{session ? "草稿快照已固定" : "尚未开始"}</Tag>
        {current && <Status value={current.status} />}
        <span>保存不会自动发布</span>
      </div>
      {dirty && session && (
        <div className="debug-alert">
          <Alert
            type="warning"
            title="编辑器有新修改，当前调试仍使用原快照"
            description="点击“新调试”保存并使用新配置。"
          />
        </div>
      )}
      {state?.disabled_tools?.length > 0 && (
        <div className="debug-alert">
          <Alert
            type="info"
            title="外部写入工具在网页调试中关闭"
            description={state?.disabled_tools?.join("、")}
          />
        </div>
      )}
      {error && (
        <div className="debug-alert">
          <Alert type="error" showIcon title={error} />
        </div>
      )}
      <div className="chat-scroll">
        {!runs.length ? (
          <div className="chat-welcome">
            <span className="chat-welcome-icon">
              <Icon name="agent" size={33} />
            </span>
            <h3>在发布之前，先聊一聊。</h3>
            <p>
              发送一条消息，验证模型、指令和工具行为。
              <br />
              需要执行 Skill 时，会在这里请求你的批准。
            </p>
            <div className="suggestions">
              {["你好，请介绍你的职责。", "请说明你当前可以使用哪些工具。"].map(
                (text) => (
                  <button key={text} onClick={() => setInput(text)}>
                    {text}
                    <Icon name="arrow" size={14} />
                  </button>
                ),
              )}
            </div>
            <span className="field-note">
              使用真实模型可能产生费用；身份、记忆与业务会话隔离。
            </span>
          </div>
        ) : (
          runs.map((run) => (
            <div key={run.id} className="chat-turn">
              {!run.data.continuation_of && (
                <div className="chat-message user">
                  <div className="message-avatar">
                    {principal.name.slice(0, 1).toUpperCase()}
                  </div>
                  <div>
                    <div className="message-label">
                      你 <span>{date(run.created_at)}</span>
                    </div>
                    <div className="message-content">{run.data.input}</div>
                  </div>
                </div>
              )}
              <div className="chat-message assistant">
                <div className="message-avatar">
                  <Icon name="agent" size={18} />
                </div>
                <div>
                  <div className="message-label">
                    Agent <Status value={run.status} />
                  </div>
                  {run.data.reply ? (
                    <div className="message-content">{run.data.reply}</div>
                  ) : working(run) ? (
                    <div className="typing">
                      <i />
                      <i />
                      <i />
                      <span>
                        {run.status === "queued"
                          ? "等待执行节点接收…"
                          : "正在执行，请稍候…"}
                      </span>
                    </div>
                  ) : run.status === "awaiting_approval" ? (
                    <span className="muted">需要你的批准后才能继续执行。</span>
                  ) : null}
                  {run.data.error_type && (
                    <div className="run-error">
                      <Tag color="error">{run.data.error_type}</Tag>
                      <p>失败或取消不代表此前工具操作已经回滚。</p>
                    </div>
                  )}
                  <small className="run-reference">{run.id}</small>
                </div>
              </div>
            </div>
          ))
        )}
        {(state?.tools || []).length > 0 && (
          <div className="live-tools">
            <div className="eyebrow">当前请求 · 工具执行</div>
            {state?.tools?.map((tool: Dict) => (
              <div className="tool-event" key={tool.execution_id}>
                <Icon name="code" />
                <div>
                  <strong>{tool.tool_name}</strong>
                  {tool.error_type && <small>{tool.error_type}</small>}
                </div>
                <Status value={tool.status} />
              </div>
            ))}
          </div>
        )}
        {(state?.approvals || []).map((approval: Dict) => (
          <div className="approval-card" key={approval.approval_id}>
            <Tag color="orange">等待你的批准</Tag>
            <h4>执行 {approval.tool_name}</h4>
            <p>批准仅适用于本次工具和固定参数，不会授予后续调用权限。</p>
            <div className="approval-hash">
              参数指纹：{approval.arguments_hash?.slice(0, 16)}…
            </div>
            <Space>
              <Button
                type="primary"
                disabled={busy}
                onClick={() => decide(approval.approval_id, "approved")}
              >
                批准执行
              </Button>
              <Button
                disabled={busy}
                onClick={() => decide(approval.approval_id, "denied")}
              >
                拒绝
              </Button>
            </Space>
          </div>
        ))}
        <div ref={bottom} />
      </div>
      <div className="composer">
        <Input.TextArea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          autoSize={{ minRows: 2, maxRows: 6 }}
          maxLength={6000}
          disabled={!operable(principal) || pending}
          placeholder={
            !operable(principal)
              ? "当前账号没有调试执行权限"
              : pending
                ? "请先批准或拒绝当前工具请求"
                : "发送消息，开始调试…"
          }
          onKeyDown={(e) => {
            if (
              e.key === "Enter" &&
              !e.shiftKey &&
              !e.nativeEvent.isComposing
            ) {
              e.preventDefault();
              void send();
            }
          }}
        />
        <div className="composer-footer">
          <span>Enter 发送 · Shift + Enter 换行</span>
          {working(current) ? (
            <Button danger size="small" loading={busy} onClick={cancel}>
              停止执行
            </Button>
          ) : (
            <Button
              type="primary"
              icon={<Icon name="send" size={16} />}
              loading={busy}
              disabled={!input.trim() || !operable(principal) || pending}
              onClick={send}
            >
              发送
            </Button>
          )}
        </div>
      </div>
    </aside>
  );
}
