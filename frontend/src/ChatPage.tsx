import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { MessageSquare, Send } from "lucide-react";
import { APIError, api, type AgentApp, type ChatEvent, type ChatStreamEvent, type Identity, type MockFaultConfiguration } from "./api";
import { AsyncState } from "./AsyncState";

type ChatMessageView = { key: string; role: "user" | "assistant"; text: string };
type ChatRunRecovery = { requestID: string; input: string; terminal?: "completed" | "failed" | "cancelled" };

function chatStorageKey(tenantID: string) {
  return `trpc-chat-session:${tenantID}`;
}

function newRequestID() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) return crypto.randomUUID();
  return `request-${Math.random().toString(16).slice(2)}`;
}

function eventPayload(event: ChatEvent) {
  try {
    return JSON.parse(event.payload) as Record<string, unknown>;
  } catch {
    try {
      const decoded = Uint8Array.from(atob(event.payload), (character) => character.charCodeAt(0));
      return JSON.parse(new TextDecoder().decode(decoded)) as Record<string, unknown>;
    } catch {
      return {};
    }
  }
}

function chatMessages(events: ChatEvent[]): ChatMessageView[] {
  const messages: ChatMessageView[] = [];
  for (const event of events) {
    const payload = eventPayload(event);
    if (event.type === "message.input") {
      messages.push({ key: event.id, role: "user", text: String(payload.input ?? "") });
    }
    if (event.type === "message.completed") {
      messages.push({ key: event.id, role: "assistant", text: String(payload.output ?? "") });
    }
  }
  return messages;
}

function latestChatRun(events: ChatEvent[]): ChatRunRecovery | undefined {
  let run: ChatRunRecovery | undefined;
  for (const event of events) {
    const payload = eventPayload(event);
    const requestID = String(payload.request_id ?? event.idempotency_key.split(":")[0] ?? "");
    if (event.type === "message.input") {
      run = { requestID, input: String(payload.input ?? "") };
    }
    if (!run || requestID !== run.requestID) continue;
    if (event.type === "run.completed") run.terminal = "completed";
    if (event.type === "run.failed") run.terminal = "failed";
    if (event.type === "run.cancelled") run.terminal = "cancelled";
  }
  return run;
}

export function ChatPage({ identity, initialAppID }: { identity: Identity; initialAppID?: string }) {
  const [apps, setApps] = useState<AgentApp[]>();
  const [appID, setAppID] = useState(initialAppID ?? "");
  const [sessionIDInput, setSessionIDInput] = useState("");
  const [sessionID, setSessionID] = useState("");
  const [events, setEvents] = useState<ChatEvent[]>();
  const [input, setInput] = useState("");
  const [error, setError] = useState<APIError>();
  const [loadFailed, setLoadFailed] = useState(false);
  const [sending, setSending] = useState(false);
  const [activeInput, setActiveInput] = useState("");
  const [activeRequestID, setActiveRequestID] = useState("");
  const [streamOutput, setStreamOutput] = useState("");
  const [streamState, setStreamState] = useState<"idle" | "streaming" | "completed" | "failed" | "cancelled" | "reconnecting">("idle");
  const [retryCandidate, setRetryCandidate] = useState<{ input: string; requestID: string; confirmed: boolean }>();
  const [faultScenario, setFaultScenario] = useState<MockFaultConfiguration["scenario"]>("none");
  const [faultConfig, setFaultConfig] = useState<MockFaultConfiguration>();
  const [deliveryError, setDeliveryError] = useState("");
  const appGeneration = useRef(0);
  const eventGeneration = useRef(0);
  const streamSource = useRef<EventSource | undefined>(undefined);
  const seenStreamEvents = useRef(new Set<string>());
  const lastSequence = useRef(0);
  const mutable = identity.active_role === "platform_admin" || identity.active_role === "tenant_admin" || identity.active_role === "operator";

  const loadApps = () => {
    const generation = ++appGeneration.current;
    setLoadFailed(false);
    api.apps().then(({ items }) => {
      if (appGeneration.current !== generation) return;
      setApps(items);
      setAppID((current) => current || items[0]?.id || "");
    }).catch(() => {
      if (appGeneration.current === generation) setLoadFailed(true);
    });
  };

  const loadEvents = async (targetSession: string, restoreActiveRun = false) => {
    const generation = ++eventGeneration.current;
    try {
      const result = await api.chatEvents(targetSession);
      if (eventGeneration.current === generation) {
        setEvents(result.items);
        lastSequence.current = result.items.at(-1)?.sequence ?? 0;
        setError(undefined);
        if (restoreActiveRun) {
          const run = latestChatRun(result.items);
          if (run) {
            setActiveInput(run.input);
            setActiveRequestID(run.requestID);
            setRetryCandidate({ input: run.input, requestID: run.requestID, confirmed: true });
            if (run.terminal) {
              setStreamState(run.terminal);
            } else {
              setStreamState("streaming");
              seenStreamEvents.current = new Set();
              openStream(targetSession, run.requestID, 0);
              if (mutable) {
                void api.sendChatMessage(targetSession, run.input, run.requestID).catch((caught) => {
                  setError(caught as APIError);
                });
              }
            }
          }
        }
      }
    } catch (caught) {
      if (eventGeneration.current === generation) setError(caught as APIError);
    }
  };

  const loadFaults = async (targetSession: string) => {
    try {
      setFaultConfig(await api.mockFaults(targetSession));
    } catch {
      setFaultConfig(undefined);
    }
  };

  useEffect(() => {
    setApps(undefined);
    setEvents(undefined);
    setSessionID("");
    setSessionIDInput("");
    closeStream();
    setActiveInput("");
    setActiveRequestID("");
    setStreamOutput("");
    setStreamState("idle");
    setFaultScenario("none");
    setFaultConfig(undefined);
    setDeliveryError("");
    seenStreamEvents.current = new Set();
    lastSequence.current = 0;
    setAppID(initialAppID ?? "");
    loadApps();
    const saved = localStorage.getItem(chatStorageKey(identity.active_tenant_id));
    if (saved) {
      setSessionID(saved);
      setSessionIDInput(saved);
      void loadEvents(saved, true);
      void loadFaults(saved);
    }
    return () => {
      closeStream();
      appGeneration.current++;
      eventGeneration.current++;
    };
  }, [identity.active_tenant_id, initialAppID]);

  const openSession = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!mutable || !appID || !sessionIDInput) return;
    try {
      const session = await api.createChatSession(appID, sessionIDInput);
      setSessionID(session.id);
      localStorage.setItem(chatStorageKey(identity.active_tenant_id), session.id);
      setEvents([]);
      setError(undefined);
      void loadFaults(session.id);
    } catch (caught) {
      const apiError = caught as APIError;
      if (apiError.status === 409) {
        setSessionID(sessionIDInput);
        localStorage.setItem(chatStorageKey(identity.active_tenant_id), sessionIDInput);
        await loadEvents(sessionIDInput, true);
        return;
      }
      setError(apiError);
    }
  };

  const closeStream = () => {
    streamSource.current?.close();
    streamSource.current = undefined;
  };

  const openStream = (targetSession: string, requestID: string, after = lastSequence.current) => {
    closeStream();
    setStreamState("streaming");
    const source = new EventSource(`/api/v1/chat/sessions/${encodeURIComponent(targetSession)}/stream?after=${after}&request_id=${encodeURIComponent(requestID)}`);
    streamSource.current = source;
    source.onmessage = (message: MessageEvent<string>) => {
      const envelope = JSON.parse(message.data) as ChatStreamEvent;
      if (seenStreamEvents.current.has(envelope.event_id)) return;
      seenStreamEvents.current.add(envelope.event_id);
      lastSequence.current = Math.max(lastSequence.current, envelope.sequence);
      if (envelope.type === "message.delta") {
        setStreamOutput(String(envelope.data.delta ?? ""));
      } else if (envelope.type === "message.completed") {
        setStreamOutput(String(envelope.data.output ?? ""));
      } else if (envelope.type === "channel.delivery" && envelope.data.status === "failed") {
        setDeliveryError(String(envelope.data.code ?? "channel_delivery_failed"));
        setStreamState("failed");
      } else if (envelope.type === "run.failed") {
        setStreamState("failed");
        closeStream();
        void loadEvents(targetSession);
      } else if (envelope.type === "run.cancelled") {
        setStreamState("cancelled");
        closeStream();
        void loadEvents(targetSession);
      } else if (envelope.type === "run.completed") {
        setStreamState((current) => (current === "failed" ? current : "completed"));
        closeStream();
        void loadEvents(targetSession);
      }
    };
    source.onerror = () => setStreamState("reconnecting");
  };

  const sendMessage = async (messageInput: string, requestID: string) => {
    if (!mutable || !sessionID || sending) return;
    setSending(true);
    try {
      await api.sendChatMessage(sessionID, messageInput, requestID);
      setActiveInput(messageInput);
      setActiveRequestID(requestID);
      setStreamOutput("");
      setDeliveryError("");
      seenStreamEvents.current = new Set();
      setRetryCandidate({ input: messageInput, requestID, confirmed: true });
      openStream(sessionID, requestID);
    } catch (caught) {
      setError(caught as APIError);
      setRetryCandidate((current) => current ?? { input: messageInput, requestID, confirmed: false });
    } finally {
      setSending(false);
    }
  };

  const send = () => {
    if (!input) return;
    const requestID = newRequestID();
    setRetryCandidate({ input, requestID, confirmed: false });
    void sendMessage(input, requestID);
    setInput("");
  };

  const retry = () => {
    if (!retryCandidate) return;
    let requestID = retryCandidate.requestID;
    if (retryCandidate.confirmed && (streamState === "failed" || streamState === "cancelled")) {
      requestID = newRequestID();
    }
    void sendMessage(retryCandidate.input, requestID);
  };

  const cancel = async () => {
    if (!sessionID || !activeRequestID) return;
    try {
      await api.cancelChatRun(sessionID, activeRequestID);
      setStreamState("cancelled");
    } catch (caught) {
      setError(caught as APIError);
    }
  };

  const applyFaults = async (scenario: MockFaultConfiguration["scenario"]) => {
    if (!mutable || !sessionID) return;
    try {
      setFaultConfig(await api.setMockFaults(sessionID, scenario));
      setFaultScenario(scenario);
      setError(undefined);
    } catch (caught) {
      setError(caught as APIError);
    }
  };

  const messages = useMemo(() => {
    const result = chatMessages(events ?? []);
    if (activeInput && !result.some((message) => message.role === "user" && message.text === activeInput)) {
      result.push({ key: activeRequestID + ":input", role: "user", text: activeInput });
    }
    if (streamOutput && !result.some((message) => message.role === "assistant" && message.text === streamOutput)) {
      result.push({ key: activeRequestID + ":output", role: "assistant", text: streamOutput });
    }
    return result;
  }, [events, activeInput, activeRequestID, streamOutput]);
  if (loadFailed) return <AsyncState kind="error" retry={loadApps} />;
  if (!apps) return <AsyncState kind="loading" />;

  return (
    <div className="chat-page">
      <form className="chat-toolbar" onSubmit={(event) => void openSession(event)}>
        <label>Agent 应用
          <select value={appID} onChange={(event) => setAppID(event.target.value)} disabled={!mutable}>
            {apps.map((app) => <option key={app.id} value={app.id}>{app.name}</option>)}
          </select>
        </label>
        <label>Session ID
          <input value={sessionIDInput} onChange={(event) => setSessionIDInput(event.target.value)} placeholder="chat-session" disabled={!mutable} />
        </label>
        <button className="primary" type="submit" disabled={!mutable || !appID || !sessionIDInput}>创建 / 打开</button>
      </form>
      <div className="chat-faults">
        <label>Mock 故障
          <select value={faultScenario} onChange={(event) => setFaultScenario(event.target.value as MockFaultConfiguration["scenario"])} disabled={!mutable || !sessionID}>
            <option value="none">无</option>
            <option value="timeout">超时</option>
            <option value="retry">重试耗尽</option>
            <option value="rate_limit">限流</option>
            <option value="message_length">消息超长</option>
            <option value="attachment">附件失败</option>
          </select>
        </label>
        <button type="button" onClick={() => void applyFaults(faultScenario)} disabled={!mutable || !sessionID}>设置故障</button>
        <button type="button" onClick={() => void applyFaults("none")} disabled={!mutable || !sessionID}>清除故障</button>
        <small>{faultConfig ? `当前：${faultConfig.scenario}` : "当前：未设置"}</small>
      </div>
      {error && <div className="inline-error" role="alert">{error.message}</div>}
      {deliveryError && <div className="inline-error" role="alert">Mock IM 投递失败：{deliveryError}</div>}
      {!sessionID ? <AsyncState kind="empty" /> : (
        <section className="chat-thread" aria-label="会话消息">
          {events === undefined ? <AsyncState kind="loading" /> : messages.length === 0 ? <AsyncState kind="empty" /> : (
            <ol className="chat-messages">
              {messages.map((message) => (
                <li key={message.key} className={message.role}>
                  <span>{message.role === "user" ? "用户" : "Agent"}</span>
                  <p>{message.text}</p>
                </li>
              ))}
            </ol>
          )}
        </section>
      )}
      <form className="chat-composer" onSubmit={(event) => { event.preventDefault(); void send(); }}>
        <label className="visually-hidden" htmlFor="chat-input">消息内容</label>
        <textarea id="chat-input" rows={3} value={input} onChange={(event) => setInput(event.target.value)} placeholder="输入消息" disabled={!mutable || !sessionID} />
        <div className="chat-actions">
        <button type="button" onClick={retry} disabled={!mutable || !sessionID || !retryCandidate || sending || streamState === "completed" || streamState === "reconnecting"}>重试</button>
        <button type="button" onClick={() => void cancel()} disabled={!mutable || !sessionID || !activeRequestID || streamState === "completed" || streamState === "failed" || streamState === "cancelled"}>取消</button>
        <button className="primary" type="submit" disabled={!mutable || !sessionID || !input || sending}>
          <Send aria-hidden="true" />{sending ? "发送中" : "发送"}
        </button>
        </div>
      </form>
      {sessionID && <output className="chat-status" role="status"><MessageSquare aria-hidden="true" />{sessionID}{activeRequestID && ` · ${streamState}`}</output>}
    </div>
  );
}
