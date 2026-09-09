import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ChatPage } from "./ChatPage";
import type { Identity } from "./api";

const identity: Identity = {
  id: "developer",
  name: "Developer",
  active_tenant_id: "tenant-a",
  active_role: "operator",
  assignments: [{ tenant_id: "tenant-a", tenant_name: "Tenant A", role: "operator" }],
};

const storage = new Map<string, string>();
vi.stubGlobal("localStorage", {
  getItem: (key: string) => storage.get(key) ?? null,
  setItem: (key: string, value: string) => storage.set(key, value),
  removeItem: (key: string) => storage.delete(key),
});

beforeEach(() => {
  storage.clear();
});

function jsonResponse(value: unknown, status = 200) {
  return new Response(JSON.stringify(value), { status });
}

test("restores backend history after refresh without persisting conversation data", async () => {
  localStorage.setItem("trpc-chat-session:tenant-a", "session-one");
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    if (path.endsWith("/chat/sessions/session-one/events")) {
      return jsonResponse({
        items: [
          { id: "1", tenant_id: "tenant-a", session_id: "session-one", sequence: 1, idempotency_key: "created", type: "session.created", payload: JSON.stringify({ app_id: "app-one" }), occurred_at: "now" },
          { id: "2", tenant_id: "tenant-a", session_id: "session-one", sequence: 2, idempotency_key: "request-one:input", type: "message.input", payload: btoa(JSON.stringify({ input: "hello" })), occurred_at: "now" },
          { id: "3", tenant_id: "tenant-a", session_id: "session-one", sequence: 3, idempotency_key: "request-one:completed", type: "message.completed", payload: btoa(JSON.stringify({ output: "echo:hello" })), occurred_at: "now" },
        ],
      });
    }
    return jsonResponse({});
  });
  vi.stubGlobal("fetch", fetchMock);

  render(<ChatPage identity={identity} />);
  expect(await screen.findByText("hello")).toBeInTheDocument();
  expect(screen.getByText("echo:hello")).toBeInTheDocument();
  expect(localStorage.getItem("trpc-chat-session:tenant-a")).toBe("session-one");
});

test("reconnects and resumes the active run after refresh", async () => {
  localStorage.setItem("trpc-chat-session:tenant-a", "session-one");
  class RefreshEventSource {
    onmessage: ((event: MessageEvent<string>) => void) | null = null;
    onerror: (() => void) | null = null;
    closed = false;
    constructor(public readonly url: string) {}
    close() { this.closed = true; }
    emit(event: Record<string, unknown>) {
      this.onmessage?.({ data: JSON.stringify(event) } as MessageEvent<string>);
    }
  }
  const sources: RefreshEventSource[] = [];
  vi.stubGlobal("EventSource", class extends RefreshEventSource {
    constructor(url: string) {
      super(url);
      sources.push(this);
    }
  });
  const messageCalls: Array<RequestInit | undefined> = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    if (path.endsWith("/chat/sessions/session-one/events")) {
      return jsonResponse({
        items: [
          { id: "event-input", tenant_id: "tenant-a", session_id: "session-one", sequence: 2, idempotency_key: "request-one:input", type: "message.input", payload: btoa(JSON.stringify({ input: "hello", request_id: "request-one" })), occurred_at: "now" },
          { id: "event-started", tenant_id: "tenant-a", session_id: "session-one", sequence: 3, idempotency_key: "request-one:started", type: "run.started", payload: btoa(JSON.stringify({ request_id: "request-one" })), occurred_at: "now" },
        ],
      });
    }
    if (path.includes("/messages")) {
      messageCalls.push(init);
      return jsonResponse({ session_id: "session-one", request_id: "request-one", status: "running" }, 202);
    }
    return jsonResponse({});
  }));

  render(<ChatPage identity={identity} />);
  await waitFor(() => expect(sources).toHaveLength(1));
  expect(sources[0].url).toContain("/stream?after=0&request_id=request-one");
  await waitFor(() => expect(messageCalls).toHaveLength(1));
  expect((messageCalls[0]?.headers as Record<string, string>)["X-Request-ID"]).toBe("request-one");
  expect(JSON.parse(String(messageCalls[0]?.body))).toEqual({ input: "hello" });

  act(() => sources[0].emit({ event_id: "event-terminal", request_id: "request-one", session_id: "session-one", sequence: 4, type: "run.completed", data: {} }));
  await waitFor(() => expect(sources[0].closed).toBe(true));
  expect(await screen.findByText("session-one · completed")).toBeInTheDocument();
});

test("creates a session and sends a request-scoped message", async () => {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    if (path.endsWith("/chat/sessions")) return jsonResponse({ id: "session-one", tenant_id: "tenant-a", app_id: "app-one", user_id: "developer", sequence: 1 }, 201);
    if (path.includes("/messages")) return jsonResponse({ session_id: "session-one", request_id: "request-one", status: "running" }, 202);
    if (path.endsWith("/events")) {
      return jsonResponse({
        items: [
          { id: "event-input", tenant_id: "tenant-a", session_id: "session-one", sequence: 4, idempotency_key: "request-one:input", type: "message.input", payload: JSON.stringify({ input: "hello" }), occurred_at: "now" },
          { id: "event-output", tenant_id: "tenant-a", session_id: "session-one", sequence: 5, idempotency_key: "request-one:completed", type: "message.completed", payload: JSON.stringify({ output: "echo:hello" }), occurred_at: "now" },
        ],
      });
    }
    return jsonResponse({});
  });
  vi.stubGlobal("fetch", fetchMock);

  render(<ChatPage identity={identity} />);
  await screen.findByDisplayValue("App One");
  fireEvent.change(screen.getByLabelText("Session ID"), { target: { value: "session-one" } });
  fireEvent.click(screen.getByRole("button", { name: "创建 / 打开" }));
  await waitFor(() => expect(screen.getByText("session-one")).toBeInTheDocument());
  fireEvent.change(screen.getByLabelText("消息内容"), { target: { value: "hello" } });
  fireEvent.click(screen.getByRole("button", { name: "发送" }));

  await waitFor(() => {
    const messageCall = fetchMock.mock.calls.find(([path]) => String(path).endsWith("/messages"));
    expect(messageCall).toBeTruthy();
    expect((messageCall![1] as RequestInit).headers).toMatchObject({ "X-Request-ID": expect.any(String) });
    expect(JSON.parse(String((messageCall![1] as RequestInit).body))).toEqual({ input: "hello" });
  });
});

test("disables chat writes for viewers", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    return jsonResponse({ items: [] });
  }));
  render(<ChatPage identity={{ ...identity, active_role: "viewer" }} />);
  expect(await screen.findByDisplayValue("App One")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "创建 / 打开" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "发送" })).toBeDisabled();
});

test("renders SSE deltas, deduplicates by event id, and closes on terminal events", async () => {
  const instances: FakeEventSource[] = [];
  class FakeEventSource {
    onmessage: ((event: MessageEvent<string>) => void) | null = null;
    onerror: (() => void) | null = null;
    closed = false;
    constructor(public readonly url: string) {
      instances.push(this);
    }
    close() { this.closed = true; }
    emit(event: Record<string, unknown>) {
      this.onmessage?.({ data: JSON.stringify(event) } as MessageEvent<string>);
    }
  }
  vi.stubGlobal("EventSource", FakeEventSource);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    if (path.endsWith("/chat/sessions")) return jsonResponse({ id: "session-one", tenant_id: "tenant-a", app_id: "app-one" }, 201);
    if (path.includes("/messages")) return jsonResponse({ session_id: "session-one", request_id: "request-one", status: "running" }, 202);
    if (path.endsWith("/events")) return jsonResponse({ items: [] });
    return jsonResponse({});
  }));

  render(<ChatPage identity={identity} />);
  await screen.findByDisplayValue("App One");
  fireEvent.change(screen.getByLabelText("Session ID"), { target: { value: "session-one" } });
  fireEvent.click(screen.getByRole("button", { name: "创建 / 打开" }));
  await waitFor(() => expect(screen.getByText("session-one")).toBeInTheDocument());
  fireEvent.change(screen.getByLabelText("消息内容"), { target: { value: "hello" } });
  fireEvent.click(screen.getByRole("button", { name: "发送" }));
  await waitFor(() => expect(instances).toHaveLength(1));
  expect(instances[0].url).toContain("/stream?after=0&request_id=");
  act(() => instances[0].emit({ event_id: "event-1", request_id: "request-one", session_id: "session-one", sequence: 4, type: "message.delta", data: { delta: "echo:he" } }));
  act(() => instances[0].emit({ event_id: "event-1", request_id: "request-one", session_id: "session-one", sequence: 4, type: "message.delta", data: { delta: "duplicate" } }));
  expect(await screen.findByText("echo:he")).toBeInTheDocument();
  expect(screen.queryByText("duplicate")).not.toBeInTheDocument();
  act(() => instances[0].emit({ event_id: "event-2", request_id: "request-one", session_id: "session-one", sequence: 5, type: "message.completed", data: { output: "echo:hello" } }));
  expect(await screen.findByText("echo:hello")).toBeInTheDocument();
  act(() => instances[0].emit({ event_id: "event-3", request_id: "request-one", session_id: "session-one", sequence: 6, type: "run.completed", data: {} }));
  await waitFor(() => expect(instances[0].closed).toBe(true));
  await waitFor(() => expect(screen.getAllByText("hello")).toHaveLength(1));
  expect(screen.getAllByText("echo:hello")).toHaveLength(1);
});

test("reuses the request key after an ambiguous send failure and uses a new key after a terminal failure", async () => {
  class RetryEventSource {
    onmessage: ((event: MessageEvent<string>) => void) | null = null;
    onerror: (() => void) | null = null;
    closed = false;
    constructor(_url: string) {}
    close() { this.closed = true; }
    emit(event: Record<string, unknown>) {
      this.onmessage?.({ data: JSON.stringify(event) } as MessageEvent<string>);
    }
  }
  const sources: RetryEventSource[] = [];
  vi.stubGlobal("EventSource", class extends RetryEventSource {
    constructor(url: string) {
      super(url);
      sources.push(this);
    }
  });
  const messageCalls: Array<RequestInit | undefined> = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    if (path.endsWith("/chat/sessions")) return jsonResponse({ id: "session-one", tenant_id: "tenant-a", app_id: "app-one" }, 201);
    if (path.includes("/messages")) {
      messageCalls.push(init);
      if (messageCalls.length === 1) throw new Error("network dropped");
      return jsonResponse({ session_id: "session-one", request_id: String((init?.headers as Record<string, string>)["X-Request-ID"]), status: "running" }, 202);
    }
    if (path.endsWith("/events")) return jsonResponse({ items: [] });
    return jsonResponse({});
  }));

  render(<ChatPage identity={identity} />);
  await screen.findByDisplayValue("App One");
  fireEvent.change(screen.getByLabelText("Session ID"), { target: { value: "session-one" } });
  fireEvent.click(screen.getByRole("button", { name: "创建 / 打开" }));
  await waitFor(() => expect(screen.getByText("session-one")).toBeInTheDocument());
  fireEvent.change(screen.getByLabelText("消息内容"), { target: { value: "hello" } });
  fireEvent.click(screen.getByRole("button", { name: "发送" }));
  await waitFor(() => expect(messageCalls).toHaveLength(1));
  const requestID = String((messageCalls[0]?.headers as Record<string, string>)["X-Request-ID"]);
  await waitFor(() => expect(screen.getByRole("button", { name: "重试" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "重试" }));
  await waitFor(() => expect(messageCalls).toHaveLength(2));
  expect((messageCalls[1]?.headers as Record<string, string>)["X-Request-ID"]).toBe(requestID);

  await waitFor(() => expect(sources).toHaveLength(1));
  act(() => sources[0].emit({ event_id: "event-terminal", request_id: requestID, session_id: "session-one", sequence: 5, type: "run.failed", data: {} }));
  await waitFor(() => expect(screen.getByRole("button", { name: "重试" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "重试" }));
  await waitFor(() => expect(messageCalls).toHaveLength(3));
  const retriedID = String((messageCalls[2]?.headers as Record<string, string>)["X-Request-ID"]);
  expect(retriedID).not.toBe(requestID);
  expect(JSON.parse(String(messageCalls[2]?.body))).toEqual({ input: "hello" });
});

test("sets session-scoped Mock IM faults from the workspace", async () => {
  const faultCalls: Array<RequestInit | undefined> = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/agent-apps")) return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-a", name: "App One", created_at: "now" }] });
    if (path.endsWith("/chat/sessions")) return jsonResponse({ id: "session-one", tenant_id: "tenant-a", app_id: "app-one" }, 201);
    if (path.includes("/mock/faults")) {
      if (init?.method === "POST") faultCalls.push(init);
      return jsonResponse({ scenario: "message_length", message_length_limit: 4, attachment_size_limit: 8, rate_limit: 1, timeout_ms: 20, retry_limit: 2 });
    }
    return jsonResponse({ items: [] });
  }));

  render(<ChatPage identity={identity} />);
  await screen.findByDisplayValue("App One");
  fireEvent.change(screen.getByLabelText("Session ID"), { target: { value: "session-one" } });
  fireEvent.click(screen.getByRole("button", { name: "创建 / 打开" }));
  await waitFor(() => expect(screen.getByText("session-one")).toBeInTheDocument());
  fireEvent.change(screen.getByLabelText("Mock 故障"), { target: { value: "message_length" } });
  fireEvent.click(screen.getByRole("button", { name: "设置故障" }));

  await waitFor(() => expect(faultCalls).toHaveLength(1));
  expect(JSON.parse(String(faultCalls[0]?.body))).toEqual({ scenario: "message_length", session_id: "session-one" });
  expect(await screen.findByText("当前：message_length")).toBeInTheDocument();
});
