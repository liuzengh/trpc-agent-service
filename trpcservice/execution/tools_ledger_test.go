package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// This file is the P3 acceptance suite ("Tool 执行账本与人工核对") driven
// through the real worker, a real MySQL and real HTTP upstreams:
//
//	TestToolCallEndToEndThroughWorker          a governed tool call is
//	                                           journaled and answered
//	TestToolUnknownBlocksAndResolutionRequeues an ambiguous write parks the
//	                                           session; a human disposition
//	                                           requeues it and it succeeds
//	TestRecoveryBlocksStaleWriteAbandonsStaleRead  the ledger decides what a
//	                                           dead attempt allows
//	TestGovernorRejectionMatrix                schema/risk/revocation/budget
//	                                           refusals land in the ledger
//
// The transport-level behaviours (SSRF classification, redirect refusal,
// retry caps) live in trpcservice/tool's own tests; this file is about the
// wiring those tests cannot see.

// toolUpstream is an OpenAI-compatible stream that emits a tool call on the
// first request of a conversation and a final answer once it sees the tool
// result in the payload — the exact two-request shape a real function-calling
// loop produces.
type toolUpstream struct {
	mu        sync.Mutex
	requests  int
	sawTool   bool
	toolName  string
	arguments string
	finalText string
}

func (u *toolUpstream) setScript(name, args string) {
	u.mu.Lock()
	u.toolName, u.arguments = name, args
	u.mu.Unlock()
}

func (u *toolUpstream) stats() (requests int, sawTool bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.requests, u.sawTool
}

func (u *toolUpstream) handler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []struct {
			Role string `json:"role"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	hasToolResult := false
	for _, m := range req.Messages {
		if m.Role == "tool" {
			hasToolResult = true
		}
	}

	u.mu.Lock()
	u.requests++
	if hasToolResult {
		u.sawTool = true
	}
	count := u.requests
	toolName, args, final := u.toolName, u.arguments, u.finalText
	u.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	write := func(payload string) {
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		if flusher != nil {
			flusher.Flush()
		}
	}
	chunk := func(choices []map[string]any, usage map[string]int) {
		body, err := json.Marshal(map[string]any{
			"id": fmt.Sprintf("chatcmpl-tool-%d", count), "object": "chat.completion.chunk",
			"created": time.Now().Unix(), "model": "fake-model",
			"choices": choices, "usage": usage,
		})
		if err != nil {
			return
		}
		write(string(body))
	}

	if !hasToolResult {
		chunk([]map[string]any{{
			"index": 0,
			"delta": map[string]any{
				"tool_calls": []map[string]any{{
					"index": 0, "id": "call_1", "type": "function",
					"function": map[string]string{"name": toolName, "arguments": args},
				}},
			},
			"finish_reason": nil,
		}}, nil)
		chunk([]map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}, nil)
		write("[DONE]")
		return
	}

	chunk([]map[string]any{{"index": 0, "delta": map[string]string{"content": final}, "finish_reason": nil}}, nil)
	usage := map[string]int{"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
	chunk([]map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}, usage)
	write("[DONE]")
}

// toolTarget is the HTTP service a binding points at. Its behaviour is
// switchable at runtime so one test can present a healthy upstream and a
// hanging one without rebuilding the binding.
type toolTarget struct {
	srv     *httptest.Server
	mu      sync.Mutex
	mode    string // ok | hang
	hits    int
	lastRaw string
}

func newToolTarget(t *testing.T) *toolTarget {
	t.Helper()
	target := &toolTarget{mode: "ok"}
	target.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		target.mu.Lock()
		target.hits++
		target.lastRaw = string(body)
		mode := target.mode
		target.mu.Unlock()
		if mode == "hang" {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tool_said":"alpha ok"}`))
	}))
	t.Cleanup(target.srv.Close)
	return target
}

func (tt *toolTarget) setMode(mode string) {
	tt.mu.Lock()
	tt.mode = mode
	tt.mu.Unlock()
}

func (tt *toolTarget) stats() (hits int, raw string) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.hits, tt.lastRaw
}

type toolHarness struct {
	svc       *Service
	worker    *Worker
	cdp       *controlplane.DB
	scope     controlplane.Scope
	tenantID  string
	appID     int64
	modelID   int64
	backendID int64
	bindingID int64
	upstream  *toolUpstream
	target    *toolTarget
}

func setupToolTest(t *testing.T) *toolHarness {
	t.Helper()
	dsn := os.Getenv("TOOLS_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("TOOLS_MYSQL_TEST_DSN not set; skipping real-mysql tool ledger test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetToolSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	upstream := &toolUpstream{finalText: "final answer after tools"}
	srv := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(srv.Close)

	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "acme"
	if err := cdp.CreateTenant(ctx, tenantID, "Acme"); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(tenantID)

	t.Setenv("TOOLS_TEST_API_KEY", "not-a-real-key")
	modelID, err := scope.CreateModelProfile(ctx, "default-model", "fake-model", srv.URL, "env:TOOLS_TEST_API_KEY")
	if err != nil {
		t.Fatal(err)
	}
	backendID, err := scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	appID, err := scope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatal(err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}

	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: []string{"TOOLS_TEST_API_KEY"}})
	factory := DefaultRunnerFactory(cdp, resolver)
	svc := NewService(cdp, DefaultLeaseTTL)
	w := NewWorker(svc, WorkerOptions{WorkerID: "worker-tools-1", IdleWait: 10 * time.Millisecond}, factory)

	return &toolHarness{
		svc: svc, worker: w, cdp: cdp, scope: scope, tenantID: tenantID,
		appID: appID, modelID: modelID, backendID: backendID, bindingID: bindingID,
		upstream: upstream, target: newToolTarget(t),
	}
}

// bindHTTPTool registers one HTTP binding against the harness's target and
// returns its pinned-tools JSON for a revision.
func (h *toolHarness) bindHTTPTool(t *testing.T, name, sideEffect string, idempotent bool, timeoutMS int) string {
	t.Helper()
	host := h.target.srv.Listener.Addr().(*net.TCPAddr).IP.String()
	spec := map[string]any{
		"method":      "POST",
		"url":         h.target.srv.URL + "/invoke",
		"allow_hosts": []string{host},
		"allow_cidrs": []string{host + "/32"},
	}
	specJSON, _ := json.Marshal(spec)
	schema := json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`)
	if _, err := h.scope.BindTool(context.Background(), controlplane.ToolBinding{
		AppID: h.appID, Name: name, Kind: "http",
		RiskLevel: "low", SideEffect: sideEffect, Idempotent: idempotent,
		Spec: specJSON, InputSchema: schema, TimeoutMS: timeoutMS,
	}); err != nil {
		t.Fatalf("bind tool %s: %v", name, err)
	}
	pinned, _ := json.Marshal(map[string]any{
		"pinned": []map[string]any{{"name": name, "version": 1}},
	})
	return string(pinned)
}

func (h *toolHarness) publish(t *testing.T, toolsJSON string) {
	t.Helper()
	if _, err := h.scope.PublishRevision(context.Background(), "assistant", controlplane.RevisionSpec{
		Instruction: "be terse", ModelProfileID: h.modelID, BackendProfileID: h.backendID,
		MaxLLMCalls: 8, MessageTimeoutMS: 30000,
		Tools: json.RawMessage(toolsJSON),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func (h *toolHarness) accept(t *testing.T, actor, msgID string) {
	t.Helper()
	rev, err := h.scope.CurrentRevision(context.Background(), "assistant")
	if err != nil {
		t.Fatal(err)
	}
	_, err = inbox.NewService(h.cdp).Accept(context.Background(), h.tenantID, inbox.Request{
		AppID: h.appID, ChannelType: "webchat", BindingID: h.bindingID,
		ActorKey: actor, RevisionID: rev.ID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: msgID, Text: "please call the tool",
	})
	if err != nil {
		t.Fatalf("accept %s: %v", msgID, err)
	}
}

func (h *toolHarness) runOnce(t *testing.T, workerID string) bool {
	t.Helper()
	claim, err := h.svc.ClaimNext(context.Background(), h.tenantID, workerID)
	if errors.Is(err, ErrNothingToClaim) {
		return false
	}
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := h.worker.RunOne(context.Background(), claim); err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	return true
}

// sessionOf reads the session of one actor. Tests that touch more than one
// conversation must name the actor: ordering by "the first session" silently
// asserts about the wrong row the moment a second one exists.
func (h *toolHarness) sessionOf(t *testing.T, actor string) (blocked sql.NullString, head, inSeq uint32) {
	t.Helper()
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT blocked_reason, head_seq, in_seq FROM sessions WHERE tenant_id = ? AND actor_key = ?",
		h.tenantID, actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&blocked, &head, &inSeq); err != nil {
		t.Fatal(err)
	}
	return
}

func (h *toolHarness) sessionPK(t *testing.T, actor string) int64 {
	t.Helper()
	var pk int64
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT session_pk FROM sessions WHERE tenant_id = ? AND actor_key = ?", h.tenantID, actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&pk); err != nil {
		t.Fatal(err)
	}
	return pk
}

func (h *toolHarness) callRows(t *testing.T) []tool.Call {
	t.Helper()
	calls, err := tool.NewJournal(h.cdp).List(context.Background(), h.tenantID, tool.CallFilter{})
	if err != nil {
		t.Fatal(err)
	}
	return calls
}

// scalarString reads a one-column, one-row query; the controlplane scope's
// QueryRow returns a (row, error) pair, which does not chain into Scan.
func (h *toolHarness) scalarString(t *testing.T, query string, args ...any) string {
	t.Helper()
	row, err := h.scope.QueryRow(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	var v string
	if err := row.Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func (h *toolHarness) scalarInt(t *testing.T, query string, args ...any) int {
	t.Helper()
	row, err := h.scope.QueryRow(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	var v int
	if err := row.Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func resetToolSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
		"delivery_attempts", "reply_outbox", "session_events", "execution_attempts",
		"executions", "inbox_messages", "channel_reply_routes", "channel_checkpoints",
		"channel_notifications", "outbox_events", "audit_events", "sessions",
		"knowledge_bindings", "knowledge_bases", "tool_bindings",
		"channel_identities", "channel_bindings",
		"agent_revisions", "agent_apps", "backend_profiles", "model_profiles",
		"tenant_users", "principals", "tenants", "schema_migrations",
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatal(err)
	}
}

func TestToolCallEndToEndThroughWorker(t *testing.T) {
	h := setupToolTest(t)
	h.upstream.setScript("lookup", `{"text":"alpha"}`)
	pinned := h.bindHTTPTool(t, "lookup", "read", false, 5000)
	h.publish(t, pinned)
	h.accept(t, "user-1", "msg-tool-1")

	if !h.runOnce(t, "worker-tools-1") {
		t.Fatal("nothing claimable")
	}

	if requests, sawTool := h.upstream.stats(); requests != 2 || !sawTool {
		t.Fatalf("upstream = %d requests (sawTool=%t), want 2 with the tool result fed back", requests, sawTool)
	}
	hits, raw := h.target.stats()
	if hits != 1 {
		t.Fatalf("tool target hits = %d, want 1", hits)
	}
	if !strings.Contains(raw, `"text":"alpha"`) {
		t.Fatalf("tool target body = %q, want the model's arguments", raw)
	}

	calls := h.callRows(t)
	if len(calls) != 1 {
		t.Fatalf("ledger rows = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.Status != "succeeded" || c.Attempts != 1 || c.ToolKind != "http" || c.SideEffect != "read" {
		t.Fatalf("ledger row = %+v", c)
	}
	if c.ArgumentsHash == "" || len(c.ArgumentsMasked) == 0 {
		t.Fatalf("ledger row is missing the argument digest/masked copy: %+v", c)
	}

	var attemptStatus string
	var httpStatus int
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT status, http_status FROM tool_call_attempts WHERE tenant_id = ? AND call_id = ?",
		h.tenantID, c.CallID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&attemptStatus, &httpStatus); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "succeeded" || httpStatus != 200 {
		t.Fatalf("attempt row = %s/%d, want succeeded/200", attemptStatus, httpStatus)
	}

	reply := h.scalarString(t,
		"SELECT text FROM reply_outbox WHERE tenant_id = ?", h.tenantID)
	if reply != "final answer after tools" {
		t.Fatalf("reply = %q", reply)
	}
	blocked, head, inSeq := h.sessionOf(t, "user-1")
	if blocked.Valid {
		t.Fatalf("session blocked after a healthy tool call: %q", blocked.String)
	}
	if head != 2 || inSeq != 1 {
		t.Fatalf("head/in_seq = %d/%d, want 2/1", head, inSeq)
	}
}

func TestToolUnknownBlocksAndResolutionRequeues(t *testing.T) {
	h := setupToolTest(t)
	h.upstream.setScript("charge", `{"text":"99"}`)
	// A non-idempotent write: its timeout is the "外部副作用不确定" case.
	pinned := h.bindHTTPTool(t, "charge", "write", false, 700)
	h.publish(t, pinned)
	h.accept(t, "user-1", "msg-write-1")

	h.target.setMode("hang")
	if !h.runOnce(t, "worker-tools-1") {
		t.Fatal("nothing claimable")
	}

	blocked, head, _ := h.sessionOf(t, "user-1")
	if !blocked.Valid || !strings.HasPrefix(blocked.String, "tool ") {
		t.Fatalf("blocked_reason = %q, want a tool block", blocked.String)
	}
	if head != 1 {
		t.Fatalf("head_seq = %d, want 1 (a blocked message stays at the head)", head)
	}
	inboxStatus := h.scalarString(t, "SELECT status FROM inbox_messages WHERE tenant_id = ?", h.tenantID)
	execStatus := h.scalarString(t, "SELECT status FROM executions WHERE tenant_id = ?", h.tenantID)
	if inboxStatus != "unknown" || execStatus != "unknown" {
		t.Fatalf("inbox/execution status = %s/%s, want unknown/unknown", inboxStatus, execStatus)
	}
	replies := h.scalarInt(t, "SELECT COUNT(*) FROM reply_outbox WHERE tenant_id = ?", h.tenantID)
	if replies != 0 {
		t.Fatalf("reply rows = %d; a blocked run must not tell the user anything", replies)
	}
	calls := h.callRows(t)
	if len(calls) != 1 || calls[0].Status != "unknown" {
		t.Fatalf("ledger = %+v, want one unknown row", calls)
	}
	if calls[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (a non-idempotent write is never retried)", calls[0].Attempts)
	}

	// The human disposes: the effect did not happen, requeue.
	journal := tool.NewJournal(h.cdp)
	if _, err := journal.Resolve(context.Background(), h.tenantID, calls[0].CallID, "cancelled", "operator:1"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	out, err := h.svc.TryUnblock(context.Background(), h.tenantID, calls[0].SessionPK)
	if err != nil {
		t.Fatalf("unblock: %v", err)
	}
	if !out.Unblocked || out.Disposition != DispositionCancelled {
		t.Fatalf("unblock outcome = %+v", out)
	}

	// The upstream is healthy again; the re-run must produce the real answer.
	h.target.setMode("ok")
	if !h.runOnce(t, "worker-tools-1") {
		t.Fatal("the requeued message was not claimable")
	}
	reply := h.scalarString(t,
		"SELECT text FROM reply_outbox WHERE tenant_id = ?", h.tenantID)
	if reply != "final answer after tools" {
		t.Fatalf("reply after requeue = %q", reply)
	}
	blocked, head, _ = h.sessionOf(t, "user-1")
	if blocked.Valid {
		t.Fatalf("still blocked after a successful re-run: %q", blocked.String)
	}
	if head != 2 {
		t.Fatalf("head_seq = %d, want 2 after the re-run committed", head)
	}
	calls = h.callRows(t)
	if len(calls) != 2 {
		t.Fatalf("ledger rows = %d, want 2 (the dead attempt and the re-run)", len(calls))
	}
	var succeeded, resolvedCancelled int
	for _, c := range calls {
		if c.Status == "succeeded" {
			succeeded++
		}
		if c.Status == "unknown" && c.Resolution == "cancelled" {
			resolvedCancelled++
		}
	}
	if succeeded != 1 || resolvedCancelled != 1 {
		t.Fatalf("ledger states = %+v", calls)
	}
	if requests, _ := h.upstream.stats(); requests < 3 {
		t.Fatalf("upstream requests = %d, want the model answered after the re-run's tool result", requests)
	}
}

func TestRecoveryBlocksStaleWriteAbandonsStaleRead(t *testing.T) {
	h := setupToolTest(t)
	h.upstream.setScript("lookup", `{"text":"alpha"}`)
	pinned := h.bindHTTPTool(t, "lookup", "read", false, 5000)
	h.publish(t, pinned)
	journal := tool.NewJournal(h.cdp)

	// --- a stale write must block ------------------------------------------
	h.accept(t, "user-1", "msg-stale-write")
	claim, err := h.svc.ClaimNext(context.Background(), h.tenantID, "worker-tools-1")
	if err != nil {
		t.Fatal(err)
	}
	// The dead attempt got as far as journaling a write intent and died.
	if err := journal.Begin(context.Background(), tool.CallIntent{
		TenantID: h.tenantID, ExecutionID: claim.ExecutionID, SessionPK: claim.SessionPK,
		CallSeq: 1, ToolName: "charge", ToolVersion: 1, ToolKind: "http",
		SideEffect: "write", Idempotent: false,
		ArgsHash: tool.HashArguments([]byte(`{"text":"x"}`)), ArgsMasked: tool.MaskArguments([]byte(`{"text":"x"}`)),
		WorkerID: "dead-worker", Fence: claim.FenceToken,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.svc.StopRenewing(context.Background(), claim); err != nil {
		t.Fatal(err)
	}

	if !h.runOnce(t, "worker-tools-2") {
		t.Fatal("nothing claimable after the dead attempt released its lease")
	}
	blocked, head, _ := h.sessionOf(t, "user-1")
	if !blocked.Valid || !strings.Contains(blocked.String, "previous attempt") {
		t.Fatalf("blocked_reason = %q, want the recovery rule's wording", blocked.String)
	}
	if !strings.HasPrefix(blocked.String, ToolBlockPrefix) {
		t.Fatalf("recovery block %q does not carry the tool prefix TryUnblock requires", blocked.String)
	}
	if head != 1 {
		t.Fatalf("head_seq = %d, want 1", head)
	}
	if hits, _ := h.target.stats(); hits != 0 {
		t.Fatalf("the healthy tool target was called %d times during recovery", hits)
	}

	// Dispose and re-run to completion, proving the block was not permanent.
	calls := h.callRows(t)
	if len(calls) != 1 || calls[0].Status != "running" {
		t.Fatalf("stale ledger row = %+v, want one running row", calls)
	}
	if _, err := journal.Resolve(context.Background(), h.tenantID, calls[0].CallID, "cancelled", "operator:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.TryUnblock(context.Background(), h.tenantID, calls[0].SessionPK); err != nil {
		t.Fatal(err)
	}
	if !h.runOnce(t, "worker-tools-2") {
		t.Fatal("the requeued message was not claimable")
	}
	blocked, head, _ = h.sessionOf(t, "user-1")
	if blocked.Valid || head != 2 {
		t.Fatalf("after recovery: blocked = %v, head = %d; want clean/2", blocked, head)
	}

	// --- a stale read does not block ---------------------------------------
	h.accept(t, "user-2", "msg-stale-read")
	claim2, err := h.svc.ClaimNext(context.Background(), h.tenantID, "worker-tools-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Begin(context.Background(), tool.CallIntent{
		TenantID: h.tenantID, ExecutionID: claim2.ExecutionID, SessionPK: claim2.SessionPK,
		CallSeq: 1, ToolName: "lookup", ToolVersion: 1, ToolKind: "http",
		SideEffect: "read", Idempotent: false,
		ArgsHash: tool.HashArguments([]byte(`{"text":"y"}`)), ArgsMasked: tool.MaskArguments([]byte(`{"text":"y"}`)),
		WorkerID: "dead-worker", Fence: claim2.FenceToken,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.svc.StopRenewing(context.Background(), claim2); err != nil {
		t.Fatal(err)
	}
	// This claim is for user-2's session, but runOnce claims the *oldest*
	// claimable session; user-1's conversation has no pending work, so the
	// next claim is user-2's.
	if !h.runOnce(t, "worker-tools-2") {
		t.Fatal("nothing claimable for the stale-read session")
	}
	blocked, head, _ = h.sessionOf(t, "user-2")
	if blocked.Valid {
		t.Fatalf("a stale read blocked the session: %q", blocked.String)
	}
	if head != 2 {
		t.Fatalf("head_seq = %d, want 2 (the run completed)", head)
	}
	pk := h.sessionPK(t, "user-2")
	abandoned := h.scalarInt(t, `
		SELECT COUNT(*) FROM tool_calls
		WHERE tenant_id = ? AND session_pk = ? AND status = 'failed' AND error_type = 'abandoned'`,
		h.tenantID, pk)
	if abandoned != 1 {
		t.Fatalf("abandoned stale-read rows = %d, want 1", abandoned)
	}
}

func TestGovernorRejectionMatrix(t *testing.T) {
	h := setupToolTest(t)
	pinned := h.bindHTTPTool(t, "lookup", "read", false, 5000)
	h.publish(t, pinned)
	h.accept(t, "user-1", "msg-reject")

	claim, err := h.svc.ClaimNext(context.Background(), h.tenantID, "worker-tools-1")
	if err != nil {
		t.Fatal(err)
	}
	gov, err := tool.NewGovernor(tool.GovernorOptions{
		Journal: tool.NewJournal(h.cdp), DB: h.cdp,
		TenantID: h.tenantID, ExecutionID: claim.ExecutionID, SessionPK: claim.SessionPK,
		WorkerID: "worker-tools-1", Fence: claim.FenceToken, Budget: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	echo, ok := tool.Builtins()["echo"].(frameworktool.CallableTool)
	if !ok {
		t.Fatal("the echo builtin is not callable")
	}
	echoTool := gov.Wrap(echo, tool.ToolMeta{Name: "echo"})
	risky := gov.Wrap(echo, tool.ToolMeta{Name: "risky", RiskLevel: "high"})

	built, err := tool.BuildPinned(ctx, h.scope, h.appID, json.RawMessage(pinned), gov, nil, nil)
	if err != nil {
		t.Fatalf("BuildPinned: %v", err)
	}
	if len(built) != 1 {
		t.Fatalf("built %d tools, want 1", len(built))
	}
	lookup, ok := built[0].(frameworktool.CallableTool)
	if !ok {
		t.Fatalf("the pinned tool is not callable: %T", built[0])
	}

	// 1. schema: a wrong-typed argument never reaches the network.
	if _, err := lookup.Call(ctx, []byte(`{"text":7}`)); err == nil {
		t.Fatal("schema-invalid arguments were accepted")
	} else if !strings.Contains(err.Error(), "schema") {
		t.Fatalf("schema refusal = %v", err)
	}
	if hits, _ := h.target.stats(); hits != 0 {
		t.Fatalf("the target was called %d times for a schema-refused call", hits)
	}

	// 2. risk: high-risk bindings are refused outright.
	if _, err := risky.Call(ctx, []byte(`{}`)); !errors.Is(err, tool.ErrHighRisk) {
		t.Fatalf("high-risk call error = %v, want ErrHighRisk", err)
	}

	// 3. revocation lands after assembly; the next call must see it.
	if _, err := h.scope.Exec(ctx,
		"UPDATE tool_bindings SET status = 'revoked' WHERE tenant_id = ? AND app_id = ? AND name = ?",
		h.tenantID, h.appID, "lookup"); err != nil {
		t.Fatal(err)
	}
	if _, err := lookup.Call(ctx, []byte(`{"text":"ok"}`)); err == nil || !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("revoked call error = %v", err)
	}
	if hits, _ := h.target.stats(); hits != 0 {
		t.Fatalf("the target was called %d times for a revoked binding", hits)
	}

	// 4. a healthy governed call still works.
	if _, err := echoTool.Call(ctx, []byte(`{"text":"hi"}`)); err != nil {
		t.Fatalf("echo: %v", err)
	}

	// 5. the budget is exhausted by all of the above (rejections count).
	if _, err := echoTool.Call(ctx, []byte(`{"text":"again"}`)); !errors.Is(err, tool.ErrBudgetExceeded) {
		t.Fatalf("over-budget call error = %v, want ErrBudgetExceeded", err)
	}

	calls := h.callRows(t)
	byType := map[string]int{}
	for _, c := range calls {
		byType[c.Status+"/"+c.ErrorType]++
	}
	for _, want := range []string{
		"rejected/schema_invalid", "rejected/risk_high", "rejected/revoked",
		"rejected/budget_exceeded", "succeeded/",
	} {
		if byType[want] != 1 {
			t.Fatalf("ledger states = %v, missing a single %q", byType, want)
		}
	}
}
