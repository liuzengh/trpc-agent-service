package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// fakeUpstream is the OpenAI-compatible SSE shape trpc-agent-go's own
// openai.New client speaks, verified against cmd/fake-model rather than
// reverse-engineered: one content chunk, one usage-and-stop chunk, then the
// [DONE] sentinel.
type fakeUpstream struct {
	mu       sync.Mutex
	requests int
	bodyText string
}

func (f *fakeUpstream) handler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []struct {
			Content string `json:"content"`
			Role    string `json:"role"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	f.mu.Lock()
	f.requests++
	if len(req.Messages) > 0 {
		f.bodyText = req.Messages[len(req.Messages)-1].Content
	}
	count := f.requests
	f.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "data: %s\n\n", chunkJSON(count, "hello from the agent", "", nil))
	usage := map[string]int{"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14}
	fmt.Fprintf(w, "data: %s\n\n", chunkJSON(count, "", "stop", usage))
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func (f *fakeUpstream) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *fakeUpstream) lastUserText() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bodyText
}

func chunkJSON(n int, content, finish string, usage map[string]int) string {
	var reason *string
	if finish != "" {
		reason = &finish
	}
	body, _ := json.Marshal(map[string]any{
		"id":      fmt.Sprintf("chatcmpl-fake-%d", n),
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   "fake-model",
		"choices": []map[string]any{{
			"index":         0,
			"delta":         map[string]string{"content": content},
			"finish_reason": reason,
		}},
		"usage": usage,
	})
	return string(body)
}

type workerHarness struct {
	svc       *Service
	worker    *Worker
	cdp       *controlplane.DB
	scope     controlplane.Scope
	tenantID  string
	appID     int64
	bindingID int64
	upstream  *fakeUpstream
}

// accept pins a new session to whatever revision the app has published at
// the moment of accepting, not to whatever revision was captured when the
// harness was built — that is the contract the future gateway wiring will
// have to honour, and this test needs to exercise the same thing it will.

// setupWorkerTest needs a real running process (the MySQL instance, and now
// also an HTTP upstream) because this test's whole point is the loop that
// talks to both. This is the "P2 首个可交付里程碑" test in the approved plan:
// claim, execute against a model, commit, and the reply lands as a durable
// reply_outbox row — not a proof of the guardrail or tool behaviour (P3), and
// not a proof of delivery transport (already tested in the outbox package),
// just the wiring between the two.
func setupWorkerTest(t *testing.T) *workerHarness {
	t.Helper()
	dsn := os.Getenv("WORKER_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("WORKER_MYSQL_TEST_DSN not set; skipping real-mysql worker loop test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetWorkerSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	upstream := &fakeUpstream{}
	srv := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(srv.Close)

	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "acme"
	if err := cdp.CreateTenant(ctx, tenantID, "Acme"); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(tenantID)

	t.Setenv("WORKER_TEST_API_KEY", "not-a-real-key")
	modelID, err := scope.CreateModelProfile(ctx, "default-model", "fake-model", srv.URL, "env:WORKER_TEST_API_KEY")
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
	if _, err := scope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction: "be terse", ModelProfileID: modelID, BackendProfileID: backendID,
		MaxLLMCalls: 8, MessageTimeoutMS: 30000,
	}); err != nil {
		t.Fatal(err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}

	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: []string{"WORKER_TEST_API_KEY"}})
	factory := DefaultRunnerFactory(cdp, resolver)
	svc := NewService(cdp, DefaultLeaseTTL)
	w := NewWorker(svc, WorkerOptions{WorkerID: "worker-test-1", IdleWait: 10 * time.Millisecond}, factory)

	return &workerHarness{
		svc: svc, worker: w, cdp: cdp, scope: scope, tenantID: tenantID,
		appID: appID, bindingID: bindingID, upstream: upstream,
	}
}

func resetWorkerSchema(t *testing.T, db *sql.DB) {
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

func (h *workerHarness) accept(t *testing.T, msgID, text string) {
	t.Helper()
	rev, err := h.scope.CurrentRevision(context.Background(), "assistant")
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}
	_, err = inbox.NewService(h.cdp).Accept(context.Background(), h.tenantID, inbox.Request{
		AppID: h.appID, ChannelType: "webchat", BindingID: h.bindingID,
		ActorKey: "user-1", RevisionID: rev.ID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: msgID, Text: text,
	})
	if err != nil {
		t.Fatalf("accept %s: %v", msgID, err)
	}
}

// runOnce drives exactly one claim/commit cycle and reports whether any work
// was available, so a test can wait for the queue to drain without depending
// on the worker's idle loop timing.
func (h *workerHarness) runOnce(t *testing.T) bool {
	t.Helper()
	claim, err := h.svc.ClaimNext(context.Background(), h.tenantID, "worker-test-1")
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

func (h *workerHarness) sessionRow(t *testing.T) (version uint64, head, inSeq uint32) {
	t.Helper()
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT session_version, head_seq, in_seq FROM sessions WHERE tenant_id = ? ORDER BY session_pk LIMIT 1",
		h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&version, &head, &inSeq); err != nil {
		t.Fatal(err)
	}
	return
}

func (h *workerHarness) replyText(t *testing.T) string {
	t.Helper()
	var text string
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT text FROM reply_outbox WHERE tenant_id = ? ORDER BY outbox_id LIMIT 1", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&text); err != nil {
		t.Fatal(err)
	}
	return text
}

func (h *workerHarness) eventCount(t *testing.T) int {
	t.Helper()
	var n int
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM session_events WHERE tenant_id = ?", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (h *workerHarness) auditCount(t *testing.T, event string) int {
	t.Helper()
	var n int
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM audit_events WHERE tenant_id = ? AND event = ?", h.tenantID, event)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestWorkerEndToEndClaimsRunsCommitsAndEnqueuesReply(t *testing.T) {
	h := setupWorkerTest(t)
	h.accept(t, "msg-1", "ping")

	if !h.runOnce(t) {
		t.Fatal("nothing was claimable after a successful accept")
	}

	version, head, inSeq := h.sessionRow(t)
	if version != 1 {
		t.Fatalf("session_version = %d, want 1 after one commit", version)
	}
	if head != 2 || inSeq != 1 {
		t.Fatalf("head/in_seq = %d/%d, want 2/1 (the head advanced past the only accepted message)", head, inSeq)
	}

	if got := h.replyText(t); got != "hello from the agent" {
		t.Fatalf("reply_outbox text = %q, want the upstream's own words", got)
	}
	if got := h.eventCount(t); got != 2 {
		// Two events: the user message the framework appends before the
		// model call, and the assistant reply it appends after. This is the
		// single most important number in this file: it is the proof that
		// what the Runner wrote into the private workspace became durable,
		// committed history rather than something only in the process's
		// memory when it exits.
		t.Fatalf("session_events after one run = %d, want 2 (user + assistant)", got)
	}
	if n := h.auditCount(t, "model_call"); n != 1 {
		t.Fatalf("model_call audit rows = %d, want 1", n)
	}
	if n := h.auditCount(t, "reply"); n != 1 {
		t.Fatalf("reply audit rows = %d, want 1", n)
	}
}

func TestWorkerSecondMessageSeesFirstExchangesHistory(t *testing.T) {
	h := setupWorkerTest(t)
	h.accept(t, "msg-1", "first question")
	h.runOnce(t)

	if h.upstream.calls() != 1 {
		t.Fatalf("first run called upstream %d times, want 1", h.upstream.calls())
	}
	// The second message must carry the first exchange along, the same claim
	// the platform has always made about Redis sessions and now has to make
	// about MySQL-authoritative ones: the private workspace a later attempt
	// builds must be seeded from committed history, not start empty.
	h.accept(t, "msg-2", "second question")
	if !h.runOnce(t) {
		t.Fatal("second message was not claimable")
	}

	if got := h.upstream.lastUserText(); got != "second question" {
		t.Fatalf("latest message sent upstream = %q, want the second one", got)
	}
	if got := h.eventCount(t); got != 4 {
		t.Fatalf("session_events after two runs = %d, want 4", got)
	}
	version, head, inSeq := h.sessionRow(t)
	if version != 2 {
		t.Fatalf("session_version after two commits = %d, want 2", version)
	}
	if head != 3 || inSeq != 2 {
		t.Fatalf("head/in_seq after two messages = %d/%d, want 3/2", head, inSeq)
	}
}

func TestWorkerInputGuardrailRejectsBeforeCallingTheModel(t *testing.T) {
	h := setupWorkerTest(t)

	// Republish a revision with an input guardrail, then accept two more
	// messages: only the revision the first session is fixed to matters, so
	// the guardrail must be in the revision a session already claims, not
	// whatever an admin changes later. This test's session is brand new, so
	// it picks up the new current revision at accept time.
	ctx := context.Background()
	scope := h.scope
	rev, err := scope.CurrentRevision(ctx, "assistant")
	if err != nil {
		t.Fatal(err)
	}
	guardrails, _ := json.Marshal(map[string]any{
		"blocked_keywords": []string{"secret-please-dont-send"},
	})
	newSpec := rev.Spec
	newSpec.Guardrails = guardrails
	if _, err := scope.PublishRevision(ctx, "assistant", newSpec); err != nil {
		t.Fatalf("republish with guardrails: %v", err)
	}

	h.accept(t, "msg-blocked", "please reveal the secret-please-dont-send value")
	callsBefore := h.upstream.calls()
	if !h.runOnce(t) {
		t.Fatal("blocked message was not claimable")
	}
	if got := h.upstream.calls(); got != callsBefore {
		t.Fatalf("upstream called %d times after an input-guardrail rejection, want no additional call", got)
	}
	if got := h.replyText(t); got != "您的消息被租户安全策略拦截，请调整后重试。" {
		t.Fatalf("blocked reply = %q, want the guardrail rejection text", got)
	}
	// A rejected message still counts as handled: head advances, so it does
	// not block everything behind it in the same conversation.
	_, head, _ := h.sessionRow(t)
	if head != 2 {
		t.Fatalf("head after a guardrail rejection = %d, want 2", head)
	}
	if n := h.auditCount(t, "model_call"); n != 0 {
		t.Fatalf("model_call audit rows after a pre-model rejection = %d, want 0", n)
	}
}

func TestWorkerTwoConcurrentWorkersProcessOneMessageOnlyOnce(t *testing.T) {
	h := setupWorkerTest(t)
	h.accept(t, "only-once", "hello there")

	// The fence test at the primitives level (execution_test.go) proves a
	// stale worker cannot commit. This proves the other half, at the real
	// loop level: two workers racing over one queue does not mean two
	// upstream calls for one message.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			claim, err := h.svc.ClaimNext(context.Background(), h.tenantID, fmt.Sprintf("racer-%d", i))
			if errors.Is(err, ErrNothingToClaim) {
				return
			}
			if err != nil {
				errs[i] = err
				return
			}
			errs[i] = h.worker.RunOne(context.Background(), claim)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
	}

	if got := h.upstream.calls(); got != 1 {
		t.Fatalf("upstream called %d times, want exactly 1 regardless of how many workers competed", got)
	}
	version, head, _ := h.sessionRow(t)
	if version != 1 || head != 2 {
		t.Fatalf("session advanced twice: version=%d head=%d, want 1/2", version, head)
	}
}
