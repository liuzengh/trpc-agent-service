//go:build integration

// Full-chain integration test: inbound bus message → worker (idempotency →
// session lock → runner.Run with a canned model) → MySQL outbox → dispatcher →
// stream:outbound. Also verifies the double-insurance idempotency: a
// redelivered inbound message produces no second reply.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mysql"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/agentstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/ledgerstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/llmstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/tenantstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/toolstore"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/model"
	fwreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
)

// bagEmbedder embeds text as a hashed bag of words (test fake).
type bagEmbedder struct{ dim int }

func (e *bagEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, e.dim)
	for _, tok := range strings.Fields(strings.ToLower(text)) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		v[h.Sum32()%uint32(e.dim)]++
	}
	return v, nil
}

func (e *bagEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := e.GetEmbedding(ctx, text)
	return v, nil, err
}

func (e *bagEmbedder) GetDimensions() int { return e.dim }

var (
	once     sync.Once
	platform struct {
		dsn, redisURL string
		err           error
	}
)

func startContainers() {
	ctx := context.Background()

	scripts, err := filepath.Glob(filepath.Join("..", "..", "..", "deployments", "mysql", "init", "*.sql"))
	if err != nil {
		platform.err = err
		return
	}
	mc, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
		mysql.WithScripts(scripts...))
	if err != nil {
		platform.err = err
		return
	}
	platform.dsn, err = mc.ConnectionString(ctx, "multiStatements=true")
	if err != nil {
		platform.err = err
		return
	}

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		platform.err = err
		return
	}
	platform.redisURL, err = rc.ConnectionString(ctx)
	if err != nil {
		platform.err = err
	}
}

// cannedModel always answers with a fixed text, shaped like the framework's
// own mock models (single response, channel closed).
type cannedModel struct {
	text string
}

func (m *cannedModel) Info() model.Info { return model.Info{Name: "canned"} }

func (m *cannedModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{
		Done: true, // llmflow terminates the invocation on the Done marker
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, Content: m.text},
		}},
	}
	close(ch)
	return ch, nil
}

func TestWorkerHandleDirect(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	factory := func(_ context.Context, _ llm.Endpoint) (model.Model, error) {
		return &cannedModel{text: "canned reply"}, nil
	}
	reg := llmstore.NewMySQLRegistry(db, factory)
	tenants := tenantstore.NewMySQLManager(db)
	agents := agentstore.NewMySQLManager(db, reg)
	tools := toolstore.NewMySQLRegistry(db)
	outbox := bus.NewOutbox(db)
	router := storage.NewRouter(tenants,
		storage.SessionConfig{Backend: storage.BackendInMemory},
		storage.MemoryConfig{Backend: storage.BackendInMemory},
	)
	w := New(rb, agents, NewToolResolver(tools, nil, nil), outbox, router, nil, nil, nil, nil)

	_ = reg.Create(ctx, llm.Endpoint{
		ID: "e-direct", Scope: llm.ScopeTenant, TenantID: "t-direct", Name: "main",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	})
	_ = agents.Create(ctx, agent.Agent{ID: "a-direct", TenantID: "t-direct", Name: "helper"})
	_, _ = agents.Publish(ctx, "a-direct", agent.RuntimeProfile{
		SystemPrompt: "be helpful", EndpointID: "e-direct",
	})

	userMsg := model.NewUserMessage("hi")
	in := &bus.Message{
		ID: "in-direct-1", TenantID: "t-direct", AgentID: "a-direct",
		SessionID: "s-1", Channel: "admin", UserID: "u-1",
		Content: &userMsg,
	}
	if err := w.handle(ctx, in); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var status string
	if err := db.QueryRow(
		`SELECT status FROM outbox_events WHERE tenant_id = 't-direct'`).Scan(&status); err != nil {
		t.Fatalf("outbox row: %v", err)
	}
	if status != "pending" {
		t.Errorf("outbox status = %q, want pending", status)
	}

	// The durable marker: re-appending under the same msgKey is rejected.
	second := *in
	second.ID = "in-direct-2"
	if err := outbox.Append(ctx, &second, "in-direct-1"); !errors.Is(err, bus.ErrDuplicateIdem) {
		t.Errorf("Append with duplicate key = %v, want ErrDuplicateIdem", err)
	}
	var rows int
	_ = db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE tenant_id = 't-direct'`).Scan(&rows)
	if rows != 1 {
		t.Errorf("outbox rows = %d, want 1", rows)
	}
}

func TestWorkerFullChain(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	factory := func(_ context.Context, _ llm.Endpoint) (model.Model, error) {
		return &cannedModel{text: "canned reply"}, nil
	}
	reg := llmstore.NewMySQLRegistry(db, factory)
	tenants := tenantstore.NewMySQLManager(db)
	agents := agentstore.NewMySQLManager(db, reg)
	tools := toolstore.NewMySQLRegistry(db)
	outbox := bus.NewOutbox(db)
	router := storage.NewRouter(tenants,
		storage.SessionConfig{Backend: storage.BackendInMemory},
		storage.MemoryConfig{Backend: storage.BackendInMemory},
	)
	kbs := knowledgestore.NewMySQLManager(db, knowledge.InMemoryVectorStoreFactory(),
		func(_ context.Context, _ *knowledge.KnowledgeBase) (embedder.Embedder, error) {
			return &bagEmbedder{dim: 64}, nil
		})
	w := New(rb, agents, NewToolResolver(tools, nil, kbs), outbox, router, nil, nil, nil, nil)

	// Seed one endpoint + one KB + one published agent mounting the KB.
	if err := reg.Create(ctx, llm.Endpoint{
		ID: "e-chain", Scope: llm.ScopeTenant, TenantID: "t-chain", Name: "main",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	}); err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if err := kbs.Create(ctx, &knowledge.KnowledgeBase{
		ID: "kb-chain", TenantID: "t-chain", Name: "docs",
		EmbeddingEndpointID: "e-chain", Dimension: 64,
	}); err != nil {
		t.Fatalf("kb: %v", err)
	}
	if err := kbs.AddDocument(ctx, &knowledge.Document{
		ID: "d-chain", KBID: "kb-chain", Title: "facts", Text: "the platform stores vectors in milvus",
	}); err != nil {
		t.Fatalf("kb document: %v", err)
	}
	if err := agents.Create(ctx, agent.Agent{ID: "a-chain", TenantID: "t-chain", Name: "helper"}); err != nil {
		t.Fatalf("agent: %v", err)
	}
	if _, err := agents.Publish(ctx, "a-chain", agent.RuntimeProfile{
		SystemPrompt: "be helpful", EndpointID: "e-chain", KnowledgeIDs: []string{"kb-chain"},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()
	go func() { _ = outbox.Run(runCtx, rb, 50*time.Millisecond) }()

	userMsg := model.NewUserMessage("hi")
	in := &bus.Message{
		ID: "in-chain-1", TenantID: "t-chain", AgentID: "a-chain",
		SessionID: "s-1", Channel: "admin", UserID: "u-1",
		Content: &userMsg,
	}
	if err := rb.PublishInbound(ctx, in); err != nil {
		t.Fatalf("publish inbound: %v", err)
	}

	reply := awaitOutbound(t, rb, "in-chain-1", 15*time.Second)
	if reply == nil {
		t.Fatal("no outbound reply observed")
	}
	if reply.Content == nil || reply.Content.Content != "canned reply" {
		t.Errorf("reply content = %+v, want canned reply", reply.Content)
	}
	if reply.TenantID != "t-chain" || reply.SessionID != "s-1" || reply.AgentID != "a-chain" {
		t.Errorf("reply envelope mismatch: %+v", reply)
	}

	// Redelivery of the same inbound id must not produce a second reply.
	if err := rb.PublishInbound(ctx, in); err != nil {
		t.Fatalf("republish inbound: %v", err)
	}
	time.Sleep(2 * time.Second)
	if n := countOutbound(t, rb, "in-chain-1"); n != 1 {
		t.Errorf("outbound replies for in-chain-1 = %d, want 1", n)
	}
}

// awaitOutbound polls stream:outbound for a reply whose ReplyTo matches.
func awaitOutbound(t *testing.T, rb *bus.RedisBus, replyTo string, timeout time.Duration) *bus.Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range outbound(t, rb) {
			if m.ReplyTo == replyTo {
				return m
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func countOutbound(t *testing.T, rb *bus.RedisBus, replyTo string) int {
	t.Helper()
	n := 0
	for _, m := range outbound(t, rb) {
		if m.ReplyTo == replyTo {
			n++
		}
	}
	return n
}

// outbound reads stream:outbound via XRange and decodes reply_to/content.
func outbound(t *testing.T, rb *bus.RedisBus) []*bus.Message {
	t.Helper()
	ctx := context.Background()
	msgs, err := rb.Client().XRange(ctx, bus.StreamOutbound, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange outbound: %v", err)
	}
	out := make([]*bus.Message, 0, len(msgs))
	for _, xm := range msgs {
		m := &bus.Message{
			ReplyTo:   stringField(xm.Values, "reply_to"),
			TenantID:  stringField(xm.Values, "tenant_id"),
			SessionID: stringField(xm.Values, "session_id"),
			AgentID:   stringField(xm.Values, "agent_id"),
		}
		if raw, ok := xm.Values["content"].(string); ok {
			var content model.Message
			if err := json.Unmarshal([]byte(raw), &content); err == nil {
				m.Content = &content
			}
		}
		out = append(out, m)
	}
	return out
}

func stringField(vals map[string]any, key string) string {
	if s, ok := vals[key].(string); ok {
		return s
	}
	return ""
}

// TestWorkerApprovalFullCycle drives the human-approval core against real
// MySQL (outbox) + Redis (approval state / lock): a turn starts a blocking
// review, an approval notice lands in the outbox, the user's 批准 reply is
// recognized lock-free, and the blocked review wakes up approved.
func TestWorkerApprovalFullCycle(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	outbox := bus.NewOutbox(db)
	memReg := llm.NewRegistry(func(_ context.Context, _ llm.Endpoint) (model.Model, error) {
		return nil, nil
	})
	w := New(rb, agent.NewManager(memReg), nil, outbox, nil, nil, nil, nil, nil)

	// Serialize the session as the worker would before running a turn.
	lockTok := "lock-approve"
	if ok, _ := rb.LockSession(ctx, "t-approve", "s-1", lockTok); !ok {
		t.Fatal("session lock should be free")
	}
	t.Cleanup(func() { _ = rb.UnlockSession(context.Background(), "t-approve", "s-1", lockTok) })

	userMsg := model.NewUserMessage("deploy to main please")
	in := &bus.Message{
		ID: "in-approve-1", TenantID: "t-approve", AgentID: "a-1",
		SessionID: "s-1", Channel: "admin", UserID: "u-1", Content: &userMsg,
	}
	reviewer := &humanReviewer{w: w, m: in, lockTok: lockTok}
	req := &fwreview.Request{
		Action: fwreview.Action{ToolName: "deploy", Arguments: json.RawMessage(`{"branch":"main"}`)},
	}
	decCh := make(chan *fwreview.Decision, 1)
	errCh := make(chan error, 1)
	go func() {
		d, err := reviewer.Review(ctx, req)
		if err != nil {
			errCh <- err
			return
		}
		decCh <- d
	}()

	// Wait until the pending approval + notice are in place.
	deadline := time.Now().Add(15 * time.Second)
	for {
		pending, _ := rb.PendingApproval(ctx, "t-approve", "s-1")
		if pending != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending approval never registered")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The user approves; the reply is recognized without holding the lock.
	approveMsg := model.NewUserMessage("批准")
	reply := &bus.Message{
		ID: "in-approve-2", TenantID: "t-approve", SessionID: "s-1",
		Channel: "admin", UserID: "u-1", Content: &approveMsg,
	}
	handled, err := w.tryResolveApproval(ctx, reply)
	if err != nil {
		t.Fatalf("tryResolveApproval: %v", err)
	}
	if !handled {
		t.Fatal("批准 reply should be consumed as an approval decision")
	}

	select {
	case d := <-decCh:
		if !d.Approved {
			t.Errorf("decision = %+v, want approved", d)
		}
	case err := <-errCh:
		t.Fatalf("review failed: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("review never woke up after approval")
	}

	// Notice + confirmation both landed in the outbox (durable delivery).
	var rows int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM outbox_events WHERE tenant_id = 't-approve'`).Scan(&rows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if rows != 2 {
		t.Errorf("outbox rows = %d, want 2 (notice + confirmation)", rows)
	}
}

// TestWorkerApprovalDenyByReply: a 拒绝 reply denies the tool call and the
// blocked review wakes up with Approved=false.
func TestWorkerApprovalDenyByReply(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	outbox := bus.NewOutbox(db)
	memReg := llm.NewRegistry(func(_ context.Context, _ llm.Endpoint) (model.Model, error) {
		return nil, nil
	})
	w := New(rb, agent.NewManager(memReg), nil, outbox, nil, nil, nil, nil, nil)

	userMsg := model.NewUserMessage("deploy")
	in := &bus.Message{
		ID: "in-deny-1", TenantID: "t-deny", AgentID: "a-1",
		SessionID: "s-1", Channel: "admin", UserID: "u-1", Content: &userMsg,
	}
	reviewer := &humanReviewer{w: w, m: in, lockTok: "unused-token-no-lock-needed"}
	req := &fwreview.Request{
		Action: fwreview.Action{ToolName: "deploy", Arguments: json.RawMessage(`{}`)},
	}
	decCh := make(chan *fwreview.Decision, 1)
	go func() {
		d, _ := reviewer.Review(ctx, req)
		decCh <- d
	}()

	deadline := time.Now().Add(15 * time.Second)
	for {
		pending, _ := rb.PendingApproval(ctx, "t-deny", "s-1")
		if pending != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending approval never registered")
		}
		time.Sleep(100 * time.Millisecond)
	}

	denyMsg := model.NewUserMessage("拒绝")
	reply := &bus.Message{
		ID: "in-deny-2", TenantID: "t-deny", SessionID: "s-1",
		Channel: "admin", UserID: "u-1", Content: &denyMsg,
	}
	handled, err := w.tryResolveApproval(ctx, reply)
	if err != nil || !handled {
		t.Fatalf("deny reply handled=%v err=%v", handled, err)
	}

	select {
	case d := <-decCh:
		if d.Approved {
			t.Errorf("decision = %+v, want denied", d)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("review never woke up after denial")
	}
}

// TestWorkerLedgerWritesTurn: after a full worker turn, the business
// conversation ledger holds the session plus USER + ASSISTANT rows sharing one
// turn_id.
func TestWorkerLedgerWritesTurn(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	factory := func(_ context.Context, _ llm.Endpoint) (model.Model, error) {
		return &cannedModel{text: "ledger reply"}, nil
	}
	reg := llm.NewRegistry(factory)
	_ = reg.Create(ctx, llm.Endpoint{
		ID: "e-ledger", Scope: llm.ScopeTenant, TenantID: "t-ledger", Name: "m",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	})
	agents := agent.NewManager(reg)
	_ = agents.Create(ctx, agent.Agent{ID: "a-ledger", TenantID: "t-ledger", Name: "h"})
	_, _ = agents.Publish(ctx, "a-ledger", agent.RuntimeProfile{
		SystemPrompt: "help", EndpointID: "e-ledger",
	})
	outbox := bus.NewOutbox(db)
	router := storage.NewRouter(tenant.NewManager(),
		storage.SessionConfig{Backend: storage.BackendInMemory},
		storage.MemoryConfig{Backend: storage.BackendInMemory},
	)
	ledger := ledgerstore.NewMySQLLedger(db)
	w := New(rb, agents, nil, outbox, router, nil, nil, nil, ledger)

	userMsg := model.NewUserMessage("hello ledger")
	in := &bus.Message{
		ID: "in-ledger-1", TenantID: "t-ledger", AgentID: "a-ledger",
		SessionID: "s-ledger", Channel: "admin", UserID: "u-ledger", Content: &userMsg,
	}
	if err := w.handle(ctx, in); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var sessions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM chat_sessions WHERE session_id = 's-ledger'`).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 1 {
		t.Errorf("ledger sessions = %d, want 1", sessions)
	}
	var role, content, turnUser, turnAssistant string
	rows, err := db.Query(`SELECT role, content FROM chat_messages WHERE session_id = 's-ledger' ORDER BY role`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&role, &content); err != nil {
			t.Fatal(err)
		}
		if role == "USER" {
			turnUser = content
		} else if role == "ASSISTANT" {
			turnAssistant = content
		}
	}
	if turnUser != "hello ledger" {
		t.Errorf("ledger USER content = %q", turnUser)
	}
	if turnAssistant != "ledger reply" {
		t.Errorf("ledger ASSISTANT content = %q", turnAssistant)
	}

	// turn grouping: both rows share one turn id
	var distinctTurns int
	if err := db.QueryRow(`SELECT COUNT(DISTINCT turn_id) FROM chat_messages WHERE session_id = 's-ledger'`).Scan(&distinctTurns); err != nil {
		t.Fatal(err)
	}
	if distinctTurns != 1 {
		t.Errorf("distinct turns = %d, want 1", distinctTurns)
	}
}
