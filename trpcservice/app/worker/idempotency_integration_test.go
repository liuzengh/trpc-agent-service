//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/agentstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/llmstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/tenantstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/toolstore"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// TestWorkerCommitsIdempotencyAfterTheReplyIsDurable guards the second phase of
// dedup. The claim taken at the start of a turn is only a lease; if the worker
// never upgraded it, a redelivery would re-run the turn for the whole lease
// window (duplicate model spend), and if it upgraded it too early the message
// would be lost on a crash. The observable contract: after a successful turn the
// marker carries the durable TTL.
func TestWorkerCommitsIdempotencyAfterTheReplyIsDurable(t *testing.T) {
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
		return &cannedModel{text: "committed reply"}, nil
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

	if err := reg.Create(ctx, llm.Endpoint{
		ID: "e-idem", Scope: llm.ScopeTenant, TenantID: "t-idem", Name: "main",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	}); err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if err := agents.Create(ctx, agent.Agent{ID: "a-idem", TenantID: "t-idem", Name: "helper"}); err != nil {
		t.Fatalf("agent: %v", err)
	}
	if _, err := agents.Publish(ctx, "a-idem", agent.RuntimeProfile{
		SystemPrompt: "be helpful", EndpointID: "e-idem",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	userMsg := model.NewUserMessage("hi")
	in := &bus.Message{
		ID: "in-idem-1", TenantID: "t-idem", AgentID: "a-idem",
		SessionID: "s-idem", Channel: "admin", UserID: "u-1",
		Content: &userMsg,
	}
	if err := w.handle(ctx, in); err != nil {
		t.Fatalf("handle: %v", err)
	}

	ttl, err := rb.Client().TTL(ctx, bus.IdemKey(in.ID)).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= time.Hour {
		t.Fatalf("idempotency marker TTL = %v, want the durable window after a successful turn", ttl)
	}

	// A redelivery of the committed message must be a no-op: no second reply.
	if err := w.handle(ctx, in); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	var rows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbox_events WHERE tenant_id = 't-idem'`).Scan(&rows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if rows != 1 {
		t.Errorf("outbox rows = %d, want 1 (the redelivery must not reply again)", rows)
	}
}

// TestWorkerReleasesTheLeaseOnAFailedTurn is the other half: a turn that failed
// before recording anything must give the message back immediately, so the
// redelivery retries it instead of waiting out the lease.
func TestWorkerReleasesTheLeaseOnAFailedTurn(t *testing.T) {
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
		return &errorModel{}, nil
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

	if err := reg.Create(ctx, llm.Endpoint{
		ID: "e-fail", Scope: llm.ScopeTenant, TenantID: "t-fail", Name: "main",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	}); err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if err := agents.Create(ctx, agent.Agent{ID: "a-fail", TenantID: "t-fail", Name: "helper"}); err != nil {
		t.Fatalf("agent: %v", err)
	}
	if _, err := agents.Publish(ctx, "a-fail", agent.RuntimeProfile{
		SystemPrompt: "be helpful", EndpointID: "e-fail",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	userMsg := model.NewUserMessage("hi")
	in := &bus.Message{
		ID: "in-fail-1", TenantID: "t-fail", AgentID: "a-fail",
		SessionID: "s-fail", Channel: "admin", UserID: "u-1",
		Content: &userMsg,
	}
	if err := w.handle(ctx, in); err == nil {
		t.Fatal("expected the failing turn to report an error so the message is redelivered")
	}

	// The claim is gone: the redelivery can take it right away.
	first, err := rb.Idempotent(ctx, in.ID)
	if err != nil {
		t.Fatalf("Idempotent: %v", err)
	}
	if !first {
		t.Error("a failed turn must release the claim instead of holding the message for the lease window")
	}
}

// errorModel fails every generation, standing in for an upstream model outage.
type errorModel struct{}

func (m *errorModel) Info() model.Info { return model.Info{Name: "error"} }

func (m *errorModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return nil, context.DeadlineExceeded
}
