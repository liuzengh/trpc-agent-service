//go:build integration

// Integration test for the MySQL-backed management domains: tenants, model
// endpoints, tools, and agents. One container runs the full schema
// (deployments/mysql/init); every domain verifies CRUD plus restart recovery
// (a fresh manager over the same database).
package trpcservice_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

var (
	once     sync.Once
	platform struct {
		dsn string
		err error
	}
)

func TestMain(m *testing.M) {
	once.Do(startMySQL)
	if platform.err != nil {
		// No Docker (or broken MySQL image): skip the whole suite.
		os.Stderr.WriteString("skip mysql persistence suite: " + platform.err.Error() + "\n")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func startMySQL() {
	ctx := context.Background()
	scripts, err := filepath.Glob(filepath.Join("..", "deployments", "mysql", "init", "*.sql"))
	if err != nil {
		platform.err = err
		return
	}

	opts := append([]testcontainers.ContainerCustomizer{
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
	}, mysql.WithScripts(scripts...))
	c, err := mysql.Run(ctx, "mysql:8.0", opts...)
	if err != nil {
		platform.err = err
		return
	}
	dsn, err := c.ConnectionString(ctx, "multiStatements=true")
	if err != nil {
		_ = c.Terminate(ctx)
		platform.err = err
		return
	}
	platform.dsn = dsn
}

// managers bundles fresh managers over the shared database; building a second
// bundle simulates a process restart.
type managers struct {
	tenants *tenant.Manager
	agents  *agent.Manager
	llm     *llm.Registry
	tools   *tool.Registry
	kbs     *knowledge.Manager
}

func openManagers(t *testing.T) *managers {
	t.Helper()
	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	kbs := knowledge.NewMySQLManager(db,
		knowledge.InMemoryVectorStoreFactory(),
		func(_ context.Context, _ *knowledge.KnowledgeBase) (embedder.Embedder, error) {
			return nil, errors.New("no embedder in metadata tests")
		})
	return &managers{
		tenants: tenant.NewMySQLManager(db),
		agents:  agent.NewMySQLManager(db, llm.NewMySQLRegistry(db, nil)),
		llm:     llm.NewMySQLRegistry(db, nil),
		tools:   tool.NewMySQLRegistry(db),
		kbs:     kbs,
	}
}

func TestTenantMySQLPersistence(t *testing.T) {
	ctx := context.Background()
	m := openManagers(t)

	in := &tenant.Tenant{
		ID:          "t-1",
		Name:        "acme",
		Status:      tenant.StatusActive,
		DataBackend: map[string]string{tenant.DomainSession: tenant.BackendRedis},
	}
	if err := m.tenants.Create(ctx, in); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.tenants.Create(ctx, in); err == nil {
		t.Error("duplicate create should fail")
	}

	got, err := m.tenants.Get(ctx, "t-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.DataBackend[tenant.DomainSession] != tenant.BackendRedis {
		t.Errorf("data_backend[session] = %q, want redis", got.DataBackend[tenant.DomainSession])
	}

	// restart: a fresh manager sees the same tenant
	if _, err := openManagers(t).tenants.Get(ctx, "t-1"); err != nil {
		t.Fatalf("get after restart: %v", err)
	}
}

func TestEndpointMySQLPersistence(t *testing.T) {
	ctx := context.Background()
	m := openManagers(t)

	ep := llm.Endpoint{
		ID: "e-1", Scope: llm.ScopeTenant, TenantID: "t-1", Name: "main",
		Provider: "anthropic", BaseURL: "https://api.x", ModelName: "claude", APIKey: "secret",
	}
	if err := m.llm.Create(ctx, ep); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.llm.Create(ctx, ep); err == nil {
		t.Error("duplicate create should fail")
	}

	got, err := openManagers(t).llm.Get(ctx, "e-1") // fresh registry = restart
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if got.Provider != "anthropic" || got.APIKey != "secret" || got.TenantID != "t-1" {
		t.Errorf("endpoint round-trip mismatch: %+v", got)
	}

	eps, err := openManagers(t).llm.List(ctx, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(eps) == 0 {
		t.Error("list should include the persisted endpoint")
	}

	if err := m.llm.Delete(ctx, "e-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.llm.Get(ctx, "e-1"); !errors.Is(err, llm.ErrEndpointNotFound) {
		t.Errorf("get after delete = %v, want ErrEndpointNotFound", err)
	}
}

func TestToolMySQLPersistence(t *testing.T) {
	ctx := context.Background()
	m := openManagers(t)

	d := tool.Definition{
		ID: "tool-1", Scope: tool.ScopeTenant, TenantID: "t-1",
		Name: "search", Description: "web search", RiskLevel: tool.RiskHigh,
	}
	if err := m.tools.Register(ctx, d); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := m.tools.Grant(ctx, "agent-1", "tool-1"); err != nil {
		t.Fatalf("grant: %v", err)
	}

	// restart: definition and grant survive
	m2 := openManagers(t)
	got, err := m2.tools.Get(ctx, "tool-1")
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if got.Name != "search" || got.RiskLevel != tool.RiskHigh {
		t.Errorf("tool round-trip mismatch: %+v", got)
	}
	ok, err := m2.tools.IsAllowed(ctx, "agent-1", "tool-1")
	if err != nil || !ok {
		t.Errorf("IsAllowed after restart = %v, %v; want true, nil", ok, err)
	}
	allowed, err := m2.tools.Allowed(ctx, "agent-1")
	if err != nil || len(allowed) != 1 || allowed[0] != "tool-1" {
		t.Errorf("Allowed after restart = %v, %v; want [tool-1]", allowed, err)
	}

	// cross-tenant isolation: tenant-b sees no tenant-a tool
	list, err := m2.tools.List(ctx, "t-b")
	if err != nil {
		t.Fatalf("list t-b: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("t-b should see no tenant tools, got %d", len(list))
	}
}

func TestAgentMySQLPersistence(t *testing.T) {
	ctx := context.Background()
	m := openManagers(t)

	a := agent.Agent{ID: "a-1", TenantID: "t-1", Name: "helper"}
	if err := m.agents.Create(ctx, a); err != nil {
		t.Fatalf("create: %v", err)
	}

	v1 := agent.RuntimeProfile{SystemPrompt: "v1", EndpointID: "e-1", ToolIDs: []string{"tool-1"}}
	v2 := agent.RuntimeProfile{SystemPrompt: "v2", EndpointID: "e-1"}
	got1, err := m.agents.Publish(ctx, "a-1", v1)
	if err != nil || got1 != 1 {
		t.Fatalf("publish v1 = %d, %v; want 1, nil", got1, err)
	}
	got2, err := m.agents.Publish(ctx, "a-1", v2)
	if err != nil || got2 != 2 {
		t.Fatalf("publish v2 = %d, %v; want 2, nil", got2, err)
	}

	// restart: version state survives; current is v2
	m2 := openManagers(t)
	p, err := m2.agents.Resolve(ctx, "a-1")
	if err != nil {
		t.Fatalf("resolve after restart: %v", err)
	}
	if p.SystemPrompt != "v2" {
		t.Errorf("resolved prompt = %q, want v2", p.SystemPrompt)
	}

	if err := m2.agents.Rollback(ctx, "a-1", 1); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	p, _ = m2.agents.Resolve(ctx, "a-1")
	if p.SystemPrompt != "v1" {
		t.Errorf("after rollback resolved prompt = %q, want v1", p.SystemPrompt)
	}

	// rollback survives restart too
	p, err = openManagers(t).agents.Resolve(ctx, "a-1")
	if err != nil || p.SystemPrompt != "v1" {
		t.Errorf("rollback lost after restart: %v %q", err, p.SystemPrompt)
	}

	// cross-tenant isolation
	list, err := m2.agents.List(ctx, "t-b")
	if err != nil {
		t.Fatalf("list t-b: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("t-b should see no agents, got %d", len(list))
	}
}

func TestKnowledgeMySQLPersistence(t *testing.T) {
	ctx := context.Background()
	m := openManagers(t)

	kb := &knowledge.KnowledgeBase{
		ID: "kb-1", TenantID: "t-1", Name: "docs",
		EmbeddingEndpointID: "e-1", Dimension: 256,
	}
	if err := m.kbs.Create(ctx, kb); err != nil {
		t.Fatalf("create: %v", err)
	}

	// restart: KB metadata survives, custom dimension included
	kb2, err := openManagers(t).kbs.Get(ctx, "kb-1")
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if kb2.CollectionName != "t_1_kb_1" || kb2.Dimension != 256 {
		t.Errorf("kb round-trip mismatch: %+v", kb2)
	}

	// cross-tenant isolation
	list, err := openManagers(t).kbs.List(ctx, "t-b")
	if err != nil || len(list) != 0 {
		t.Errorf("t-b should see no KBs, got %v (%v)", list, err)
	}

	// failed ingestion (no embedder configured) still records document state
	if err := m.kbs.Create(ctx, &knowledge.KnowledgeBase{
		ID: "kb-2", TenantID: "t-1", Name: "more", EmbeddingEndpointID: "e-1",
	}); err != nil {
		t.Fatalf("create kb-2: %v", err)
	}
	if err := m.kbs.AddDocument(ctx, &knowledge.Document{ID: "d-1", KBID: "kb-2", Title: "x", Text: "content"}); err == nil {
		t.Error("ingestion without embedder should fail")
	}
	doc, err := openManagers(t).kbs.GetDocument(ctx, "d-1")
	if err != nil {
		t.Fatalf("get document after restart: %v", err)
	}
	if doc.Status != knowledge.StatusFailed || doc.Error == "" {
		t.Errorf("document state = %+v, want failed with reason", doc)
	}
}
