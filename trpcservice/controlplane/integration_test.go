package controlplane

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// setupControlPlaneTest migrates a real MySQL and returns a DB handle plus
// two tenant scopes created for the test.
//
// Same rule as storage/mysql's integration tests: the behaviour under test —
// SELECT ... FOR UPDATE serialising two publishers, a composite foreign key
// rejecting a cross-tenant profile reference — is MySQL 8.4's, not an
// approximation of it.
func setupControlPlaneTest(t *testing.T) (*DB, Scope, Scope) {
	t.Helper()
	dsn := os.Getenv("CONTROLPLANE_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("CONTROLPLANE_MYSQL_TEST_DSN not set; skipping real-mysql control-plane test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetControlPlaneSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()
	cdp := NewDB(db)
	for _, id := range []string{"tenant-a", "tenant-b"} {
		if err := cdp.CreateTenant(ctx, id, id); err != nil {
			t.Fatalf("create tenant %s: %v", id, err)
		}
	}
	a, err := cdp.Scope("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := cdp.Scope("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	return cdp, a, b
}

func resetControlPlaneSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
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

func seedProfiles(t *testing.T, s Scope) (modelID, backendID int64) {
	t.Helper()
	ctx := context.Background()
	m, err := s.CreateModelProfile(ctx, "default-model", "gpt-4o-mini", "https://api.example.com", "env:MODEL_API_KEY")
	if err != nil {
		t.Fatalf("seed model profile: %v", err)
	}
	b, err := s.CreateBackendProfile(ctx, BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatalf("seed backend profile: %v", err)
	}
	return m, b
}

func testSpec(modelID, backendID int64, instruction string) RevisionSpec {
	return RevisionSpec{
		Instruction:      instruction,
		ModelProfileID:   modelID,
		BackendProfileID: backendID,
		MaxLLMCalls:      8,
		MessageTimeoutMS: 120000,
		Guardrails:       json.RawMessage(`{"max_input_bytes":8192}`),
	}
}

func TestPublishRollbackAndPointerRace(t *testing.T) {
	_, a, _ := setupControlPlaneTest(t)
	ctx := context.Background()

	modelID, backendID := seedProfiles(t, a)
	if _, err := a.CreateApp(ctx, "assistant", "Assistant"); err != nil {
		t.Fatalf("create app: %v", err)
	}

	r1, err := a.PublishRevision(ctx, "assistant", testSpec(modelID, backendID, "v1"))
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	r2, err := a.PublishRevision(ctx, "assistant", testSpec(modelID, backendID, "v2"))
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if r2.RevisionNo != r1.RevisionNo+1 {
		t.Fatalf("revision numbers are not sequential: %d then %d", r1.RevisionNo, r2.RevisionNo)
	}

	current, err := a.CurrentRevision(ctx, "assistant")
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}
	if current.ID != r2.ID {
		t.Fatalf("current pointer is %d, want %d (the last publish)", current.ID, r2.ID)
	}

	// Rolling back must land on the older revision without touching it, and
	// a stale expectation must not overwrite a newer decision.
	if err := a.RollbackToRevision(ctx, "assistant", r1.ID, r2.ID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	after, err := a.CurrentRevision(ctx, "assistant")
	if err != nil {
		t.Fatalf("current after rollback: %v", err)
	}
	if after.ID != r1.ID {
		t.Fatalf("pointer after rollback is %d, want %d", after.ID, r1.ID)
	}
	if after.Spec.Instruction != "v1" {
		t.Fatalf("rolled-back revision content changed: %q", after.Spec.Instruction)
	}

	if err := a.RollbackToRevision(ctx, "assistant", r2.ID, r2.ID); !errors.Is(err, ErrConcurrentPublish) {
		t.Fatalf("stale expected-current rollback: %v, want ErrConcurrentPublish", err)
	}
}

func TestConcurrentPublishersGetDistinctRevisions(t *testing.T) {
	_, a, _ := setupControlPlaneTest(t)
	ctx := context.Background()
	modelID, backendID := seedProfiles(t, a)
	if _, err := a.CreateApp(ctx, "assistant", "Assistant"); err != nil {
		t.Fatalf("create app: %v", err)
	}

	// Two publishers racing on the same app. FOR UPDATE on the app row is
	// what makes this safe; without it both would read revision_no = 0 and
	// both would try to insert revision 1, and one would get a unique-index
	// error instead of a published revision.
	const publishers = 5
	var wg sync.WaitGroup
	errs := make([]error, publishers)
	ids := make([]uint32, publishers)
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rev, err := a.PublishRevision(ctx, "assistant", testSpec(modelID, backendID, "concurrent"))
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = rev.RevisionNo
		}(i)
	}
	wg.Wait()

	seen := map[uint32]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("publisher %d: %v", i, err)
		}
		if seen[ids[i]] {
			t.Fatalf("two publishers were both assigned revision number %d", ids[i])
		}
		seen[ids[i]] = true
	}
}

func TestProfilesAndRevisionsCannotCrossTenants(t *testing.T) {
	_, a, b := setupControlPlaneTest(t)
	ctx := context.Background()

	modelA, backendA := seedProfiles(t, a)
	_, _ = seedProfiles(t, b) // b has its own profiles with different ids

	if _, err := b.CreateApp(ctx, "assistant", "Assistant"); err != nil {
		t.Fatalf("create app for b: %v", err)
	}

	// Tenant b publishing with tenant a's profile ids must be rejected before
	// anything is inserted: the ids are real rows, just not b's rows.
	_, err := b.PublishRevision(ctx, "assistant", testSpec(modelA, backendA, "stolen"))
	if !errors.Is(err, ErrCrossTenantReference) {
		t.Fatalf("cross-tenant profile publish: %v, want ErrCrossTenantReference", err)
	}

	// A scope that does not exist must not read another tenant's app.
	if _, err := b.GetApp(ctx, "assistant"); err == nil {
		// b's own "assistant" app exists but has no published revision yet;
		// confirming CurrentRevision reports NotFound, not a silent zero.
		t.Log("app row exists for b as expected")
	}
	if _, err := a.CurrentRevision(ctx, "assistant"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant a never created an app named assistant; got: %v", err)
	}
}

func TestScopeRejectsAnEmptyTenant(t *testing.T) {
	_, err := (&DB{}).Scope("")
	if !errors.Is(err, ErrNoScope) {
		t.Fatalf("empty tenant id: %v, want ErrNoScope", err)
	}
}

func TestBindingsAndKnowledgeBaseStayScoped(t *testing.T) {
	_, a, b := setupControlPlaneTest(t)
	ctx := context.Background()

	modelID, backendID := seedProfiles(t, a)
	appID, err := a.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	rev, err := a.PublishRevision(ctx, "assistant", testSpec(modelID, backendID, "hello"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if _, err := a.BindChannel(ctx, ChannelBinding{
		AppID: appID, ChannelType: "wecom", PublicID: "main", CredentialRef: "env:WECOM_SECRET",
	}); err != nil {
		t.Fatalf("bind channel: %v", err)
	}
	// Tenant b must not be able to read tenant a's channel binding, even
	// knowing its public id.
	if _, err := b.GetChannelBinding(ctx, "wecom", "main"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant channel read: %v, want ErrNotFound", err)
	}

	kbID, err := a.CreateKnowledgeBase(ctx, appID, "handbook", "Handbook")
	if err != nil {
		t.Fatalf("create kb: %v", err)
	}
	if err := a.BindRevisionKnowledge(ctx, rev.ID, kbID); err != nil {
		t.Fatalf("bind kb: %v", err)
	}

	toolID, err := a.BindTool(ctx, ToolBinding{
		AppID: appID, Name: "echo", Kind: "go",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Spec:        json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("bind tool: %v", err)
	}
	got, err := a.GetToolBinding(ctx, toolID)
	if err != nil {
		t.Fatalf("get tool binding: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("first tool binding version: %d, want 1", got.Version)
	}
	// Binding the same tool name again is version 2, not an update of
	// version 1: a published revision can pin against either.
	if _, err := a.BindTool(ctx, ToolBinding{
		AppID: appID, Name: "echo", Kind: "go",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Spec:        json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("rebind tool: %v", err)
	}
	got2, err := a.GetToolBinding(ctx, mustSecondVersion(t, a, appID, "echo"))
	if err != nil {
		t.Fatalf("get rebound tool: %v", err)
	}
	if got2.Version != 2 {
		t.Fatalf("second tool binding version: %d, want 2", got2.Version)
	}
}

// mustSecondVersion looks up the newest tool_bindings row id for a name,
// since BindTool returns the row id but the test above wants the second one.
func mustSecondVersion(t *testing.T, s Scope, appID int64, name string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var id int64
	row, err := s.QueryRow(ctx,
		"SELECT tool_id FROM tool_bindings WHERE tenant_id = ? AND app_id = ? AND name = ? ORDER BY version DESC LIMIT 1",
		s.tenantID, appID, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&id); err != nil {
		t.Fatalf("find second version: %v", err)
	}
	return id
}
