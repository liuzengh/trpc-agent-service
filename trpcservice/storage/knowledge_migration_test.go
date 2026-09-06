package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func migrationFixture(t *testing.T, override ...controlplane.BackendBinding) (*controlplane.MemoryRepository, *KnowledgeRouter, controlplane.AgentRevision, controlplane.BackendMigration, *knowledgeHandle, *knowledgeHandle) {
	t.Helper()
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{"enabled":true,"embedding":{"provider":"hash","dimensions":32}}`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	for _, id := range []string{"source", "target"} {
		state := "active"
		if id == "target" {
			state = "migration_target"
		}
		data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{ID: id, TenantID: "tutorial-tenant", AppID: "tutorial-app", ResourceType: "knowledge", BackendType: "inmemory", Config: json.RawMessage(`{"dimensions":32}`), Version: 1, MigrationState: state})
		if id == "target" && len(override) > 0 {
			b := &data.BackendBindings[len(data.BackendBindings)-1]
			b.BackendType = override[0].BackendType
			b.Config = override[0].Config
		}
	}
	repo := controlplane.NewMemoryRepository(data)
	r, err := NewKnowledgeRouter(repo, secret.StaticStore{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(); repo.Close() })
	ctx := context.Background()
	scope := runtimecontext.TutorialScope()
	rev := data.Revisions[0]
	cfg, _ := parseRevisionKnowledgeConfig(rev.KnowledgeConfig)
	s, _ := r.cachedHandle(ctx, scope, rev, cfg, data.BackendBindings[len(data.BackendBindings)-2])
	target, _ := r.cachedHandle(ctx, scope, rev, cfg, data.BackendBindings[len(data.BackendBindings)-1])
	m := controlplane.BackendMigration{ID: "knowledge-move", TenantID: scope.TenantID, AppID: scope.AppID, ResourceType: "knowledge", SourceBindingID: "source", TargetBindingID: "target", State: controlplane.MigrationBackfill, Version: 1, Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`), CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := repo.CreateBackendMigration(ctx, m); err != nil {
		t.Fatal(err)
	}
	return repo, r, rev, m, s, target
}

func TestKnowledgeMigrationQdrantIntegration(t *testing.T) {
	host := os.Getenv("TEST_QDRANT_HOST")
	if host == "" {
		t.Skip("isolated Qdrant not configured")
	}
	port, _ := strconv.Atoi(os.Getenv("TEST_QDRANT_PORT"))
	if port == 0 {
		port = 6334
	}
	cfg, _ := json.Marshal(map[string]any{"host": host, "port": port, "collection_name": fmt.Sprintf("migration_%x", time.Now().UnixNano()), "dimensions": 32})
	_, r, rev, m, source, target := migrationFixture(t, controlplane.BackendBinding{BackendType: "qdrant", Config: cfg})
	ctx := context.Background()
	scope := runtimecontext.TutorialScope()
	for i := 0; i < 61; i++ {
		if _, err := upsertKnowledgeHandle(ctx, scope, source, KnowledgeDocument{ID: fmt.Sprintf("legacy-%d", i), Content: fmt.Sprintf("legacy migration policy %d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	finishKnowledgeMigration(t, r, rev, m.ID)
	ids, err := migrationIDs(ctx, scope, target.store)
	if err != nil || len(ids) != 61 {
		t.Fatalf("migrated IDs=%d err=%v", len(ids), err)
	}
}
func finishKnowledgeMigration(t *testing.T, r *KnowledgeRouter, rev controlplane.AgentRevision, id string) {
	t.Helper()
	ctx := context.Background()
	scope := runtimecontext.TutorialScope()
	for i := 0; i < 20; i++ {
		done, err := r.BackfillKnowledgeBatch(ctx, scope, rev, id)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if i == 19 {
			t.Fatal("backfill did not terminate")
		}
	}
	for i := 0; i < 20; i++ {
		done, err := r.VerifyKnowledgeBatch(ctx, scope, rev, id)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
	}
	t.Fatal("verification did not terminate")
}
func TestKnowledgeHistoricalMigrationAndStaleProof(t *testing.T) {
	repo, r, rev, m, source, target := migrationFixture(t)
	ctx := context.Background()
	scope := runtimecontext.TutorialScope()
	// Legacy history exists only in the source vector store: no document
	// manifest or pending intent can enumerate it for us.
	for i := 0; i < 123; i++ {
		if _, err := upsertKnowledgeHandle(ctx, scope, source, KnowledgeDocument{ID: fmt.Sprintf("doc-%03d", i), Content: fmt.Sprintf("historical policy number %d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := upsertKnowledgeHandle(ctx, scope, target, KnowledgeDocument{ID: "stale-target", Content: "deleted document"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.TransitionBackendMigration(ctx, m.TenantID, m.ID, controlplane.MigrationCutover, 1, nil, []byte(`{"passed":true}`)); err == nil {
		t.Fatal("caller forged proof")
	}
	done, err := r.BackfillKnowledgeBatch(ctx, scope, rev, m.ID)
	if err != nil || done {
		t.Fatalf("first batch=%t %v", done, err)
	}
	if n, _ := target.store.Count(ctx); n != migrationBatch {
		t.Fatalf("first batch chunks=%d", n)
	}
	// A write between batches invalidates the cursor; shortened/deleted
	// content must not reappear when the old scan resumes.
	if err := r.DeleteDocument(ctx, scope, rev, "doc-002"); err != nil {
		t.Fatal(err)
	}
	finishKnowledgeMigration(t, r, rev, m.ID)
	if n, _ := target.store.Count(ctx); n != 122 {
		t.Fatalf("historical chunks=%d", n)
	}
	if _, err := r.UpsertDocument(ctx, scope, rev, KnowledgeDocument{ID: "new", Content: "latest policy"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.TransitionBackendMigration(ctx, m.TenantID, m.ID, controlplane.MigrationCutover, 1, nil, nil); err == nil {
		t.Fatal("stale proof accepted")
	}
	finishKnowledgeMigration(t, r, rev, m.ID)
	m, err = repo.TransitionBackendMigration(ctx, m.TenantID, m.ID, controlplane.MigrationCutover, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.TransitionBackendMigration(ctx, m.TenantID, m.ID, controlplane.MigrationCompleted, m.Version, nil, nil); err != nil {
		t.Fatal(err)
	}
	kb, _, err := r.KnowledgeForRevision(ctx, scope, rev)
	if err != nil {
		t.Fatal(err)
	}
	result, err := kb.Search(ctx, &knowledge.SearchRequest{Query: "latest policy"})
	if err != nil || result.Document == nil {
		t.Fatal("cutover lost InMemory store when binding version changed")
	}
	changed := rev
	changed.KnowledgeConfig = json.RawMessage(`{"enabled":true,"max_results":2,"chunk_size":300,"embedding":{"provider":"hash","dimensions":32}}`)
	changed.Checksum = controlplane.RevisionChecksum(changed)
	kb, _, err = r.KnowledgeForRevision(ctx, scope, changed)
	if err != nil {
		t.Fatal(err)
	}
	result, err = kb.Search(ctx, &knowledge.SearchRequest{Query: "latest policy"})
	if err != nil || result.Document == nil {
		t.Fatal("retrieval configuration changed the physical data store")
	}
}

type failingVector struct {
	vectorstore.VectorStore
	fail bool
}

func (s *failingVector) Add(ctx context.Context, doc *document.Document, v []float64) error {
	if s.fail {
		return errors.New("synthetic vector failure")
	}
	return s.VectorStore.Add(ctx, doc, v)
}
func TestKnowledgePendingIntentRepairAndVerificationMismatch(t *testing.T) {
	repo, r, rev, m, _, target := migrationFixture(t)
	ctx := context.Background()
	scope := runtimecontext.TutorialScope()
	broken := &failingVector{target.store, true}
	target.store = broken
	if _, err := r.UpsertDocument(ctx, scope, rev, KnowledgeDocument{ID: "pending", Content: "repair this write"}); err == nil {
		t.Fatal("expected secondary write failure")
	}
	if err := repo.WithKnowledgeSync(ctx, scope.TenantID, scope.AppID, func(_ context.Context, s *controlplane.KnowledgeSync, _ func() error) error {
		if len(s.Intents) != 1 {
			t.Fatal("failed write lost its durable intent")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	broken.fail = false
	if _, err := r.BackfillKnowledgeBatch(ctx, scope, rev, m.ID); err != nil {
		t.Fatal(err)
	}
	id := knowledgeChunkID(scope.TenantID, scope.AppID, "pending", 0)
	doc, v, err := target.store.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	doc.Content = "tampered"
	if err = target.store.Add(ctx, doc, v); err != nil {
		t.Fatal(err)
	}
	if _, err = r.VerifyKnowledgeBatch(ctx, scope, rev, m.ID); err == nil {
		t.Fatal("content corruption passed verification")
	}
	finishKnowledgeMigration(t, r, rev, m.ID)
	if err := repo.WithKnowledgeSync(ctx, scope.TenantID, scope.AppID, func(_ context.Context, s *controlplane.KnowledgeSync, _ func() error) error {
		if len(s.Intents) != 0 || !s.Migrations[m.ID].Passed {
			t.Fatal("repair not verified")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	forged := rev
	forged.TenantID = "other"
	if _, err := r.BackfillKnowledgeBatch(ctx, scope, forged, m.ID); err == nil {
		t.Fatal("cross-tenant revision accepted")
	}
}
